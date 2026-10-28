#!/usr/bin/env python3
"""Write the ``stac-geoparquet`` footer key into a Parquet file.

A stac-geoparquet file is a Parquet file whose rows are STAC Items. The
specification lets it carry the Collections those Items belong to in the
file's own footer key-value metadata, under the key ``stac-geoparquet``, so a
reader that has the file has the Collection too and does not need a catalog
to interpret it.

The value is a JSON object::

    {"version": "1.1.0", "collections": {"<collection id>": { ...Collection... }}}

``collections`` is a **map keyed by collection id**. ``version`` is the version
of the *stac-geoparquet* specification, not of STAC or of GeoParquet, and the
specification defines only ``1.0.0`` and ``1.1.0``.

The 1.0.0 release defined a *singular* ``collection`` key holding one
Collection; 1.1.0 replaced it with the ``collections`` map and deprecated the
singular one. So a 1.1.0 payload never carries ``collection``, while a 1.0.0
payload carries it (and the map beside it, which is what the reference
``stac-geoparquet`` library writes under 1.0.0) -- and, since ``collection``
can name only one, a 1.0.0 payload naming two Collections is refused.

A file that already carries a ``stac-geoparquet`` key keeps every collection
it already named -- in its ``collections`` map or in a 1.0.0 singular
``collection`` -- and any other top-level field: the new call's Collection is
unioned into the existing map (replacing its own id if that id was already
there), rather than the whole key being overwritten. The map exists so one
stac-geoparquet file can index Items from several Collections, and
overwriting it on every call would throw that away. The declared ``version``
always becomes the one this call asked for.

The file path patches the footer in place and copies every page below it
verbatim (``core/parquet_footer.py``, #1141), the same way ``add
bbox-metadata`` does: adding one footer key does not need the data decoded, so
the compression codec *and level*, the encodings, the bloom filters, the page
index, the row-group boundaries and the row order all come out as they went
in. The file is still copied once, byte for byte, to a staged file that
replaces the original -- so it costs a sequential read and write and needs
room for a second copy -- but nothing is decoded or re-encoded. Unlike ``add bbox-metadata`` there is no DuckDB rewrite behind it -- a footer
that cannot be patched is reported rather than re-encoded, because re-encoding
to add a key that is not the ``geo`` block would cost the file's compression
level to buy nothing.

Note: only local files. A remote URL is refused rather than downloaded.
"""

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow.parquet as pq

from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.file_utils import is_same_file_path
from geoparquet_io.core.logging_config import debug, success, warn
from geoparquet_io.core.parquet_footer import FooterPatchUnsupported, patch_footer_kv
from geoparquet_io.core.remote import is_remote_url

if TYPE_CHECKING:
    import pyarrow as pa

#: The footer key the stac-geoparquet specification reserves.
STAC_GEOPARQUET_KEY = "stac-geoparquet"

#: The specification versions that define the key's shape. There is no third.
SUPPORTED_STAC_GEOPARQUET_VERSIONS = ("1.0.0", "1.1.0")

DEFAULT_STAC_GEOPARQUET_VERSION = "1.1.0"

#: The columns the 1.1.0 specification lists as required for an Item row.
REQUIRED_ITEM_COLUMNS = ("id", "geometry", "bbox", "links", "assets", "stac_extensions")

#: The payload fields this module interprets; any other top-level field an
#: existing value carries is passed through untouched.
_INTERPRETED_FIELDS = ("version", "collection", "collections")


def _validated_version(version: str) -> str:
    """The stac-geoparquet spec version, or a refusal naming the ones that exist.

    The gate lives here rather than only in the Click choice so that
    ``ops.add_stac_metadata`` and ``Table.add_stac_metadata`` refuse exactly
    what the CLI refuses, with the same sentence.
    """
    if version in SUPPORTED_STAC_GEOPARQUET_VERSIONS:
        return version
    allowed = ", ".join(SUPPORTED_STAC_GEOPARQUET_VERSIONS)
    raise GeoParquetError(
        f"Unsupported stac-geoparquet version '{version}'. "
        f"The specification defines {allowed} and nothing else, and the "
        f"version written into the footer is the one a reader dispatches on."
    )


def load_collection(collection: "dict | str | Path") -> dict:
    """A STAC Collection as a dict, read from a path when one is given.

    Args:
        collection: The Collection itself, or a path to a JSON file holding it.

    Returns:
        dict: The parsed Collection.

    Raises:
        GeoParquetError: If the file cannot be read, is not JSON, or holds
            something other than a JSON object.
    """
    if isinstance(collection, dict):
        return collection

    source = Path(collection)
    try:
        text = source.read_text(encoding="utf-8")
    except OSError as exc:
        raise GeoParquetError(f"Cannot read the STAC Collection at {source}: {exc}") from exc

    try:
        parsed = json.loads(text)
    # RecursionError: a pathologically nested document, which the decoder
    # reports by blowing the stack rather than as a JSONDecodeError.
    except (json.JSONDecodeError, RecursionError) as exc:
        raise GeoParquetError(f"{source} is not valid JSON: {exc}") from exc

    if not isinstance(parsed, dict):
        raise GeoParquetError(
            f"{source} does not hold a JSON object, so it is not a STAC Collection."
        )
    return parsed


def _collection_id(collection: dict) -> str:
    """The id the ``collections`` map is keyed by, or a refusal.

    The spec: "the ID used as the key of the mapping must match the ID in the
    Collection object". Taking the key from the object rather than from a
    separate argument is what makes that true by construction. An Item's
    ``collection`` field names this id, so a Collection without one cannot be
    looked up and there is no key to file it under either.
    """
    identifier = collection.get("id")
    if isinstance(identifier, str) and identifier:
        kind = collection.get("type")
        if kind is not None and kind != "Collection":
            warn(
                f"The STAC object '{identifier}' has type '{kind}', not 'Collection'. "
                "stac-geoparquet's footer holds the Collections the rows belong to; "
                "writing it anyway."
            )
        return identifier
    raise GeoParquetError(
        "The STAC Collection has no 'id'. stac-geoparquet keys its 'collections' "
        "map by collection id -- it is what an Item's 'collection' field points "
        "at -- so a Collection without one cannot be written into the footer."
    )


def stac_geoparquet_metadata(
    collection: "dict | str | Path",
    version: str = DEFAULT_STAC_GEOPARQUET_VERSION,
) -> dict:
    """The value of the ``stac-geoparquet`` footer key, as a dict.

    Args:
        collection: A STAC Collection, or a path to a JSON file holding one.
        version: The stac-geoparquet specification version to declare.

    Returns:
        dict: ``{"version": ..., "collections": {<id>: <collection>}}``. The
        deprecated singular ``collection`` key is not written.

    Raises:
        GeoParquetError: If the version is not one the spec defines, or the
            Collection cannot be read or has no ``id``.
    """
    version = _validated_version(version)
    parsed = load_collection(collection)
    return {"version": version, "collections": {_collection_id(parsed): parsed}}


def _parsed_existing_payload(raw: bytes | None, source: str) -> dict | None:
    """Any ``stac-geoparquet`` value `source` already carries, parsed -- or ``None``.

    Shared by the file path and the in-memory path so a value neither of them
    wrote is refused with the same sentence rather than silently discarded or
    merged with: not valid JSON, not a JSON object, or a ``collections`` entry
    that is itself not a JSON object (the one shape :func:`_merged_payload`
    could merge into) are all refusals here, before `_merged_payload` ever
    sees the value.
    """
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, RecursionError) as exc:
        raise GeoParquetError(
            f"{source} already carries a stac-geoparquet key that is not valid JSON: {exc}"
        ) from exc
    if not isinstance(parsed, dict):
        raise GeoParquetError(
            f"{source} already carries a stac-geoparquet key that is not a JSON object, "
            "so it has no 'collections' map to merge the new Collection into."
        )
    carried = parsed.get("collections")
    if carried is not None and not isinstance(carried, dict):
        raise GeoParquetError(
            f"{source} already carries a stac-geoparquet 'collections' entry that is not "
            "a JSON object (a map keyed by collection id), so it cannot be merged with."
        )
    singular = parsed.get("collection")
    if singular is not None and not (
        isinstance(singular, dict) and isinstance(singular.get("id"), str) and singular["id"]
    ):
        raise GeoParquetError(
            f"{source} already carries a stac-geoparquet singular 'collection' that is not "
            "a Collection object with an 'id', so it cannot be folded into 'collections'."
        )
    return parsed


def _carried_collections(existing: dict) -> dict[str, Any]:
    """Every Collection an existing value names, keyed by id.

    The ``collections`` map, plus a 1.0.0 singular ``collection`` under its own
    id (the map's entry wins if both name it). Both shapes were validated by
    :func:`_parsed_existing_payload`.
    """
    collections: dict[str, Any] = {}
    singular = existing.get("collection")
    if singular is not None:
        collections[singular["id"]] = singular
    collections.update(existing.get("collections") or {})
    return collections


def _merged_payload(existing: dict | None, payload: dict) -> dict[str, Any]:
    """`payload`'s one collection, unioned into any ``collections`` map already there.

    ``collections`` is a map precisely so one stac-geoparquet file can index
    Items from more than one Collection -- replacing the whole key on every
    call would throw that away and leave only the most recently added
    Collection, silently. A new collection id is added alongside the ones
    already present; a repeated id replaces its own entry, since writing the
    same id again is what a caller asking to update it looks like.

    A 1.0.0 singular ``collection`` already there is folded into the map under
    its own id, so a file a 1.0.0 writer produced loses nothing. Any top-level
    field this module does not interpret is carried through as it was.

    `existing`'s ``version``, if it names a different one, does not survive:
    ``version`` describes how to interpret the write being made right now, not
    a property of any one collection, so a call that did not ask to change it
    should get the version it asked for rather than whatever an earlier call
    declared. Under 1.0.0 the singular ``collection`` is written too, and a
    map of more than one Collection -- which 1.0.0 cannot express -- is refused.
    """
    existing = existing or {}
    collections = {**_carried_collections(existing), **payload["collections"]}

    version = payload["version"]
    merged = {k: v for k, v in existing.items() if k not in _INTERPRETED_FIELDS}
    merged["version"] = version
    merged["collections"] = collections
    if version == "1.0.0":
        if len(collections) > 1:
            raise GeoParquetError(
                "stac-geoparquet 1.0.0 holds a single 'collection', but the result would "
                f"name {len(collections)} ({', '.join(collections)}). Use version 1.1.0, "
                "whose 'collections' map holds several."
            )
        merged["collection"] = next(iter(collections.values()))
    return merged


def _serialized(payload: dict) -> str:
    """`payload` as strict JSON, or a refusal.

    ``allow_nan=False`` because ``NaN``/``Infinity`` are not JSON: Python's
    encoder writes them, and strict readers (JavaScript, serde) then reject the
    whole footer value. A dict Collection holding a non-JSON value (a
    ``datetime``, say) is refused here too, rather than escaping as a TypeError.
    """
    try:
        return json.dumps(payload, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise GeoParquetError(f"The stac-geoparquet value is not valid JSON: {exc}") from exc


def _warn_if_not_items(column_names: "list[str]", source: str) -> None:
    """A warning when `source` lacks columns the spec requires of an Item row.

    stac-geoparquet's rows are STAC Items; the spec does not oblige a writer to
    check, and a partial file may be the point, so this informs, never refuses.
    """
    missing = [name for name in REQUIRED_ITEM_COLUMNS if name not in column_names]
    if missing:
        warn(
            f"{source} has no {', '.join(missing)} column(s), which stac-geoparquet "
            "requires of a STAC Item row. Writing the footer key anyway."
        )


def _summary(payload: dict, added: str) -> str:
    """The success line: the Collection this call wrote, then any already there."""
    line = f"Added stac-geoparquet {payload['version']} metadata for collection '{added}'"
    others = [name for name in payload["collections"] if name != added]
    if others:
        line += f" (the file also names {', '.join(others)})"
    return line


def add_stac_metadata_table(
    table: "pa.Table",
    collection: "dict | str | Path",
    *,
    version: str = DEFAULT_STAC_GEOPARQUET_VERSION,
) -> "pa.Table":
    """Set the ``stac-geoparquet`` key on an in-memory Arrow table's schema.

    The in-memory counterpart of :func:`add_stac_metadata`, and the single
    implementation behind ``Table.add_stac_metadata`` and
    ``ops.add_stac_metadata_table``, so the Python API refuses what the CLI
    refuses. Every other schema metadata key the table carries -- ``geo``
    first among them -- is kept.

    A ``stac-geoparquet`` key the table already carries is merged with, not
    replaced by, this call's Collection: see :func:`_merged_payload`.

    Args:
        table: Input Arrow table.
        collection: A STAC Collection, or a path to a JSON file holding one.
        version: The stac-geoparquet specification version to declare.

    Returns:
        pa.Table: A new table carrying the key. The columns are untouched.

    Raises:
        GeoParquetError: If the version or the Collection is refused, or the
            table already carries a ``stac-geoparquet`` key that is not valid
            JSON.
    """
    payload = stac_geoparquet_metadata(collection, version)
    schema_metadata = dict(table.schema.metadata) if table.schema.metadata else {}
    existing = _parsed_existing_payload(
        schema_metadata.get(STAC_GEOPARQUET_KEY.encode("utf-8")), "the table"
    )
    merged = _serialized(_merged_payload(existing, payload))
    _warn_if_not_items(table.column_names, "The table")
    schema_metadata[STAC_GEOPARQUET_KEY.encode("utf-8")] = merged.encode("utf-8")
    return table.replace_schema_metadata(schema_metadata)


def _refuse_remote(*paths: "str | None") -> None:
    """The footer patch reads and writes a local file; a URL is not one."""
    for path in paths:
        if path is not None and is_remote_url(path):
            raise GeoParquetError(
                f"Remote URLs are not supported for footer metadata: {path}\n"
                "Download the file locally, add the metadata, then upload it."
            )


def _existing_payload(parquet_file: str) -> "tuple[dict | None, list[str]]":
    """`parquet_file`'s current ``stac-geoparquet`` value, and its column names.

    Read through the *effective* Arrow schema rather than the plain footer
    entries directly: pyarrow keeps a second, base64'd copy of schema metadata
    in the footer (``ARROW:schema``), and an Arrow reader -- ``pq.read_table``,
    pandas, geopandas, the kind of reader most likely to read this value back
    -- resolves metadata from that copy when the file carries one. Merging
    against whichever copy an Arrow reader would see is what keeps both copies
    in agreement once :func:`patch_footer_kv` has updated them (see
    ``core/parquet_footer.py``'s ``_arrow_schema_in_step``).

    The plain entry is read too: a file another tool patched at the Parquet
    level (DuckDB's ``KV_METADATA``, an older gpio) can carry a value there that
    the Arrow copy lacks, and the patch overwrites the plain entry. The two
    copies' ``collections`` are unioned -- the Arrow copy winning an id both
    name -- so neither copy's Collections are lost.
    """
    key = STAC_GEOPARQUET_KEY.encode("utf-8")
    try:
        schema = pq.read_schema(parquet_file)
        plain = pq.read_metadata(parquet_file).metadata or {}
    except Exception as exc:  # pyarrow raises assorted error types here
        raise GeoParquetError(f"Cannot read {parquet_file}'s existing metadata: {exc}") from exc
    arrow_value = _parsed_existing_payload((schema.metadata or {}).get(key), parquet_file)
    plain_value = _parsed_existing_payload(plain.get(key), parquet_file)
    names = list(schema.names)
    if plain_value is None or plain_value == arrow_value:
        return arrow_value, names
    if arrow_value is None:
        return plain_value, names
    union = {**plain_value, **arrow_value}
    union.pop("collection", None)
    union["collections"] = {
        **_carried_collections(plain_value),
        **_carried_collections(arrow_value),
    }
    return union, names


def add_stac_metadata(
    parquet_file: str,
    collection: "dict | str | Path",
    *,
    version: str = DEFAULT_STAC_GEOPARQUET_VERSION,
    output_file: str | None = None,
    overwrite: bool = False,
    verbose: bool = False,
) -> None:
    """Write the ``stac-geoparquet`` key into a Parquet file's footer.

    The payload is built and validated before anything is opened, so a refused
    Collection or version leaves the input exactly as it was. A
    ``stac-geoparquet`` key the input already carries is merged with, not
    replaced by, this call's Collection: see :func:`_merged_payload`.

    Args:
        parquet_file: Path to a local Parquet file. Only read when
            ``output_file`` names somewhere else.
        collection: A STAC Collection, or a path to a JSON file holding one.
        version: The stac-geoparquet specification version to declare.
        output_file: Where to write the result instead of over the input.
        overwrite: Replace ``output_file`` if it already exists.
        verbose: Report the footer sizes and the payload.

    Raises:
        GeoParquetError: If a path is remote, the output exists and
            ``overwrite`` was not given, the Collection or version is refused,
            the input already carries a ``stac-geoparquet`` key that is not
            valid JSON, or the footer cannot be patched.
    """
    payload = stac_geoparquet_metadata(collection, version)
    _serialized(payload)  # refuse a non-JSON Collection before opening anything
    added = next(iter(payload["collections"]))

    _refuse_remote(parquet_file, output_file)
    if not os.path.exists(parquet_file):
        raise GeoParquetError(f"Input file not found: {parquet_file}")
    destination = output_file or parquet_file
    if is_same_file_path(destination, parquet_file):
        # `./a.parquet` for `a.parquet` is the in-place patch, not an output.
        destination = parquet_file
    elif os.path.exists(destination) and not overwrite:
        raise GeoParquetError(
            f"Output file already exists: {destination}\nUse --overwrite to replace it."
        )

    existing, column_names = _existing_payload(parquet_file)
    payload = _merged_payload(existing, payload)
    serialized = _serialized(payload)
    _warn_if_not_items(column_names, parquet_file)

    if verbose:
        debug(f"stac-geoparquet payload:\n{json.dumps(payload, indent=2)}")

    try:
        patch_footer_kv(
            parquet_file,
            {STAC_GEOPARQUET_KEY: serialized},
            output_file=destination,
            verbose=verbose,
        )
    except FooterPatchUnsupported as exc:
        raise GeoParquetError(
            f"Cannot write the stac-geoparquet key into {parquet_file}: {exc}.\n"
            "Rewriting the file to add one footer key would cost its compression "
            "level, so gpio stops here instead."
        ) from exc

    success(_summary(payload, added))
