#!/usr/bin/env python3
"""Write the ``stac-geoparquet`` footer key into a Parquet file.

A stac-geoparquet file is a Parquet file whose rows are STAC Items. The
specification asks it to carry the Collections those Items belong to in the
file's own footer key-value metadata, under the key ``stac-geoparquet``, so a
reader that has the file has the Collection too and does not need a catalog
to interpret it.

The value is a JSON object::

    {"version": "1.1.0", "collections": {"<collection id>": { ...Collection... }}}

``collections`` is a **map keyed by collection id**. The singular ``collection``
key that the 1.0.0 draft allowed is deprecated in 1.1.0, so it is never
written. ``version`` is the version of the *stac-geoparquet* specification, not
of STAC or of GeoParquet, and the specification defines only ``1.0.0`` and
``1.1.0``.

The file path patches the footer in place and copies every page below it
verbatim (``core/parquet_footer.py``, #1141), the same way ``add
bbox-metadata`` does: adding one footer key does not need the data decoded, so
the compression codec *and level*, the encodings, the bloom filters, the page
index, the row-group boundaries and the row order all come out as they went
in, and the work is proportional to the footer rather than to the file.
Unlike ``add bbox-metadata`` there is no DuckDB rewrite behind it -- a footer
that cannot be patched is reported rather than re-encoded, because re-encoding
to add a key that is not the ``geo`` block would cost the file's compression
level to buy nothing.

Note: only local files. A remote URL is refused rather than downloaded.
"""

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.logging_config import debug, success
from geoparquet_io.core.parquet_footer import FooterPatchUnsupported, patch_footer_kv
from geoparquet_io.core.remote import is_remote_url

if TYPE_CHECKING:
    import pyarrow as pa

#: The footer key the stac-geoparquet specification reserves.
STAC_GEOPARQUET_KEY = "stac-geoparquet"

#: The specification versions that define the key's shape. There is no third.
SUPPORTED_STAC_GEOPARQUET_VERSIONS = ("1.0.0", "1.1.0")

DEFAULT_STAC_GEOPARQUET_VERSION = "1.1.0"


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
    except json.JSONDecodeError as exc:
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

    Args:
        table: Input Arrow table.
        collection: A STAC Collection, or a path to a JSON file holding one.
        version: The stac-geoparquet specification version to declare.

    Returns:
        pa.Table: A new table carrying the key. The columns are untouched.

    Raises:
        GeoParquetError: If the version or the Collection is refused.
    """
    payload = stac_geoparquet_metadata(collection, version)
    schema_metadata = dict(table.schema.metadata) if table.schema.metadata else {}
    schema_metadata[STAC_GEOPARQUET_KEY.encode("utf-8")] = json.dumps(payload).encode("utf-8")
    return table.replace_schema_metadata(schema_metadata)


def _refuse_remote(*paths: "str | None") -> None:
    """The footer patch reads and writes a local file; a URL is not one."""
    for path in paths:
        if path is not None and is_remote_url(path):
            raise GeoParquetError(
                f"Remote URLs are not supported for footer metadata: {path}\n"
                "Download the file locally, add the metadata, then upload it."
            )


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
    Collection or version leaves the input exactly as it was.

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
            or the footer cannot be patched.
    """
    payload = stac_geoparquet_metadata(collection, version)

    _refuse_remote(parquet_file, output_file)
    destination = output_file or parquet_file
    if destination != parquet_file and os.path.exists(destination) and not overwrite:
        raise GeoParquetError(
            f"Output file already exists: {destination}\nUse --overwrite to replace it."
        )

    if verbose:
        debug(f"stac-geoparquet payload:\n{json.dumps(payload, indent=2)}")

    try:
        patch_footer_kv(
            parquet_file,
            {STAC_GEOPARQUET_KEY: json.dumps(payload)},
            output_file=destination,
            verbose=verbose,
        )
    except FooterPatchUnsupported as exc:
        raise GeoParquetError(
            f"Cannot write the stac-geoparquet key into {parquet_file}: {exc}.\n"
            "Rewriting the file to add one footer key would cost its compression "
            "level, so gpio stops here instead."
        ) from exc

    named = ", ".join(payload["collections"])
    success(f"Added stac-geoparquet {payload['version']} metadata for collection '{named}'")
