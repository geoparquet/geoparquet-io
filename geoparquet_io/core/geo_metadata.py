"""
GeoParquet metadata handling functions.

This module provides functions for parsing, creating, and applying GeoParquet
metadata to Arrow tables and Parquet files. It handles both GeoParquet 1.x and
2.0 formats, including bbox covering metadata and native geometry types.

Usage in core modules:
    from geoparquet_io.core.geo_metadata import (
        read_raw_geo,           # what a file literally holds (check, inspect)
        sanitized_carried_geo,  # what a write/transform path may carry
        compose_geo_block,      # the only geo block composer
    )

Those two are the only ``geo`` decoders (#1367 concept 3): nothing outside this
module calls ``json.loads`` on a ``geo`` value (``tests/test_geo_block_decoders.py``).

Note: This module uses lazy imports for functions from other core modules
to avoid circular dependencies.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from functools import cache, lru_cache
from typing import TYPE_CHECKING

from geoparquet_io.core.duckdb_utils import (
    _geoarrow_coord_exprs,
    _strip_trailing_order_by,
    describe_query,
    fetch_one,
    query_schema,
    quote_identifier,
    sql_literal,
)
from geoparquet_io.core.edges import (
    EDGES,
    carry_edges,
    declares_edges,
    edges_for_version,
    edges_of,
    implied_orientation,
    strip_edges,
)
from geoparquet_io.core.logging_config import debug, warn

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

# =============================================================================
# GeoParquet Version Configuration
# =============================================================================

GEOPARQUET_VERSIONS = {
    "1.0": {"duckdb_param": "V1", "metadata_version": "1.0.0", "rewrite_metadata": True},
    "1.1": {"duckdb_param": "V1", "metadata_version": "1.1.0", "rewrite_metadata": True},
    "1.1-geoarrow": {"duckdb_param": "V1", "metadata_version": "1.1.0", "rewrite_metadata": True},
    "2.0": {"duckdb_param": "V2", "metadata_version": "2.0.0", "rewrite_metadata": False},
    "parquet-geo-only": {
        "duckdb_param": "NONE",
        "metadata_version": None,
        "rewrite_metadata": False,
    },
}

DEFAULT_GEOPARQUET_VERSION = "1.1"

#: The output versions whose geometry columns are native Parquet GEOMETRY types.
#: In both, the type is where each column's CRS lives (parquet-geo-only has no
#: ``geo`` block at all), so every geometry column is typed, not just the primary.
NATIVE_GEOMETRY_VERSIONS = frozenset({"2.0", "parquet-geo-only"})

#: The spec's single-geometry-type GeoArrow encodings. GeoParquet 1.1 permits
#: these alongside "WKB" ("Supported values: "WKB"; one of "point",
#: "linestring", "polygon", "multipoint", "multilinestring", "multipolygon"");
#: 1.0 and the 2.0 draft are both WKB-only. The spec spells them lowercase and,
#: unlike "WKB"/"wkb", there is no established lenient casing to honour.
GEOARROW_ENCODINGS = (
    "point",
    "linestring",
    "polygon",
    "multipoint",
    "multilinestring",
    "multipolygon",
)


def geoarrow_primary_encoding(geo_meta) -> str | None:
    """The GeoArrow encoding a block declares for its primary column, if any.

    None for a WKB (or unreadable, or absent) block. What it answers is "is this
    file's geometry *natively* encoded", which the version alone cannot say: a
    native file declares version 1.1.0 exactly as a WKB one does, so auto mode
    resolved it to plain "1.1" and the write relabelled nested-list geometry as
    ``encoding: WKB`` -- metadata contradicting the schema beside it (#1176).
    """
    if not isinstance(geo_meta, dict):
        return None
    columns = geo_meta.get("columns")
    if not isinstance(columns, dict):
        return None
    primary = geo_meta.get("primary_column")
    entry = columns.get(primary) if isinstance(primary, str) else None
    if not isinstance(entry, dict):
        return None
    encoding = entry.get("encoding")
    return encoding if encoding in GEOARROW_ENCODINGS else None


# =============================================================================
# Levelled overview files (tylertoo OVERVIEWS_SPEC)
# =============================================================================

#: Footer key marking a levelled overview GeoParquet: every pyramid level
#: stacked as rows, distinguished by an INT32 ``level`` column, with this key
#: describing the row-group layout. The spec (OVERVIEWS_SPEC 3.1) requires the
#: key to stay one named constant; this module owns it because the key sits
#: beside ``geo`` in the same footer and every consumer -- the writer in
#: ``process/overview``, the ``pmtiles`` chunker that must refuse such a file,
#: the rewrite paths that must not carry a stale copy -- reads it from here.
OVERVIEWS_KEY = "geo:overviews"
OVERVIEW_LEVEL_COLUMN = "level"


def is_levelled_overview(schema_or_metadata) -> bool:
    """True when the footer carries the overviews key.

    Takes a ``pyarrow.Schema`` or a KV-metadata mapping (PyArrow hands back
    ``bytes`` keys, DuckDB ``str`` ones; both are accepted). Keyed on the
    footer alone, not on a ``level`` column: buildings carry a ``level``
    (storeys), flood models a water level. The key is the spec's marker; a
    bare column is a coincidence.
    """
    metadata = getattr(schema_or_metadata, "metadata", schema_or_metadata) or {}
    return OVERVIEWS_KEY in metadata or OVERVIEWS_KEY.encode("utf-8") in metadata


# =============================================================================
# Geometry Type Mappings
# =============================================================================

#: The one geometry-type table: WKB base type code -> GeoParquet spec name.
#: Code 0 ("Geometry", type unknown) names no spec type and has no entry, so
#: every reader drops it rather than inventing an "Unknown" type.
_GEOMETRY_TYPE_CODES = {
    1: "Point",
    2: "LineString",
    3: "Polygon",
    4: "MultiPoint",
    5: "MultiLineString",
    6: "MultiPolygon",
    7: "GeometryCollection",
}

# Dimensional suffixes based on WKB type code modifier
_DIMENSION_SUFFIXES = {
    0: "",  # 2D (no suffix)
    1: " Z",  # Z dimension (codes 1001-1007)
    2: " M",  # M dimension (codes 2001-2007)
    3: " ZM",  # ZM dimensions (codes 3001-3007)
}

#: The same names keyed by their upper-case spelling, which is how DuckDB's
#: ``ST_GeometryType`` and the Parquet statistics' strings ("polygon_z") name them.
_SPEC_TYPE_BY_UPPER = {name.upper(): name for name in _GEOMETRY_TYPE_CODES.values()}

# geoarrow's Dimensions enum (XY=1, XYZ=2, XYM=3, XYZM=4) to the WKB type
# code's dimensional modifier above. UNSPECIFIED (-1) and UNKNOWN (0) have no
# entry and fall back to 2D, the only reading that claims nothing extra.
_GEOARROW_DIMENSION_CODES = {1: 0, 2: 1, 3: 2, 4: 3}

#: The same two tables read the other way: a spec suffix -> geoarrow Dimensions.
_GEOARROW_DIMENSION_BY_SUFFIX = {
    _DIMENSION_SUFFIXES[code]: dim for dim, code in _GEOARROW_DIMENSION_CODES.items()
}


def geoarrow_dimension(suffix: str) -> int:
    """The geoarrow ``Dimensions`` value (XY=1, XYZ=2, XYM=3, XYZM=4) of a spec
    dimension suffix as :func:`split_zm_suffix` returns it ('', ' Z', ' M', ' ZM')."""
    return _GEOARROW_DIMENSION_BY_SUFFIX[suffix]


def geoarrow_wkb_codes(types_struct) -> list[int]:
    """WKB type codes for a geoarrow ``unique_geometry_types`` result.

    geoarrow reports the base type and the dimensions in two *separate* struct
    fields, so reading only ``geometry_type`` spells ``Polygon M`` data as
    ``Polygon`` — dropping a suffix the spec makes part of the type name, and
    which every other gpio write path emits (#892). Recombining the two fields
    yields the code :func:`_get_geometry_type_name` already understands.
    """
    bases = types_struct.field("geometry_type").to_pylist()
    dims = types_struct.field("dimensions").to_pylist()
    return [
        1000 * _GEOARROW_DIMENSION_CODES.get(dim, 0) + base
        for base, dim in zip(bases, dims, strict=True)
    ]


# =============================================================================
# Carried-block shape check
# =============================================================================

#: Python type -> the JSON type name a user would recognize in their own file.
_JSON_TYPE_NAMES = {
    type(None): "null",
    bool: "boolean",
    int: "number",
    float: "number",
    str: "string",
    list: "array",
    tuple: "array",
    dict: "object",
}


def _json_type_name(value) -> str:
    """Name ``value``'s JSON type, so a warning can say what the file actually holds."""
    return _JSON_TYPE_NAMES.get(type(value), type(value).__name__)


@lru_cache(maxsize=256)
def _emit_malformed_geo_warning(detail: str, source: str | None = None) -> None:
    """Emit the malformed-block warning once per ``(detail, source)`` (LRU-bounded).

    ``source`` names the file the block came from where the caller knows it. It
    is part of the dedup key as well as the message: without it two different
    malformed inputs in one process (a Python API loop, a shell ``for``) share
    one cache entry and the second file is ignored in silence.
    """
    warn(f"Ignoring malformed 'geo' metadata on {source or 'the input'}: {detail}")


def reset_malformed_geo_warnings() -> None:
    """Clear the malformed-``geo`` warn-once cache. Intended for tests."""
    _emit_malformed_geo_warning.cache_clear()


def carried_column_name(
    value, key: str = "primary_column", source: str | None = None
) -> str | None:
    """A carried geometry-column *name*, or ``None`` when the block does not give one.

    A column name is a string. A carried ``primary_column: 123`` handed on
    reaches ``quote_identifier`` and fails there with a bare ``TypeError``
    several frames later (#887), so the readers that must keep seeing the file
    as it really is -- the column-name readers are shared with ``gpio check``
    and so guard rather than sanitize -- come through here instead of repeating
    the check, and their caller falls through to schema detection.

    An ignored key gets named in a warning, the rule #883 established: without
    it ``gpio sort hilbert``, ``gpio check spatial`` and ``gpio add bbox`` all
    exit 0 with nothing said, while ``gpio convert geoparquet`` on the same file
    warns. An *absent* key is not malformed and says nothing.
    """
    if isinstance(value, str):
        return value or None
    if value is None:
        return None
    _emit_malformed_geo_warning(
        f"'{key}' is {_article(_json_type_name(value))}, expected a string", source
    )
    return None


def carried_version(value, source: str | None = None) -> str | None:
    """A carried ``geo.version`` *string*, or ``None`` when the block does not give one.

    Every reader of this key parses it -- ``version.startswith("2.")``,
    ``version.split(".")`` -- and each of them guarded truthiness only, so a
    ``version`` that is ``2``, ``2.0``, ``["2.0"]``, ``{"major": 2}`` or ``true``
    walked past the guard and raised ``AttributeError`` several frames from
    anything a user can act on (#979). In ``streaming`` the ``except`` beside
    the call named ``json.JSONDecodeError`` and ``UnicodeDecodeError``
    specifically, so it did not even catch it.

    Version readers are shared between the write paths and ``gpio check``, so
    this takes the same line as :func:`carried_column_name`: guard rather than
    sanitize, hand back nothing rather than a value that cannot be a version,
    name the ignored key once, and let each caller fall through to the default
    it already has for a file that declares no version. Degrading to "unknown
    version" is the whole point -- ``gpio check`` is the command a user runs to
    be *told* their file is malformed, so crashing on the malformed input is the
    one thing it must not do.

    An *absent* version is not malformed and says nothing; an empty string is
    not a version either, and ``validate`` already FAILs it as one.
    """
    if isinstance(value, str):
        return value or None
    if value is None:
        return None
    _emit_malformed_geo_warning(
        f"'version' is {_article(_json_type_name(value))}, expected a string", source
    )
    return None


def declared_version(
    geo_meta: Mapping | None, default: str | None = "", source: str | None = None
) -> str | None:
    """The ``version`` string a decoded ``geo`` block declares (*default* when it gives none).

    The one reader of an *input's* declared version -- ``gpio inspect``
    printing it, ``Table.geoparquet_version``, ``add bbox-metadata`` refusing a
    1.0 file, ``add bbox``'s advice, ``file_type``'s ``geo_version`` and ``check
    spec``'s outdated-version issue; never for choosing the output's, which is
    ``parquet_writer.resolve_output_geoparquet_version``. A version that is not
    a string is no version (:func:`carried_version` names it once, against
    ``source`` when given, #979), so every reader of a declared version gets the
    same answer for the same block.
    """
    if not isinstance(geo_meta, Mapping):
        return default
    return carried_version(geo_meta.get("version"), source=source) or default


def _decode_geo_json(raw) -> tuple[object, str | None, str]:
    """Decode a raw ``geo`` value: ``(value, None, "")``, or ``(None, why, detail)``.

    The one ``json.loads`` of a ``geo`` value in the package; both decoders,
    :func:`read_raw_geo` and :func:`sanitized_carried_geo`, come through it, so
    they cannot disagree about which bytes are decodable. Bytes are UTF-8;
    a ``str`` is JSON text; anything else (a dict handed over already decoded)
    passes through untouched. ``why`` is the phrase the warning uses, ``detail``
    the decoder's own message. Never raises, never warns.
    """
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if isinstance(raw, str):
            return json.loads(raw), None, ""
    except UnicodeDecodeError as exc:
        return None, "the value is not valid UTF-8", str(exc)
    except json.JSONDecodeError as exc:
        return None, "the value is not valid JSON", str(exc)
    return raw, None, ""


def _decode_carried_geo(raw, source: str | None = None):
    """Decode a raw carried ``geo`` value (bytes or str) to JSON, or None.

    The value-level first step of :func:`sanitized_carried_geo`, for a write
    path that holds the value rather than the metadata mapping. The decode step
    can fail one call before :func:`sanitize_geo_metadata` ever sees a shape:
    bytes that are not UTF-8 crash ``.decode`` and a truncated payload crashes
    ``json.loads``. Both get the malformed-block treatment -- the value is
    dropped so fresh metadata gets built, and one warning names the cause and,
    when the caller knows it, the file (#957) -- instead of aborting the write
    with a raw decoder traceback (approved change 6 of #1367).

    Already-decoded values (a dict handed over directly) pass through untouched.
    """
    value, problem, _detail = _decode_geo_json(raw)
    if problem is not None:
        _emit_malformed_geo_warning(problem, source)
    return value


def _is_json_number(value) -> bool:
    """A JSON number: int or float, but not the bool subtype of int."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


#: Carried per-column keys that write paths pass through to the output
#: verbatim, with the values they may hold. A well-shaped entry can still
#: poison the output -- ``"crs": 42`` survives a shape-only check and the
#: written file is then refused by DuckDB ("has invalid CRS") and by
#: ``gpio check spec``. Each key maps to (predicate, expected-phrase); a value
#: the predicate rejects is dropped with a warning naming both. ``crs: null``
#: stays: JSON null is the spec's spelling of "CRS is unknown".
_COLUMN_VALUE_CHECKS: dict[str, tuple] = {
    "crs": (lambda v: v is None or isinstance(v, (dict, str)), "an object, a string or null"),
    "encoding": (lambda v: isinstance(v, str), "a string"),
    "epoch": (_is_json_number, "a number"),
    "orientation": (lambda v: isinstance(v, str), "a string"),
    "edges": (lambda v: isinstance(v, str), "a string"),
    "covering": (lambda v: isinstance(v, dict), "an object"),
    "geometry_types": (
        lambda v: isinstance(v, list) and all(isinstance(t, str) for t in v),
        "an array of strings",
    ),
    "bbox": (
        lambda v: isinstance(v, list) and len(v) in (4, 6) and all(_is_json_number(x) for x in v),
        "an array of 4 or 6 numbers",
    ),
}


def _empty_crs_as_null(entry: dict) -> dict:
    """``entry`` with an empty ``crs`` (``""``, ``{}``, ``[]``, ``0``) read as null.

    An empty value names no CRS, so it is unknown, the same as ``null``. Dropping
    it, as a wrong-typed value is dropped, would make it *absent*, which the spec
    reads as OGC:CRS84, and a projected file's metres would then be keyed as
    degrees (#1267). ``null`` is also what a write path may safely carry, where
    an empty value would make the output unreadable. Warns once, naming the value.
    """
    from geoparquet_io.core.crs_utils import is_empty_crs_value, warn_empty_crs_once

    if "crs" not in entry or not is_empty_crs_value(entry["crs"]):
        return entry
    warn_empty_crs_once(entry["crs"])
    return {**entry, "crs": None}


def _sanitize_column_entry(name: str, entry: dict) -> tuple[dict, list[str]]:
    """Drop wrong-typed carried values from one column entry; never mutates.

    Returns ``entry`` itself when every value is acceptable, else a cleaned
    shallow copy, plus the problems to warn about.
    """
    problems: list[str] = []
    cleaned = _empty_crs_as_null(entry)
    for key, (accepts, expected) in _COLUMN_VALUE_CHECKS.items():
        if key in cleaned and not accepts(cleaned[key]):
            problems.append(
                f"'{key}' on column '{name}' is "
                f"{_article(_json_type_name(cleaned[key]))}, expected {expected}"
            )
            if cleaned is entry:
                cleaned = dict(entry)
            cleaned.pop(key)
    return cleaned, problems


def _sanitize_columns(columns: dict) -> tuple[dict, list[str]]:
    """Sanitize a ``columns`` mapping known to be a dict; never mutates.

    Returns ``columns`` itself when nothing needed dropping, else a cleaned
    copy, plus the problems to warn about.
    """
    problems: list[str] = []
    kept: dict = {}
    dropped: list[str] = []
    changed = False
    for name, entry in columns.items():
        if not (isinstance(name, str) and isinstance(entry, dict)):
            dropped.append(str(name))
            changed = True
            continue
        entry_clean, entry_problems = _sanitize_column_entry(name, entry)
        changed = changed or entry_clean is not entry
        problems.extend(entry_problems)
        kept[name] = entry_clean
    if dropped:
        problems.append(
            f"'columns' entries {', '.join(repr(n) for n in sorted(dropped))} "
            "are not objects, expected an object per column"
        )
    return (kept if changed else columns), problems


def sanitize_geo_metadata(geo_meta, source: str | None = None):
    """Drop the parts of a carried ``geo`` block that readers cannot accept.

    The ``geo`` key on an input file is arbitrary JSON written by some other
    tool, but every writer here reads it as ``geo["columns"][name][key]``. A
    block whose ``columns`` is null, an array, a string, or a mapping to
    non-objects used to abort the write with a bare ``TypeError`` raised three
    frames deep, in the middle of building the output metadata (#771). And a
    well-shaped entry can still carry a wrong-typed value (``"crs": 42``) that
    makes the *output* unreadable, so the carried values write paths pass
    through verbatim are type-checked too (:data:`_COLUMN_VALUE_CHECKS`).

    A malformed block is a property of the *input*, not a caller error, so it is
    treated the way an absent one is: the malformed parts are dropped so fresh
    metadata gets built from the table, and one warning names what was wrong.

    This is the single shape check every reader of the raw block that *assumes a
    dict* goes through: the write paths, and the ``inspect`` displays
    (``inspect_utils.extract_geo_info`` / ``get_preview_data``,
    ``metadata_utils``). The read-only readers (:func:`read_raw_geo` and
    :func:`raw_geo_of_file`, its file-level form) deliberately do *not*
    sanitize: ``gpio check`` has to see the file as it really is.

    Args:
        geo_meta: A decoded ``geo`` block, or None.
        source: The file the block came from, when the caller knows it. It goes
            into every warning and its warn-once key, so a loop over several
            malformed files names each of them (#957).

    Returns:
        ``geo_meta`` itself when it is already well-shaped, a repaired shallow
        copy when it is not, or None when nothing usable is left.
    """
    if geo_meta is None:
        return None

    if not isinstance(geo_meta, dict):
        _emit_malformed_geo_warning(
            f"the block is {_article(_json_type_name(geo_meta))}, expected an object", source
        )
        return None

    problems: list[str] = []
    cleaned = geo_meta

    primary = geo_meta.get("primary_column")
    # An empty string is a string, but it is not a column name -- and it lands in
    # the same place a non-string does: `"" not in col_entries`, so pruning drops
    # the whole block and the CRS with it. `carried_column_name` and the spec
    # check in `validate` both already reject it; this is the third reader and
    # has to agree, or the write paths corrupt what the check correctly flags.
    dropped_primary = "primary_column" in geo_meta and not (isinstance(primary, str) and primary)
    if dropped_primary:
        problems.append(
            "'primary_column' is an empty string, expected a column name"
            if isinstance(primary, str)
            else f"'primary_column' is {_article(_json_type_name(primary))}, expected a string"
        )
        cleaned = dict(cleaned)
        cleaned.pop("primary_column")

    if "columns" in geo_meta:
        columns = geo_meta["columns"]
        if not isinstance(columns, dict):
            problems.append(
                f"'columns' is {_article(_json_type_name(columns))}, "
                "expected an object keyed by column name"
            )
            cleaned = dict(cleaned)
            cleaned.pop("columns")
        else:
            kept, column_problems = _sanitize_columns(columns)
            problems.extend(column_problems)
            if kept is not columns:
                cleaned = dict(cleaned)
                cleaned["columns"] = kept

    if dropped_primary:
        _repair_primary_column(cleaned)

    for problem in problems:
        _emit_malformed_geo_warning(problem, source)
    return cleaned


def _repair_primary_column(cleaned: dict) -> None:
    """Name the primary column again when exactly one candidate survives sanitizing.

    Dropping a malformed ``primary_column`` and stopping there is not a safe
    recovery: the readers downstream then fall back to the literal string
    ``"geometry"``, which *misses* a file whose column is called ``geom`` (or
    ``wkb_geometry``, ``shape``, ``the_geom``) and reports its CRS as absent.
    That is worse than the crash it replaced -- ``convert reproject`` bypasses
    the explicit-null-CRS guard and a projected file is aggregated as lon/lat,
    silently, where the raw block used to raise (#887 review).

    One surviving column is an unambiguous primary: the spec requires
    ``primary_column`` to name a key of ``columns``, and there is only one. Two
    or more is a guess, so the block is left without one and the callers ask the
    file's schema instead.
    """
    columns = cleaned.get("columns")
    if isinstance(columns, dict) and len(columns) == 1:
        cleaned["primary_column"] = next(iter(columns))


def _geo_key_value(metadata):
    """``(True, value)`` for the ``geo`` key of a metadata mapping, else ``(False, None)``.

    Both key spellings are accepted: PyArrow hands back ``bytes`` keys and
    DuckDB's KV metadata ``str`` ones, and several callers see both. ``b"geo"``
    wins when both are present, as it did for every reader this replaced.
    """
    if not metadata:
        return False, None
    for key in (b"geo", "geo"):
        if key in metadata:
            return True, metadata[key]
    return False, None


def sanitized_carried_geo(metadata, source: str | None = None) -> dict:
    """The ``geo`` block on a Parquet/Arrow metadata mapping, sanitized, or ``{}``.

    The sanitized carried decoder: one of the two ``geo`` decoders (the other is
    :func:`read_raw_geo`), and the one every write and transform path reads a
    carried block through. The three steps every such reader used to repeat --
    find the key, decode it, check its shape -- in one place, so a reader is one
    call away from the rule rather than three lines of its own (#947). Bytes or
    str keys; bad UTF-8 or bad JSON warns once and reads as absent (approved
    change 6 of #1367); a non-object block, a non-object ``columns`` and
    non-object entries are sanitized by :func:`sanitize_geo_metadata`.

    ``source`` names the file for the warning (#957).

    Returns ``{}``, never ``None``, so callers can index the result directly;
    a block that sanitizing leaves unusable is indistinguishable from an absent
    one, which is exactly how #883 decided a malformed block should read.
    """
    present, raw = _geo_key_value(metadata)
    if not present:
        return {}
    cleaned = sanitize_geo_metadata(_decode_carried_geo(raw, source), source)
    return cleaned if isinstance(cleaned, dict) else {}


def sanitized_carried_geo_of_file(parquet_file, con=None) -> dict:
    """:func:`sanitized_carried_geo` of a Parquet file's footer, warning with its path.

    The file-level form of the sanitized carried decoder, for the write paths
    that start from a path: the footer's KV metadata (PyArrow locally, DuckDB
    remotely or on ``con``) decoded and sanitized in one call, so a truncated
    or non-UTF-8 ``geo`` value warns and reads as absent here too (approved
    change 6 of #1367) and the warning names the file (#957). ``parquet_file``
    is a RAW path/URL (#718).
    """
    from geoparquet_io.core.duckdb_metadata import get_kv_metadata

    return sanitized_carried_geo(get_kv_metadata(str(parquet_file), con), source=str(parquet_file))


def raw_geo_of_file(parquet_file, con=None) -> RawGeo:
    """:func:`read_raw_geo` of a Parquet file's footer, for the check paths.

    The file-level form of the raw diagnostic decoder: what the file's ``geo``
    key literally holds, never sanitized and never warned about. A check that
    wants the object uses ``.block`` (a dict, or None for an absent, undecodable
    or non-object value). ``parquet_file`` is a RAW path/URL (#718).
    """
    from geoparquet_io.core.duckdb_metadata import get_kv_metadata

    return read_raw_geo(get_kv_metadata(str(parquet_file), con))


@dataclass(frozen=True)
class RawGeo:
    """What a ``geo`` key literally holds, as :func:`read_raw_geo` reports it.

    ``present`` is whether the key exists at all. ``value`` is the decoded JSON
    exactly as the file holds it -- any JSON type, never sanitized -- or ``None``
    when the key is absent or undecodable. ``error`` says why an undecodable
    value could not be decoded ("the value is not valid UTF-8: ...", "the value
    is not valid JSON: ..."), and is ``None`` otherwise.
    """

    present: bool
    value: object = None
    error: str | None = None

    @property
    def block(self) -> dict | None:
        """``value`` when it is a JSON object, else ``None``."""
        return self.value if isinstance(self.value, dict) else None


def read_raw_geo(metadata) -> RawGeo:
    """The raw diagnostic ``geo`` decoder: what a metadata mapping's ``geo`` key holds.

    One of the two ``geo`` decoders (the other is :func:`sanitized_carried_geo`),
    for the readers that must report what is literally there -- the ``gpio
    check`` paths and the raw ``Table.metadata`` view -- and so neither sanitize
    nor warn (:func:`raw_geo_of_file` is its file-level form). Bytes
    or str keys. Never raises: an undecodable value (bad UTF-8, bad JSON) comes
    back with ``error`` set instead of a decoder traceback.

    A write path must not read a carried *input* block's contents through it --
    it would carry a malformed block forward; that is
    :func:`sanitized_carried_geo`'s job. Two uses are allowed there: asking only
    whether a ``geo`` key is present (``.present``, as
    ``write_strategies.base.needs_metadata_rewrite`` does), and re-reading the
    block gpio itself just wrote (the footer patch in ``write_funnels``).
    """
    present, raw = _geo_key_value(metadata)
    if not present:
        return RawGeo(present=False)
    value, problem, detail = _decode_geo_json(raw)
    return RawGeo(present=True, value=value, error=problem and f"{problem}: {detail}")


def _article(type_name: str) -> str:
    """``"a list"`` / ``"an object"`` -- correct article for a JSON type name."""
    if type_name == "null":
        return "null"
    return f"{'an' if type_name[0] in 'aeiou' else 'a'} {type_name}"


# =============================================================================
# Metadata Parsing Functions
# =============================================================================


def is_geoparquet_1x(geo_version: object) -> bool:
    """Whether ``geo_version`` is a GeoParquet 1.x version: the edges gate.

    The one parse of "is this 1.x": a string whose first dotted part is ``1``.
    The write facade's ``parquet_writer.writable_geoparquet_version`` branches
    on it, and ``edges.edges_for_version`` gets it on every path. A missing or
    unparsable version is not 1.x, so it gates nothing (#1367 concept 6).
    """
    return isinstance(geo_version, str) and geo_version.startswith("1.")


def covering_supported(version: str | None) -> bool:
    """Whether a GeoParquet version may carry the ``covering`` column key.

    ``covering`` was introduced in GeoParquet 1.1 — the word does not appear
    anywhere in the v1.0.0 specification — so 1.0 output must omit it. The bbox
    *column* itself is an ordinary Parquet column and stays legal at 1.0; only
    the metadata key is gated.

    Accepts both the short option form ("1.0", "1.1") and the metadata form
    ("1.0.0", "1.1.0"). Unknown/absent versions are treated as supporting it.
    """
    return not str(version or "").startswith("1.0")


def strip_unsupported_covering(geo_meta: dict, version: str | None, verbose: bool = False) -> dict:
    """Return ``geo_meta`` without ``covering`` on any column when ``version`` predates 1.1.

    Single gate shared by every write path, applied after metadata assembly so it
    also catches coverings carried in from a 1.1 source file or supplied through
    ``custom_metadata`` (h3/s2/a5/quadkey).

    Never mutates its input: partition loops reuse one ``original_metadata``
    dict across many writes, so popping in place would strip the shared dict
    permanently and silently cost a later 1.1 write its covering.
    """
    if covering_supported(version):
        return geo_meta
    return _strip_covering_where(
        geo_meta,
        lambda col_meta: "covering" in col_meta,
        f"1.1-only covering metadata (version {version})",
        verbose,
    )


def _strip_covering_where(geo_meta: dict, drop, reason: str, verbose: bool) -> dict:
    """``geo_meta`` without the ``covering`` of every column dict ``drop`` selects.

    The mechanics both covering gates share. Never mutates its input: only the
    stripped column dicts and the ``columns`` mapping are copied, for the
    aliasing reason :func:`strip_unsupported_covering` gives.
    """
    columns = geo_meta.get("columns")
    if not isinstance(columns, dict):
        return geo_meta
    if not any(isinstance(col, dict) and drop(col) for col in columns.values()):
        return geo_meta

    stripped = {}
    for col_name, col_meta in columns.items():
        if isinstance(col_meta, dict) and drop(col_meta):
            col_meta = {k: v for k, v in col_meta.items() if k != "covering"}
            if verbose:
                debug(f"Dropped {reason} for column '{col_name}'")
        stripped[col_name] = col_meta

    result = dict(geo_meta)
    result["columns"] = stripped
    return result


def covering_lacks_bbox(col_meta: object) -> bool:
    """Is this column's ``covering`` an object with no ``bbox`` member?

    The one definition of the illegal shape, shared by the write gate
    (:func:`strip_bboxless_covering`), the read-side verdict
    (``validate._check_cover_has_bbox``) and the ``check --fix`` repair, so
    what gpio refuses to write, what it fails, and what it drops cannot drift
    apart (#954, #1173).

    A ``covering`` that is not an object at all is somebody else's verdict
    (``covering_is_object``) and not this one: a string satisfies
    ``"bbox" in covering`` character-wise, so judging it here would report the
    wrong defect (#1062).
    """
    if not isinstance(col_meta, dict):
        return False
    covering = col_meta.get("covering")
    return isinstance(covering, dict) and "bbox" not in covering


def bboxless_covering_columns(geo_meta: object) -> list[str]:
    """Names of the columns whose ``covering`` carries no ``bbox`` member.

    Reads a block exactly as a file holds it: a ``geo`` key or a ``columns``
    value that is not an object declares no columns, so it declares no illegal
    covering either.
    """
    if not isinstance(geo_meta, dict):
        return []
    columns = geo_meta.get("columns")
    if not isinstance(columns, dict):
        return []
    return [str(name) for name, col_meta in columns.items() if covering_lacks_bbox(col_meta)]


def strip_bboxless_covering(geo_meta: dict, verbose: bool = False) -> dict:
    """Return ``geo_meta`` without any ``covering`` that has no ``bbox`` member.

    The GeoParquet 1.1.0 spec's ``covering`` section reads: "The keys of the
    'covering' object MUST be a supported encoding. Currently the only
    supported encoding is 'bbox'". gpio additionally records its spatial-index
    entries (h3/s2/a5/quadkey/kdtree) *beside* a bbox member (#694/#738), but a
    covering carrying only those members is one real readers reject: geopandas
    indexes ``covering["bbox"]["xmin"][0]`` unguarded, so ``partition quadkey``
    wrote a dataset ``geopandas.read_parquet`` could not open at all (#954).

    Step 13 of :func:`compose_geo_block`, the last gate: it runs after the
    composer's bbox-covering step (step 9) has had its chance to supply the
    member.

    Never mutates its input, for the same aliasing reason as
    :func:`strip_unsupported_covering`: partition loops reuse one metadata dict
    across many writes.
    """

    return _strip_covering_where(
        geo_meta,
        covering_lacks_bbox,
        "covering metadata with no bbox member (spec allows only the bbox encoding)",
        verbose,
    )


def _with_covering(col_meta: dict, remaining: dict) -> dict:
    """``col_meta`` with its ``covering`` replaced by ``remaining`` where it stood.

    The key keeps its position (a gate removes members, it does not reorder the
    entry), and goes when nothing is left. Never mutates its input.
    """
    return {
        key: (remaining if key == "covering" else value)
        for key, value in col_meta.items()
        if key != "covering" or remaining
    }


def strip_illegal_bbox_covering(geo_meta: dict, schema: pa.Schema) -> dict:
    """Return ``geo_meta`` without any ``covering.bbox`` naming a struct the spec forbids.

    The *drop* half of :func:`bbox_column_to_declare`'s rule 2: step 12 of
    :func:`compose_geo_block`, over the source's output schema. A write that
    may not declare a column must not write the entry the *input* had already
    declared either (#1172).

    Only the ``bbox`` member goes: an ``h3``/``quadkey`` entry beside it is not
    what the validator rejected, and :func:`strip_bboxless_covering` (step 13)
    decides whether what is left may stand alone (#954). A covering naming a
    column the schema does not have is step 11's question
    (:func:`strip_absent_covering`).

    Never mutates its input, for the aliasing reason
    :func:`strip_unsupported_covering` gives.
    """
    columns = geo_meta.get("columns")
    if not isinstance(columns, dict):
        return geo_meta

    stripped: dict = {}
    changed = False
    for col_name, col_meta in columns.items():
        covering = col_meta.get("covering") if isinstance(col_meta, dict) else None
        bbox_column = _covering_column(covering.get("bbox")) if isinstance(covering, dict) else None
        field = _unambiguous_field(schema, bbox_column) if bbox_column is not None else None
        problem = arrow_bbox_covering_problem(bbox_column, field) if field is not None else None
        if problem is None:
            stripped[col_name] = col_meta
            continue
        _note_undeclarable_bbox_column(bbox_column, problem, True)
        stripped[col_name] = _with_covering(
            col_meta, {k: v for k, v in covering.items() if k != "bbox"}
        )
        changed = True

    if not changed:
        return geo_meta
    result = dict(geo_meta)
    result["columns"] = stripped
    return result


def strip_absent_covering(geo_meta: dict, columns: Collection[str]) -> dict:
    """Return ``geo_meta`` without any ``covering`` entry naming a column not in ``columns``.

    The per-entry half of what :func:`prune_geo_metadata_to_columns` does to a
    carried KV block, for a gate handed a ``geo`` block and the output's column
    names instead. ``write_parquet_with_metadata`` prunes the carried block
    against the output's columns before any strategy sees it; the Arrow *table*
    funnel had no equivalent, so ``write_geoparquet_table`` of a table whose own
    block declared a covering over a column the table does not have wrote the
    entry verbatim and ``gpio check spec`` failed the result three ways -- the
    column is absent, so its structure and its field types cannot be read either.

    Only the entries naming an absent column go, for the reason
    :func:`strip_illegal_bbox_covering` gives, and the ``covering`` key itself
    goes when nothing is left. Never mutates its input.
    """
    col_entries = geo_meta.get("columns")
    if not isinstance(col_entries, dict):
        return geo_meta

    present = set(columns)
    stripped: dict = {}
    changed = False
    for col_name, col_meta in col_entries.items():
        covering = col_meta.get("covering") if isinstance(col_meta, dict) else None
        if not isinstance(covering, dict):
            stripped[col_name] = col_meta
            continue
        remaining = {k: v for k, v in covering.items() if _covering_column(v) in present}
        if len(remaining) == len(covering):
            stripped[col_name] = col_meta
            continue
        stripped[col_name] = _with_covering(col_meta, remaining)
        changed = True

    if not changed:
        return geo_meta
    result = dict(geo_meta)
    result["columns"] = stripped
    return result


#: Per-column geo metadata keys that are derived from the data itself and are
#: therefore invalidated by anything that changes which rows/coordinates are
#: written (row filters, reprojection, per-partition splits, multi-file merges).
DERIVED_STAT_KEYS = ("bbox", "geometry_types")

#: Sentinel returned by a rewrite callback to mean "drop the geo key entirely".
_DROP_GEO = object()


def _decode_geo_value(raw):
    """Decode a KV ``geo`` value (bytes/str/dict) to a dict, or ``None``.

    The rewrite helpers below run on carried *input* blocks (a projection's
    prune, the stream's covering gates), so the value goes through the carried
    decoder's value step, :func:`_decode_carried_geo`: bad UTF-8 or bad JSON
    warns once (approved change 6 of #1367). :func:`_rewrite_geo_metadata` then
    leaves a value it cannot decode, or one that is not an object, as it found
    it; the write rebuilds fresh metadata from the output instead.
    """
    value = _decode_carried_geo(raw)
    return value if isinstance(value, dict) else None


def _encode_geo_value(geo_dict: dict, like):
    """Re-encode ``geo_dict`` in the same form (bytes/str/dict) as ``like``."""
    if isinstance(like, bytes):
        return geo_key_value(geo_dict).encode("utf-8")
    if isinstance(like, str):
        return geo_key_value(geo_dict)
    return geo_dict


def _rewrite_geo_metadata(metadata: dict | None, rewrite) -> dict | None:
    """Return a deep copy of KV ``metadata`` with ``geo`` passed through ``rewrite``.

    Handles both the ``"geo"`` and ``b"geo"`` keys, hands ``rewrite`` a mutable
    decoded dict, and re-encodes the result in whichever form the value arrived
    in. A ``rewrite`` returning :data:`_DROP_GEO` removes the key; an
    unparsable value is left untouched. The input is never mutated.
    """
    if not metadata:
        return metadata

    result = copy.deepcopy(metadata)
    for geo_key in ("geo", b"geo"):
        if geo_key not in result:
            continue
        raw = result[geo_key]
        geo_dict = _decode_geo_value(raw)
        if geo_dict is None:
            continue
        rewritten = rewrite(geo_dict)
        if rewritten is _DROP_GEO:
            del result[geo_key]
        else:
            result[geo_key] = _encode_geo_value(rewritten, raw)
    return result


def compose_carried_geo(
    metadata: dict | None, schema: pa.Schema, version: str, verbose: bool = False
) -> dict | None:
    """A Parquet KV ``metadata`` dict with its carried block composed for ``schema``.

    For a path that carries the input's whole block forward instead of
    rebuilding it: the Arrow IPC stream ``gpio ... -`` writes to stdout. A
    stream is read and persisted exactly like the file a gpio write makes --
    ``pq.write_table`` of one is how it becomes that file -- so its block is a
    write block at the output ``version`` the caller resolved: every covering
    gate in the composer's order (``gpio extract geoparquet f.parquet -
    --exclude-cols bbox`` used to stream a covering with only a spatial-index
    entry left, the shape ``geopandas.read_parquet`` cannot open, #954/#1172),
    and a conventional bbox column declared as every file write declares it.

    A block that names no primary column it describes is left as it is.
    Returns a copy; the input is never mutated.
    """

    def _compose(geo: dict) -> dict:
        primary = geo.get("primary_column")
        if not isinstance(primary, str) or primary not in (geo.get("columns") or {}):
            return geo
        return compose_geo_block(
            SchemaSource(schema, {b"geo": json.dumps(geo).encode("utf-8")}),
            GeoBlockOptions(geometry_column=primary, version=version, verbose=verbose),
        )

    return _rewrite_geo_metadata(metadata, _compose)


def strip_derived_stats(
    metadata: dict | None,
    columns: Collection[str] | None = None,
    recomputed_columns: Collection[str] | None = None,
) -> dict | None:
    """Return a copy of Parquet KV ``metadata`` without derived geo stats.

    Drops the per-column ``bbox`` and ``geometry_types`` (see
    :data:`DERIVED_STAT_KEYS`) from the ``geo`` metadata so the write machinery
    recomputes them from the data actually written — or omits them when there is
    nothing to describe (both are optional per spec for an empty result).

    Callers are anything that changes which rows or coordinates land in the
    output: row filters (``extract``), coordinate transforms (``reproject``),
    per-partition splits, and multi-file merges whose carried metadata came from
    only the first input file.

    ``columns`` limits the strip to those column entries, for a caller that
    changes coordinates in some geometry columns but not others: reproject
    transforms only the primary column, so a secondary column's bytes reach the
    output unchanged and its carried stats still describe them (#890). Dropping
    them anyway left the output without the ``geometry_types`` GeoParquet 1.1
    requires — only duckdb-kv recomputes a secondary column's (#952) — and
    DuckDB then refuses to open the file at all. ``None`` strips every column, which is right for a
    merge, whose carried stats UNDER-cover every column of the output.

    ``recomputed_columns`` names the stripped columns the caller's write path
    will fill back in — on every file-write path the primary geometry column
    (duckdb-kv fills a secondary's ``[]`` too, #952). Removing ``geometry_types`` is a way of *asking*
    for a recompute, so for any other stripped column it is not a marker but a
    REQUIRED key silently deleted, and the same unreadable output comes back by
    another route (#934). Those columns therefore keep the key with the spec's
    "not known" value, ``[]``, and lose only ``bbox``, which is optional and
    whose absence already means "not known". ``None`` (the default) means the
    caller recomputes everything it strips.

    Both ``"geo"`` and ``b"geo"`` keys are handled, and the value is returned in
    the same form (``bytes``/``str``/``dict``) it arrived in. The input is never
    mutated; unparsable ``geo`` values are passed through untouched.
    """

    def _drop(geo_dict: dict) -> dict:
        for col_name, col_meta in (geo_dict.get("columns") or {}).items():
            if columns is not None and col_name not in columns:
                continue
            if not isinstance(col_meta, dict):
                continue
            for key in DERIVED_STAT_KEYS:
                col_meta.pop(key, None)
            if recomputed_columns is not None and col_name not in recomputed_columns:
                col_meta["geometry_types"] = []
        return geo_dict

    return _rewrite_geo_metadata(metadata, _drop)


def strip_orientation(metadata: dict | None, column: str) -> dict | None:
    """Return a copy of KV ``metadata`` without ``column``'s ``orientation``.

    A geometry repair (``ST_MakeValid``) can rewind rings — the repaired bowtie
    of issue #812 comes back with clockwise exterior rings — so a carried
    ``orientation: "counterclockwise"`` declaration no longer describes the
    rows. Unlike :data:`DERIVED_STAT_KEYS`, which the write path recomputes,
    ``orientation`` is only removed: gpio does not re-orient rings, and the key
    is optional per spec, so absence is the honest value after a rewrite.

    Only ``column``'s entry is touched — a repair runs on one geometry column,
    and any other column's declaration still holds. Both ``"geo"`` and
    ``b"geo"`` keys are handled; the input is never mutated.
    """

    def _drop(geo_dict: dict) -> dict:
        col_meta = (geo_dict.get("columns") or {}).get(column)
        if isinstance(col_meta, dict):
            col_meta.pop("orientation", None)
        return geo_dict

    return _rewrite_geo_metadata(metadata, _drop)


def strip_nonplanar_edges(
    metadata: dict | None, columns: Collection[str] | None = None
) -> dict | None:
    """Return a copy of KV ``metadata`` without non-planar ``edges`` (#601).

    Reprojection transforms vertices, not edges: gpio does not densify along
    great circles, so once the destination CRS is projected the output's edges
    really are straight lines and ``planar`` — the spec default, expressed by
    the key's absence — is the truthful description. Like ``orientation``, the
    key is only removed, never rewritten to something gpio cannot vouch for.

    ``columns`` limits the strip to those column entries; reproject transforms
    only the primary geometry column, so a secondary column — still geographic,
    still great-circle — keeps its declaration. ``None`` strips every column.

    Both ``"geo"`` and ``b"geo"`` keys are handled; the input is never mutated.
    """

    return _rewrite_geo_metadata(metadata, lambda geo: strip_edges(geo, columns))


def backfill_derived_stats(
    metadata: dict | None,
    table: pa.Table,
    verbose: bool = False,
) -> dict | None:
    """Return a copy of KV ``metadata`` with missing derived geo stats computed.

    The counterpart to :func:`strip_derived_stats`: the strip invalidates stats a
    filter or transform made stale, and the write path is then responsible for
    recomputing them from the rows actually written. Every column entry in the
    ``geo`` metadata that names a column of ``table`` and lacks ``geometry_types``
    or ``bbox`` gets it computed from that column's data.

    ``geometry_types`` is REQUIRED by GeoParquet 1.1, and DuckDB refuses to open a
    Parquet file whose ``geo`` metadata omits it, so a stream written without it
    is unreadable by the next stage of a pipe (issue #722). ``bbox`` is optional
    and stays absent when the data cannot supply one (an empty result).

    A *known* value already present is left alone — this fills gaps, it does not
    audit. An empty ``geometry_types`` is a gap, not a value: ``[]`` is how the
    spec spells "not known", and it is what the strip leaves on a column a
    file-write path would not have recomputed (#934). This path holds the rows,
    so it can answer the question the sentinel leaves open.

    The input is never mutated; unparsable ``geo`` values pass through untouched.
    """

    def _fill(geo_dict: dict) -> dict:
        for name, col_meta in (geo_dict.get("columns") or {}).items():
            if not isinstance(col_meta, dict) or name not in table.column_names:
                continue
            if not col_meta.get("geometry_types"):
                col_meta["geometry_types"] = geometry_types_of(table, name, verbose=verbose)
            if "bbox" not in col_meta:
                bbox = bounds_of(table, name, verbose=verbose)
                if bbox:
                    col_meta["bbox"] = bbox
        return geo_dict

    return _rewrite_geo_metadata(metadata, _fill)


def is_covering_path(path: object) -> bool:
    """A ``[column, field]`` covering path: two strings, in a list.

    The one shape test for a covering path. Length alone is not shape: a
    string of two characters and a two-key object both have ``len()`` 2, and
    a string indexes character-wise, so ``"bbox.xmin"[0]`` once named a column
    ``"b"`` the file never mentions (#1062).
    """
    return isinstance(path, list) and len(path) == 2 and all(isinstance(p, str) for p in path)


def _covering_column(covering_entry) -> str | None:
    """Return the data column a single ``covering`` entry points at, if any."""
    if not isinstance(covering_entry, dict):
        return None
    # Spatial-index coverings (h3/s2/a5/quadkey): {"column": name, ...}
    column = covering_entry.get("column")
    if isinstance(column, str):
        return column
    # bbox covering: {"xmin": [column, "xmin"], ...}
    for ref in covering_entry.values():
        if isinstance(ref, (list, tuple)) and ref and isinstance(ref[0], str):
            return ref[0]
    return None


def _prune_geo_dict_to_columns(geo_dict: dict, columns: set[str], repoint_primary: bool = False):
    """Drop column entries and coverings that reference absent columns.

    Returns :data:`_DROP_GEO` when no declared geometry column survives, meaning
    the file must not advertise ``geo`` metadata at all. When the primary column
    itself is gone but another declared geometry column remains,
    ``repoint_primary`` decides between naming that survivor as the new primary
    and dropping the whole block.

    This is a write path -- whatever survives is re-encoded into the output's
    ``geo`` key -- so the carried block goes through the shared shape check
    first. It used to compare the *raw* ``primary_column`` against the surviving
    keys, and ``123`` is neither ``None`` nor a key, so the whole block was
    dropped: the file lost its ``crs`` too, and an absent ``crs`` is spec-defined
    as OGC:CRS84, silently relabelling a projected file lon/lat (#968).
    """
    # Sanitizing drops a non-string `primary_column` and repairs it from a lone
    # surviving column, which is the recovery this function needs and #883/#945
    # already shared out. `_rewrite_geo_metadata` hands over the decoded object,
    # so the sanitized carried decoder takes it as the value of a `geo` key; the
    # raw key is read first because sanitizing drops a malformed one.
    declared_primary = "primary_column" in geo_dict
    geo_dict = sanitized_carried_geo({b"geo": geo_dict})

    col_entries = geo_dict.get("columns")
    if not isinstance(col_entries, dict):
        return geo_dict

    for name in [n for n in col_entries if n not in columns]:
        del col_entries[name]

    primary = geo_dict.get("primary_column")
    if primary is None and declared_primary:
        # Sanitizing dropped a malformed `primary_column` and could not repair
        # it, because `columns` held more than one entry at the time. Pruning
        # may just have left exactly one, which is an unambiguous primary.
        _repair_primary_column(geo_dict)
        primary = geo_dict.get("primary_column")

    if primary is not None and primary not in col_entries:
        if not (repoint_primary and col_entries):
            return _DROP_GEO
        geo_dict["primary_column"] = next(iter(col_entries))

    return strip_absent_covering(geo_dict, columns)


def prune_geo_metadata_to_columns(
    metadata: dict | None, columns: list[str], repoint_primary: bool = False
) -> dict | None:
    """Return a copy of KV ``metadata`` with references to absent columns removed.

    A column projection (``gpio extract --exclude-cols``) can remove the bbox
    column a ``covering`` points at, or a secondary geometry column, leaving geo
    metadata that references a schema root that no longer exists — which readers
    and ``gpio check spec`` both reject. Entries for columns missing from
    ``columns`` are dropped; if the primary geometry column is among them the
    whole ``geo`` key is dropped, since the output is no longer GeoParquet.

    ``repoint_primary`` keeps the block alive in the one case where the output
    is still GeoParquet: the primary column was dropped but another *declared*
    geometry column survives, which then becomes the primary. Callers that
    rebuild the geo metadata from the output schema anyway (the DuckDB write
    path) leave it off; callers that carry the input's block forward verbatim
    (``extract_table``, on Arrow tables) turn it on.
    """
    present = set(columns)
    return _rewrite_geo_metadata(
        metadata, lambda geo: _prune_geo_dict_to_columns(geo, present, repoint_primary)
    )


@dataclass(frozen=True)
class ArrowTableSource:
    """:func:`compose_geo_block` source: an Arrow table (its schema and metadata)."""

    table: pa.Table


@dataclass(frozen=True)
class SchemaSource:
    """:func:`compose_geo_block` source: an output schema plus the carried KV metadata."""

    schema: pa.Schema
    metadata: Mapping | None = None


@dataclass(frozen=True)
class QuerySource:
    """:func:`compose_geo_block` source: a DuckDB query (its output schema) plus carried metadata."""

    con: duckdb.DuckDBPyConnection
    query: str
    metadata: Mapping | None = None


GeoBlockSource = ArrowTableSource | SchemaSource | QuerySource


class _Sentinel:
    """A named option value no caller can spell by accident."""

    def __init__(self, name: str) -> None:
        self._name = name

    def __repr__(self) -> str:
        return self._name


#: ``GeoBlockOptions.covering``: decide the bbox covering from the source's schema.
DETECT_BBOX_COLUMN = _Sentinel("DETECT_BBOX_COLUMN")
#: ``GeoBlockOptions.crs``: leave the carried ``crs`` exactly as it is.
KEEP_CRS = _Sentinel("KEEP_CRS")


@dataclass(frozen=True)
class GeoBlockOptions:
    """What a write asks :func:`compose_geo_block` to declare.

    ``version`` is an output-version key (``"1.0"``, ``"1.1"``,
    ``"1.1-geoarrow"``, ``"2.0"``, ``"parquet-geo-only"``), resolved by the
    write facade (``parquet_writer.resolve_output_geoparquet_version``) before
    composing; the block gets that key's metadata form (``None`` for
    parquet-geo-only, whose block is built only to key the native types and
    never written). ``None`` (or ``describe=True``) makes an in-memory
    *description* of a table (a stream's carried block, a block a repair
    edits), which keeps the version the carried block declares
    (:func:`declared_version`) and which the write at the other end
    re-composes. The composer decides no version itself: a description of a
    source that declares none takes ``version``, the facade's answer
    (``parquet_writer.version_to_describe``), and raises without one. A
    description records its ``edges`` and ``crs`` as declared.

    ``covering`` is the bbox covering request: :data:`DETECT_BBOX_COLUMN` (the
    default) declares the one column the source's schema and the block's own
    provenance allow (:func:`bbox_column_to_declare`), a column name declares
    that column, and ``None`` declares none. Every value still runs every gate.

    ``custom_metadata``'s ``covering`` mapping (h3/s2/a5/quadkey, or a bbox a
    caller computed) is merged one entry deep into the primary's ``covering``;
    any other key goes to the top level of the block. ``edges`` is the
    primary's requested edges, made representable in ``version``, with the
    orientation a spherical request implies. ``column_edges`` is
    ``{column: edges}`` as each column's data already has them (a post-write
    restore, a stream's resolved edges): mapped the same way, implying nothing.
    The two are distinct inputs -- a request versus a fact about the data --
    so naming the primary in ``column_edges`` with a value other than
    ``edges`` is a contradiction the composer refuses (``ValueError``).
    ``encoding`` is the primary's output encoding as the writer
    knows it; with none given, 1.1-geoarrow keeps the native encoding the input
    declared (``geometry_info``'s), and otherwise the carried one stays, or WKB.
    ``crs`` is the requested output CRS as
    :func:`~geoparquet_io.core.crs_utils.apply_output_crs` takes it (``None``:
    unchanged but normalized), or for a description (``version=None``) the CRS
    the source declares, recorded as it is; :data:`KEEP_CRS` leaves the carried
    value alone. ``column_crs`` is ``{column: crs}`` for other columns, by the
    same rule (a repair that resolves every unknown CRS at once).
    ``stats`` maps a column to the measured ``bbox`` and/or ``geometry_types``
    it declares (a key absent from the inner mapping is left as carried; a
    ``None`` value removes the key).
    ``geometry_info`` names the secondary geometry columns and what the input
    declared for each. Each declared entry is an input to the steps, never an
    entry pasted in: its ``encoding``, ``edges``, ``crs`` and stats go through
    steps 3, 4, 5 and 8 wherever the carried entry says nothing, and only its
    other keys (``epoch``, ``orientation``, ...) are carried as declared. The
    primary is never a secondary: naming it there raises.
    ``covering_provenance`` is the KV metadata whose block says which column
    the primary's covering names, for a source that carries no block of its
    own (a table DuckDB built from a query, #953); a carried block wins.
    ``describe`` makes the block a description of a table, not a file's
    block, even with a ``version`` (which is then only the facade's answer for
    a source that declares none). It is not a version switch: beyond keeping
    the declared version, a description records ``crs`` and ``edges`` as
    declared -- no output CRS rule, no mapping of edges to the version, no
    implied orientation -- so the write at the other end decides those. The
    gates run either way. A block that is written (a repair, a patch) is never
    a description: it takes ``version`` from the facade's repair policy
    (``resolve_output_geoparquet_version(..., preserve_input=True)``).

    ``geometry_info`` declares each column's entry. The carried-wins rule for
    the primary's declared entry applies in description mode only; on a write
    the primary entry describes the input and the composer builds the output.
    ``primary_column`` relabels the block's primary (a repair of DuckDB's
    first-column pick); otherwise the carried primary stays, or
    ``geometry_column`` is named. ``geometry_column`` is the column the
    options describe, which a write of a transformed secondary (``process
    simplify --geometry-column``) does not make the primary.
    ``measure_missing`` is called with the block once steps 1-8 have applied
    every carried and given stat; the stats it returns (``{column: {bbox,
    geometry_types}}``, for what the block still lacks) are applied as step 8
    is -- so a writer that measures only the gaps composes once.
    """

    geometry_column: str
    version: str | None = None
    covering: object = DETECT_BBOX_COLUMN
    custom_metadata: Mapping | None = None
    edges: str | None = None
    column_edges: Mapping[str, str | None] | None = None
    encoding: str | None = None
    crs: object = KEEP_CRS
    column_crs: Mapping[str, object] | None = None
    stats: Mapping[str, Mapping[str, object]] | None = None
    geometry_info: Mapping | None = None
    covering_provenance: Mapping | None = None
    describe: bool = False
    primary_column: str | None = None
    measure_missing: Callable[[dict], Mapping] | None = None
    verbose: bool = False


class _SourceView:
    """The carried metadata and the output schema of a source, probed at most once."""

    def __init__(self, source: GeoBlockSource) -> None:
        if isinstance(source, ArrowTableSource):
            self.metadata, self._schema = source.table.schema.metadata, source.table.schema
        elif isinstance(source, SchemaSource):
            self.metadata, self._schema = source.metadata, source.schema
        elif isinstance(source, QuerySource):
            self.metadata, self._schema = source.metadata, None
        else:
            raise TypeError(f"compose_geo_block: unsupported source {type(source).__name__}")
        self._source = source

    @property
    def schema(self) -> pa.Schema:
        if self._schema is None:
            self._schema = query_schema(self._source.con, self._source.query)
        return self._schema


def _metadata_version(options: GeoBlockOptions, carried: dict) -> str | None:
    """The block's ``version`` for an output-version key (see :class:`GeoBlockOptions`).

    A description keeps the version the carried block declares; the facade
    decides every other one, so a description with none to keep takes the
    caller's ``version`` and is refused without one.
    """
    version = options.version
    if version is None or options.describe:
        declared = declared_version(carried, default=None)
        if declared is not None:
            return declared
    if version is None:
        raise ValueError(
            "compose_geo_block: a description keeps the carried block's version, and "
            "this source declares none; pass the version "
            "parquet_writer.version_to_describe resolves"
        )
    if version not in GEOPARQUET_VERSIONS:
        raise ValueError(
            f"compose_geo_block: version must be one of {sorted(GEOPARQUET_VERSIONS)}, "
            f"not {version!r}"
        )
    return GEOPARQUET_VERSIONS[version]["metadata_version"]


#: The keys of a declared secondary entry the steps decide, not carry.
_STEP_KEYS = frozenset({"encoding", "edges", "crs", "bbox", "geometry_types"})


@dataclass
class _Secondaries:
    """What ``geometry_info`` declares for the secondaries, as inputs to steps 3/4/5/8."""

    edges: dict
    crs: dict
    stats: dict
    other: dict


def _secondary_inputs(
    geo_meta: dict, geometry_info: Mapping | None, primary: str, described: bool
) -> _Secondaries:
    """Step 2's secondary entries, and what each column's declared entry says for the steps.

    One rule for every column ``geometry_info`` declares: only what the
    carried entry does not already say is an input (the carried keys win), and
    an option the caller set wins over it. The primary's declared entry is an
    input to a *description* only (a stream's or a read's block, which no
    later step re-measures): a write decides its primary's CRS and stats
    itself, and a declared entry handed down the write path describes the
    input, not the output (a forced-2D write, a rewrite of stale stats). The
    primary's declared ``encoding`` and other keys are never inputs: step 3
    decides its encoding.
    """
    found = _Secondaries({}, {}, {}, {})
    if not geometry_info:
        return found
    declared_by_col = geometry_info.get("metadata") or {}
    columns = geo_meta["columns"]
    for column in geometry_info.get("secondary") or ():
        if column == primary:
            raise ValueError(
                f"compose_geo_block: {column!r} is the primary geometry column, not a secondary"
            )
        entry = columns.setdefault(column, {})
        declared = declared_by_col.get(column) or {}
        if "encoding" not in entry:
            entry["encoding"] = declared.get("encoding") or "WKB"
        _declared_gaps(found, column, entry, declared)
        found.other[column] = {k: v for k, v in declared.items() if k not in _STEP_KEYS}
    if described and isinstance(declared_by_col.get(primary), Mapping):
        _declared_gaps(found, primary, columns[primary], declared_by_col[primary])
    return found


def _declared_gaps(found: _Secondaries, column: str, entry: dict, declared: Mapping) -> None:
    """``declared``'s edges, crs and stats where the carried ``entry`` says nothing."""
    if declares_edges(declared) and not declares_edges(entry):
        # A value outside the vocabulary says nothing (#1288).
        if edges_of(declared) in EDGES:
            found.edges[column] = edges_of(declared)
    if "crs" in declared and "crs" not in entry:
        found.crs[column] = declared["crs"]
    stats = {k: declared[k] for k in ("bbox", "geometry_types") if k in declared}
    stats = {k: v for k, v in stats.items() if k not in entry}
    if stats:
        found.stats[column] = stats


def _merge_custom_metadata(geo_meta: dict, col_meta: dict, custom_metadata, verbose) -> None:
    """Step 6: ``custom_metadata``'s coverings onto the primary, its other keys on top.

    ``covering`` is merged one entry deep rather than replaced: it holds one
    entry per covering kind (bbox, h3, s2, a5, quadkey), and callers supply
    only the one they just produced. Replacing the dict made ``gpio sort
    quadkey`` destroy the bbox covering its input declared while adding its own
    (#738). Individual entries still win, so a caller can correct one.
    """
    for key, value in (custom_metadata or {}).items():
        if key != "covering":
            geo_meta[key] = value
            continue
        if not isinstance(value, Mapping):
            continue
        existing = col_meta.get("covering")
        col_meta["covering"] = {**existing, **value} if isinstance(existing, dict) else dict(value)
        if verbose:
            for name in value:
                debug(f"Added {name} covering metadata")


def _apply_stats(geo_meta: dict, stats) -> None:
    """Step 8: each column's measured ``bbox`` and ``geometry_types``, in that order.

    A measured ``None`` is "nothing to declare": the key goes, which is how the
    spec spells an unknown ``bbox`` (and lets a write's own empty-table rule
    decide ``geometry_types``).
    """
    for column, measured in (stats or {}).items():
        col_meta = geo_meta["columns"].setdefault(column, {})
        for key in ("bbox", "geometry_types"):
            if key not in measured:
                continue
            if measured[key] is None:
                col_meta.pop(key, None)
            else:
                col_meta[key] = measured[key]


def _declare_bbox_covering(
    geo_meta, col_meta, covering, view, version, provenance, verbose
) -> None:
    """Step 9: the bbox covering request, for a version that can say it.

    A requested column is declared as the caller asked (the illegal-struct gate
    still judges it). :data:`DETECT_BBOX_COLUMN` asks
    :func:`bbox_column_to_declare` of ``provenance`` (the block so far, or the
    caller's ``covering_provenance`` when nothing was carried) -- the one rule
    for a carried, a computed or a conventional bbox column -- and leaves a
    ``covering.bbox`` that already names that column as it came (#738).
    """
    if covering is None or not covering_supported(version):
        return
    if covering is DETECT_BBOX_COLUMN:
        name = bbox_column_to_declare(view.schema, provenance, verbose=verbose)
        if name is None or _declared_bbox_column(geo_meta) == name:
            return
    else:
        name = covering
    col_meta.setdefault("covering", {})["bbox"] = build_bbox_covering(name)
    if verbose:
        debug(f"Added bbox covering metadata for column '{name}'")


def _covering_gates(geo_meta: dict, view: _SourceView, version, verbose: bool) -> dict:
    """Steps 10-13, the covering gates, in their one order. Each returns a copy."""
    geo_meta = strip_unsupported_covering(geo_meta, version, verbose)
    if _declares_a_covering(geo_meta):
        geo_meta = strip_absent_covering(geo_meta, view.schema.names)
    if _declares_a_bbox_covering(geo_meta):
        geo_meta = strip_illegal_bbox_covering(geo_meta, view.schema)
    return strip_bboxless_covering(geo_meta, verbose)


def declares_covering_member(col_meta: object, member: str | None = None) -> bool:
    """Is this column's ``covering`` an object (with a ``member`` key, when named)?

    A string ``covering`` satisfies ``"bbox" in covering`` too (#1062), so the
    object test comes first; ``covering_is_object`` reports that shape.
    """
    covering = col_meta.get("covering") if isinstance(col_meta, dict) else None
    return isinstance(covering, dict) and (member is None or member in covering)


def _declares_a_covering(geo_meta: dict, member: str | None = None) -> bool:
    return any(
        declares_covering_member(col_meta, member)
        for col_meta in (geo_meta.get("columns") or {}).values()
    )


def _declares_a_bbox_covering(geo_meta: dict) -> bool:
    return _declares_a_covering(geo_meta, "bbox")


def compose_geo_block(source: GeoBlockSource, options: GeoBlockOptions) -> dict:
    """Compose the ``geo`` block a write declares. The only composer (#1367 concept 4).

    Every ``geo`` block gpio writes or describes a table with comes from here,
    in this one order:

    1. the carried block, sanitized (:func:`sanitized_carried_geo`), copied;
    2. ``version`` (the metadata form of the output-version key, or the
       declared one for a description), ``primary_column`` (the requested
       relabel, else the carried one, else ``geometry_column``), the primary
       entry, and an entry for every
       secondary ``geometry_info`` names, with what it declares split into the
       inputs of steps 3, 4, 5 and 8;
    3. the ``encoding`` (:func:`_primary_encoding`; a secondary's declared one);
    4. ``edges`` -- the carried ones as every writer says them, then each
       column's (``column_edges``, a secondary's declared ones) and the
       primary's request, mapped by
       :func:`~geoparquet_io.core.edges.edges_for_version` -- and the
       ``orientation`` a spherical *request* (``edges``) implies; the data's
       own edges (``column_edges``) imply none, and the two disagreeing on the
       primary raise (a description records the edges as declared and
       implies nothing);
    5. the ``crs`` (:func:`~geoparquet_io.core.crs_utils.apply_output_crs`, the
       one CRS writer; a description records the CRS its source declares);
    6. ``custom_metadata``: coverings merged into the primary's, other keys on top;
    7. the secondaries' other declared keys (``epoch``, ``orientation``, ...);
    8. stats per column: a secondary's declared ones, then the measured ``stats``;
    9. the bbox covering request (``covering``);
    10. :func:`strip_unsupported_covering` (no ``covering`` before 1.1);
    11. :func:`strip_absent_covering` (no entry naming a column the source lacks);
    12. :func:`strip_illegal_bbox_covering` (no ``covering.bbox`` over a struct
        the spec forbids);
    13. :func:`strip_bboxless_covering` (no covering without a ``bbox``, #954).

    ``source`` supplies the carried KV metadata and the output's schema: an
    Arrow table, a schema plus metadata, or a DuckDB connection and query (its
    schema probed once with :func:`~geoparquet_io.core.duckdb_utils.query_schema`,
    and only when step 9 or a gate needs it). There is no second entry point and
    no gate a caller can skip: every option has the gates as its consequence.
    Never mutates the source's metadata.
    """
    from geoparquet_io.core.crs_utils import apply_output_crs

    view = _SourceView(source)
    # Step 1.
    carried = sanitized_carried_geo(view.metadata)
    geo_meta = copy.deepcopy(carried)
    primary = options.geometry_column
    described = options.version is None or options.describe
    # Step 2.
    version = _metadata_version(options, geo_meta)
    geo_meta["version"] = version
    if options.primary_column is not None:
        geo_meta["primary_column"] = options.primary_column
    else:
        geo_meta.setdefault("primary_column", primary)
    geo_meta.setdefault("columns", {})
    col_meta = geo_meta["columns"].setdefault(primary, {})
    secondaries = _secondary_inputs(geo_meta, options.geometry_info, primary, described)
    if options.crs is not KEEP_CRS:
        secondaries.crs.pop(primary, None)  # the caller's CRS wins over the declared one
    # Step 3.
    col_meta["encoding"] = _primary_encoding(options, col_meta)
    # Step 4. A description (``version=None``) records the requested edges as
    # declared: its version is not an output decision, and the write that
    # re-composes it maps them to what the output can say.
    carry_edges(geo_meta, None)
    _refuse_contradicting_edges(options, primary)
    by_col = {**secondaries.edges, **(options.column_edges or {})}
    if options.edges:
        by_col[primary] = options.edges
    by_col = {column: edges for column, edges in by_col.items() if edges}
    if not described:
        by_col = {c: edges_for_version(e, is_geoparquet_1x(version)) for c, e in by_col.items()}
    carry_edges(geo_meta, by_col)
    orientation = None if described else implied_orientation(options.edges)
    if orientation:
        col_meta["orientation"] = orientation
    # Step 5. A description records the CRS its source declares; a write's
    # block says it the way the output rule does (the default by omission).
    if options.crs is KEEP_CRS:
        pass
    elif described:
        col_meta["crs"] = options.crs
    else:
        apply_output_crs(col_meta, options.crs)
    for column, crs in secondaries.crs.items():
        geo_meta["columns"][column]["crs"] = crs
        if not described:
            apply_output_crs(geo_meta["columns"][column], None)
    for column, crs in (options.column_crs or {}).items():
        entry = geo_meta["columns"].get(column)
        if not isinstance(entry, dict):
            continue
        if described:
            entry["crs"] = crs
        else:
            apply_output_crs(entry, crs)
    # Steps 6-9.
    _merge_custom_metadata(geo_meta, col_meta, options.custom_metadata, options.verbose)
    for column, other in secondaries.other.items():
        for key, value in other.items():
            geo_meta["columns"][column].setdefault(key, value)
    _apply_stats(geo_meta, secondaries.stats)
    _apply_stats(geo_meta, options.stats)
    if options.measure_missing is not None:
        _apply_stats(geo_meta, options.measure_missing(geo_meta))
    provenance = geo_meta if carried else sanitized_carried_geo(options.covering_provenance)
    _declare_bbox_covering(
        geo_meta, col_meta, options.covering, view, version, provenance, options.verbose
    )
    # Steps 10-13.
    return _covering_gates(geo_meta, view, version, options.verbose)


def _refuse_contradicting_edges(options: GeoBlockOptions, primary: str) -> None:
    """``edges`` (the request) and ``column_edges`` (the data) must agree on the primary."""
    data = (options.column_edges or {}).get(primary, options.edges)
    if options.edges and primary in (options.column_edges or {}) and data != options.edges:
        raise ValueError(
            f"compose_geo_block: edges={options.edges!r} is requested for '{primary}' "
            f"but column_edges says its data has {data!r}"
        )


def composes_a_covering(source: GeoBlockSource, options: GeoBlockOptions) -> bool:
    """Whether the composer's block for ``source`` declares a covering on the column.

    The one answer to "would a write declare a covering here?", asked of the
    composer rather than predicted beside it: every gate has run (#954).
    """
    return "covering" in compose_geo_block(source, options)["columns"][options.geometry_column]


def _primary_encoding(options: GeoBlockOptions, col_meta: dict) -> str:
    """Step 3: the primary's ``encoding``, the one rule for every writer.

    The writer's own answer -- the encoding it actually writes -- when it
    gives one. Otherwise 1.1-geoarrow keeps the native encoding the input
    declared (``geometry_info``'s), and every block keeps the carried one, or
    says WKB.
    """
    if options.encoding is not None:
        return options.encoding
    if options.version == "1.1-geoarrow":
        declared = ((options.geometry_info or {}).get("metadata") or {}).get(
            options.geometry_column
        ) or {}
        if declared.get("encoding"):
            return declared["encoding"]
    return col_meta.get("encoding") or "WKB"


# =============================================================================
# SQL-based Metadata Computation
# =============================================================================


# =============================================================================
# Geometry statistics and dataset bounds (#1367 concept 5)
# =============================================================================
#
# One routine measures ``bbox`` and ``geometry_types`` for every source gpio
# holds geometry in -- an Arrow table, a DuckDB query, a Parquet file -- with
# one degenerate-geometry policy:
#
# * NULL and EMPTY geometries are skipped, for the bbox and for the types. A
#   geometry is EMPTY when its envelope is inverted or absent: that is how
#   ``POLYGON EMPTY``, ``POINT(NaN NaN)`` (WKB's empty point) and a
#   coordinate-free collection measure, on DuckDB and on geoarrow alike.
#   geoarrow cannot name the type of every EMPTY geometry, so counting their
#   types made the two paths disagree.
# * Empty or all-degenerate input yields ``bbox=None`` and ``[]`` types --
#   never ``[0, 0, 0, 0]``, never the inverted ``[inf, inf, -inf, -inf]`` a
#   min/max fold of EMPTY envelopes gives, never a raise.
# * A located geometry with an infinite coordinate has no bbox anyone can
#   use. It raises :class:`GeometryError` naming the column and the geometry
#   (#1165), so no write declares ``Infinity`` in a ``geo`` bbox (#236).
# * ``geometry_types`` is sorted (the SQL path always was; the Arrow path
#   listed them in type-code order).
# * ``antimeridian=True`` reports lon/lat data astride the antimeridian in the
#   RFC 7946 wrap form, ``xmin > xmax`` (moved here from process/aggregate).

#: How close to +/-180 data must reach before the antimeridian scan runs.
_ANTIMERIDIAN_EPS = 1e-6


def split_zm_suffix(name: str) -> tuple[str, str]:
    """Split a spec geometry type into (base, suffix): 'Point ZM' -> ('Point', ' ZM').

    Names without a dimension suffix (and non-string inputs) return ('' suffix).
    """
    if isinstance(name, str):
        for suffix in (" ZM", " Z", " M"):
            if name.endswith(suffix):
                return name[: -len(suffix)], suffix
    return name, ""


def zm_suffix_sql(geom_expr: str, sep: str = " ") -> str:
    """SQL fragment producing the dimension suffix (''/'{sep}Z'/'{sep}M'/'{sep}ZM').

    The GeoParquet spec treats "LineString" and "LineString ZM" as distinct
    geometry_types entries, so any SQL type scan must append this suffix to
    ST_GeometryType() output instead of collapsing dimensions.
    """
    return (
        f"CASE WHEN ST_HasZ({geom_expr}) AND ST_HasM({geom_expr}) THEN '{sep}ZM' "
        f"WHEN ST_HasZ({geom_expr}) THEN '{sep}Z' "
        f"WHEN ST_HasM({geom_expr}) THEN '{sep}M' "
        f"ELSE '' END"
    )


@dataclass(frozen=True)
class GeometryStats:
    """What :func:`geometry_stats_of` measured: ``[xmin, ymin, xmax, ymax]`` or None, and the types."""

    bbox: list[float] | None
    geometry_types: list[str]


_NO_STATS = GeometryStats(None, [])


def merged_geometry_stats(parts, *, geographic: bool) -> GeometryStats:
    """The one fold: the stats of the union of what ``parts`` were measured on.

    Each part's bbox is a spec bbox, ``[xmin, ymin, xmax, ymax]`` or the 6-value
    ``[xmin, ymin, zmin, xmax, ymax, zmax]``, or None. X is merged on the
    circle (``crs_utils.merge_longitude_ranges``), so a part whose xmin exceeds
    its xmax -- an antimeridian wrap, which a bbox and the Parquet statistics
    both permit -- is not inverted into its complement; with no wrapping part
    that is plain min/max. Y is min/max; Z is merged over the parts that carry
    one and makes the result 6 values. Types are the sorted union.

    ``geographic`` has no default, so every fold says which CRS it is in
    (``crs_utils.is_geographic_crs``): for projected coordinates the wrap
    reading splits at +/-180, meaningless in metres, so X is plain min/max
    there (#886) -- xmin > xmax in a projected bbox is broken metadata.

    Batch writers, row-group statistics readers and multi-file summaries all
    fold here.
    """
    from geoparquet_io.core.crs_utils import merge_longitude_ranges

    parts = list(parts)
    boxes = [p.bbox for p in parts if p.bbox is not None]
    types = sorted({t for p in parts for t in p.geometry_types})
    if not boxes:
        return GeometryStats(None, types)
    xy = [bbox_xy(b) for b in boxes]
    if geographic:
        xmin, xmax = merge_longitude_ranges([(b[0], b[2]) for b in xy])
    else:
        xmin, xmax = min(b[0] for b in xy), max(b[2] for b in xy)
    bbox = [xmin, min(b[1] for b in xy), xmax, max(b[3] for b in xy)]
    zs = [(b[2], b[5]) for b in boxes if len(b) == 6]
    if zs:
        bbox = [bbox[0], bbox[1], min(z[0] for z in zs), bbox[2], bbox[3], max(z[1] for z in zs)]
    return GeometryStats(bbox, types)


def bbox_xy(bbox) -> tuple[float, float, float, float]:
    """``(xmin, ymin, xmax, ymax)`` of a 4-, 6- or 8-value spec bbox.

    The spec interleaves the dimensions (``[xmin, ymin, zmin, xmax, ymax,
    zmax]``), so indices 2 and 3 are xmax/ymax only in the 4-value form. The one
    reader of X/Y off an interleaved bbox.
    """
    half = len(bbox) // 2
    return float(bbox[0]), float(bbox[1]), float(bbox[half]), float(bbox[half + 1])


def row_group_geometry_stats(chunks, *, geographic: bool) -> GeometryStats:
    """The file's stats from its per-row-group statistics, through the one fold.

    ``chunks`` are dicts with ``xmin``..``ymax``, optional ``zmin``/``zmax``
    and ``geometry_types`` (WKB codes or DuckDB strings), as the Parquet
    geospatial statistics readers in ``duckdb_metadata`` return them. A chunk
    without an extent still contributes its types; the bbox comes back in the
    spec's interleaved layout. ``geographic`` is the column's CRS, as
    :func:`merged_geometry_stats` takes it.
    """
    parts = []
    for chunk in chunks:
        bbox = None
        if all(chunk.get(k) is not None for k in ("xmin", "ymin", "xmax", "ymax")):
            bbox = [chunk["xmin"], chunk["ymin"], chunk["xmax"], chunk["ymax"]]
            if chunk.get("zmin") is not None:
                bbox = [bbox[0], bbox[1], chunk["zmin"], bbox[2], bbox[3], chunk["zmax"]]
        names = {geometry_type_name(t) for t in chunk.get("geometry_types") or []}
        parts.append(GeometryStats(bbox, sorted(n for n in names if n)))
    return merged_geometry_stats(parts, geographic=geographic)


def _non_finite_message(column: str, example: str | None = None) -> str:
    """The one sentence for a located geometry with an infinite coordinate (#1165).

    ``example`` names the offending value where the caller has one; the SQL
    struct expression raises it from inside a query, where it has none.
    """
    where = f", {example}," if example else ","
    return (
        f"Column '{column}' holds a geometry with a non-finite (infinite) coordinate"
        f"{where} so its bounds cannot be measured. Fix or remove that geometry and "
        "try again."
    )


def _non_finite_error(column: str, example: str) -> Exception:
    from geoparquet_io.core.exceptions import GeometryError

    return GeometryError(_non_finite_message(column, example))


def _located_sql(x0: str, y0: str, x1: str, y1: str) -> str:
    """The SQL rule for "this envelope is a location": not NaN, not inverted.

    The SQL twin of the Arrow kernel's ``_located``. NaN is no location:
    Arrow compares it false, but DuckDB orders it last and calls
    ``NaN <= NaN`` true, so it is said explicitly. A NULL or EMPTY geometry
    (an absent or inverted envelope) is not located either.
    """
    no_nan = " AND ".join(f"NOT isnan({v})" for v in (x0, y0, x1, y1))
    return f"({no_nan} AND {x0} <= {x1} AND {y0} <= {y1})"


def _finite_sql(*envelope: str) -> str:
    """The SQL rule for a located envelope whose bounds can be measured."""
    return " AND ".join(f"isfinite({v})" for v in envelope)


# -- DuckDB ------------------------------------------------------------------


def _envelope_columns(read) -> tuple[str, list[str]]:
    """``(type expression, the four envelope expressions)`` of one row.

    ``read`` is the owner's :class:`~geoparquet_io.core.geometry_detection.GeometryRead`.
    The type expression is NULL where SQL cannot name the type (a GeoArrow
    coordinate struct, which ``ST_GeometryType`` does not take).
    """
    if read.kind == "geoarrow":
        enc = read.geoarrow_encoding
        xmin_e, ymin_e, xmax_e, ymax_e, _, _ = _geoarrow_coord_exprs(read.quoted, enc)
        # A GeoArrow EMPTY carries NaN coordinates instead of none.
        xmin_e, ymin_e = (f"nullif({e}, 'NaN'::DOUBLE)" for e in (xmin_e, ymin_e))
        xmax_e, ymax_e = (f"nullif({e}, 'NaN'::DOUBLE)" for e in (xmax_e, ymax_e))
        return "NULL::VARCHAR", [xmin_e, ymin_e, xmax_e, ymax_e]
    g = read.shape_expr
    return (
        f"ST_GeometryType({g}) || {zm_suffix_sql(g)}",
        [f"ST_{axis}({g})" for axis in ("XMin", "YMin", "XMax", "YMax")],
    )


def _located_rows_sql(con, query: str, geometry_column: str, bbox_column: str | None):
    """A SELECT of ``(__t, __x0, __y0, __x1, __y1, __g)`` over the located rows, or None.

    ``__t`` is the spec type name base from DuckDB (NULL when unknowable),
    ``__g`` the GEOMETRY (NULL on the struct paths). None when the column is
    not in the query's output; a query that cannot be bound raises, as it
    would at the write it is measured for.
    """
    from geoparquet_io.core.geometry_detection import GeometryRead

    target = bbox_column or geometry_column
    col_type = dict(describe_query(con, query)).get(target)
    if col_type is None:
        return None
    quoted = quote_identifier(target)
    if bbox_column is not None:
        type_expr, env = (
            "NULL::VARCHAR",
            [f"{quoted}.{f}" for f in ("xmin", "ymin", "xmax", "ymax")],
        )
        geom = "NULL::GEOMETRY"
    else:
        read = GeometryRead.for_type(target, col_type)
        type_expr, env = _envelope_columns(read)
        geom = "NULL::GEOMETRY" if read.kind == "geoarrow" else read.shape_expr
    return f"""
        SELECT * FROM (
            SELECT {type_expr} AS __t, {", ".join(env)}, {geom} AS __g
            FROM ({query})
        ) AS __m(__t, __x0, __y0, __x1, __y1, __g)
        WHERE {_located_sql("__x0", "__y0", "__x1", "__y1")}
    """


_FINITE_SQL = _finite_sql("__x0", "__y0", "__x1", "__y1")


def _query_stats(
    con, query: str, geometry_column: str, bbox_column: str | None, need_bbox: bool
) -> tuple[GeometryStats, str | None]:
    """The stats of a query, and the located-rows SQL they came from (None: nothing to read)."""
    query = _strip_trailing_order_by(query)
    located = _located_rows_sql(con, query, geometry_column, bbox_column)
    if located is None:
        return _NO_STATS, None
    rows = con.execute(f"""
        SELECT __t, MIN(__x0), MIN(__y0), MAX(__x1), MAX(__y1),
               COUNT(*) FILTER (WHERE NOT ({_FINITE_SQL}))
        FROM ({located})
        GROUP BY 1
    """).fetchall()
    if need_bbox and any(r[5] for r in rows):
        example = fetch_one(
            con.execute(f"""
            SELECT COALESCE(ST_AsText(__g),
                   'envelope [' || __x0 || ', ' || __y0 || ', ' || __x1 || ', ' || __y1 || ']')
            FROM ({located}) WHERE NOT ({_FINITE_SQL}) LIMIT 1
        """)
        )
        raise _non_finite_error(geometry_column, example[0] if example else "")
    types = sorted({n for n in (geometry_type_name(r[0]) for r in rows) if n})
    if not rows:
        return GeometryStats(None, types), located
    bbox = [
        min(r[1] for r in rows),
        min(r[2] for r in rows),
        max(r[3] for r in rows),
        max(r[4] for r in rows),
    ]
    return GeometryStats(bbox, types), located


#: The widest longitude gap between the parts of the located geometries, by a
#: running max over the sorted part extents -- the standard interval merge.
#: The parts, not the whole geometries: a cut cell's own extent already spans
#: -180 to 180 and would hide the gap that makes the crossing visible.
_WIDEST_LON_GAP_SQL = """
WITH parts AS (
    SELECT ST_XMin(p) AS lo, ST_XMax(p) AS hi
    FROM (SELECT UNNEST(ST_Dump(__g)).geom AS p FROM ({located}) WHERE __g IS NOT NULL)
    WHERE NOT ST_IsEmpty(p)
), merged AS (
    SELECT lo, max(hi) OVER (ORDER BY lo ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev
    FROM parts
)
SELECT lo, prev FROM merged WHERE prev IS NOT NULL AND lo > prev ORDER BY lo - prev DESC LIMIT 1
"""


def _antimeridian_wrap(con, located: str, bbox: list[float]) -> list[float]:
    """``bbox`` in RFC 7946 wrap form when the data sits astride the antimeridian.

    A cell that straddles the antimeridian is written cut into a MultiPolygon
    with parts at both -180 and +180 (RFC 7946 3.1.9). Plain min/max over such
    a dataset reports [-180, ..., 180, ...] -- true, but it claims the whole
    globe for data that only touches the seam. RFC 7946 5.2, which GeoParquet's
    ``bbox`` follows, writes the crossing extent as xmin > xmax instead, when
    the data leaves a longitude gap wider than the one the plain extent
    implies. Longitudes are assumed: the caller establishes a geographic CRS.
    """
    xmin, ymin, xmax, ymax = bbox
    # Only data reaching both edges can be hiding a crossing; anything else is
    # already reported tightly and must not pay for the part-level scan.
    if xmin > -180.0 + _ANTIMERIDIAN_EPS or xmax < 180.0 - _ANTIMERIDIAN_EPS:
        return bbox
    gap = fetch_one(con.execute(_WIDEST_LON_GAP_SQL.format(located=located)))
    if gap is None or (gap[0] - gap[1]) <= (xmin + 360.0) - xmax:
        return bbox
    return [gap[0], ymin, gap[1], ymax]


# -- Arrow ------------------------------------------------------------------


def _arrow_types(wkb, verbose: bool) -> list[str]:
    import geoarrow.pyarrow as ga

    try:
        codes = geoarrow_wkb_codes(ga.unique_geometry_types(ga.as_wkb(wkb)))
    except Exception as e:  # geoarrow C++ errors, e.g. a type code of 0
        if verbose:
            debug(f"Could not compute geometry_types: {e}")
        return []
    names = sorted({n for n in map(_get_geometry_type_name, codes) if n})
    if verbose:
        debug(f"Computed geometry_types from data: {names}")
    return names


def bbox_struct_type():
    """The bbox struct gpio computes: four doubles, in the spec's field order."""
    import pyarrow as pa

    return pa.struct([(f, pa.float64()) for f in ("xmin", "ymin", "xmax", "ymax")])


#: ``(x0, y0, x1, y1)``: the order every envelope tuple here is in.
_ENVELOPE_ORDER = ("xmin", "ymin", "xmax", "ymax")


def _row_envelopes(table: pa.Table, geometry_column: str):
    """``(storage, x0, y0, x1, y1)``: every row's WKB and envelope, NULL rows null."""
    import geoarrow.pyarrow as ga
    import pyarrow as pa
    import pyarrow.compute as pc

    wkb = ga.as_wkb(table.column(geometry_column)).combine_chunks()
    storage = wkb.storage if isinstance(wkb, pa.ExtensionArray) else wkb
    box = ga.box(ga.as_wkb(storage)).storage
    return (storage, *(pc.struct_field(box, f) for f in _ENVELOPE_ORDER))


def _row_envelopes_of_readable(table: pa.Table, geometry_column: str):
    """:func:`_row_envelopes`, with a row whose bytes are not WKB measured as NULL.

    The kernel refuses a whole array over one unreadable row; only then is it
    asked row by row, so a clean column pays nothing for the tolerance.
    """
    try:
        return _row_envelopes(table, geometry_column)
    except Exception:  # geoarrow C++ errors on malformed WKB
        import geoarrow.pyarrow as ga
        import pyarrow as pa
        import pyarrow.compute as pc

        wkb = ga.as_wkb(table.column(geometry_column)).combine_chunks()
        storage = wkb.storage if isinstance(wkb, pa.ExtensionArray) else wkb
        readable = []
        for i in range(len(storage)):
            try:
                ga.box(ga.as_wkb(storage.slice(i, 1)))
                readable.append(storage[i].is_valid)
            except Exception:
                readable.append(False)
        storage = pc.if_else(pa.array(readable), storage, pa.scalar(None, storage.type))
        box = ga.box(ga.as_wkb(storage)).storage
        return (storage, *(pc.struct_field(box, f) for f in _ENVELOPE_ORDER))


def _located(x0, y0, x1, y1):
    """The one EMPTY rule: an envelope is a location when it is not inverted (or NULL)."""
    import pyarrow.compute as pc

    return pc.fill_null(pc.and_(pc.less_equal(x0, x1), pc.less_equal(y0, y1)), False)


def _unusable(located, x0, y0, x1, y1):
    """The located rows with an infinite coordinate: no bbox anyone can use (#1165)."""
    import pyarrow.compute as pc

    finite = pc.and_(
        pc.and_(pc.is_finite(x0), pc.is_finite(y0)), pc.and_(pc.is_finite(x1), pc.is_finite(y1))
    )
    return pc.and_(located, pc.invert(finite))


def _raise_unusable(geometry_column: str, storage, bad, envelope) -> None:
    """Raise the degenerate-geometry error naming the first unusable row."""
    import pyarrow.compute as pc

    example = "envelope " + str([pc.filter(v, bad)[0].as_py() for v in envelope])
    if storage is not None:
        import geoarrow.pyarrow as ga

        example = ga.as_wkt(ga.as_wkb(pc.filter(storage, bad)))[0].as_py()
    raise _non_finite_error(geometry_column, example)


def _arrow_envelopes(table: pa.Table, geometry_column: str, bbox_column: str | None):
    """``(storage, x0, y0, x1, y1)`` of the non-NULL rows as flat arrays, or None."""
    import pyarrow.compute as pc

    if bbox_column is not None:
        storage = table.column(bbox_column).combine_chunks()
        storage = storage.filter(pc.is_valid(storage))
        fields = [pc.struct_field(storage, f) for f in ("xmin", "ymin", "xmax", "ymax")]
        return (None, *fields)
    storage, *envelope = _row_envelopes(table, geometry_column)
    valid = pc.is_valid(storage)
    if not pc.any(valid).as_py():
        return None
    return (storage.filter(valid), *(pc.filter(v, valid) for v in envelope))


def geo_key_value(geo: dict) -> str:
    """``geo`` serialized as a ``geo`` key's value (the footer-patch form)."""
    return json.dumps(geo)


def with_geo_key(metadata: Mapping | None, geo: dict) -> dict:
    """A copy of KV ``metadata`` with ``geo`` as its ``geo`` key, every other key kept."""
    out = dict(metadata or {})
    out.pop("geo", None)
    out[b"geo"] = geo_key_value(geo).encode("utf-8")
    return out


def with_geo_block(table: pa.Table, geo: dict) -> pa.Table:
    """``table`` with ``geo`` as its schema's ``geo`` key, every other key kept."""
    return table.replace_schema_metadata(with_geo_key(table.schema.metadata, geo))


def bbox_struct_array(table: pa.Table, geometry_column: str, struct_type: pa.DataType):
    """Each row's bbox of ``geometry_column`` as a bbox struct of ``struct_type``.

    The per-row half of the stats owner, on the same envelopes and the same
    degenerate-geometry rule the file ``bbox`` folds (:func:`_row_envelopes`,
    :func:`_located`): a NULL, EMPTY or unreadable geometry is a NULL struct, and a located
    geometry with an infinite coordinate raises (#1165). The struct has the
    field order and child types ``struct_type`` names (the input's own bbox
    column, for a write that rebuilds it over new geometry); a float32 child is
    rounded outward, so the box never shrinks past the geometry.
    """
    import pyarrow as pa

    from geoparquet_io.core.optional_deps import load_module

    np = load_module("numpy")
    storage, *envelope = _row_envelopes_of_readable(table, geometry_column)
    located = _located(*envelope)
    bad = _unusable(located, *envelope)
    if bad.true_count:
        _raise_unusable(geometry_column, storage, bad, envelope)
    mask = located.to_numpy(zero_copy_only=False)
    corners = dict(zip(_ENVELOPE_ORDER, envelope, strict=True))
    children = []
    for field in struct_type:
        values = np.where(mask, corners[field.name].to_numpy(zero_copy_only=False), 0.0)
        if pa.types.is_float32(field.type):
            values = _outward_float32(np, values, field.name.endswith("min"))
        children.append(pa.array(values, type=field.type))
    return pa.StructArray.from_arrays(
        children, fields=list(struct_type), mask=pa.array(~mask) if not mask.all() else None
    )


def bbox_struct_sql(geometry_sql: str, column: str, read=None) -> str:
    """SQL for each row's bbox struct of a GEOMETRY expression: the SQL twin of
    :func:`bbox_struct_array`, with the stats path's one degenerate-geometry rule.

    A NULL or EMPTY geometry (an inverted, absent or NaN envelope:
    :func:`_located_sql`) is a NULL struct, and a located geometry with an
    infinite coordinate stops the query with the stats owner's error
    (:func:`_non_finite_message`, #1165). ``read`` is the owner's
    :class:`~geoparquet_io.core.geometry_detection.GeometryRead` of a column
    DuckDB does not measure with ``ST_XMin`` (a GeoArrow coordinate struct);
    its envelope is the stats path's (``_envelope_columns``). ``column`` names
    the column in the error.
    """
    if read is not None and read.kind == "geoarrow":
        _, envelope = _envelope_columns(read)
    else:
        envelope = [f"ST_{axis}({geometry_sql})" for axis in ("XMin", "YMin", "XMax", "YMax")]
    x0, y0, x1, y1 = envelope
    message = sql_literal(_non_finite_message(column))
    return f"""(CASE
        WHEN {_located_sql(x0, y0, x1, y1)} IS NOT TRUE THEN NULL
        WHEN NOT ({_finite_sql(*envelope)}) THEN error({message})
        ELSE STRUCT_PACK(xmin := {x0}, ymin := {y0}, xmax := {x1}, ymax := {y1})
    END)"""


def _outward_float32(np, values, is_min: bool):
    """``values`` as float32, each rounded away from the box's inside."""
    cast = values.astype(np.float32)
    short = cast.astype(np.float64) > values if is_min else cast.astype(np.float64) < values
    toward = np.float32(-np.inf) if is_min else np.float32(np.inf)
    return np.where(short, np.nextafter(cast, toward), cast)


def _arrow_stats(
    table: pa.Table, geometry_column: str, bbox_column: str | None, need_bbox: bool, verbose: bool
) -> GeometryStats:
    import pyarrow.compute as pc

    target = bbox_column or geometry_column
    if target not in table.column_names or table.num_rows == 0:
        return _NO_STATS
    try:
        found = _arrow_envelopes(table, geometry_column, bbox_column)
    except Exception as e:  # geoarrow C++ errors on malformed WKB
        if verbose:
            debug(f"Could not compute bbox: {e}")
        return _NO_STATS
    if found is None:
        return _NO_STATS
    storage, x0, y0, x1, y1 = found
    located = _located(x0, y0, x1, y1)
    if not pc.any(located).as_py():
        return _NO_STATS
    bad = _unusable(located, x0, y0, x1, y1)
    if need_bbox and pc.any(bad).as_py():
        _raise_unusable(geometry_column, storage, bad, (x0, y0, x1, y1))
    types = [] if storage is None else _arrow_types(pc.filter(storage, located), verbose)
    bbox = [
        pc.min(pc.filter(x0, located)).as_py(),
        pc.min(pc.filter(y0, located)).as_py(),
        pc.max(pc.filter(x1, located)).as_py(),
        pc.max(pc.filter(y1, located)).as_py(),
    ]
    if verbose:
        debug(
            f"Computed bbox from data: [{bbox[0]:.6f}, {bbox[1]:.6f}, {bbox[2]:.6f}, {bbox[3]:.6f}]"
        )
    return GeometryStats(bbox, types)


# -- the owner ----------------------------------------------------------------


def _stats(
    source,
    geometry_column: str | None,
    *,
    antimeridian: bool,
    bbox_column: str | None,
    need_bbox: bool,
    verbose: bool,
) -> GeometryStats:
    import pyarrow as pa

    if isinstance(source, ArrowTableSource):
        source = source.table
    if isinstance(source, pa.Table):
        if not antimeridian:
            return _arrow_stats(source, geometry_column, bbox_column, need_bbox, verbose)
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection, register_arrow

        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            keep = [c for c in (geometry_column, bbox_column) if c in source.column_names]
            register_arrow(con, "__stats_source", source.select(keep))
            return _stats(
                QuerySource(con, "SELECT * FROM __stats_source"),
                geometry_column,
                antimeridian=True,
                bbox_column=bbox_column,
                need_bbox=need_bbox,
                verbose=verbose,
            )
        finally:
            con.close()
    if isinstance(source, QuerySource):
        stats, located = _query_stats(
            source.con, source.query, geometry_column, bbox_column, need_bbox
        )
        if antimeridian and stats.bbox is not None and bbox_column is None:
            stats = GeometryStats(
                _antimeridian_wrap(source.con, located, stats.bbox), stats.geometry_types
            )
        return stats
    return _file_stats(
        source,
        geometry_column,
        antimeridian=antimeridian,
        bbox_column=bbox_column,
        need_bbox=need_bbox,
        verbose=verbose,
    )


def _file_stats(path, geometry_column: str | None, **kwargs) -> GeometryStats:
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
    from geoparquet_io.core.file_utils import resolve_file_url
    from geoparquet_io.core.geometry_detection import primary_geometry_column
    from geoparquet_io.core.remote import needs_httpfs

    path = str(path)
    if geometry_column is None:
        geometry_column = primary_geometry_column(path)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(path))
    try:
        query = f"SELECT * FROM read_parquet({sql_path(resolve_file_url(path, verbose=False))})"
        return _stats(QuerySource(con, query), geometry_column, **kwargs)
    finally:
        con.close()


def geometry_stats_of(
    source,
    geometry_column: str | None,
    *,
    antimeridian: bool = False,
    bbox_column: str | None = None,
    verbose: bool = False,
) -> GeometryStats:
    """The ``bbox`` and ``geometry_types`` of ``geometry_column`` in ``source``, in one scan.

    ``source`` is an Arrow table (or :class:`ArrowTableSource`), a
    :class:`QuerySource` (only its ``con`` and ``query`` are read), or the RAW
    path or URL of a Parquet file. ``geometry_column`` may be None only for a
    file, whose primary geometry column is then looked up. The degenerate-
    geometry policy is the one in the section comment above: NULL and EMPTY
    are skipped, nothing measurable is ``GeometryStats(None, [])``, and an
    infinite coordinate raises :class:`~geoparquet_io.core.exceptions.GeometryError`.

    ``bbox_column`` measures a bbox covering's struct (its ``xmin``..``ymax``
    fields) instead of the geometry: the fast path for a file that has one.
    It yields no types. ``antimeridian=True`` returns a lon/lat extent astride
    the antimeridian with ``xmin > xmax``.

    A trailing ``ORDER BY`` on a query is dropped first: an aggregate cannot
    depend on row order, and DuckDB would otherwise sort the input to measure
    it (#1177). A column the query does not output measures nothing.
    """
    return _stats(
        source,
        geometry_column,
        antimeridian=antimeridian,
        bbox_column=bbox_column,
        need_bbox=True,
        verbose=verbose,
    )


def bounds_of(
    source,
    geometry_column: str | None,
    *,
    antimeridian: bool = False,
    bbox_column: str | None = None,
    verbose: bool = False,
) -> list[float] | None:
    """``[xmin, ymin, xmax, ymax]`` of ``geometry_column`` in ``source``, or None.

    :func:`geometry_stats_of`'s bbox; the same sources and the same policy.
    """
    return geometry_stats_of(
        source,
        geometry_column,
        antimeridian=antimeridian,
        bbox_column=bbox_column,
        verbose=verbose,
    ).bbox


def geometry_types_of(source, geometry_column: str | None, *, verbose: bool = False) -> list[str]:
    """The sorted GeoParquet ``geometry_types`` of ``geometry_column`` in ``source``.

    :func:`geometry_stats_of`'s types, with the dimension suffix ("Point Z").
    Needs no envelope, so a non-finite coordinate does not stop it.
    """
    return _stats(
        source,
        geometry_column,
        antimeridian=False,
        bbox_column=None,
        need_bbox=False,
        verbose=verbose,
    ).geometry_types


# The struct fields GeoParquet's "Bounding Box Columns" requires. The *name*
# rule that used to live beside it is gone: conventional-name matching over
# `bbox`/`bounds`/`extent`/`*_bbox` is no longer anybody's answer, on either the
# declare side or the read side. Both now go through
# `_self_evident_bbox_column`, which takes only the exact
# `SELF_EVIDENT_BBOX_COLUMN` — a `covering` asserts that a column's values bound
# the geometry, and a broader name made an unrelated `tile_bounds`, or a
# secondary geometry's `boundary_bbox`, the primary's answer (#738/#953/#1171).
_BBOX_STRUCT_FIELDS = frozenset({"xmin", "ymin", "xmax", "ymax"})

#: The struct shapes a GeoParquet 1.1 ``covering.bbox`` may point at, as ordered
#: tuples: the spec fixes the field order, so a membership test is the wrong
#: predicate here. ``core/validate.py`` derives its table from this one (#1035).
BBOX_COVERING_FIELD_ORDERS = (
    ("xmin", "ymin", "xmax", "ymax"),
    ("xmin", "ymin", "zmin", "xmax", "ymax", "zmax"),
)

#: What every "cannot declare a covering" message ends with.
BBOX_REWRITE_HINT = "Rewrite it in the spec's order: gpio add bbox --force IN.parquet OUT.parquet"


def bbox_field_order_is_covering_legal(field_names: Sequence[str]) -> bool:
    """Whether a bbox struct's field names, in this order, may carry a covering."""
    return tuple(field_names) in BBOX_COVERING_FIELD_ORDERS


def bbox_struct_child_names_match(names: Collection[str]) -> bool:
    """Whether a struct's children, **as spelled**, are a bbox column's four corners.

    The one predicate every "is this really a bbox column?" detector shares, so
    the answer cannot differ between the file path, the Arrow path and the
    streaming one (#1176). Case-sensitive on purpose: Parquet child names are
    matched as spelled, and no 1.1 ``covering`` may point at ``XMIN, YMIN, XMAX,
    YMAX`` (see :func:`bbox_covering_problem`). A struct spelled that way is
    therefore the user's own data -- a computed bbox moves aside rather than
    replacing it. Order is not checked here: ``add bbox --force`` must still
    replace an Overture-order ``xmin, xmax, ymin, ymax`` column in place, which
    is the repair :data:`BBOX_REWRITE_HINT` points at.
    """
    return not missing_bbox_struct_fields(names)


def missing_bbox_struct_fields(names: Collection[str]) -> list[str]:
    """The bbox corners a struct's children, as spelled, lack (sorted; empty: none).

    The other half of :func:`bbox_struct_child_names_match`, for a caller that
    names what is missing in its refusal.
    """
    return sorted(_BBOX_STRUCT_FIELDS - set(names))


def is_bbox_struct_field(field: pa.Field) -> bool:
    """Whether an Arrow field is a struct whose children are a bbox's four corners."""
    import pyarrow as pa

    return pa.types.is_struct(field.type) and bbox_struct_child_names_match(
        [child.name for child in field.type]
    )


def bbox_covering_problem(
    column: str, field_names: Sequence[str] | None, field_types: Sequence[str] | None = None
) -> str | None:
    """Why a ``covering`` may not name ``column``, or None when it may.

    ``field_names`` are the struct's children in schema order (None for a
    non-struct); ``field_types``, when given, are their type names, and a
    corner that is not FLOAT/DOUBLE is refused too -- ``check spec`` rejects
    both shapes, and a writer that declares what the validator rejects is the
    defect (#1035).
    """
    if field_names is None:
        return f"bbox column '{column}' is not a struct"
    if not bbox_field_order_is_covering_legal(field_names):
        legal = " or ".join(", ".join(order) for order in BBOX_COVERING_FIELD_ORDERS)
        return (
            f"bbox column '{column}' has struct fields {list(field_names)}; a GeoParquet 1.1 "
            f"'covering' may only point at {legal}, in that order"
        )
    if field_types is not None:
        floats = {"float", "double", "float32", "float64", "halffloat", "float16"}
        wrong = [
            f"{n}: {t}"
            for n, t in zip(field_names, field_types, strict=True)
            if str(t).lower() not in floats
        ]
        if wrong:
            return f"bbox column '{column}' has non-float fields ({', '.join(wrong)}); a 'covering' requires FLOAT or DOUBLE"
    return None


def _unambiguous_field(schema: pa.Schema, name: str) -> pa.Field | None:
    """``schema``'s field called ``name``, or None when there is not exactly one.

    Parquet allows a schema to hold one name twice, and ``Schema.field(name)``
    raises ``KeyError: 'Column <name> does not exist in schema'`` when it does --
    while ``name in schema.names`` says True. Every reader of a bbox column's
    field asked the second question and then the first, so a table with two
    ``bbox`` columns aborted the write instead of declining to declare over an
    ambiguous name.

    Absent and duplicated answer the same here on purpose: a covering names a
    column, and a name that resolves to two of them names neither.
    """
    indices = schema.get_all_field_indices(name)
    return schema.field(indices[0]) if len(indices) == 1 else None


def arrow_bbox_covering_problem(column: str, field: pa.Field) -> str | None:
    """:func:`bbox_covering_problem` read off an Arrow field."""
    import pyarrow as pa

    if not pa.types.is_struct(field.type):
        return bbox_covering_problem(column, None)
    children = list(field.type)
    return bbox_covering_problem(
        column, [c.name for c in children], [str(c.type) for c in children]
    )


@cache
def _note_undeclarable_bbox_column(column: str, problem: str, declared: bool) -> None:
    """Warn once per process per (column, reason): a partition write says it once, not N times."""
    if declared:
        warn(
            f"Dropping the 'covering' declared over '{column}': {problem}. "
            f"gpio check spec rejects it, so it is not written. {BBOX_REWRITE_HINT}"
        )
    else:
        warn(f"Not declaring a 'covering' over '{column}': {problem}. {BBOX_REWRITE_HINT}")


def _declared_bbox_column(geo_meta: object) -> str | None:
    """The column the primary column's ``covering.bbox`` names, if the block is well-formed.

    ``primary_column`` is whatever someone else's file put there, and it is the
    key this indexes ``columns`` by, so a list or an object value raised
    ``unhashable type`` rather than reporting the truth: a block that names no
    primary entry declares no covering (#947).
    """
    if not isinstance(geo_meta, dict):
        return None
    primary = geo_meta.get("primary_column")
    if not isinstance(primary, str):
        return None
    columns = geo_meta.get("columns")
    col_meta = columns.get(primary) if isinstance(columns, dict) else None
    covering = col_meta.get("covering") if isinstance(col_meta, dict) else None
    return _covering_column(covering.get("bbox")) if isinstance(covering, dict) else None


def carried_covering_as_provenance(
    table_metadata: dict | None, geometry_column: str | None, column_names: Collection[str]
) -> dict | None:
    """The ``covering`` an Arrow table's own ``geo`` block declares, as provenance.

    A ``write_from_table`` is handed a table and ``original_metadata=None`` --
    deliberately, because the input's *stats* no longer describe whatever the
    caller did to the table in between. The ``covering`` is not a stat, though: it
    says which column holds the geometry's envelope, and nothing else in a write
    can establish that for a column not named exactly
    :data:`SELF_EVIDENT_BBOX_COLUMN`. Three of the four strategies therefore lost
    a GDAL ``geometry_bbox`` covering, and the one
    ``Table.add_bbox(column_name="bounds")`` records, on ``Table.write`` (#1172).

    The answer goes back in through ``custom_metadata["covering"]``, which is the
    channel a caller that *computed* a bbox column already uses -- the one kind of
    provenance a write trusts. It is provenance only: the struct-shape gate still
    runs downstream, so this cannot smuggle an illegal covering past it.

    Entries naming a column the table no longer has are dropped, the same
    question :func:`prune_geo_metadata_to_columns` answers for a file write: a
    projection can take the bbox column (or a spatial-index column) with it, and
    an entry pointing at nothing is metadata every reader and ``gpio check spec``
    rejects.

    Returns ``None`` when there is nothing to carry.
    """
    geo_meta = sanitized_carried_geo(table_metadata)
    columns = geo_meta.get("columns")
    if not isinstance(columns, dict):
        return None
    col_meta = columns.get(geometry_column or geo_meta.get("primary_column"))
    covering = col_meta.get("covering") if isinstance(col_meta, dict) else None
    if not isinstance(covering, dict):
        return None
    present = set(column_names)
    carried = {key: entry for key, entry in covering.items() if _covering_column(entry) in present}
    return carried or None


def may_hold_a_self_evident_bbox(output_columns: Collection[str] | None) -> bool:
    """Whether an output with these column names could carry a bbox column
    :func:`bbox_column_to_declare` declares by its name alone.

    ``None`` (the columns are not known) could. A write that knows its output
    names asks this to skip the schema probe the declare step would otherwise
    run; the name rule stays here, beside the gate it shortcuts.
    """
    return output_columns is None or SELF_EVIDENT_BBOX_COLUMN in output_columns


def bbox_column_to_declare(
    schema: pa.Schema, geo_meta: dict | None = None, *, verbose: bool = False
) -> str | None:
    """The bbox column this write may put in a ``covering``, or None. The one gate.

    Every write path that invents a covering asks here, so the rule has one
    spelling: the struct's field order and types decide (see
    :func:`bbox_covering_problem`), never the output version. Three rules follow:

    1. A carried bbox column whose struct the spec forbids (Overture's
       ``xmin, xmax, ymin, ymax``) is written through unchanged and left
       undeclared, with one warning per process pointing at
       ``gpio add bbox --force``, which rewrites it in the spec's order.
    2. An input that already declares a covering over such a struct is written
       *without* that entry, with a warning saying it was dropped: gpio does not
       write a file its own validator rejects, even when the input asked it to.
       A legal declared covering is carried unchanged (#738).
    3. The output version is left alone. Issue #1035 offered "keep a 1.0 input at
       1.0 so no covering is written" instead; that was measured and rejected,
       because auto mode's 1.x -> 1.1 is shared policy and, more to the point, a
       clean 1.1 input with an Overture-order column and no covering is valid
       and was still turned invalid by every rewrite -- version preservation
       cannot help a file already at the version it will be written at.

    Reordering the struct during a rewrite was also rejected: that is a data
    change, and the rewrite fixes promise not to make one.

    Provenance comes only from the PRIMARY column's own ``covering``. With
    none, only the exact conventional name (:data:`SELF_EVIDENT_BBOX_COLUMN`)
    may be declared -- the #738 policy. The broader read-side matching
    (``bounds``, ``extent``, ``*_bbox``) used to leak in here, so a
    multi-geometry file whose secondary ``boundary`` travelled with a
    ``boundary_bbox`` struct got that struct declared as the *primary*'s
    covering: a Point column advertising a Polygon column's extents (#953).
    The exact name is no exception when another column's own covering already
    claims it.
    """
    declared = _declared_bbox_column(geo_meta)
    name = declared if declared in schema.names else _self_evident_bbox_column(schema, verbose)
    if name is None:
        return None
    if name != declared and _bbox_claimed_by_another_column(geo_meta, name):
        if verbose:
            debug(f"Not declaring '{name}' for the primary: another column's covering names it")
        return None
    field = _unambiguous_field(schema, name)
    if field is None:
        if verbose:
            debug(f"Not declaring '{name}': the schema holds that name more than once")
        return None
    problem = arrow_bbox_covering_problem(name, field)
    if problem is None:
        return name
    _note_undeclarable_bbox_column(name, problem, name == declared)
    return None


def build_bbox_covering(column: str) -> dict:
    """The ``covering.bbox`` entry describing ``column``'s four struct fields.

    One constructor so every writer emits the same shape. Callers must only use
    it for a column whose values are known to bound the geometry -- one gpio
    computed in this write, or one the input's own metadata already declared.
    A covering derived from a column *name* asserts a relationship nothing
    checked, and readers prune on it (#738).
    """
    return {axis: [column, axis] for axis in ("xmin", "ymin", "xmax", "ymax")}


#: The only column name a writer will treat as self-evidently the geometry's
#: bounding box. `covering` asserts that a column's values bound the geometry,
#: and a name is weak evidence -- but `bbox`, as a struct of xmin/ymin/xmax/ymax,
#: is the universal GeoParquet convention and is what every 1.0-era writer
#: emitted before `covering` existed. Broader matching (`bounds`, `extent`,
#: `*_bbox`) let an unrelated `tile_bounds` column become the declared covering,
#: so readers pruned away rows that genuinely matched; those names now require
#: explicit provenance (#738).
SELF_EVIDENT_BBOX_COLUMN = "bbox"


def _bbox_claimed_by_another_column(geo_meta: object, column: str) -> bool:
    """Whether a non-primary column's ``covering.bbox`` names ``column`` (#953).

    That column is the secondary geometry's envelope, whatever its name, so the
    primary must not claim it on name alone.
    """
    if not isinstance(geo_meta, dict) or not isinstance(geo_meta.get("columns"), dict):
        return False
    primary = geo_meta.get("primary_column")
    for name, col_meta in geo_meta["columns"].items():
        if name == primary or not isinstance(col_meta, dict):
            continue
        covering = col_meta.get("covering")
        if isinstance(covering, dict) and _covering_column(covering.get("bbox")) == column:
            return True
    return False


def _self_evident_bbox_column(schema: pa.Schema, verbose: bool = False) -> str | None:
    """The one column a writer may declare, or a reader may read, with no provenance.

    Only the exact :data:`SELF_EVIDENT_BBOX_COLUMN` qualifies (#738); the
    struct-shape gate, and the "another column's covering claims it" check,
    stay with the caller. The broader conventional names (``bounds``,
    ``extent``, any ``*_bbox`` suffix) matched in a sibling detector until
    #1171: a ``boundary_bbox`` that name-matches is the *secondary*
    ``boundary`` column's envelope, not the primary's (#953), so both the
    declare side (:func:`bbox_column_to_declare`) and the read side
    (:func:`~geoparquet_io.core.bbox_structure.check_bbox_structure`,
    :func:`~geoparquet_io.core.arrow_geo_metadata._detect_bbox_column_from_table`)
    now come here.
    """

    field = _unambiguous_field(schema, SELF_EVIDENT_BBOX_COLUMN)
    if field is None or not is_bbox_struct_field(field):
        return None
    if verbose:
        debug(f"Found conventional bbox column in table: {SELF_EVIDENT_BBOX_COLUMN}")
    return SELF_EVIDENT_BBOX_COLUMN


# =============================================================================
# Geometry Type Helpers
# =============================================================================


def _get_geometry_type_name(code: int) -> str | None:
    """WKB type code -> spec name ("Point Z" for 1001), or None for no spec type.

    Code 0, an unknown base (8, 1008) or an unknown dimension (4001) is None:
    a ``geometry_types`` list holds only spec values, so callers drop it.
    """
    dimension, base_type = divmod(int(code), 1000)
    base_name = _GEOMETRY_TYPE_CODES.get(base_type)
    suffix = _DIMENSION_SUFFIXES.get(dimension)
    if base_name is None or suffix is None:
        return None
    return base_name + suffix


def geometry_type_name(raw) -> str | None:
    """A geometry type as the spec names it, from a WKB code or a DuckDB string.

    ``1003``, ``'polygon_z'``, ``'POLYGON Z'`` and ``'Polygon Z'`` all read as
    ``'Polygon Z'`` (DuckDB's ``ST_`` prefix is dropped); code 0 and anything
    that is no spec type (``'TRIANGLE Z'``) read as None, on every path. The
    one reader of the Parquet geospatial statistics' ``geo_types`` and of
    DuckDB's ``ST_GeometryType``.
    """
    if isinstance(raw, int) and not isinstance(raw, bool):
        return _get_geometry_type_name(raw)
    if not isinstance(raw, str):
        return None
    text = raw.replace("_", " ").strip().upper().removeprefix("ST ")
    for suffix in (" ZM", " Z", " M", ""):
        if suffix and not text.endswith(suffix):
            continue
        base = _SPEC_TYPE_BY_UPPER.get(text[: len(text) - len(suffix)].strip())
        if base:
            return base + suffix
    return None
