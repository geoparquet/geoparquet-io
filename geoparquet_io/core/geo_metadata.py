"""
GeoParquet metadata handling functions.

This module provides functions for parsing, creating, and applying GeoParquet
metadata to Arrow tables and Parquet files. It handles both GeoParquet 1.x and
2.0 formats, including bbox covering metadata and native geometry types.

Usage in core modules:
    from geoparquet_io.core.geo_metadata import (
        parse_geo_metadata,
        create_geo_metadata,
    )

Note: This module uses lazy imports for functions from other core modules
to avoid circular dependencies.
"""

from __future__ import annotations

import copy
import json
from functools import lru_cache
from typing import TYPE_CHECKING

import duckdb

from geoparquet_io.core.duckdb_utils import (
    _geoarrow_coord_exprs,
    _get_query_column_type,
    quote_identifier,
)
from geoparquet_io.core.logging_config import debug, warn

if TYPE_CHECKING:
    from collections.abc import Collection

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

# =============================================================================
# Geometry Type Mappings
# =============================================================================

# WKB geometry type codes to GeoParquet base names (2D types)
_GEOMETRY_TYPE_CODES = {
    0: "Unknown",
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

# geoarrow's Dimensions enum (XY=1, XYZ=2, XYM=3, XYZM=4) to the WKB type
# code's dimensional modifier above. UNSPECIFIED (-1) and UNKNOWN (0) have no
# entry and fall back to 2D, the only reading that claims nothing extra.
_GEOARROW_DIMENSION_CODES = {1: 0, 2: 1, 3: 2, 4: 3}


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
    where = f" of {source}" if source else ""
    warn(f"Ignoring malformed 'geo' metadata on the input{where}: {detail}")


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


def decode_carried_geo(raw):
    """Decode a raw carried ``geo`` value (bytes or str) to JSON, or None.

    The decode step can fail one call before :func:`sanitize_geo_metadata` ever
    sees a shape: bytes that are not UTF-8 crash ``.decode`` and a truncated
    payload crashes ``json.loads``. Both get the malformed-block treatment --
    the value is dropped so fresh metadata gets built, and one warning names
    the cause -- instead of aborting the write with a raw decoder traceback.

    Already-decoded values (a dict handed over directly) pass through untouched.
    """
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if isinstance(raw, str):
            return json.loads(raw)
    except UnicodeDecodeError:
        _emit_malformed_geo_warning("the value is not valid UTF-8; fresh metadata will be written")
        return None
    except json.JSONDecodeError:
        _emit_malformed_geo_warning("the value is not valid JSON; fresh metadata will be written")
        return None
    return raw


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


def _sanitize_column_entry(name: str, entry: dict) -> tuple[dict, list[str]]:
    """Drop wrong-typed carried values from one column entry; never mutates.

    Returns ``entry`` itself when every value is acceptable, else a cleaned
    shallow copy, plus the problems to warn about.
    """
    problems: list[str] = []
    cleaned = entry
    for key, (accepts, expected) in _COLUMN_VALUE_CHECKS.items():
        if key in entry and not accepts(entry[key]):
            problems.append(
                f"'{key}' on column '{name}' is "
                f"{_article(_json_type_name(entry[key]))}, expected {expected}"
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


def sanitize_geo_metadata(geo_meta):
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

    This is the single shape check every write-path reader of the raw block goes
    through. The read-only readers (``parse_geo_metadata``,
    ``crs_utils.parse_geo_metadata_from_schema``,
    ``duckdb_metadata.get_geo_metadata``) deliberately do *not* sanitize:
    ``gpio check`` has to see the file as it really is.

    Args:
        geo_meta: A decoded ``geo`` block, or None.

    Returns:
        ``geo_meta`` itself when it is already well-shaped, a repaired shallow
        copy when it is not, or None when nothing usable is left.
    """
    if geo_meta is None:
        return None

    if not isinstance(geo_meta, dict):
        _emit_malformed_geo_warning(
            f"the block is {_article(_json_type_name(geo_meta))}, expected an object"
        )
        return None

    problems: list[str] = []
    cleaned = geo_meta

    primary = geo_meta.get("primary_column")
    dropped_primary = "primary_column" in geo_meta and not isinstance(primary, str)
    if dropped_primary:
        problems.append(
            f"'primary_column' is {_article(_json_type_name(primary))}, expected a string"
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
        _emit_malformed_geo_warning(problem)
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


def sanitized_carried_geo(metadata) -> dict:
    """The ``geo`` block on a Parquet/Arrow metadata mapping, sanitized, or ``{}``.

    The three steps every write-path reader of a carried block repeats -- find
    the key, decode it, check its shape -- in one place, so a reader is one call
    away from the rule rather than three lines of its own (#947). Both key
    spellings are accepted: PyArrow hands back ``bytes`` keys and DuckDB's KV
    metadata ``str`` ones, and several callers see both.

    Returns ``{}``, never ``None``, so callers can index the result directly;
    a block that sanitizing leaves unusable is indistinguishable from an absent
    one, which is exactly how #883 decided a malformed block should read.
    """
    if not metadata:
        return {}
    for key in (b"geo", "geo"):
        if key in metadata:
            cleaned = sanitize_geo_metadata(decode_carried_geo(metadata[key]))
            return cleaned if isinstance(cleaned, dict) else {}
    return {}


def carried_geometry_column(geo_meta, column_names) -> str | None:
    """The column a *sanitized* block names as primary, else the one the schema shows.

    Sanitizing *drops* a malformed ``primary_column`` (and repairs it only from
    a single surviving column), so a block reaches a reader without one.
    Defaulting to the literal string ``"geometry"`` there is silently wrong on a
    file whose column is called ``geom`` -- the write then names a column that
    is not in the table (#887 review). ``column_names`` is the schema to ask
    instead; the caller supplies the last-resort default, if it wants one.
    """
    from geoparquet_io.core.geometry_detection import detect_geometry_column_from_names

    primary = geo_meta.get("primary_column") if isinstance(geo_meta, dict) else None
    if isinstance(primary, str) and primary:
        return primary
    return detect_geometry_column_from_names(column_names)


def _article(type_name: str) -> str:
    """``"a list"`` / ``"an object"`` -- correct article for a JSON type name."""
    if type_name == "null":
        return "null"
    return f"{'an' if type_name[0] in 'aeiou' else 'a'} {type_name}"


# =============================================================================
# Metadata Parsing Functions
# =============================================================================


def parse_geo_metadata(metadata: dict | None, verbose: bool = False) -> dict | None:
    """
    Parse GeoParquet metadata from Parquet file metadata.

    Extracts and decodes the 'geo' key from Parquet metadata bytes.

    Args:
        metadata: Parquet file metadata dict with bytes keys
        verbose: Print verbose output

    Returns:
        Parsed geo metadata dict, or None if not present or invalid
    """
    if not metadata or b"geo" not in metadata:
        return None

    try:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        if verbose:
            debug("\nParsed geo metadata:")
            debug(json.dumps(geo_meta, indent=2))
        return geo_meta
    except json.JSONDecodeError:
        if verbose:
            warn("Failed to parse geo metadata as JSON")
        return None


def _parse_existing_geo_metadata(original_metadata: dict | None) -> dict | None:
    """
    Parse existing geo metadata from original parquet metadata.

    This is a write-path reader: what it returns is indexed into while building
    the output block, so it goes through :func:`sanitize_geo_metadata` (#771).

    Args:
        original_metadata: Original parquet file metadata dict

    Returns:
        Parsed geo metadata dict, or None if not present or not usable
    """
    if not original_metadata or b"geo" not in original_metadata:
        return None
    return sanitize_geo_metadata(decode_carried_geo(original_metadata[b"geo"]))


# =============================================================================
# Metadata Initialization and Building
# =============================================================================


def _initialize_geo_metadata(geo_meta: dict | None, geom_col: str, version: str = "1.1.0") -> dict:
    """
    Initialize or upgrade geo metadata structure.

    Creates a minimal valid GeoParquet metadata structure if none exists,
    or ensures existing metadata has the required structure.

    Args:
        geo_meta: Existing geo metadata dict or None
        geom_col: Name of the geometry column
        version: GeoParquet version string (e.g., "1.0.0", "1.1.0", "2.0.0")

    Returns:
        Initialized geo metadata structure
    """
    if not geo_meta:
        return {"version": version, "primary_column": geom_col, "columns": {geom_col: {}}}

    # Set the specified version
    geo_meta["version"] = version
    # `primary_column` is required by every version of the spec. A carried block
    # can arrive without one -- absent in the input, or dropped by
    # `sanitize_geo_metadata` because it was not a string (#771) -- and the
    # output would then be invalid. Matches `write_strategies.base`.
    if "primary_column" not in geo_meta:
        geo_meta["primary_column"] = geom_col
    if "columns" not in geo_meta:
        geo_meta["columns"] = {}
    if geom_col not in geo_meta["columns"]:
        geo_meta["columns"][geom_col] = {}

    return geo_meta


def _add_bbox_covering(
    geo_meta: dict, geom_col: str, bbox_info: dict | None, verbose: bool
) -> None:
    """
    Add bbox covering metadata to geometry column.

    Updates geo_meta in place with bbox covering information that points
    to the bbox struct column fields.

    Args:
        geo_meta: Geo metadata dict to update
        geom_col: Name of the geometry column
        bbox_info: Result from check_bbox_structure, or None
        verbose: Print verbose output
    """
    if not bbox_info or not bbox_info.get("has_bbox_column"):
        return

    if "covering" not in geo_meta["columns"][geom_col]:
        geo_meta["columns"][geom_col]["covering"] = {}

    geo_meta["columns"][geom_col]["covering"]["bbox"] = {
        "xmin": [bbox_info["bbox_column_name"], "xmin"],
        "ymin": [bbox_info["bbox_column_name"], "ymin"],
        "xmax": [bbox_info["bbox_column_name"], "xmax"],
        "ymax": [bbox_info["bbox_column_name"], "ymax"],
    }
    if verbose:
        debug(f"Added bbox covering metadata for column '{bbox_info['bbox_column_name']}'")


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

    Never mutates its input. The assembled metadata still aliases the caller's
    column dicts through the shallow copy in ``_initialize_geo_metadata``, and
    partition loops reuse one ``original_metadata`` dict across many writes, so
    popping in place would strip the shared dict permanently and silently cost a
    later 1.1 write its covering.
    """
    if covering_supported(version):
        return geo_meta

    columns = geo_meta.get("columns")
    if not isinstance(columns, dict):
        return geo_meta
    if not any(isinstance(col, dict) and "covering" in col for col in columns.values()):
        return geo_meta

    stripped = {}
    for col_name, col_meta in columns.items():
        if isinstance(col_meta, dict) and "covering" in col_meta:
            col_meta = {k: v for k, v in col_meta.items() if k != "covering"}
            if verbose:
                debug(
                    f"Dropped 1.1-only covering metadata for column '{col_name}' "
                    f"(version {version})"
                )
        stripped[col_name] = col_meta

    result = dict(geo_meta)
    result["columns"] = stripped
    return result


def _add_custom_covering(
    geo_meta: dict, geom_col: str, custom_metadata: dict | None, verbose: bool
) -> None:
    """
    Add custom covering metadata (e.g., H3, S2).

    Updates geo_meta in place with custom covering information for spatial
    indices like H3 or S2.

    Args:
        geo_meta: Geo metadata dict to update
        geom_col: Name of the geometry column
        custom_metadata: Dict with custom metadata including 'covering' key
        verbose: Print verbose output
    """
    if not custom_metadata or "covering" not in custom_metadata:
        return

    if "covering" not in geo_meta["columns"][geom_col]:
        geo_meta["columns"][geom_col]["covering"] = {}

    geo_meta["columns"][geom_col]["covering"].update(custom_metadata["covering"])
    if verbose:
        for key in custom_metadata["covering"]:
            debug(f"Added {key} covering metadata")


#: Per-column geo metadata keys that are derived from the data itself and are
#: therefore invalidated by anything that changes which rows/coordinates are
#: written (row filters, reprojection, per-partition splits, multi-file merges).
DERIVED_STAT_KEYS = ("bbox", "geometry_types")

#: Sentinel returned by a rewrite callback to mean "drop the geo key entirely".
_DROP_GEO = object()


def _decode_geo_value(raw):
    """Decode a KV ``geo`` value (bytes/str/dict) to a dict, or ``None``."""
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if isinstance(raw, str):
            raw = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return raw if isinstance(raw, dict) else None


def _encode_geo_value(geo_dict: dict, like):
    """Re-encode ``geo_dict`` in the same form (bytes/str/dict) as ``like``."""
    if isinstance(like, bytes):
        return json.dumps(geo_dict).encode("utf-8")
    if isinstance(like, str):
        return json.dumps(geo_dict)
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
    requires — nothing recomputes a secondary column's — and DuckDB then refuses
    to open the file at all. ``None`` strips every column, which is right for a
    merge, whose carried stats UNDER-cover every column of the output.

    ``recomputed_columns`` names the stripped columns the caller's write path
    will fill back in — on every file-write path that is the primary geometry
    column and nothing else. Removing ``geometry_types`` is a way of *asking*
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

    def _drop(geo_dict: dict) -> dict:
        for col_name, col_meta in (geo_dict.get("columns") or {}).items():
            if columns is not None and col_name not in columns:
                continue
            if isinstance(col_meta, dict) and col_meta.get("edges", "planar") != "planar":
                del col_meta["edges"]
        return geo_dict

    return _rewrite_geo_metadata(metadata, _drop)


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
                col_meta["geometry_types"] = _compute_geometry_types(table, name, verbose)
            if "bbox" not in col_meta:
                bbox = _compute_bbox_from_data(table, name, verbose)
                if bbox:
                    col_meta["bbox"] = bbox
        return geo_dict

    return _rewrite_geo_metadata(metadata, _fill)


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


def _prune_coverings(col_meta, columns: set[str]) -> None:
    """Drop ``covering`` entries pointing at columns not in ``columns``."""
    covering = col_meta.get("covering") if isinstance(col_meta, dict) else None
    if not isinstance(covering, dict):
        return
    for key in [k for k, v in covering.items() if _covering_column(v) not in columns]:
        del covering[key]
    if not covering:
        del col_meta["covering"]


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
    # already shared out. `_rewrite_geo_metadata` only calls this with a decoded
    # object -- a block that is not one never gets here -- so sanitizing a dict
    # hands back a dict.
    declared_primary = "primary_column" in geo_dict
    geo_dict = sanitize_geo_metadata(geo_dict)

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

    for col_meta in col_entries.values():
        _prune_coverings(col_meta, columns)
    return geo_dict


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


def create_geo_metadata(
    original_metadata: dict | None,
    geom_col: str,
    bbox_info: dict | None,
    custom_metadata: dict | None = None,
    verbose: bool = False,
    version: str = "1.1.0",
    edges: str | None = None,
) -> dict:
    """
    Create or update GeoParquet metadata with spatial index covering information.

    Builds a complete GeoParquet metadata structure from existing metadata
    and new covering information.

    Args:
        original_metadata: Original parquet metadata dict
        geom_col: Name of the geometry column
        bbox_info: Result from check_bbox_structure
        custom_metadata: Optional dict with custom metadata (e.g., H3 info)
        verbose: Whether to print verbose output
        version: GeoParquet version string (e.g., "1.0.0", "1.1.0", "2.0.0")
        edges: Edge interpretation, "spherical" or "planar" (default None = planar).
               Use "spherical" for data from BigQuery or other S2-based sources.

    Returns:
        Updated geo metadata dict
    """
    geo_meta = _parse_existing_geo_metadata(original_metadata)
    geo_meta = _initialize_geo_metadata(geo_meta, geom_col, version=version)

    # Add encoding if not present (required by GeoParquet spec)
    if "encoding" not in geo_meta["columns"][geom_col]:
        geo_meta["columns"][geom_col]["encoding"] = "WKB"

    # Add edges if specified (for spherical geometry from BigQuery, etc.)
    if edges:
        geo_meta["columns"][geom_col]["edges"] = edges
        # When spherical, orientation should be counterclockwise per GeoParquet spec
        if edges == "spherical":
            geo_meta["columns"][geom_col]["orientation"] = "counterclockwise"

    # Add bbox covering if needed
    _add_bbox_covering(geo_meta, geom_col, bbox_info, verbose)

    # Add custom covering if needed
    _add_custom_covering(geo_meta, geom_col, custom_metadata, verbose)

    # Add any top-level custom metadata
    if custom_metadata:
        for key, value in custom_metadata.items():
            if key != "covering":
                geo_meta[key] = value

    return strip_unsupported_covering(geo_meta, version, verbose)


# =============================================================================
# SQL-based Metadata Computation
# =============================================================================


def _get_query_columns(con, query: str) -> list[str]:
    """
    Get column names from a query without executing it fully.

    Uses LIMIT 0 to get schema information efficiently.

    Args:
        con: DuckDB connection
        query: SQL SELECT query

    Returns:
        Column names from the query result
    """
    describe_query = f"SELECT * FROM ({query}) AS __subq LIMIT 0"
    result = con.execute(describe_query)
    return [col[0] for col in result.description]


def compute_bbox_via_sql(
    con,
    query: str,
    geometry_column: str,
) -> list[float] | None:
    """
    Compute bounding box from query using DuckDB spatial functions.

    Uses ST_XMin/YMin/XMax/YMax aggregate functions to compute the
    overall bounding box of all geometries.

    Args:
        con: DuckDB connection with spatial extension loaded
        query: SQL query containing geometry column
        geometry_column: Name of geometry column

    Returns:
        [xmin, ymin, xmax, ymax] or None if query returns no rows
        or geometry column not in query
    """
    # Check if geometry column exists in query result
    try:
        columns = _get_query_columns(con, query)
        if geometry_column not in columns:
            return None
    except (duckdb.Error, RuntimeError, ValueError, AttributeError):
        # If we can't determine schema, return None rather than failing
        return None

    quoted_geom = quote_identifier(geometry_column)

    # GeoArrow native types (STRUCT(x DOUBLE, y DOUBLE)[N]) cannot be passed to
    # ST_XMin directly. Detect at runtime and use UNNEST to extract coordinates.
    col_type = _get_query_column_type(con, query, geometry_column) or ""
    if "STRUCT" in col_type:
        # bracket_depth = col_type.count("[]"): 0=point, 1=linestring/multipoint,
        # 2=polygon/multilinestring, 3=multipolygon. Maps directly to _GEOARROW_FLATTEN_DEPTH
        # (flatten_count = bracket_depth - 1 for non-point).
        _depth_to_encoding = {0: "point", 1: "linestring", 2: "polygon", 3: "multipolygon"}
        enc = _depth_to_encoding.get(col_type.count("[]"), "linestring")
        xmin_e, ymin_e, xmax_e, ymax_e, _, _ = _geoarrow_coord_exprs(quoted_geom, enc)
        bbox_query = f"""
            SELECT
                MIN({xmin_e}) as xmin,
                MIN({ymin_e}) as ymin,
                MAX({xmax_e}) as xmax,
                MAX({ymax_e}) as ymax
            FROM ({query})
            WHERE NOT isnan({xmax_e}) AND NOT isnan({ymax_e})
        """
    else:
        bbox_query = f"""
            SELECT
                MIN(ST_XMin({quoted_geom})) as xmin,
                MIN(ST_YMin({quoted_geom})) as ymin,
                MAX(ST_XMax({quoted_geom})) as xmax,
                MAX(ST_YMax({quoted_geom})) as ymax
            FROM ({query})
        """
    result = con.execute(bbox_query).fetchone()

    if result and all(v is not None for v in result):
        return list(result)
    return None


def _fold_geo_stat_rows(rows) -> tuple[list[float] | None, list[str]]:
    """Fold ``(geom_type, xmin, ymin, xmax, ymax)`` rows into ``(bbox, types)``."""
    from geoparquet_io.core.common import _DUCKDB_TO_SPEC_TYPE, split_zm_suffix

    types: set[str] = set()
    extents: list[tuple[float, float, float, float]] = []
    for geom_type, xmin, ymin, xmax, ymax in rows:
        if geom_type:
            base, suffix = split_zm_suffix(geom_type)
            types.add(_DUCKDB_TO_SPEC_TYPE.get(base.upper(), base) + suffix)
        if None not in (xmin, ymin, xmax, ymax):
            extents.append((xmin, ymin, xmax, ymax))

    if not extents:
        return None, sorted(types)
    bbox = [
        min(e[0] for e in extents),
        min(e[1] for e in extents),
        max(e[2] for e in extents),
        max(e[3] for e in extents),
    ]
    return bbox, sorted(types)


def _geo_stats_unsupported(con, query: str, geometry_column: str) -> bool:
    """True when the combined per-type aggregation cannot run on this column.

    GeoArrow native types (``STRUCT(x DOUBLE, y DOUBLE)[N]``) support neither
    ``ST_GeometryType`` nor the shared aggregation, and a column that is not in
    the query result obviously cannot be aggregated. Both cases fall back to the
    single-stat helpers, which already handle them.
    """
    try:
        col_type = _get_query_column_type(con, query, geometry_column) or ""
        if "STRUCT" in col_type:
            return True
        return geometry_column not in _get_query_columns(con, query)
    except (duckdb.Error, RuntimeError, ValueError, AttributeError):
        return True


def compute_geo_stats_via_sql(
    con,
    query: str,
    geometry_column: str,
    need_bbox: bool = True,
    need_geometry_types: bool = True,
) -> tuple[list[float] | None, list[str]]:
    """Compute ``bbox`` and ``geometry_types`` in a SINGLE scan of ``query``.

    Both stats are aggregates over the same rows, so grouping by geometry type
    yields one small row per type carrying that type's extent — the union of
    which is the collection bbox. That replaces the two independent full scans
    the write strategies used to run, which matters because invalidating a
    carried bbox forces the (possibly expensive, e.g. ``ST_Transform``) query to
    be re-executed for it.

    Args:
        con: DuckDB connection with spatial extension loaded
        query: SQL query containing the geometry column
        geometry_column: Name of the geometry column
        need_bbox: Compute the bbox (``False`` returns ``None`` for it)
        need_geometry_types: Compute geometry types (``False`` returns ``[]``)

    Returns:
        ``(bbox_or_None, geometry_types)``
    """
    from geoparquet_io.core.common import compute_geometry_types_via_sql, zm_suffix_sql

    def _separately() -> tuple[list[float] | None, list[str]]:
        return (
            compute_bbox_via_sql(con, query, geometry_column) if need_bbox else None,
            compute_geometry_types_via_sql(con, query, geometry_column)
            if need_geometry_types
            else [],
        )

    if not (need_bbox and need_geometry_types):
        return _separately()
    if _geo_stats_unsupported(con, query, geometry_column):
        return _separately()

    quoted = quote_identifier(geometry_column)
    stats_query = f"""
        SELECT
            ST_GeometryType({quoted}) || {zm_suffix_sql(quoted)} AS geom_type,
            MIN(ST_XMin({quoted})) AS xmin,
            MIN(ST_YMin({quoted})) AS ymin,
            MAX(ST_XMax({quoted})) AS xmax,
            MAX(ST_YMax({quoted})) AS ymax
        FROM ({query})
        WHERE {quoted} IS NOT NULL
        GROUP BY 1
    """
    return _fold_geo_stat_rows(con.execute(stats_query).fetchall())


def compute_geometry_types_via_sql(
    con,
    query: str,
    geometry_column: str,
) -> list[str]:
    """
    Compute distinct geometry types from query using DuckDB.

    Delegates to the canonical dimension-aware implementation in
    ``geoparquet_io.core.common`` (lazy import: common imports this module
    at top level, so a module-level import here would be circular).

    Returns:
        List of spec geometry type names with dimension suffixes
        (e.g., ["Point", "LineString ZM"]) or empty list if column not in query
    """
    from geoparquet_io.core.common import compute_geometry_types_via_sql as _impl

    return _impl(con, query, geometry_column)


# What makes a column a bbox covering column: a conventional name, and the
# struct fields GeoParquet's "Bounding Box Columns" requires.
#
# The name rule is deliberately conservative — exact `bbox`/`bounds`/`extent`,
# or an explicit `_bbox` suffix. A bare `endswith(("bbox","bounds","extent"))`
# also swallows unrelated columns like `tile_bounds` or `parcel_extent`, and a
# `covering` is an assertion that those values bound the geometry. gpio cannot
# verify that from a name, and a covering pointing at unrelated values makes
# readers prune away rows that genuinely match — strictly worse than declaring
# nothing (#738).
_BBOX_COLUMN_NAMES = frozenset({"bbox", "bounds", "extent"})
_BBOX_COLUMN_SUFFIXES = ("_bbox",)
_BBOX_STRUCT_FIELDS = frozenset({"xmin", "ymin", "xmax", "ymax"})


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


def declare_carried_bbox_column(
    con: duckdb.DuckDBPyConnection,
    query: str,
    col_meta: dict,
    verbose: bool,
    geoparquet_version: str,
    output_columns: list[str] | None = None,
) -> bool:
    """Declare a conventional ``bbox`` column the output carries but nothing declared.

    This is the 1.0 -> 1.1 upgrade path: a 1.0 file cannot declare a covering,
    so its bbox column arrives undeclared and would otherwise stay that way
    forever. Callers that *computed* a bbox column, or read a covering from the
    input, supply it through ``custom_metadata`` instead and never reach the
    branch below.

    Shared by both write paths — the metadata rewrite and the 2.0 no-rewrite
    fast path — so that the same input gets the same covering either way (#772).
    Mutates ``col_meta`` in place; returns whether a covering was added.

    ``output_columns``, when the caller already knows the output's column names,
    settles the common "no bbox column at all" case without paying for the
    schema probe below.
    """
    import pyarrow as pa

    if not covering_supported(geoparquet_version):
        if verbose:
            debug(f"Skipping 1.1-only covering metadata for version {geoparquet_version}")
        return False
    # Never override a covering that arrived with provenance.
    if isinstance(col_meta.get("covering"), dict) and "bbox" in col_meta["covering"]:
        return False

    name = SELF_EVIDENT_BBOX_COLUMN
    if output_columns is not None and name not in output_columns:
        return False
    schema = con.execute(f"SELECT * FROM ({query}) LIMIT 0").arrow().schema
    if name not in schema.names:
        return False
    field = schema.field(name)
    if not pa.types.is_struct(field.type):
        return False
    if not _BBOX_STRUCT_FIELDS.issubset({f.name for f in field.type}):
        return False

    col_meta.setdefault("covering", {})["bbox"] = build_bbox_covering(name)
    if verbose:
        debug(f"Declared the carried conventional bbox column '{name}'")
    return True


def _is_bbox_column_name(name: str) -> bool:
    """Whether ``name`` conventionally denotes a bbox covering column."""
    return name in _BBOX_COLUMN_NAMES or name.endswith(_BBOX_COLUMN_SUFFIXES)


def detect_bbox_column_from_schema(schema: pa.Schema, verbose: bool = False) -> str | None:
    """
    Detect a bbox covering column in an Arrow schema.

    Looks for a column with a conventional name (see ``_is_bbox_column_name``)
    that is a struct carrying the required xmin/ymin/xmax/ymax fields.

    Shared by every writer so that where a covering *is* written, the entry and
    the column it names cannot disagree. It is deliberately not used to decide
    *whether* to declare a covering: that requires knowing the values bound the
    geometry, which only the input's own metadata or a gpio-computed column can
    establish.

    Distinct from ``common._detect_bbox_column_from_table``, which consults the
    table's ``covering`` metadata first and only falls back to the naming
    convention. The two answer different questions and used to share a name.

    When several columns qualify, an exact ``bbox`` wins — it is the name gpio
    itself writes, so preferring it avoids picking some other file's
    ``centroid_bbox`` over the geometry's real envelope.

    Args:
        schema: PyArrow Schema to check
        verbose: Whether to print verbose output

    Returns:
        Name of bbox column if found, None otherwise
    """
    import pyarrow as pa

    matches = [
        field.name
        for field in schema
        if _is_bbox_column_name(field.name)
        and pa.types.is_struct(field.type)
        and _BBOX_STRUCT_FIELDS.issubset({f.name for f in field.type})
    ]
    if not matches:
        return None

    name = "bbox" if "bbox" in matches else matches[0]
    if verbose:
        debug(f"Found bbox column in table: {name}")
    return name


# =============================================================================
# Geometry Type Helpers
# =============================================================================


def _get_geometry_type_name(code: int) -> str:
    """
    Convert WKB geometry type code to GeoParquet geometry type name.

    Handles 2D types (0-7) and Z/M/ZM variants (1001-1007, 2001-2007, 3001-3007).

    Args:
        code: WKB geometry type code

    Returns:
        GeoParquet geometry type name (e.g., "Point", "Point Z", "Polygon ZM")
    """
    # Extract base type (0-7) and dimensional modifier (0, 1, 2, or 3)
    base_type = code % 1000
    dimension = code // 1000

    base_name = _GEOMETRY_TYPE_CODES.get(base_type, "Unknown")
    if base_name == "Unknown":
        return "Unknown"

    suffix = _DIMENSION_SUFFIXES.get(dimension, "")
    return base_name + suffix


# =============================================================================
# Geometry Data Computation
# =============================================================================


def _compute_geometry_types(table: pa.Table, geometry_column: str, verbose: bool) -> list[str]:
    """
    Compute geometry types from a geometry column using geoarrow.

    Analyzes the actual geometry data to determine the set of geometry types
    present in the column.

    Args:
        table: PyArrow Table containing the geometry column
        geometry_column: Name of the geometry column
        verbose: Whether to print verbose output

    Returns:
        List of GeoParquet geometry type names (e.g., ["Point", "Polygon"])
    """
    import geoarrow.pyarrow as ga
    import pyarrow.compute as pc

    # Skip for empty tables (geoarrow crashes on empty arrays)
    if table.num_rows == 0:
        return []

    try:
        geom_col = table.column(geometry_column)

        # Filter out NULL values to avoid geoarrow errors on invalid geometries
        # This handles cases where BigQuery returns NULL or empty geometries
        non_null_mask = pc.is_valid(geom_col)
        if pc.any(non_null_mask).as_py():
            geom_col = pc.filter(geom_col, non_null_mask)
        else:
            # All values are NULL
            return []

        # Skip if no valid geometries remain after filtering
        if len(geom_col) == 0:
            return []

        wkb_arr = ga.as_wkb(geom_col)
        types_struct = ga.unique_geometry_types(wkb_arr)

        # Extract geometry type codes from struct array. geoarrow reports the
        # base type and the dimensions separately, so the two are recombined
        # into a WKB code -- reading `geometry_type` alone dropped the " Z" /
        # " M" / " ZM" the spec makes part of the type name (#892).
        type_codes = geoarrow_wkb_codes(types_struct)

        # Map codes to GeoParquet standard names (avoid duplicates)
        type_names = []
        for code in type_codes:
            name = _get_geometry_type_name(code)
            if name not in type_names:
                type_names.append(name)

        if verbose:
            debug(f"Computed geometry_types from data: {type_names}")
        return type_names

    except Exception as e:
        # Catch all exceptions including geoarrow C++ errors
        # (e.g., "Expected valid geometry type code but found 0")
        if verbose:
            debug(f"Could not compute geometry_types: {e}")
        # Return empty list as fallback (allowed by spec - means any type)
        return []


def _compute_bbox_from_data(
    table: pa.Table, geometry_column: str, verbose: bool
) -> list[float] | None:
    """
    Compute bounding box from geometry column data.

    Uses geoarrow to compute the overall bounding box of all geometries
    in the column.

    Args:
        table: PyArrow Table containing the geometry column
        geometry_column: Name of the geometry column
        verbose: Whether to print verbose output

    Returns:
        [xmin, ymin, xmax, ymax] or None if computation fails
    """
    import geoarrow.pyarrow as ga
    import pyarrow.compute as pc

    # Skip for empty tables
    if table.num_rows == 0:
        return None

    try:
        geom_col = table.column(geometry_column)

        # Filter out NULL values to avoid geoarrow errors on invalid geometries
        non_null_mask = pc.is_valid(geom_col)
        if pc.any(non_null_mask).as_py():
            geom_col = pc.filter(geom_col, non_null_mask)
        else:
            # All values are NULL
            return None

        # Skip if no valid geometries remain after filtering
        if len(geom_col) == 0:
            return None

        wkb_arr = ga.as_wkb(geom_col)
        box_arr = ga.box(wkb_arr)

        # Combine chunks and get storage (underlying struct array)
        combined = box_arr.combine_chunks()
        storage = combined.storage

        # Extract struct fields and compute min/max
        xmin = pc.min(pc.struct_field(storage, "xmin")).as_py()
        ymin = pc.min(pc.struct_field(storage, "ymin")).as_py()
        xmax = pc.max(pc.struct_field(storage, "xmax")).as_py()
        ymax = pc.max(pc.struct_field(storage, "ymax")).as_py()

        if all(v is not None for v in [xmin, ymin, xmax, ymax]):
            if verbose:
                debug(f"Computed bbox from data: [{xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f}]")
            return [xmin, ymin, xmax, ymax]

    except Exception as e:
        # Catch all exceptions including geoarrow C++ errors
        if verbose:
            debug(f"Could not compute bbox: {e}")

    return None
