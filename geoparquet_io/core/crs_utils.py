"""
CRS (Coordinate Reference System) utilities for GeoParquet files.

This module provides functions for extracting, parsing, and validating
CRS information from GeoParquet files and other spatial formats.
"""

import json
import os
import re
from functools import lru_cache
from typing import Any

from geoparquet_io.core.duckdb_utils import _escape_sql_string, quote_identifier, sql_path
from geoparquet_io.core.logging_config import debug, warn


class _CrsAbsent:
    """Type of :data:`CRS_ABSENT`. Singleton, falsy, with a readable repr."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __bool__(self) -> bool:
        # Call sites guard with ``if crs:`` all over the codebase; an absent CRS
        # must stay falsy so substituting the sentinel for None cannot flip one.
        return False

    def __repr__(self) -> str:
        return "CRS_ABSENT"


#: Marker for a geometry column that has **no** ``crs`` key at all.
#:
#: The GeoParquet spec gives the two shapes different meanings: an omitted
#: ``crs`` defaults to OGC:CRS84, while an explicit ``"crs": null`` means the
#: CRS is *unknown*. Both collapse to ``None`` under ``col_meta.get("crs")``,
#: so any helper that must tell them apart takes this sentinel for the first
#: case and ``None`` for the second. Extract with :func:`crs_from_column_meta`.
CRS_ABSENT = _CrsAbsent()


def crs_from_column_meta(col_meta: dict | None) -> Any:
    """Read a geometry column's ``crs``, preserving absent-vs-explicit-null.

    Returns :data:`CRS_ABSENT` when the key is missing (the OGC:CRS84 default),
    ``None`` when it is present and null (CRS unknown), else the value itself.

    Use this instead of ``col_meta.get("crs")`` at every call site that feeds a
    CRS comparison helper — ``.get`` erases the distinction the helpers need.
    """
    if not isinstance(col_meta, dict) or "crs" not in col_meta:
        return CRS_ABSENT
    return col_meta["crs"]


#: Authority ids that name the GeoParquet default CRS.
#:
#: OGC:CRS84 and EPSG:4326 differ only in axis order, and GeoParquet fixes the
#: stored coordinate order to (x, y) regardless of what the CRS itself declares.
#: The two therefore describe the same coordinates in every GeoParquet file, so
#: comparison helpers must not report them as different CRSs. Normalized to
#: upper-case strings because a PROJJSON ``code`` may be an int or a str.
_CRS84_EQUIVALENT_IDS = frozenset({("OGC", "CRS84"), ("EPSG", "4326")})


def is_crs84_identifier(identifier: tuple[Any, Any] | None) -> bool:
    """True when an ``(authority, code)`` pair names the OGC:CRS84 default.

    Accepts the output of :func:`_extract_crs_identifier` (code may be int or
    str) as well as ``None``.
    """
    if not isinstance(identifier, tuple) or len(identifier) != 2:
        return False
    authority, code = identifier
    return (str(authority).upper(), str(code).upper()) in _CRS84_EQUIVALENT_IDS


def _parse_crs_value(crs):
    """Normalize a CRS that may arrive as a PROJJSON string (from a logical type)."""
    if isinstance(crs, str):
        stripped = crs.strip()
        if stripped.startswith("{"):
            try:
                return json.loads(stripped)
            except (ValueError, TypeError):
                return crs
    return crs


def _is_ogc_crs84(crs) -> bool:
    """Check if CRS is OGC:CRS84.

    The two "no CRS value" shapes get opposite answers, per the spec: an
    absent ``crs`` key (:data:`CRS_ABSENT`) *is* the OGC:CRS84 default, while an
    explicit ``"crs": null`` (``None``) means the CRS is unknown.
    """
    if crs is CRS_ABSENT:
        return True  # Omitted crs key -> the OGC:CRS84 default

    if crs is None:
        return False  # Explicit crs: null -> unknown CRS, not the default

    if isinstance(crs, dict):
        crs_id = crs.get("id", {})
        if isinstance(crs_id, dict):
            # str() guards against malformed metadata with non-string values
            authority = str(crs_id.get("authority", "")).upper()
            code = str(crs_id.get("code", "")).upper()
            return authority == "OGC" and code == "CRS84"

    return False


def _is_crs84_equivalent(crs) -> bool:
    """True when a declared CRS is the spec default (OGC:CRS84) or its lon/lat twin.

    GeoParquet fixes coordinate order to (x, y) regardless of the CRS's own
    axis definition, so EPSG:4326 metadata describes the same coordinates as
    the OGC:CRS84 default.

    Pass :data:`CRS_ABSENT` for a column with no ``crs`` key (the default, so
    True) and ``None`` for an explicit ``"crs": null`` (unknown CRS, so False).

    This is *the* "is this value the default CRS?" predicate. Both comparison
    helpers (``validate._crs_equals`` and ``inspect_utils._crs_are_equivalent``)
    resolve their absent branch through it, so it lives here rather than in
    either of them — they used to call different predicates and disagreed on
    id-less CRS84 PROJJSON and on the ``"SRID:4326"`` spelling.
    """
    crs = _parse_crs_value(crs)
    if _is_ogc_crs84(crs):
        return True
    if isinstance(crs, dict):
        crs_id = crs.get("id", {})
        if isinstance(crs_id, dict) and crs_id:
            return is_crs84_identifier((crs_id.get("authority", ""), crs_id.get("code", "")))
        # No id member (allowed in PROJJSON): fall back to a semantic
        # comparison. Axis order is ignored because GeoParquet fixes
        # coordinate order to (x, y) regardless of the CRS definition.
        try:
            from pyproj import CRS as PyprojCRS

            return PyprojCRS.from_json_dict(crs).equals(
                PyprojCRS.from_user_input("OGC:CRS84"), ignore_axis_order=True
            )
        except Exception:
            return False
    if isinstance(crs, str):
        return crs.strip().upper() in (
            "OGC:CRS84",
            "EPSG:4326",
            "SRID:4326",
            "URN:OGC:DEF:CRS:OGC:1.3:CRS84",
            "URN:OGC:DEF:CRS:OGC::CRS84",
            "URN:OGC:DEF:CRS:EPSG::4326",
        )
    return False


def _extract_crs_identifier(crs_info):
    """
    Extract normalized CRS identifier (authority, code) from various formats.

    Handles PROJJSON dicts, "EPSG:CODE" strings, and URN formats.
    Returns tuple of (authority, code) like ("EPSG", 31287) or ("OGC", "CRS84"), or None.
    Code is int for numeric codes, str for non-numeric (e.g., CRS84).
    """
    if isinstance(crs_info, dict):
        if "id" in crs_info:
            crs_id = crs_info["id"]
            if isinstance(crs_id, dict):
                authority = crs_id.get("authority", "").upper()
                code = crs_id.get("code")
                if authority and code:
                    try:
                        return (authority, int(code))
                    except (ValueError, TypeError):
                        return (authority, str(code).upper())
        return None

    if isinstance(crs_info, str):
        crs_str = crs_info.strip().upper()
        if ":" in crs_str and not crs_str.startswith("URN:"):
            parts = crs_str.split(":")
            if len(parts) == 2:
                try:
                    return (parts[0], int(parts[1]))
                except ValueError:
                    return (parts[0], parts[1])
        if crs_str.startswith("URN:OGC:DEF:CRS:"):
            parts = crs_str.split(":")
            if len(parts) >= 7:
                try:
                    return (parts[4], int(parts[-1]))
                except ValueError:
                    return (parts[4], parts[-1])

    return None


def is_default_crs(crs):
    """
    Check if CRS is the default (OGC:CRS84 or EPSG:4326).

    Returns True if CRS is None, empty, or represents WGS84.
    Used to skip CRS rewriting when output would be default anyway.
    """
    if not crs:
        return True

    identifier = _extract_crs_identifier(crs)
    if identifier:
        authority, code = identifier
        if authority == "EPSG" and code == 4326:
            return True
        if authority == "OGC" and str(code).upper() == "CRS84":
            return True

    return False


def note_default_crs_normalized(declared) -> None:
    """Note at ``--verbose`` that a declared default ``crs`` is dropped on write.

    GeoParquet signals the default CRS (OGC:CRS84 / EPSG:4326) by *omitting* the
    ``crs`` key, so an input that spelled it out loses that key on write. The
    output is correct GeoParquet, but the drop is invisible — a caller diffing
    ``geo`` blocks sees a key they wrote go missing (#815).

    ``declared`` is the input's raw ``crs`` value. Only a value that really names
    the default is noted: an empty value names nothing, and an explicit
    ``crs: null`` means *unknown*, which has its own warning
    (:data:`NULL_CRS_HINT`). :func:`is_default_crs` is the sole predicate here so
    the note and the drop can never disagree about what the default is (#796).

    This is the single wording of the note. :func:`apply_output_crs` calls it for
    every write path that carries the input's ``geo`` block through; paths that
    rebuild the block from the converted data (``gpio convert geoparquet``, whose
    2.0 fast path lets DuckDB regenerate it) call it themselves, because their
    input's ``crs`` never reaches ``apply_output_crs`` (#844).
    """
    if not declared or not is_default_crs(declared):
        return
    debug(
        "Normalized an explicit default CRS: dropped the geometry column's "
        "`crs` key, which is how GeoParquet spells the OGC:CRS84 / EPSG:4326 "
        "default (the coordinates are unchanged)."
    )


def apply_output_crs(col_meta: dict, input_crs) -> None:
    """Set or clear a geometry column's ``crs`` for the requested output CRS.

    Single source of truth for the GeoParquet null-vs-default CRS rule across all
    write paths. The default CRS (OGC:CRS84 / EPSG:4326) is signalled by
    *omitting* the ``crs`` key — never an explicit ``crs: null`` or ``crs: 4326``.

    - ``input_crs`` non-default -> write it explicitly.
    - ``input_crs`` default -> drop ``crs`` (output is the spec default), so a
      stale value carried from the source (e.g. EPSG:3857 after reprojecting to
      4326, or an explicit ``crs: null``) is not written through.
    - ``input_crs`` is ``None`` (CRS unchanged) -> preserve a real ``crs`` but
      still strip a stray default/null one the source or DuckDB may have attached.

    Dropping a ``crs`` the input *spelled out* as the default is invisible in the
    output, so :func:`note_default_crs_normalized` reports it once at
    ``--verbose`` (#815).

    Mutates ``col_meta`` in place.
    """
    # Captured before the pop: the note describes what the input declared.
    declared = col_meta.get("crs")

    if input_crs and not is_default_crs(input_crs):
        col_meta["crs"] = input_crs
        return
    if input_crs or is_default_crs(col_meta.get("crs")):
        col_meta.pop("crs", None)
        note_default_crs_normalized(declared)


#: Shared guidance appended to null-CRS warnings and the validate message.
NULL_CRS_HINT = (
    "An explicit null CRS means the CRS is *unknown* (not the OGC:CRS84 default). "
    "If the coordinates are really lon/lat WGS84, run "
    "`gpio convert reproject <input> <output> --assume-crs84` to set the default."
)

#: Raised by the file/streaming reproject paths when DuckDB hits a ``crs: null``
#: input without ``--assume-crs84`` (DuckDB's GeoParquet reader rejects it).
NULL_CRS_NO_FLAG_ERROR = (
    "Input has an explicit null CRS (unknown). DuckDB cannot read it. "
    "If the coordinates are lon/lat WGS84, re-run with --assume-crs84 to "
    "treat them as OGC:CRS84 and write the default."
)


def crs_is_explicitly_null(col_meta: dict) -> bool:
    """Return True only when a geometry column's metadata sets ``crs`` to null.

    An explicit ``"crs": null`` means the CRS is unknown, which is different
    from omitting the key entirely (the omitted case defaults to OGC:CRS84).
    """
    return isinstance(col_meta, dict) and "crs" in col_meta and col_meta["crs"] is None


def geoparquet_crs_is_null(parquet_file) -> bool:
    """Return True if the primary geometry column has an explicit ``crs: null``.

    ``parquet_file`` must be a *raw* path/URL — ``get_geo_metadata`` SQL-escapes
    it internally, so passing an already-escaped URL double-escapes it.
    """
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata
    from geoparquet_io.core.geo_metadata import sanitize_geo_metadata

    # `get_geo_metadata` is the read-only reader and hands the block back as the
    # file really holds it; the reproject paths that ask this question then act
    # on the answer, so the malformed parts get dropped here (#887).
    geo_meta = sanitize_geo_metadata(get_geo_metadata(str(parquet_file)))
    if not geo_meta:
        return False
    primary_col = geo_meta.get("primary_column", "geometry")
    col_meta = geo_meta.get("columns", {}).get(primary_col, {})
    return crs_is_explicitly_null(col_meta)


@lru_cache(maxsize=256)
def _emit_null_crs_warning(key: str) -> None:
    """Emit the null-CRS warning exactly once per ``key`` (LRU-bounded)."""
    warn(f"Input has an explicit null CRS (unknown). {NULL_CRS_HINT}")


def warn_null_crs_once(key: str) -> None:
    """Emit the null-CRS warning at most once per ``key`` for this process.

    Dedup is bounded by an LRU cache so long-running processes (the Python API)
    don't accumulate keys without limit.
    """
    if key:
        _emit_null_crs_warning(key)


def reset_null_crs_warnings() -> None:
    """Clear the warn-once dedup cache. Intended for tests."""
    _emit_null_crs_warning.cache_clear()


def apply_target_crs_to_geo_meta(geo_meta: dict, geom_col: str, target_crs: str, con) -> None:
    """Set or clear a geometry column's CRS in ``geo_meta`` in place.

    The GeoParquet default CRS (EPSG:4326 / OGC:CRS84) is signalled by *omitting*
    the ``crs`` key entirely — never by writing an explicit CRS84 object or null.
    """
    columns = geo_meta.get("columns", {})
    if geom_col not in columns:
        return
    if is_default_crs(target_crs):
        columns[geom_col].pop("crs", None)
    else:
        columns[geom_col]["crs"] = parse_crs_string_to_projjson(target_crs, con)


# CRS type values allowed by the PROJJSON v0.7 schema's "crs" definition.
# validate.py's crs check reads the same set, so a CRS gpio writes and a CRS
# gpio validates can never disagree about what counts as PROJJSON.
PROJJSON_CRS_TYPES = frozenset(
    {
        "GeodeticCRS",
        "GeographicCRS",
        "ProjectedCRS",
        "VerticalCRS",
        "CompoundCRS",
        "BoundCRS",
        "EngineeringCRS",
        "ParametricCRS",
        "TemporalCRS",
        "DerivedGeodeticCRS",
        "DerivedGeographicCRS",
        "DerivedProjectedCRS",
        "DerivedVerticalCRS",
        "DerivedEngineeringCRS",
        "DerivedParametricCRS",
        "DerivedTemporalCRS",
    }
)


# PROJJSON members that define a CRS rather than merely identify or annotate it.
# A type-less dict carrying any of these is not repaired from its id (see
# normalize_projjson_crs): the body may contradict the id.
_CRS_DEFINING_MEMBERS = frozenset(
    {"datum", "datum_ensemble", "coordinate_system", "base_crs", "conversion", "components"}
)


def _projjson_authority_code(crs: dict) -> tuple[str, str] | None:
    """Return the ``(authority, code)`` of a PROJJSON ``id`` member, if it has one."""
    crs_id = crs.get("id")
    if not isinstance(crs_id, dict):
        return None
    authority = crs_id.get("authority")
    code = crs_id.get("code")
    if authority in (None, "") or code in (None, ""):
        return None
    return str(authority), str(code)


@lru_cache(maxsize=64)
def _projjson_from_authority(authority: str, code: str) -> str | None:
    """Canonical PROJJSON (as a JSON string, for caching) for an authority code."""
    try:
        from pyproj import CRS

        return json.dumps(CRS.from_authority(authority, code).to_json_dict())
    except Exception:  # unknown authority/code, or no PROJ database entry
        return None


def normalize_projjson_crs(crs, source_description: str):
    """Return a CRS that is valid PROJJSON, repairing or rejecting one that is not.

    gpio copies an input's CRS straight into the file it writes. A CRS that is
    not valid PROJJSON therefore becomes an invalid *output* — a file gpio's own
    ``check spec`` rejects (#705). Rather than pass the defect on:

    * valid PROJJSON (and anything that is not a CRS object, e.g. a
      ``"EPSG:3857"`` string resolved elsewhere) is returned untouched;
    * PROJJSON missing only the required ``"type"`` member, carrying an ``id``
      that resolves to a real CRS and no CRS definition of its own, is repaired
      from that authority code — the id names the CRS unambiguously, so nothing
      is guessed;
    * anything else raises, naming the input and the CRS it could not make sense
      of, so the user gets an error instead of a silently invalid file.

    ``source_description`` is the input path, quoted back to the user in errors.
    """
    from geoparquet_io.core.exceptions import GeoParquetError

    if not isinstance(crs, dict):
        return crs

    crs_type = crs.get("type")
    if crs_type in PROJJSON_CRS_TYPES:
        return crs

    authority_code = _projjson_authority_code(crs)
    name = crs.get("name")
    described = f"{authority_code[0]}:{authority_code[1]}" if authority_code else "no id"
    if name:
        described = f"{described}, name {name!r}"

    # Rebuilding from the id is only safe when the id is all the dict carries:
    # a body with its own CRS definition (datum, conversion, ...) may contradict
    # the id, and replacing it with the authority's definition would silently
    # relabel the data. Those are rejected below instead.
    defining_members = _CRS_DEFINING_MEMBERS.intersection(crs)
    if crs_type is None and authority_code is not None and not defining_members:
        repaired = _projjson_from_authority(*authority_code)
        if repaired is not None:
            warn(
                f'CRS ({described}) carries no PROJJSON "type" member; '
                f"rebuilt it from {authority_code[0]}:{authority_code[1]}"
            )
            return json.loads(repaired)

    if crs_type is None and defining_members:
        problem = (
            'is missing the required PROJJSON "type" member, and carries its own '
            f"CRS definition ({', '.join(sorted(defining_members))}) that gpio "
            "will not overwrite from the id"
        )
    elif crs_type is None:
        problem = 'is missing the required PROJJSON "type" member'
    else:
        problem = f"has unknown PROJJSON type {crs_type!r}"
    raise GeoParquetError(
        f"CRS in {source_description} {problem} ({described}), and could not be "
        "repaired from its identifier. Writing it through would produce a "
        "GeoParquet file that 'gpio check spec' rejects. Fix the CRS in the "
        "input, or re-export it from a tool that writes valid PROJJSON."
    )


def _validate_projjson(crs: dict) -> bool:
    """Validate that a CRS dict has the expected PROJJSON structure."""
    if not isinstance(crs, dict):
        return False
    if "$schema" not in crs and "type" not in crs and "id" not in crs:
        return False
    return True


def _wrap_query_with_crs(
    query: str,
    geometry_column: str | None,
    input_crs: dict | None,
) -> str:
    """Wrap query with ST_SetCRS() so DuckDB writes CRS into the Parquet schema natively."""
    if not input_crs or is_default_crs(input_crs):
        return query

    if not geometry_column:
        raise ValueError(
            "geometry_column is required when input_crs is specified — "
            "cannot apply CRS without a geometry column"
        )

    if not _validate_projjson(input_crs):
        warn("input_crs does not look like valid PROJJSON — skipping CRS application")
        return query

    crs_json = _escape_sql_string(json.dumps(input_crs))
    return f"""
        SELECT * REPLACE (ST_SetCRS({quote_identifier(geometry_column)}, '{crs_json}') AS {quote_identifier(geometry_column)})
        FROM ({query})
    """


#: Default target CRS for lon/lat operations (grid keying, admin joins). The
#: GeoParquet default; lon/lat axis order is guaranteed by the session-level
#: ``geometry_always_xy = true`` that ``get_duckdb_connection`` sets.
WGS84_TRANSFORM_TARGET = "OGC:CRS84"


def resolve_crs_to_string(crs_info) -> str | None:
    """Resolve CRS info (PROJJSON dict or string) to a CRS string for ST_Transform.

    Tries the authority identifier first, then pyproj resolution, then the raw
    PROJJSON. Returns None if ``crs_info`` is falsy or unresolvable.
    """
    if not crs_info:
        return None

    identifier = _extract_crs_identifier(crs_info)
    if identifier:
        authority, code = identifier
        return f"{authority}:{code}"

    if isinstance(crs_info, dict):
        try:
            from pyproj import CRS

            authority = CRS.from_json_dict(crs_info).to_authority()
            if authority:
                return f"{authority[0]}:{authority[1]}"
        except Exception:
            pass
        return json.dumps(crs_info)

    return None


def crs_transform_sql_expr(
    geom_sql: str,
    source_crs,
    target_crs: str = WGS84_TRANSFORM_TARGET,
) -> str:
    """Return a SQL expression yielding ``geom_sql`` in ``target_crs``.

    Wraps ``geom_sql`` in ``ST_Transform(..., '<src>', '<target>')`` only when
    the source CRS is known and differs from the target; otherwise returns
    ``geom_sql`` unchanged. This is the single source of truth for making the
    CRS-blind spatial operations (grid keying, admin joins) CRS-aware.

    The rules mirror the GeoParquet/DuckDB contract:

    - A missing/``None`` or default (OGC:CRS84 / EPSG:4326) source is treated as
      already being the target — no transform. A CRS-less geometry (e.g. from an
      in-memory Arrow table via ``ST_GeomFromWKB``) is therefore accepted as-is,
      and ``ST_Intersects`` accepts a CRS-less geometry against a CRS-bearing one.
    - An unresolvable source CRS is left untransformed rather than guessed.

    Relies on the session-level ``geometry_always_xy = true`` so the transformed
    coordinates come out as lon/lat (x/y) for the ``*_lonlat_to_cell`` keying.

    Args:
        geom_sql: A SQL geometry expression (e.g. a quoted column name or
            ``ST_Centroid("geometry")``).
        source_crs: The source CRS as a PROJJSON dict, an ``AUTH:CODE`` string,
            or ``None``.
        target_crs: The target CRS string (default OGC:CRS84).
    """
    if not source_crs or is_default_crs(source_crs):
        return geom_sql

    src = resolve_crs_to_string(source_crs)
    if not src:
        return geom_sql

    src_literal = _escape_sql_string(src)
    target_literal = _escape_sql_string(target_crs)
    return f"ST_Transform({geom_sql}, '{src_literal}', '{target_literal}')"


def extract_crs_from_table(table, geometry_column: str | None = None):
    """Return the CRS of ``geometry_column`` from a pyarrow table's geo metadata.

    Returns the CRS value (PROJJSON dict or string) when present and non-default,
    else ``None``. ``geometry_column`` defaults to the geo metadata's declared
    primary column. Used by the table-centric (Python API) operations to detect
    a projected input before grid keying.

    A write-path reader: the CRS it returns decides how the output is keyed, so
    a malformed carried block goes through :func:`sanitize_geo_metadata` and is
    treated the way an absent one is (#887).
    """
    from geoparquet_io.core.geo_metadata import decode_carried_geo, sanitize_geo_metadata

    metadata = table.schema.metadata
    if not metadata or b"geo" not in metadata:
        return None
    geo_meta = sanitize_geo_metadata(decode_carried_geo(metadata[b"geo"]))
    if not isinstance(geo_meta, dict):
        return None
    columns = geo_meta.get("columns", {})
    col = geometry_column or geo_meta.get("primary_column", "geometry")
    crs = columns.get(col, {}).get("crs")
    if crs and not is_default_crs(crs):
        return crs
    return None


def extract_crs_from_parquet(parquet_file, verbose=False):
    """
    Extract CRS (as PROJJSON dict) from a Parquet file.

    ``parquet_file`` must be a **RAW** path/URL: every helper it reaches
    (``get_geo_metadata``, ``get_schema_info``, ``resolve_crs_reference``)
    SQL-escapes its own argument, so a pre-escaped path is escaped twice and
    the existence check then fails on a file that is plainly there (#718).

    Checks in order:
    1. GeoParquet metadata (columns.<geom_col>.crs)
    2. Parquet native geo type (from schema logical_type)

    A write-path reader: ``convert``, ``reproject``, the format writers and
    ``process aggregate`` all turn this answer into a transform or an output
    file, so a malformed carried block goes through
    :func:`sanitize_geo_metadata` rather than being indexed as-is (#887).
    ``get_geo_metadata`` itself stays unsanitized -- ``gpio check`` reads
    through it and has to see the file as it really is.
    """
    from geoparquet_io.core.duckdb_metadata import (
        get_geo_metadata,
        get_schema_info,
        parse_geometry_logical_type,
        resolve_crs_reference,
    )
    from geoparquet_io.core.geo_metadata import sanitize_geo_metadata

    geo_meta = sanitize_geo_metadata(get_geo_metadata(parquet_file))
    if geo_meta:
        primary_col = geo_meta.get("primary_column", "geometry")
        columns = geo_meta.get("columns", {})
        if primary_col in columns:
            if crs_is_explicitly_null(columns[primary_col]):
                warn_null_crs_once(str(parquet_file))
            crs = columns[primary_col].get("crs")
            if crs and not is_default_crs(crs):
                if verbose:
                    debug(f"Found CRS in GeoParquet metadata: {_format_crs_display(crs)}")
                return crs

    schema_info = get_schema_info(parquet_file)
    for col in schema_info:
        logical_type = col.get("logical_type") or ""
        if logical_type and (
            logical_type.startswith("GeometryType(") or logical_type.startswith("GeographyType(")
        ):
            parsed = parse_geometry_logical_type(logical_type)
            if parsed and "crs" in parsed:
                raw_crs = parsed["crs"]
                crs = resolve_crs_reference(parquet_file, raw_crs)
                if crs and not is_default_crs(crs):
                    if verbose:
                        debug(f"Found CRS in Parquet geo type: {_format_crs_display(crs)}")
                    return crs

    return None


def _detect_crs_from_filegdb(gdb_path, con, verbose=False):
    """Detect CRS from a FileGDB directory by iterating internal .gdbtable files."""
    gdb_path = gdb_path.rstrip("/\\")

    if not os.path.isdir(gdb_path):
        return None

    try:
        gdbtable_files = sorted(
            [f for f in os.listdir(gdb_path) if f.endswith(".gdbtable")],
            reverse=True,
        )
    except OSError:
        return None

    for gdbtable_file in gdbtable_files:
        gdbtable_path = os.path.join(gdb_path, gdbtable_file)
        try:
            result = con.execute(f"""
                SELECT * FROM ST_Read_Meta({sql_path(gdbtable_path)})
            """).fetchone()

            if not result or not result[3]:
                continue

            for layer in result[3]:
                layer_name = layer.get("name", "")
                if layer_name.startswith("GDB_"):
                    continue

                geometry_fields = layer.get("geometry_fields", [])
                if not geometry_fields:
                    continue

                crs_info = geometry_fields[0].get("crs", {})

                projjson_str = crs_info.get("projjson")
                if projjson_str:
                    crs = json.loads(projjson_str)
                    if verbose:
                        debug(
                            f"Found CRS in FileGDB layer '{layer_name}': {_format_crs_display(crs)}"
                        )
                    return crs

                auth_name = crs_info.get("auth_name")
                auth_code = crs_info.get("auth_code")
                if auth_name and auth_code:
                    crs = {"id": {"authority": auth_name, "code": int(auth_code)}}
                    if verbose:
                        debug(f"Found CRS in FileGDB layer '{layer_name}': {auth_name}:{auth_code}")
                    return crs

        except Exception:
            continue

    return None


def detect_crs_from_spatial_file(input_file, con, verbose=False):
    """Detect CRS from a spatial file (GeoJSON, GPKG, Shapefile, FileGDB)."""
    try:
        result = con.execute(f"""
            SELECT * FROM ST_Read_Meta({sql_path(input_file)})
        """).fetchone()

        if result:
            layers = result[3]
            if layers and len(layers) > 0:
                layer = layers[0]
                geometry_fields = layer.get("geometry_fields", [])
                if geometry_fields:
                    crs_info = geometry_fields[0].get("crs", {})
                    projjson_str = crs_info.get("projjson")
                    if projjson_str:
                        crs = json.loads(projjson_str)
                        if verbose:
                            debug(f"Found CRS in spatial file: {_format_crs_display(crs)}")
                        return crs
                    auth_name = crs_info.get("auth_name")
                    auth_code = crs_info.get("auth_code")
                    if auth_name and auth_code:
                        crs = {"id": {"authority": auth_name, "code": int(auth_code)}}
                        if verbose:
                            debug(f"Found CRS: {auth_name}:{auth_code}")
                        return crs
    except Exception as e:
        if verbose:
            warn(f"Could not detect CRS from spatial file: {e}")

    if input_file.rstrip("/\\").lower().endswith(".gdb"):
        if verbose:
            debug("ST_Read_Meta returned empty for FileGDB, trying workaround...")
        return _detect_crs_from_filegdb(input_file, con, verbose)

    return None


def _format_crs_display(crs):
    """Format CRS for display (extract EPSG code if possible)."""
    if not crs:
        return "None"
    identifier = _extract_crs_identifier(crs)
    if identifier:
        return f"{identifier[0]}:{identifier[1]}"
    return str(crs)[:50] + "..." if len(str(crs)) > 50 else str(crs)


def get_crs_display_name(crs_info: dict | str | None) -> str:
    """Get human-readable CRS name with authority code."""
    if crs_info is CRS_ABSENT:
        return "OGC:CRS84 (default)"

    # An explicit ``"crs": null`` means the CRS is *unknown*, not the default —
    # the opposite of CRS_ABSENT above. This string renders inside validation
    # failure details, where calling it OGC:CRS84 makes the message contradict
    # the failure it explains.
    if crs_info is None:
        return "null (CRS unknown)"

    if isinstance(crs_info, str):
        return crs_info

    if isinstance(crs_info, dict):
        name = crs_info.get("name", "")
        crs_id = crs_info.get("id", {})
        if isinstance(crs_id, dict):
            authority = crs_id.get("authority", "EPSG")
            code = crs_id.get("code")
            if code:
                return f"{name} ({authority}:{code})" if name else f"{authority}:{code}"
        if name:
            return name
        return "PROJJSON object"

    return "unknown"


def is_geographic_crs(crs: dict | str | None) -> bool:
    """Check if CRS is geographic (lat/lon) vs projected."""
    # No crs key at all means the OGC:CRS84 default, which is geographic. An
    # explicit null (unknown CRS) is answered the same way deliberately, for the
    # reason ``validate._get_crs_bounds`` gives: callers use this to sanity-check
    # coordinates, and lon/lat is the only guess worth making about an unknown
    # CRS. The null itself is reported by validate's ``_check_crs_valid``.
    if crs is CRS_ABSENT or crs is None:
        return True

    if isinstance(crs, dict):
        crs_type = crs.get("type", "").lower()
        if crs_type == "geographiccrs":
            return True
        if crs_type == "projectedcrs":
            return False

        crs_id = crs.get("id", {})
        if isinstance(crs_id, dict):
            authority = crs_id.get("authority", "").upper()
            code = crs_id.get("code")
            if authority == "EPSG" and code == 4326:
                return True
            if authority == "OGC" and str(code).upper() == "CRS84":
                return True

        name = crs.get("name", "").upper()
        projected_indicators = ["UTM", "ZONE", "MERCATOR", "ALBERS", "LAMBERT", "STATE PLANE"]
        if any(indicator in name for indicator in projected_indicators):
            return False
        if any(x in name for x in ["WGS 84", "WGS84", "CRS84", "4326"]):
            return True

    if isinstance(crs, str):
        crs_upper = crs.upper()
        projected_indicators = ["UTM", "ZONE", "MERCATOR", "ALBERS", "LAMBERT"]
        if any(indicator in crs_upper for indicator in projected_indicators):
            return False
        return any(x in crs_upper for x in ["4326", "CRS84", "WGS84"])

    return False


def merge_longitude_ranges(ranges: list[tuple[float, float]]) -> tuple[float, float]:
    """Union of ``(xmin, xmax)`` longitude ranges, antimeridian-aware.

    ``xmin > xmax`` marks a range that wraps the antimeridian -- the convention
    of RFC 7946 5.2, which GeoParquet's ``bbox`` and Parquet's own geospatial
    statistics both follow. Plain ``min``/``max`` over such a pair yields the
    *complement* of the real extent, so the union is taken on the circle: the
    arcs are merged and the result is the complement of the widest gap left
    between them.

    With no wrapping input the result is exactly ``(min(xmin), max(xmax))``, so
    data from a producer that never wraps is reported as before.

    The ranges must be **longitudes**: the split of a wrapping range happens at
    +/-180, so callers have to establish a geographic CRS first (see
    :func:`is_geographic_crs`). For a projected CRS the spec gives a bbox as
    minima then maxima with no wrap-around, and the caller takes plain min/max.
    """
    if not ranges:
        raise ValueError("merge_longitude_ranges() needs at least one range")
    if all(xmin <= xmax for xmin, xmax in ranges):
        return min(r[0] for r in ranges), max(r[1] for r in ranges)

    arcs: list[list[float]] = []
    for xmin, xmax in ranges:
        arcs.extend([[xmin, xmax]] if xmin <= xmax else [[xmin, 180.0], [-180.0, xmax]])
    arcs.sort()

    merged = [arcs[0]]
    for start, end in arcs[1:]:
        if start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])

    if len(merged) == 1:
        return merged[0][0], merged[0][1]

    # The widest gap is the part of the circle the data does not cover, so the
    # answer is everything else. Reaching here means some range wrapped, which
    # put arcs on both sides of the antimeridian: the -180/180 seam is covered
    # and the gaps between the merged arcs are the only candidates.
    _, at = max((merged[i + 1][0] - merged[i][1], i) for i in range(len(merged) - 1))
    return merged[at + 1][0], merged[at][1]


def parse_crs_string_to_projjson(crs_string, con=None):
    """Convert a CRS string (like "EPSG:5070") to full PROJJSON dict."""
    identifier = _extract_crs_identifier(crs_string)
    if not identifier:
        return None

    authority, code = identifier

    try:
        from pyproj import CRS

        crs = CRS.from_authority(authority, code)
        return crs.to_json_dict()
    except Exception:
        return {"id": {"authority": authority, "code": code}}


#: Canonical lon/lat target CRS for grid keying and admin spatial joins.
#: With ``SET geometry_always_xy = true`` this is interchangeable with EPSG:4326.
DEFAULT_TARGET_CRS = "OGC:CRS84"

#: The only shape :func:`crs_string_for_transform` may ever return.
#:
#: The same strict quote-free ``<authority>:<code>`` shape the logical-type
#: parser enforced before #870 (``_AUTHORITY_CODE_CRS`` in
#: ``core/duckdb_metadata.py``). #866/#870 made the parser carry *arbitrary*
#: free-form ``crs`` strings through so validation can fail closed on them --
#: but a value headed for ``ST_Transform`` is headed for SQL, so this choke
#: point restores the old invariant for transforms: authority code or nothing.
#: The sinks still SQL-escape on top of this (defense in depth, and the
#: reproject path legitimately passes PROJJSON strings that bypass this
#: helper).
_TRANSFORM_CRS_SHAPE = re.compile(r"^[A-Za-z][A-Za-z0-9_.\-]*:[A-Za-z0-9_.\-]+$")


def crs_string_for_transform(crs) -> str | None:
    """Return an ``"AUTH:CODE"`` CRS string for ``ST_Transform``, or ``None``.

    ``None`` means no transform is needed or possible: the CRS is absent, is the
    default (OGC:CRS84 / EPSG:4326), or is not identifiable as an authority code.
    ``crs`` may be PROJJSON (as returned by :func:`extract_crs_from_parquet`) or
    an ``"AUTH:CODE"`` string.

    The result is guaranteed to match :data:`_TRANSFORM_CRS_SHAPE`: a file's
    free-form ``crs`` (carried through for validation since #866) or a hostile
    PROJJSON ``id`` must never come out of here, because callers interpolate
    the result into SQL.
    """
    if not crs or is_default_crs(crs):
        return None
    identifier = _extract_crs_identifier(crs)
    if not identifier:
        return None
    authority, code = identifier
    candidate = f"{authority}:{code}"
    if not _TRANSFORM_CRS_SHAPE.match(candidate):
        return None
    return candidate


def transform_geom_sql(geom_expr: str, source_crs, target_crs: str = DEFAULT_TARGET_CRS) -> str:
    """Wrap ``geom_expr`` in ``ST_Transform`` to ``target_crs`` when needed.

    Returns ``geom_expr`` unchanged when the source CRS is absent, the default,
    or unidentifiable — so CRS-less / already-lon-lat input is untouched and the
    common (CRS84) path pays nothing. The caller's DuckDB session should have
    ``geometry_always_xy = true`` so transformed coordinates come out as lon/lat.

    This is the shared "normalize geometry to the operation's expected CRS"
    utility used by the admin spatial joins and the lon/lat grid keying (#525).
    """
    src = crs_string_for_transform(source_crs)
    if src is None:
        return geom_expr
    src_esc = src.replace("'", "''")
    tgt_esc = target_crs.replace("'", "''")
    return f"ST_Transform({geom_expr}, '{src_esc}', '{tgt_esc}')"


def source_crs_string(parquet_file, verbose: bool = False) -> str | None:
    """Detect ``parquet_file``'s CRS as an ``"AUTH:CODE"`` transform string.

    Returns ``None`` for CRS84/default/CRS-less input (no transform needed).
    """
    return crs_string_for_transform(extract_crs_from_parquet(parquet_file, verbose))


def reproject_to_source_sql(geom_expr: str, source_crs, base_crs: str = DEFAULT_TARGET_CRS) -> str:
    """Wrap ``geom_expr`` to reproject FROM ``base_crs`` (default CRS84) TO ``source_crs``.

    The inverse direction of :func:`transform_geom_sql`. Used to bring the
    (small) OGC:CRS84 admin polygons into a non-CRS84 input's CRS so the spatial
    join and its bbox pre-filter run in one CRS *without* transforming the large
    input per row — this restores the cheap bbox pre-filter on non-CRS84 admin
    joins instead of degrading to a full nested-loop ``ST_Intersects`` (#525).

    Returns ``geom_expr`` unchanged when ``source_crs`` is absent/default.
    """
    if not source_crs or is_default_crs(source_crs):
        return geom_expr
    src = resolve_crs_to_string(source_crs)
    if not src:
        return geom_expr
    src_lit = _escape_sql_string(src)
    base_lit = _escape_sql_string(base_crs)
    return f"ST_Transform({geom_expr}, '{base_lit}', '{src_lit}')"


def parse_geo_metadata_from_schema(metadata: dict | None) -> dict | None:
    """Parse GeoParquet ``geo`` metadata from an Arrow schema metadata dict.

    The schema metadata may use bytes or string keys/values depending on how it
    was accessed. Returns the parsed dict, or ``None`` if absent/unparsable.
    """
    if not metadata:
        return None
    geo_bytes = metadata.get(b"geo") or metadata.get("geo")
    if not geo_bytes:
        return None
    try:
        if isinstance(geo_bytes, bytes):
            return json.loads(geo_bytes.decode("utf-8"))
        return json.loads(geo_bytes)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def crs_string_from_geo_meta(geo_meta: dict | None, geom_col: str) -> str | None:
    """Return an ``"AUTH:CODE"`` transform string for a geometry column's CRS.

    ``geo_meta`` is parsed GeoParquet ``geo`` metadata (as carried on a PyArrow
    schema for the table-centric Python API). Returns ``None`` when no transform
    is needed — the CRS is absent, the default (OGC:CRS84 / EPSG:4326), explicitly
    null (unknown), or not identifiable as an authority code.

    A write-path reader: the string it returns is spliced into an
    ``ST_Transform`` call, so the block is sanitized first (#887). Sanitizing
    here rather than in the callers keeps the single check in one place --
    ``parse_geo_metadata_from_schema``, which both callers parse with, is a
    read-only reader and deliberately hands the block over untouched.
    """
    from geoparquet_io.core.geo_metadata import sanitize_geo_metadata

    geo_meta = sanitize_geo_metadata(geo_meta)
    if not geo_meta:
        return None
    columns = geo_meta.get("columns", {})
    col_meta = columns.get(geom_col)
    if col_meta is None:
        col_meta = columns.get(geo_meta.get("primary_column", "geometry"), {})
    crs = col_meta.get("crs") if isinstance(col_meta, dict) else None
    return crs_string_for_transform(crs)


#: GeoArrow geometry extension names (registered by ``geoarrow.pyarrow``).
_GEOARROW_EXTENSION_NAMES = frozenset(
    {
        "geoarrow.wkb",
        "ogc.wkb",
        "geoarrow.point",
        "geoarrow.linestring",
        "geoarrow.polygon",
        "geoarrow.multipoint",
        "geoarrow.multilinestring",
        "geoarrow.multipolygon",
        "geoarrow.geometry",
    }
)


def geoarrow_crs_to_projjson(crs: object) -> dict | str | None:
    """Return an Arrow extension type's ``.crs`` as PROJJSON, or ``None``.

    The single reader for "what is on ``field.type.crs``" — ``streaming``,
    ``duckdb_metadata`` and :func:`_crs_from_extension_type` all delegate here so
    the same object cannot resolve three different ways depending on which read
    path touched it (issue #863).

    ``geom_type.crs`` is whatever the extension type chose to hold. Once
    ``geoarrow.pyarrow`` is imported, PyArrow resolves ``geoarrow.wkb`` fields
    into real extension types and this is a ``geoarrow.types.crs.Crs`` object
    (``ProjJsonCrs``, ``StringCrs``, or a ``pyproj.CRS``) — never a plain value.

    Those objects must be read through their PROJJSON accessor. ``str()`` on one
    returns its *repr* (``"ProjJsonCrs(OGC:CRS84)"``), which downstream code then
    split on ``:`` into the fabricated identifier
    ``{"id": {"authority": "PROJJSONCRS(OGC", "code": "CRS84)"}}`` — invalid
    GeoParquet that gpio's own validator rejects (issue #816).

    Unknown objects are rejected (``None`` + a warning) rather than stringified: a
    repr is never a CRS, and inventing one writes a file that lies about its
    coordinates. ``None`` lets the caller fall back to the ``geo`` metadata, which
    is where the truth usually is; raising would fail an otherwise-fine read over
    a metadata detail this function documents as optional. Every rejection warns,
    so a CRS is never dropped silently.
    """
    if crs is None:
        return None

    # Already a usable representation: PROJJSON dict, or a string identifier
    # such as "EPSG:3857" that callers normalise themselves. An *empty* dict or
    # string is not a CRS — returning it would shadow the real `geo` metadata on
    # the caller's ``is not None`` check, so it is rejected like any other
    # unreadable value.
    if isinstance(crs, dict | str):
        return crs or None
    if isinstance(crs, bytes | bytearray):
        try:
            decoded = crs.decode("utf-8")
        except UnicodeDecodeError as exc:
            warn(f"Ignoring geometry CRS: {len(crs)} bytes that are not valid UTF-8 ({exc})")
            return None
        return decoded or None

    # The geoarrow ``Crs`` protocol (and pyproj.CRS) expose PROJJSON directly:
    # ``to_json_dict()`` returns a Mapping, ``to_json()`` the same as a string.
    for accessor, parse in (("to_json_dict", dict), ("to_json", json.loads)):
        method = getattr(crs, accessor, None)
        if not callable(method):
            continue
        try:
            resolved = parse(method())
        except Exception as exc:  # pyproj lookup failure, malformed PROJJSON, ...
            warn(f"Could not read PROJJSON from CRS object {type(crs).__name__}: {exc}")
            return None
        # An empty PROJJSON object is not a CRS; let the caller fall back.
        return resolved if isinstance(resolved, dict) and resolved else None

    warn(
        f"Ignoring geometry CRS of unsupported type {type(crs).__name__}: it exposes no "
        "PROJJSON accessor, and its text form is not a coordinate reference system."
    )
    return None


def _crs_from_extension_type(field_type):
    """Extract a ``crs`` (PROJJSON dict/str) from a registered GeoArrow type.

    When ``geoarrow.pyarrow`` is imported anywhere in the process it registers
    its extension types, and ``pyarrow.parquet`` then returns geometry columns
    as those types — moving the CRS onto ``field.type.crs`` and *consuming* the
    raw ``ARROW:extension:metadata`` key off ``field.metadata``. Returns ``None``
    for non-GeoArrow types or when no CRS is present.
    """
    if getattr(field_type, "extension_name", None) not in _GEOARROW_EXTENSION_NAMES:
        return None
    resolved = geoarrow_crs_to_projjson(getattr(field_type, "crs", None))
    if resolved is not None:
        return resolved
    ext_meta = getattr(field_type, "extension_metadata", None)
    if ext_meta:
        try:
            parsed = json.loads(ext_meta)
        except (json.JSONDecodeError, TypeError):
            return None
        if isinstance(parsed, dict):
            return parsed.get("crs")
    return None


def _crs_from_geoarrow_field(table, geom_col: str):
    """Return the PROJJSON ``crs`` from a geometry field's GeoArrow metadata.

    Parquet-geo-only / GeoArrow inputs carry the CRS on the geometry field, but
    *where* depends on whether ``geoarrow.pyarrow`` has been imported in the
    process (it registers extension types globally — many code paths and tests
    do this transitively):

    1. Imported -> the field is a registered extension type and the CRS lives on
       ``field.type.crs`` (the raw metadata key is consumed off the field).
    2. Not imported -> the CRS is in ``field.metadata['ARROW:extension:metadata']``.

    Both are checked so detection is import-order-independent. Returns the raw
    ``crs`` value (PROJJSON dict or string), or ``None``.
    """
    try:
        field = table.schema.field(geom_col)
    except (KeyError, ValueError):
        return None

    # Case 1: geoarrow-pyarrow registered the type and consumed the metadata.
    crs = _crs_from_extension_type(field.type)
    if crs is not None:
        return crs

    # Case 2: plain binary field — CRS is in the raw extension metadata.
    md = getattr(field, "metadata", None)
    if not md:
        return None
    ext = md.get(b"ARROW:extension:metadata") or md.get("ARROW:extension:metadata")
    if not ext:
        return None
    try:
        if isinstance(ext, bytes):
            ext = ext.decode("utf-8")
        ext_dict = json.loads(ext)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return ext_dict.get("crs") if isinstance(ext_dict, dict) else None


def crs_string_from_table(table, geom_col: str) -> str | None:
    """Detect an Arrow table's geometry CRS as an ``"AUTH:CODE"`` transform string.

    Checks both CRS carriers used by the table-centric Python API:
    1. The schema-level GeoParquet ``geo`` metadata.
    2. The geometry field's GeoArrow ``ARROW:extension:metadata`` (parquet-geo-only).

    Returns ``None`` for CRS84/default/CRS-less input (no transform needed).
    """
    geo_meta = parse_geo_metadata_from_schema(table.schema.metadata)
    crs = crs_string_from_geo_meta(geo_meta, geom_col)
    if crs:
        return crs
    return crs_string_for_transform(_crs_from_geoarrow_field(table, geom_col))
