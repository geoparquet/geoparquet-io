"""
Geometry column detection for GeoParquet files.

This module provides functions to detect geometry columns in Parquet files
by examining GeoParquet metadata and column names.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import duckdb

from geoparquet_io.core.duckdb_utils import (
    _geoarrow_coord_exprs,
    quote_identifier,
    register_arrow,
    sql_path,
)
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geo_metadata import carried_column_name
from geoparquet_io.core.logging_config import debug
from geoparquet_io.core.optional_deps import load_module
from geoparquet_io.core.remote import needs_httpfs

# Standard geometry column names for fallback detection
STANDARD_GEOMETRY_NAMES = ["geometry", "geom", "wkb_geometry", "shape", "the_geom"]

# How DuckDB's DESCRIBE renders a geometry type: `GEOMETRY`, or
# `GEOMETRY('EPSG:3857')` once it carries a CRS. Anchored at both ends so a
# struct or list type that merely mentions GEOMETRY is not matched. The CRS is a
# SQL string literal, so an embedded `'` arrives doubled (`O''Brien`).
_DESCRIBE_GEO_TYPE = re.compile(
    r"^(GEOMETRY|GEOGRAPHY)(?:\(\s*'((?:[^']|'')*)'\s*\))?$", re.IGNORECASE | re.DOTALL
)


def detect_geometry_column_from_names(column_names) -> str | None:
    """First standard geometry name present in ``column_names``, matched case-insensitively.

    The name-based half of detection, split out so the callers that already
    hold a schema -- an in-memory ``pa.Table`` in ``reproject``, a CRS reader
    that has to answer "which column did the block mean?" -- do not fall back
    to the literal string ``"geometry"`` and miss a ``geom`` file (#887 review).
    """
    by_lowered: dict[str, str] = {}
    for name in column_names:
        by_lowered.setdefault(str(name).lower(), name)
    for std_name in STANDARD_GEOMETRY_NAMES:
        if std_name in by_lowered:
            return by_lowered[std_name]
    return None


def detect_parquet_geometry_column(parquet_file: str, verbose: bool = False) -> str | None:
    """
    Detect the geometry column in a Parquet file.

    Checks GeoParquet metadata first (primary_column), then falls back to
    matching column names against standard geometry column names.

    Args:
        parquet_file: Path to the Parquet file (local or remote).
        verbose: If True, print debug information.

    Returns:
        The name of the detected geometry column, or None if not found.
    """
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

    # Normalize path for consistent handling of URLs and local files
    raw_url = resolve_file_url(parquet_file, verbose=False)

    # 1. Check GeoParquet metadata first. A carried `primary_column` that is not
    # a string is somebody else's malformed block: `carried_column_name` names
    # the ignored key in a warning and answers None, so detection falls through
    # to the schema rather than handing `quote_identifier` a number (#887).
    geo_meta = get_geo_metadata(parquet_file)
    if geo_meta and isinstance(geo_meta, dict):
        primary = carried_column_name(geo_meta.get("primary_column"), source=str(parquet_file))
        if primary:
            if verbose:
                debug(f"Detected geometry column from metadata: {primary}")
            return primary

    # 2. Fall back to name-based detection from schema using DuckDB
    con = None
    try:
        con = get_duckdb_connection(load_httpfs=needs_httpfs(raw_url))
        result = con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_path(raw_url)})").fetchall()
        col = detect_geometry_column_from_names(row[0] for row in result)
        if col:
            if verbose:
                debug(f"Detected geometry column from schema: {col}")
            return col
    except (OSError, duckdb.InvalidInputException) as e:
        # OSError for file access issues
        # InvalidInputException for DuckDB rejecting invalid GeoParquet metadata
        if verbose:
            debug(f"Failed to read schema: {e}")
    finally:
        if con:
            con.close()

    if verbose:
        debug("No geometry column found in parquet file")
    return None


def _crs_short(crs) -> str:
    """Short CRS label (e.g. 'EPSG:28992') for verbose output, avoiding full PROJJSON."""
    if crs is None:
        return "none"
    if isinstance(crs, str):
        return crs
    if isinstance(crs, dict):
        cid = crs.get("id")
        if isinstance(cid, dict):
            return f"{cid.get('authority', '?')}:{cid.get('code', '?')}"
        return crs.get("name", "custom")
    return "custom"


def _summarize_geo_metadata(geo_meta) -> str:
    """One-line geo-metadata summary for verbose output (no full CRS dump)."""
    if not isinstance(geo_meta, dict):
        return str(geo_meta)
    columns = geo_meta.get("columns", {})
    cols = (
        "; ".join(
            f"{name}(encoding={col.get('encoding', '?')}, crs={_crs_short(col.get('crs'))}, "
            f"types={col.get('geometry_types', [])})"
            for name, col in columns.items()
        )
        if isinstance(columns, dict)
        else ""
    )
    return (
        f"version={geo_meta.get('version', '?')}, "
        f"primary_column={geo_meta.get('primary_column', '?')}, columns=[{cols}]"
    )


def find_primary_geometry_column(parquet_file: str, verbose: bool = False) -> str:
    """
    Find the primary geometry column from GeoParquet metadata.

    Looks up the geometry column name from GeoParquet metadata. Falls back
    to detect_parquet_geometry_column() (which checks standard geometry column
    names in the schema) if no metadata is present or if the primary column
    is not specified. Final fallback is 'geometry'.

    Args:
        parquet_file: Path to the parquet file (local or remote URL)
        verbose: Print verbose output

    Returns:
        str: Name of the primary geometry column (defaults to 'geometry')
    """
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata

    geo_meta = get_geo_metadata(parquet_file)

    if verbose and geo_meta:
        debug(f"Geo metadata: {_summarize_geo_metadata(geo_meta)}")

    # Only a string is a column name. A non-string one reaches
    # `quote_identifier` and fails with a bare TypeError (#887), so
    # `carried_column_name` warns that the key was ignored and the schema
    # fallback below gives the answer the file's data supports.
    source = str(parquet_file)
    if geo_meta:
        if isinstance(geo_meta, dict):
            primary = carried_column_name(geo_meta.get("primary_column"), source=source)
            if primary:
                return primary
        elif isinstance(geo_meta, list):
            for col in geo_meta:
                if isinstance(col, dict) and col.get("primary", False):
                    name = carried_column_name(col.get("name"), key="name", source=source)
                    if name:
                        return name

    # No geo metadata or no primary_column specified - use schema-based detection
    detected = detect_parquet_geometry_column(parquet_file, verbose=verbose)
    return detected if detected else "geometry"


def native_geometry_types_from_query(con, query: str, verbose: bool = False) -> dict[str, str]:
    """``{column: logical type string}`` for every geometry column the query emits.

    The output query's own ``DESCRIBE`` already names them and the CRS each one
    carries -- ``GEOMETRY``, ``GEOMETRY('EPSG:3857')`` -- so it answers "which
    geometry columns does this write emit, and in which CRS" with **no input-file
    witness at all**. That is what lets ``partition`` staging, whose per-partition
    write passes none, describe a secondary native geometry column (#1175).

    Rendered in the ``GeometryType(crs=EPSG:3857)`` spelling
    :func:`~geoparquet_io.core.duckdb_metadata.parse_geometry_logical_type` reads,
    so one derivation (``derive_secondary_geometry_info``) serves both this and an
    input's own footer. The CRS arrives as a bare authority reference rather than
    the inline PROJJSON a logical type carries, which is why a caller that *has* a
    witness prefers it.

    DuckDB has no ``GEOGRAPHY`` type, so this source can never report ``edges``;
    those travel separately, through ``write_funnels.apply_nonplanar_edges``.

    Best-effort: a ``DESCRIBE`` that fails answers ``{}`` rather than failing the
    write.
    """
    try:
        described = con.execute(f"DESCRIBE ({query})").fetchall()
    except Exception as e:  # noqa: BLE001 - a probe, never the write's failure
        if verbose:
            debug(f"Could not read geometry types off the output query: {e}")
        return {}

    native: dict[str, str] = {}
    for row in described:
        name, col_type = row[0], str(row[1] if len(row) > 1 else "")
        match = _DESCRIBE_GEO_TYPE.match(col_type.strip())
        if not name or not match:
            continue
        family = "Geography" if match.group(1).upper() == "GEOGRAPHY" else "Geometry"
        crs = match.group(2).replace("''", "'") if match.group(2) else None
        native[name] = f"{family}Type(crs={crs})" if crs else f"{family}Type()"
    return native


def _detect_geometry_from_query(
    con, query: str, original_metadata: dict | None = None, verbose: bool = False
) -> str | None:
    """
    Detect geometry column from a SQL query result.

    Args:
        con: DuckDB connection
        query: SQL query to analyze
        original_metadata: Optional GeoParquet metadata from source file
        verbose: If True, print debug information

    Returns:
        The name of the detected geometry column, or None if not found.
    """
    try:
        describe_result = con.execute(f"DESCRIBE ({query})").fetchall()
        # Lowercase key -> the column's own spelling. The match below is
        # case-insensitive, but what is returned has to be the name the schema
        # actually carries: it becomes the `geo.columns` key, and a lowercased
        # `GEOMETRY` names a column the written file does not contain (#1138).
        columns = {row[0].lower(): row[0] for row in describe_result if row[0]}

        if original_metadata and "primary_column" in original_metadata:
            primary = original_metadata["primary_column"].lower()
            if primary in columns:
                if verbose:
                    debug(f"Found original primary geometry column in query: {primary}")
                return original_metadata["primary_column"]

        for std_name in STANDARD_GEOMETRY_NAMES:
            if std_name in columns:
                if verbose:
                    debug(f"Found geometry column by standard name in query: {columns[std_name]}")
                return columns[std_name]

        for row in describe_result:
            col_name, col_type = row[0], row[1] if len(row) > 1 else ""
            if col_type and "GEOMETRY" in str(col_type).upper():
                if verbose:
                    debug(f"Found geometry column by type in query: {col_name}")
                return col_name

        if verbose:
            debug("No geometry column detected in query result")
        return None

    except Exception as e:
        if verbose:
            debug(f"Error detecting geometry from query: {e}")
        return None


# =============================================================================
# How a geometry column is read (#624, #1082)
# =============================================================================

# A GeoArrow coordinate struct, bare (a point) or wrapped in one list level per
# nesting step, so `STRUCT(x DOUBLE, y DOUBLE)[][]` is a polygon's rings.
_GEOARROW_STRUCT_TYPE = re.compile(r"^STRUCT\((.*)\)((?:\[\])*)$", re.IGNORECASE | re.DOTALL)

# List depth of a GeoArrow column -> an encoding whose coordinate path it
# exposes. Depth alone cannot tell linestring from multipoint, nor polygon from
# multilinestring, but both twins share a coordinate path, which is all a reader
# of coordinates needs.
_GEOARROW_ENCODING_BY_DEPTH = {0: "point", 1: "linestring", 2: "polygon", 3: "multipolygon"}

_READABLE_KINDS = frozenset({"native", "wkb", "geoarrow"})


def _geoarrow_struct_depth(duckdb_type: str) -> int | None:
    """List depth of a GeoArrow coordinate struct type, None when it is not one."""
    match = _GEOARROW_STRUCT_TYPE.match(duckdb_type.strip())
    if not match:
        return None
    fields = {f.strip().split(" ")[0].strip('"').lower() for f in match.group(1).split(",")}
    if not {"x", "y"} <= fields:
        return None
    return len(match.group(2)) // 2


def _read_kind(duckdb_type: str | None) -> str:
    if duckdb_type is None:
        return "missing"
    text = duckdb_type.strip()
    if _DESCRIBE_GEO_TYPE.match(text):
        return "native"
    if text.upper() == "BLOB":
        return "wkb"
    if _geoarrow_struct_depth(text) in _GEOARROW_ENCODING_BY_DEPTH:
        return "geoarrow"
    return "unsupported"


@dataclass(frozen=True)
class GeometryRead:
    """The one answer to "how does SQL read this geometry column?".

    Every check that reads geometry needs the same facts, and each used to
    guess them from ``"GEOMETRY" in col_type`` (#624). The expression that
    yields a GEOMETRY, the predicate for "this row holds a value" and, when the
    column cannot be read at all, a sentence saying why. ``kind`` is one of
    ``native`` (a GEOMETRY column), ``wkb`` (a BLOB of WKB), ``geoarrow`` (a
    coordinate struct), ``missing`` or ``unsupported``.

    A zero-length BLOB is not WKB (#1082), and neither are bytes that do not
    parse (#1064). ``expr`` maps both to NULL and ``present_sql`` excludes them,
    so a check reads the other rows instead of dying on them. Only an encoding
    check goes looking for them, through ``empty_sql`` and ``unparsable_sql``.

    ``flat`` says the column reaches SQL as flat vectors, as a registered
    non-dictionary Arrow column does. The ``list_extract`` guard against
    dictionary vectors (see ``parseable_sql``) copies every value, which over
    a whole geometry column is a second copy of it, so a flat read skips it.
    """

    column: str
    kind: str
    duckdb_type: str = ""
    flat: bool = False

    @classmethod
    def for_type(cls, column: str, duckdb_type: str | None, *, flat: bool = False) -> GeometryRead:
        return cls(column, _read_kind(duckdb_type), duckdb_type or "", flat)

    @property
    def readable(self) -> bool:
        return self.kind in _READABLE_KINDS

    @property
    def quoted(self) -> str:
        return quote_identifier(self.column)

    @property
    def expr(self) -> str:
        """SQL for the value as GEOMETRY (NULL for a NULL or zero-length value).

        For a GeoArrow column this is a representative point, the centre of the
        value's coordinate extent, which is what a distance-based reader needs.
        """
        if self.kind == "wkb" and self.flat:
            q = self.quoted
            return f"(CASE WHEN octet_length({q}) > 0 THEN TRY(ST_GeomFromWKB({q})) END)"
        if self.kind == "wkb":
            # CASE, not a WHERE conjunct, so no evaluation order can hand a bad
            # value to ST_GeomFromWKB. The THEN branch reads a flat vector as
            # well. On a dictionary vector DuckDB evaluates a bare call over the
            # whole dictionary, ignoring the CASE, and raises on its bad entry.
            flat = f"list_extract([{self.quoted}], 1)"
            return f"(CASE WHEN {self.parseable_sql} THEN ST_GeomFromWKB({flat}) END)"
        if self.kind == "geoarrow":
            return self._geoarrow_point_expr()
        return self.quoted

    @property
    def shape_expr(self) -> str:
        """SQL for the whole value as GEOMETRY, never a representative point.

        ``expr`` for a native or WKB column. Any other column is handed over
        bare, so a function that needs a real shape (a bbox, a sort key, a
        validity check) fails on it as it always did rather than reading the
        GeoArrow centre point ``expr`` gives.
        """
        return self.expr if self.kind in ("native", "wkb") else self.quoted

    @property
    def present_sql(self) -> str:
        """Predicate for a row that holds a geometry value to read."""
        return f"{self.quoted} IS NOT NULL AND {self.parseable_sql}"

    @property
    def parseable_sql(self) -> str:
        """Predicate for a value SQL can read as a geometry.

        For WKB, non-empty bytes that ``ST_GeomFromWKB`` parses. The length test
        comes first because ``TRY(ST_GeomFromWKB(''))`` is not NULL, it is a
        value every later function rejects. ``list_extract([v], 1)`` hands
        ``TRY`` a flat vector. On a dictionary-encoded column, pyarrow's
        default, one failing row otherwise nulls every row of the vector
        (measured on DuckDB 1.5.6, see tests/test_validate_malformed_wkb.py).
        """
        if self.kind != "wkb":
            return "TRUE"
        q = self.quoted
        if self.flat:
            return f"(octet_length({q}) > 0 AND TRY(ST_GeomFromWKB({q})) IS NOT NULL)"
        return (
            f"(octet_length({q}) > 0 AND TRY(ST_GeomFromWKB(list_extract([{q}], 1))) IS NOT NULL)"
        )

    @property
    def empty_sql(self) -> str:
        """Predicate for a value that is present but zero-length (never valid WKB)."""
        if self.kind == "wkb":
            return f"octet_length({self.quoted}) = 0"
        return "FALSE"

    @property
    def unparsable_sql(self) -> str:
        """Predicate for a non-empty value that is not valid WKB."""
        if self.kind == "wkb":
            return f"(octet_length({self.quoted}) > 0 AND NOT {self.parseable_sql})"
        return "FALSE"

    @property
    def problem(self) -> str | None:
        """Why this column cannot be read, or None when it can."""
        if self.kind == "missing":
            return f'geometry column "{self.column}" not found in the file'
        if self.kind == "unsupported":
            return (
                f'geometry column "{self.column}" has type {self.duckdb_type}, '
                "which is not WKB, a GEOMETRY type or a GeoArrow coordinate struct"
            )
        return None

    def _geoarrow_point_expr(self) -> str:
        depth = _geoarrow_struct_depth(self.duckdb_type) or 0
        encoding = _GEOARROW_ENCODING_BY_DEPTH[depth]
        _, _, _, _, cx, cy = _geoarrow_coord_exprs(self.quoted, encoding)
        return f"ST_Point({cx}, {cy})"


@dataclass(frozen=True)
class GeometryPresence:
    """Row counts for a geometry column, and how many hold no readable value.

    The ``first_*_row`` fields are 0-based row indexes in the file, so a user
    can find the value.
    """

    total: int
    nulls: int
    empties: int
    first_empty_row: int | None
    unparsable: int = 0
    first_unparsable_row: int | None = None


def _find_described_type(described, column: str) -> str | None:
    by_name = {row[0]: str(row[1]) for row in described if row and row[0]}
    if column in by_name:
        return by_name[column]
    folded = {name.lower(): col_type for name, col_type in by_name.items()}
    return folded.get(column.lower())


def resolve_geometry_read(con, raw_url: str, column: str) -> GeometryRead:
    """How ``column`` of the Parquet file at RAW ``raw_url`` is read on ``con``.

    The answer depends on the connection. With GeoParquet conversion on, DuckDB
    presents a WKB column as GEOMETRY and decodes it lazily, so a caller that
    must see the stored bytes calls ``disable_geoparquet_conversion`` first.
    """
    described = con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_path(raw_url)})").fetchall()
    return GeometryRead.for_type(column, _find_described_type(described, column))


def register_geometry_column(
    con, name: str, table, column: str, *, row_index: str | None = None
) -> GeometryRead:
    """Register only ``column`` of the Arrow ``table`` on ``con`` as ``name``.

    The one way a table-centric helper hands geometry to DuckDB (#1268): the
    other columns never cross, so nothing but the geometry is scanned and the
    caller's table is never rebuilt from a DuckDB result. Registration is
    zero-copy. With ``row_index``, an int64 column of that name numbering the
    rows from 0 is registered beside it, for a caller that wants positions
    back. A dictionary-typed column is decoded first, so the reader can use
    the flat-vector WKB parse (see ``GeometryRead.flat``).

    Returns how SQL reads the registered column.
    """
    np = load_module("numpy")
    import pyarrow as pa

    geometry = table.column(column)
    if pa.types.is_dictionary(geometry.type):
        geometry = geometry.cast(geometry.type.value_type)
    columns = {}
    if row_index is not None:
        columns[row_index] = pa.array(np.arange(table.num_rows, dtype=np.int64))
    columns[column] = geometry
    register_arrow(con, name, pa.table(columns))
    described = con.execute(f"DESCRIBE {quote_identifier(name)}").fetchall()
    return GeometryRead.for_type(column, _find_described_type(described, column), flat=True)


def count_geometry_presence(
    con, raw_url: str, read: GeometryRead, limit_clause: str
) -> GeometryPresence:
    """Count rows, NULL, zero-length and unparsable values over the sampled rows."""
    q = read.quoted
    empty, bad = read.empty_sql, read.unparsable_sql
    row = con.execute(f"""
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE {q} IS NULL),
               COUNT(*) FILTER (WHERE {empty}),
               MIN(file_row_number) FILTER (WHERE {empty}),
               COUNT(*) FILTER (WHERE {bad}),
               MIN(file_row_number) FILTER (WHERE {bad})
        FROM (
            SELECT {q}, file_row_number
            FROM read_parquet({sql_path(raw_url)}, file_row_number = true)
            {limit_clause}
        )
    """).fetchone()
    return GeometryPresence(*row) if row else GeometryPresence(0, 0, 0, None)
