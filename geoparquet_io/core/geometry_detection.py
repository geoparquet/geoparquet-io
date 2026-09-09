"""
Geometry column detection for GeoParquet files.

This module provides functions to detect geometry columns in Parquet files
by examining GeoParquet metadata and column names.
"""

import duckdb

from geoparquet_io.core.logging_config import debug

# Standard geometry column names for fallback detection
STANDARD_GEOMETRY_NAMES = ["geometry", "geom", "wkb_geometry", "shape", "the_geom"]


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
    from geoparquet_io.core.file_utils import resolve_file_url
    from geoparquet_io.core.geo_metadata import carried_column_name
    from geoparquet_io.core.remote import needs_httpfs

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
    from geoparquet_io.core.geo_metadata import carried_column_name

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
        columns = [row[0].lower() for row in describe_result if row[0]]

        if original_metadata and "primary_column" in original_metadata:
            primary = original_metadata["primary_column"].lower()
            if primary in columns:
                if verbose:
                    debug(f"Found original primary geometry column in query: {primary}")
                return original_metadata["primary_column"]

        for std_name in STANDARD_GEOMETRY_NAMES:
            if std_name in columns:
                if verbose:
                    debug(f"Found geometry column by standard name in query: {std_name}")
                return std_name

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
