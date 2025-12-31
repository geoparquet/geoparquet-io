#!/usr/bin/env python3
"""
GeoJSON streaming output for Unix-style piping to tools like tippecanoe.

Outputs newline-delimited GeoJSON (GeoJSONSeq/NDJSON) suitable for:
    gpio convert input.parquet --to geojson | tippecanoe -P -o out.pmtiles

Supports RFC 8142 record separators (enabled by default) for tippecanoe
auto-parallel mode.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

# RFC 8142 record separator character
RS = "\x1e"


def _quote_identifier(name: str) -> str:
    """Quote a SQL identifier for safe use in DuckDB queries."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _get_property_columns(
    con: duckdb.DuckDBPyConnection,
    source_ref: str,
    geometry_column: str,
) -> list[str]:
    """
    Get list of property columns (all columns except geometry and bbox).

    Args:
        con: DuckDB connection
        source_ref: Table reference for SQL query
        geometry_column: Name of geometry column to exclude

    Returns:
        List of column names to include as properties
    """
    # Get schema from the source
    schema_query = f"SELECT * FROM {source_ref} LIMIT 0"
    result = con.execute(schema_query)
    columns = [col[0] for col in result.description]

    # Exclude geometry column and bbox
    excluded = {geometry_column.lower(), "bbox"}
    return [col for col in columns if col.lower() not in excluded]


def _build_feature_query(
    source_ref: str,
    geometry_column: str,
    property_columns: list[str],
    precision: int | None = None,
) -> str:
    """
    Build SQL query that outputs GeoJSON Feature strings.

    Args:
        source_ref: Table reference for SQL query
        geometry_column: Name of geometry column
        property_columns: List of property column names
        precision: Optional coordinate precision (decimal places)

    Returns:
        SQL query string
    """
    quoted_geom = _quote_identifier(geometry_column)

    # Build geometry expression
    # Note: DuckDB's ST_AsGeoJSON doesn't support precision parameter
    # We'd need to use ROUND on coordinates if precision is needed
    geom_expr = f"ST_AsGeoJSON({quoted_geom})"

    # Build properties expression
    if property_columns:
        prop_pairs = ", ".join(
            f"{_quote_identifier(col)} := {_quote_identifier(col)}" for col in property_columns
        )
        props_expr = f"to_json(struct_pack({prop_pairs}))"
    else:
        props_expr = "'{}'"

    # Build complete Feature JSON using string concatenation
    query = f"""
        SELECT
            '{{"type":"Feature","geometry":' ||
            COALESCE({geom_expr}, 'null') ||
            ',"properties":' ||
            {props_expr} ||
            '}}' AS feature
        FROM {source_ref}
        WHERE {quoted_geom} IS NOT NULL
    """

    return query


def _stream_to_stdout(
    con: duckdb.DuckDBPyConnection,
    query: str,
    rs: bool = True,
) -> int:
    """
    Stream GeoJSON features to stdout line by line.

    Args:
        con: DuckDB connection
        query: SQL query that returns feature JSON strings
        rs: Whether to include RFC 8142 record separators

    Returns:
        Number of features written
    """
    result = con.execute(query)
    count = 0
    output = sys.stdout

    while True:
        row = result.fetchone()
        if row is None:
            break

        if rs:
            output.write(RS)

        output.write(row[0])
        output.write("\n")
        count += 1

    output.flush()
    return count


def _find_geometry_column(
    con: duckdb.DuckDBPyConnection,
    source_ref: str,
) -> str:
    """
    Find the geometry column in a table.

    Args:
        con: DuckDB connection
        source_ref: Table reference for SQL query

    Returns:
        Name of geometry column

    Raises:
        ValueError: If no geometry column found
    """
    schema_query = f"SELECT * FROM {source_ref} LIMIT 0"
    result = con.execute(schema_query)

    # Look for common geometry column names
    columns = [col[0] for col in result.description]
    common_names = ["geometry", "geom", "wkb_geometry", "the_geom", "shape"]

    for name in common_names:
        for col in columns:
            if col.lower() == name:
                return col

    # Check column types for GEOMETRY type
    for col in columns:
        try:
            type_query = f"SELECT typeof({_quote_identifier(col)}) FROM {source_ref} LIMIT 1"
            type_result = con.execute(type_query).fetchone()
            if type_result and "GEOMETRY" in str(type_result[0]).upper():
                return col
        except Exception:
            continue

    raise ValueError(
        "Could not find geometry column. "
        "Expected column named 'geometry', 'geom', 'wkb_geometry', or 'the_geom'."
    )


def _convert_from_file(
    input_path: str,
    rs: bool = True,
    precision: int | None = None,
    verbose: bool = False,
    profile: str | None = None,
) -> int:
    """
    Convert a GeoParquet file to GeoJSON stream.

    Args:
        input_path: Path to input file
        rs: Whether to include RFC 8142 record separators
        precision: Optional coordinate precision
        verbose: Enable verbose output
        profile: AWS profile for S3 files

    Returns:
        Number of features written
    """
    from geoparquet_io.core.common import (
        get_duckdb_connection,
        needs_httpfs,
        safe_file_url,
        setup_aws_profile_if_needed,
    )
    from geoparquet_io.core.logging_config import configure_verbose, debug

    configure_verbose(verbose)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, input_path)

    # Get safe URL for input
    input_url = safe_file_url(input_path, verbose)

    # Create DuckDB connection
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_path))

    try:
        source_ref = f"read_parquet('{input_url}')"

        # Find geometry column
        geometry_column = _find_geometry_column(con, source_ref)
        if verbose:
            debug(f"Using geometry column: {geometry_column}")

        # Get property columns
        property_columns = _get_property_columns(con, source_ref, geometry_column)
        if verbose:
            debug(f"Property columns: {', '.join(property_columns)}")

        # Build and execute query
        query = _build_feature_query(source_ref, geometry_column, property_columns, precision)
        if verbose:
            debug(f"Query: {query}")

        return _stream_to_stdout(con, query, rs)

    finally:
        con.close()


def _convert_from_stream(
    rs: bool = True,
    precision: int | None = None,
    verbose: bool = False,
) -> int:
    """
    Convert Arrow IPC stream from stdin to GeoJSON stream.

    Args:
        rs: Whether to include RFC 8142 record separators
        precision: Optional coordinate precision
        verbose: Enable verbose output

    Returns:
        Number of features written
    """
    from geoparquet_io.core.common import get_duckdb_connection
    from geoparquet_io.core.logging_config import configure_verbose, debug
    from geoparquet_io.core.stream_io import _create_view_with_geometry
    from geoparquet_io.core.streaming import (
        find_geometry_column_from_table,
        read_arrow_stream,
    )

    configure_verbose(verbose)

    if verbose:
        debug("Reading Arrow IPC stream from stdin...")

    # Read Arrow IPC from stdin
    table = read_arrow_stream()

    if verbose:
        debug(f"Read {table.num_rows} rows from stream")

    # Find geometry column
    geometry_column = find_geometry_column_from_table(table)
    if not geometry_column:
        geometry_column = "geometry"

    if verbose:
        debug(f"Using geometry column: {geometry_column}")

    # Get property columns
    excluded = {geometry_column.lower(), "bbox"}
    property_columns = [col for col in table.column_names if col.lower() not in excluded]

    if verbose:
        debug(f"Property columns: {', '.join(property_columns)}")

    # Register table with DuckDB
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)

    try:
        con.register("input_stream", table)

        # Create view with proper geometry type
        source_ref = _create_view_with_geometry(con, "input_stream", geometry_column)

        # Build and execute query
        query = _build_feature_query(source_ref, geometry_column, property_columns, precision)
        if verbose:
            debug(f"Query: {query}")

        return _stream_to_stdout(con, query, rs)

    finally:
        con.close()


def convert_to_geojson(
    input_path: str,
    rs: bool = True,
    precision: int | None = None,
    verbose: bool = False,
    profile: str | None = None,
) -> int:
    """
    Stream GeoParquet as newline-delimited GeoJSON to stdout.

    Outputs RFC 8142 GeoJSONSeq format by default, suitable for piping to
    tippecanoe with the -P (parallel) flag.

    Args:
        input_path: Path to input file, or "-" to read Arrow IPC from stdin
        rs: Include RFC 8142 record separators (default True for tippecanoe)
        precision: Optional coordinate decimal precision
        verbose: Enable verbose output (to stderr)
        profile: AWS profile name for S3 files

    Returns:
        Number of features written

    Example:
        # Direct file to tippecanoe
        gpio convert input.parquet --to geojson | tippecanoe -P -o out.pmtiles

        # Pipeline
        gpio extract in.parquet --bbox ... | gpio convert - --to geojson | tippecanoe -P -o out.pmtiles
    """
    from geoparquet_io.core.streaming import is_stdin

    if is_stdin(input_path):
        return _convert_from_stream(rs=rs, precision=precision, verbose=verbose)
    else:
        return _convert_from_file(
            input_path, rs=rs, precision=precision, verbose=verbose, profile=profile
        )
