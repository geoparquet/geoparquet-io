"""
GeoJSON conversion for GeoParquet files.

Supports two output modes:
1. Streaming to stdout: Newline-delimited GeoJSON (GeoJSONSeq) with RFC 8142
   record separators for piping to tippecanoe.
2. File output: Standard GeoJSON FeatureCollection using DuckDB's GDAL integration.

Examples:
    # Stream to tippecanoe for PMTiles
    gpio convert geojson input.parquet | tippecanoe -P -o out.pmtiles

    # Pipeline with filtering
    gpio extract in.parquet --bbox ... | gpio convert geojson - | tippecanoe -P -o out.pmtiles

    # Write to file
    gpio convert geojson input.parquet output.geojson
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
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
) -> str:
    """
    Build SQL query that outputs GeoJSON Feature strings.

    Args:
        source_ref: Table reference for SQL query
        geometry_column: Name of geometry column
        property_columns: List of property column names
        precision: Coordinate decimal precision
        write_bbox: Whether to include bbox property
        id_field: Optional field to use as feature id

    Returns:
        SQL query string
    """
    quoted_geom = _quote_identifier(geometry_column)

    # ST_AsGeoJSON doesn't support precision directly in DuckDB
    # We use it as-is; precision is handled by GDAL for file output
    geom_expr = f"ST_AsGeoJSON({quoted_geom})"

    # Build properties expression
    if property_columns:
        prop_pairs = ", ".join(
            f"{_quote_identifier(col)} := {_quote_identifier(col)}" for col in property_columns
        )
        props_expr = f"to_json(struct_pack({prop_pairs}))"
    else:
        props_expr = "'{}'"

    # Build id expression if specified
    id_expr = ""
    if id_field:
        quoted_id = _quote_identifier(id_field)
        id_expr = f"'\"id\":' || to_json({quoted_id}) || ',',"

    # Build bbox expression if requested
    bbox_expr = ""
    if write_bbox:
        bbox_expr = (
            f"'\"bbox\":[' || "
            f"ST_XMin({quoted_geom}) || ',' || "
            f"ST_YMin({quoted_geom}) || ',' || "
            f"ST_XMax({quoted_geom}) || ',' || "
            f"ST_YMax({quoted_geom}) || '],',"
        )

    # Build complete Feature JSON using string concatenation
    query = f"""
        SELECT
            '{{\"type\":\"Feature\",' ||
            {id_expr}
            {bbox_expr}
            '\"geometry\":' ||
            COALESCE({geom_expr}, 'null') ||
            ',\"properties\":' ||
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


def _build_layer_creation_options(
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
) -> str:
    """
    Build GDAL layer creation options string for GeoJSON output.

    Args:
        precision: Coordinate decimal precision
        write_bbox: Whether to write bbox for features
        id_field: Field to use as feature id

    Returns:
        Layer creation options string for GDAL
    """
    options = [
        "RFC7946=YES",  # Use RFC 7946 standard
        f"COORDINATE_PRECISION={precision}",
    ]

    if write_bbox:
        options.append("WRITE_BBOX=YES")

    if id_field:
        options.append(f"ID_FIELD={id_field}")

    return ",".join(options)


def convert_to_geojson_stream(
    input_path: str,
    rs: bool = True,
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
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
        precision: Coordinate decimal precision (default 7 per RFC 7946)
        write_bbox: Include bbox property for each feature
        id_field: Field to use as feature 'id' member
        verbose: Enable verbose output (to stderr)
        profile: AWS profile name for S3 files

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
    from geoparquet_io.core.streaming import is_stdin

    configure_verbose(verbose)

    # Handle stdin input
    if is_stdin(input_path):
        return _convert_from_stream(
            rs=rs,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
            verbose=verbose,
        )

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
        debug(f"Using geometry column: {geometry_column}")

        # Get property columns
        property_columns = _get_property_columns(con, source_ref, geometry_column)
        debug(f"Property columns: {', '.join(property_columns)}")

        # Build and execute query
        query = _build_feature_query(
            source_ref,
            geometry_column,
            property_columns,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
        )
        debug(f"Query: {query}")

        return _stream_to_stdout(con, query, rs)

    finally:
        con.close()


def _convert_from_stream(
    rs: bool = True,
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
    verbose: bool = False,
) -> int:
    """
    Convert Arrow IPC stream from stdin to GeoJSON stream.

    Args:
        rs: Whether to include RFC 8142 record separators
        precision: Coordinate decimal precision
        write_bbox: Include bbox property for each feature
        id_field: Field to use as feature 'id' member
        verbose: Enable verbose output

    Returns:
        Number of features written
    """
    from geoparquet_io.core.common import get_duckdb_connection
    from geoparquet_io.core.logging_config import debug
    from geoparquet_io.core.stream_io import _create_view_with_geometry
    from geoparquet_io.core.streaming import (
        find_geometry_column_from_table,
        read_arrow_stream,
    )

    debug("Reading Arrow IPC stream from stdin...")

    # Read Arrow IPC from stdin
    table = read_arrow_stream()

    debug(f"Read {table.num_rows} rows from stream")

    # Find geometry column
    geometry_column = find_geometry_column_from_table(table)
    if not geometry_column:
        geometry_column = "geometry"

    debug(f"Using geometry column: {geometry_column}")

    # Get property columns
    excluded = {geometry_column.lower(), "bbox"}
    property_columns = [col for col in table.column_names if col.lower() not in excluded]

    debug(f"Property columns: {', '.join(property_columns)}")

    # Register table with DuckDB
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)

    try:
        con.register("input_stream", table)

        # Create view with proper geometry type
        source_ref = _create_view_with_geometry(con, "input_stream", geometry_column)

        # Build and execute query
        query = _build_feature_query(
            source_ref,
            geometry_column,
            property_columns,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
        )
        debug(f"Query: {query}")

        return _stream_to_stdout(con, query, rs)

    finally:
        con.close()


def convert_to_geojson_file(
    input_path: str,
    output_path: str,
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
    verbose: bool = False,
    profile: str | None = None,
) -> int:
    """
    Write GeoParquet to GeoJSON file using GDAL.

    Outputs a standard GeoJSON FeatureCollection.

    Args:
        input_path: Path to input GeoParquet file
        output_path: Path to output GeoJSON file
        precision: Coordinate decimal precision (default 7 per RFC 7946)
        write_bbox: Include bbox property for features
        id_field: Field to use as feature 'id' member
        verbose: Enable verbose output
        profile: AWS profile name for S3 files

    Returns:
        Number of features written
    """
    from geoparquet_io.core.common import (
        get_duckdb_connection,
        needs_httpfs,
        safe_file_url,
        setup_aws_profile_if_needed,
    )
    from geoparquet_io.core.logging_config import configure_verbose, debug, success

    configure_verbose(verbose)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, input_path)

    # Get safe URL for input
    input_url = safe_file_url(input_path, verbose)

    # Build layer creation options
    layer_options = _build_layer_creation_options(
        precision=precision,
        write_bbox=write_bbox,
        id_field=id_field,
    )

    # Create DuckDB connection with GDAL support
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_path))

    try:
        source_ref = f"read_parquet('{input_url}')"

        # Find geometry column
        geometry_column = _find_geometry_column(con, source_ref)
        debug(f"Using geometry column: {geometry_column}")

        # Get row count first
        count_result = con.execute(f"SELECT COUNT(*) FROM {source_ref}").fetchone()
        row_count = count_result[0] if count_result else 0

        # Get columns, excluding bbox (STRUCT type not supported by OGR)
        schema_query = f"SELECT * FROM {source_ref} LIMIT 0"
        result = con.execute(schema_query)
        all_columns = [col[0] for col in result.description]
        columns_to_export = [col for col in all_columns if col.lower() != "bbox"]
        columns_sql = ", ".join(_quote_identifier(col) for col in columns_to_export)

        # Use COPY TO with GDAL driver
        copy_query = f"""
            COPY (
                SELECT {columns_sql} FROM {source_ref}
            ) TO '{output_path}'
            WITH (
                FORMAT GDAL,
                DRIVER 'GeoJSON',
                LAYER_CREATION_OPTIONS '{layer_options}'
            )
        """
        debug(f"COPY query: {copy_query}")

        con.execute(copy_query)

        success(f"Wrote {row_count:,} features to {output_path}")
        return row_count

    finally:
        con.close()


def convert_to_geojson(
    input_path: str,
    output_path: str | None = None,
    rs: bool = True,
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
    verbose: bool = False,
    profile: str | None = None,
) -> int:
    """
    Convert GeoParquet to GeoJSON.

    Routes to streaming mode (stdout) or file mode based on output_path.

    Args:
        input_path: Path to input file, or "-" to read Arrow IPC from stdin
        output_path: Output file path, or None to stream to stdout
        rs: Include RFC 8142 record separators (streaming only)
        precision: Coordinate decimal precision (default 7 per RFC 7946)
        write_bbox: Include bbox property for features
        id_field: Field to use as feature 'id' member
        verbose: Enable verbose output
        profile: AWS profile name for S3 files

    Returns:
        Number of features written
    """
    if output_path:
        return convert_to_geojson_file(
            input_path=input_path,
            output_path=output_path,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
            verbose=verbose,
            profile=profile,
        )
    else:
        return convert_to_geojson_stream(
            input_path=input_path,
            rs=rs,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
            verbose=verbose,
            profile=profile,
        )
