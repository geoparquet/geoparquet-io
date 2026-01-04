"""
GeoJSON conversion for GeoParquet files.

Outputs GeoJSON using DuckDB's ST_AsGeoJSON function. Supports:
1. Streaming to stdout: Newline-delimited GeoJSON (GeoJSONSeq) with RFC 8142
   record separators for piping to tippecanoe.
2. File output: Standard GeoJSON FeatureCollection written to a file.
3. Stdin input: Read Arrow IPC streams from stdin for pipeline use.

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

# WGS84 CRS identifier for RFC 7946 compliance
WGS84_CRS = "EPSG:4326"


def _get_source_crs(input_path: str) -> str | None:
    """
    Get CRS from GeoParquet metadata.

    Args:
        input_path: Path to input GeoParquet file

    Returns:
        CRS string (e.g., "EPSG:4326") or None if not found
    """
    from geoparquet_io.core.common import get_parquet_metadata, parse_geo_metadata

    try:
        metadata, _ = get_parquet_metadata(input_path, verbose=False)
        geo_meta = parse_geo_metadata(metadata, verbose=False)

        if not geo_meta:
            return None

        # Get primary geometry column's CRS
        primary_col = geo_meta.get("primary_column", "geometry")
        columns = geo_meta.get("columns", {})
        col_meta = columns.get(primary_col, {})

        crs = col_meta.get("crs")
        if crs:
            # Extract EPSG code from CRS object
            if isinstance(crs, dict):
                # Check for EPSG identifier
                auth = crs.get("id", {})
                if auth.get("authority") == "EPSG":
                    return f"EPSG:{auth.get('code')}"
                # Fall back to projjson parsing
                return None
            return str(crs)

        return None
    except Exception:
        return None


def _needs_reprojection(source_crs: str | None) -> bool:
    """
    Check if reprojection to WGS84 is needed for RFC 7946 compliance.

    Args:
        source_crs: Source CRS string or None

    Returns:
        True if data needs reprojection to WGS84
    """
    if source_crs is None:
        # No CRS info - assume WGS84
        return False

    # Normalize CRS string for comparison
    crs_upper = source_crs.upper().replace(" ", "")

    # Common WGS84 representations
    wgs84_variants = {
        "EPSG:4326",
        "OGC:CRS84",
        "CRS84",
        "WGS84",
    }

    return crs_upper not in wgs84_variants


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
    source_crs: str | None = None,
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
        source_crs: If provided, reproject from this CRS to WGS84

    Returns:
        SQL query string
    """
    quoted_geom = _quote_identifier(geometry_column)

    # Apply reprojection if needed
    if source_crs:
        # Transform to WGS84 before converting to GeoJSON
        geom_for_output = f"ST_Transform({quoted_geom}, '{source_crs}', '{WGS84_CRS}')"
    else:
        geom_for_output = quoted_geom

    # Apply coordinate precision using ST_ReducePrecision before GeoJSON conversion.
    # The grid size is 10^-precision (e.g., precision=7 -> grid=0.0000001).
    # Note: Very low precision values may collapse small geometries to empty.
    geom_with_precision = f"ST_ReducePrecision({geom_for_output}, pow(10, -{precision}))"
    geom_expr = f"ST_AsGeoJSON({geom_with_precision})"

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

    # Build bbox expression if requested (use reprojected geometry)
    # Bbox coordinates honor the precision parameter via ROUND()
    bbox_expr = ""
    if write_bbox:
        bbox_expr = (
            f"'\"bbox\":[' || "
            f"ROUND(ST_XMin({geom_for_output}), {precision}) || ',' || "
            f"ROUND(ST_YMin({geom_for_output}), {precision}) || ',' || "
            f"ROUND(ST_XMax({geom_for_output}), {precision}) || ',' || "
            f"ROUND(ST_YMax({geom_for_output}), {precision}) || '],',"
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
    pretty: bool = False,
) -> int:
    """
    Stream GeoJSON features to stdout line by line (GeoJSONSeq format).

    Args:
        con: DuckDB connection
        query: SQL query that returns feature JSON strings
        rs: Whether to include RFC 8142 record separators
        pretty: Whether to pretty-print each feature

    Returns:
        Number of features written
    """
    import json

    result = con.execute(query)
    count = 0
    output = sys.stdout

    while True:
        row = result.fetchone()
        if row is None:
            break

        if rs:
            output.write(RS)

        if pretty:
            # Parse and re-serialize with indentation
            feature = json.loads(row[0])
            output.write(json.dumps(feature, indent=2))
        else:
            output.write(row[0])

        output.write("\n")
        count += 1

    output.flush()
    return count


def _stream_feature_collection(
    con: duckdb.DuckDBPyConnection,
    query: str,
    description: str | None = None,
    pretty: bool = False,
    output_path: str | None = None,
) -> int:
    """
    Output GeoJSON as a complete FeatureCollection to stdout or file.

    Args:
        con: DuckDB connection
        query: SQL query that returns feature JSON strings
        description: Optional description for the FeatureCollection
        pretty: Whether to pretty-print the output
        output_path: If provided, write to this file; otherwise write to stdout

    Returns:
        Number of features written
    """
    import json

    result = con.execute(query)
    features = []
    count = 0

    # Collect all features
    while True:
        row = result.fetchone()
        if row is None:
            break
        features.append(json.loads(row[0]))
        count += 1

    # Build FeatureCollection
    fc: dict = {
        "type": "FeatureCollection",
    }

    if description:
        fc["description"] = description

    fc["features"] = features

    # Output to file or stdout
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            if pretty:
                json.dump(fc, f, indent=2)
            else:
                json.dump(fc, f)
            f.write("\n")
    else:
        output = sys.stdout
        if pretty:
            output.write(json.dumps(fc, indent=2))
        else:
            output.write(json.dumps(fc))
        output.write("\n")
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


def convert_to_geojson_stream(
    input_path: str,
    output_path: str | None = None,
    rs: bool = True,
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
    description: str | None = None,
    seq: bool = True,
    pretty: bool = False,
    verbose: bool = False,
    profile: str | None = None,
    keep_crs: bool = False,
) -> int:
    """
    Convert GeoParquet to GeoJSON.

    By default (no output_path), outputs RFC 8142 GeoJSONSeq format (newline-delimited
    features) to stdout, suitable for piping to tippecanoe with the -P (parallel) flag.

    When output_path is provided, writes a FeatureCollection to the file.

    Automatically reprojects to WGS84 (EPSG:4326) for RFC 7946 compliance unless
    keep_crs is True.

    Args:
        input_path: Path to input file, or "-" to read Arrow IPC from stdin
        output_path: Output file path (writes FeatureCollection), or None to stream
        rs: Include RFC 8142 record separators (streaming only, default True)
        precision: Coordinate decimal precision (default 7 per RFC 7946)
        write_bbox: Include bbox property for each feature
        id_field: Field to use as feature 'id' member
        description: Description for FeatureCollection
        seq: If True and no output_path, output GeoJSONSeq; if False, FeatureCollection
        pretty: Pretty-print the JSON output
        verbose: Enable verbose output (to stderr)
        profile: AWS profile name for S3 files
        keep_crs: If True, keep original CRS instead of reprojecting to WGS84

    Returns:
        Number of features written
    """
    from geoparquet_io.core.common import (
        get_duckdb_connection,
        needs_httpfs,
        safe_file_url,
        setup_aws_profile_if_needed,
    )
    from geoparquet_io.core.logging_config import configure_verbose, debug, info, success
    from geoparquet_io.core.streaming import is_stdin

    configure_verbose(verbose)

    # Handle stdin input
    if is_stdin(input_path):
        return _convert_from_stream(
            output_path=output_path,
            rs=rs,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
            description=description,
            seq=seq,
            pretty=pretty,
            verbose=verbose,
            keep_crs=keep_crs,
        )

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, input_path)

    # Get safe URL for input
    input_url = safe_file_url(input_path, verbose)

    # Check if reprojection is needed
    source_crs = _get_source_crs(input_path)
    needs_reproject = not keep_crs and _needs_reprojection(source_crs)
    if needs_reproject:
        info(f"Reprojecting from {source_crs} to WGS84 (RFC 7946)")
        debug(f"Source CRS: {source_crs}, reprojecting to {WGS84_CRS}")

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

        # Build and execute query (pass source_crs if reprojection needed)
        query = _build_feature_query(
            source_ref,
            geometry_column,
            property_columns,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
            source_crs=source_crs if needs_reproject else None,
        )
        debug(f"Query: {query}")

        # File output: always write FeatureCollection
        if output_path:
            count = _stream_feature_collection(
                con, query, description, pretty, output_path=output_path
            )
            success(f"Wrote {count:,} features to {output_path}")
            return count

        # Streaming output: GeoJSONSeq or FeatureCollection to stdout
        if seq:
            return _stream_to_stdout(con, query, rs, pretty)
        else:
            return _stream_feature_collection(con, query, description, pretty)

    finally:
        con.close()


def _convert_from_stream(
    output_path: str | None = None,
    rs: bool = True,
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
    description: str | None = None,
    seq: bool = True,
    pretty: bool = False,
    verbose: bool = False,
    keep_crs: bool = False,
) -> int:
    """
    Convert Arrow IPC stream from stdin to GeoJSON.

    Args:
        output_path: Output file path, or None to stream to stdout
        rs: Whether to include RFC 8142 record separators (streaming only)
        precision: Coordinate decimal precision
        write_bbox: Include bbox property for each feature
        id_field: Field to use as feature 'id' member
        description: Description for FeatureCollection
        seq: If True and no output_path, output GeoJSONSeq; if False, FeatureCollection
        pretty: Pretty-print the JSON output
        verbose: Enable verbose output
        keep_crs: If True, keep original CRS instead of reprojecting to WGS84

    Returns:
        Number of features written

    Note:
        CRS detection from Arrow IPC stream is limited. For pipeline use,
        ensure source data is already in WGS84 or use gpio convert reproject first.
    """
    from geoparquet_io.core.common import get_duckdb_connection
    from geoparquet_io.core.logging_config import debug, info, success
    from geoparquet_io.core.stream_io import _create_view_with_geometry
    from geoparquet_io.core.streaming import (
        find_geometry_column_from_table,
        get_crs_from_arrow_table,
        read_arrow_stream,
    )

    debug("Reading Arrow IPC stream from stdin...")

    # Read Arrow IPC from stdin
    table = read_arrow_stream()

    debug(f"Read {table.num_rows} rows from stream")

    # Find geometry column
    geometry_column = find_geometry_column_from_table(table)
    if not geometry_column:
        available_columns = ", ".join(table.column_names)
        raise ValueError(
            f"No geometry column found in Arrow stream. "
            f"Available columns: [{available_columns}]. "
            "Expected a column named 'geometry', 'geom', 'wkb_geometry', or 'the_geom', "
            "or a column with GeoParquet 'geo' metadata."
        )

    debug(f"Using geometry column: {geometry_column}")

    # Check CRS from Arrow table metadata (if available)
    source_crs = get_crs_from_arrow_table(table, geometry_column)
    needs_reproject = not keep_crs and _needs_reprojection(source_crs)
    if needs_reproject and source_crs:
        info(f"Reprojecting from {source_crs} to WGS84 (RFC 7946)")

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
            source_crs=source_crs if needs_reproject else None,
        )
        debug(f"Query: {query}")

        # File output: always write FeatureCollection
        if output_path:
            count = _stream_feature_collection(
                con, query, description, pretty, output_path=output_path
            )
            success(f"Wrote {count:,} features to {output_path}")
            return count

        # Streaming output: GeoJSONSeq or FeatureCollection to stdout
        if seq:
            return _stream_to_stdout(con, query, rs, pretty)
        else:
            return _stream_feature_collection(con, query, description, pretty)

    finally:
        con.close()


def convert_to_geojson(
    input_path: str,
    output_path: str | None = None,
    rs: bool = True,
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
    description: str | None = None,
    seq: bool = True,
    pretty: bool = False,
    verbose: bool = False,
    profile: str | None = None,
    keep_crs: bool = False,
) -> int:
    """
    Convert GeoParquet to GeoJSON.

    When output_path is provided, writes a FeatureCollection to the file.
    Otherwise, streams to stdout (GeoJSONSeq by default, or FeatureCollection with seq=False).

    Automatically reprojects to WGS84 (EPSG:4326) for RFC 7946 compliance
    unless keep_crs is True.

    Args:
        input_path: Path to input file, or "-" to read Arrow IPC from stdin
        output_path: Output file path, or None to stream to stdout
        rs: Include RFC 8142 record separators (streaming only, seq mode)
        precision: Coordinate decimal precision (default 7 per RFC 7946)
        write_bbox: Include bbox property for features
        id_field: Field to use as feature 'id' member
        description: Description for FeatureCollection
        seq: If True and streaming, output GeoJSONSeq; if False, FeatureCollection
        pretty: Pretty-print the output
        verbose: Enable verbose output
        profile: AWS profile name for S3 files
        keep_crs: If True, keep original CRS instead of reprojecting to WGS84

    Returns:
        Number of features written
    """
    return convert_to_geojson_stream(
        input_path=input_path,
        output_path=output_path,
        rs=rs,
        precision=precision,
        write_bbox=write_bbox,
        id_field=id_field,
        description=description,
        seq=seq,
        pretty=pretty,
        verbose=verbose,
        profile=profile,
        keep_crs=keep_crs,
    )
