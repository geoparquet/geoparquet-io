"""
Extract columns and rows from GeoParquet files.

Supports column selection, spatial filtering (bbox, geometry),
SQL filtering, and multiple input files via glob patterns.
"""

import json
import sys
from pathlib import Path

import click
import fsspec
import pyarrow.parquet as pq

from geoparquet_io.core.common import (
    check_bbox_structure,
    find_primary_geometry_column,
    get_duckdb_connection,
    get_parquet_metadata,
    needs_httpfs,
    safe_file_url,
    write_parquet_with_metadata,
)


def parse_bbox(bbox_str: str) -> tuple[float, float, float, float]:
    """
    Parse bounding box string into tuple of floats.

    Args:
        bbox_str: Comma-separated string "xmin,ymin,xmax,ymax"

    Returns:
        tuple: (xmin, ymin, xmax, ymax)

    Raises:
        click.ClickException: If format is invalid
    """
    try:
        parts = [float(x.strip()) for x in bbox_str.split(",")]
        if len(parts) != 4:
            raise click.ClickException(
                f"Invalid bbox format. Expected 4 values (xmin,ymin,xmax,ymax), got {len(parts)}"
            )
        return tuple(parts)
    except ValueError as e:
        raise click.ClickException(
            f"Invalid bbox format. Expected numeric values: xmin,ymin,xmax,ymax. Error: {e}"
        ) from e


def convert_geojson_to_wkt(geojson: dict) -> str:
    """
    Convert GeoJSON geometry to WKT using DuckDB.

    Args:
        geojson: GeoJSON geometry dict

    Returns:
        str: WKT representation
    """
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        geojson_str = json.dumps(geojson).replace("'", "''")
        result = con.execute(f"""
            SELECT ST_AsText(ST_GeomFromGeoJSON('{geojson_str}'))
        """).fetchone()
        return result[0]
    finally:
        con.close()


def parse_geometry_input(geometry_input: str, use_first: bool = False) -> str:
    """
    Parse geometry from various input formats.

    Supports:
    - Inline GeoJSON: {"type": "Polygon", ...}
    - Inline WKT: POLYGON((...))
    - File reference: @path/to/file.geojson or @path/to/file.wkt
    - Stdin: - (reads from sys.stdin)
    - Auto-detect file: path/to/file.geojson (if file exists)

    Args:
        geometry_input: Geometry string, file path, or "-" for stdin
        use_first: If True, use first geometry from FeatureCollection

    Returns:
        str: WKT representation of the geometry

    Raises:
        click.ClickException: If geometry cannot be parsed or multiple geometries found
    """
    original_input = geometry_input

    # Handle stdin
    if geometry_input == "-":
        if sys.stdin.isatty():
            raise click.ClickException(
                "No geometry provided on stdin. Pipe geometry data or use @file syntax."
            )
        geometry_input = sys.stdin.read().strip()

    # Handle file reference (@filepath or auto-detect)
    file_path = None
    if geometry_input.startswith("@"):
        file_path = geometry_input[1:]
    elif not geometry_input.strip().startswith(("{", "POLYGON", "POINT", "LINESTRING", "MULTI")):
        # Check if it looks like a file path
        potential_path = Path(geometry_input)
        if potential_path.exists() and potential_path.suffix.lower() in (
            ".geojson",
            ".json",
            ".wkt",
        ):
            file_path = geometry_input

    if file_path:
        path = Path(file_path)
        if not path.exists():
            raise click.ClickException(f"Geometry file not found: {file_path}")
        geometry_input = path.read_text().strip()

    # Parse GeoJSON to WKT if needed
    if geometry_input.strip().startswith("{"):
        try:
            geojson = json.loads(geometry_input)
        except json.JSONDecodeError as e:
            raise click.ClickException(f"Invalid GeoJSON: {e}") from e

        # Handle FeatureCollection - extract geometry
        if geojson.get("type") == "FeatureCollection":
            features = geojson.get("features", [])
            if not features:
                raise click.ClickException("FeatureCollection is empty - no geometries found")
            if len(features) > 1 and not use_first:
                raise click.ClickException(
                    f"Multiple geometries ({len(features)}) found in FeatureCollection. "
                    "Use --use-first-geometry to use only the first geometry."
                )
            geojson = features[0].get("geometry")
            if not geojson:
                raise click.ClickException("First feature has no geometry")
        elif geojson.get("type") == "Feature":
            geojson = geojson.get("geometry")
            if not geojson:
                raise click.ClickException("Feature has no geometry")

        # Convert GeoJSON to WKT
        try:
            wkt = convert_geojson_to_wkt(geojson)
        except Exception as e:
            raise click.ClickException(f"Failed to convert GeoJSON to WKT: {e}") from e
    else:
        # Assume it's WKT - validate it
        wkt = geometry_input.strip()
        valid_prefixes = (
            "POINT",
            "LINESTRING",
            "POLYGON",
            "MULTIPOINT",
            "MULTILINESTRING",
            "MULTIPOLYGON",
            "GEOMETRYCOLLECTION",
        )
        if not any(wkt.upper().startswith(prefix) for prefix in valid_prefixes):
            raise click.ClickException(
                f"Could not parse geometry input as GeoJSON or WKT.\n"
                f"Input: {original_input[:100]}{'...' if len(original_input) > 100 else ''}"
            )

    return wkt


def get_schema_columns(input_parquet: str) -> list[str]:
    """
    Get list of column names from parquet file schema.

    Args:
        input_parquet: Path to parquet file (or glob pattern - uses first file)

    Returns:
        list: Column names
    """
    # For glob patterns, we need to get schema from one file
    # DuckDB can handle this for us
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))
    try:
        safe_url = safe_file_url(input_parquet, verbose=False)
        result = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{safe_url}')").fetchall()
        return [row[0] for row in result]
    finally:
        con.close()


def build_column_selection(
    all_columns: list[str],
    include_cols: list[str] | None,
    exclude_cols: list[str] | None,
    geometry_col: str,
    bbox_col: str | None,
) -> list[str]:
    """
    Build list of columns to select.

    Rules:
    - If include_cols: select only those + geometry + bbox (unless explicitly excluded)
    - If exclude_cols: select all except those
    - geometry and bbox always included unless in exclude_cols

    Args:
        all_columns: All columns in schema
        include_cols: Columns to include (or None)
        exclude_cols: Columns to exclude (or None)
        geometry_col: Name of geometry column
        bbox_col: Name of bbox column (or None if not present)

    Returns:
        list: Columns to select (preserving original order)
    """
    if include_cols:
        # Start with requested columns
        selected = set(include_cols)

        # Always add geometry unless explicitly excluded
        if exclude_cols and geometry_col in exclude_cols:
            pass  # User explicitly excluded geometry
        else:
            selected.add(geometry_col)

        # Always add bbox unless explicitly excluded
        if bbox_col:
            if exclude_cols and bbox_col in exclude_cols:
                pass  # User explicitly excluded bbox
            else:
                selected.add(bbox_col)

    elif exclude_cols:
        selected = set(all_columns) - set(exclude_cols)
    else:
        selected = set(all_columns)

    # Preserve original column order
    return [c for c in all_columns if c in selected]


def validate_columns(
    requested_cols: list[str] | None, all_columns: list[str], option_name: str
) -> None:
    """
    Validate that requested columns exist in schema.

    Args:
        requested_cols: Columns requested by user
        all_columns: All columns in schema
        option_name: Name of the option for error message

    Raises:
        click.ClickException: If any columns not found
    """
    if not requested_cols:
        return

    missing = set(requested_cols) - set(all_columns)
    if missing:
        raise click.ClickException(
            f"Columns not found in schema ({option_name}): {', '.join(sorted(missing))}\n"
            f"Available columns: {', '.join(all_columns)}"
        )


def build_spatial_filter(
    bbox: tuple[float, float, float, float] | None,
    geometry_wkt: str | None,
    bbox_info: dict,
    geometry_col: str,
) -> str | None:
    """
    Build WHERE clause for spatial filtering.

    Uses bbox column for fast filtering when available (bbox covering),
    then applies precise geometry intersection.

    Args:
        bbox: Bounding box tuple (xmin, ymin, xmax, ymax) or None
        geometry_wkt: WKT geometry string or None
        bbox_info: Result from check_bbox_structure
        geometry_col: Name of geometry column

    Returns:
        str: WHERE clause conditions or None if no spatial filter
    """
    conditions = []

    if bbox:
        xmin, ymin, xmax, ymax = bbox
        if bbox_info.get("has_bbox_column"):
            # Fast bbox filter using covering column
            bbox_col = bbox_info["bbox_column_name"]
            conditions.append(
                f'("{bbox_col}".xmax >= {xmin} AND "{bbox_col}".xmin <= {xmax} '
                f'AND "{bbox_col}".ymax >= {ymin} AND "{bbox_col}".ymin <= {ymax})'
            )
        else:
            # Slower but works without bbox column
            conditions.append(
                f'ST_Intersects("{geometry_col}", '
                f"ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))"
            )

    if geometry_wkt:
        # Escape single quotes in WKT
        escaped_wkt = geometry_wkt.replace("'", "''")
        conditions.append(f"ST_Intersects(\"{geometry_col}\", ST_GeomFromText('{escaped_wkt}'))")

    return " AND ".join(conditions) if conditions else None


def build_extract_query(
    input_path: str,
    columns: list[str],
    spatial_filter: str | None,
    where_clause: str | None,
) -> str:
    """
    Build the complete extraction query.

    Args:
        input_path: Input file path or glob pattern
        columns: Columns to select
        spatial_filter: Spatial WHERE conditions or None
        where_clause: User-provided WHERE clause or None

    Returns:
        str: Complete SELECT query
    """
    col_list = ", ".join(f'"{c}"' for c in columns)
    query = f"SELECT {col_list} FROM read_parquet('{input_path}')"

    conditions = []
    if spatial_filter:
        conditions.append(f"({spatial_filter})")
    if where_clause:
        conditions.append(f"({where_clause})")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    return query


def extract(
    input_parquet: str,
    output_parquet: str,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    bbox: str | None = None,
    geometry: str | None = None,
    where: str | None = None,
    use_first_geometry: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
) -> None:
    """
    Extract columns and rows from GeoParquet files.

    Supports column selection, spatial filtering (bbox, geometry),
    SQL filtering, and multiple input files via glob patterns.

    Args:
        input_parquet: Input file path, URL, or glob pattern
        output_parquet: Output file path
        include_cols: Comma-separated columns to include
        exclude_cols: Comma-separated columns to exclude
        bbox: Bounding box string "xmin,ymin,xmax,ymax"
        geometry: GeoJSON/WKT geometry, @filepath, or "-" for stdin
        where: DuckDB WHERE clause
        use_first_geometry: Use first geometry from FeatureCollection
        dry_run: Print SQL without executing
        verbose: Print verbose output
        compression: Compression type
        compression_level: Compression level
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name for S3
    """
    # Parse column lists
    include_list = [c.strip() for c in include_cols.split(",")] if include_cols else None
    exclude_list = [c.strip() for c in exclude_cols.split(",")] if exclude_cols else None

    # Validate mutually exclusive options
    if include_list and exclude_list:
        raise click.ClickException("--include-cols and --exclude-cols are mutually exclusive")

    # Get safe URL for input
    safe_url = safe_file_url(input_parquet, verbose)

    # Get schema info
    all_columns = get_schema_columns(input_parquet)
    geometry_col = find_primary_geometry_column(input_parquet, verbose)
    bbox_info = check_bbox_structure(input_parquet, verbose=False)
    bbox_col = bbox_info.get("bbox_column_name")

    # Validate columns
    validate_columns(include_list, all_columns, "--include-cols")
    validate_columns(exclude_list, all_columns, "--exclude-cols")

    # Build column selection
    selected_columns = build_column_selection(
        all_columns, include_list, exclude_list, geometry_col, bbox_col
    )

    # Parse bbox if provided
    bbox_tuple = parse_bbox(bbox) if bbox else None

    # Parse geometry if provided
    geometry_wkt = parse_geometry_input(geometry, use_first_geometry) if geometry else None

    # Build spatial filter
    spatial_filter = build_spatial_filter(bbox_tuple, geometry_wkt, bbox_info, geometry_col)

    # Build the query
    query = build_extract_query(safe_url, selected_columns, spatial_filter, where)

    # Dry run mode
    if dry_run:
        click.echo(
            click.style(
                "\n=== DRY RUN MODE - SQL Commands that would be executed ===\n",
                fg="yellow",
                bold=True,
            )
        )
        click.echo(click.style(f"-- Input: {input_parquet}", fg="cyan"))
        click.echo(click.style(f"-- Output: {output_parquet}", fg="cyan"))
        click.echo(click.style(f"-- Geometry column: {geometry_col}", fg="cyan"))
        if bbox_col:
            click.echo(click.style(f"-- Bbox column: {bbox_col}", fg="cyan"))
        click.echo(click.style(f"-- Selected columns: {len(selected_columns)}", fg="cyan"))
        if bbox:
            click.echo(click.style(f"-- Bbox filter: {bbox}", fg="cyan"))
        if geometry:
            click.echo(click.style(f"-- Geometry filter: (provided)", fg="cyan"))
        if where:
            click.echo(click.style(f"-- WHERE clause: {where}", fg="cyan"))
        click.echo()

        compression_desc = compression
        if compression in ["GZIP", "ZSTD", "BROTLI"] and compression_level:
            compression_desc = f"{compression}:{compression_level}"

        duckdb_compression = compression.lower() if compression != "UNCOMPRESSED" else "uncompressed"
        display_query = f"""COPY ({query})
TO '{output_parquet}'
(FORMAT PARQUET, COMPRESSION '{duckdb_compression}');"""

        click.echo(click.style("-- Main query:", fg="cyan"))
        click.echo(display_query)
        click.echo(click.style(f"\n-- Note: Using {compression_desc} compression", fg="cyan"))
        return

    # Execute the extraction
    if verbose:
        click.echo(f"Input: {input_parquet}")
        click.echo(f"Output: {output_parquet}")
        click.echo(f"Selecting {len(selected_columns)} columns: {', '.join(selected_columns)}")
        if spatial_filter:
            click.echo(f"Applying spatial filter")
        if where:
            click.echo(f"Applying WHERE clause: {where}")

    # Create DuckDB connection
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))

    try:
        # Get row count for progress
        count_query = f"SELECT COUNT(*) FROM read_parquet('{safe_url}')"
        conditions = []
        if spatial_filter:
            conditions.append(f"({spatial_filter})")
        if where:
            conditions.append(f"({where})")
        if conditions:
            count_query = f"SELECT COUNT(*) FROM read_parquet('{safe_url}') WHERE " + " AND ".join(
                conditions
            )

        total_count = con.execute(count_query).fetchone()[0]
        click.echo(f"Extracting {total_count:,} rows...")

        if total_count == 0:
            click.echo(click.style("Warning: No rows match the specified filters.", fg="yellow"))

        # Get metadata from input for preservation
        metadata = None
        try:
            metadata, _ = get_parquet_metadata(input_parquet, verbose=False)
        except Exception:
            pass  # Metadata preservation is optional

        # Write output
        write_parquet_with_metadata(
            con,
            query,
            output_parquet,
            original_metadata=metadata,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
        )

        click.echo(click.style(f"Extracted {total_count:,} rows to {output_parquet}", fg="green"))

    finally:
        con.close()
