"""
Extract columns and rows from GeoParquet files.

Supports column selection, spatial filtering (bbox, geometry),
SQL filtering, and multiple input files via glob patterns.
"""

import json
import sys
from pathlib import Path

import click

from geoparquet_io.core.common import (
    check_bbox_structure,
    find_primary_geometry_column,
    get_duckdb_connection,
    get_parquet_metadata,
    needs_httpfs,
    parse_geo_metadata,
    safe_file_url,
    write_parquet_with_metadata,
)


def _looks_like_latlong_bbox(bbox: tuple[float, float, float, float]) -> bool:
    """Check if bbox values look like lat/long coordinates."""
    xmin, ymin, xmax, ymax = bbox
    # Lat/long: x (lon) is -180 to 180, y (lat) is -90 to 90
    return -180 <= xmin <= 180 and -180 <= xmax <= 180 and -90 <= ymin <= 90 and -90 <= ymax <= 90


def _is_geographic_crs(crs_info: dict | str | None) -> bool | None:
    """
    Check if CRS is geographic (lat/long) vs projected.

    Returns:
        True if geographic, False if projected, None if unknown
    """
    if crs_info is None:
        return None

    if isinstance(crs_info, str):
        crs_upper = crs_info.upper()
        # Common geographic CRS codes
        if any(
            code in crs_upper
            for code in ["4326", "CRS84", "CRS:84", "OGC:CRS84", "4269", "4267"]
        ):
            return True
        return None

    if isinstance(crs_info, dict):
        # Check PROJJSON type
        crs_type = crs_info.get("type", "")
        if crs_type == "GeographicCRS":
            return True
        if crs_type == "ProjectedCRS":
            return False

        # Check EPSG code
        crs_id = crs_info.get("id", {})
        if isinstance(crs_id, dict):
            code = crs_id.get("code")
            if code in [4326, 4269, 4267]:  # Common geographic codes
                return True

    return None


def _get_crs_from_file(input_parquet: str, geometry_col: str) -> dict | str | None:
    """
    Get CRS info from GeoParquet metadata or Arrow extension metadata.

    Returns CRS info dict/string or None if not found.
    """
    import fsspec
    import pyarrow.parquet as pq

    # First try GeoParquet file-level metadata
    try:
        metadata, _ = get_parquet_metadata(input_parquet, verbose=False)
        geo_meta = parse_geo_metadata(metadata, verbose=False)
        if geo_meta:
            columns_meta = geo_meta.get("columns", {})
            if geometry_col in columns_meta:
                crs_info = columns_meta[geometry_col].get("crs")
                if crs_info:
                    return crs_info
    except Exception:
        pass

    # Fall back to Arrow extension metadata on geometry column
    try:
        safe_url = safe_file_url(input_parquet, verbose=False)
        with fsspec.open(safe_url, "rb") as f:
            pf = pq.ParquetFile(f)
            schema = pf.schema_arrow
            if geometry_col in schema.names:
                field = schema.field(geometry_col)
                if field.metadata:
                    ext_meta = field.metadata.get(b"ARROW:extension:metadata")
                    if ext_meta:
                        ext_data = json.loads(ext_meta.decode("utf-8"))
                        if "crs" in ext_data:
                            return ext_data["crs"]
    except Exception:
        pass

    return None


def _get_data_bounds(input_parquet: str, geometry_col: str) -> tuple | None:
    """Get actual data bounds from file."""
    try:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))
        safe_url = safe_file_url(input_parquet, verbose=False)
        result = con.execute(f"""
            SELECT
                MIN(ST_XMin("{geometry_col}")) as xmin,
                MIN(ST_YMin("{geometry_col}")) as ymin,
                MAX(ST_XMax("{geometry_col}")) as xmax,
                MAX(ST_YMax("{geometry_col}")) as ymax
            FROM read_parquet('{safe_url}')
        """).fetchone()
        con.close()
        if result and all(v is not None for v in result):
            return result
    except Exception:
        pass
    return None


def _get_crs_display_name(crs_info: dict | str | None) -> str:
    """Get human-readable CRS name."""
    if crs_info is None:
        return "unknown"

    if isinstance(crs_info, str):
        return crs_info

    if isinstance(crs_info, dict):
        # Try to get name
        name = crs_info.get("name")
        if name:
            # Also add EPSG code if available
            crs_id = crs_info.get("id", {})
            if isinstance(crs_id, dict):
                authority = crs_id.get("authority", "EPSG")
                code = crs_id.get("code")
                if code:
                    return f"{name} ({authority}:{code})"
            return name

        # Fall back to EPSG code
        crs_id = crs_info.get("id", {})
        if isinstance(crs_id, dict):
            authority = crs_id.get("authority", "EPSG")
            code = crs_id.get("code")
            if code:
                return f"{authority}:{code}"

    return "unknown"


def _warn_if_crs_mismatch(
    bbox: tuple[float, float, float, float],
    input_parquet: str,
    geometry_col: str,
) -> None:
    """Warn if bbox looks like lat/long but data is in a projected CRS."""
    if not _looks_like_latlong_bbox(bbox):
        return  # User's bbox doesn't look like lat/long, no warning needed

    crs_info = _get_crs_from_file(input_parquet, geometry_col)
    is_geographic = _is_geographic_crs(crs_info)

    if is_geographic is False:  # Definitely projected
        crs_name = _get_crs_display_name(crs_info)
        data_bounds = _get_data_bounds(input_parquet, geometry_col)

        msg = (
            f"\nWarning: Your bbox appears to be in lat/long coordinates, but the data "
            f"uses a projected CRS ({crs_name})."
        )
        if data_bounds:
            msg += (
                f"\nData bounds: xmin={data_bounds[0]:.2f}, ymin={data_bounds[1]:.2f}, "
                f"xmax={data_bounds[2]:.2f}, ymax={data_bounds[3]:.2f}"
            )
        msg += "\nIf you get 0 results, try using coordinates in the data's CRS."

        click.echo(click.style(msg, fg="yellow"))


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


def _read_geometry_from_stdin() -> str:
    """Read geometry from stdin."""
    if sys.stdin.isatty():
        raise click.ClickException(
            "No geometry provided on stdin. Pipe geometry data or use @file syntax."
        )
    return sys.stdin.read().strip()


def _resolve_geometry_file(geometry_input: str) -> str | None:
    """
    Resolve file path from geometry input.

    Returns file path if input refers to a file, None otherwise.
    """
    if geometry_input.startswith("@"):
        return geometry_input[1:]

    # Check if it looks like a file path (not inline geometry)
    if not geometry_input.strip().startswith(("{", "POLYGON", "POINT", "LINESTRING", "MULTI")):
        potential_path = Path(geometry_input)
        if potential_path.exists() and potential_path.suffix.lower() in (
            ".geojson",
            ".json",
            ".wkt",
        ):
            return geometry_input

    return None


def _extract_geometry_from_geojson(geojson: dict, use_first: bool) -> dict:
    """
    Extract geometry from GeoJSON, handling Feature and FeatureCollection.

    Args:
        geojson: Parsed GeoJSON object
        use_first: If True, use first geometry from FeatureCollection

    Returns:
        dict: The geometry object

    Raises:
        click.ClickException: If geometry cannot be extracted
    """
    if geojson.get("type") == "FeatureCollection":
        features = geojson.get("features", [])
        if not features:
            raise click.ClickException("FeatureCollection is empty - no geometries found")
        if len(features) > 1 and not use_first:
            raise click.ClickException(
                f"Multiple geometries ({len(features)}) found in FeatureCollection. "
                "Use --use-first-geometry to use only the first geometry."
            )
        geom = features[0].get("geometry")
        if not geom:
            raise click.ClickException("First feature has no geometry")
        return geom

    if geojson.get("type") == "Feature":
        geom = geojson.get("geometry")
        if not geom:
            raise click.ClickException("Feature has no geometry")
        return geom

    # Already a geometry object
    return geojson


def _parse_geojson_to_wkt(geometry_input: str, use_first: bool) -> str:
    """Parse GeoJSON string to WKT."""
    try:
        geojson = json.loads(geometry_input)
    except json.JSONDecodeError as e:
        raise click.ClickException(f"Invalid GeoJSON: {e}") from e

    geojson = _extract_geometry_from_geojson(geojson, use_first)

    try:
        return convert_geojson_to_wkt(geojson)
    except Exception as e:
        raise click.ClickException(f"Failed to convert GeoJSON to WKT: {e}") from e


def _validate_wkt(wkt: str, original_input: str) -> str:
    """Validate WKT string."""
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
        geometry_input = _read_geometry_from_stdin()

    # Handle file reference
    file_path = _resolve_geometry_file(geometry_input)
    if file_path:
        path = Path(file_path)
        if not path.exists():
            raise click.ClickException(f"Geometry file not found: {file_path}")
        geometry_input = path.read_text().strip()

    # Parse to WKT
    if geometry_input.strip().startswith("{"):
        return _parse_geojson_to_wkt(geometry_input, use_first)

    return _validate_wkt(geometry_input.strip(), original_input)


def get_schema_columns(input_parquet: str) -> list[str]:
    """
    Get list of column names from parquet file schema.

    Args:
        input_parquet: Path to parquet file (or glob pattern - uses first file)

    Returns:
        list: Column names
    """
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
        selected = set(include_cols)
        # Always add geometry unless explicitly excluded
        if not (exclude_cols and geometry_col in exclude_cols):
            selected.add(geometry_col)
        # Always add bbox unless explicitly excluded
        if bbox_col and not (exclude_cols and bbox_col in exclude_cols):
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
    """
    conditions = []

    if bbox:
        xmin, ymin, xmax, ymax = bbox
        if bbox_info.get("has_bbox_column"):
            bbox_col = bbox_info["bbox_column_name"]
            conditions.append(
                f'("{bbox_col}".xmax >= {xmin} AND "{bbox_col}".xmin <= {xmax} '
                f'AND "{bbox_col}".ymax >= {ymin} AND "{bbox_col}".ymin <= {ymax})'
            )
        else:
            conditions.append(
                f'ST_Intersects("{geometry_col}", ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))'
            )

    if geometry_wkt:
        escaped_wkt = geometry_wkt.replace("'", "''")
        conditions.append(f"ST_Intersects(\"{geometry_col}\", ST_GeomFromText('{escaped_wkt}'))")

    return " AND ".join(conditions) if conditions else None


def build_extract_query(
    input_path: str,
    columns: list[str],
    spatial_filter: str | None,
    where_clause: str | None,
) -> str:
    """Build the complete extraction query."""
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


def _print_dry_run_output(
    input_parquet: str,
    output_parquet: str,
    geometry_col: str,
    bbox_col: str | None,
    selected_columns: list[str],
    bbox: str | None,
    geometry: str | None,
    where: str | None,
    query: str,
    compression: str,
    compression_level: int | None,
) -> None:
    """Print dry run output."""
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
        click.echo(click.style("-- Geometry filter: (provided)", fg="cyan"))
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


def _execute_extraction(
    input_parquet: str,
    output_parquet: str,
    query: str,
    safe_url: str,
    spatial_filter: str | None,
    where: str | None,
    selected_columns: list[str],
    verbose: bool,
    show_sql: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
) -> None:
    """Execute the extraction query and write output."""
    if verbose:
        click.echo(f"Input: {input_parquet}")
        click.echo(f"Output: {output_parquet}")
        click.echo(f"Selecting {len(selected_columns)} columns: {', '.join(selected_columns)}")
        if spatial_filter:
            click.echo("Applying spatial filter")
        if where:
            click.echo(f"Applying WHERE clause: {where}")

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))

    try:
        # Build count query with filters
        conditions = []
        if spatial_filter:
            conditions.append(f"({spatial_filter})")
        if where:
            conditions.append(f"({where})")

        count_query = f"SELECT COUNT(*) FROM read_parquet('{safe_url}')"
        if conditions:
            count_query += " WHERE " + " AND ".join(conditions)

        if show_sql:
            click.echo(click.style("\n-- Count query:", fg="cyan"))
            click.echo(f"{count_query};")

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
            show_sql=show_sql,
            profile=profile,
        )

        click.echo(click.style(f"Extracted {total_count:,} rows to {output_parquet}", fg="green"))

    finally:
        con.close()


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
    show_sql: bool = False,
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

    # Warn if bbox looks like lat/long but data is projected
    if bbox_tuple and not dry_run:
        _warn_if_crs_mismatch(bbox_tuple, input_parquet, geometry_col)

    # Parse geometry if provided
    geometry_wkt = parse_geometry_input(geometry, use_first_geometry) if geometry else None

    # Build spatial filter
    spatial_filter = build_spatial_filter(bbox_tuple, geometry_wkt, bbox_info, geometry_col)

    # Build the query
    query = build_extract_query(safe_url, selected_columns, spatial_filter, where)

    if dry_run:
        _print_dry_run_output(
            input_parquet,
            output_parquet,
            geometry_col,
            bbox_col,
            selected_columns,
            bbox,
            geometry,
            where,
            query,
            compression,
            compression_level,
        )
    else:
        _execute_extraction(
            input_parquet,
            output_parquet,
            query,
            safe_url,
            spatial_filter,
            where,
            selected_columns,
            verbose,
            show_sql,
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            profile,
        )
