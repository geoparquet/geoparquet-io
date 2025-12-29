"""
Extract columns and rows from GeoParquet files.

Supports column selection, spatial filtering (bbox, geometry),
SQL filtering, and multiple input files via glob patterns.

Also supports Arrow IPC streaming for Unix-style piping:
    gpio extract --bbox ... input.parquet | gpio add bbox - output.parquet
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import click
import pyarrow as pa

from geoparquet_io.core.common import (
    check_bbox_structure,
    find_primary_geometry_column,
    get_crs_display_name,
    get_duckdb_connection,
    get_duckdb_connection_for_s3,
    get_parquet_metadata,
    needs_httpfs,
    safe_file_url,
    write_parquet_with_metadata,
)
from geoparquet_io.core.logging_config import debug, info, progress, success, warn
from geoparquet_io.core.stream_io import open_input, write_output
from geoparquet_io.core.streaming import (
    find_geometry_column_from_metadata,
    find_geometry_column_from_table,
    is_stdin,
    should_stream_output,
)


def get_parquet_row_count(parquet_file: str) -> int:
    """Get row count from parquet file metadata using DuckDB (O(1) - reads footer only)."""
    from geoparquet_io.core.duckdb_metadata import get_row_count

    return get_row_count(parquet_file)


# SQL keywords that could be dangerous in a WHERE clause
# These could modify data or database structure
DANGEROUS_SQL_KEYWORDS = [
    "DROP",
    "DELETE",
    "INSERT",
    "UPDATE",
    "CREATE",
    "ALTER",
    "TRUNCATE",
    "EXEC",
    "EXECUTE",
    "MERGE",
    "REPLACE",
    "GRANT",
    "REVOKE",
]


def validate_where_clause(where_clause: str) -> None:
    """
    Validate WHERE clause for potentially dangerous SQL keywords.

    This is a basic safety check to prevent accidental or intentional
    SQL injection attacks. It checks for keywords that could modify
    data or database structure.

    Note: This feature is intended for trusted users. For untrusted input,
    additional validation or parameterized queries would be required.

    Args:
        where_clause: The WHERE clause string to validate

    Raises:
        click.ClickException: If dangerous SQL keywords are found
    """
    # Build pattern to match dangerous keywords as whole words (case-insensitive)
    # Use word boundaries to avoid false positives (e.g., "UPDATED_AT" shouldn't match)
    upper_clause = where_clause.upper()
    found_keywords = []

    for keyword in DANGEROUS_SQL_KEYWORDS:
        # Match keyword as a whole word
        pattern = rf"\b{keyword}\b"
        if re.search(pattern, upper_clause):
            found_keywords.append(keyword)

    if found_keywords:
        raise click.ClickException(
            f"WHERE clause contains potentially dangerous SQL keywords: {', '.join(found_keywords)}. "
            "Only SELECT-style filtering expressions are allowed in --where. "
            "If you need to perform data modifications, use DuckDB directly."
        )


def looks_like_latlong_bbox(bbox: tuple[float, float, float, float]) -> bool:
    """Check if bbox values look like lat/long coordinates."""
    xmin, ymin, xmax, ymax = bbox
    # Lat/long: x (lon) is -180 to 180, y (lat) is -90 to 90
    return -180 <= xmin <= 180 and -180 <= xmax <= 180 and -90 <= ymin <= 90 and -90 <= ymax <= 90


def is_geographic_crs(crs_info: dict | str | None) -> bool | None:
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
            code in crs_upper for code in ["4326", "CRS84", "CRS:84", "OGC:CRS84", "4269", "4267"]
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
    Get CRS info from GeoParquet metadata or Parquet geo logical type.

    Returns CRS info dict/string or None if not found.
    """
    from geoparquet_io.core.duckdb_metadata import (
        get_geo_metadata,
        get_schema_info,
        parse_geometry_logical_type,
    )

    safe_url = safe_file_url(input_parquet, verbose=False)

    # First try GeoParquet file-level metadata
    try:
        geo_meta = get_geo_metadata(safe_url)
        if geo_meta:
            columns_meta = geo_meta.get("columns", {})
            if geometry_col in columns_meta:
                crs_info = columns_meta[geometry_col].get("crs")
                if crs_info:
                    return crs_info
    except Exception:
        # CRS extraction is optional
        pass

    # Fall back to Parquet geo logical type (for GeoParquet 2.0 / parquet-geo)
    try:
        schema_info = get_schema_info(safe_url)
        for col in schema_info:
            name = col.get("name", "")
            if name != geometry_col:
                continue
            logical_type = col.get("logical_type", "")
            # DuckDB returns GeometryType(...) and GeographyType(...) from parquet_schema()
            if logical_type and (
                logical_type.startswith("GeometryType(")
                or logical_type.startswith("GeographyType(")
            ):
                parsed = parse_geometry_logical_type(logical_type)
                if parsed and "crs" in parsed:
                    return parsed["crs"]
    except Exception:
        # CRS extraction is optional
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
        # Bounds extraction is optional - used only for warning messages
        pass
    return None


def _warn_if_crs_mismatch(
    bbox: tuple[float, float, float, float],
    input_parquet: str,
    geometry_col: str,
) -> None:
    """Warn if bbox looks like lat/long but data is in a projected CRS."""
    if not looks_like_latlong_bbox(bbox):
        return  # User's bbox doesn't look like lat/long, no warning needed

    crs_info = _get_crs_from_file(input_parquet, geometry_col)
    is_geographic = is_geographic_crs(crs_info)

    if is_geographic is False:  # Definitely projected
        crs_name = get_crs_display_name(crs_info)
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

        warn(msg)


def parse_bbox(bbox_str: str) -> tuple[float, float, float, float]:
    """
    Parse bounding box string into tuple of floats.

    Args:
        bbox_str: Comma-separated string "xmin,ymin,xmax,ymax"

    Returns:
        tuple: (xmin, ymin, xmax, ymax)

    Raises:
        click.ClickException: If format is invalid or coordinates are reversed
    """
    try:
        parts = [float(x.strip()) for x in bbox_str.split(",")]
        if len(parts) != 4:
            raise click.ClickException(
                f"Invalid bbox format. Expected 4 values (xmin,ymin,xmax,ymax), got {len(parts)}"
            )
        xmin, ymin, xmax, ymax = parts

        # Validate coordinate ordering
        if xmin > xmax or ymin > ymax:
            raise click.ClickException(
                f"Invalid bbox: coordinates appear to be reversed. "
                f"xmin ({xmin}) must be <= xmax ({xmax}), and ymin ({ymin}) must be <= ymax ({ymax}). "
                "Expected order: xmin,ymin,xmax,ymax."
            )

        return (xmin, ymin, xmax, ymax)
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
        input_parquet: Path to parquet file (or glob pattern/directory - uses first file)

    Returns:
        list: Column names
    """
    from geoparquet_io.core.common import get_first_parquet_file, is_partition_path

    # For partitions, use first file for schema
    file_to_check = input_parquet
    if is_partition_path(input_parquet):
        first_file = get_first_parquet_file(input_parquet)
        if first_file:
            file_to_check = first_file

    # Use auto-detecting S3 connection for S3 paths
    if needs_httpfs(file_to_check):
        con = get_duckdb_connection_for_s3(file_to_check, load_spatial=True)
    else:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        safe_url = safe_file_url(file_to_check, verbose=False)
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
    - If include_cols only: select those + geometry + bbox
    - If exclude_cols only: select all except those
    - If both: select include_cols, but exclude_cols can remove geometry/bbox
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
    exclude_set = set(exclude_cols) if exclude_cols else set()

    if include_cols:
        selected = set(include_cols)
        # Always add geometry unless explicitly excluded
        if geometry_col not in exclude_set:
            selected.add(geometry_col)
        # Always add bbox unless explicitly excluded
        if bbox_col and bbox_col not in exclude_set:
            selected.add(bbox_col)
    elif exclude_cols:
        selected = set(all_columns) - exclude_set
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
    limit: int | None = None,
    allow_schema_diff: bool = False,
    hive_input: bool = False,
) -> str:
    """Build the complete extraction query."""
    from geoparquet_io.core.partition_reader import build_read_parquet_expr

    col_list = ", ".join(f'"{c}"' for c in columns)
    # Use partition reader to build read_parquet expression with proper options
    read_expr = build_read_parquet_expr(
        input_path,
        allow_schema_diff=allow_schema_diff,
        hive_input=hive_input,
        verbose=False,
    )
    query = f"SELECT {col_list} FROM {read_expr}"

    conditions = []
    if spatial_filter:
        conditions.append(f"({spatial_filter})")
    if where_clause:
        conditions.append(f"({where_clause})")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    if limit is not None:
        query += f" LIMIT {limit}"

    return query


def _build_query_for_source(
    source_ref: str,
    columns: list[str],
    spatial_filter: str | None,
    where_clause: str | None,
    limit: int | None = None,
) -> str:
    """Build extraction query for a DuckDB source reference (table or read_parquet)."""
    col_list = ", ".join(f'"{c}"' for c in columns)
    query = f"SELECT {col_list} FROM {source_ref}"

    conditions = []
    if spatial_filter:
        conditions.append(f"({spatial_filter})")
    if where_clause:
        conditions.append(f"({where_clause})")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    if limit is not None:
        query += f" LIMIT {limit}"

    return query


def _get_table_column_info(
    table: pa.Table,
) -> tuple[list[str], str | None, dict]:
    """Get column information from an Arrow table."""
    all_columns = table.column_names
    geometry_col = find_geometry_column_from_table(table)

    # Check for bbox column
    bbox_col = None
    for name in ["bbox", "bounds", "bounding_box"]:
        if name in all_columns:
            bbox_col = name
            break

    bbox_info = {
        "has_bbox_column": bbox_col is not None,
        "bbox_column_name": bbox_col,
    }

    return all_columns, geometry_col, bbox_info


def extract_table(
    table: pa.Table,
    columns: list[str] | None = None,
    exclude_columns: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    where: str | None = None,
    limit: int | None = None,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Extract columns and rows from an Arrow Table.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table
        columns: Columns to include (None = all)
        exclude_columns: Columns to exclude
        bbox: Bounding box filter (xmin, ymin, xmax, ymax)
        where: SQL WHERE clause
        limit: Maximum rows to return
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        Filtered PyArrow Table
    """
    all_columns, geom_col, bbox_info = _get_table_column_info(table)

    if geometry_column:
        geom_col = geometry_column
    if not geom_col:
        geom_col = "geometry"

    # Validate geometry column exists
    if geom_col not in all_columns:
        raise ValueError(
            f"geometry_column '{geom_col}' not found in table columns: {list(all_columns)}"
        )

    # Build column selection
    selected_columns = build_column_selection(
        all_columns,
        columns,
        exclude_columns,
        geom_col,
        bbox_info.get("bbox_column_name"),
    )

    # Build spatial filter
    spatial_filter = build_spatial_filter(bbox, None, bbox_info, geom_col) if bbox else None

    # Validate WHERE clause if provided
    if where:
        validate_where_clause(where)

    # Register table and execute query
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.register("__input_table", table)

        # Check if geometry column is BLOB (needs conversion)
        columns_info = con.execute("DESCRIBE __input_table").fetchall()
        geom_is_blob = any(col[0] == geom_col and "BLOB" in col[1].upper() for col in columns_info)

        if geom_is_blob and geom_col in table.column_names:
            # Create view with geometry conversion
            other_cols = [c for c in table.column_names if c != geom_col]
            col_defs = other_cols + [f'ST_GeomFromWKB("{geom_col}") AS "{geom_col}"']
            view_query = (
                f"CREATE VIEW __input_view AS SELECT {', '.join(col_defs)} FROM __input_table"
            )
            con.execute(view_query)
            source_ref = "__input_view"
            needs_wkb_conversion = True
        else:
            source_ref = "__input_table"
            needs_wkb_conversion = False

        # Build query, but if geometry was converted, we need to convert back to WKB
        if needs_wkb_conversion and geom_col in selected_columns:
            # Replace geometry column with WKB conversion in column list
            cols_with_wkb = [
                f'ST_AsWKB("{geom_col}") AS "{geom_col}"' if c == geom_col else f'"{c}"'
                for c in selected_columns
            ]
            col_list = ", ".join(cols_with_wkb)

            conditions = []
            if spatial_filter:
                conditions.append(f"({spatial_filter})")
            if where:
                conditions.append(f"({where})")

            query = f"SELECT {col_list} FROM {source_ref}"
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            if limit is not None:
                query += f" LIMIT {limit}"
        else:
            query = _build_query_for_source(
                source_ref, selected_columns, spatial_filter, where, limit
            )
        result = con.execute(query).fetch_arrow_table()

        # Preserve metadata
        if table.schema.metadata:
            result = result.replace_schema_metadata(table.schema.metadata)

        return result
    finally:
        con.close()


def _extract_streaming(
    input_path: str,
    output_path: str | None,
    include_cols: list[str] | None,
    exclude_cols: list[str] | None,
    bbox_tuple: tuple[float, float, float, float] | None,
    geometry_wkt: str | None,
    where: str | None,
    limit: int | None,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
) -> None:
    """Handle extraction with streaming input/output."""
    # Suppress verbose when streaming to stdout
    if should_stream_output(output_path):
        verbose = False

    with open_input(input_path, verbose=verbose) as (source, metadata, is_stream, con):
        # Get column names from query result (works with both table names and read_parquet)
        sample = con.execute(f"SELECT * FROM {source} LIMIT 0").description
        all_columns = [col[0] for col in sample]

        # Find geometry column
        geom_col = find_geometry_column_from_metadata(metadata)
        if not geom_col:
            for name in ["geometry", "geom", "the_geom"]:
                if name in all_columns:
                    geom_col = name
                    break
        if not geom_col:
            geom_col = "geometry"

        # Check for bbox column
        bbox_col = None
        for name in ["bbox", "bounds"]:
            if name in all_columns:
                bbox_col = name
                break

        bbox_info = {"has_bbox_column": bbox_col is not None, "bbox_column_name": bbox_col}

        # Build column selection
        selected_columns = build_column_selection(
            all_columns, include_cols, exclude_cols, geom_col, bbox_col
        )

        # Build spatial filter
        spatial_filter = build_spatial_filter(bbox_tuple, geometry_wkt, bbox_info, geom_col)

        # Build query
        query = _build_query_for_source(source, selected_columns, spatial_filter, where, limit)

        if verbose:
            debug(f"Streaming extraction query: {query}")

        # Write output
        write_output(
            con,
            query,
            output_path,
            original_metadata=metadata,
            geometry_column=geom_col,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
        )

        if not should_stream_output(output_path):
            success(f"Extracted data to {output_path}")


def _print_dry_run_output(
    input_parquet: str,
    output_parquet: str,
    geometry_col: str,
    bbox_col: str | None,
    selected_columns: list[str],
    bbox: str | None,
    geometry: str | None,
    where: str | None,
    limit: int | None,
    query: str,
    compression: str,
    compression_level: int | None,
) -> None:
    """Print dry run output."""
    warn("\n=== DRY RUN MODE - SQL Commands that would be executed ===\n")
    info(f"-- Input: {input_parquet}")
    info(f"-- Output: {output_parquet}")
    info(f"-- Geometry column: {geometry_col}")
    if bbox_col:
        info(f"-- Bbox column: {bbox_col}")
    info(f"-- Selected columns: {len(selected_columns)}")
    if bbox:
        info(f"-- Bbox filter: {bbox}")
    if geometry:
        info("-- Geometry filter: (provided)")
    if where:
        info(f"-- WHERE clause: {where}")
    if limit:
        info(f"-- Limit: {limit}")
    progress("")

    compression_desc = compression
    if compression in ["GZIP", "ZSTD", "BROTLI"] and compression_level:
        compression_desc = f"{compression}:{compression_level}"

    duckdb_compression = compression.lower() if compression != "UNCOMPRESSED" else "uncompressed"
    display_query = f"""COPY ({query})
TO '{output_parquet}'
(FORMAT PARQUET, COMPRESSION '{duckdb_compression}');"""

    info("-- Main query:")
    progress(display_query)
    info(f"\n-- Note: Using {compression_desc} compression")


def _execute_extraction(
    input_parquet: str,
    output_parquet: str,
    query: str,
    safe_url: str,
    spatial_filter: str | None,
    where: str | None,
    limit: int | None,
    skip_count: bool,
    selected_columns: list[str],
    verbose: bool,
    show_sql: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None = None,
) -> None:
    """Execute the extraction query and write output."""
    if verbose:
        debug(f"Input: {input_parquet}")
        debug(f"Output: {output_parquet}")
        debug(f"Selecting {len(selected_columns)} columns: {', '.join(selected_columns)}")
        if spatial_filter:
            debug("Applying spatial filter")
        if where:
            debug(f"Applying WHERE clause: {where}")
        if limit:
            debug(f"Limiting to {limit:,} rows")

    # Use auto-detecting S3 connection for S3 paths
    if needs_httpfs(input_parquet):
        con = get_duckdb_connection_for_s3(input_parquet, load_spatial=True)
    else:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)

    try:
        # Get total row count from input file metadata (fast - reads footer only)
        # DuckDB handles glob patterns natively
        input_total_rows = None
        if not skip_count:
            try:
                input_total_rows = get_parquet_row_count(input_parquet)
            except Exception:
                pass  # Total count is optional

        progress("Extracting rows...")

        # Get metadata from input for preservation
        # DuckDB handles glob patterns natively
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
            geoparquet_version=geoparquet_version,
        )

        # Get extracted row count from output file metadata (fast - reads footer only)
        extracted_count = get_parquet_row_count(output_parquet)

        if input_total_rows is not None:
            success(
                f"Extracted {extracted_count:,} rows (out of {input_total_rows:,} total) to {output_parquet}"
            )
        else:
            success(f"Extracted {extracted_count:,} rows to {output_parquet}")

    finally:
        con.close()


def extract(
    input_parquet: str,
    output_parquet: str | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    bbox: str | None = None,
    geometry: str | None = None,
    where: str | None = None,
    limit: int | None = None,
    skip_count: bool = False,
    use_first_geometry: bool = False,
    dry_run: bool = False,
    show_sql: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    allow_schema_diff: bool = False,
    hive_input: bool = False,
) -> None:
    """
    Extract columns and rows from GeoParquet files.

    Supports column selection, spatial filtering (bbox, geometry),
    SQL filtering, and multiple input files via glob patterns.

    Also supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    S3 access mode (anonymous vs authenticated) is auto-detected per bucket.
    """
    _extract_impl(
        input_parquet,
        output_parquet,
        include_cols,
        exclude_cols,
        bbox,
        geometry,
        where,
        limit,
        skip_count,
        use_first_geometry,
        dry_run,
        show_sql,
        verbose,
        compression,
        compression_level,
        row_group_size_mb,
        row_group_rows,
        profile,
        geoparquet_version,
        allow_schema_diff,
        hive_input,
    )


def _extract_impl(
    input_parquet: str,
    output_parquet: str | None,
    include_cols: str | None,
    exclude_cols: str | None,
    bbox: str | None,
    geometry: str | None,
    where: str | None,
    limit: int | None,
    skip_count: bool,
    use_first_geometry: bool,
    dry_run: bool,
    show_sql: bool,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
    allow_schema_diff: bool = False,
    hive_input: bool = False,
) -> None:
    """Internal implementation of extract with auto-detecting S3 access."""
    # Parse column lists
    include_list = [c.strip() for c in include_cols.split(",")] if include_cols else None
    exclude_list = [c.strip() for c in exclude_cols.split(",")] if exclude_cols else None

    # Check for streaming mode (stdin input or stdout output)
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    if is_streaming and not dry_run:
        # Parse bbox and geometry if provided
        bbox_tuple = parse_bbox(bbox) if bbox else None
        geometry_wkt = parse_geometry_input(geometry, use_first_geometry) if geometry else None

        # Validate WHERE clause
        if where:
            validate_where_clause(where)

        return _extract_streaming(
            input_parquet,
            output_parquet,
            include_list,
            exclude_list,
            bbox_tuple,
            geometry_wkt,
            where,
            limit,
            verbose,
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            profile,
            geoparquet_version,
        )

    # File-based mode - get schema info early for validation
    # DuckDB handles glob patterns natively for all metadata operations
    all_columns = get_schema_columns(input_parquet)
    geometry_col = find_primary_geometry_column(input_parquet, verbose)
    bbox_info = check_bbox_structure(input_parquet, verbose=False)
    bbox_col = bbox_info.get("bbox_column_name")

    # Validate columns in both lists - only geometry and bbox allowed in both
    if include_list and exclude_list:
        special_cols = {geometry_col}
        if bbox_col:
            special_cols.add(bbox_col)

        overlap = set(include_list) & set(exclude_list)
        non_special_overlap = overlap - special_cols
        if non_special_overlap:
            raise click.ClickException(
                f"Columns cannot be in both --include-cols and --exclude-cols: "
                f"{', '.join(sorted(non_special_overlap))}\n"
                f"Only geometry ({geometry_col}) and bbox ({bbox_col}) columns can appear in both."
            )

    # Validate columns
    validate_columns(include_list, all_columns, "--include-cols")
    validate_columns(exclude_list, all_columns, "--exclude-cols")

    # Validate WHERE clause for dangerous SQL keywords
    if where:
        validate_where_clause(where)

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

    # Build the query - pass original input_parquet, partition reader handles path resolution
    query = build_extract_query(
        input_parquet,
        selected_columns,
        spatial_filter,
        where,
        limit,
        allow_schema_diff=allow_schema_diff,
        hive_input=hive_input,
    )
    safe_url = safe_file_url(input_parquet, verbose)

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
            limit,
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
            limit,
            skip_count,
            selected_columns,
            verbose,
            show_sql,
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            profile,
            geoparquet_version,
        )
