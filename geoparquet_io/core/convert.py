#!/usr/bin/env python3

import os
import time

import click
import duckdb

from geoparquet_io.core.common import (
    _format_crs_display,
    detect_crs_from_spatial_file,
    extract_crs_from_parquet,
    format_size,
    get_duckdb_connection,
    get_remote_error_hint,
    is_default_crs,
    is_partition_path,
    is_remote_url,
    needs_httpfs,
    parse_crs_string_to_projjson,
    safe_file_url,
    setup_aws_profile_if_needed,
    show_remote_read_message,
    validate_output_path,
    validate_profile_for_urls,
    write_parquet_with_metadata,
)
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success, warn
from geoparquet_io.core.partition_reader import require_single_file
from geoparquet_io.core.stream_io import _quote_identifier


def _detect_geometry_column(con, input_file, verbose, is_parquet=False):
    """Detect geometry column name from input file."""
    if verbose:
        debug("Detecting geometry column from input...")

    # For parquet files, read directly; for other formats use ST_Read
    if is_parquet:
        detect_query = f"SELECT * FROM read_parquet('{input_file}') LIMIT 0"
    else:
        detect_query = f"SELECT * FROM ST_Read('{input_file}') LIMIT 0"

    schema_result = con.execute(detect_query).description

    for col_info in schema_result:
        col_name = col_info[0].lower()
        if col_name in ["geom", "geometry", "wkb_geometry", "shape"]:
            if verbose:
                debug(f"Detected geometry column: {col_info[0]}")
            return col_info[0]

    raise click.ClickException(
        "Could not detect geometry column in input file. "
        "Expected column named 'geom', 'geometry', 'wkb_geometry', or 'shape'."
    )


def _calculate_bounds(con, input_file, geom_column, verbose, is_parquet=False):
    """Calculate dataset bounds from input file."""
    if verbose:
        debug("Calculating dataset bounds...")

    # For parquet files, read directly; for other formats use ST_Read
    if is_parquet:
        table_expr = f"read_parquet('{input_file}')"
    else:
        table_expr = f"ST_Read('{input_file}')"

    bounds_query = f"""
        SELECT
            MIN(ST_XMin({geom_column})) as xmin,
            MIN(ST_YMin({geom_column})) as ymin,
            MAX(ST_XMax({geom_column})) as xmax,
            MAX(ST_YMax({geom_column})) as ymax
        FROM {table_expr}
    """
    bounds_result = con.execute(bounds_query).fetchone()

    if not bounds_result or any(v is None for v in bounds_result):
        raise click.ClickException("Could not calculate dataset bounds")

    if verbose:
        xmin, ymin, xmax, ymax = bounds_result
        debug(f"Dataset bounds: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")

    return bounds_result


def _is_csv_file(input_file):
    """Check if input file is CSV/TSV format."""
    ext = os.path.splitext(input_file)[1].lower()
    return ext in [".csv", ".tsv", ".txt"]


def _is_parquet_file(input_file):
    """Check if input file is already Parquet format."""
    # Handle URLs by extracting path before query params
    path = input_file.split("?")[0]
    ext = os.path.splitext(path)[1].lower()
    return ext == ".parquet"


def _build_csv_read_expr(input_file, delimiter):
    """Build DuckDB CSV read expression."""
    if delimiter:
        return f"read_csv('{input_file}', delim='{delimiter}', header=true, AUTO_DETECT=TRUE)"
    return f"read_csv_auto('{input_file}')"


def _get_csv_columns(con, csv_read):
    """Get column names from CSV, return (columns_list, col_names_lower_dict)."""
    columns = con.execute(f"SELECT * FROM {csv_read} LIMIT 0").description
    col_names_lower = {col[0].lower(): col[0] for col in columns}
    return columns, col_names_lower


def _validate_explicit_wkt_column(wkt_column, columns):
    """Validate explicitly specified WKT column exists."""
    actual_cols = [col[0] for col in columns]
    if wkt_column not in actual_cols:
        raise click.ClickException(
            f"Specified WKT column '{wkt_column}' not found in CSV. "
            f"Available columns: {', '.join(actual_cols)}"
        )


def _validate_explicit_latlon_columns(lat_column, lon_column, columns):
    """Validate explicitly specified lat/lon columns exist."""
    if not (lat_column and lon_column):
        raise click.ClickException("Both --lat-column and --lon-column must be specified together")

    actual_cols = [col[0] for col in columns]
    if lat_column not in actual_cols:
        raise click.ClickException(
            f"Specified latitude column '{lat_column}' not found in CSV. "
            f"Available columns: {', '.join(actual_cols)}"
        )
    if lon_column not in actual_cols:
        raise click.ClickException(
            f"Specified longitude column '{lon_column}' not found in CSV. "
            f"Available columns: {', '.join(actual_cols)}"
        )


def _try_detect_wkt_column(con, csv_read, col_names_lower):
    """Try to auto-detect WKT column. Returns column name or None."""
    wkt_candidates = ["wkt", "geometry", "geom", "the_geom", "shape"]
    for candidate in wkt_candidates:
        if candidate in col_names_lower:
            actual_col = col_names_lower[candidate]
            try:
                # Validate by trying to parse sample row
                sample = con.execute(
                    f"SELECT {actual_col} FROM {csv_read} WHERE {actual_col} IS NOT NULL LIMIT 1"
                ).fetchone()
                if sample and sample[0]:
                    con.execute(f"SELECT ST_GeomFromText('{sample[0]}')").fetchone()
                    return actual_col
            except Exception:
                continue
    return None


def _try_detect_latlon_columns(col_names_lower):
    """Try to auto-detect lat/lon columns. Returns (lat_col, lon_col) or (None, None)."""
    lat_candidates = ["lat", "latitude", "y"]
    lon_candidates = ["lon", "lng", "long", "longitude", "x"]

    found_lat = next(
        (col_names_lower[name] for name in lat_candidates if name in col_names_lower), None
    )
    found_lon = next(
        (col_names_lower[name] for name in lon_candidates if name in col_names_lower), None
    )

    return found_lat, found_lon


def _handle_explicit_columns(wkt_column, lat_column, lon_column, columns, csv_read):
    """Handle explicitly specified columns. Returns geom_info dict or None."""
    if wkt_column:
        _validate_explicit_wkt_column(wkt_column, columns)
        return {"type": "wkt", "wkt_column": wkt_column, "csv_read": csv_read}

    if lat_column or lon_column:
        _validate_explicit_latlon_columns(lat_column, lon_column, columns)
        return {
            "type": "latlon",
            "lat_column": lat_column,
            "lon_column": lon_column,
            "csv_read": csv_read,
        }

    return None


def _auto_detect_geometry(con, csv_read, col_names_lower, verbose):
    """Auto-detect geometry columns. Returns geom_info dict or None."""
    # Try WKT first
    wkt_col = _try_detect_wkt_column(con, csv_read, col_names_lower)
    if wkt_col:
        if verbose:
            debug(f"Auto-detected WKT column: {wkt_col}")
        return {"type": "wkt", "wkt_column": wkt_col, "csv_read": csv_read}

    # Try lat/lon
    found_lat, found_lon = _try_detect_latlon_columns(col_names_lower)
    if found_lat and found_lon:
        if verbose:
            debug(f"Auto-detected lat/lon columns: {found_lat}, {found_lon}")
        return {
            "type": "latlon",
            "lat_column": found_lat,
            "lon_column": found_lon,
            "csv_read": csv_read,
        }

    return None


def _detect_csv_geometry_column(
    con, input_file, delimiter, wkt_column, lat_column, lon_column, verbose
):
    """Detect geometry columns in CSV/TSV."""
    csv_read = _build_csv_read_expr(input_file, delimiter)
    columns, col_names_lower = _get_csv_columns(con, csv_read)

    if verbose:
        delim_msg = delimiter if delimiter else "auto-detected"
        debug(f"Reading CSV/TSV with delimiter: {delim_msg}")
        debug(f"Detected columns: {', '.join([col[0] for col in columns])}")

    # Try explicit columns first
    geom_info = _handle_explicit_columns(wkt_column, lat_column, lon_column, columns, csv_read)
    if geom_info:
        return geom_info

    # Auto-detect
    geom_info = _auto_detect_geometry(con, csv_read, col_names_lower, verbose)
    if geom_info:
        return geom_info

    # No geometry found
    raise click.ClickException(
        "Could not detect geometry columns in CSV/TSV file.\n"
        "Expected one of:\n"
        "  - WKT column named: wkt, geometry, geom, the_geom, or shape\n"
        "  - Lat/lon columns named: lat/lon, latitude/longitude, or y/x\n"
        "Use --wkt-column or --lat-column/--lon-column to specify explicitly."
    )


def _validate_latlon_ranges(con, csv_read, lat_col, lon_col, verbose):
    """Validate lat/lon columns have valid numeric ranges."""
    if verbose:
        debug(f"Validating lat/lon ranges for columns: {lat_col}, {lon_col}")

    query = f"""
        SELECT
            MIN(CAST({lat_col} AS DOUBLE)) as min_lat,
            MAX(CAST({lat_col} AS DOUBLE)) as max_lat,
            MIN(CAST({lon_col} AS DOUBLE)) as min_lon,
            MAX(CAST({lon_col} AS DOUBLE)) as max_lon,
            COUNT(*) FILTER ({lat_col} IS NULL OR {lon_col} IS NULL) as null_count
        FROM {csv_read}
    """

    try:
        result = con.execute(query).fetchone()
        min_lat, max_lat, min_lon, max_lon, null_count = result

        if null_count > 0:
            warn(f"⚠️  Warning: {null_count} rows have NULL lat/lon values and will be skipped")

        if min_lat < -90 or max_lat > 90:
            raise click.ClickException(
                f"Invalid latitude values (range: {min_lat:.6f} to {max_lat:.6f}). "
                f"Latitude must be between -90 and 90."
            )

        if min_lon < -180 or max_lon > 180:
            raise click.ClickException(
                f"Invalid longitude values (range: {min_lon:.6f} to {max_lon:.6f}). "
                f"Longitude must be between -180 and 180."
            )

        if verbose:
            debug(
                f"Lat/lon ranges validated: lat=[{min_lat:.6f}, {max_lat:.6f}], "
                f"lon=[{min_lon:.6f}, {max_lon:.6f}]"
            )

    except duckdb.ConversionException as e:
        raise click.ClickException(
            f"Lat/lon columns contain non-numeric values: {str(e)}\n"
            "Ensure lat/lon columns contain only numbers."
        ) from e


def _check_null_wkt_rows(con, csv_read, wkt_col):
    """Check and warn about NULL WKT values."""
    null_count = con.execute(
        f"SELECT COUNT(*) FILTER ({wkt_col} IS NULL) FROM {csv_read}"
    ).fetchone()[0]

    if null_count > 0:
        warn(f"⚠️  Warning: {null_count} rows have NULL WKT values and will be skipped")


def _check_invalid_wkt_rows(con, csv_read, wkt_col):
    """Check and warn about invalid WKT (when skip_invalid is True)."""
    try:
        invalid_count = con.execute(
            f"SELECT COUNT(*) FROM {csv_read} "
            f"WHERE {wkt_col} IS NOT NULL AND TRY_CAST(ST_GeomFromText({wkt_col}) AS VARCHAR) IS NULL"
        ).fetchone()[0]

        if invalid_count > 0:
            warn(f"⚠️  Warning: {invalid_count} rows have invalid WKT and will be skipped")
    except Exception:
        pass  # DuckDB version might not support TRY_CAST


def _validate_wkt_strict(con, csv_read, wkt_col):
    """Strictly validate WKT (when skip_invalid is False)."""
    try:
        con.execute(
            f"SELECT ST_GeomFromText({wkt_col}) FROM {csv_read} WHERE {wkt_col} IS NOT NULL LIMIT 1"
        ).fetchone()
    except Exception as e:
        raise click.ClickException(
            f"Invalid WKT in column '{wkt_col}': {str(e)}\n"
            f"Use --skip-invalid to skip rows with invalid geometries."
        ) from e


def _warn_if_projected_crs(con, csv_read, wkt_col):
    """Warn if coordinates suggest projected CRS instead of WGS84."""
    try:
        result = con.execute(
            f"SELECT MAX(ABS(ST_XMax(ST_GeomFromText({wkt_col})))) as max_x, "
            f"MAX(ABS(ST_YMax(ST_GeomFromText({wkt_col})))) as max_y "
            f"FROM {csv_read} WHERE {wkt_col} IS NOT NULL LIMIT 1000"
        ).fetchone()

        if result and result[0] is not None:
            max_x, max_y = result
            if max_x > 180 or max_y > 90:
                warn(
                    f"⚠️  Large coordinate values detected (max X: {max_x:.2f}, max Y: {max_y:.2f}). "
                    f"Data may be in projected CRS, not WGS84. "
                    f"Verify CRS or use --crs flag if needed."
                )
    except Exception:
        pass


def _validate_wkt_and_check_crs(con, csv_read, wkt_col, skip_invalid, verbose):
    """Validate WKT column and warn if coordinates suggest non-WGS84 CRS."""
    if verbose:
        debug(f"Validating WKT column: {wkt_col}")

    _check_null_wkt_rows(con, csv_read, wkt_col)

    if skip_invalid:
        _check_invalid_wkt_rows(con, csv_read, wkt_col)
    else:
        _validate_wkt_strict(con, csv_read, wkt_col)

    _warn_if_projected_crs(con, csv_read, wkt_col)


def _build_csv_conversion_query(geom_info, skip_hilbert, bounds, skip_invalid, skip_bbox=False):
    """Build SQL query for CSV/TSV conversion with geometry construction.

    Args:
        geom_info: Dict with geometry detection info
        skip_hilbert: Skip Hilbert ordering
        bounds: Tuple of bounds for Hilbert ordering
        skip_invalid: Skip invalid geometries
        skip_bbox: Skip adding bbox column (for 2.0/parquet-geo-only)
    """
    csv_read = geom_info["csv_read"]

    # Build bbox expression (empty string if skipping)
    def bbox_expr(geom):
        if skip_bbox:
            return ""
        return f""",
                STRUCT_PACK(
                    xmin := ST_XMin({geom}),
                    ymin := ST_YMin({geom}),
                    xmax := ST_XMax({geom}),
                    ymax := ST_YMax({geom})
                ) AS bbox"""

    # Build geometry expression and exclusion list
    if geom_info["type"] == "wkt":
        wkt_col = geom_info["wkt_column"]
        geom_expr = f"ST_GeomFromText({wkt_col})"
        exclude_cols = wkt_col

        # For skip_invalid, we need to wrap the geometry parsing to skip errors
        # Use TRY_CAST which silently returns NULL for invalid WKT
        if skip_invalid:
            # Use TRY_CAST to parse WKT and return NULL for invalid strings
            query_base = f"""
                WITH parsed_geoms AS (
                    SELECT
                        * EXCLUDE ({exclude_cols}),
                        TRY_CAST({wkt_col} AS GEOMETRY) AS geometry
                    FROM {csv_read}
                )
                SELECT
                    * EXCLUDE (geometry),
                    geometry{bbox_expr("geometry")}
                FROM parsed_geoms
                WHERE geometry IS NOT NULL
            """
            return query_base
        else:
            where_clause = f"WHERE {wkt_col} IS NOT NULL"

    elif geom_info["type"] == "latlon":
        lat_col = geom_info["lat_column"]
        lon_col = geom_info["lon_column"]
        # Note: ST_Point expects (lon, lat) order
        geom_expr = f"ST_Point(CAST({lon_col} AS DOUBLE), CAST({lat_col} AS DOUBLE))"
        exclude_cols = f"{lat_col}, {lon_col}"

        # Skip rows with NULL lat/lon
        where_clause = f"WHERE {lat_col} IS NOT NULL AND {lon_col} IS NOT NULL"

    else:
        raise click.ClickException("Unknown geometry type in CSV detection")

    # Build base query (for non-skip_invalid or lat/lon)
    if skip_hilbert:
        return f"""
            SELECT
                * EXCLUDE ({exclude_cols}),
                {geom_expr} AS geometry{bbox_expr(geom_expr)}
            FROM {csv_read}
            {where_clause}
        """

    # With Hilbert ordering - use subquery
    xmin, ymin, xmax, ymax = bounds
    return f"""
        SELECT
            * EXCLUDE ({exclude_cols}),
            {geom_expr} AS geometry{bbox_expr(geom_expr)}
        FROM {csv_read}
        {where_clause}
        ORDER BY ST_Hilbert(
            {geom_expr},
            ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))
        )
    """


def _get_geom_expr_and_where(geom_info, skip_invalid):
    """Get geometry expression and WHERE clause for CSV bounds/query."""
    if geom_info["type"] == "wkt":
        wkt_col = geom_info["wkt_column"]
        geom_expr = f"ST_GeomFromText({wkt_col})"
        where_clause = (
            f"WHERE {wkt_col} IS NOT NULL AND {geom_expr} IS NOT NULL"
            if skip_invalid
            else f"WHERE {wkt_col} IS NOT NULL"
        )
        return geom_expr, where_clause

    # latlon
    lat_col = geom_info["lat_column"]
    lon_col = geom_info["lon_column"]
    geom_expr = f"ST_Point(CAST({lon_col} AS DOUBLE), CAST({lat_col} AS DOUBLE))"
    where_clause = f"WHERE {lat_col} IS NOT NULL AND {lon_col} IS NOT NULL"
    return geom_expr, where_clause


def _calculate_csv_bounds(con, geom_info, skip_invalid, verbose):
    """Calculate dataset bounds from CSV geometry."""
    if verbose:
        debug("Calculating dataset bounds from CSV...")

    csv_read = geom_info["csv_read"]
    geom_expr, where_clause = _get_geom_expr_and_where(geom_info, skip_invalid)

    bounds_query = f"""
        SELECT
            MIN(ST_XMin({geom_expr})) as xmin,
            MIN(ST_YMin({geom_expr})) as ymin,
            MAX(ST_XMax({geom_expr})) as xmax,
            MAX(ST_YMax({geom_expr})) as ymax
        FROM {csv_read}
        {where_clause}
    """

    try:
        bounds_result = con.execute(bounds_query).fetchone()
    except Exception as e:
        msg = (
            "Could not calculate bounds - no valid geometries found in CSV"
            if skip_invalid
            else str(e)
        )
        raise click.ClickException(msg) from e

    if not bounds_result or any(v is None for v in bounds_result):
        raise click.ClickException("Could not calculate dataset bounds from CSV")

    if verbose:
        xmin, ymin, xmax, ymax = bounds_result
        debug(f"Dataset bounds: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")

    return bounds_result


def _build_conversion_query(
    input_file,
    geom_column,
    skip_hilbert,
    bounds=None,
    is_parquet=False,
    skip_bbox=False,
    existing_bbox_col=None,
    preserve_existing_bbox=False,
):
    """Build SQL query for conversion with optional Hilbert ordering.

    Args:
        input_file: Path to input file
        geom_column: Name of geometry column
        skip_hilbert: Skip Hilbert ordering
        bounds: Tuple of (xmin, ymin, xmax, ymax) for Hilbert ordering
        is_parquet: Whether input is a parquet file
        skip_bbox: Skip adding bbox column (for 2.0/parquet-geo-only)
        existing_bbox_col: Name of existing bbox column to remove (for parquet input)
        preserve_existing_bbox: If True, keep existing bbox column instead of adding new one
    """
    # For parquet files, read directly; for other formats use ST_Read
    if is_parquet:
        table_expr = f"read_parquet('{input_file}')"
    else:
        table_expr = f"ST_Read('{input_file}')"

    # Build exclusion list - always exclude geom_column, optionally exclude existing bbox
    exclude_cols = [geom_column]
    if existing_bbox_col and skip_bbox:
        # For 2.0: remove existing bbox column (not needed for native geo types)
        exclude_cols.append(existing_bbox_col)
    exclude_clause = ", ".join(exclude_cols)

    if skip_bbox:
        # For 2.0/parquet-geo-only: don't add bbox column
        base_select = f"""
            SELECT
                * EXCLUDE ({exclude_clause}),
                {geom_column} AS geometry
            FROM {table_expr}
        """
    elif preserve_existing_bbox:
        # For 1.x with existing bbox: preserve existing bbox column, don't add new one
        base_select = f"""
            SELECT
                * EXCLUDE ({exclude_clause}),
                {geom_column} AS geometry
            FROM {table_expr}
        """
    else:
        # For 1.x without existing bbox: add bbox column
        base_select = f"""
            SELECT
                * EXCLUDE ({exclude_clause}),
                {geom_column} AS geometry,
                STRUCT_PACK(
                    xmin := ST_XMin({geom_column}),
                    ymin := ST_YMin({geom_column}),
                    xmax := ST_XMax({geom_column}),
                    ymax := ST_YMax({geom_column})
                ) AS bbox
            FROM {table_expr}
        """

    if skip_hilbert:
        return base_select

    xmin, ymin, xmax, ymax = bounds
    return f"""{base_select}
        ORDER BY ST_Hilbert(
            {geom_column},
            ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))
        )
    """


def _convert_csv_path(
    con,
    input_file,
    delimiter,
    wkt_column,
    lat_column,
    lon_column,
    crs,
    skip_hilbert,
    skip_invalid,
    verbose,
    geoparquet_version=None,
):
    """Handle CSV/TSV conversion path. Returns SQL query."""
    from geoparquet_io.core.common import should_skip_bbox

    # Determine if bbox should be skipped for this version
    skip_bbox = should_skip_bbox(geoparquet_version)

    geom_info = _detect_csv_geometry_column(
        con, input_file, delimiter, wkt_column, lat_column, lon_column, verbose
    )

    # Validate geometry
    if geom_info["type"] == "wkt":
        progress(f"Using WKT column: {geom_info['wkt_column']}")
        _validate_wkt_and_check_crs(
            con, geom_info["csv_read"], geom_info["wkt_column"], skip_invalid, verbose
        )
    else:  # latlon
        progress(f"Using lat/lon columns: {geom_info['lat_column']}, {geom_info['lon_column']}")
        _validate_latlon_ranges(
            con, geom_info["csv_read"], geom_info["lat_column"], geom_info["lon_column"], verbose
        )

    progress(f"Assuming CRS: {crs}")

    # Skip Hilbert if using skip_invalid
    effective_skip_hilbert = skip_hilbert or skip_invalid
    if skip_invalid and not skip_hilbert:
        warn("Note: Skipping Hilbert ordering due to --skip-invalid flag")

    # Calculate bounds if needed
    bounds = (
        None
        if effective_skip_hilbert
        else _calculate_csv_bounds(con, geom_info, skip_invalid, verbose)
    )

    if verbose:
        if skip_bbox:
            msg = "Reading CSV and creating geometries (skipping bbox for native geo types)..."
            if not effective_skip_hilbert:
                msg = "Reading CSV, creating geometries, and applying Hilbert ordering (skipping bbox)..."
        else:
            msg = "Reading CSV and creating geometries..."
            if not effective_skip_hilbert:
                msg = "Reading CSV, creating geometries, and applying Hilbert ordering..."
        debug(msg)

    return _build_csv_conversion_query(
        geom_info, effective_skip_hilbert, bounds, skip_invalid, skip_bbox=skip_bbox
    )


def _convert_spatial_path(
    con, input_file, skip_hilbert, verbose, is_parquet=False, geoparquet_version=None
):
    """Handle standard spatial format conversion path. Returns SQL query."""
    from geoparquet_io.core.common import check_bbox_structure, should_skip_bbox

    geom_column = _detect_geometry_column(con, input_file, verbose, is_parquet=is_parquet)

    # Determine if bbox should be skipped for this version
    skip_bbox = should_skip_bbox(geoparquet_version)

    # Check for existing bbox column if input is parquet
    existing_bbox_col = None
    preserve_existing_bbox = False
    if is_parquet:
        bbox_info = check_bbox_structure(input_file, verbose=False)
        if bbox_info["has_bbox_column"]:
            existing_bbox_col = bbox_info["bbox_column_name"]
            if skip_bbox:
                # For 2.0/parquet-geo-only: remove bbox (not needed for native geo types)
                progress(
                    f"Removing bbox column '{existing_bbox_col}' (not needed for native geo types)"
                )
            else:
                # For 1.x: preserve existing valid bbox column
                preserve_existing_bbox = True
                if verbose:
                    debug(f"Preserving existing bbox column: {existing_bbox_col}")

    bounds = (
        None
        if skip_hilbert
        else _calculate_bounds(con, input_file, geom_column, verbose, is_parquet=is_parquet)
    )

    if verbose:
        if skip_bbox:
            msg = "Reading input (skipping bbox for native geo types)..."
            if not skip_hilbert:
                msg = "Pass 1: Reading input and applying Hilbert ordering (skipping bbox)..."
        elif preserve_existing_bbox:
            msg = "Reading input (preserving existing bbox)..."
            if not skip_hilbert:
                msg = "Pass 1: Reading input and applying Hilbert ordering (preserving bbox)..."
        else:
            msg = "Reading input and adding bbox column..."
            if not skip_hilbert:
                msg = "Pass 1: Reading input, adding bbox, and applying Hilbert ordering..."
        debug(msg)

    return _build_conversion_query(
        input_file,
        geom_column,
        skip_hilbert,
        bounds,
        is_parquet=is_parquet,
        skip_bbox=skip_bbox,
        existing_bbox_col=existing_bbox_col,
        preserve_existing_bbox=preserve_existing_bbox,
    )


def read_spatial_to_arrow(
    input_file,
    *,
    verbose=False,
    wkt_column=None,
    lat_column=None,
    lon_column=None,
    delimiter=None,
    crs="EPSG:4326",
    skip_invalid=False,
    profile=None,
    geometry_column="geometry",
):
    """
    Read a geospatial file and return an Arrow table with geometry.

    This is the core reading function used by both the Python API and CLI.
    Does NOT apply Hilbert sorting or bbox column - those are chainable operations.

    Args:
        input_file: Path to input file (GeoPackage, GeoJSON, Shapefile, CSV/TSV, etc.)
        verbose: Print detailed progress
        wkt_column: CSV/TSV only - WKT column name (auto-detected if not specified)
        lat_column: CSV/TSV only - Latitude column name (requires lon_column)
        lon_column: CSV/TSV only - Longitude column name (requires lat_column)
        delimiter: CSV/TSV only - Delimiter character (auto-detected if not specified)
        crs: CRS for CSV geometry data (default: EPSG:4326/WGS84)
        skip_invalid: Skip rows with invalid geometries instead of failing
        profile: AWS profile name for S3 operations
        geometry_column: Name for output geometry column (default: 'geometry')

    Returns:
        tuple: (arrow_table, detected_crs_projjson, geometry_column_name)

    Raises:
        click.ClickException: If input file not found or reading fails
    """

    configure_verbose(verbose)

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, input_file)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, input_file)

    # Show progress for remote files
    show_remote_read_message(input_file, verbose=False)

    # Get safe URL
    input_url = safe_file_url(input_file, verbose)

    # Check input file type
    is_csv = _is_csv_file(input_file)
    is_parquet = _is_parquet_file(input_file)

    # Check for partitioned parquet input (not supported)
    if is_parquet and is_partition_path(input_file):
        require_single_file(input_file, "read_spatial_to_arrow")

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_file))

    # Determine CRS
    user_specified_crs = crs != "EPSG:4326"
    detected_crs = None

    try:
        if user_specified_crs:
            if not is_csv:
                raise click.ClickException(
                    f"The crs option is only valid for CSV/TSV files.\n"
                    f"For {os.path.splitext(input_file)[1]} files, CRS is read from the file metadata."
                )
            detected_crs = parse_crs_string_to_projjson(crs, con)
            if verbose:
                debug(f"Using user-specified CRS: {crs}")
        elif is_csv:
            # CSV with default CRS - detected_crs stays None
            pass
        elif is_parquet:
            crs_from_file = extract_crs_from_parquet(input_url, verbose=verbose)
            if crs_from_file and not is_default_crs(crs_from_file):
                detected_crs = crs_from_file
                if verbose:
                    debug(f"Preserving input CRS: {_format_crs_display(detected_crs)}")
        else:
            # Spatial files - detect CRS
            crs_from_file = detect_crs_from_spatial_file(input_url, con, verbose=verbose)
            if crs_from_file is None:
                raise click.ClickException(
                    f"No CRS found in input file: {input_file}\n"
                    f"Spatial files (GeoPackage, Shapefile, GeoJSON, etc.) must have a defined CRS."
                )
            if not is_default_crs(crs_from_file):
                detected_crs = crs_from_file
                if verbose:
                    debug(f"Detected input CRS: {_format_crs_display(detected_crs)}")

        # Build and execute query
        if is_csv:
            arrow_table = _read_csv_to_arrow(
                con, input_url, delimiter, wkt_column, lat_column, lon_column, skip_invalid, verbose
            )
        else:
            arrow_table = _read_spatial_to_arrow(con, input_url, verbose, is_parquet=is_parquet)

        return arrow_table, detected_crs, geometry_column

    except duckdb.IOException as e:
        error_msg = str(e)
        if is_remote_url(input_file):
            hints = get_remote_error_hint(error_msg, input_file)
            raise click.ClickException(
                f"Failed to read remote file.\n\n{hints}\n\nOriginal error: {error_msg}"
            ) from e
        raise click.ClickException(f"Failed to read input file: {error_msg}") from e

    except duckdb.BinderException as e:
        raise click.ClickException(f"Invalid geometry data: {str(e)}") from e

    except Exception as e:
        raise click.ClickException(f"Reading failed: {str(e)}") from e

    finally:
        con.close()


def _read_csv_to_arrow(
    con, input_url, delimiter, wkt_column, lat_column, lon_column, skip_invalid, verbose
):
    """Read CSV/TSV to Arrow table with geometry as WKB."""
    geom_info = _detect_csv_geometry_column(
        con, input_url, delimiter, wkt_column, lat_column, lon_column, verbose
    )

    # Validate geometry
    if geom_info["type"] == "wkt":
        if verbose:
            progress(f"Using WKT column: {geom_info['wkt_column']}")
        _validate_wkt_and_check_crs(
            con, geom_info["csv_read"], geom_info["wkt_column"], skip_invalid, verbose
        )
    else:
        if verbose:
            progress(f"Using lat/lon columns: {geom_info['lat_column']}, {geom_info['lon_column']}")
        _validate_latlon_ranges(
            con, geom_info["csv_read"], geom_info["lat_column"], geom_info["lon_column"], verbose
        )

    csv_read = geom_info["csv_read"]

    # Build query based on geometry type
    if geom_info["type"] == "wkt":
        wkt_col = _quote_identifier(geom_info["wkt_column"])
        if skip_invalid:
            query = f"""
                SELECT * EXCLUDE ({wkt_col}),
                       ST_AsWKB(TRY_CAST({wkt_col} AS GEOMETRY)) AS geometry
                FROM {csv_read}
                WHERE TRY_CAST({wkt_col} AS GEOMETRY) IS NOT NULL
            """
        else:
            query = f"""
                SELECT * EXCLUDE ({wkt_col}),
                       ST_AsWKB(ST_GeomFromText({wkt_col})) AS geometry
                FROM {csv_read}
                WHERE {wkt_col} IS NOT NULL
            """
    else:  # latlon
        lat_col = _quote_identifier(geom_info["lat_column"])
        lon_col = _quote_identifier(geom_info["lon_column"])
        query = f"""
            SELECT * EXCLUDE ({lat_col}, {lon_col}),
                   ST_AsWKB(ST_Point(CAST({lon_col} AS DOUBLE), CAST({lat_col} AS DOUBLE))) AS geometry
            FROM {csv_read}
            WHERE {lat_col} IS NOT NULL AND {lon_col} IS NOT NULL
        """

    result = con.execute(query)
    return result.fetch_arrow_table()


def _read_spatial_to_arrow(con, input_url, verbose, is_parquet=False):
    """Read spatial file to Arrow table with geometry as WKB."""
    geom_column = _detect_geometry_column(con, input_url, verbose, is_parquet=is_parquet)
    quoted_geom = _quote_identifier(geom_column)

    if is_parquet:
        table_expr = f"read_parquet('{input_url}')"
    else:
        table_expr = f"ST_Read('{input_url}')"

    # Convert geometry to WKB for geoarrow compatibility
    query = f"""
        SELECT * EXCLUDE ({quoted_geom}),
               ST_AsWKB({quoted_geom}) AS geometry
        FROM {table_expr}
    """

    result = con.execute(query)
    return result.fetch_arrow_table()


def convert_to_geoparquet(
    input_file,
    output_file,
    skip_hilbert=False,
    verbose=False,
    compression="ZSTD",
    compression_level=15,
    row_group_rows=100000,
    wkt_column=None,
    lat_column=None,
    lon_column=None,
    delimiter=None,
    crs="EPSG:4326",
    skip_invalid=False,
    profile=None,
    geoparquet_version=None,
):
    """
    Convert vector format to optimized GeoParquet.

    Applies best practices:
    - ZSTD compression
    - 100k row groups
    - Bbox column with metadata
    - Hilbert spatial ordering (unless --skip-hilbert)
    - GeoParquet metadata (version configurable)

    Args:
        input_file: Path to input file (Shapefile, GeoJSON, GeoPackage, CSV/TSV, etc.)
        output_file: Path to output GeoParquet file
        skip_hilbert: Skip Hilbert ordering (faster, less optimal)
        verbose: Print detailed progress
        compression: Compression type (default: ZSTD)
        compression_level: Compression level (default: 15)
        row_group_rows: Rows per group (default: 100000)
        wkt_column: CSV/TSV only - WKT column name (auto-detected if not specified)
        lat_column: CSV/TSV only - Latitude column name (requires lon_column)
        lon_column: CSV/TSV only - Longitude column name (requires lat_column)
        delimiter: CSV/TSV only - Delimiter character (auto-detected if not specified)
        crs: CRS for geometry data (default: EPSG:4326/WGS84)
        skip_invalid: Skip rows with invalid geometries instead of failing
        profile: AWS profile name for S3 operations
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)

    Raises:
        click.ClickException: If input file not found or conversion fails
    """
    # Configure logging verbosity
    configure_verbose(verbose)

    start_time = time.time()

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, input_file, output_file)

    # Setup AWS profile if needed (for DuckDB reads and obstore writes)
    setup_aws_profile_if_needed(profile, input_file, output_file)

    # Show single progress message for remote files
    show_remote_read_message(input_file, verbose=False)

    # Validate input file exists (for local files) and get safe URL
    input_url = safe_file_url(input_file, verbose)

    # Validate output directory exists and is writable (for local files)
    validate_output_path(output_file, verbose)

    progress(f"Converting {input_file}...")

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_file))

    # Check input file type
    is_csv = _is_csv_file(input_file)
    is_parquet = _is_parquet_file(input_file)

    # Check for partitioned parquet input (not supported)
    if is_parquet and is_partition_path(input_file):
        require_single_file(input_file, "convert")

    # Determine effective CRS for output
    # --crs parameter is ONLY valid for CSV/TSV files
    # For spatial files, CRS is auto-detected and must be present
    user_specified_crs = crs != "EPSG:4326"  # User explicitly provided non-default CRS
    effective_crs = None

    if user_specified_crs:
        # --crs is only valid for CSV/TSV files
        if not is_csv:
            raise click.ClickException(
                f"The --crs option is only valid for CSV/TSV files.\n"
                f"For {os.path.splitext(input_file)[1]} files, CRS is read from the file metadata."
            )
        # User explicitly specified a CRS for CSV, convert to PROJJSON
        effective_crs = parse_crs_string_to_projjson(crs, con)
        if verbose:
            debug(f"Using user-specified CRS: {crs}")
    elif is_csv:
        # CSV with default CRS - effective_crs stays None (use default)
        pass
    elif is_parquet:
        # Parquet files - detect CRS from file
        detected_crs = extract_crs_from_parquet(input_url, verbose=verbose)
        if detected_crs and not is_default_crs(detected_crs):
            effective_crs = detected_crs
            if verbose:
                debug(f"Preserving input CRS: {_format_crs_display(detected_crs)}")
    else:
        # Spatial files (GPKG, GeoJSON, Shapefile) - CRS must be present
        detected_crs = detect_crs_from_spatial_file(input_url, con, verbose=verbose)
        if detected_crs is None:
            raise click.ClickException(
                f"No CRS found in input file: {input_file}\n"
                f"Spatial files (GeoPackage, Shapefile, GeoJSON, etc.) must have a defined CRS."
            )
        # Only set effective_crs if it's non-default (skip writing default CRS)
        if is_default_crs(detected_crs):
            if verbose:
                debug("Input has default CRS (WGS84), not writing explicit CRS")
        else:
            effective_crs = detected_crs
            if verbose:
                debug(f"Detected input CRS: {_format_crs_display(detected_crs)}")

    try:
        if is_csv:
            query = _convert_csv_path(
                con,
                input_url,
                delimiter,
                wkt_column,
                lat_column,
                lon_column,
                crs,
                skip_hilbert,
                skip_invalid,
                verbose,
                geoparquet_version=geoparquet_version,
            )
        else:
            query = _convert_spatial_path(
                con,
                input_url,
                skip_hilbert,
                verbose,
                is_parquet=is_parquet,
                geoparquet_version=geoparquet_version,
            )

        write_parquet_with_metadata(
            con,
            query,
            output_file,
            original_metadata=None,
            compression=compression,
            compression_level=compression_level,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
            input_crs=effective_crs,
        )

        # Report results
        elapsed = time.time() - start_time
        # For remote files, we can't get the size locally, skip it
        if is_remote_url(output_file):
            file_size = None
        else:
            file_size = os.path.getsize(output_file)

        progress(f"Done in {elapsed:.1f}s")
        if file_size is not None:
            progress(f"Output: {output_file} ({format_size(file_size)})")
        else:
            progress(f"Output: {output_file}")
        success("✓ Output passes GeoParquet validation")

    except duckdb.IOException as e:
        con.close()
        error_msg = str(e)
        # Provide helpful hints for remote file errors
        if is_remote_url(input_file):
            hints = get_remote_error_hint(error_msg, input_file)
            raise click.ClickException(
                f"Failed to read remote file.\n\n{hints}\n\nOriginal error: {error_msg}"
            ) from e
        else:
            raise click.ClickException(f"Failed to read input file: {error_msg}") from e

    except duckdb.BinderException as e:
        con.close()
        raise click.ClickException(f"Invalid geometry data: {str(e)}") from e

    except OSError as e:
        con.close()
        if e.errno == 28:  # ENOSPC
            raise click.ClickException("Not enough disk space for output file") from e
        else:
            raise click.ClickException(f"File system error: {str(e)}") from e

    except Exception as e:
        con.close()
        raise click.ClickException(f"Conversion failed: {str(e)}") from e

    finally:
        con.close()


if __name__ == "__main__":
    convert_to_geoparquet()
