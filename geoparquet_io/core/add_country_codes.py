#!/usr/bin/env python3

import click
import duckdb

from geoparquet_io.core.common import (
    check_bbox_structure,
    find_primary_geometry_column,
    get_bbox_advice,
    get_dataset_bounds,
    get_parquet_metadata,
    safe_file_url,
    write_parquet_with_metadata,
)
from geoparquet_io.core.logging_config import debug, info, progress, success, warn


def find_country_code_column(con, countries_source, is_subquery=False):
    """
    Find the country code column in a countries dataset.

    Args:
        con: DuckDB connection
        countries_source: Either a file path or a subquery
        is_subquery: Whether countries_source is a subquery (True) or file path (False)

    Returns:
        str: The name of the country code column

    Raises:
        click.UsageError: If no suitable country code column is found
    """
    # Build appropriate query based on source type
    if is_subquery:
        columns_query = f"SELECT * FROM {countries_source} LIMIT 0;"
    else:
        columns_query = f"SELECT * FROM '{countries_source}' LIMIT 0;"

    countries_columns = [col[0] for col in con.execute(columns_query).description]

    # Define possible country code column names in priority order
    country_code_options = [
        "admin:country_code",
        "country_code",
        "country",
        "ISO_A2",
        "ISO_A3",
        "ISO3",
        "ISO2",
    ]

    # Find the first matching column
    for col in country_code_options:
        if col in countries_columns:
            return col

    # If no column found, raise an error
    raise click.UsageError(
        f"Could not find country code column in countries file. "
        f"Expected one of: {', '.join(country_code_options)}"
    )


def find_subdivision_code_column(con, countries_source, is_subquery=False):
    """
    Find the subdivision code column in a countries dataset.

    Args:
        con: DuckDB connection
        countries_source: Either a file path or a subquery
        is_subquery: Whether countries_source is a subquery (True) or file path (False)

    Returns:
        str or None: The name of the subdivision code column, or None if not found
    """
    # Build appropriate query based on source type
    if is_subquery:
        columns_query = f"SELECT * FROM {countries_source} LIMIT 0;"
    else:
        columns_query = f"SELECT * FROM '{countries_source}' LIMIT 0;"

    countries_columns = [col[0] for col in con.execute(columns_query).description]

    # Define possible subdivision code column names in priority order
    subdivision_code_options = [
        "admin:subdivision_code",
        "subdivision_code",
        "region",
        "state",
        "province",
    ]

    # Find the first matching column
    for col in subdivision_code_options:
        if col in countries_columns:
            return col

    # Subdivision is optional, return None if not found
    return None


def _handle_bbox_optimization(file_path, bbox_info, add_bbox_flag, file_label, verbose):
    """Handle bbox structure warning and optionally add bbox."""
    if bbox_info["status"] == "optimal":
        return bbox_info

    warn(f"\nWarning: {file_label} could benefit from bbox optimization:\n" + bbox_info["message"])

    if not add_bbox_flag:
        info(
            f"💡 Tip: Run this command with --add-bbox to automatically add bbox optimization to the {file_label.lower()}"
        )
        return bbox_info

    if not bbox_info["has_bbox_column"]:
        progress(f"Adding bbox column to {file_label.lower()}...")
        from geoparquet_io.core.common import add_bbox

        add_bbox(file_path, "bbox", verbose)
        success(f"✓ Added bbox column and metadata to {file_label.lower()}")
    elif not bbox_info["has_bbox_metadata"]:
        progress(f"Adding bbox metadata to {file_label.lower()}...")
        from geoparquet_io.core.add_bbox_metadata import add_bbox_metadata

        add_bbox_metadata(file_path, verbose)

    return check_bbox_structure(file_path, verbose)


def _build_select_clause(country_code_col, subdivision_code_col, using_default):
    """Build the SELECT clause for country and subdivision codes."""
    # Country code selection
    if country_code_col == "admin:country_code":
        country_select = f'b."{country_code_col}"'
    else:
        country_select = f'b."{country_code_col}" as "admin:country_code"'

    # Subdivision code selection
    if not subdivision_code_col:
        return country_select

    if using_default and subdivision_code_col == "region":
        subdivision_select = (
            ", CASE WHEN b.region LIKE '%-%' THEN split_part(b.region, '-', 2) "
            'ELSE b.region END as "admin:subdivision_code"'
        )
    elif subdivision_code_col == "admin:subdivision_code":
        subdivision_select = f', b."{subdivision_code_col}"'
    else:
        subdivision_select = f', b."{subdivision_code_col}" as "admin:subdivision_code"'

    return country_select + subdivision_select


def _build_spatial_join_query(
    input_url,
    countries_source,
    select_clause,
    input_geom_col,
    countries_geom_col,
    input_bbox_col,
    countries_bbox_col,
):
    """Build the spatial join query based on bbox availability."""
    if input_bbox_col and countries_bbox_col:
        return f"""
    SELECT
        a.*,
        {select_clause}
    FROM '{input_url}' a
    LEFT JOIN {countries_source} b
    ON (a.{input_bbox_col}.xmin <= b.{countries_bbox_col}.xmax AND
        a.{input_bbox_col}.xmax >= b.{countries_bbox_col}.xmin AND
        a.{input_bbox_col}.ymin <= b.{countries_bbox_col}.ymax AND
        a.{input_bbox_col}.ymax >= b.{countries_bbox_col}.ymin)
        AND ST_Intersects(b.{countries_geom_col}, a.{input_geom_col})
"""
    return f"""
    SELECT
        a.*,
        {select_clause}
    FROM '{input_url}' a
    LEFT JOIN {countries_source} b
    ON ST_Intersects(b.{countries_geom_col}, a.{input_geom_col})
"""


def _build_filter_table_sql(table_name, source_url, bbox_col, bounds):
    """Build SQL to create filtered countries table from bounds."""
    xmin, ymin, xmax, ymax = bounds
    if isinstance(xmin, str):  # placeholder values
        return f"""CREATE TEMP TABLE {table_name} AS
SELECT * FROM '{source_url}'
WHERE {bbox_col}.xmin <= {xmax}
  AND {bbox_col}.xmax >= {xmin}
  AND {bbox_col}.ymin <= {ymax}
  AND {bbox_col}.ymax >= {ymin};"""
    return f"""CREATE TEMP TABLE {table_name} AS
SELECT * FROM '{source_url}'
WHERE {bbox_col}.xmin <= {xmax:.6f}
  AND {bbox_col}.xmax >= {xmin:.6f}
  AND {bbox_col}.ymin <= {ymax:.6f}
  AND {bbox_col}.ymax >= {ymin:.6f};"""


def _print_dry_run_bounds_info(input_bbox_col, input_url, input_geom_col):
    """Print dry-run info for bounds calculation step."""
    info("-- Step 1: Calculate bounding box of input data to filter remote countries")
    if input_bbox_col:
        bounds_sql = f"SELECT MIN({input_bbox_col}.xmin) as xmin, ... FROM '{input_url}';"
    else:
        bounds_sql = f"SELECT MIN(ST_XMin({input_geom_col})) as xmin, ... FROM '{input_url}';"
    progress(bounds_sql)
    progress("")
    warn("-- Calculating actual bounds...")


def _get_bounds_for_filtering(input_parquet, input_geom_col, dry_run, verbose):
    """Get dataset bounds, handling dry-run mode."""
    bounds = get_dataset_bounds(input_parquet, input_geom_col, verbose=(verbose and not dry_run))

    if not bounds:
        if dry_run:
            warn("-- Note: Could not calculate actual bounds")
            return ("<xmin>", "<ymin>", "<xmax>", "<ymax>")
        raise click.ClickException("Could not calculate dataset bounds")

    if dry_run:
        success(f"-- Bounds calculated: {bounds}")
    elif verbose:
        debug(f"Input bbox: {bounds}")
    return bounds


def _create_filtered_countries_table(
    con, countries_table, default_countries_url, countries_bbox_col, bounds, dry_run, verbose
):
    """Create the filtered countries temporary table."""
    if dry_run:
        progress("")
        info("-- Step 2: Create filtered countries table")

    create_table_sql = _build_filter_table_sql(
        countries_table, default_countries_url, countries_bbox_col, bounds
    )

    if dry_run:
        progress(create_table_sql)
        progress("")
    else:
        if verbose:
            debug("Creating temporary table with filtered countries...")
        con.execute(create_table_sql)
        if verbose:
            count = con.execute(f"SELECT COUNT(*) FROM {countries_table}").fetchone()[0]
            debug(f"Loaded {count} countries overlapping with input data")


def _print_dry_run_header(
    input_url,
    countries_url,
    output_parquet,
    input_geom_col,
    countries_geom_col,
    input_bbox_col,
    countries_bbox_col,
):
    """Print dry-run mode header information."""
    warn("\n=== DRY RUN MODE - SQL Commands that would be executed ===\n")
    info(f"-- Input file: {input_url}")
    info(f"-- Countries file: {countries_url}")
    info(f"-- Output file: {output_parquet}")
    info(f"-- Geometry columns: {input_geom_col} (input), {countries_geom_col} (countries)")
    info(
        f"-- Bbox columns: {input_bbox_col or 'none'} (input), {countries_bbox_col or 'none'} (countries)\n"
    )


def _get_countries_config(countries_parquet, using_default, verbose):
    """Get countries URL, geometry column, and bbox column."""
    default_countries_url = (
        "s3://overturemaps-us-west-2/release/2025-10-22.0/theme=divisions/type=division_area/*"
    )

    if using_default:
        return default_countries_url, "geometry", "bbox"

    countries_url = safe_file_url(countries_parquet, verbose)
    countries_geom_col = find_primary_geometry_column(countries_parquet, verbose)
    countries_bbox_info = check_bbox_structure(countries_parquet, verbose)
    return countries_url, countries_geom_col, countries_bbox_info["bbox_column_name"]


def _determine_code_columns(
    con, countries_url, countries_source, countries_table, using_default, dry_run, verbose
):
    """Determine country and subdivision code columns."""
    if using_default:
        country_code_col = "country"
        subdivision_code_col = "region"
        if verbose and not dry_run:
            debug(f"Using country code column: {country_code_col} (default countries file)")
            debug(f"Using subdivision code column: {subdivision_code_col} (default countries file)")
        return country_code_col, subdivision_code_col

    if dry_run:
        return "admin:country_code", None

    country_code_col = find_country_code_column(con, countries_url, is_subquery=False)
    if verbose:
        debug(f"Using country code column: {country_code_col}")

    subdivision_code_col = find_subdivision_code_column(
        con, countries_source, is_subquery=(countries_source == countries_table)
    )
    if subdivision_code_col and verbose:
        debug(f"Using subdivision code column: {subdivision_code_col}")

    return country_code_col, subdivision_code_col


def _print_dry_run_query(
    query,
    output_parquet,
    compression,
    compression_level,
    using_default,
    input_bbox_col,
    countries_bbox_col,
):
    """Print the dry-run query output."""
    final_step = "3" if using_default else "1"
    info(f"-- Step {final_step}: Main spatial join query")

    if input_bbox_col and countries_bbox_col:
        info("-- Using bbox columns for optimized spatial join")
    else:
        info("-- Using full geometry intersection (no bbox optimization)")

    compression_str = (
        f"{compression}:{compression_level}"
        if compression in ["GZIP", "ZSTD", "BROTLI"]
        else compression
    )
    duckdb_compression = compression.lower() if compression != "UNCOMPRESSED" else "uncompressed"

    display_query = f"""COPY ({query.strip()})
TO '{output_parquet}'
(FORMAT PARQUET, COMPRESSION '{duckdb_compression}');"""
    progress(display_query)

    info(f"\n-- Note: Using {compression_str} compression")
    info("-- Original metadata would also be preserved in the output file")


def _print_results_summary(con, output_parquet):
    """Print the results summary after processing."""
    stats_query = f"""
    SELECT
        COUNT(*) as total_features,
        COUNT(CASE WHEN "admin:country_code" IS NOT NULL THEN 1 END) as features_with_country,
        COUNT(CASE WHEN "admin:subdivision_code" IS NOT NULL THEN 1 END) as features_with_subdivision,
        COUNT(DISTINCT "admin:country_code") as unique_countries,
        COUNT(DISTINCT "admin:subdivision_code") as unique_subdivisions
    FROM '{output_parquet}';
    """
    stats = con.execute(stats_query).fetchone()

    progress("\nResults:")
    progress(f"- Added country codes to {stats[1]:,} of {stats[0]:,} features")
    if stats[2] > 0:
        progress(f"- Added subdivision codes to {stats[2]:,} of {stats[0]:,} features")
    progress(f"- Found {stats[3]:,} unique countries")
    if stats[4] > 0:
        progress(f"- Found {stats[4]:,} unique subdivisions")
    success(f"\nSuccessfully wrote output to: {output_parquet}")


def _setup_default_countries(
    con,
    input_parquet,
    input_url,
    input_geom_col,
    input_bbox_col,
    default_countries_url,
    countries_bbox_col,
    countries_table,
    dry_run,
    verbose,
):
    """Setup filtered countries table for default Overture dataset."""
    if dry_run:
        _print_dry_run_bounds_info(input_bbox_col, input_url, input_geom_col)

    if verbose and not dry_run:
        debug("Calculating bounding box of input data to filter remote countries file...")

    bounds = _get_bounds_for_filtering(input_parquet, input_geom_col, dry_run, verbose)

    _create_filtered_countries_table(
        con, countries_table, default_countries_url, countries_bbox_col, bounds, dry_run, verbose
    )


def _prepare_bbox_columns(
    input_parquet, countries_parquet, using_default, add_bbox_flag, dry_run, verbose
):
    """Prepare and optionally optimize bbox columns for input and countries files.

    For GeoParquet 2.0 / parquet-geo files with native geometry types,
    skip bbox pre-filtering entirely as native geometry row group statistics
    are faster than manual bbox filtering.
    """
    # Check if input file has native geometry (2.0 / parquet-geo)
    input_bbox_advice = get_bbox_advice(input_parquet, "spatial_filtering", verbose)

    # For native geometry files, skip bbox pre-filtering
    if input_bbox_advice["skip_bbox_prefilter"]:
        if verbose:
            debug("Input has native geometry - skipping bbox pre-filter (native stats are faster)")
        return None, None

    # For 1.x files, use bbox optimization if available
    input_bbox_info = check_bbox_structure(input_parquet, verbose)
    input_bbox_col = input_bbox_info["bbox_column_name"]

    if using_default:
        countries_bbox_col = "bbox"
    else:
        countries_bbox_info = check_bbox_structure(countries_parquet, verbose)
        countries_bbox_col = countries_bbox_info["bbox_column_name"]

    if not dry_run:
        # Show warning and suggest options for 1.x files without bbox
        if input_bbox_advice["needs_warning"]:
            warn(f"\nWarning: {input_bbox_advice['message']}")
            if not add_bbox_flag:
                for suggestion in input_bbox_advice["suggestions"]:
                    info(f"💡 Tip: {suggestion}")

        # Handle bbox optimization if --add-bbox flag is used
        if add_bbox_flag and not input_bbox_info["has_bbox_column"]:
            input_bbox_info = _handle_bbox_optimization(
                input_parquet, input_bbox_info, add_bbox_flag, "Input file", verbose
            )
            input_bbox_col = input_bbox_info["bbox_column_name"]

        if not using_default:
            countries_bbox_info = check_bbox_structure(countries_parquet, verbose)
            countries_bbox_info = _handle_bbox_optimization(
                countries_parquet, countries_bbox_info, add_bbox_flag, "Countries file", verbose
            )
            countries_bbox_col = countries_bbox_info["bbox_column_name"]

    return input_bbox_col, countries_bbox_col


def _setup_countries_source(
    con,
    using_default,
    countries_url,
    input_parquet,
    input_url,
    input_geom_col,
    input_bbox_col,
    countries_bbox_col,
    dry_run,
    verbose,
):
    """Setup countries source - either filtered table or direct file reference."""
    countries_table = "filtered_countries"
    default_countries_url = (
        "s3://overturemaps-us-west-2/release/2025-10-22.0/theme=divisions/type=division_area/*"
    )

    if using_default:
        _setup_default_countries(
            con,
            input_parquet,
            input_url,
            input_geom_col,
            input_bbox_col,
            default_countries_url,
            countries_bbox_col,
            countries_table,
            dry_run,
            verbose,
        )
        return countries_table
    return f"'{countries_url}'"


def _create_duckdb_connection(using_default):
    """Create and configure DuckDB connection."""
    con = duckdb.connect()
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    if using_default:
        con.execute("SET s3_region='us-west-2';")
    return con


def _print_bbox_status(input_bbox_col, countries_bbox_col, verbose, dry_run):
    """Print bbox optimization status message."""
    if dry_run:
        return
    if input_bbox_col and countries_bbox_col and verbose:
        debug("Using bbox columns for initial filtering...")
    elif not (input_bbox_col and countries_bbox_col):
        progress("No bbox columns available, using full geometry intersection...")


def add_country_codes(
    input_parquet,
    countries_parquet,
    output_parquet,
    add_bbox_flag,
    dry_run,
    verbose,
    compression="ZSTD",
    compression_level=None,
    row_group_size_mb=None,
    row_group_rows=None,
):
    """Add country ISO codes to a GeoParquet file based on spatial intersection."""
    input_url = safe_file_url(input_parquet, verbose)
    using_default = countries_parquet is None

    countries_url, countries_geom_col, _ = _get_countries_config(
        countries_parquet, using_default, verbose
    )

    if using_default and not dry_run:
        info("\nNo countries file specified, using default from Overture Maps")
        info(
            "This will filter the remote file to only the area of your data, but may take longer than using a local file."
        )

    input_geom_col = find_primary_geometry_column(input_parquet, verbose)
    input_bbox_col, countries_bbox_col = _prepare_bbox_columns(
        input_parquet, countries_parquet, using_default, add_bbox_flag, dry_run, verbose
    )

    if dry_run:
        _print_dry_run_header(
            input_url,
            countries_url,
            output_parquet,
            input_geom_col,
            countries_geom_col,
            input_bbox_col,
            countries_bbox_col,
        )

    metadata = None if dry_run else get_parquet_metadata(input_parquet, verbose)[0]

    if not dry_run and verbose:
        debug(f"Using geometry columns: {input_geom_col} (input), {countries_geom_col} (countries)")

    con = _create_duckdb_connection(using_default)

    if not dry_run:
        total_count = con.execute(f"SELECT COUNT(*) FROM '{input_url}'").fetchone()[0]
        progress(f"Processing {total_count:,} input features...")

    countries_table = "filtered_countries"
    countries_source = _setup_countries_source(
        con,
        using_default,
        countries_url,
        input_parquet,
        input_url,
        input_geom_col,
        input_bbox_col,
        countries_bbox_col,
        dry_run,
        verbose,
    )

    country_code_col, subdivision_code_col = _determine_code_columns(
        con, countries_url, countries_source, countries_table, using_default, dry_run, verbose
    )

    select_clause = _build_select_clause(country_code_col, subdivision_code_col, using_default)
    _print_bbox_status(input_bbox_col, countries_bbox_col, verbose, dry_run)

    query = _build_spatial_join_query(
        input_url,
        countries_source,
        select_clause,
        input_geom_col,
        countries_geom_col,
        input_bbox_col,
        countries_bbox_col,
    )

    if dry_run:
        _print_dry_run_query(
            query,
            output_parquet,
            compression,
            compression_level,
            using_default,
            input_bbox_col,
            countries_bbox_col,
        )
        return

    if verbose:
        debug("Performing spatial join with country boundaries...")

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
    )

    _print_results_summary(con, output_parquet)


if __name__ == "__main__":
    add_country_codes()
