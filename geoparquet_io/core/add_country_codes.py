#!/usr/bin/env python3

import click
import duckdb

from geoparquet_io.core.common import (
    check_bbox_structure,
    find_primary_geometry_column,
    get_dataset_bounds,
    get_parquet_metadata,
    safe_file_url,
    write_parquet_with_metadata,
)


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

    click.echo(
        click.style(
            f"\nWarning: {file_label} could benefit from bbox optimization:\n"
            + bbox_info["message"],
            fg="yellow",
        )
    )

    if not add_bbox_flag:
        click.echo(
            click.style(
                f"💡 Tip: Run this command with --add-bbox to automatically add bbox optimization to the {file_label.lower()}",
                fg="cyan",
            )
        )
        return bbox_info

    if not bbox_info["has_bbox_column"]:
        click.echo(f"Adding bbox column to {file_label.lower()}...")
        from geoparquet_io.core.common import add_bbox

        add_bbox(file_path, "bbox", verbose)
        click.echo(
            click.style(f"✓ Added bbox column and metadata to {file_label.lower()}", fg="green")
        )
    elif not bbox_info["has_bbox_metadata"]:
        click.echo(f"Adding bbox metadata to {file_label.lower()}...")
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
    # Show dry-run info for bounds calculation
    if dry_run:
        click.echo(
            click.style(
                "-- Step 1: Calculate bounding box of input data to filter remote countries",
                fg="cyan",
            )
        )
        if input_bbox_col:
            bounds_sql = f"SELECT MIN({input_bbox_col}.xmin) as xmin, ... FROM '{input_url}';"
        else:
            bounds_sql = f"SELECT MIN(ST_XMin({input_geom_col})) as xmin, ... FROM '{input_url}';"
        click.echo(bounds_sql)
        click.echo()
        click.echo(click.style("-- Calculating actual bounds...", fg="yellow"))

    if verbose and not dry_run:
        click.echo("Calculating bounding box of input data to filter remote countries file...")

    bounds = get_dataset_bounds(input_parquet, input_geom_col, verbose=(verbose and not dry_run))

    if not bounds:
        if dry_run:
            bounds = ("<xmin>", "<ymin>", "<xmax>", "<ymax>")
            click.echo(click.style("-- Note: Could not calculate actual bounds", fg="yellow"))
        else:
            raise click.ClickException("Could not calculate dataset bounds")
    else:
        if dry_run:
            click.echo(click.style(f"-- Bounds calculated: {bounds}", fg="green"))
        elif verbose:
            click.echo(f"Input bbox: {bounds}")

    if dry_run:
        click.echo()
        click.echo(click.style("-- Step 2: Create filtered countries table", fg="cyan"))

    create_table_sql = _build_filter_table_sql(
        countries_table, default_countries_url, countries_bbox_col, bounds
    )

    if dry_run:
        click.echo(create_table_sql)
        click.echo()
    else:
        if verbose:
            click.echo("Creating temporary table with filtered countries...")
        con.execute(create_table_sql)
        if verbose:
            count = con.execute(f"SELECT COUNT(*) FROM {countries_table}").fetchone()[0]
            click.echo(f"Loaded {count} countries overlapping with input data")


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
    # Get safe URLs for both input files
    input_url = safe_file_url(input_parquet, verbose)

    # Use default countries file if not provided
    default_countries_url = (
        "s3://overturemaps-us-west-2/release/2025-10-22.0/theme=divisions/type=division_area/*"
    )
    using_default = countries_parquet is None

    if using_default:
        if not dry_run:
            click.echo(
                click.style(
                    "\nNo countries file specified, using default from Overture Maps", fg="cyan"
                )
            )
            click.echo(
                click.style(
                    "This will filter the remote file to only the area of your data, but may take longer than using a local file.",
                    fg="cyan",
                )
            )
        countries_url = default_countries_url
    else:
        countries_url = safe_file_url(countries_parquet, verbose)

    # Get geometry column names
    input_geom_col = find_primary_geometry_column(input_parquet, verbose)

    # For countries file geometry column
    if using_default:
        countries_geom_col = "geometry"
    else:
        countries_geom_col = find_primary_geometry_column(countries_parquet, verbose)

    # Check bbox columns
    input_bbox_info = check_bbox_structure(input_parquet, verbose)
    input_bbox_col = input_bbox_info["bbox_column_name"]

    if using_default:
        countries_bbox_col = "bbox"  # Default countries file has bbox column
    else:
        countries_bbox_info = check_bbox_structure(countries_parquet, verbose)
        countries_bbox_col = countries_bbox_info["bbox_column_name"]

    # Start dry-run mode output if needed
    if dry_run:
        click.echo(
            click.style(
                "\n=== DRY RUN MODE - SQL Commands that would be executed ===\n",
                fg="yellow",
                bold=True,
            )
        )
        click.echo(click.style(f"-- Input file: {input_url}", fg="cyan"))
        click.echo(click.style(f"-- Countries file: {countries_url}", fg="cyan"))
        click.echo(click.style(f"-- Output file: {output_parquet}", fg="cyan"))
        click.echo(
            click.style(
                f"-- Geometry columns: {input_geom_col} (input), {countries_geom_col} (countries)",
                fg="cyan",
            )
        )
        click.echo(
            click.style(
                f"-- Bbox columns: {input_bbox_col or 'none'} (input), {countries_bbox_col or 'none'} (countries)\n",
                fg="cyan",
            )
        )

    # Get metadata before processing (skip in dry-run)
    metadata = None
    if not dry_run:
        metadata, _ = get_parquet_metadata(input_parquet, verbose)

        # Check and optionally fix bbox structure for input file
        input_bbox_info = _handle_bbox_optimization(
            input_parquet, input_bbox_info, add_bbox_flag, "Input file", verbose
        )
        input_bbox_col = input_bbox_info["bbox_column_name"]

        # Check and optionally fix bbox structure for countries file (only if not using default)
        if not using_default:
            countries_bbox_info = check_bbox_structure(countries_parquet, verbose)
            countries_bbox_info = _handle_bbox_optimization(
                countries_parquet, countries_bbox_info, add_bbox_flag, "Countries file", verbose
            )
            countries_bbox_col = countries_bbox_info["bbox_column_name"]

        if verbose:
            click.echo(
                f"Using geometry columns: {input_geom_col} (input), {countries_geom_col} (countries)"
            )

    # Create DuckDB connection and load spatial extension
    con = duckdb.connect()
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    # Configure S3 settings if using default Overture dataset
    if using_default:
        con.execute("SET s3_region='us-west-2';")

    # Get total input count (skip in dry-run)
    if not dry_run:
        total_count = con.execute(f"SELECT COUNT(*) FROM '{input_url}'").fetchone()[0]
        click.echo(f"Processing {total_count:,} input features...")

    # Handle filtering for default countries file
    countries_table = "filtered_countries"
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
        countries_source = countries_table
    else:
        # For custom countries file, just reference the file directly
        countries_source = f"'{countries_url}'"

    # Determine the country code column
    if using_default:
        # We know the default countries file uses 'country' column
        country_code_col = "country"
        if verbose and not dry_run:
            click.echo(f"Using country code column: {country_code_col} (default countries file)")
    else:
        if dry_run:
            # For dry-run with custom file, assume typical column
            country_code_col = "admin:country_code"
        else:
            # For actual execution, find the appropriate column
            country_code_col = find_country_code_column(con, countries_url, is_subquery=False)
            if verbose:
                click.echo(f"Using country code column: {country_code_col}")

    # Determine the subdivision code column
    subdivision_code_col = None
    if using_default:
        subdivision_code_col = "region"
        if verbose and not dry_run:
            click.echo(
                f"Using subdivision code column: {subdivision_code_col} (default countries file)"
            )
    elif not dry_run:
        subdivision_code_col = find_subdivision_code_column(
            con, countries_source, is_subquery=(countries_source == countries_table)
        )
        if subdivision_code_col and verbose:
            click.echo(f"Using subdivision code column: {subdivision_code_col}")

    # Build select clause using helper
    select_clause = _build_select_clause(country_code_col, subdivision_code_col, using_default)

    # Build spatial join query using helper
    if input_bbox_col and countries_bbox_col:
        if verbose and not dry_run:
            click.echo("Using bbox columns for initial filtering...")
    elif not dry_run:
        click.echo("No bbox columns available, using full geometry intersection...")

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
        # In dry-run mode, just show the query
        final_step = "3" if using_default else "1"
        click.echo(click.style(f"-- Step {final_step}: Main spatial join query", fg="cyan"))
        if input_bbox_col and countries_bbox_col:
            click.echo(click.style("-- Using bbox columns for optimized spatial join", fg="cyan"))
        else:
            click.echo(
                click.style("-- Using full geometry intersection (no bbox optimization)", fg="cyan")
            )

        # Show the query with COPY wrapper for display
        if compression in ["GZIP", "ZSTD", "BROTLI"]:
            compression_str = f"{compression}:{compression_level}"
        else:
            compression_str = compression

        # Use lowercase for DuckDB format
        duckdb_compression = (
            compression.lower() if compression != "UNCOMPRESSED" else "uncompressed"
        )
        display_query = f"""COPY ({query.strip()})
TO '{output_parquet}'
(FORMAT PARQUET, COMPRESSION '{duckdb_compression}');"""
        click.echo(display_query)

        click.echo(click.style(f"\n-- Note: Using {compression_str} compression", fg="cyan"))
        click.echo(
            click.style(
                "-- Original metadata would also be preserved in the output file", fg="cyan"
            )
        )
        return

    # Execute the query using the common write method
    if verbose:
        click.echo("Performing spatial join with country boundaries...")

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

    # Get statistics about the results
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
    total_features = stats[0]
    features_with_country = stats[1]
    features_with_subdivision = stats[2]
    unique_countries = stats[3]
    unique_subdivisions = stats[4]

    click.echo("\nResults:")
    click.echo(f"- Added country codes to {features_with_country:,} of {total_features:,} features")
    if features_with_subdivision > 0:
        click.echo(
            f"- Added subdivision codes to {features_with_subdivision:,} of {total_features:,} features"
        )
    click.echo(f"- Found {unique_countries:,} unique countries")
    if unique_subdivisions > 0:
        click.echo(f"- Found {unique_subdivisions:,} unique subdivisions")

    click.echo(f"\nSuccessfully wrote output to: {output_parquet}")


if __name__ == "__main__":
    add_country_codes()
