#!/usr/bin/env python3

import click
import duckdb
from geoparquet_tools.core.common import (
    safe_file_url, find_primary_geometry_column, get_parquet_metadata,
    update_metadata, check_bbox_structure, get_dataset_bounds
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
        "ISO2"
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


def add_country_codes(input_parquet, countries_parquet, output_parquet, add_bbox_flag, dry_run, verbose):
    """Add country ISO codes to a GeoParquet file based on spatial intersection."""
    # Get safe URLs for both input files
    input_url = safe_file_url(input_parquet, verbose)

    # Use default countries file if not provided
    default_countries_url = "https://data.source.coop/cholmes/admin-boundaries/countries.parquet"
    using_default = countries_parquet is None

    if using_default:
        if not dry_run:
            click.echo(click.style(
                "\nNo countries file specified, using default from source.coop",
                fg="cyan"
            ))
            click.echo(click.style(
                "This will filter the remote file to only the area of your data, but may take longer than using a local file.",
                fg="cyan"
            ))
        countries_url = default_countries_url
    else:
        countries_url = safe_file_url(countries_parquet, verbose)

    # Get geometry column names
    input_geom_col = find_primary_geometry_column(input_parquet, verbose)

    # For countries file geometry column
    if using_default:
        countries_geom_col = 'geometry'
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
        click.echo(click.style("\n=== DRY RUN MODE - SQL Commands that would be executed ===\n", fg="yellow", bold=True))
        click.echo(click.style(f"-- Input file: {input_url}", fg="cyan"))
        click.echo(click.style(f"-- Countries file: {countries_url}", fg="cyan"))
        click.echo(click.style(f"-- Output file: {output_parquet}", fg="cyan"))
        click.echo(click.style(f"-- Geometry columns: {input_geom_col} (input), {countries_geom_col} (countries)", fg="cyan"))
        click.echo(click.style(f"-- Bbox columns: {input_bbox_col or 'none'} (input), {countries_bbox_col or 'none'} (countries)\n", fg="cyan"))

    # Get metadata before processing (skip in dry-run)
    metadata = None
    if not dry_run:
        metadata, _ = get_parquet_metadata(input_parquet, verbose)

        # Check bbox structure and provide warnings
        if input_bbox_info["status"] != "optimal":
            click.echo(click.style(
                "\nWarning: Input file could benefit from bbox optimization:\n" +
                input_bbox_info["message"],
                fg="yellow"
            ))
            if add_bbox_flag:
                # Fix the bbox issue based on what's missing
                if not input_bbox_info["has_bbox_column"]:
                    click.echo("Adding bbox column to input file...")
                    from geoparquet_tools.core.common import add_bbox
                    add_bbox(input_parquet, 'bbox', verbose)
                    click.echo(click.style("✓ Added bbox column and metadata to input file", fg="green"))
                    # Re-check after adding bbox
                    input_bbox_info = check_bbox_structure(input_parquet, verbose)
                    input_bbox_col = input_bbox_info["bbox_column_name"]
                elif not input_bbox_info["has_bbox_metadata"]:
                    click.echo("Adding bbox metadata to input file...")
                    from geoparquet_tools.core.add_bbox_metadata import add_bbox_metadata
                    add_bbox_metadata(input_parquet, verbose)
                    # Re-check after adding metadata
                    input_bbox_info = check_bbox_structure(input_parquet, verbose)
            else:
                click.echo(click.style(
                    "💡 Tip: Run this command with --add-bbox to automatically add bbox optimization to the input file",
                    fg="cyan"
                ))

        # Check bbox structure for countries file (only if not using default)
        if not using_default:
            countries_bbox_info = check_bbox_structure(countries_parquet, verbose)
            countries_bbox_col = countries_bbox_info["bbox_column_name"]
            if countries_bbox_info["status"] != "optimal":
                click.echo(click.style(
                    "\nWarning: Countries file could benefit from bbox optimization:\n" +
                    countries_bbox_info["message"],
                    fg="yellow"
                ))
                if add_bbox_flag:
                    # Fix the bbox issue based on what's missing
                    if not countries_bbox_info["has_bbox_column"]:
                        click.echo("Adding bbox column to countries file...")
                        from geoparquet_tools.core.common import add_bbox
                        add_bbox(countries_parquet, 'bbox', verbose)
                        click.echo(click.style("✓ Added bbox column and metadata to countries file", fg="green"))
                        # Re-check after adding bbox
                        countries_bbox_info = check_bbox_structure(countries_parquet, verbose)
                        countries_bbox_col = countries_bbox_info["bbox_column_name"]
                    elif not countries_bbox_info["has_bbox_metadata"]:
                        click.echo("Adding bbox metadata to countries file...")
                        from geoparquet_tools.core.add_bbox_metadata import add_bbox_metadata
                        add_bbox_metadata(countries_parquet, verbose)
                        # Re-check after adding metadata
                        countries_bbox_info = check_bbox_structure(countries_parquet, verbose)
                else:
                    click.echo(click.style(
                        "💡 Tip: Run this command with --add-bbox to automatically add bbox optimization to the countries file",
                        fg="cyan"
                    ))

        if verbose:
            click.echo(f"Using geometry columns: {input_geom_col} (input), {countries_geom_col} (countries)")

    # Create DuckDB connection and load spatial extension
    con = duckdb.connect()
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    # Get total input count (skip in dry-run)
    if not dry_run:
        total_count = con.execute(f"SELECT COUNT(*) FROM '{input_url}'").fetchone()[0]
        click.echo(f"Processing {total_count:,} input features...")

    # Handle filtering for default countries file
    countries_table = "filtered_countries"
    if using_default:
        # Need to calculate bounds and create filtered table
        if dry_run:
            # Show the SQL for calculating bounds
            click.echo(click.style("-- Step 1: Calculate bounding box of input data to filter remote countries", fg="cyan"))

            # Build the bounds SQL based on whether bbox column exists
            if input_bbox_col:
                bounds_sql = f"""SELECT
    MIN({input_bbox_col}.xmin) as xmin,
    MIN({input_bbox_col}.ymin) as ymin,
    MAX({input_bbox_col}.xmax) as xmax,
    MAX({input_bbox_col}.ymax) as ymax
FROM '{input_url}';"""
            else:
                bounds_sql = f"""SELECT
    MIN(ST_XMin({input_geom_col})) as xmin,
    MIN(ST_YMin({input_geom_col})) as ymin,
    MAX(ST_XMax({input_geom_col})) as xmax,
    MAX(ST_YMax({input_geom_col})) as ymax
FROM '{input_url}';"""

            click.echo(bounds_sql)
            click.echo()

            # Calculate actual bounds for the dry-run display
            click.echo(click.style("-- Calculating actual bounds for use in subsequent queries...", fg="yellow"))

        # Calculate bounds (for both dry-run and actual execution)
        if verbose and not dry_run:
            click.echo("Calculating bounding box of input data to filter remote countries file...")

        bounds = get_dataset_bounds(input_parquet, input_geom_col, verbose=(verbose and not dry_run))

        if not bounds:
            if dry_run:
                # Use placeholder values for dry-run
                xmin, ymin, xmax, ymax = "<xmin>", "<ymin>", "<xmax>", "<ymax>"
                click.echo(click.style("-- Note: Could not calculate actual bounds, showing placeholder values", fg="yellow"))
            else:
                raise click.ClickException("Could not calculate dataset bounds")
        else:
            xmin, ymin, xmax, ymax = bounds
            if dry_run:
                click.echo(click.style(f"-- Bounds calculated: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})", fg="green"))
            elif verbose:
                click.echo(f"Input bbox: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")

        if dry_run:
            click.echo()
            click.echo(click.style("-- Step 2: Create temporary table with filtered countries using bbox filtering", fg="cyan"))
            click.echo(click.style("-- Note: source.coop countries file has bbox column for fast filtering", fg="cyan"))

        # Build the CREATE TABLE query using bbox filtering
        # We know the default countries file has a bbox column, so use it for filtering
        if isinstance(xmin, str):  # Using placeholder values
            create_table_sql = f"""CREATE TEMP TABLE {countries_table} AS
SELECT * FROM '{default_countries_url}'
WHERE {countries_bbox_col}.xmin <= {xmax}
  AND {countries_bbox_col}.xmax >= {xmin}
  AND {countries_bbox_col}.ymin <= {ymax}
  AND {countries_bbox_col}.ymax >= {ymin};"""
        else:  # Using actual numeric bounds
            create_table_sql = f"""CREATE TEMP TABLE {countries_table} AS
SELECT * FROM '{default_countries_url}'
WHERE {countries_bbox_col}.xmin <= {xmax:.6f}
  AND {countries_bbox_col}.xmax >= {xmin:.6f}
  AND {countries_bbox_col}.ymin <= {ymax:.6f}
  AND {countries_bbox_col}.ymax >= {ymin:.6f};"""

        if dry_run:
            click.echo(create_table_sql)
            click.echo()
        else:
            # Execute the CREATE TABLE query
            if verbose:
                click.echo("Creating temporary table with filtered countries...")

            con.execute(create_table_sql)

            if verbose:
                # Count how many countries were loaded
                country_count = con.execute(f"SELECT COUNT(*) FROM {countries_table}").fetchone()[0]
                click.echo(f"Loaded {country_count} countries overlapping with input data")

        # For the main query, use the temp table
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

    # Build the SELECT clause based on whether we need to rename the column
    if country_code_col == "admin:country_code":
        # No need to rename if it's already the right name
        select_clause = f'b."{country_code_col}"'
    else:
        # Rename to standard admin:country_code
        select_clause = f'b."{country_code_col}" as "admin:country_code"'

    # Build spatial join query based on bbox availability
    if input_bbox_col and countries_bbox_col:
        if verbose and not dry_run:
            click.echo("Using bbox columns for initial filtering...")

        query = f"""COPY (
    SELECT
        a.*,
        {select_clause}
    FROM '{input_url}' a
    LEFT JOIN {countries_source} b
    ON (a.{input_bbox_col}.xmin <= b.{countries_bbox_col}.xmax AND
        a.{input_bbox_col}.xmax >= b.{countries_bbox_col}.xmin AND
        a.{input_bbox_col}.ymin <= b.{countries_bbox_col}.ymax AND
        a.{input_bbox_col}.ymax >= b.{countries_bbox_col}.ymin)  -- Fast bbox intersection test
        AND ST_Intersects(  -- More expensive precise check only on bbox matches
            b.{countries_geom_col},
            a.{input_geom_col}
        )
)
TO '{output_parquet}'
(FORMAT PARQUET);"""
    else:
        if not dry_run:
            click.echo("No bbox columns available, using full geometry intersection...")

        query = f"""COPY (
    SELECT
        a.*,
        {select_clause}
    FROM '{input_url}' a
    LEFT JOIN {countries_source} b
    ON ST_Intersects(b.{countries_geom_col}, a.{input_geom_col})
)
TO '{output_parquet}'
(FORMAT PARQUET);"""

    if dry_run:
        # In dry-run mode, just show the query
        final_step = "3" if using_default else "1"
        click.echo(click.style(f"-- Step {final_step}: Main spatial join query", fg="cyan"))
        if input_bbox_col and countries_bbox_col:
            click.echo(click.style("-- Using bbox columns for optimized spatial join", fg="cyan"))
        else:
            click.echo(click.style("-- Using full geometry intersection (no bbox optimization)", fg="cyan"))

        # Clean up the query formatting for display
        display_query = query.strip()
        click.echo(display_query)

        click.echo(click.style("\n-- Note: Original metadata would also be preserved in the output file", fg="cyan"))
        return

    # Execute the query
    if verbose:
        click.echo("Performing spatial join with country boundaries...")

    con.execute(query)

    # Get statistics about the results
    stats_query = f"""
    SELECT
        COUNT(*) as total_features,
        COUNT(CASE WHEN "admin:country_code" IS NOT NULL THEN 1 END) as features_with_country,
        COUNT(DISTINCT "admin:country_code") as unique_countries
    FROM '{output_parquet}'
    WHERE "admin:country_code" IS NOT NULL;
    """

    stats = con.execute(stats_query).fetchone()
    total_features = stats[0]
    features_with_country = stats[1]
    unique_countries = stats[2]

    click.echo(f"\nResults:")
    click.echo(f"- Added country codes to {features_with_country:,} of {total_features:,} features")
    click.echo(f"- Found {unique_countries:,} unique countries")

    # Update the output file with the original metadata
    if metadata:
        update_metadata(output_parquet, metadata)
        if verbose:
            click.echo("Updated output file with original metadata")

    click.echo(f"\nSuccessfully wrote output to: {output_parquet}")

if __name__ == "__main__":
    add_country_codes()