#!/usr/bin/env python3

import click
import duckdb
from geoparquet_tools.core.common import (
    safe_file_url, find_primary_geometry_column, get_parquet_metadata,
    update_metadata, check_bbox_structure
)

def add_country_codes(input_parquet, countries_parquet, output_parquet, add_bbox_flag, verbose):
    """Add country ISO codes to a GeoParquet file based on spatial intersection."""
    # Get safe URLs for both input files
    input_url = safe_file_url(input_parquet, verbose)

    # Use default countries file if not provided
    default_countries_url = "https://data.source.coop/cholmes/admin-boundaries/countries.parquet"
    using_default = countries_parquet is None

    if using_default:
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
    
    # Get metadata before processing
    metadata, _ = get_parquet_metadata(input_parquet, verbose)
    
    # Check bbox structure for input file
    input_bbox_info = check_bbox_structure(input_parquet, verbose)
    input_bbox_col = input_bbox_info["bbox_column_name"]
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
    countries_bbox_col = None
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
    else:
        # Default countries file has bbox column
        countries_bbox_col = "bbox"
    
    # Get geometry column names
    input_geom_col = find_primary_geometry_column(input_parquet, verbose)

    # For remote countries file, we need to determine geometry column differently
    if using_default:
        # The default file uses 'geometry' as the column name
        countries_geom_col = 'geometry'
    else:
        countries_geom_col = find_primary_geometry_column(countries_parquet, verbose)

    if verbose:
        click.echo(f"Using geometry columns: {input_geom_col} (input), {countries_geom_col} (countries)")
    
    # Create DuckDB connection and load spatial extension
    con = duckdb.connect()
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    # Get total input count
    total_count = con.execute(f"SELECT COUNT(*) FROM '{input_url}'").fetchone()[0]
    click.echo(f"Processing {total_count:,} input features...")

    # If using default countries file, filter it to the input bbox
    if using_default:
        if verbose:
            click.echo("Calculating bounding box of input data to filter remote countries file...")

        # Get the overall bbox of the input data
        bbox_query = f"""
        SELECT
            MIN(ST_XMin({input_geom_col})) as xmin,
            MIN(ST_YMin({input_geom_col})) as ymin,
            MAX(ST_XMax({input_geom_col})) as xmax,
            MAX(ST_YMax({input_geom_col})) as ymax
        FROM '{input_url}'
        """
        bbox = con.execute(bbox_query).fetchone()
        xmin, ymin, xmax, ymax = bbox

        if verbose:
            click.echo(f"Input bbox: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")
            click.echo("Filtering remote countries file to overlapping countries only...")

        # Create a filtered view of the countries file
        # We'll use a subquery that filters based on bbox intersection
        countries_url = f"""(
            SELECT * FROM '{default_countries_url}'
            WHERE ST_Intersects(
                {countries_geom_col},
                ST_GeomFromText('POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))')
            )
        )"""

        if verbose:
            # Count how many countries overlap
            country_count = con.execute(f"SELECT COUNT(*) FROM {countries_url}").fetchone()[0]
            click.echo(f"Found {country_count} countries overlapping with input data")

    # Check available columns in countries file
    columns_query = f"SELECT * FROM {countries_url} LIMIT 0;"
    countries_columns = [col[0] for col in con.execute(columns_query).description]
    
    # Find the country code column
    country_code_options = [
        "admin:country_code",
        "country_code",
        "country",
        "ISO_A2",
        "ISO_A3",
        "ISO3",
        "ISO2"
    ]
    
    country_code_col = None
    for col in country_code_options:
        if col in countries_columns:
            country_code_col = col
            break
    
    if not country_code_col:
        raise click.UsageError(
            f"Could not find country code column in countries file. "
            f"Expected one of: {', '.join(country_code_options)}"
        )
    
    if verbose:
        click.echo(f"Using country code column: {country_code_col}")
    
    # Build the countries source - either quoted URL or subquery
    if using_default:
        countries_source = countries_url  # Already a subquery, don't quote
    else:
        countries_source = f"'{countries_url}'"  # Quote the file path/URL

    # Build spatial join query based on bbox availability
    if input_bbox_col and countries_bbox_col:
        if verbose:
            click.echo("Using bbox columns for initial filtering...")
        query = f"""
        COPY (
            SELECT
                a.*,
                b."{country_code_col}" as "admin:country_code"
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
        (FORMAT PARQUET);
        """
    else:
        click.echo("No bbox columns available, using full geometry intersection...")
        query = f"""
        COPY (
            SELECT
                a.*,
                b."{country_code_col}" as "admin:country_code"
            FROM '{input_url}' a
            LEFT JOIN {countries_source} b
            ON ST_Intersects(b.{countries_geom_col}, a.{input_geom_col})
        )
        TO '{output_parquet}'
        (FORMAT PARQUET);
        """
    
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