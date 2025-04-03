#!/usr/bin/env python3

import click
import duckdb
from geoparquet_tools.core.common import (
    safe_file_url, find_primary_geometry_column, get_parquet_metadata,
    update_metadata, check_bbox_structure
)

def add_country_codes(input_parquet, countries_parquet, output_parquet, verbose):
    """Add country ISO codes to a GeoParquet file based on spatial intersection."""
    # Get safe URLs for both input files
    input_url = safe_file_url(input_parquet, verbose)
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
    
    # Check bbox structure for countries file
    countries_bbox_info = check_bbox_structure(countries_parquet, verbose)
    countries_bbox_col = countries_bbox_info["bbox_column_name"]
    if countries_bbox_info["status"] != "optimal":
        click.echo(click.style(
            "\nWarning: Countries file could benefit from bbox optimization:\n" + 
            countries_bbox_info["message"],
            fg="yellow"
        ))
    
    # Get geometry column names
    input_geom_col = find_primary_geometry_column(input_parquet, verbose)
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
    
    # Check available columns in countries file
    columns_query = f"SELECT * FROM '{countries_url}' LIMIT 0;"
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
            LEFT JOIN '{countries_url}' b
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
            LEFT JOIN '{countries_url}' b
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