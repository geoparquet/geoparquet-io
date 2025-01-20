#!/usr/bin/env python3

import click
import duckdb
from geoparquet_tools.core.common import (
    safe_file_url, find_primary_geometry_column, get_parquet_metadata,
    update_metadata
)

def add_country_codes(input_parquet, countries_parquet, output_parquet, verbose):
    """Add country ISO codes to a GeoParquet file based on spatial intersection."""
    # Get safe URLs for both input files
    input_url = safe_file_url(input_parquet, verbose)
    countries_url = safe_file_url(countries_parquet, verbose)
    
    # Get metadata before processing
    metadata, _ = get_parquet_metadata(input_parquet, verbose)
    
    # Get geometry column name
    geometry_column = find_primary_geometry_column(input_parquet, verbose)
    if verbose:
        click.echo(f"Using geometry column: {geometry_column}")
    
    # Create DuckDB connection and load spatial extension
    con = duckdb.connect()
    con.execute("LOAD spatial;")
    
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
    
    # Spatial join query
    query = f"""
    COPY (
        SELECT 
            a.*,
            b."{country_code_col}" as "admin:country_code"
        FROM '{input_url}' a
        LEFT JOIN '{countries_url}' b
        ON ST_Intersects(a.{geometry_column}, b.geometry)
    )
    TO '{output_parquet}'
    (FORMAT PARQUET);
    """
    
    if verbose:
        click.echo("Performing spatial join with country boundaries...")
    
    con.execute(query)
    
    if verbose:
        click.echo(f"Successfully wrote output to: {output_parquet}")
    
    # Update the output file with the original metadata
    if metadata:
        update_metadata(output_parquet, metadata)
        if verbose:
            click.echo("Updated output file with original metadata")

if __name__ == "__main__":
    add_country_codes() 