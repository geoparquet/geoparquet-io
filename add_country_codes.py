#!/usr/bin/env python3

import click
import duckdb
import pyarrow.parquet as pq
import json
import fsspec
import urllib.parse
import os
import datetime

def find_primary_geometry_column(parquet_file, verbose=False):
    """
    Parse the 'geo' metadata from Parquet file to find primary geometry column.
    Returns the column name or 'geometry' as default.
    """
    with fsspec.open(parquet_file, 'rb') as f:
        metadata = pq.ParquetFile(f).schema_arrow.metadata
        
    if metadata and b'geo' in metadata:
        try:
            geo_meta = json.loads(metadata[b'geo'].decode('utf-8'))
            if verbose:
                click.echo("\nParsed geo metadata:")
                click.echo(json.dumps(geo_meta, indent=2))
            
            if isinstance(geo_meta, dict):
                return geo_meta.get("primary_column", "geometry")
            elif isinstance(geo_meta, list):
                for col in geo_meta:
                    if isinstance(col, dict) and col.get("primary", False):
                        return col.get("name", "geometry")
        except json.JSONDecodeError:
            if verbose:
                click.echo("Failed to parse geo metadata as JSON")
    
    return "geometry"

def safe_file_url(file_path, verbose=False):
    """Handle both local and remote files, returning safe URL."""
    if file_path.startswith(('http://', 'https://')):
        parsed = urllib.parse.urlparse(file_path)
        encoded_path = urllib.parse.quote(parsed.path)
        safe_url = parsed._replace(path=encoded_path).geturl()
        if verbose:
            click.echo(f"Reading remote file: {safe_url}")
    else:
        if not os.path.exists(file_path):
            raise click.BadParameter(f"Local file not found: {file_path}")
        safe_url = file_path
    return safe_url

@click.command()
@click.argument("input_parquet")
@click.argument("countries_parquet")
@click.argument("output_parquet")
@click.option("--verbose", is_flag=True, help="Print additional information.")
def add_country_codes(input_parquet, countries_parquet, output_parquet, verbose):
    """
    Add country ISO codes to a GeoParquet file based on spatial intersection.
    
    Takes an input GeoParquet file and a countries GeoParquet file (with 'country' field),
    adds country codes to each row, and writes to a new GeoParquet file.
    """
    # Get safe URLs for both input files
    input_url = safe_file_url(input_parquet, verbose)
    countries_url = safe_file_url(countries_parquet, verbose)
    
    # Find geometry column names
    input_geom_col = find_primary_geometry_column(input_url, verbose)
    countries_geom_col = find_primary_geometry_column(countries_url, verbose)
    
    if verbose:
        click.echo(f"Using geometry column '{input_geom_col}' from input file")
        click.echo(f"Using geometry column '{countries_geom_col}' from countries file")
    
    # Create DuckDB connection and load spatial extension
    con = duckdb.connect()
    con.execute("LOAD spatial;")
    
    if verbose:
        click.echo(f"[{datetime.datetime.now()}] Creating temporary tables...")
    
    # Create temporary tables
    con.execute(f"CREATE TABLE input AS SELECT * FROM '{input_url}'")
    con.execute(f"CREATE TABLE countries AS SELECT * FROM '{countries_url}'")
    
    if verbose:
        click.echo(f"[{datetime.datetime.now()}] Executing spatial join with bbox pre-filtering...")
    
    # Perform the spatial join with bbox pre-filtering
    query = f"""
    COPY (
        SELECT 
            input.*,
            countries.country as "admin:country_code"
        FROM input
        LEFT JOIN countries
        ON (input.bbox.xmin <= countries.bbox.xmax AND 
            input.bbox.xmax >= countries.bbox.xmin AND 
            input.bbox.ymin <= countries.bbox.ymax AND 
            input.bbox.ymax >= countries.bbox.ymin)  -- Fast bbox intersection test
            AND ST_Intersects(  -- More expensive precise check only on bbox matches
                countries.{countries_geom_col},
                input.{input_geom_col}
            )
    )
    TO '{output_parquet}'
    (FORMAT PARQUET);
    """
    
    if verbose:
        click.echo(f"[{datetime.datetime.now()}] Query:")
        click.echo(query)
    
    # Execute the query
    con.execute(query)
    
    if verbose:
        click.echo(f"[{datetime.datetime.now()}] Writing output file...")
    
    # Copy over the original GeoParquet metadata
    with fsspec.open(input_url, 'rb') as f:
        input_metadata = pq.ParquetFile(f).schema_arrow.metadata
    
    if input_metadata:
        # Read the output file
        table = pq.read_table(output_parquet)
        
        # Update metadata
        existing_metadata = table.schema.metadata or {}
        new_metadata = {
            k: v for k, v in existing_metadata.items()
        }
        # Add original geo metadata
        for k, v in input_metadata.items():
            if k.decode('utf-8').startswith('geo'):
                new_metadata[k] = v
        
        # Write back with updated metadata
        new_table = table.replace_schema_metadata(new_metadata)
        pq.write_table(new_table, output_parquet)
        
        if verbose:
            click.echo(f"Updated output file with original metadata")
    
    click.echo(f"Successfully wrote output to: {output_parquet}")

if __name__ == "__main__":
    add_country_codes() 