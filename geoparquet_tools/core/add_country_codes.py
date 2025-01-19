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

def check_bbox_structure(parquet_file, verbose=False):
    """
    Check if the parquet file has bbox covering metadata and proper bbox column structure.
    Returns the name of the bbox column to use.
    Raises error if bbox column structure is invalid.
    """
    with fsspec.open(parquet_file, 'rb') as f:
        pf = pq.ParquetFile(f)
        metadata = pf.schema_arrow.metadata
        schema = pf.schema_arrow

    # Check for bbox covering metadata
    bbox_column_name = 'bbox'  # default name
    if metadata and b'geo' in metadata:
        try:
            geo_meta = json.loads(metadata[b'geo'].decode('utf-8'))
            if verbose:
                click.echo("\nParsed geo metadata:")
                click.echo(json.dumps(geo_meta, indent=2))
            
            if isinstance(geo_meta, dict) and 'columns' in geo_meta:
                columns = geo_meta['columns']
                if isinstance(columns, dict):
                    # Check each column for covering information
                    for col_name, col_info in columns.items():
                        if isinstance(col_info, dict) and 'covering' in col_info:
                            covering = col_info['covering']
                            if isinstance(covering, dict) and 'bbox' in covering:
                                bbox_info = covering['bbox']
                                if isinstance(bbox_info, dict) and all(k in bbox_info for k in ['xmin', 'ymin', 'xmax', 'ymax']):
                                    # Get the first part of the reference as the column name
                                    bbox_ref = bbox_info['xmin']
                                    if isinstance(bbox_ref, list) and len(bbox_ref) > 0:
                                        bbox_column_name = bbox_ref[0]
                                        break
                elif isinstance(columns, list):
                    for col in columns:
                        if isinstance(col, dict) and col.get('bbox_covering', False):
                            bbox_column_name = col.get('name', 'bbox')
                            break
            elif isinstance(geo_meta, list):
                for col in geo_meta:
                    if isinstance(col, dict) and col.get('bbox_covering', False):
                        bbox_column_name = col.get('name', 'bbox')
                        break
        except json.JSONDecodeError:
            if verbose:
                click.echo("Failed to parse geo metadata as JSON")

    if bbox_column_name == 'bbox' and verbose:
        click.echo("Warning: No bbox covering metadata found in the file. Attempting to find a 'bbox' column name.")

    # Check for bbox column structure
    bbox_field = None
    for field in schema:
        if field.name == bbox_column_name:
            bbox_field = field
            break

    if not bbox_field:
        raise click.BadParameter(f"No '{bbox_column_name}' column found in the file: {parquet_file}")

    required_fields = {'xmin', 'ymin', 'xmax', 'ymax'}
    if bbox_field.type.num_fields < 4 or not all(f.name in required_fields for f in bbox_field.type):
        raise click.BadParameter(f"Invalid bbox column structure in file: {parquet_file}. "
                               f"Must be a struct with xmin, ymin, xmax, ymax fields.")

    return bbox_column_name

def add_country_codes(input_parquet, countries_parquet, output_parquet, verbose):
    """
    Add country ISO codes to a GeoParquet file based on spatial intersection.
    
    Takes an input GeoParquet file and a countries GeoParquet file (with 'country' field),
    adds country codes to each row, and writes to a new GeoParquet file.
    """
    # Get safe URLs for both input files
    input_url = safe_file_url(input_parquet, verbose)
    countries_url = safe_file_url(countries_parquet, verbose)
    
    # Check bbox structure in both files and get column names
    input_bbox_col = check_bbox_structure(input_url, verbose)
    countries_bbox_col = check_bbox_structure(countries_url, verbose)
    
    if verbose:
        click.echo(f"Using bbox column '{input_bbox_col}' from input file")
        click.echo(f"Using bbox column '{countries_bbox_col}' from countries file")
    
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
        ON (input.{input_bbox_col}.xmin <= countries.{countries_bbox_col}.xmax AND 
            input.{input_bbox_col}.xmax >= countries.{countries_bbox_col}.xmin AND 
            input.{input_bbox_col}.ymin <= countries.{countries_bbox_col}.ymax AND 
            input.{input_bbox_col}.ymax >= countries.{countries_bbox_col}.ymin)  -- Fast bbox intersection test
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