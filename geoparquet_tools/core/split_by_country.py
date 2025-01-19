#!/usr/bin/env python3

import click
import duckdb
import pyarrow.parquet as pq
import json
import fsspec
import urllib.parse
import os

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

def check_country_code_column(parquet_file):
    """Check if admin:country_code column exists and is populated."""
    with fsspec.open(parquet_file, 'rb') as f:
        parquet = pq.ParquetFile(f)
        schema = parquet.schema
        
        # Check if column exists
        if 'admin:country_code' not in schema.names:
            raise click.UsageError(
                "Column 'admin:country_code' not found in the Parquet file. "
                "Please add country codes first using the add_country_codes.py script."
            )
        
        # Check if column has values
        table = parquet.read(['admin:country_code'])
        if table.column('admin:country_code').null_count == table.num_rows:
            raise click.UsageError(
                "Column 'admin:country_code' exists but contains only NULL values. "
                "Please populate country codes using the add_country_codes.py script."
            )

def check_crs(parquet_file, verbose=False):
    """Check if CRS is WGS84 or null, warn if not."""
    with fsspec.open(parquet_file, 'rb') as f:
        metadata = pq.ParquetFile(f).schema_arrow.metadata
        
    if metadata and b'geo' in metadata:
        try:
            geo_meta = json.loads(metadata[b'geo'].decode('utf-8'))
            
            # Check CRS in both metadata formats
            if isinstance(geo_meta, dict):
                for col_name, col_meta in geo_meta.get("columns", {}).items():
                    crs = col_meta.get("crs")
                    if crs and not _is_wgs84(crs):
                        click.echo(click.style(
                            "Warning: Input file uses a CRS other than WGS84. "
                            "Results may be incorrect.", 
                            fg="yellow"
                        ))
                        return
            elif isinstance(geo_meta, list):
                for col in geo_meta:
                    if isinstance(col, dict):
                        crs = col.get("crs")
                        if crs and not _is_wgs84(crs):
                            click.echo(click.style(
                                "Warning: Input file uses a CRS other than WGS84. "
                                "Results may be incorrect.", 
                                fg="yellow"
                            ))
                            return
            
        except json.JSONDecodeError:
            if verbose:
                click.echo("Failed to parse geo metadata")

def _is_wgs84(crs):
    """Check if CRS is WGS84 or equivalent."""
    if not crs:
        return True
    
    # Common WGS84 identifiers
    wgs84_identifiers = [
        "4326",
        "EPSG:4326",
        "WGS84",
        "WGS 84",
        "urn:ogc:def:crs:EPSG::4326",
        "urn:ogc:def:crs:OGC:1.3:CRS84"
    ]
    
    if isinstance(crs, str):
        return any(id.lower() in crs.lower() for id in wgs84_identifiers)
    elif isinstance(crs, dict):
        # Check PROJJSON format
        return (
            crs.get("type", "").lower() == "geographiccrs" and
            "wgs" in str(crs).lower() and "84" in str(crs)
        )
    return False

@click.command()
@click.argument("input_parquet")
@click.argument("output_folder")
@click.option("--hive", is_flag=True, help="Use Hive-style partitioning in output folder structure.")
@click.option("--verbose", is_flag=True, help="Print additional information.")
@click.option("--overwrite", is_flag=True, help="Overwrite existing country files.")
def split_by_country(input_parquet, output_folder, hive, verbose, overwrite):
    """
    Split a GeoParquet file into separate files by country code.
    
    Requires an input GeoParquet file with 'admin:country_code' column
    and an output folder path. Creates one file per country,
    optionally using Hive-style partitioning.
    """
    input_url = safe_file_url(input_parquet, verbose)
    
    # Verify admin:country_code column exists and is populated
    if verbose:
        click.echo("Checking country code column...")
    check_country_code_column(input_url)
    
    # Check CRS
    check_crs(input_url, verbose)
    
    # Create output directory
    os.makedirs(output_folder, exist_ok=True)
    if verbose:
        click.echo(f"Created output directory: {output_folder}")
    
    # Create DuckDB connection
    con = duckdb.connect()
    con.execute("LOAD spatial;")
    
    # Get unique country codes
    if verbose:
        click.echo("Finding unique country codes...")
    
    result = con.execute("""
        SELECT DISTINCT "admin:country_code"
        FROM '{input_url}'
        WHERE "admin:country_code" IS NOT NULL
        ORDER BY "admin:country_code" DESC
    """.format(input_url=input_url))
    
    countries = result.fetchall()
    
    if verbose:
        click.echo(f"Found {len(countries)} unique countries")
    
    # Process each country
    for country in countries:
        country_code = country[0]
        
        # Determine output path
        if hive:
            write_folder = os.path.join(output_folder, f'country_code={country_code}')
            os.makedirs(write_folder, exist_ok=True)
        else:
            write_folder = output_folder
            
        output_filename = os.path.join(write_folder, f'{country_code}.parquet')
        
        # Skip if file exists and not overwriting
        if os.path.exists(output_filename) and not overwrite:
            if verbose:
                click.echo(f"Output file for country {country_code} already exists, skipping...")
            continue
        
        if verbose:
            click.echo(f"Processing country {country_code}...")
        
        # Extract and write country data
        query = f"""
        COPY (
            SELECT *
            FROM '{input_url}'
            WHERE "admin:country_code" = '{country_code}'
        )
        TO '{output_filename}'
        (FORMAT PARQUET);
        """
        
        con.execute(query)
        
        if verbose:
            click.echo(f"Wrote {output_filename}")
    
    click.echo(f"Successfully split file into {len(countries)} country files")

if __name__ == "__main__":
    split_by_country() 