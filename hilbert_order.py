#!/usr/bin/env python3

import click
import duckdb
import pyarrow.parquet as pq
import os
import urllib.parse

@click.command()
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option("--geometry-column", default="geometry", show_default=True,
              help="Name of the geometry column to use for ordering.")
@click.option("--verbose", is_flag=True, help="Print additional information.")
def hilbert_order(input_parquet, output_parquet, geometry_column, verbose):
    """
    Reorder a GeoParquet file using Hilbert curve ordering.
    
    Takes an input GeoParquet file and creates a new file with rows ordered
    by their position along a Hilbert space-filling curve.
    """
    # Read the original GeoParquet metadata to get CRS
    pq_file = pq.ParquetFile(input_parquet)
    metadata = pq_file.schema_arrow.metadata
    if metadata:
        geo_metadata = {k.decode('utf-8'): v.decode('utf-8') 
                       for k, v in metadata.items() 
                       if k.decode('utf-8').startswith('geo')}
        if verbose:
            click.echo(f"Original GeoParquet metadata: {geo_metadata}")
    
    con = duckdb.connect()
    con.execute("LOAD spatial;")
    
    # Handle both local and remote files
    if input_parquet.startswith(('http://', 'https://')):
        parsed = urllib.parse.urlparse(input_parquet)
        encoded_path = urllib.parse.quote(parsed.path)
        safe_url = parsed._replace(path=encoded_path).geturl()
        if verbose:
            click.echo(f"Reading remote file: {safe_url}")
    else:
        if not os.path.exists(input_parquet):
            raise click.BadParameter(f"Local file not found: {input_parquet}")
        safe_url = input_parquet

    # First get the extent of all geometries
    extent_query = f"""
    SELECT ST_Extent(ST_Extent_Agg({geometry_column}))::BOX_2D AS bounds
    FROM '{safe_url}';
    """
    
    if verbose:
        click.echo("Calculating spatial extent...")
    
    bounds = con.execute(extent_query).fetchone()[0]
    
    if verbose:
        click.echo(f"Spatial bounds: {bounds}")
    
    # Now order by Hilbert value and write to new file
    order_query = f"""
    COPY (
        SELECT *
        FROM '{safe_url}'
        ORDER BY ST_Hilbert({geometry_column}, 
            ST_Extent(ST_MakeEnvelope({bounds['min_x']}, {bounds['min_y']}, 
                                 {bounds['max_x']}, {bounds['max_y']})))
    )
    TO '{output_parquet}'
    (FORMAT PARQUET);
    """
    
    if verbose:
        click.echo("Reordering data using Hilbert curve...")
    
    con.execute(order_query)
    
    if verbose:
        click.echo(f"Successfully wrote ordered data to: {output_parquet}")

    # Now update the output file with the original CRS information
    if metadata:
        # Read the output file
        table = pq.read_table(output_parquet)
        
        # Combine existing metadata with geo metadata
        existing_metadata = table.schema.metadata or {}
        new_metadata = {
            k.encode('utf-8'): v.encode('utf-8')
            for k, v in {**{k.decode('utf-8'): v.decode('utf-8') 
                           for k, v in existing_metadata.items()},
                        **geo_metadata}.items()
        }
        
        # Write back with updated metadata
        new_table = table.replace_schema_metadata(new_metadata)
        pq.write_table(new_table, output_parquet)
        
        if verbose:
            click.echo(f"Updated output file with original CRS metadata")

if __name__ == "__main__":
    hilbert_order() 