#!/usr/bin/env python3

import click
import duckdb
import pyarrow.parquet as pq
from geoparquet_tools.core.common import (
    safe_file_url, find_primary_geometry_column, get_parquet_metadata,
    update_metadata
)

def hilbert_order(input_parquet, output_parquet, geometry_column, verbose):
    """
    Reorder a GeoParquet file using Hilbert curve ordering.
    
    Takes an input GeoParquet file and creates a new file with rows ordered
    by their position along a Hilbert space-filling curve.
    """
    safe_url = safe_file_url(input_parquet, verbose)
    
    # Get metadata before reordering
    metadata, _ = get_parquet_metadata(input_parquet, verbose)
    
    # Use specified geometry column or find primary one
    if geometry_column == "geometry":
        geometry_column = find_primary_geometry_column(input_parquet, verbose)
    
    if verbose:
        click.echo(f"Using geometry column: {geometry_column}")
    
    # Create DuckDB connection and load spatial extension
    con = duckdb.connect()
    con.execute("LOAD spatial;")
    
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
    
    # Update the output file with the original metadata
    if metadata:
        update_metadata(output_parquet, metadata)
        if verbose:
            click.echo("Updated output file with original metadata")

if __name__ == "__main__":
    hilbert_order() 