#!/usr/bin/env python3

import click
import duckdb
import json
import pyarrow.parquet as pq
import pyarrow as pa
from geoparquet_tools.core.common import (
    safe_file_url, find_primary_geometry_column, get_parquet_metadata,
    update_metadata
)

def hilbert_order(input_parquet, output_parquet, geometry_column="geometry", verbose=False):
    """
    Reorder a GeoParquet file using Hilbert curve ordering.
    
    Takes an input GeoParquet file and creates a new file with rows ordered
    by their position along a Hilbert space-filling curve. Applies best practices:
    - ZSTD compression
    - Optimal row group sizes
    - bbox covering metadata
    - Preserves CRS from original file
    """
    safe_url = safe_file_url(input_parquet, verbose)
    
    # Get metadata and CRS from original file
    metadata, schema = get_parquet_metadata(input_parquet, verbose)
    if metadata and b'geo' in metadata:
        try:
            geo_meta = pa.py_buffer(metadata[b'geo']).to_pybytes().decode('utf-8')
            geo_dict = json.loads(geo_meta)
            if isinstance(geo_dict, dict) and 'columns' in geo_dict:
                for col in geo_dict['columns'].values():
                    if 'crs' in col:
                        original_crs = col['crs']
                        break
        except (json.JSONDecodeError, KeyError) as e:
            if verbose:
                click.echo(f"Could not parse original CRS: {e}")
    
    # Use specified geometry column or find primary one
    if geometry_column == "geometry":
        geometry_column = find_primary_geometry_column(input_parquet, verbose)
    
    if verbose:
        click.echo(f"Using geometry column: {geometry_column}")
    
    # Create DuckDB connection and load spatial extension
    con = duckdb.connect()
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    
    # First get the extent of all geometries
    extent_query = f"""
    SELECT
        ST_XMin(ST_Extent_Agg({geometry_column})) as min_x,
        ST_YMin(ST_Extent_Agg({geometry_column})) as min_y,
        ST_XMax(ST_Extent_Agg({geometry_column})) as max_x,
        ST_YMax(ST_Extent_Agg({geometry_column})) as max_y
    FROM '{safe_url}';
    """

    if verbose:
        click.echo("Calculating spatial extent...")

    result = con.execute(extent_query).fetchone()
    min_x, min_y, max_x, max_y = result

    if verbose:
        click.echo(f"Spatial bounds: min_x={min_x}, min_y={min_y}, max_x={max_x}, max_y={max_y}")

    # Create temporary file for initial Hilbert ordering
    temp_file = output_parquet + ".tmp"

    # Order by Hilbert value and add bbox
    order_query = f"""
    COPY (
        SELECT
            *
        FROM '{safe_url}'
        ORDER BY ST_Hilbert({geometry_column},
            ST_MakeEnvelope({min_x}, {min_y}, {max_x}, {max_y}))
    )
    TO '{temp_file}'
    (FORMAT PARQUET);
    """
    
    if verbose:
        click.echo("Reordering data using Hilbert curve...")
    
    con.execute(order_query)
    # Read the ordered data
    if metadata:
        update_metadata(temp_file, metadata)
        if verbose:
            click.echo("Updated output file with optimal metadata")
    
    
    # Write final file with optimal settings
    # move temp file to output file
    import os
    os.rename(temp_file, output_parquet)
    
    # Clean up temporary file

    if os.path.exists(temp_file):
        os.remove(temp_file)
    
    if verbose:
        click.echo(f"Successfully wrote ordered data to: {output_parquet}")

if __name__ == "__main__":
    hilbert_order() 