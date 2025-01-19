#!/usr/bin/env python3

import click
import duckdb
import json
import pyarrow.parquet as pq
import fsspec
import urllib.parse
import os

def check_spatial_order(parquet_file, random_sample_size, limit_rows, verbose):
    """
    Check approximate spatial ordering of a GeoParquet file by comparing
    the average distance of consecutive rows (in on-disk order) vs. random pairs.
    """
    con = duckdb.connect()
    con.execute("LOAD spatial;")
    
    # Handle both local and remote files
    if parquet_file.startswith(('http://', 'https://')):
        # URL encode the path component of the URL (preserve the domain)
        parsed = urllib.parse.urlparse(parquet_file)
        encoded_path = urllib.parse.quote(parsed.path)
        safe_url = parsed._replace(path=encoded_path).geturl()
        if verbose:
            click.echo(f"Reading remote file: {safe_url}")
    else:
        if not os.path.exists(parquet_file):
            raise click.BadParameter(f"Local file not found: {parquet_file}")
        safe_url = parquet_file

    # Find geometry column and projection type from metadata
    geometry_col, is_geographic, geo_metadata = find_primary_geometry_column(safe_url, verbose)
    
    if verbose and geo_metadata:
        click.echo("Full GeoParquet metadata:")
        click.echo(json.dumps(geo_metadata, indent=2))
    
    if not geometry_col:
        click.echo("No primary_column found in GeoParquet metadata, defaulting to 'geometry'.")
        geometry_col = "geometry"
    else:
        click.echo(f"Found primary geometry column: {geometry_col}")
        click.echo(f"Coordinate system type: {'geographic' if is_geographic else 'projected'}")

    # Modify query to use appropriate distance function
    distance_func = "ST_Distance_Sphere" if is_geographic else "ST_Distance"
    
    query = f"""
    WITH
    -- A. Read up to {limit_rows} rows from the Parquet file in approximate on-disk order
    all_rows AS (
        SELECT
            row_number() OVER () AS rid,
            {geometry_col} AS geom
        FROM '{safe_url}'
        LIMIT {limit_rows}
    ),

    -- B. Consecutive pairs using LEAD() on rid
    consecutive_pairs AS (
        SELECT
            {distance_func}(
                ST_Centroid(geom),
                ST_Centroid(LEAD(geom) OVER (ORDER BY rid))
            ) AS dist
        FROM all_rows
    ),
    avg_consecutive AS (
        SELECT AVG(dist) AS avg_dist
        FROM consecutive_pairs
        WHERE dist IS NOT NULL
    ),

    -- C. Random pairs: sample two subsets of N rows each, cross join
    sample1 AS (
        SELECT geom
        FROM all_rows
        USING SAMPLE reservoir({random_sample_size})
    ),
    sample2 AS (
        SELECT geom
        FROM all_rows
        USING SAMPLE reservoir({random_sample_size})
    ),
    random_pairs AS (
        SELECT
            {distance_func}(
                ST_Centroid(s1.geom),
                ST_Centroid(s2.geom)
            ) AS dist
        FROM sample1 s1
        CROSS JOIN sample2 s2
    ),
    avg_random AS (
        SELECT AVG(dist) AS avg_dist
        FROM random_pairs
    )

    SELECT
        (SELECT avg_dist FROM avg_consecutive) AS avg_consecutive_dist,
        (SELECT avg_dist FROM avg_random)      AS avg_random_dist,
        CASE
          WHEN (SELECT avg_dist FROM avg_random) = 0
               OR (SELECT avg_dist FROM avg_random) IS NULL
          THEN NULL
          ELSE (SELECT avg_dist FROM avg_consecutive) /
               (SELECT avg_dist FROM avg_random)
        END AS ratio
    ;
    """

    # 3) Run the query and fetch the results
    result = con.execute(query).fetchone()
    avg_consecutive_dist = result[0]
    avg_random_dist      = result[1]
    ratio               = result[2]

    click.echo(f"Avg consecutive distance: {avg_consecutive_dist}")
    click.echo(f"Avg random distance:      {avg_random_dist}")
    click.echo(f"Ratio (consecutive / random): {ratio}")

    if ratio is not None and ratio < 0.5:
        click.echo("=> Data seems strongly spatially clustered.")
    elif ratio is not None:
        click.echo("=> Data might not be strongly clustered (or is partially clustered).")

def find_primary_geometry_column(parquet_file, verbose=False):
    """
    Tries to parse the 'geo' JSON metadata from the Parquet file
    to discover 'primary_column' and projection. Returns tuple of 
    (column_name, is_geographic, full_metadata).
    """
    # Use fsspec to handle both local and remote files
    with fsspec.open(parquet_file, 'rb') as f:
        parquet_file = pq.ParquetFile(f)
        metadata = parquet_file.metadata
    
    if verbose:
        click.echo("\nParquet schema:")
        click.echo(metadata.schema)
        click.echo("\nParquet metadata key-value pairs:")
        for key in metadata.metadata:
            click.echo(f"{key}: {metadata.metadata[key]}")
    
    # Look for geo metadata
    if metadata.metadata:
        geo_metadata = metadata.metadata.get(b'geo')
        if geo_metadata:
            try:
                meta = json.loads(geo_metadata.decode('utf-8'))
                if verbose:
                    click.echo("\nParsed geo metadata:")
                    click.echo(json.dumps(meta, indent=2))
                
                # Get geometry column name and check if geographic
                column_name = None
                is_geographic = False
                
                # Handle both dictionary and list metadata formats
                if isinstance(meta, dict):
                    column_name = meta.get("primary_column")
                    # Check if the CRS is geographic
                    crs = meta.get("columns", {}).get(column_name, {}).get("crs", {})
                    is_geographic = is_crs_geographic(crs)
                elif isinstance(meta, list) and len(meta) > 0:
                    # For list format, look for the first column with primary=true
                    for column in meta:
                        if isinstance(column, dict) and column.get("primary", False):
                            column_name = column.get("name")
                            is_geographic = is_crs_geographic(column.get("crs", {}))
                            break
                
                return column_name, is_geographic, meta
                
            except json.JSONDecodeError:
                if verbose:
                    click.echo("Failed to parse geo metadata as JSON")
    
    # Default to geometry column and geographic coordinates if no metadata found
    return None, True, None

def is_crs_geographic(crs):
    """
    Determine if a CRS is geographic based on its definition.
    """
    if not crs:
        return True  # Default to geographic if no CRS info
    
    # If we have a string, it's likely a PROJJSON or WKT
    if isinstance(crs, str):
        return "GEOGCS" in crs or "GeographicCRS" in crs
    
    # If we have a dict (PROJJSON format)
    if isinstance(crs, dict):
        # Check for common indicators of geographic CRS
        type_str = crs.get("type", "").lower()
        return "geographic" in type_str or "geogcs" in type_str
    
    return True  # Default to geographic if unknown format

if __name__ == "__main__":
    check_spatial_order()
