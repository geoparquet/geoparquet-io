#!/usr/bin/env python3

import click
import pyarrow.parquet as pq
import json
import fsspec
from geoparquet_tools.core.common import safe_file_url
from geoparquet_tools.core.partition_common import partition_by_column, preview_partition


def check_country_code_column(parquet_file, column_name="admin:country_code"):
    """Check if the specified column exists and is populated."""
    with pq.ParquetFile(parquet_file) as parquet:
        schema = parquet.schema

        # Check if column exists
        if column_name not in schema.names:
            raise click.UsageError(
                f"Column '{column_name}' not found in the Parquet file. "
                "Please add country codes first using the add_country_codes command."
            )

        # Check if column has values
        table = parquet.read([column_name])
        if table.column(column_name).null_count == table.num_rows:
            raise click.UsageError(
                f"Column '{column_name}' exists but contains only NULL values. "
                "Please populate country codes using the add_country_codes command."
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

def split_by_country(input_parquet, output_folder, column="admin:country_code", hive=False, verbose=False, overwrite=False, preview=False, preview_limit=15):
    """
    Split a GeoParquet file into separate files by country code.

    Requires an input GeoParquet file with a country code column
    and an output folder path. Creates one file per country,
    optionally using Hive-style partitioning.

    Args:
        input_parquet: Input GeoParquet file
        output_folder: Output directory
        column: Column name to partition by (default: "admin:country_code")
        hive: Use Hive-style partitioning
        verbose: Print detailed information
        overwrite: Overwrite existing files
        preview: Show preview of partitions without creating files
        preview_limit: Maximum number of partitions to show in preview (default: 15)
    """
    input_url = safe_file_url(input_parquet, verbose)

    # Verify column exists and is populated
    if verbose:
        click.echo(f"Checking column '{column}'...")
    check_country_code_column(input_url, column)

    # Check CRS (only if not in preview mode)
    if not preview:
        check_crs(input_url, verbose)

    # If preview mode, show preview and exit
    if preview:
        preview_partition(
            input_parquet=input_parquet,
            column_name=column,
            column_prefix_length=None,
            limit=preview_limit,
            verbose=verbose
        )
        return

    # Use common partition function
    num_partitions = partition_by_column(
        input_parquet=input_parquet,
        output_folder=output_folder,
        column_name=column,
        column_prefix_length=None,
        hive=hive,
        overwrite=overwrite,
        verbose=verbose
    )

    click.echo(f"Successfully split file into {num_partitions} country file(s)")

if __name__ == "__main__":
    split_by_country() 