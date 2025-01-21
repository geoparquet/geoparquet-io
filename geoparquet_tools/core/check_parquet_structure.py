#!/usr/bin/env python3

import click
import pyarrow.parquet as pq
import fsspec
import json
from geoparquet_tools.core.common import (
    safe_file_url, format_size, get_parquet_metadata, parse_geo_metadata,
    find_primary_geometry_column, check_bbox_structure
)

def get_row_group_stats(parquet_file):
    """
    Get basic row group statistics from a parquet file.
    
    Returns:
        dict: Statistics including:
            - num_groups: Number of row groups
            - total_rows: Total number of rows
            - avg_rows_per_group: Average rows per group
            - total_size: Total file size in bytes
            - avg_group_size: Average group size in bytes
    """
    with fsspec.open(safe_file_url(parquet_file), 'rb') as f:
        metadata = pq.ParquetFile(f).metadata
        
        total_rows = metadata.num_rows
        num_groups = metadata.num_row_groups
        avg_rows_per_group = total_rows / num_groups if num_groups > 0 else 0
        total_size = sum(metadata.row_group(i).total_byte_size for i in range(num_groups))
        avg_group_size = total_size / num_groups if num_groups > 0 else 0
        
        return {
            "num_groups": num_groups,
            "total_rows": total_rows,
            "avg_rows_per_group": avg_rows_per_group,
            "total_size": total_size,
            "avg_group_size": avg_group_size
        }

def assess_row_group_size(avg_group_size_bytes, total_size_bytes):
    """
    Assess if row group size is optimal.
    
    Returns:
        tuple: (status, message, color) where status is one of:
            - "optimal"
            - "suboptimal"
            - "poor"
    """
    avg_group_size_mb = avg_group_size_bytes / (1024 * 1024)
    total_size_mb = total_size_bytes / (1024 * 1024)
    
    if total_size_mb < 128:
        return "optimal", "Row group size is appropriate for small file", "green"
    
    if 128 <= avg_group_size_mb <= 256:
        return "optimal", "Row group size is optimal (128-256 MB)", "green"
    elif 64 <= avg_group_size_mb < 128 or 256 < avg_group_size_mb <= 512:
        return "suboptimal", "Row group size is suboptimal. Recommended size is 128-256 MB", "yellow"
    else:
        return "poor", "Row group size is outside recommended range. Target 128-256 MB for best performance", "red"

def assess_row_count(avg_rows):
    """
    Assess if average row count per group is optimal.
    
    Returns:
        tuple: (status, message, color) where status is one of:
            - "optimal"
            - "suboptimal"
            - "poor"
    """
    if avg_rows < 2000:
        return "poor", "Row count per group is very low. Target 50,000-200,000 rows per group", "red"
    elif avg_rows > 1000000:
        return "poor", "Row count per group is very high. Target 50,000-200,000 rows per group", "red"
    elif 50000 <= avg_rows <= 200000:
        return "optimal", "Row count per group is optimal", "green"
    else:
        return "suboptimal", "Row count per group is outside recommended range (50,000-200,000)", "yellow"

def get_compression_info(parquet_file, column_name=None):
    """
    Get compression information for specified column(s).
    
    Returns:
        dict: Mapping of column names to their compression algorithms
    """
    with fsspec.open(safe_file_url(parquet_file), 'rb') as f:
        metadata = pq.ParquetFile(f).metadata
        
        compression_info = {}
        for i in range(metadata.num_columns):
            col = metadata.schema.column(i)
            if column_name is None or col.name == column_name:
                compression = metadata.row_group(0).column(i).compression
                compression_info[col.name] = compression
                
        return compression_info

def check_row_groups(parquet_file, verbose=False):
    """Check row group optimization and print results."""
    stats = get_row_group_stats(parquet_file)
    
    click.echo("\nRow Group Analysis:")
    click.echo(f"Number of row groups: {stats['num_groups']}")
    
    size_status, size_message, size_color = assess_row_group_size(
        stats['avg_group_size'], stats['total_size']
    )
    click.echo(click.style(f"Average group size: {format_size(stats['avg_group_size'])}", fg=size_color))
    click.echo(click.style(size_message, fg=size_color))
    
    row_status, row_message, row_color = assess_row_count(stats['avg_rows_per_group'])
    click.echo(click.style(f"Average rows per group: {stats['avg_rows_per_group']:,.0f}", fg=row_color))
    click.echo(click.style(row_message, fg=row_color))
    
    click.echo(f"\nTotal file size: {format_size(stats['total_size'])}")
    
    if size_status != "optimal" or row_status != "optimal":
        click.echo("\nRow Group Guidelines:")
        click.echo("- Optimal size: 128-256 MB per row group")
        click.echo("- Optimal rows: 50,000-200,000 rows per group")
        click.echo("- Small files (<128 MB): single row group is fine")

def check_metadata_and_bbox(parquet_file, verbose=False):
    """Check GeoParquet metadata version and bbox structure."""
    metadata, _ = get_parquet_metadata(parquet_file)
    geo_meta = parse_geo_metadata(metadata, False)
    
    if not geo_meta:
        click.echo(click.style("\n❌ No GeoParquet metadata found", fg="red"))
        return
        
    version = geo_meta.get("version", "0.0.0")
    click.echo("\nGeoParquet Metadata:")
    version_color = "green" if version >= "1.1.0" else "yellow"
    version_prefix = "✓" if version >= "1.1.0" else "⚠️"
    version_suffix = "" if version >= "1.1.0" else " (upgrade to 1.1.0+ recommended)"
    
    click.echo(
        click.style(
            f"{version_prefix} Version {version}{version_suffix}",
            fg=version_color
        )
    )
    
    bbox_info = check_bbox_structure(parquet_file, verbose)
    if bbox_info["has_bbox_column"]:
        if bbox_info["has_bbox_metadata"]:
            click.echo(click.style(f"✓ Found bbox column '{bbox_info['bbox_column_name']}' with proper metadata covering", fg="green"))
        else:
            click.echo(click.style(
                f"⚠️  Found bbox column '{bbox_info['bbox_column_name']}' but missing bbox covering metadata "
                "(add to metadata to help inform clients)", 
                fg="yellow"
            ))
    else:
        click.echo(click.style("❌ No bbox column found (recommended for better performance)", fg="red"))

def check_compression(parquet_file, verbose=False):
    """Check compression settings for geometry column."""
    primary_col = find_primary_geometry_column(parquet_file, verbose)
    if not primary_col:
        click.echo(click.style("\n❌ No geometry column found", fg="red"))
        return
        
    compression = get_compression_info(parquet_file, primary_col)[primary_col]
    click.echo("\nCompression Analysis:")
    if compression == 'ZSTD':
        click.echo(click.style(f"✓ ZSTD compression on geometry column '{primary_col}'", fg="green"))
    else:
        click.echo(click.style(f"⚠️  {compression} compression on geometry column '{primary_col}' (ZSTD recommended)", fg="yellow"))

def check_all(parquet_file, verbose=False):
    """Run all structure checks."""
    check_row_groups(parquet_file, verbose)
    check_metadata_and_bbox(parquet_file, verbose)
    check_compression(parquet_file, verbose)

if __name__ == "__main__":
    check_all() 