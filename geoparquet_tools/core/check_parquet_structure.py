#!/usr/bin/env python3

import click
import pyarrow.parquet as pq
import json
import fsspec
import urllib.parse
import os

def format_size(size_bytes):
    """Convert bytes to human readable string"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"

def get_row_group_size_assessment(avg_group_size_mb, total_size_mb):
    """Assess row group size and return (color, message)"""
    if total_size_mb < 128:
        return "green", "✓ Row group size is appropriate for small file"
    
    if 128 <= avg_group_size_mb <= 256:
        return "green", "✓ Row group size is optimal (128-256 MB)"
    elif 64 <= avg_group_size_mb < 128 or 256 < avg_group_size_mb <= 512:
        return "yellow", "⚠️  Row group size is suboptimal. Recommended size is 128-256 MB"
    else:
        return "red", "❌ Row group size is outside recommended range. Target 128-256 MB for best performance"

def get_row_count_assessment(avg_rows):
    """Assess average row count and return (color, message)"""
    if avg_rows < 2000:
        return "red", "❌ Row count per group is very low. Target 50,000-200,000 rows per group"
    elif avg_rows > 1000000:
        return "red", "❌ Row count per group is very high. Target 50,000-200,000 rows per group"
    elif 50000 <= avg_rows <= 200000:
        return "green", "✓ Row count per group is optimal"
    else:
        return "yellow", "⚠️  Row count per group is outside recommended range (50,000-200,000)"

def check_parquet_structure(parquet_file, verbose):
    """
    Analyze key GeoParquet file characteristics and provide recommendations.
    """
    # Handle both local and remote files
    if parquet_file.startswith(('http://', 'https://')):
        parsed = urllib.parse.urlparse(parquet_file)
        encoded_path = urllib.parse.quote(parsed.path)
        safe_url = parsed._replace(path=encoded_path).geturl()
    else:
        if not os.path.exists(parquet_file):
            raise click.BadParameter(f"Local file not found: {parquet_file}")
        safe_url = parquet_file

    # Open and read Parquet metadata
    with fsspec.open(safe_url, 'rb') as f:
        parquet_file = pq.ParquetFile(f)
        metadata = parquet_file.metadata

    # Row group analysis
    total_rows = metadata.num_rows
    num_groups = metadata.num_row_groups
    avg_rows_per_group = total_rows / num_groups if num_groups > 0 else 0
    total_size = sum(metadata.row_group(i).total_byte_size for i in range(num_groups))
    avg_group_size = total_size / num_groups if num_groups > 0 else 0
    
    # Convert to MB for assessment
    avg_group_size_mb = avg_group_size / (1024 * 1024)
    total_size_mb = total_size / (1024 * 1024)
    
    click.echo("\nRow Group Analysis:")
    click.echo(f"Number of row groups: {num_groups}")
    
    # Size assessment
    size_color, size_message = get_row_group_size_assessment(avg_group_size_mb, total_size_mb)
    click.echo(click.style(f"Average group size: {format_size(avg_group_size)}", fg=size_color))
    click.echo(click.style(size_message, fg=size_color))
    
    # Row count assessment
    row_color, row_message = get_row_count_assessment(avg_rows_per_group)
    click.echo(click.style(f"Average rows per group: {avg_rows_per_group:,.0f}", fg=row_color))
    click.echo(click.style(row_message, fg=row_color))
    
    click.echo(f"\nTotal file size: {format_size(total_size)}")
    
    if size_color != "green" or row_color != "green":
        click.echo("\nRow Group Guidelines:")
        click.echo("- Optimal size: 128-256 MB per row group")
        click.echo("- Optimal rows: 50,000-200,000 rows per group")
        click.echo("- Small files (<128 MB): single row group is fine")
    # GeoParquet metadata analysis
    if metadata.metadata and b'geo' in metadata.metadata:
        try:
            geo_meta = json.loads(metadata.metadata[b'geo'].decode('utf-8'))
            
            if verbose:
                click.echo("\nFull GeoParquet metadata:")
                click.echo(json.dumps(geo_meta, indent=2))
            
            # Version check and bbox covering check
            version = geo_meta.get("version", "0.0.0")
            has_bbox_covering = False
            if isinstance(geo_meta, dict):
                columns = geo_meta.get("columns", {})
                for col_name, col_meta in columns.items():
                    if col_meta.get("covering", {}).get("bbox"):
                        has_bbox_covering = True
                        break

            click.echo("\nGeoParquet Metadata:")
            if version >= "1.1.0":
                click.echo(click.style(f"✓ Version {version}", fg="green"), nl=False)
                if has_bbox_covering:
                    click.echo(click.style(" | ✓ bbox covering", fg="green"))
                else:
                    click.echo(click.style(" | ⚠️  No bbox covering (recommended for better performance)", fg="yellow"))
            else:
                click.echo(click.style(f"⚠️  Version {version} (upgrade to 1.1.0+ recommended)", fg="yellow"))
            
            # Get primary geometry column to check compression
            primary_col = None
            if isinstance(geo_meta, dict):
                primary_col = geo_meta.get("primary_column")
            elif isinstance(geo_meta, list):
                for col in geo_meta:
                    if isinstance(col, dict) and col.get("primary", False):
                        primary_col = col.get("name")
                        break

            if primary_col:
                # Find geometry column index and check compression
                for i in range(len(metadata.schema)):
                    col = metadata.schema.column(i)
                    if col.name == primary_col:
                        compression = metadata.row_group(0).column(i).compression
                        if compression == 'ZSTD':
                            click.echo(click.style(f"✓ ZSTD compression", fg="green"))
                        else:
                            click.echo(click.style(f"⚠️  {compression} compression (ZSTD recommended)", fg="yellow"))
                        break

        except json.JSONDecodeError:
            click.echo(click.style("\n❌ Failed to parse GeoParquet metadata", fg="red"))
    else:
        click.echo(click.style("\n❌ No GeoParquet metadata found", fg="red"))

if __name__ == "__main__":
    check_parquet_structure() 