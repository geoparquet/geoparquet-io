import click
import json
import fsspec
import pyarrow.parquet as pq
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

def get_parquet_metadata(parquet_file, verbose=False):
    """Get Parquet file metadata."""
    with fsspec.open(parquet_file, 'rb') as f:
        pf = pq.ParquetFile(f)
        metadata = pf.schema_arrow.metadata
        schema = pf.schema_arrow
        
    if verbose and metadata:
        click.echo("\nParquet metadata key-value pairs:")
        for key in metadata:
            click.echo(f"{key}: {metadata[key]}")
            
    return metadata, schema

def parse_geo_metadata(metadata, verbose=False):
    """Parse GeoParquet metadata from Parquet metadata."""
    if not metadata or b'geo' not in metadata:
        return None
        
    try:
        geo_meta = json.loads(metadata[b'geo'].decode('utf-8'))
        if verbose:
            click.echo("\nParsed geo metadata:")
            click.echo(json.dumps(geo_meta, indent=2))
        return geo_meta
    except json.JSONDecodeError:
        if verbose:
            click.echo("Failed to parse geo metadata as JSON")
        return None

def find_primary_geometry_column(parquet_file, verbose=False):
    """Find primary geometry column from GeoParquet metadata."""
    metadata, _ = get_parquet_metadata(parquet_file, verbose)
    geo_meta = parse_geo_metadata(metadata, verbose)
    
    if not geo_meta:
        return "geometry"
        
    if isinstance(geo_meta, dict):
        return geo_meta.get("primary_column", "geometry")
    elif isinstance(geo_meta, list):
        for col in geo_meta:
            if isinstance(col, dict) and col.get("primary", False):
                return col.get("name", "geometry")
    
    return "geometry"

def update_metadata(output_file, original_metadata):
    """Update a parquet file with original metadata."""
    if not original_metadata:
        return
        
    table = pq.read_table(output_file)
    existing_metadata = table.schema.metadata or {}
    new_metadata = {
        k: v for k, v in existing_metadata.items()
    }
    
    # Add original geo metadata
    for k, v in original_metadata.items():
        if k.decode('utf-8').startswith('geo'):
            new_metadata[k] = v
    
    new_table = table.replace_schema_metadata(new_metadata)
    pq.write_table(new_table, output_file)

def format_size(size_bytes):
    """Convert bytes to human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"

def check_bbox_structure(parquet_file, verbose=False):
    """
    Check if the parquet file has bbox covering metadata and proper bbox column structure.
    
    Args:
        parquet_file (str): Path to parquet file
        verbose (bool): Whether to print debug information
        
    Returns:
        str: Name of bbox column to use
        
    Raises:
        click.BadParameter: If bbox column structure is invalid
    """
    with fsspec.open(parquet_file, 'rb') as f:
        pf = pq.ParquetFile(f)
        metadata = pf.schema_arrow.metadata
        schema = pf.schema_arrow

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
                    for col_name, col_info in columns.items():
                        if isinstance(col_info, dict) and 'covering' in col_info:
                            covering = col_info['covering']
                            if isinstance(covering, dict) and 'bbox' in covering:
                                bbox_info = covering['bbox']
                                if isinstance(bbox_info, dict) and all(k in bbox_info for k in ['xmin', 'ymin', 'xmax', 'ymax']):
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