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
    Check bbox structure and metadata coverage in a GeoParquet file.
    
    Returns:
        dict: Results including:
            - has_bbox_column (bool): Whether a valid bbox struct column exists
            - bbox_column_name (str): Name of the bbox column if found
            - has_bbox_metadata (bool): Whether bbox covering is specified in metadata
            - status (str): "optimal", "suboptimal", or "poor"
            - message (str): Human readable description
    """
    with fsspec.open(safe_file_url(parquet_file), 'rb') as f:
        pf = pq.ParquetFile(f)
        metadata = pf.schema_arrow.metadata
        schema = pf.schema_arrow

    if verbose:
        click.echo("\nSchema fields:")
        for field in schema:
            click.echo(f"  {field.name}: {field.type}")

    # First find the bbox column in the schema
    bbox_column_name = None
    has_bbox_column = False
    
    # Look for conventional names first
    conventional_names = ['bbox', 'bounds', 'extent']
    for field in schema:
        if field.name in conventional_names or (
            isinstance(field.type, type(schema[0].type)) and 
            str(field.type).startswith('struct<') and 
            all(f in str(field.type) for f in ['xmin', 'ymin', 'xmax', 'ymax'])
        ):
            bbox_column_name = field.name
            has_bbox_column = True
            if verbose:
                click.echo(f"Found bbox column: {field.name} with type {field.type}")
            break

    # Then check metadata for bbox covering that specifically references the bbox column
    has_bbox_metadata = False
    if metadata and b'geo' in metadata and has_bbox_column:
        try:
            geo_meta = json.loads(metadata[b'geo'].decode('utf-8'))
            if verbose:
                click.echo("\nParsed geo metadata:")
                click.echo(json.dumps(geo_meta, indent=2))
            
            if isinstance(geo_meta, dict) and 'columns' in geo_meta:
                columns = geo_meta['columns']
                for col_name, col_info in columns.items():
                    if isinstance(col_info, dict) and col_info.get("covering", {}).get("bbox"):
                        bbox_refs = col_info["covering"]["bbox"]
                        # Check if the bbox covering references our bbox column
                        if isinstance(bbox_refs, dict) and all(
                            isinstance(ref, list) and 
                            len(ref) == 2 and 
                            ref[0] == bbox_column_name
                            for ref in bbox_refs.values()
                        ):
                            has_bbox_metadata = True
                            if verbose:
                                click.echo(f"Found bbox covering in metadata referencing column: {bbox_column_name}")
                            break
        except json.JSONDecodeError:
            if verbose:
                click.echo("Failed to parse geo metadata as JSON")

    # Determine status and message
    if has_bbox_column and has_bbox_metadata:
        status = "optimal"
        message = f"✓ Found bbox column '{bbox_column_name}' with proper metadata covering"
    elif has_bbox_column:
        status = "suboptimal"
        message = f"⚠️  Found bbox column '{bbox_column_name}' but no bbox covering metadata (recommended for better performance)"
    else:
        status = "poor"
        message = "❌ No valid bbox column found"

    if verbose:
        click.echo(f"\nFinal results:")
        click.echo(f"  has_bbox_column: {has_bbox_column}")
        click.echo(f"  bbox_column_name: {bbox_column_name}")
        click.echo(f"  has_bbox_metadata: {has_bbox_metadata}")
        click.echo(f"  status: {status}")
        click.echo(f"  message: {message}")

    return {
        "has_bbox_column": has_bbox_column,
        "bbox_column_name": bbox_column_name if has_bbox_column else None,
        "has_bbox_metadata": has_bbox_metadata,
        "status": status,
        "message": message
    }