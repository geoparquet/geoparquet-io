import click
import json
import fsspec
import pyarrow.parquet as pq
import urllib.parse
import os
import duckdb

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
    """Update a parquet file with original metadata and add bbox covering if present."""
    if not original_metadata:
        return
        
    table = pq.read_table(output_file)
    existing_metadata = table.schema.metadata or {}
    new_metadata = {
        k: v for k, v in existing_metadata.items()
        if not k.decode('utf-8').startswith('geo')  # Remove existing geo metadata
    }
    
    # Check for bbox column
    bbox_info = check_bbox_structure(output_file, verbose=False)
    
    # Get geometry column
    geom_col = find_primary_geometry_column(output_file, verbose=False)
    
    # Start with original geo metadata if it exists
    try:
        if b'geo' in original_metadata:
            geo_meta = json.loads(original_metadata[b'geo'].decode('utf-8'))
        else:
            geo_meta = {
                "version": "1.1.0",
                "primary_column": geom_col,
                "columns": {}
            }
    except json.JSONDecodeError:
        geo_meta = {
            "version": "1.1.0",
            "primary_column": geom_col,
            "columns": {}
        }
    
    # Ensure proper structure
    if "columns" not in geo_meta:
        geo_meta["columns"] = {}
    if geom_col not in geo_meta["columns"]:
        geo_meta["columns"][geom_col] = {}
    
    # Add bbox covering if bbox column exists
    if bbox_info["has_bbox_column"]:
        geo_meta["columns"][geom_col]["covering"] = {
            "bbox": {
                "xmin": [bbox_info["bbox_column_name"], "xmin"],
                "ymin": [bbox_info["bbox_column_name"], "ymin"],
                "xmax": [bbox_info["bbox_column_name"], "xmax"],
                "ymax": [bbox_info["bbox_column_name"], "ymax"]
            }
        }
    
    # Add updated geo metadata
    new_metadata[b'geo'] = json.dumps(geo_meta).encode('utf-8')
    
    # Update table schema with new metadata
    new_table = table.replace_schema_metadata(new_metadata)
    
    # Get file size for row group calculation
    file_size = os.path.getsize(output_file)
    
    # Calculate optimal row groups using actual file size
    num_row_groups = calculate_row_group_count(new_table.num_rows, file_size)
    rows_per_group = new_table.num_rows // num_row_groups

    pq.write_table(
        new_table,
        output_file,
        row_group_size=rows_per_group,
        compression='ZSTD',
        write_statistics=True,
        use_dictionary=True,
        version='2.6'
    )

def calculate_row_group_count(total_rows, file_size_bytes, target_row_group_size_mb=130):
    """Calculate optimal number of row groups based on target size in MB."""
    # Convert target size to bytes
    target_bytes = target_row_group_size_mb * 1024 * 1024
    
    # Calculate average bytes per row
    bytes_per_row = file_size_bytes / total_rows if total_rows > 0 else 0
    
    # Calculate number of rows that would fit in target size
    rows_per_group = int(target_bytes / bytes_per_row) if bytes_per_row > 0 else total_rows
    
    # Calculate number of groups needed
    num_groups = max(1, total_rows // rows_per_group)
        
    return num_groups

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
                        # Check if the bbox covering has the required structure
                        if isinstance(bbox_refs, dict) and all(
                            key in bbox_refs for key in ['xmin', 'ymin', 'xmax', 'ymax']
                        ) and all(
                            isinstance(ref, list) and len(ref) == 2
                            for ref in bbox_refs.values()
                        ):
                            referenced_bbox_column = bbox_refs['xmin'][0]  # Get column name from any coordinate
                            has_bbox_metadata = True
                            if verbose:
                                click.echo(f"Found bbox covering in metadata referencing column: {referenced_bbox_column}")
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

def add_bbox(parquet_file, verbose=False):
    """
    Add a bbox struct column to a GeoParquet file if it doesn't exist.
    
    Args:
        parquet_file: Path to the parquet file
        verbose: Whether to print verbose output
        
    Returns:
        bool: True if bbox was added, False if it already existed
    """
    # Check if bbox already exists
    bbox_info = check_bbox_structure(parquet_file, verbose)
    if bbox_info["has_bbox_column"]:
        if verbose:
            click.echo(f"Bbox column '{bbox_info['bbox_column_name']}' already exists, no action needed")
        return False
        
    safe_url = safe_file_url(parquet_file, verbose)
    
    # Get geometry column
    geom_col = find_primary_geometry_column(parquet_file, verbose)
    
    if verbose:
        click.echo(f"Adding bbox column for geometry column: {geom_col}")
    
    # Create temporary file path
    temp_file = parquet_file + ".tmp"
    
    try:
        # Create DuckDB connection
        con = duckdb.connect()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        
        # Add bbox column
        query = f"""
        COPY (
            SELECT 
                *,
                STRUCT_PACK(
                    xmin := ST_XMin({geom_col}),
                    ymin := ST_YMin({geom_col}),
                    xmax := ST_XMax({geom_col}),
                    ymax := ST_YMax({geom_col})
                ) as bbox
            FROM '{safe_url}'
        )
        TO '{temp_file}'
        (FORMAT PARQUET);
        """
        
        con.execute(query)
        
        # move temp file to original file
        os.replace(temp_file, parquet_file)
        
        if verbose:
            click.echo("Successfully added bbox column")
        
        return True
        
    except Exception as e:
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise click.ClickException(f"Failed to add bbox: {str(e)}")