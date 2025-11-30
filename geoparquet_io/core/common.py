import json
import os
import re
import shutil
import tempfile
import urllib.parse
from contextlib import contextmanager
from pathlib import Path

import click
import duckdb
import fsspec
import pyarrow.parquet as pq


def is_remote_url(path):
    """
    Check if path is a remote URL that DuckDB can read.

    Supports:
    - HTTP/HTTPS: http://, https://
    - AWS S3: s3://, s3a://
    - Azure: az://, azure://, abfs://, abfss://
    - Google Cloud Storage: gs://, gcs://

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is a remote URL, False otherwise
    """
    remote_schemes = [
        "http://",
        "https://",
        "s3://",
        "s3a://",
        "gs://",
        "gcs://",
        "az://",
        "azure://",
        "abfs://",
        "abfss://",
    ]
    return any(path.startswith(scheme) for scheme in remote_schemes)


def upload_if_remote(local_path, remote_path, profile=None, is_directory=False, verbose=False):
    """
    Upload local file/dir to remote path if remote_path is a remote URL.

    Args:
        local_path: Local file or directory path to upload
        remote_path: Remote URL or local path
        profile: AWS profile name (S3 only, optional)
        is_directory: Whether local_path is a directory
        verbose: Whether to print verbose output

    Returns:
        bool: True if upload was performed, False if not remote
    """
    if not is_remote_url(remote_path):
        return False

    from geoparquet_io.core.upload import upload

    if verbose:
        # Calculate size for progress indication
        if is_directory:
            total_size = sum(
                os.path.getsize(os.path.join(dirpath, filename))
                for dirpath, _, filenames in os.walk(local_path)
                for filename in filenames
            )
        else:
            total_size = os.path.getsize(local_path)

        size_mb = total_size / (1024 * 1024)
        click.echo(f"Uploading {size_mb:.1f} MB to {remote_path}...")

    pattern = "*.parquet" if is_directory else None
    upload(
        source=Path(local_path),
        destination=remote_path,
        profile=profile,
        pattern=pattern,
        dry_run=False,
    )

    if verbose:
        click.echo(f"✓ Successfully uploaded to {remote_path}")

    return True


@contextmanager
def remote_write_context(output_path, is_directory=False, verbose=False):
    """
    Context manager for remote writes with automatic temp file/dir cleanup.

    Yields actual write path (temp for remote, original for local).
    Handles cleanup automatically on exit.

    Args:
        output_path: Output path (local or remote URL)
        is_directory: Whether output is a directory (for partitioning)
        verbose: Whether to print verbose output

    Yields:
        tuple: (actual_write_path, is_remote)
            - actual_write_path: Path to write to (temp for remote, original for local)
            - is_remote: Boolean indicating if output is remote

    Example:
        with remote_write_context('s3://bucket/file.parquet', verbose=True) as (path, is_remote):
            # Write to path
            write_file(path)
            # Cleanup and upload handled automatically
    """
    is_remote = is_remote_url(output_path)

    if is_remote:
        if is_directory:
            temp_path = tempfile.mkdtemp(prefix="gpio_")
        else:
            temp_fd, temp_path = tempfile.mkstemp(suffix=".parquet")
            os.close(temp_fd)

        if verbose:
            click.echo(f"Remote output detected: {output_path}")
            click.echo(
                f"Writing to temporary {'directory' if is_directory else 'file'}: {temp_path}"
            )
    else:
        temp_path = output_path

    try:
        yield temp_path, is_remote
    finally:
        if is_remote and os.path.exists(temp_path):
            try:
                if is_directory:
                    shutil.rmtree(temp_path)
                else:
                    os.unlink(temp_path)
                if verbose:
                    click.echo(
                        f"Cleaned up temporary {'directory' if is_directory else 'file'}: {temp_path}"
                    )
            except Exception as e:
                if verbose:
                    click.echo(
                        f"Warning: Could not clean up temp {'directory' if is_directory else 'file'} {temp_path}: {e}"
                    )


def is_s3_url(path):
    """
    Check if path is an S3 URL.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is S3
    """
    return isinstance(path, str) and path.startswith(("s3://", "s3a://"))


def is_azure_url(path):
    """
    Check if path is an Azure Blob Storage URL.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is Azure
    """
    return isinstance(path, str) and path.startswith(("az://", "azure://", "abfs://", "abfss://"))


def is_gcs_url(path):
    """
    Check if path is a Google Cloud Storage URL.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is GCS
    """
    return isinstance(path, str) and path.startswith(("gs://", "gcs://"))


def needs_httpfs(path):
    """
    Check if path requires httpfs extension (S3, Azure, GCS).

    HTTP/HTTPS work without httpfs, but cloud storage protocols need it.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if httpfs extension is needed
    """
    httpfs_schemes = [
        "s3://",
        "s3a://",
        "gs://",
        "gcs://",
        "az://",
        "azure://",
        "abfs://",
        "abfss://",
    ]
    return any(path.startswith(scheme) for scheme in httpfs_schemes)


def setup_aws_profile_if_needed(profile, *paths):
    """
    Set AWS_PROFILE environment variable if profile specified and S3 URLs detected.

    This allows both DuckDB (via credential_chain) and obstore to use the specified
    AWS profile for authentication. The profile is resolved using standard AWS SDK
    mechanisms (reads from ~/.aws/credentials, ~/.aws/config, etc.).

    Note: This is a convenience wrapper. Setting AWS_PROFILE env var directly
    has the same effect.

    Args:
        profile: AWS profile name or None
        *paths: Variable number of file paths to check for S3 URLs

    Example:
        setup_aws_profile_if_needed(profile, input_file, output_file)
        # Equivalent to: os.environ['AWS_PROFILE'] = profile
    """
    if not profile:
        return

    # Check if any path is S3
    has_s3 = any(p and is_s3_url(p) for p in paths)
    if has_s3:
        os.environ["AWS_PROFILE"] = profile


def validate_profile_for_urls(profile, *urls):
    """
    Validate that profile parameter is only used with S3 URLs.

    The --profile flag sets AWS credentials for S3 operations. Using it with
    other cloud providers (GCS, Azure) would be confusing since they use
    different authentication mechanisms.

    Args:
        profile: AWS profile name or None
        *urls: Variable number of file paths to validate

    Raises:
        click.BadParameter: If profile is used with non-S3 remote URLs

    Example:
        validate_profile_for_urls(profile, input_file, output_file)
    """
    if not profile:
        return

    for url in urls:
        if url and is_remote_url(url) and not is_s3_url(url):
            protocol = url.split("://")[0].upper() if "://" in url else "unknown"
            raise click.BadParameter(
                f"--profile flag is only valid for S3 URLs, but got {protocol} URL: {url}\n"
                f"For {protocol} authentication, use environment variables or default credentials."
            )


def show_remote_read_message(file_path, verbose=False):
    """
    Show consistent message when reading from remote files.

    Args:
        file_path: Path to check (local or remote)
        verbose: If True, show detailed message
    """
    if not is_remote_url(file_path):
        return

    protocol = file_path.split("://")[0].upper() if "://" in file_path else "HTTP"
    if verbose:
        click.echo(f"📡 Reading from {protocol}: {file_path}")
    else:
        click.echo(f"📡 Reading from {protocol} (network operations may take time)...")


def validate_output_path(output_path, verbose=False):
    """
    Validate output path for local files (remote URLs pass through).

    For local paths:
    - Check parent directory exists
    - Check parent directory is writable

    For remote URLs:
    - No validation needed (handled by remote_write_context)

    Args:
        output_path: Local file path or remote URL
        verbose: Whether to print verbose output

    Raises:
        click.ClickException: If local directory doesn't exist or isn't writable
    """
    if is_remote_url(output_path):
        # Remote outputs handled by remote_write_context
        return

    output_dir = os.path.dirname(output_path) or "."
    if not os.path.exists(output_dir):
        raise click.ClickException(f"Output directory not found: {output_dir}")
    if not os.access(output_dir, os.W_OK):
        raise click.ClickException(f"No write permission for: {output_dir}")


def get_duckdb_connection(load_spatial=True, load_httpfs=None):
    """
    Create a DuckDB connection with necessary extensions loaded.

    When load_httpfs=True, also loads the aws extension and configures
    automatic credential discovery for S3 access. Credentials are discovered
    via the AWS SDK in this order:
    1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    2. AWS profile (AWS_PROFILE env var or ~/.aws/credentials)
    3. IAM role (EC2/ECS/EKS instance metadata)

    Args:
        load_spatial: Whether to load spatial extension (default: True)
        load_httpfs: Whether to load httpfs extension for S3/Azure/GCS.
                    If None (default), auto-detects based on usage.

    Returns:
        duckdb.DuckDBPyConnection: Configured connection with extensions loaded
    """
    con = duckdb.connect()

    # Always load spatial extension by default (core use case)
    if load_spatial:
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")

    # Load httpfs for cloud storage support
    if load_httpfs:
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

        # Load aws extension for S3 authentication
        # This works in conjunction with httpfs to provide authenticated S3 access
        con.execute("INSTALL aws;")
        con.execute("LOAD aws;")

        # Configure automatic credential discovery using AWS SDK
        # This respects AWS_PROFILE, ~/.aws/credentials, env vars, and IAM roles
        # Use VALIDATION 'none' to allow creating secret without immediate credentials
        # (credentials will be discovered at query time)
        con.execute("""
            CREATE OR REPLACE SECRET (
                TYPE s3,
                PROVIDER credential_chain,
                VALIDATION 'none'
            );
        """)

    return con


def safe_file_url(file_path, verbose=False):
    """
    Handle both local and remote files, returning safe URL.

    For remote URLs, performs URL encoding if needed.
    For local files, validates existence.

    Args:
        file_path: Local file path or remote URL
        verbose: Whether to print verbose output

    Returns:
        str: Safe URL or file path

    Raises:
        click.BadParameter: If local file doesn't exist
    """
    if is_remote_url(file_path):
        # Remote URL - URL encode if HTTP/HTTPS
        if file_path.startswith(("http://", "https://")):
            parsed = urllib.parse.urlparse(file_path)
            encoded_path = urllib.parse.quote(parsed.path)
            safe_url = parsed._replace(path=encoded_path).geturl()
        else:
            safe_url = file_path

        if verbose:
            protocol = file_path.split("://")[0].upper() if "://" in file_path else "HTTP"
            click.echo(f"Reading from {protocol}: {safe_url}")
        return safe_url
    else:
        # Local file - check existence
        if not os.path.exists(file_path):
            raise click.BadParameter(f"Local file not found: {file_path}")
        return file_path


def get_remote_error_hint(error_msg, file_path=""):
    """
    Generate helpful error messages for remote file access failures.

    Args:
        error_msg: Original error message from DuckDB or other library
        file_path: The remote file path/URL that failed

    Returns:
        str: User-friendly error message with troubleshooting hints
    """
    # Simple pattern matching - check error type and return appropriate hint
    error_lower = error_msg.lower()
    path_lower = file_path.lower()

    # Check for 403/auth errors
    auth_error = "403" in error_msg or "forbidden" in error_lower or "access denied" in error_lower
    if auth_error:
        if "s3://" in path_lower:
            return "Authentication required or access denied:\n  • S3: Check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables\n  • Or configure ~/.aws/credentials file"
        if "az://" in path_lower or "azure" in path_lower or "blob.core" in path_lower:
            return "Authentication required or access denied:\n  • Azure: Check AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY\n  • Or set AZURE_STORAGE_SAS_TOKEN for SAS token auth"
        if "gs://" in path_lower or "gcs://" in path_lower:
            return "Authentication required or access denied:\n  • GCS: Check GOOGLE_APPLICATION_CREDENTIALS points to service account JSON"
        return "Authentication required or access denied:\n  • File may be private or require authentication"

    # Check for 404 errors
    if "404" in error_msg or "not found" in error_lower or "does not exist" in error_lower:
        base = "File not found at remote location:\n  • Verify the URL is correct\n  • Check the file exists at the specified path"
        return f"{base}\n  • URL: {file_path}" if file_path else base

    # Check for timeout
    if "timeout" in error_lower or "timed out" in error_lower:
        return "Connection timed out:\n  • Check network connectivity\n  • File may be very large - try a smaller file first\n  • Remote server may be slow or overloaded"

    # Check for connection issues
    if "unable to connect" in error_lower or "connection" in error_lower:
        return "Cannot connect to remote server:\n  • Check network connectivity\n  • Verify the hostname/URL is correct\n  • Server may be down or unreachable"

    # Generic
    return "Remote file access failed:\n  • Check network connectivity\n  • Verify file URL and access permissions"


def get_parquet_metadata(parquet_file, verbose=False):
    """Get Parquet file metadata."""
    with fsspec.open(parquet_file, "rb") as f:
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
    if not metadata or b"geo" not in metadata:
        return None

    try:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
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


def _parse_existing_geo_metadata(original_metadata):
    """Parse existing geo metadata from original parquet metadata."""
    if not original_metadata or b"geo" not in original_metadata:
        return None
    try:
        return json.loads(original_metadata[b"geo"].decode("utf-8"))
    except json.JSONDecodeError:
        return None


def _initialize_geo_metadata(geo_meta, geom_col):
    """Initialize or upgrade geo metadata structure."""
    if not geo_meta:
        return {"version": "1.1.0", "primary_column": geom_col, "columns": {}}

    # Upgrade to 1.1.0 if it's an older version
    geo_meta["version"] = "1.1.0"
    if "columns" not in geo_meta:
        geo_meta["columns"] = {}
    if geom_col not in geo_meta["columns"]:
        geo_meta["columns"][geom_col] = {}

    return geo_meta


def _add_bbox_covering(geo_meta, geom_col, bbox_info, verbose):
    """Add bbox covering metadata to geometry column."""
    if not bbox_info or not bbox_info.get("has_bbox_column"):
        return

    if "covering" not in geo_meta["columns"][geom_col]:
        geo_meta["columns"][geom_col]["covering"] = {}

    geo_meta["columns"][geom_col]["covering"]["bbox"] = {
        "xmin": [bbox_info["bbox_column_name"], "xmin"],
        "ymin": [bbox_info["bbox_column_name"], "ymin"],
        "xmax": [bbox_info["bbox_column_name"], "xmax"],
        "ymax": [bbox_info["bbox_column_name"], "ymax"],
    }
    if verbose:
        click.echo(f"Added bbox covering metadata for column '{bbox_info['bbox_column_name']}'")


def _add_custom_covering(geo_meta, geom_col, custom_metadata, verbose):
    """Add custom covering metadata (e.g., H3, S2)."""
    if not custom_metadata or "covering" not in custom_metadata:
        return

    if "covering" not in geo_meta["columns"][geom_col]:
        geo_meta["columns"][geom_col]["covering"] = {}

    geo_meta["columns"][geom_col]["covering"].update(custom_metadata["covering"])
    if verbose:
        for key in custom_metadata["covering"]:
            click.echo(f"Added {key} covering metadata")


def create_geo_metadata(
    original_metadata, geom_col, bbox_info, custom_metadata=None, verbose=False
):
    """
    Create or update GeoParquet metadata with spatial index covering information.

    Args:
        original_metadata: Original parquet metadata dict
        geom_col: Name of the geometry column
        bbox_info: Result from check_bbox_structure
        custom_metadata: Optional dict with custom metadata (e.g., H3 info)
        verbose: Whether to print verbose output

    Returns:
        dict: Updated geo metadata
    """
    geo_meta = _parse_existing_geo_metadata(original_metadata)
    geo_meta = _initialize_geo_metadata(geo_meta, geom_col)

    # Add encoding if not present (required by GeoParquet spec)
    if "encoding" not in geo_meta["columns"][geom_col]:
        geo_meta["columns"][geom_col]["encoding"] = "WKB"

    # Add bbox covering if needed
    _add_bbox_covering(geo_meta, geom_col, bbox_info, verbose)

    # Add custom covering if needed
    _add_custom_covering(geo_meta, geom_col, custom_metadata, verbose)

    # Add any top-level custom metadata
    if custom_metadata:
        for key, value in custom_metadata.items():
            if key != "covering":
                geo_meta[key] = value

    return geo_meta


def parse_size_string(size_str):
    """
    Parse a human-readable size string into bytes.

    Args:
        size_str: String like '256MB', '1GB', '128' (assumed MB if no unit)

    Returns:
        int: Size in bytes
    """
    if not size_str:
        return None

    # Handle plain numbers (assume MB)
    try:
        return int(size_str) * 1024 * 1024
    except ValueError:
        pass

    # Parse with units
    size_str = size_str.strip().upper()
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([KMGT]?B?)$", size_str)
    if not match:
        raise ValueError(f"Invalid size format: {size_str}")

    value = float(match.group(1))
    unit = match.group(2)

    # Convert to bytes
    multipliers = {
        "B": 1,
        "KB": 1024,
        "MB": 1024 * 1024,
        "GB": 1024 * 1024 * 1024,
        "TB": 1024 * 1024 * 1024 * 1024,
        "K": 1024,
        "M": 1024 * 1024,
        "G": 1024 * 1024 * 1024,
        "T": 1024 * 1024 * 1024 * 1024,
    }

    multiplier = multipliers.get(unit, 1024 * 1024)  # Default to MB
    return int(value * multiplier)


def calculate_row_group_size(
    total_rows, file_size_bytes, target_row_group_size_mb=None, target_row_group_rows=None
):
    """
    Calculate optimal row group size for parquet file.

    Args:
        total_rows: Total number of rows in the file
        file_size_bytes: Current file size in bytes
        target_row_group_size_mb: Target size per row group in MB
        target_row_group_rows: Exact number of rows per row group

    Returns:
        int: Number of rows per row group
    """
    if target_row_group_rows:
        # Use exact row count if specified
        return min(target_row_group_rows, total_rows)

    if not target_row_group_size_mb:
        target_row_group_size_mb = 130  # Default 130MB

    # Convert target size to bytes
    target_bytes = target_row_group_size_mb * 1024 * 1024

    # Calculate average bytes per row
    if total_rows > 0 and file_size_bytes > 0:
        bytes_per_row = file_size_bytes / total_rows
        # Calculate number of rows that would fit in target size
        rows_per_group = int(target_bytes / bytes_per_row)
        # Ensure at least 1 row per group but not more than total rows
        return max(1, min(rows_per_group, total_rows))
    else:
        # Default to all rows in one group if we can't calculate
        return max(1, total_rows)


def validate_compression_settings(compression, compression_level, verbose=False):
    """
    Validate and normalize compression settings.

    Args:
        compression: Compression type string
        compression_level: Compression level (can be None for defaults)
        verbose: Whether to print verbose output

    Returns:
        tuple: (normalized_compression, validated_level, compression_desc)
    """
    compression = compression.upper()
    valid_compressions = ["ZSTD", "GZIP", "BROTLI", "LZ4", "SNAPPY", "UNCOMPRESSED"]

    if compression not in valid_compressions:
        raise click.BadParameter(
            f"Invalid compression '{compression}'. Must be one of: {', '.join(valid_compressions)}"
        )

    # Handle compression level based on format
    compression_ranges = {
        "GZIP": (1, 9, 6),  # min, max, default
        "ZSTD": (1, 22, 15),  # min, max, default
        "BROTLI": (1, 11, 6),  # min, max, default
    }

    if compression in compression_ranges:
        min_level, max_level, default_level = compression_ranges[compression]

        # Use default if not specified
        if compression_level is None:
            compression_level = default_level

        if compression_level < min_level or compression_level > max_level:
            raise click.BadParameter(
                f"{compression} compression level must be between {min_level} and {max_level}, got {compression_level}"
            )
        compression_desc = f"{compression}:{compression_level}"
    elif compression in ["LZ4", "SNAPPY"]:
        if compression_level and compression_level != 15 and verbose:  # Not default
            click.echo(
                click.style(
                    f"Note: {compression} does not support compression levels. Ignoring level {compression_level}.",
                    fg="yellow",
                )
            )
        compression_level = None  # These formats don't use compression levels
        compression_desc = compression
    else:
        compression_level = None  # UNCOMPRESSED doesn't use levels
        compression_desc = compression

    return compression, compression_level, compression_desc


def build_copy_query(query, output_file, compression):
    """
    Build a DuckDB COPY query with proper compression settings.

    Args:
        query: SELECT query or existing COPY query
        output_file: Output file path
        compression: Compression type (already validated)

    Returns:
        str: Complete COPY query
    """
    # Map to DuckDB compression names
    duckdb_compression_map = {
        "ZSTD": "zstd",
        "GZIP": "gzip",
        "BROTLI": "brotli",
        "LZ4": "lz4",
        "SNAPPY": "snappy",
        "UNCOMPRESSED": "uncompressed",
    }
    duckdb_compression = duckdb_compression_map[compression]

    # Modify query to use the specified compression
    if "COPY (" in query and "TO '" in query:
        # Extract the query parts
        query_parts = query.split("TO '")
        if len(query_parts) == 2:
            output_path_and_rest = query_parts[1]
            # Find the end of the output path
            path_end = output_path_and_rest.find("'")
            if path_end > 0:
                # Rebuild query with compression
                base_query = query_parts[0] + f"TO '{output_file}'"
                query = base_query + f"\n(FORMAT PARQUET, COMPRESSION '{duckdb_compression}');"
    else:
        # Assume it's a SELECT query that needs COPY wrapper
        query = f"""COPY ({query})
TO '{output_file}'
(FORMAT PARQUET, COMPRESSION '{duckdb_compression}');"""

    return query


def rewrite_with_metadata(
    output_file,
    original_metadata,
    compression,
    compression_level,
    row_group_size_mb=None,
    row_group_rows=None,
    custom_metadata=None,
    verbose=False,
):
    """
    Rewrite a parquet file with updated metadata and compression settings.

    Args:
        output_file: Path to the parquet file to rewrite
        original_metadata: Original metadata to preserve
        compression: Compression type
        compression_level: Compression level
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        custom_metadata: Optional dict with custom metadata (e.g., H3 info)
        verbose: Whether to print verbose output
    """
    if verbose:
        click.echo("Updating metadata and optimizing file structure...")

    # Check if this is a Hive-partitioned file by examining the parent directory
    parent_dir = os.path.basename(os.path.dirname(output_file))
    is_hive_partition = "=" in parent_dir

    if is_hive_partition:
        # For Hive-partitioned files, read directly as a single file
        # to avoid PyArrow trying to interpret it as a dataset
        with open(output_file, "rb") as f:
            table = pq.read_table(f)
    else:
        # Read the written file normally
        table = pq.read_table(output_file)

    # Prepare metadata
    existing_metadata = table.schema.metadata or {}
    new_metadata = {}

    # Copy non-geo metadata from existing
    for k, v in existing_metadata.items():
        if not k.decode("utf-8").startswith("geo"):
            new_metadata[k] = v

    # Get geometry column and bbox info
    geom_col = find_primary_geometry_column(output_file, verbose=False)
    bbox_info = check_bbox_structure(output_file, verbose=False)

    # Create geo metadata - use existing metadata if no original provided
    # This preserves DuckDB-generated metadata (encoding, geometry_types, etc.)
    metadata_source = original_metadata if original_metadata else existing_metadata
    geo_meta = create_geo_metadata(metadata_source, geom_col, bbox_info, custom_metadata, verbose)
    new_metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")

    # Update table schema with new metadata
    new_table = table.replace_schema_metadata(new_metadata)

    # Calculate optimal row groups
    file_size = os.path.getsize(output_file)
    rows_per_group = calculate_row_group_size(
        new_table.num_rows,
        file_size,
        target_row_group_size_mb=row_group_size_mb,
        target_row_group_rows=row_group_rows,
    )

    # Set PyArrow compression parameters
    pa_compression = compression if compression != "UNCOMPRESSED" else None
    # PyArrow supports compression levels for GZIP, ZSTD, and BROTLI
    if compression in ["GZIP", "ZSTD", "BROTLI"]:
        pa_compression_level = compression_level
    else:
        pa_compression_level = None

    # Build write kwargs
    write_kwargs = {
        "row_group_size": rows_per_group,
        "compression": pa_compression,
        "write_statistics": True,
        "use_dictionary": True,
        "version": "2.6",
    }

    # Add compression level for supported formats
    if pa_compression_level is not None:
        write_kwargs["compression_level"] = pa_compression_level

    # Rewrite the file
    pq.write_table(new_table, output_file, **write_kwargs)

    if verbose:
        if compression in ["GZIP", "ZSTD", "BROTLI"]:
            compression_desc = f"{compression}:{compression_level}"
        else:
            compression_desc = compression
        click.echo(f"✓ File written with {compression_desc} compression and updated metadata")
        if row_group_rows:
            click.echo(f"  Row group size: {rows_per_group:,} rows")
        elif row_group_size_mb:
            click.echo(f"  Row group size: ~{row_group_size_mb}MB ({rows_per_group:,} rows)")


def write_parquet_with_metadata(
    con,
    query,
    output_file,
    original_metadata=None,
    compression="ZSTD",
    compression_level=15,
    row_group_size_mb=None,
    row_group_rows=None,
    custom_metadata=None,
    verbose=False,
    profile=None,
):
    """
    Write a parquet file with proper compression and metadata handling.

    Supports both local and remote outputs (S3, GCS, Azure). Remote outputs
    are written to a temporary local file, then uploaded.

    Note: Remote writes require ~2× output file size in local disk space
    for temporary processing.

    Args:
        con: DuckDB connection
        query: SQL query to execute
        output_file: Path to output file (local path or remote URL)
        original_metadata: Original metadata from source file
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        custom_metadata: Optional dict with custom metadata (e.g., H3 info)
        verbose: Whether to print verbose output
        profile: AWS profile name (S3 only, optional)

    Returns:
        None
    """
    # Setup AWS profile if needed (for writes to S3)
    setup_aws_profile_if_needed(profile, output_file)

    with remote_write_context(output_file, is_directory=False, verbose=verbose) as (
        actual_output,
        is_remote,
    ):
        # Validate compression settings
        compression, compression_level, compression_desc = validate_compression_settings(
            compression, compression_level, verbose
        )

        if verbose:
            click.echo(f"Writing output with {compression_desc} compression...")

        # Build and execute query
        final_query = build_copy_query(query, actual_output, compression)
        con.execute(final_query)

        # Rewrite with metadata and optimal settings
        if original_metadata or verbose or row_group_size_mb or row_group_rows:
            rewrite_with_metadata(
                actual_output,
                original_metadata,
                compression,
                compression_level,
                row_group_size_mb,
                row_group_rows,
                custom_metadata,
                verbose,
            )

        # Upload to remote if needed
        if is_remote:
            upload_if_remote(
                actual_output, output_file, profile=profile, is_directory=False, verbose=verbose
            )


def update_metadata(output_file, original_metadata):
    """Update a parquet file with original metadata and add bbox covering if present."""
    if not original_metadata:
        return

    # Use the rewrite function with default compression settings
    rewrite_with_metadata(
        output_file, original_metadata, compression="ZSTD", compression_level=15, verbose=False
    )


def format_size(size_bytes):
    """Convert bytes to human readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def _find_bbox_column_in_schema(schema, verbose):
    """Find bbox column in schema by conventional names or structure."""
    conventional_names = ["bbox", "bounds", "extent"]
    for field in schema:
        if field.name in conventional_names or (
            isinstance(field.type, type(schema[0].type))
            and str(field.type).startswith("struct<")
            and all(f in str(field.type) for f in ["xmin", "ymin", "xmax", "ymax"])
        ):
            if verbose:
                click.echo(f"Found bbox column: {field.name} with type {field.type}")
            return field.name
    return None


def _check_bbox_metadata_covering(metadata, has_bbox_column, verbose):
    """Check if metadata contains proper bbox covering."""
    if not (metadata and b"geo" in metadata and has_bbox_column):
        return False

    try:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        if verbose:
            click.echo("\nParsed geo metadata:")
            click.echo(json.dumps(geo_meta, indent=2))

        if isinstance(geo_meta, dict) and "columns" in geo_meta:
            columns = geo_meta["columns"]
            for _col_name, col_info in columns.items():
                if isinstance(col_info, dict) and col_info.get("covering", {}).get("bbox"):
                    bbox_refs = col_info["covering"]["bbox"]
                    # Check if the bbox covering has the required structure
                    if (
                        isinstance(bbox_refs, dict)
                        and all(key in bbox_refs for key in ["xmin", "ymin", "xmax", "ymax"])
                        and all(
                            isinstance(ref, list) and len(ref) == 2 for ref in bbox_refs.values()
                        )
                    ):
                        referenced_bbox_column = bbox_refs["xmin"][0]
                        if verbose:
                            click.echo(
                                f"Found bbox covering in metadata referencing column: {referenced_bbox_column}"
                            )
                        return True
    except json.JSONDecodeError:
        if verbose:
            click.echo("Failed to parse geo metadata as JSON")

    return False


def _determine_bbox_status(has_bbox_column, bbox_column_name, has_bbox_metadata):
    """Determine bbox status and message."""
    if has_bbox_column and has_bbox_metadata:
        return "optimal", f"✓ Found bbox column '{bbox_column_name}' with proper metadata covering"
    elif has_bbox_column:
        return (
            "suboptimal",
            f"⚠️  Found bbox column '{bbox_column_name}' but no bbox covering metadata (recommended for better performance)",
        )
    else:
        return "poor", "❌ No valid bbox column found"


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
    with fsspec.open(safe_file_url(parquet_file), "rb") as f:
        pf = pq.ParquetFile(f)
        metadata = pf.schema_arrow.metadata
        schema = pf.schema_arrow

    if verbose:
        click.echo("\nSchema fields:")
        for field in schema:
            click.echo(f"  {field.name}: {field.type}")

    # Find the bbox column in the schema
    bbox_column_name = _find_bbox_column_in_schema(schema, verbose)
    has_bbox_column = bbox_column_name is not None

    # Check metadata for bbox covering
    has_bbox_metadata = _check_bbox_metadata_covering(metadata, has_bbox_column, verbose)

    # Determine status and message
    status, message = _determine_bbox_status(has_bbox_column, bbox_column_name, has_bbox_metadata)

    if verbose:
        click.echo("\nFinal results:")
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
        "message": message,
    }


def _build_bounds_query(safe_url, bbox_info, geometry_column, verbose):
    """Build query for bounds calculation."""
    if bbox_info["has_bbox_column"]:
        bbox_col = bbox_info["bbox_column_name"]
        if verbose:
            click.echo(f"Using bbox column '{bbox_col}' for fast bounds calculation")

        return f"""
        SELECT
            MIN({bbox_col}.xmin) as xmin,
            MIN({bbox_col}.ymin) as ymin,
            MAX({bbox_col}.xmax) as xmax,
            MAX({bbox_col}.ymax) as ymax
        FROM '{safe_url}'
        """
    else:
        click.echo(
            click.style(
                f"⚠️  No bbox column found - calculating bounds from geometry column '{geometry_column}' (this may be slow)",
                fg="yellow",
            )
        )
        click.echo(
            click.style(
                "💡 Tip: Add a bbox column for faster operations with 'gpio add bbox'", fg="cyan"
            )
        )

        return f"""
        SELECT
            MIN(ST_XMin({geometry_column})) as xmin,
            MIN(ST_YMin({geometry_column})) as ymin,
            MAX(ST_XMax({geometry_column})) as xmax,
            MAX(ST_YMax({geometry_column})) as ymax
        FROM '{safe_url}'
        """


def get_dataset_bounds(parquet_file, geometry_column=None, verbose=False):
    """
    Calculate the bounding box of the entire dataset.

    Uses bbox column if available for fast calculation, otherwise calculates
    from geometry column (slower).

    Args:
        parquet_file: Path to the parquet file
        geometry_column: Geometry column name (if None, will auto-detect)
        verbose: Whether to print verbose output

    Returns:
        tuple: (xmin, ymin, xmax, ymax) or None if error
    """
    safe_url = safe_file_url(parquet_file, verbose)

    # Get geometry column if not specified
    if not geometry_column:
        geometry_column = find_primary_geometry_column(parquet_file, verbose)

    # Check for bbox column
    bbox_info = check_bbox_structure(parquet_file, verbose)

    # Create DuckDB connection with httpfs if needed
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(parquet_file))

    try:
        query = _build_bounds_query(safe_url, bbox_info, geometry_column, verbose)
        result = con.execute(query).fetchone()

        if result and all(v is not None for v in result):
            xmin, ymin, xmax, ymax = result
            if verbose:
                click.echo(f"Dataset bounds: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")
            return (xmin, ymin, xmax, ymax)
        else:
            if verbose:
                click.echo("Warning: Could not calculate bounds (empty dataset or null geometries)")
            return None

    except Exception as e:
        if verbose:
            click.echo(f"Error calculating bounds: {e}")
        return None
    finally:
        con.close()


def add_computed_column(
    input_parquet,
    output_parquet,
    column_name,
    sql_expression,
    extensions=None,
    dry_run=False,
    verbose=False,
    compression="ZSTD",
    compression_level=None,
    row_group_size_mb=None,
    row_group_rows=None,
    dry_run_description=None,
    custom_metadata=None,
    profile=None,
):
    """
    Add a computed column to a GeoParquet file using SQL expression.

    Handles all boilerplate for adding columns derived from existing data:
    - Input validation
    - Schema checking
    - DuckDB connection and extension loading
    - Query execution
    - Metadata preservation
    - Dry-run support
    - Remote input/output support
    - Streaming input/output support (Arrow IPC via stdin/stdout)

    Args:
        input_parquet: Path to input file, remote URL, or "-" for stdin
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
        column_name: Name for the new column
        sql_expression: SQL expression to compute column value
        extensions: DuckDB extensions to load beyond 'spatial' (e.g., ['h3'])
        dry_run: Whether to print SQL without executing
        verbose: Whether to print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        dry_run_description: Optional description for dry-run output
        custom_metadata: Optional dict with custom metadata (e.g., H3 info)
        profile: AWS profile name (S3 only, optional)

    Example:
        add_computed_column(
            'input.parquet', 'output.parquet',
            column_name='h3_cell',
            sql_expression="h3_latlng_to_cell(ST_Y(ST_Centroid(geometry)), "
                          "ST_X(ST_Centroid(geometry)), 9)",
            extensions=['h3'],
            custom_metadata={'covering': {'h3': {'column': 'h3_cell', 'resolution': 9}}}
        )
    """
    from geoparquet_io.core.stream_io import open_input, write_output
    from geoparquet_io.core.streaming import is_stdin, should_stream_output

    # Check if we're in streaming mode
    is_streaming_input = is_stdin(input_parquet)
    is_streaming_output = should_stream_output(output_parquet)

    # Suppress verbose output when streaming to stdout (would corrupt the stream)
    if is_streaming_output:
        verbose = False

    # Handle dry-run mode for file-based input
    if dry_run and not is_streaming_input:
        input_url = safe_file_url(input_parquet, verbose)
        geom_col = find_primary_geometry_column(input_parquet, verbose)

        click.echo(
            click.style(
                "\n=== DRY RUN MODE - SQL Commands that would be executed ===\n",
                fg="yellow",
                bold=True,
            )
        )
        click.echo(click.style(f"-- Input file: {input_url}", fg="cyan"))
        click.echo(click.style(f"-- Output: {output_parquet or '(stdout)'}", fg="cyan"))
        click.echo(click.style(f"-- Geometry column: {geom_col}", fg="cyan"))
        click.echo(click.style(f"-- New column: {column_name}", fg="cyan"))
        if dry_run_description:
            click.echo(click.style(f"-- Description: {dry_run_description}", fg="cyan"))
        click.echo()

        compression_desc = compression
        if compression in ["GZIP", "ZSTD", "BROTLI"] and compression_level:
            compression_desc = f"{compression}:{compression_level}"

        duckdb_compression = (
            compression.lower() if compression != "UNCOMPRESSED" else "uncompressed"
        )
        query = f"""
        SELECT
            *,
            {sql_expression} AS {column_name}
        FROM '{input_url}'
    """
        display_query = f"""COPY ({query.strip()})
TO '{output_parquet or "(stdout)"}'
(FORMAT PARQUET, COMPRESSION '{duckdb_compression}');"""

        click.echo(click.style("-- Main query:", fg="cyan"))
        click.echo(display_query)
        click.echo(click.style(f"\n-- Note: Using {compression_desc} compression", fg="cyan"))
        click.echo(
            click.style(
                "-- This query creates output with the computed column added", fg="cyan"
            )
        )
        return

    # Use unified streaming/file I/O
    with open_input(input_parquet, verbose=verbose) as (source_ref, metadata, is_stream, con):
        # Load additional extensions if specified
        if extensions:
            for ext in extensions:
                if verbose:
                    click.echo(f"Loading DuckDB extension: {ext}")
                con.execute(f"INSTALL {ext} FROM community;")
                con.execute(f"LOAD {ext};")

        # Check if column already exists
        if is_stream:
            # For streaming, check the registered table's schema
            schema_result = con.execute(f"DESCRIBE {source_ref}").fetchall()
            existing_columns = [row[0] for row in schema_result]
        else:
            # For files, use fsspec
            input_url = safe_file_url(input_parquet, verbose=False)
            with fsspec.open(input_url, "rb") as f:
                pf = pq.ParquetFile(f)
                existing_columns = [field.name for field in pf.schema_arrow]

        if column_name in existing_columns:
            raise click.ClickException(
                f"Column '{column_name}' already exists in the input. "
                f"Please choose a different name."
            )

        # Get row count for progress display
        if verbose:
            total_count = con.execute(f"SELECT COUNT(*) FROM {source_ref}").fetchone()[0]
            click.echo(f"Processing {total_count:,} features...")
            click.echo(f"Adding column '{column_name}'...")

        # Build the query
        query = f"""
            SELECT
                *,
                {sql_expression} AS {column_name}
            FROM {source_ref}
        """

        # Execute and write output (handles both streaming and file output)
        write_output(
            con,
            query,
            output_parquet,
            original_metadata=metadata,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
        )


def add_bbox(parquet_file, bbox_column_name="bbox", verbose=False):
    """
    Add a bbox struct column to a GeoParquet file in-place.

    Internal helper function used by --add-bbox flags in other commands
    (hilbert_order, add_country_codes). Modifies the file in-place by
    writing to a temporary file and replacing the original.

    Raises an error if the bbox column already exists.

    Args:
        parquet_file: Path to the parquet file (will be modified in-place)
        bbox_column_name: Name for the bbox column (default: 'bbox')
        verbose: Whether to print verbose output

    Returns:
        bool: True if bbox was added successfully

    Raises:
        click.ClickException: If column already exists or operation fails
    """
    # Check if column already exists
    with fsspec.open(safe_file_url(parquet_file), "rb") as f:
        pf = pq.ParquetFile(f)
        schema = pf.schema_arrow

    for field in schema:
        if field.name == bbox_column_name:
            raise click.ClickException(
                f"Column '{bbox_column_name}' already exists in the file. "
                f"Please choose a different name."
            )

    # Get geometry column for SQL expression
    geom_col = find_primary_geometry_column(parquet_file, verbose)

    if verbose:
        click.echo(f"Adding bbox column for geometry column: {geom_col}")

    # Define SQL expression
    sql_expression = f"""STRUCT_PACK(
        xmin := ST_XMin({geom_col}),
        ymin := ST_YMin({geom_col}),
        xmax := ST_XMax({geom_col}),
        ymax := ST_YMax({geom_col})
    )"""

    # Create temporary file path
    temp_file = parquet_file + ".tmp"

    try:
        # Use add_computed_column to write to temp file
        add_computed_column(
            input_parquet=parquet_file,
            output_parquet=temp_file,
            column_name=bbox_column_name,
            sql_expression=sql_expression,
            extensions=None,
            dry_run=False,
            verbose=verbose,
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            dry_run_description=None,
        )

        # Replace original file with updated file
        os.replace(temp_file, parquet_file)

        if verbose:
            click.echo(f"Successfully added bbox column '{bbox_column_name}'")

        return True

    except Exception as e:
        # Clean up temporary file if something goes wrong
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise click.ClickException(f"Failed to add bbox: {str(e)}") from e
