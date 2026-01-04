import json
import os
import re
import shutil
import tempfile
import urllib.parse
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import click
import duckdb
import pyarrow.parquet as pq

from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    error,
    info,
    progress,
    success,
    warn,
)
from geoparquet_io.core.streaming import extract_version_from_metadata

# Per-bucket cache for S3 buckets that require authentication
# Buckets not in this set are accessed without credentials (works for public buckets)
_s3_buckets_needing_auth: set[str] = set()


def _extract_bucket_name(path: str) -> str:
    """Extract bucket name from S3 URL."""
    # s3://bucket-name/path -> bucket-name
    path_without_protocol = path.split("://", 1)[1]
    return path_without_protocol.split("/")[0]


def _clear_s3_cache():
    """Clear S3 access cache (useful for testing)."""
    global _s3_buckets_needing_auth
    _s3_buckets_needing_auth = set()


def _needs_s3_auth(exception: Exception) -> bool:
    """Detect if exception indicates S3 bucket requires authentication."""
    error_str = str(exception).lower()
    # 403 without credentials means we need to authenticate
    auth_indicators = ["403", "forbidden", "access denied", "unauthorized"]
    return any(ind in error_str for ind in auth_indicators)


# GeoParquet version configuration
# Maps CLI version options to DuckDB parameters and metadata settings
# Note: For v2, we skip pyarrow rewrite to preserve native Parquet Geometry types
# that DuckDB writes. DuckDB already produces correct GeoParquet 2.0 metadata.
GEOPARQUET_VERSIONS = {
    "1.0": {"duckdb_param": "V1", "metadata_version": "1.0.0", "rewrite_metadata": True},
    "1.1": {"duckdb_param": "V1", "metadata_version": "1.1.0", "rewrite_metadata": True},
    "2.0": {"duckdb_param": "V2", "metadata_version": "2.0.0", "rewrite_metadata": False},
    "parquet-geo-only": {
        "duckdb_param": "NONE",
        "metadata_version": None,
        "rewrite_metadata": False,
    },
}
DEFAULT_GEOPARQUET_VERSION = "1.1"


@dataclass
class ParquetWriteSettings:
    """
    Central configuration for Parquet write best practices.
    Single source of truth for compression, row groups, and other settings.
    """

    compression: str = "ZSTD"
    compression_level: int = 15
    row_group_rows: int | None = None
    row_group_size_mb: int | None = None

    # Best practice constants
    DEFAULT_COMPRESSION = "ZSTD"
    DEFAULT_COMPRESSION_LEVEL = 15
    DEFAULT_ROW_GROUP_ROWS = 100_000
    DEFAULT_PARQUET_VERSION = "2.6"

    def get_pyarrow_kwargs(self, calculated_row_group_size: int | None = None) -> dict:
        """Get kwargs dict for PyArrow write_table()."""
        pa_compression = self.compression if self.compression != "UNCOMPRESSED" else None
        pa_compression_level = (
            self.compression_level if self.compression in ["GZIP", "ZSTD", "BROTLI"] else None
        )

        row_group_size = (
            calculated_row_group_size or self.row_group_rows or self.DEFAULT_ROW_GROUP_ROWS
        )

        kwargs = {
            "row_group_size": row_group_size,
            "compression": pa_compression,
            "write_statistics": True,
            "use_dictionary": True,
            "version": self.DEFAULT_PARQUET_VERSION,
        }

        if pa_compression_level is not None:
            kwargs["compression_level"] = pa_compression_level

        return kwargs


def should_skip_bbox(geoparquet_version):
    """Check if bbox column should be skipped for this GeoParquet version.

    For GeoParquet 2.0 and parquet-geo-only, bbox columns are not needed because
    native Parquet geo types provide row group statistics for spatial filtering.

    Args:
        geoparquet_version: Version string (e.g., "1.1", "2.0", "parquet-geo-only")

    Returns:
        bool: True if bbox should be skipped, False if bbox should be added
    """
    return geoparquet_version in ("2.0", "parquet-geo-only")


def detect_geoparquet_file_type(parquet_file, verbose=False):
    """
    Detect the GeoParquet/Parquet-geo type of a file.

    Determines whether a file is:
    - GeoParquet 1.x (has geo metadata with version 1.x)
    - GeoParquet 2.0 (has geo metadata with version 2.x, uses native Parquet geo types)
    - Parquet-geo-only (has native Parquet geo types but NO geo metadata)
    - Unknown (no geo indicators found)

    Args:
        parquet_file: Path to the parquet file
        verbose: Whether to print verbose output

    Returns:
        dict with:
            - has_geo_metadata: bool - Has 'geo' key in metadata
            - geo_version: str - GeoParquet version from metadata (e.g., "1.1.0", "2.0.0") or None
            - has_native_geo_types: bool - Has Parquet GEOMETRY/GEOGRAPHY logical types
            - file_type: str - One of: "geoparquet_v1", "geoparquet_v2", "parquet_geo_only", "unknown"
            - bbox_recommended: bool - Whether bbox column is recommended for this file type
    """
    from geoparquet_io.core.duckdb_metadata import (
        detect_geometry_columns,
        get_geo_metadata,
    )

    result = {
        "has_geo_metadata": False,
        "geo_version": None,
        "has_native_geo_types": False,
        "file_type": "unknown",
        "bbox_recommended": True,  # Default for v1.x
    }

    safe_url = safe_file_url(parquet_file, verbose=False)

    # Check for geo metadata using DuckDB
    geo_meta = get_geo_metadata(safe_url)
    if geo_meta:
        result["has_geo_metadata"] = True
        if isinstance(geo_meta, dict) and "version" in geo_meta:
            result["geo_version"] = geo_meta["version"]

    # Check for native Parquet geo types using DuckDB schema
    geo_columns = detect_geometry_columns(safe_url)
    if geo_columns:
        result["has_native_geo_types"] = True

    # Determine file type
    if result["has_geo_metadata"]:
        version = result["geo_version"]
        if version and version.startswith("2."):
            result["file_type"] = "geoparquet_v2"
            result["bbox_recommended"] = False  # V2 uses native geo row group stats
        else:
            result["file_type"] = "geoparquet_v1"
            result["bbox_recommended"] = True  # V1.x needs bbox for spatial filtering
    elif result["has_native_geo_types"]:
        result["file_type"] = "parquet_geo_only"
        result["bbox_recommended"] = False  # Native geo types provide row group stats
    # else: remains "unknown"

    if verbose:
        debug(f"File type detection: {result}")

    return result


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


def has_glob_pattern(path: str) -> bool:
    """
    Check if path contains glob wildcards.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path contains glob characters (*, ?, [), False otherwise
    """
    return any(c in path for c in ("*", "?", "["))


def is_partition_path(path: str) -> bool:
    """
    Check if path represents a partitioned dataset.

    Detects:
    - Local directories containing parquet files
    - Paths with glob patterns (*, ?, [)
    - Hive-style paths (key=value in path) for remote URLs

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path appears to be a partitioned dataset
    """
    # Check for glob patterns
    if has_glob_pattern(path):
        return True

    # Check if local path is a directory
    if not is_remote_url(path) and os.path.isdir(path):
        return True

    # Check for hive-style partitioning in remote URLs (key=value in path components)
    if is_remote_url(path):
        # Extract path portion after scheme and host
        # e.g., s3://bucket/prefix/country=US/data.parquet -> prefix/country=US/data.parquet
        path_parts = path.split("/")
        # Check if any path component contains = (hive-style partition)
        for part in path_parts[3:]:  # Skip scheme://host/bucket parts
            if "=" in part and not part.endswith(".parquet"):
                return True

    return False


def resolve_partition_path(path: str, hive_partitioning: bool | None = None) -> tuple[str, dict]:
    """
    Resolve a partition path to a format DuckDB can read.

    For directories, converts to glob pattern. Returns the resolved path
    and read_parquet options dict.

    Args:
        path: File path or URL (may be directory or glob pattern)
        hive_partitioning: Explicitly enable/disable hive partitioning.
                          If None, auto-detect from path structure.

    Returns:
        tuple: (resolved_path, read_options_dict)
            - resolved_path: Path/glob pattern for DuckDB
            - read_options_dict: Options for read_parquet (hive_partitioning, etc.)
    """
    options = {}
    resolved = path

    # Handle local directories
    if not is_remote_url(path) and os.path.isdir(path):
        try:
            items = os.listdir(path)
            subdirs = [d for d in items if os.path.isdir(os.path.join(path, d))]
            has_parquet_files = any(
                f.endswith(".parquet") for f in items if not os.path.isdir(os.path.join(path, f))
            )
            has_hive_subdirs = any("=" in d for d in subdirs)

            if has_hive_subdirs:
                # Hive-style partitioning with key=value directories
                resolved = os.path.join(path, "**", "*.parquet")
                options["hive_partitioning"] = True
            elif subdirs and not has_parquet_files:
                # Directory has subdirectories but no parquet files at top level
                # Use recursive glob to find parquet files in subdirectories
                resolved = os.path.join(path, "**", "*.parquet")
            elif has_parquet_files:
                # Flat directory with parquet files at top level
                resolved = os.path.join(path, "*.parquet")
            else:
                # Fallback - try recursive
                resolved = os.path.join(path, "**", "*.parquet")
        except OSError:
            # If we can't read the directory, use recursive glob
            resolved = os.path.join(path, "**", "*.parquet")

    # If path contains hive-style markers and hive_partitioning not explicitly set
    # Check path components (directories) for hive-style key=value patterns
    # Exclude glob patterns and the final filename from the check
    if hive_partitioning is None:
        path_parts = resolved.replace("\\", "/").split("/")
        # Check directory components (not filename or glob patterns like ** or *.parquet)
        dir_parts = [p for p in path_parts[:-1] if p and p not in ("**", "*")]
        has_hive_dirs = any("=" in part for part in dir_parts)
        if has_hive_dirs:
            options["hive_partitioning"] = True
    elif hive_partitioning is not None:
        options["hive_partitioning"] = hive_partitioning

    return resolved, options


def get_first_parquet_file(partition_path: str) -> str | None:
    """
    Get the first parquet file from a partitioned dataset.

    Used for metadata inspection when only need to check one file.

    Args:
        partition_path: Directory path or glob pattern

    Returns:
        str: Path to first parquet file, or None if none found
    """
    import glob as glob_module

    if is_remote_url(partition_path):
        # For remote, can't easily enumerate - return original path
        # Caller should handle this case
        return partition_path

    if os.path.isdir(partition_path):
        # Walk directory to find first parquet file
        for root, _dirs, files in os.walk(partition_path):
            for f in sorted(files):  # Sort for consistent ordering
                if f.endswith(".parquet"):
                    return os.path.join(root, f)
        return None

    if has_glob_pattern(partition_path):
        # Use glob to find first match
        matches = glob_module.glob(partition_path, recursive=True)
        parquet_matches = [m for m in sorted(matches) if m.endswith(".parquet")]
        return parquet_matches[0] if parquet_matches else None

    # Single file
    return partition_path


def get_all_parquet_files(partition_path: str) -> list[str]:
    """
    Get all parquet files from a partitioned dataset.

    Args:
        partition_path: Directory path or glob pattern

    Returns:
        list: List of paths to all parquet files, sorted for consistent ordering
    """
    import glob as glob_module

    if is_remote_url(partition_path):
        # For remote, can't easily enumerate - return as single item
        return [partition_path]

    if os.path.isdir(partition_path):
        # Walk directory to find all parquet files
        parquet_files = []
        for root, _dirs, files in os.walk(partition_path):
            for f in files:
                if f.endswith(".parquet"):
                    parquet_files.append(os.path.join(root, f))
        return sorted(parquet_files)

    if has_glob_pattern(partition_path):
        # Use glob to find all matches
        matches = glob_module.glob(partition_path, recursive=True)
        return sorted([m for m in matches if m.endswith(".parquet")])

    # Single file
    return [partition_path] if os.path.exists(partition_path) else []


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
        progress(f"Uploading {size_mb:.1f} MB to {remote_path}...")

    pattern = "*.parquet" if is_directory else None
    upload(
        source=Path(local_path),
        destination=remote_path,
        profile=profile,
        pattern=pattern,
        dry_run=False,
    )

    if verbose:
        success(f"✓ Successfully uploaded to {remote_path}")

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
            debug(f"Remote output detected: {output_path}")
            debug(f"Writing to temporary {'directory' if is_directory else 'file'}: {temp_path}")
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
                    debug(
                        f"Cleaned up temporary {'directory' if is_directory else 'file'}: {temp_path}"
                    )
            except Exception as e:
                if verbose:
                    warn(
                        f"Could not clean up temp {'directory' if is_directory else 'file'} {temp_path}: {e}"
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
        info(f"📡 Reading from {protocol}: {file_path}")
    else:
        info(f"📡 Reading from {protocol} (network operations may take time)...")


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


def validate_parquet_extension(output_file: str, any_extension: bool = False) -> None:
    """
    Validate that output file has .parquet extension.

    By default, gpio commands that write parquet files require the output
    to have a .parquet extension to prevent accidental misuse (e.g., writing
    a parquet file with .geojson extension).

    Args:
        output_file: Output file path (local or remote)
        any_extension: If True, skip validation and allow any extension

    Raises:
        click.ClickException: If extension is not .parquet and any_extension=False
    """
    # Skip for streaming output or no output specified
    if output_file is None or output_file == "-":
        return

    # User explicitly allowed any extension
    if any_extension:
        return

    # Extract the filename from the path (handles both local and remote URLs)
    if "://" in output_file:
        # Remote URL: extract path portion after protocol://bucket/
        path_part = output_file.split("://", 1)[1]
        filename = path_part.split("/")[-1] if "/" in path_part else path_part
    else:
        filename = os.path.basename(output_file)

    # Check extension (case-insensitive)
    _, ext = os.path.splitext(filename)
    if ext.lower() != ".parquet":
        raise click.ClickException(
            f"Output file '{output_file}' does not have .parquet extension. "
            f"Use --any-extension to allow non-standard extensions."
        )


def get_duckdb_connection(load_spatial=True, load_httpfs=None, use_s3_auth=False):
    """
    Create a DuckDB connection with necessary extensions loaded.

    By default, S3 access uses no credentials, which works for public buckets.
    DuckDB automatically handles region detection for public S3 buckets.

    When use_s3_auth=True, loads the aws extension and configures credential
    discovery for private S3 buckets.

    Args:
        load_spatial: Whether to load spatial extension (default: True)
        load_httpfs: Whether to load httpfs extension for S3/Azure/GCS.
                    If None (default), auto-detects based on usage.
        use_s3_auth: Whether to configure AWS credential chain for S3 (default: False).
                    Only needed for private buckets.

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

        # Only configure AWS credentials if explicitly requested (for private buckets)
        # Public buckets work without any secret - DuckDB handles them automatically
        if use_s3_auth:
            con.execute("INSTALL aws;")
            con.execute("LOAD aws;")
            con.execute("""
                CREATE OR REPLACE SECRET (
                    TYPE s3,
                    PROVIDER credential_chain,
                    VALIDATION 'none'
                );
            """)

    return con


def get_duckdb_connection_for_s3(
    path: str,
    load_spatial: bool = True,
) -> duckdb.DuckDBPyConnection:
    """
    Get DuckDB connection configured for S3 access.

    For S3 paths, uses no credentials by default (works for public buckets).
    If a bucket is known to require auth (from previous attempts), uses
    credential chain. Results are cached per bucket.

    Args:
        path: S3 path to access (used to determine bucket and access mode)
        load_spatial: Whether to load spatial extension (default: True)

    Returns:
        duckdb.DuckDBPyConnection: Configured connection with appropriate S3 access
    """
    # Non-S3 paths: use standard connection
    if not path.startswith(("s3://", "s3a://")):
        return get_duckdb_connection(load_spatial=load_spatial, load_httpfs=needs_httpfs(path))

    bucket = _extract_bucket_name(path)

    # If we know this bucket needs auth, use credential chain
    if bucket in _s3_buckets_needing_auth:
        return get_duckdb_connection(load_spatial=load_spatial, load_httpfs=True, use_s3_auth=True)

    # Try without credentials first (works for public buckets)
    con = get_duckdb_connection(load_spatial=load_spatial, load_httpfs=True, use_s3_auth=False)
    try:
        # Lightweight test query - DuckDB handles glob patterns natively
        con.execute(f"SELECT 1 FROM read_parquet('{path}') LIMIT 1").fetchone()
        return con
    except Exception as e:
        con.close()
        if _needs_s3_auth(e):
            # This bucket requires authentication - cache and retry
            _s3_buckets_needing_auth.add(bucket)
            return get_duckdb_connection(
                load_spatial=load_spatial, load_httpfs=True, use_s3_auth=True
            )
        raise


def safe_file_url(file_path, verbose=False):
    """
    Handle both local and remote files, returning safe URL.

    For remote URLs, performs URL encoding if needed.
    For local files, validates existence (unless it's a glob pattern).

    Args:
        file_path: Local file path or remote URL (may contain glob patterns)
        verbose: Whether to print verbose output

    Returns:
        str: Safe URL or file path

    Raises:
        click.BadParameter: If local file doesn't exist (non-glob paths only)
    """
    if is_remote_url(file_path):
        # Remote URL - URL encode if HTTP/HTTPS
        if file_path.startswith(("http://", "https://")):
            parsed = urllib.parse.urlparse(file_path)
            # Preserve glob wildcards and hive-style partition markers for DuckDB
            # These characters must not be encoded: * ? [ ] = , /
            duckdb_safe_chars = "/*?[]=,"
            encoded_path = urllib.parse.quote(parsed.path, safe=duckdb_safe_chars)
            safe_url = parsed._replace(path=encoded_path).geturl()
        else:
            safe_url = file_path

        if verbose:
            protocol = file_path.split("://")[0].upper() if "://" in file_path else "HTTP"
            debug(f"Reading from {protocol}: {safe_url}")
        return safe_url
    else:
        # Local file - check existence (skip for glob patterns, DuckDB will handle)
        if not has_glob_pattern(file_path) and not os.path.exists(file_path):
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
    """
    Get Parquet file metadata using DuckDB for kv_metadata and PyArrow for schema.

    For partitioned datasets (directories or glob patterns), reads metadata
    from the first file.

    Returns:
        tuple: (kv_metadata dict, PyArrow schema)

    Note: Uses DuckDB for metadata extraction but returns PyArrow schema for
    backward compatibility with code that expects schema.field() methods.
    """
    import pyarrow.parquet as pq

    from geoparquet_io.core.duckdb_metadata import get_kv_metadata

    # For partitions, use first file for metadata
    file_to_check = parquet_file
    if is_partition_path(parquet_file):
        first_file = get_first_parquet_file(parquet_file)
        if first_file:
            file_to_check = first_file

    safe_url = safe_file_url(file_to_check, verbose=False)

    # Get key-value metadata (returns dict like {b'geo': b'...'})
    kv_metadata = get_kv_metadata(safe_url)

    # Get PyArrow schema for backward compatibility
    # (some code uses schema.field(i).name patterns)
    if is_remote_url(file_to_check):
        # For remote files, use a DuckDB-based approach to read schema
        from geoparquet_io.core.duckdb_metadata import get_schema_info

        schema_info = get_schema_info(safe_url)
        # Create a simple object that mimics PyArrow schema for basic usage
        schema = _DuckDBSchemaWrapper(schema_info)
    else:
        pf = pq.ParquetFile(file_to_check)
        schema = pf.schema_arrow

    if verbose and kv_metadata:
        debug("\nParquet metadata key-value pairs:")
        for key, value in kv_metadata.items():
            key_str = key.decode("utf-8") if isinstance(key, bytes) else key
            debug(f"{key_str}: {value}")

    return kv_metadata, schema


class _DuckDBSchemaWrapper:
    """Wrapper to provide PyArrow-like interface for DuckDB schema info."""

    def __init__(self, schema_info):
        self._columns = [c for c in schema_info if c.get("name") and "." not in c.get("name", "")]

    def __len__(self):
        return len(self._columns)

    def field(self, i):
        return _DuckDBFieldWrapper(self._columns[i])


class _DuckDBFieldWrapper:
    """Wrapper to provide PyArrow-like interface for a DuckDB column."""

    def __init__(self, col_info):
        self.name = col_info.get("name", "")


def parse_geo_metadata(metadata, verbose=False):
    """Parse GeoParquet metadata from Parquet metadata."""
    if not metadata or b"geo" not in metadata:
        return None

    try:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        if verbose:
            debug("\nParsed geo metadata:")
            debug(json.dumps(geo_meta, indent=2))
        return geo_meta
    except json.JSONDecodeError:
        if verbose:
            warn("Failed to parse geo metadata as JSON")
        return None


def find_primary_geometry_column(parquet_file, verbose=False):
    """
    Find the primary geometry column from GeoParquet metadata.

    Looks up the geometry column name from GeoParquet metadata. Falls back
    to 'geometry' if no metadata is present or if the primary column is
    not specified.

    Args:
        parquet_file: Path to the parquet file (local or remote URL)
        verbose: Print verbose output

    Returns:
        str: Name of the primary geometry column (defaults to 'geometry')
    """
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata

    safe_url = safe_file_url(parquet_file, verbose=False)
    geo_meta = get_geo_metadata(safe_url)

    if verbose and geo_meta:
        debug(f"\nGeo metadata: {json.dumps(geo_meta, indent=2)}")

    if not geo_meta:
        return "geometry"

    if isinstance(geo_meta, dict):
        return geo_meta.get("primary_column", "geometry")
    elif isinstance(geo_meta, list):
        for col in geo_meta:
            if isinstance(col, dict) and col.get("primary", False):
                return col.get("name", "geometry")

    return "geometry"


def calculate_file_bounds(file_path, geom_column=None, verbose=False):
    """
    Calculate the bounding box of all geometries in a parquet file.

    Uses DuckDB's spatial extension to compute the extent of all geometries.

    Args:
        file_path: Path to the parquet file (local or remote URL)
        geom_column: Name of geometry column (auto-detected if None)
        verbose: Print verbose output

    Returns:
        tuple: (xmin, ymin, xmax, ymax) or None if calculation fails
    """
    if geom_column is None:
        geom_column = find_primary_geometry_column(file_path, verbose=False)

    safe_url = safe_file_url(file_path, verbose=False)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(file_path))

    try:
        # Quote column name to handle special characters, uppercase, etc.
        quoted_geom = geom_column.replace('"', '""')
        bounds_query = f"""
            SELECT
                MIN(ST_XMin("{quoted_geom}")) as xmin,
                MIN(ST_YMin("{quoted_geom}")) as ymin,
                MAX(ST_XMax("{quoted_geom}")) as xmax,
                MAX(ST_YMax("{quoted_geom}")) as ymax
            FROM read_parquet('{safe_url}')
        """
        result = con.execute(bounds_query).fetchone()

        if result and all(v is not None for v in result):
            if verbose:
                debug(
                    f"Calculated bounds: ({result[0]:.6f}, {result[1]:.6f}, "
                    f"{result[2]:.6f}, {result[3]:.6f})"
                )
            return result
        return None
    except Exception as e:
        if verbose:
            debug(f"Failed to calculate bounds: {e}")
        return None
    finally:
        con.close()


# CRS handling functions for GeoParquet 2.0 and parquet-geo-only


def _extract_crs_identifier(crs_info):
    """
    Extract normalized CRS identifier (authority, code) from various formats.

    Handles PROJJSON dicts, "EPSG:CODE" strings, and URN formats.
    Returns tuple of (authority, code) like ("EPSG", 31287) or ("OGC", "CRS84"), or None.
    Code is int for numeric codes, str for non-numeric (e.g., CRS84).
    """
    if isinstance(crs_info, dict):
        if "id" in crs_info:
            crs_id = crs_info["id"]
            if isinstance(crs_id, dict):
                authority = crs_id.get("authority", "").upper()
                code = crs_id.get("code")
                if authority and code:
                    # Try to convert to int, but keep as string if not numeric
                    try:
                        return (authority, int(code))
                    except (ValueError, TypeError):
                        return (authority, str(code).upper())
        return None

    if isinstance(crs_info, str):
        crs_str = crs_info.strip().upper()
        if ":" in crs_str and not crs_str.startswith("URN:"):
            parts = crs_str.split(":")
            if len(parts) == 2:
                try:
                    return (parts[0], int(parts[1]))
                except ValueError:
                    # Non-numeric code (e.g., OGC:CRS84)
                    return (parts[0], parts[1])
        if crs_str.startswith("URN:OGC:DEF:CRS:"):
            parts = crs_str.split(":")
            if len(parts) >= 7:
                try:
                    return (parts[4], int(parts[-1]))
                except ValueError:
                    return (parts[4], parts[-1])

    return None


def is_default_crs(crs):
    """
    Check if CRS is the default (OGC:CRS84 or EPSG:4326).

    Returns True if CRS is None, empty, or represents WGS84.
    Used to skip CRS rewriting when output would be default anyway.
    """
    if not crs:
        return True

    identifier = _extract_crs_identifier(crs)
    if identifier:
        authority, code = identifier
        if authority == "EPSG" and code == 4326:
            return True
        if authority == "OGC" and str(code).upper() == "CRS84":
            return True

    return False


def extract_crs_from_parquet(parquet_file, verbose=False):
    """
    Extract CRS (as PROJJSON dict) from a Parquet file.

    Checks in order:
    1. GeoParquet metadata (columns.<geom_col>.crs)
    2. Parquet native geo type (from schema logical_type)

    Args:
        parquet_file: Path to the parquet file
        verbose: Whether to print verbose output

    Returns:
        dict: PROJJSON CRS dict, or None if no CRS found or CRS is default
    """
    from geoparquet_io.core.duckdb_metadata import (
        get_geo_metadata,
        get_schema_info,
        parse_geometry_logical_type,
        resolve_crs_reference,
    )

    safe_url = safe_file_url(parquet_file, verbose=False)

    # First, try GeoParquet metadata
    geo_meta = get_geo_metadata(safe_url)
    if geo_meta:
        primary_col = geo_meta.get("primary_column", "geometry")
        columns = geo_meta.get("columns", {})
        if primary_col in columns:
            crs = columns[primary_col].get("crs")
            if crs and not is_default_crs(crs):
                if verbose:
                    debug(f"Found CRS in GeoParquet metadata: {_format_crs_display(crs)}")
                return crs

    # Second, try Parquet native geo type from schema logical_type
    # DuckDB returns GeometryType(...) and GeographyType(...) from parquet_schema()
    schema_info = get_schema_info(safe_url)
    for col in schema_info:
        logical_type = col.get("logical_type", "")
        if logical_type and (
            logical_type.startswith("GeometryType(") or logical_type.startswith("GeographyType(")
        ):
            parsed = parse_geometry_logical_type(logical_type)
            if parsed and "crs" in parsed:
                raw_crs = parsed["crs"]
                # Resolve CRS references (projjson:key_name, srid:XXXX) to PROJJSON
                crs = resolve_crs_reference(parquet_file, raw_crs)
                if crs and not is_default_crs(crs):
                    if verbose:
                        debug(f"Found CRS in Parquet geo type: {_format_crs_display(crs)}")
                    return crs

    return None


def detect_crs_from_spatial_file(input_file, con, verbose=False):
    """
    Detect CRS from a spatial file (GeoJSON, GPKG, Shapefile).

    Uses DuckDB's ST_Read_Meta which returns metadata including full PROJJSON CRS.

    Args:
        input_file: Path to the spatial file
        con: DuckDB connection (with spatial extension loaded)
        verbose: Whether to print verbose output

    Returns:
        dict: PROJJSON CRS dict, or None if no CRS found in file metadata.
              Note: Returns CRS even if it's default (EPSG:4326). Caller should
              use is_default_crs() to decide whether to write it.
    """
    try:
        result = con.execute(f"""
            SELECT * FROM ST_Read_Meta('{input_file}')
        """).fetchone()

        if result:
            # Result structure: (path, driver, driver_long, layers_list)
            layers = result[3]  # List of layer dicts
            if layers and len(layers) > 0:
                layer = layers[0]
                geometry_fields = layer.get("geometry_fields", [])
                if geometry_fields:
                    crs_info = geometry_fields[0].get("crs", {})
                    # Extract PROJJSON if available
                    projjson_str = crs_info.get("projjson")
                    if projjson_str:
                        crs = json.loads(projjson_str)
                        if verbose:
                            debug(f"Found CRS in spatial file: {_format_crs_display(crs)}")
                        return crs
                    # Fallback to auth_name/auth_code
                    auth_name = crs_info.get("auth_name")
                    auth_code = crs_info.get("auth_code")
                    if auth_name and auth_code:
                        crs = {"id": {"authority": auth_name, "code": int(auth_code)}}
                        if verbose:
                            debug(f"Found CRS: {auth_name}:{auth_code}")
                        return crs
    except Exception as e:
        if verbose:
            warn(f"Could not detect CRS from spatial file: {e}")

    return None


def _format_crs_display(crs):
    """Format CRS for display (extract EPSG code if possible)."""
    if not crs:
        return "None"
    identifier = _extract_crs_identifier(crs)
    if identifier:
        return f"{identifier[0]}:{identifier[1]}"
    return str(crs)[:50] + "..." if len(str(crs)) > 50 else str(crs)


def get_crs_display_name(crs_info: dict | str | None) -> str:
    """
    Get human-readable CRS name with authority code.

    Handles PROJJSON dicts, string CRS identifiers, and None.

    Returns:
        Human-readable string like "WGS 84 (EPSG:4326)" or "EPSG:4326" or "unknown"
    """
    if crs_info is None:
        return "None (OGC:CRS84)"

    if isinstance(crs_info, str):
        return crs_info

    if isinstance(crs_info, dict):
        name = crs_info.get("name", "")
        crs_id = crs_info.get("id", {})
        if isinstance(crs_id, dict):
            authority = crs_id.get("authority", "EPSG")
            code = crs_id.get("code")
            if code:
                return f"{name} ({authority}:{code})" if name else f"{authority}:{code}"
        if name:
            return name
        # Fallback for PROJJSON without id or name
        return "PROJJSON object"

    return "unknown"


def is_geographic_crs(crs: dict | str | None) -> bool:
    """
    Check if CRS is geographic (lat/lon) vs projected.

    Handles PROJJSON dicts, string identifiers, and None.
    None is treated as OGC:CRS84 (geographic).

    Returns:
        True if CRS is geographic, False if projected
    """
    if crs is None:
        return True  # Default is OGC:CRS84

    if isinstance(crs, dict):
        # Check PROJJSON type field first - most reliable
        crs_type = crs.get("type", "").lower()
        if crs_type == "geographiccrs":
            return True
        if crs_type == "projectedcrs":
            return False

        # Check for EPSG:4326 or OGC:CRS84
        crs_id = crs.get("id", {})
        if isinstance(crs_id, dict):
            authority = crs_id.get("authority", "").upper()
            code = crs_id.get("code")
            if authority == "EPSG" and code == 4326:
                return True
            if authority == "OGC" and str(code).upper() == "CRS84":
                return True

        # Check name for common patterns
        name = crs.get("name", "").upper()
        projected_indicators = ["UTM", "ZONE", "MERCATOR", "ALBERS", "LAMBERT", "STATE PLANE"]
        if any(indicator in name for indicator in projected_indicators):
            return False
        if any(x in name for x in ["WGS 84", "WGS84", "CRS84", "4326"]):
            return True

    if isinstance(crs, str):
        crs_upper = crs.upper()
        # Check for projected indicators in string CRS
        projected_indicators = ["UTM", "ZONE", "MERCATOR", "ALBERS", "LAMBERT"]
        if any(indicator in crs_upper for indicator in projected_indicators):
            return False
        return any(x in crs_upper for x in ["4326", "CRS84", "WGS84"])

    return False


def apply_crs_to_parquet(
    parquet_file,
    crs,
    geometry_column="geometry",
    compression="ZSTD",
    compression_level=15,
    row_group_rows=None,
    add_to_geo_metadata=False,
    verbose=False,
):
    """
    Rewrite a Parquet file to add CRS to the geometry column.

    Uses geoarrow-pyarrow to set CRS on the geometry column type,
    which writes it into the Parquet schema. Optionally also adds CRS
    to GeoParquet 'geo' metadata in the same write operation.

    Args:
        parquet_file: Path to the parquet file to rewrite
        crs: PROJJSON dict with CRS information
        geometry_column: Name of the geometry column
        compression: Compression type for output
        compression_level: Compression level
        row_group_rows: Number of rows per row group
        add_to_geo_metadata: If True, also add CRS to GeoParquet 'geo' metadata (for v2.0)
        verbose: Whether to print verbose output
    """
    import geoarrow.pyarrow as ga
    import pyarrow as pa

    if verbose:
        target = (
            "Parquet schema and GeoParquet metadata" if add_to_geo_metadata else "Parquet schema"
        )
        debug(f"Applying CRS {_format_crs_display(crs)} to {target}...")

    # Read the file
    table = pq.read_table(parquet_file)

    # Get the geometry column and convert to WKB format with geoarrow
    # Using as_wkb preserves binary WKB and writes proper Parquet GEOMETRY type
    geom_col = table.column(geometry_column)
    wkb_arr = ga.as_wkb(geom_col)

    # Create new type with CRS
    new_type = wkb_arr.type.with_crs(crs)

    # Create new chunks with the CRS type using from_storage
    # This preserves the CRS unlike cast() which resets it
    new_chunks = []
    for chunk in wkb_arr.chunks:
        new_chunk = pa.ExtensionArray.from_storage(new_type, chunk.storage)
        new_chunks.append(new_chunk)

    geom_with_crs = pa.chunked_array(new_chunks, type=new_type)

    # Replace the column in the table
    col_index = table.schema.get_field_index(geometry_column)
    new_table = table.set_column(col_index, geometry_column, geom_with_crs)

    # If requested, also add CRS to GeoParquet metadata (for v2.0)
    if add_to_geo_metadata:
        metadata = new_table.schema.metadata or {}

        # Parse existing geo metadata or create new
        if b"geo" in metadata:
            geo_meta = json.loads(metadata[b"geo"])
        else:
            geo_meta = {"version": "2.0.0", "primary_column": geometry_column, "columns": {}}

        # Add CRS to geometry column in metadata
        geom_col_name = geo_meta.get("primary_column", geometry_column)
        if geom_col_name not in geo_meta["columns"]:
            geo_meta["columns"][geom_col_name] = {}
        geo_meta["columns"][geom_col_name]["crs"] = crs

        # Update table metadata
        new_metadata = dict(metadata)
        new_metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
        new_table = new_table.replace_schema_metadata(new_metadata)

    # Use central configuration for write settings
    settings = ParquetWriteSettings(
        compression=compression, compression_level=compression_level, row_group_rows=row_group_rows
    )
    write_kwargs = settings.get_pyarrow_kwargs()

    # Write back with all best practices (single write!)
    pq.write_table(new_table, parquet_file, **write_kwargs)

    if verbose:
        success("CRS applied successfully")


def add_crs_to_geoparquet_metadata(
    parquet_file, crs, compression="ZSTD", compression_level=15, row_group_rows=None, verbose=False
):
    """
    Add CRS to GeoParquet 'geo' metadata.

    NOTE: This function is kept for compatibility but is no longer used internally.
    For GeoParquet 2.0, use apply_crs_to_parquet() with add_to_geo_metadata=True
    to write CRS to both Parquet schema and geo metadata in a single write operation.

    Args:
        parquet_file: Path to the parquet file
        crs: PROJJSON dict with CRS information
        compression: Compression type
        compression_level: Compression level
        row_group_rows: Number of rows per row group
        verbose: Whether to print verbose output
    """
    if verbose:
        debug(f"Adding CRS to GeoParquet metadata: {_format_crs_display(crs)}")

    table = pq.read_table(parquet_file)
    metadata = table.schema.metadata or {}

    # Parse existing geo metadata or create new
    if b"geo" in metadata:
        geo_meta = json.loads(metadata[b"geo"])
    else:
        geo_meta = {"version": "2.0.0", "primary_column": "geometry", "columns": {}}

    # Add CRS to geometry column
    geom_col = geo_meta.get("primary_column", "geometry")
    if geom_col not in geo_meta["columns"]:
        geo_meta["columns"][geom_col] = {}
    geo_meta["columns"][geom_col]["crs"] = crs

    # Write back with same compression settings
    new_metadata = dict(metadata)
    new_metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
    new_table = table.replace_schema_metadata(new_metadata)

    # Use central configuration for write settings
    settings = ParquetWriteSettings(
        compression=compression, compression_level=compression_level, row_group_rows=row_group_rows
    )
    write_kwargs = settings.get_pyarrow_kwargs()
    pq.write_table(new_table, parquet_file, **write_kwargs)

    if verbose:
        success("CRS added to GeoParquet metadata")


def parse_crs_string_to_projjson(crs_string, con=None):
    """
    Convert a CRS string (like "EPSG:5070") to full PROJJSON dict.

    Uses pyproj to generate the complete PROJJSON definition including
    all CRS parameters, not just the authority/code.

    Args:
        crs_string: CRS string like "EPSG:5070" or "EPSG:4326"
        con: DuckDB connection (optional, unused but kept for API compatibility)

    Returns:
        dict: Full PROJJSON dict, or simple id dict if lookup fails
    """
    identifier = _extract_crs_identifier(crs_string)
    if not identifier:
        return None

    authority, code = identifier

    try:
        from pyproj import CRS

        # Create CRS from authority:code and get full PROJJSON
        crs = CRS.from_authority(authority, code)
        return crs.to_json_dict()
    except Exception:
        # Fallback to simple id dict if pyproj fails
        return {"id": {"authority": authority, "code": code}}


def _parse_existing_geo_metadata(original_metadata):
    """Parse existing geo metadata from original parquet metadata."""
    if not original_metadata or b"geo" not in original_metadata:
        return None
    try:
        return json.loads(original_metadata[b"geo"].decode("utf-8"))
    except json.JSONDecodeError:
        return None


def _initialize_geo_metadata(geo_meta, geom_col, version="1.1.0"):
    """Initialize or upgrade geo metadata structure.

    Args:
        geo_meta: Existing geo metadata dict or None
        geom_col: Name of the geometry column
        version: GeoParquet version string (e.g., "1.0.0", "1.1.0", "2.0.0")

    Returns:
        dict: Initialized geo metadata structure
    """
    if not geo_meta:
        return {"version": version, "primary_column": geom_col, "columns": {geom_col: {}}}

    # Set the specified version
    geo_meta["version"] = version
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
        debug(f"Added bbox covering metadata for column '{bbox_info['bbox_column_name']}'")


def _add_custom_covering(geo_meta, geom_col, custom_metadata, verbose):
    """Add custom covering metadata (e.g., H3, S2)."""
    if not custom_metadata or "covering" not in custom_metadata:
        return

    if "covering" not in geo_meta["columns"][geom_col]:
        geo_meta["columns"][geom_col]["covering"] = {}

    geo_meta["columns"][geom_col]["covering"].update(custom_metadata["covering"])
    if verbose:
        for key in custom_metadata["covering"]:
            debug(f"Added {key} covering metadata")


def create_geo_metadata(
    original_metadata,
    geom_col,
    bbox_info,
    custom_metadata=None,
    verbose=False,
    version="1.1.0",
):
    """
    Create or update GeoParquet metadata with spatial index covering information.

    Args:
        original_metadata: Original parquet metadata dict
        geom_col: Name of the geometry column
        bbox_info: Result from check_bbox_structure
        custom_metadata: Optional dict with custom metadata (e.g., H3 info)
        verbose: Whether to print verbose output
        version: GeoParquet version string (e.g., "1.0.0", "1.1.0", "2.0.0")

    Returns:
        dict: Updated geo metadata
    """
    geo_meta = _parse_existing_geo_metadata(original_metadata)
    geo_meta = _initialize_geo_metadata(geo_meta, geom_col, version=version)

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
            warn(
                f"Note: {compression} does not support compression levels. Ignoring level {compression_level}."
            )
        compression_level = None  # These formats don't use compression levels
        compression_desc = compression
    else:
        compression_level = None  # UNCOMPRESSED doesn't use levels
        compression_desc = compression

    return compression, compression_level, compression_desc


# =============================================================================
# Arrow-based write helpers
# =============================================================================


def _get_query_columns(con, query: str) -> list[str]:
    """
    Get column names from a query without executing it fully.

    Uses LIMIT 0 to get schema information efficiently.

    Args:
        con: DuckDB connection
        query: SQL SELECT query

    Returns:
        list[str]: Column names from the query result
    """
    describe_query = f"SELECT * FROM ({query}) AS __subq LIMIT 0"
    result = con.execute(describe_query)
    return [col[0] for col in result.description]


def _detect_geometry_from_query(
    con,
    query: str,
    original_metadata: dict | None,
    verbose: bool = False,
) -> str:
    """
    Detect geometry column from metadata or query schema.

    Priority:
    1. GeoParquet metadata primary_column
    2. Common geometry column names in query schema
    3. Default to 'geometry'

    Args:
        con: DuckDB connection
        query: SQL SELECT query
        original_metadata: Original metadata dict (may contain geo metadata)
        verbose: Whether to print verbose output

    Returns:
        str: Name of the geometry column
    """
    # Try from original metadata first
    if original_metadata and b"geo" in original_metadata:
        try:
            geo_meta = json.loads(original_metadata[b"geo"].decode("utf-8"))
            if isinstance(geo_meta, dict) and "primary_column" in geo_meta:
                if verbose:
                    debug(f"Detected geometry column from metadata: {geo_meta['primary_column']}")
                return geo_meta["primary_column"]
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    # Detect from query schema
    try:
        columns = _get_query_columns(con, query)
        common_names = ["geometry", "geom", "wkb_geometry", "shape", "the_geom"]
        for name in common_names:
            # Case-insensitive match
            for col in columns:
                if col.lower() == name.lower():
                    if verbose:
                        debug(f"Detected geometry column from schema: {col}")
                    return col
    except (duckdb.Error, RuntimeError, ValueError, AttributeError) as e:
        if verbose:
            debug(f"Could not detect geometry column from query schema: {e}")

    # Default
    return "geometry"


def _wrap_query_with_wkb_conversion(query: str, geometry_column: str) -> str:
    """
    Wrap query to convert geometry column to WKB for Arrow export.

    DuckDB's GEOMETRY type doesn't translate directly to Arrow in a portable way.
    This wraps the query to use ST_AsWKB for geometry output, ensuring the geometry
    is in standard WKB format that geoarrow-pyarrow can handle.

    Args:
        query: Original SQL SELECT query
        geometry_column: Name of the geometry column to convert

    Returns:
        str: Wrapped query with WKB conversion
    """
    # Quote column name to handle special characters
    quoted_geom = geometry_column.replace('"', '""')

    return f"""
        WITH __arrow_source AS ({query})
        SELECT * REPLACE (ST_AsWKB("{quoted_geom}") AS "{quoted_geom}")
        FROM __arrow_source
    """


def _rebuild_array_with_type(
    chunked_array,
    new_type,
):
    """
    Rebuild a chunked array with a new extension type.

    This preserves CRS and other type metadata, unlike cast() which may reset them.
    Used when applying CRS to geoarrow geometry arrays.

    Args:
        chunked_array: PyArrow ChunkedArray to rebuild
        new_type: New PyArrow ExtensionType to apply

    Returns:
        pa.ChunkedArray: New chunked array with the new type
    """
    import pyarrow as pa

    new_chunks = []
    for chunk in chunked_array.chunks:
        new_chunk = pa.ExtensionArray.from_storage(new_type, chunk.storage)
        new_chunks.append(new_chunk)

    return pa.chunked_array(new_chunks, type=new_type)


def _detect_bbox_column_from_table(table, verbose: bool = False) -> str | None:
    """
    Detect bbox struct column from Arrow table schema.

    Looks for columns with conventional names (bbox, bounds, extent) that have
    the required struct fields (xmin, ymin, xmax, ymax).

    Args:
        table: PyArrow Table to check
        verbose: Whether to print verbose output

    Returns:
        str: Name of bbox column if found, None otherwise
    """
    import pyarrow as pa

    conventional_suffixes = ["bbox", "bounds", "extent"]
    required_fields = {"xmin", "ymin", "xmax", "ymax"}

    for field in table.schema:
        name = field.name
        field_type = field.type

        # Check if column name ends with conventional suffixes
        is_bbox_name = any(name.endswith(suffix) for suffix in conventional_suffixes)
        if not is_bbox_name:
            continue

        # Check if it's a struct with the required fields
        if pa.types.is_struct(field_type):
            struct_field_names = {f.name for f in field_type}
            if required_fields.issubset(struct_field_names):
                if verbose:
                    debug(f"Found bbox column in table: {name}")
                return name

    return None


# WKB geometry type codes to GeoParquet base names (2D types)
_GEOMETRY_TYPE_CODES = {
    0: "Unknown",
    1: "Point",
    2: "LineString",
    3: "Polygon",
    4: "MultiPoint",
    5: "MultiLineString",
    6: "MultiPolygon",
    7: "GeometryCollection",
}

# Dimensional suffixes based on WKB type code modifier
_DIMENSION_SUFFIXES = {
    0: "",  # 2D (no suffix)
    1: " Z",  # Z dimension (codes 1001-1007)
    2: " M",  # M dimension (codes 2001-2007)
    3: " ZM",  # ZM dimensions (codes 3001-3007)
}


def _get_geometry_type_name(code: int) -> str:
    """
    Convert WKB geometry type code to GeoParquet geometry type name.

    Handles 2D types (0-7) and Z/M/ZM variants (1001-1007, 2001-2007, 3001-3007).

    Args:
        code: WKB geometry type code

    Returns:
        GeoParquet geometry type name (e.g., "Point", "Point Z", "Polygon ZM")
    """
    # Extract base type (0-7) and dimensional modifier (0, 1, 2, or 3)
    base_type = code % 1000
    dimension = code // 1000

    base_name = _GEOMETRY_TYPE_CODES.get(base_type, "Unknown")
    if base_name == "Unknown":
        return "Unknown"

    suffix = _DIMENSION_SUFFIXES.get(dimension, "")
    return base_name + suffix


def _is_geoarrow_extension_type(arrow_type) -> bool:
    """Check if an Arrow type is a geoarrow extension type."""
    if hasattr(arrow_type, "extension_name"):
        return arrow_type.extension_name.startswith("geoarrow")
    return False


def _strip_geoarrow_to_plain_wkb(table, geometry_column: str, verbose: bool):
    """
    Convert geoarrow extension type back to plain binary WKB.

    Used for GeoParquet 1.x output which uses plain binary geometry
    with CRS only in metadata (not in schema).
    """
    import pyarrow as pa

    geom_col = table.column(geometry_column)
    geom_type = geom_col.type

    # Check if it's a geoarrow extension type
    if not _is_geoarrow_extension_type(geom_type):
        return table  # Already plain binary

    if verbose:
        debug("v1.x: stripping geoarrow extension type to plain binary WKB")

    try:
        # Extract storage (plain binary) from extension type
        new_chunks = []
        for chunk in geom_col.chunks:
            if hasattr(chunk, "storage"):
                new_chunks.append(chunk.storage)
            else:
                new_chunks.append(chunk)

        # Create new binary column
        plain_col = pa.chunked_array(new_chunks, type=pa.binary())

        # Replace in table
        col_index = table.schema.get_field_index(geometry_column)
        return table.set_column(col_index, geometry_column, plain_col)

    except (TypeError, ValueError, AttributeError) as e:
        if verbose:
            debug(f"Could not strip geoarrow type: {e}")
        return table


def _process_geometry_column_for_version(
    table,
    geometry_column: str,
    geoparquet_version: str | None,
    input_crs: dict | None,
    verbose: bool,
):
    """
    Process geometry column based on GeoParquet version.

    Handles different GeoParquet versions:
    - v1.x: Plain binary WKB (no extension type), CRS only in metadata
    - v2.0/parquet-geo-only: geoarrow extension type with CRS in schema

    When streaming data enters with geoarrow extension type:
    - v1.x output: strips geoarrow to plain binary WKB
    - v2.0 output: preserves/enhances geoarrow extension type

    Args:
        table: PyArrow Table to modify
        geometry_column: Name of the geometry column
        geoparquet_version: GeoParquet version
        input_crs: PROJJSON dict with CRS
        verbose: Whether to print verbose output

    Returns:
        pa.Table: Table with geometry column processed
    """
    import geoarrow.pyarrow as ga

    try:
        geom_col = table.column(geometry_column)

        if geoparquet_version in ("2.0", "parquet-geo-only"):
            # For v2.0/parquet-geo-only: use geoarrow extension type with CRS
            wkb_arr = ga.as_wkb(geom_col)

            # Apply CRS to schema type if non-default
            if input_crs and not is_default_crs(input_crs):
                if verbose:
                    debug(f"Applying CRS to geometry schema type: {_format_crs_display(input_crs)}")
                new_type = wkb_arr.type.with_crs(input_crs)
                wkb_arr = _rebuild_array_with_type(wkb_arr, new_type)

            # Replace geometry column in table
            col_index = table.schema.get_field_index(geometry_column)
            table = table.set_column(col_index, geometry_column, wkb_arr)
        else:
            # For v1.x: ensure plain binary WKB (strip geoarrow if present)
            # CRS goes only in metadata, not in schema
            if _is_geoarrow_extension_type(geom_col.type):
                table = _strip_geoarrow_to_plain_wkb(table, geometry_column, verbose)
            elif verbose:
                debug("v1.x: geometry is already plain binary WKB (CRS in metadata only)")

    except (TypeError, ValueError, AttributeError) as e:
        if verbose:
            debug(f"Could not process geometry column: {e}")
        # Continue without conversion - geometry is already WKB

    return table


def _compute_geometry_types(table, geometry_column: str, verbose: bool) -> list[str]:
    """
    Compute geometry types from a geometry column using geoarrow.

    Args:
        table: PyArrow Table containing the geometry column
        geometry_column: Name of the geometry column
        verbose: Whether to print verbose output

    Returns:
        list: List of GeoParquet geometry type names (e.g., ["Point", "Polygon"])
    """
    import geoarrow.pyarrow as ga

    # Skip for empty tables (geoarrow crashes on empty arrays)
    if table.num_rows == 0:
        return []

    try:
        geom_col = table.column(geometry_column)
        wkb_arr = ga.as_wkb(geom_col)
        types_struct = ga.unique_geometry_types(wkb_arr)

        # Extract geometry type codes from struct array
        type_codes = types_struct.field("geometry_type").to_pylist()

        # Map codes to GeoParquet standard names (avoid duplicates)
        type_names = []
        for code in type_codes:
            name = _get_geometry_type_name(code)
            if name not in type_names:
                type_names.append(name)

        if verbose:
            debug(f"Computed geometry_types from data: {type_names}")
        return type_names

    except (TypeError, ValueError, AttributeError) as e:
        if verbose:
            debug(f"Could not compute geometry_types: {e}")
        # Return empty list as fallback (allowed by spec - means any type)
        return []


def _compute_bbox_from_data(table, geometry_column: str, verbose: bool) -> list[float] | None:
    """
    Compute bounding box from geometry column data.

    Args:
        table: PyArrow Table containing the geometry column
        geometry_column: Name of the geometry column
        verbose: Whether to print verbose output

    Returns:
        list: [xmin, ymin, xmax, ymax] or None if computation fails
    """
    import geoarrow.pyarrow as ga
    import pyarrow.compute as pc

    # Skip for empty tables
    if table.num_rows == 0:
        return None

    try:
        geom_col = table.column(geometry_column)
        wkb_arr = ga.as_wkb(geom_col)
        box_arr = ga.box(wkb_arr)

        # Combine chunks and get storage (underlying struct array)
        combined = box_arr.combine_chunks()
        storage = combined.storage

        # Extract struct fields and compute min/max
        xmin = pc.min(pc.struct_field(storage, "xmin")).as_py()
        ymin = pc.min(pc.struct_field(storage, "ymin")).as_py()
        xmax = pc.max(pc.struct_field(storage, "xmax")).as_py()
        ymax = pc.max(pc.struct_field(storage, "ymax")).as_py()

        if all(v is not None for v in [xmin, ymin, xmax, ymax]):
            if verbose:
                debug(f"Computed bbox from data: [{xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f}]")
            return [xmin, ymin, xmax, ymax]

    except (TypeError, ValueError, AttributeError) as e:
        if verbose:
            debug(f"Could not compute bbox: {e}")

    return None


def _assemble_and_apply_geo_metadata(
    table,
    geometry_column: str,
    geo_meta: dict,
    input_crs: dict | None,
    metadata_version: str,
    verbose: bool,
):
    """
    Assemble final geo metadata and apply it to the table.

    Adds CRS to geo metadata if provided and applies the complete
    metadata to the table schema.

    Args:
        table: PyArrow Table to modify
        geometry_column: Name of the geometry column
        geo_meta: Geo metadata dict to finalize
        input_crs: PROJJSON dict with CRS (optional)
        metadata_version: GeoParquet metadata version string
        verbose: Whether to print verbose output

    Returns:
        pa.Table: Table with geo metadata applied
    """
    # Add CRS to geo metadata if provided (for v1.x and v2.0)
    if input_crs and not is_default_crs(input_crs):
        if geometry_column not in geo_meta.get("columns", {}):
            geo_meta["columns"][geometry_column] = {}
        geo_meta["columns"][geometry_column]["crs"] = input_crs
        if verbose:
            debug(f"Added CRS to geo metadata: {_format_crs_display(input_crs)}")

    # Apply metadata to table
    existing_metadata = dict(table.schema.metadata) if table.schema.metadata else {}
    new_metadata = {}

    # Copy non-geo metadata from existing
    for k, v in existing_metadata.items():
        key_str = k.decode("utf-8") if isinstance(k, bytes) else k
        if not key_str.startswith("geo"):
            new_metadata[k] = v

    # Add geo metadata
    new_metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
    table = table.replace_schema_metadata(new_metadata)

    if verbose:
        debug(f"Applied geo metadata with version {metadata_version}")

    return table


def _apply_geoparquet_metadata(
    table,
    geometry_column: str,
    geoparquet_version: str | None,
    original_metadata: dict | None = None,
    input_crs: dict | None = None,
    custom_metadata: dict | None = None,
    verbose: bool = False,
):
    """
    Apply GeoParquet metadata to an Arrow Table based on version.

    Handles different GeoParquet versions:
    - v1.x: Apply geo metadata to schema, CRS via geoarrow type
    - v2.0: Apply CRS to schema type AND geo metadata
    - parquet-geo-only: Apply CRS to schema type only, no geo metadata

    Args:
        table: PyArrow Table to modify
        geometry_column: Name of the geometry column
        geoparquet_version: GeoParquet version (1.0, 1.1, 2.0, parquet-geo-only)
        original_metadata: Original metadata to preserve
        input_crs: PROJJSON dict with CRS
        custom_metadata: Custom metadata (e.g., H3 covering info)
        verbose: Whether to print verbose output

    Returns:
        pa.Table: Table with GeoParquet metadata applied
    """
    version_config = GEOPARQUET_VERSIONS.get(
        geoparquet_version, GEOPARQUET_VERSIONS[DEFAULT_GEOPARQUET_VERSION]
    )
    metadata_version = version_config["metadata_version"]
    should_add_geo_metadata = geoparquet_version != "parquet-geo-only"

    if verbose:
        debug(f"Applying GeoParquet metadata for version: {geoparquet_version or 'default (1.1)'}")

    # Check if geometry column exists in table
    if geometry_column not in table.column_names:
        if verbose:
            debug(f"Geometry column '{geometry_column}' not found in table, skipping metadata")
        return table

    # Step 1: Handle geometry column based on version
    table = _process_geometry_column_for_version(
        table, geometry_column, geoparquet_version, input_crs, verbose
    )

    # Step 2: Build and apply geo metadata (unless parquet-geo-only)
    if not should_add_geo_metadata:
        return table

    # Detect bbox column from table schema
    bbox_column = _detect_bbox_column_from_table(table, verbose)
    bbox_info = {
        "has_bbox_column": bbox_column is not None,
        "bbox_column_name": bbox_column,
    }

    # Create geo metadata using existing helper
    geo_meta = create_geo_metadata(
        original_metadata,
        geometry_column,
        bbox_info,
        custom_metadata,
        verbose,
        version=metadata_version,
    )

    # Ensure geometry_types is set (required by GeoParquet spec)
    col_meta = geo_meta.get("columns", {}).get(geometry_column, {})
    if "geometry_types" not in col_meta:
        col_meta["geometry_types"] = _compute_geometry_types(table, geometry_column, verbose)
        geo_meta["columns"][geometry_column] = col_meta

    # Compute file-level bbox from geometry data
    computed_bbox = _compute_bbox_from_data(table, geometry_column, verbose)
    if computed_bbox:
        col_meta["bbox"] = computed_bbox
        geo_meta["columns"][geometry_column] = col_meta

    # Assemble and apply final metadata
    return _assemble_and_apply_geo_metadata(
        table, geometry_column, geo_meta, input_crs, metadata_version, verbose
    )


def _estimate_row_size(table) -> int:
    """
    Estimate bytes per row from PyArrow table memory usage.

    Uses table.get_total_buffer_size() if available (PyArrow >= 0.17),
    falls back to table.nbytes, and uses a default of 100 bytes if
    neither is available or returns 0.

    Args:
        table: PyArrow Table

    Returns:
        int: Estimated bytes per row (minimum 1)
    """
    default_row_size = 100
    num_rows = max(1, table.num_rows)

    # Try get_total_buffer_size() first (more accurate, includes all buffers)
    if hasattr(table, "get_total_buffer_size"):
        try:
            total_bytes = table.get_total_buffer_size()
            if total_bytes > 0:
                return max(1, total_bytes // num_rows)
        except Exception:
            pass

    # Fall back to nbytes property
    if hasattr(table, "nbytes"):
        try:
            total_bytes = table.nbytes
            if total_bytes > 0:
                return max(1, total_bytes // num_rows)
        except Exception:
            pass

    return default_row_size


def _write_table_with_settings(
    table,
    output_path: str,
    compression: str,
    compression_level: int | None,
    row_group_rows: int | None,
    row_group_size_mb: int | None,
    geoparquet_version: str | None,
    geometry_column: str,
    verbose: bool = False,
) -> None:
    """
    Write Arrow table to Parquet with proper settings.

    Uses pq.write_table directly since we've already applied all GeoParquet
    metadata to the table. This preserves the metadata we set (including version,
    geometry_types, CRS, etc.) without geoarrow overwriting it.

    Args:
        table: PyArrow Table to write
        output_path: Output file path
        compression: Compression type (ZSTD, GZIP, etc.)
        compression_level: Compression level
        row_group_rows: Exact number of rows per row group
        row_group_size_mb: Target row group size in MB
        geoparquet_version: GeoParquet version
        geometry_column: Name of the geometry column
        verbose: Whether to print verbose output
    """
    # Calculate row group size
    rows_per_group = row_group_rows
    if not rows_per_group and row_group_size_mb and table.num_rows > 0:
        # Estimate bytes per row from actual table memory usage
        estimated_row_size = _estimate_row_size(table)
        target_bytes = row_group_size_mb * 1024 * 1024
        rows_per_group = max(1, int(target_bytes // estimated_row_size))
        rows_per_group = min(rows_per_group, table.num_rows)

    # Use central configuration for write settings
    settings = ParquetWriteSettings(
        compression=compression,
        compression_level=compression_level,
        row_group_rows=row_group_rows,
        row_group_size_mb=row_group_size_mb,
    )
    write_kwargs = settings.get_pyarrow_kwargs(calculated_row_group_size=rows_per_group)

    if verbose:
        compression_desc = (
            f"{compression}:{compression_level}" if compression_level else compression
        )
        debug(f"Writing with {compression_desc} compression")
        if rows_per_group:
            debug(f"Row group size: {rows_per_group:,} rows")

    # Use pq.write_table for all versions - we've already applied all metadata
    # Using geoarrow's write_geoparquet_table would overwrite our carefully constructed metadata
    pq.write_table(table, output_path, **write_kwargs)

    if verbose:
        success(f"Wrote {table.num_rows:,} rows to {output_path}")


def write_geoparquet_via_arrow(
    con,
    query: str,
    output_file: str,
    geometry_column: str | None = None,
    original_metadata: dict | None = None,
    compression: str = "ZSTD",
    compression_level: int = 15,
    row_group_size_mb: int | None = None,
    row_group_rows: int | None = None,
    custom_metadata: dict | None = None,
    verbose: bool = False,
    show_sql: bool = False,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    input_crs: dict | None = None,
) -> None:
    """
    Write a GeoParquet file using Arrow as the internal transfer format.

    This is more efficient than the COPY-then-rewrite approach because it:
    1. Fetches query results directly as an Arrow Table
    2. Applies GeoParquet metadata in memory
    3. Writes once to disk

    Args:
        con: DuckDB connection with spatial extension loaded
        query: SQL SELECT query to execute
        output_file: Path to output file (local or remote URL)
        geometry_column: Name of geometry column (auto-detected if None)
        original_metadata: Original metadata from source file for preservation
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        custom_metadata: Optional dict with custom metadata (e.g., H3 covering info)
        verbose: Whether to print verbose output
        show_sql: Whether to print SQL statements before execution
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        input_crs: PROJJSON dict with CRS from input file
    """
    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, output_file)

    # Detect geometry column if not provided
    if geometry_column is None:
        geometry_column = _detect_geometry_from_query(con, query, original_metadata, verbose)

    # Auto-detect GeoParquet version from input metadata if not explicitly provided
    if geoparquet_version is None:
        geoparquet_version = extract_version_from_metadata(original_metadata)

    # Check if geometry column actually exists in the query result
    query_columns = _get_query_columns(con, query)
    has_geometry = geometry_column in query_columns

    with remote_write_context(output_file, is_directory=False, verbose=verbose) as (
        actual_output,
        is_remote,
    ):
        # Validate compression settings
        compression, compression_level, compression_desc = validate_compression_settings(
            compression, compression_level, verbose
        )

        if verbose:
            debug(f"Writing output with {compression_desc} compression (Arrow path)...")
            if geoparquet_version:
                debug(f"Using GeoParquet version: {geoparquet_version}")

        # Wrap query with WKB conversion only if geometry column exists
        if has_geometry:
            final_query = _wrap_query_with_wkb_conversion(query, geometry_column)
        else:
            final_query = query
            if verbose:
                debug(
                    f"Geometry column '{geometry_column}' not in query - writing as regular Parquet"
                )

        if show_sql:
            info("\n-- Arrow query (with WKB conversion):" if has_geometry else "\n-- Arrow query:")
            progress(final_query)

        # Fetch as Arrow table
        if verbose:
            debug("Fetching query results as Arrow table...")

        result = con.execute(final_query)
        table = result.fetch_arrow_table()

        if verbose:
            debug(f"Fetched {table.num_rows:,} rows, {len(table.column_names)} columns")

        # Apply GeoParquet metadata only if geometry column exists
        if has_geometry:
            table = _apply_geoparquet_metadata(
                table,
                geometry_column=geometry_column,
                geoparquet_version=geoparquet_version,
                original_metadata=original_metadata,
                input_crs=input_crs,
                custom_metadata=custom_metadata,
                verbose=verbose,
            )

        # Write to disk
        _write_table_with_settings(
            table,
            actual_output,
            compression=compression,
            compression_level=compression_level,
            row_group_rows=row_group_rows,
            row_group_size_mb=row_group_size_mb,
            geoparquet_version=geoparquet_version,
            geometry_column=geometry_column,
            verbose=verbose,
        )

        # Upload to remote if needed
        if is_remote:
            upload_if_remote(
                actual_output,
                output_file,
                profile=profile,
                is_directory=False,
                verbose=verbose,
            )


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
    show_sql=False,
    profile=None,
    geoparquet_version=None,
    input_crs=None,
):
    """
    Write a parquet file with proper compression and metadata handling.

    Uses Arrow as the internal transfer format for efficiency - fetches DuckDB
    query results directly as an Arrow table, applies metadata in memory, and
    writes once to disk (no intermediate file rewrites).

    Supports both local and remote outputs (S3, GCS, Azure). Remote outputs
    are written to a temporary local file, then uploaded.

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
        show_sql: Whether to print SQL statements before execution
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        input_crs: PROJJSON dict with CRS from input file

    Returns:
        None
    """
    # Delegate to the Arrow-based implementation
    # Bounds are always computed from actual table data during metadata application
    write_geoparquet_via_arrow(
        con=con,
        query=query,
        output_file=output_file,
        geometry_column=None,  # Auto-detect
        original_metadata=original_metadata,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        custom_metadata=custom_metadata,
        verbose=verbose,
        show_sql=show_sql,
        profile=profile,
        geoparquet_version=geoparquet_version,
        input_crs=input_crs,
    )


def write_geoparquet_table(
    table,
    output_file: str,
    geometry_column: str | None = None,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    geoparquet_version: str | None = None,
    verbose: bool = False,
    profile: str | None = None,
) -> None:
    """
    Write a PyArrow Table to a GeoParquet file with proper metadata.

    This is the table-centric version for writing GeoParquet files.
    It applies proper GeoParquet metadata and handles compression settings.

    Args:
        table: PyArrow Table to write
        output_file: Path to output file (local path or remote URL)
        geometry_column: Name of geometry column (auto-detected if None)
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0)
        verbose: Whether to print verbose output
        profile: AWS profile name (S3 only, optional)
    """
    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, output_file)

    # Auto-detect geometry column if not provided
    if geometry_column is None:
        # Try to detect from table metadata
        metadata = table.schema.metadata or {}
        if b"geo" in metadata:
            try:
                geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
                geometry_column = geo_meta.get("primary_column", "geometry")
            except (json.JSONDecodeError, UnicodeDecodeError):
                geometry_column = "geometry"
        else:
            # Check for common geometry column names
            for name in ["geometry", "geom", "wkb_geometry"]:
                if name in table.column_names:
                    geometry_column = name
                    break
            if geometry_column is None:
                geometry_column = "geometry"

    # Check if geometry column exists
    has_geometry = geometry_column in table.column_names

    # Extract original metadata for preservation
    original_metadata = table.schema.metadata

    # Extract CRS from original metadata if available
    input_crs = None
    if original_metadata and b"geo" in original_metadata:
        try:
            geo_meta = json.loads(original_metadata[b"geo"].decode("utf-8"))
            columns = geo_meta.get("columns", {})
            if geometry_column in columns:
                input_crs = columns[geometry_column].get("crs")
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    # Validate and normalize compression settings
    validated_compression, validated_level, _ = validate_compression_settings(
        compression or "ZSTD", compression_level, verbose
    )
    # Handle UNCOMPRESSED - pass None for compression when uncompressed
    if validated_compression == "UNCOMPRESSED":
        validated_compression = None

    with remote_write_context(output_file, is_directory=False, verbose=verbose) as (
        actual_output,
        is_remote,
    ):
        # Apply GeoParquet metadata only if geometry column exists
        if has_geometry:
            table = _apply_geoparquet_metadata(
                table,
                geometry_column=geometry_column,
                geoparquet_version=geoparquet_version,
                original_metadata=original_metadata,
                input_crs=input_crs,
                custom_metadata=None,
                verbose=verbose,
            )

        # Write to disk with proper settings
        _write_table_with_settings(
            table,
            actual_output,
            compression=validated_compression or "UNCOMPRESSED",
            compression_level=validated_level,
            row_group_rows=row_group_rows,
            row_group_size_mb=row_group_size_mb,
            geoparquet_version=geoparquet_version,
            geometry_column=geometry_column,
            verbose=verbose,
        )

        # Upload to remote if needed
        if is_remote:
            upload_if_remote(
                actual_output,
                output_file,
                profile=profile,
                is_directory=False,
                verbose=verbose,
            )


def format_size(size_bytes):
    """Convert bytes to human readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def _find_bbox_column_in_schema(schema_info, verbose):
    """Find bbox column in schema by conventional names or structure.

    Args:
        schema_info: List of column dicts from get_schema_info()
        verbose: Whether to print verbose output

    Note:
        DuckDB's parquet_schema() returns nested struct fields without parent prefix.
        For a struct column 'bbox' with fields xmin/ymin/xmax/ymax:
        - bbox appears with num_children=4
        - Child fields appear as 'xmin', 'ymin', 'xmax', 'ymax' (not 'bbox.xmin')
    """
    # Check for columns ending with these suffixes (e.g., geometry_bbox, bbox)
    conventional_suffixes = ["bbox", "bounds", "extent"]
    required_fields = {"xmin", "ymin", "xmax", "ymax"}

    for i, col in enumerate(schema_info):
        name = col.get("name", "")
        num_children = col.get("num_children", 0)

        if not name:
            continue

        # Check if column name ends with conventional suffixes and has struct children
        is_bbox_name = any(name.endswith(suffix) for suffix in conventional_suffixes)
        if is_bbox_name and num_children >= 4:
            # Get the next num_children entries as the struct's child fields
            child_names = set()
            for j in range(1, num_children + 1):
                if i + j < len(schema_info):
                    child_name = schema_info[i + j].get("name", "")
                    child_names.add(child_name)

            # Check if all required fields are present
            if required_fields.issubset(child_names):
                if verbose:
                    debug(f"Found bbox column: {name} with children: {child_names}")
                return name

    return None


def _check_bbox_metadata_covering(geo_meta, has_bbox_column, verbose):
    """Check if geo metadata contains proper bbox covering.

    Args:
        geo_meta: Parsed geo metadata dict (from get_geo_metadata())
        has_bbox_column: Whether a bbox column was found in schema
        verbose: Whether to print verbose output
    """
    if not (geo_meta and has_bbox_column):
        return False

    if verbose:
        debug("\nParsed geo metadata:")
        debug(json.dumps(geo_meta, indent=2))

    if isinstance(geo_meta, dict) and "columns" in geo_meta:
        columns = geo_meta["columns"]
        for _col_name, col_info in columns.items():
            if isinstance(col_info, dict) and col_info.get("covering", {}).get("bbox"):
                bbox_refs = col_info["covering"]["bbox"]
                # Check if the bbox covering has the required structure
                if (
                    isinstance(bbox_refs, dict)
                    and all(key in bbox_refs for key in ["xmin", "ymin", "xmax", "ymax"])
                    and all(isinstance(ref, list) and len(ref) == 2 for ref in bbox_refs.values())
                ):
                    referenced_bbox_column = bbox_refs["xmin"][0]
                    if verbose:
                        debug(
                            f"Found bbox covering in metadata referencing column: {referenced_bbox_column}"
                        )
                    return True

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
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata, get_schema_info

    safe_url = safe_file_url(parquet_file, verbose=False)

    # Get schema info using DuckDB
    schema_info = get_schema_info(safe_url)

    if verbose:
        debug("\nSchema fields:")
        for col in schema_info:
            name = col.get("name", "")
            col_type = col.get("type", "")
            if name:  # Skip empty names
                debug(f"  {name}: {col_type}")

    # Find the bbox column in the schema
    bbox_column_name = _find_bbox_column_in_schema(schema_info, verbose)
    has_bbox_column = bbox_column_name is not None

    # Get geo metadata and check for bbox covering
    geo_meta = get_geo_metadata(safe_url)
    has_bbox_metadata = _check_bbox_metadata_covering(geo_meta, has_bbox_column, verbose)

    # Determine status and message
    status, message = _determine_bbox_status(has_bbox_column, bbox_column_name, has_bbox_metadata)

    if verbose:
        debug("\nFinal results:")
        debug(f"  has_bbox_column: {has_bbox_column}")
        debug(f"  bbox_column_name: {bbox_column_name}")
        debug(f"  has_bbox_metadata: {has_bbox_metadata}")
        debug(f"  status: {status}")
        debug(f"  message: {message}")

    return {
        "has_bbox_column": has_bbox_column,
        "bbox_column_name": bbox_column_name if has_bbox_column else None,
        "has_bbox_metadata": has_bbox_metadata,
        "status": status,
        "message": message,
    }


def get_bbox_advice(
    parquet_file: str,
    operation: str,
    verbose: bool = False,
) -> dict:
    """
    Get version-aware bbox optimization advice.

    Provides context-aware recommendations based on file type and operation:
    - For GeoParquet 2.0/parquet-geo with spatial_filtering: No bbox needed (native stats work)
    - For GeoParquet 2.0/parquet-geo with bounds_calculation: bbox still recommended (faster)
    - For GeoParquet 1.x without bbox: Suggest adding bbox OR upgrading to 2.0

    Args:
        parquet_file: Path to the parquet file
        operation: One of:
            - "spatial_filtering": For ST_Intersects, spatial joins, etc.
            - "bounds_calculation": For centroid, extent, quadkey, etc.
            - "check": For validation/inspection
        verbose: Whether to print verbose output

    Returns:
        dict with:
            - needs_warning: bool - Whether to show a warning to the user
            - skip_bbox_prefilter: bool - Whether to skip bbox pre-filtering in queries.
              Only True for spatial_filtering with native geometry (where Parquet stats
              are used automatically). For bounds_calculation, always False since bbox
              column provides pre-computed values that are faster than geometry stats.
            - has_native_geometry: bool - Whether file uses native Parquet geometry types
            - message: str - User-facing message (if needs_warning)
            - suggestions: list[str] - Suggested actions for the user
    """
    file_info = detect_geoparquet_file_type(parquet_file, verbose)
    bbox_info = check_bbox_structure(parquet_file, verbose)

    has_native_geo = file_info["file_type"] in ("geoparquet_v2", "parquet_geo_only")
    has_bbox = bbox_info["has_bbox_column"]

    # Only skip bbox pre-filtering for spatial_filtering operations with native geometry.
    # For bounds_calculation, bbox column provides pre-computed values that are faster.
    skip_bbox = has_native_geo and operation == "spatial_filtering"

    result = {
        "needs_warning": False,
        "skip_bbox_prefilter": skip_bbox,
        "has_native_geometry": has_native_geo,
        "has_bbox_column": has_bbox,
        "bbox_column_name": bbox_info.get("bbox_column_name"),
        "message": "",
        "suggestions": [],
    }

    if operation == "spatial_filtering":
        if has_native_geo:
            # Native geometry stats are used automatically - no warning needed
            if verbose:
                debug("Using native Parquet geometry statistics for spatial filtering")
        elif not has_bbox:
            # 1.x without bbox - warn and suggest options
            result["needs_warning"] = True
            result["message"] = "No bbox column found"
            result["suggestions"] = [
                "Add a bbox column: gpio add bbox <file>",
                "Or upgrade to GeoParquet 2.0: gpio convert <file> --geoparquet-version 2.0",
            ]

    elif operation == "bounds_calculation":
        # bbox column is still faster for bounds/centroid calculation (pre-computed values)
        if not has_bbox:
            result["needs_warning"] = True
            result["message"] = "No bbox column - computing from geometry (slower)"
            result["suggestions"] = [
                "Add a bbox column for 3-4x faster bounds/centroid: gpio add bbox <file>"
            ]

    elif operation == "check":
        if has_native_geo:
            # Native geometry - bbox optional but can help with bounds queries
            if not has_bbox and verbose:
                debug("Native geometry type detected - bbox column optional for spatial queries")
        elif not has_bbox:
            # 1.x without bbox
            result["needs_warning"] = True
            result["message"] = "No bbox column found"
            result["suggestions"] = [
                "Add a bbox column: gpio add bbox <file>",
                "Or upgrade to GeoParquet 2.0: gpio convert <file> --geoparquet-version 2.0",
            ]

    return result


def _build_bounds_query(safe_url, bbox_info, geometry_column, verbose):
    """Build query for bounds calculation."""
    if bbox_info["has_bbox_column"]:
        bbox_col = bbox_info["bbox_column_name"]
        if verbose:
            debug(f"Using bbox column '{bbox_col}' for fast bounds calculation")

        return f"""
        SELECT
            MIN({bbox_col}.xmin) as xmin,
            MIN({bbox_col}.ymin) as ymin,
            MAX({bbox_col}.xmax) as xmax,
            MAX({bbox_col}.ymax) as ymax
        FROM '{safe_url}'
        """
    else:
        warn(
            f"⚠️  No bbox column found - calculating bounds from geometry column '{geometry_column}' (this may be slow)"
        )
        info("💡 Tip: Add a bbox column for faster operations with 'gpio add bbox'")

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
    configure_verbose(verbose)
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
                debug(f"Dataset bounds: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")
            return (xmin, ymin, xmax, ymax)
        else:
            if verbose:
                warn("Could not calculate bounds (empty dataset or null geometries)")
            return None

    except Exception as e:
        if verbose:
            error(f"Error calculating bounds: {e}")
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
    replace_column=None,
    geoparquet_version=None,
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

    Args:
        input_parquet: Path to input file (local or remote URL)
        output_parquet: Path to output file (local or remote URL)
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
        replace_column: Name of existing column to replace (uses EXCLUDE in query)

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
    # Get safe URL for input file
    input_url = safe_file_url(input_parquet, verbose)

    # Get geometry column (for reference)
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # Dry-run mode header
    if dry_run:
        warn("\n=== DRY RUN MODE - SQL Commands that would be executed ===\n")
        info(f"-- Input file: {input_url}")
        info(f"-- Output file: {output_parquet}")
        info(f"-- Geometry column: {geom_col}")
        info(f"-- New column: {column_name}")
        if dry_run_description:
            info(f"-- Description: {dry_run_description}")
        progress("")

    # Check if column already exists (skip in dry-run or when replacing)
    if not dry_run:
        from geoparquet_io.core.duckdb_metadata import get_column_names

        # Only check for column collision if not replacing
        if not replace_column:
            column_names = get_column_names(input_url)
            if column_name in column_names:
                raise click.ClickException(
                    f"Column '{column_name}' already exists in the file. "
                    f"Please choose a different name."
                )

        # Get metadata before processing
        metadata, _ = get_parquet_metadata(input_parquet, verbose)

        if verbose:
            if replace_column:
                debug(f"Replacing column '{replace_column}' with '{column_name}'...")
            else:
                debug(f"Adding column '{column_name}'...")

    # Create DuckDB connection with httpfs if needed
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))

    # Load additional extensions if specified
    if extensions:
        for ext in extensions:
            if verbose and not dry_run:
                debug(f"Loading DuckDB extension: {ext}")
            con.execute(f"INSTALL {ext} FROM community;")
            con.execute(f"LOAD {ext};")

    # Get total count (skip in dry-run)
    if not dry_run:
        total_count = con.execute(f"SELECT COUNT(*) FROM '{input_url}'").fetchone()[0]
        progress(f"Processing {total_count:,} features...")

    # Build the query
    # Use EXCLUDE to drop existing column when replacing
    if replace_column:
        query = f"""
        SELECT
            * EXCLUDE ({replace_column}),
            {sql_expression} AS {column_name}
        FROM '{input_url}'
    """
    else:
        query = f"""
        SELECT
            *,
            {sql_expression} AS {column_name}
        FROM '{input_url}'
    """

    # Handle dry-run display
    if dry_run:
        # Show formatted query with COPY wrapper
        compression_desc = compression
        if compression in ["GZIP", "ZSTD", "BROTLI"] and compression_level:
            compression_desc = f"{compression}:{compression_level}"

        duckdb_compression = (
            compression.lower() if compression != "UNCOMPRESSED" else "uncompressed"
        )
        display_query = f"""COPY ({query.strip()})
TO '{output_parquet}'
(FORMAT PARQUET, COMPRESSION '{duckdb_compression}');"""

        info("-- Main query:")
        progress(display_query)
        info(f"\n-- Note: Using {compression_desc} compression")
        info("-- This query creates a new parquet file with the computed column added")
        info("-- Metadata would also be updated with proper GeoParquet covering information")
        return

    # Execute the query using existing write helper
    if verbose:
        debug(f"Creating column '{column_name}'...")

    write_parquet_with_metadata(
        con,
        query,
        output_parquet,
        original_metadata=metadata,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        custom_metadata=custom_metadata,
        verbose=verbose,
        profile=profile,
        geoparquet_version=geoparquet_version,
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
    from geoparquet_io.core.duckdb_metadata import get_column_names

    # Check if column already exists using DuckDB
    safe_url = safe_file_url(parquet_file, verbose=False)
    column_names = get_column_names(safe_url)

    if bbox_column_name in column_names:
        raise click.ClickException(
            f"Column '{bbox_column_name}' already exists in the file. "
            f"Please choose a different name."
        )

    # Get geometry column for SQL expression
    geom_col = find_primary_geometry_column(parquet_file, verbose)

    if verbose:
        debug(f"Adding bbox column for geometry column: {geom_col}")

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
            success(f"Successfully added bbox column '{bbox_column_name}'")

        return True

    except Exception as e:
        # Clean up temporary file if something goes wrong
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise click.ClickException(f"Failed to add bbox: {str(e)}") from e
