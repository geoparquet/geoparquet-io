"""
Remote URL handling for S3, GCS, Azure, and HTTP/HTTPS.

This module provides utilities for detecting and working with remote URLs
in cloud object storage (S3, GCS, Azure) and HTTP/HTTPS endpoints.
"""

import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path

from geoparquet_io.core.exceptions import (
    InvalidParameterError,
    sanitize_url_for_logging,
)
from geoparquet_io.core.logging_config import debug, info, progress, success, warn

# Re-export with underscore prefix for backward compatibility
# TODO: Migrate all call sites to sanitize_url_for_logging and remove this alias
_sanitize_url_for_logging = sanitize_url_for_logging


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
    if path is None:
        return False
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


def resolve_s3_config(
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_no_ssl: bool = False,
    aws_profile: str | None = None,
) -> dict:
    """Resolve S3 configuration from explicit params with env var fallbacks.

    Resolution order (highest wins):
    1. Explicit params
    2. AWS env vars (AWS_ENDPOINT_URL, AWS_REGION/AWS_DEFAULT_REGION, AWS_PROFILE)
    3. Defaults (no endpoint, no region, SSL on, no profile)

    ``profile_from_env`` is True when the profile is only the exported
    ``AWS_PROFILE``. The AWS SDK lets env keys beat an exported ``AWS_PROFILE``
    but not ``--profile``, so the DuckDB secret needs to know which one it got.
    """
    endpoint = s3_endpoint or os.environ.get("AWS_ENDPOINT_URL") or None
    region = s3_region or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    profile = aws_profile or os.environ.get("AWS_PROFILE")

    use_ssl = True
    bare_endpoint = endpoint
    if endpoint:
        if endpoint.startswith("http://"):
            use_ssl = False
            bare_endpoint = endpoint[len("http://") :]
        elif endpoint.startswith("https://"):
            bare_endpoint = endpoint[len("https://") :]

    if s3_no_ssl:
        use_ssl = False

    return {
        "s3_endpoint": bare_endpoint,
        "s3_region": region,
        "s3_use_ssl": use_ssl,
        "profile": profile,
        "profile_from_env": bool(profile) and not aws_profile,
    }


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
        InvalidParameterError: If profile is used with non-S3 remote URLs

    Example:
        validate_profile_for_urls(profile, input_file, output_file)
    """
    if not profile:
        return

    for url in urls:
        if url and is_remote_url(url) and not is_s3_url(url):
            protocol = url.split("://")[0].upper() if "://" in url else "unknown"
            raise InvalidParameterError(
                "profile",
                f"only valid for S3 URLs, but got {protocol} URL: {sanitize_url_for_logging(url)}. "
                f"For {protocol} authentication, use environment variables or default credentials.",
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
        info(f"Reading from {protocol}: {sanitize_url_for_logging(file_path)}")
    else:
        info(f"Reading from {protocol} (network operations may take time)...")


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
            return "Authentication required or access denied:\n  - S3: Check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables\n  - Or configure ~/.aws/credentials file"
        if "az://" in path_lower or "azure" in path_lower or "blob.core" in path_lower:
            return "Authentication required or access denied:\n  - Azure: Check AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY\n  - Or set AZURE_STORAGE_SAS_TOKEN for SAS token auth"
        if "gs://" in path_lower or "gcs://" in path_lower:
            return "Authentication required or access denied:\n  - GCS: Check GOOGLE_APPLICATION_CREDENTIALS points to service account JSON"
        return "Authentication required or access denied:\n  - File may be private or require authentication"

    # Check for 404 errors
    if "404" in error_msg or "not found" in error_lower or "does not exist" in error_lower:
        base = "File not found at remote location:\n  - Verify the URL is correct\n  - Check the file exists at the specified path"
        return f"{base}\n  - URL: {sanitize_url_for_logging(file_path)}" if file_path else base

    # Check for timeout
    if "timeout" in error_lower or "timed out" in error_lower:
        return "Connection timed out:\n  - Check network connectivity\n  - File may be very large - try a smaller file first\n  - Remote server may be slow or overloaded"

    # Check for connection issues
    if "unable to connect" in error_lower or "connection" in error_lower:
        return "Cannot connect to remote server:\n  - Check network connectivity\n  - Verify the hostname/URL is correct\n  - Server may be down or unreachable"

    # Generic
    return "Remote file access failed:\n  - Check network connectivity\n  - Verify file URL and access permissions"


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
        progress(f"Uploading {size_mb:.1f} MB to {sanitize_url_for_logging(remote_path)}...")

    pattern = "*.parquet" if is_directory else None
    upload(
        source=Path(local_path),
        destination=remote_path,
        profile=profile,
        pattern=pattern,
        dry_run=False,
    )

    if verbose:
        success(f"Successfully uploaded to {sanitize_url_for_logging(remote_path)}")

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
