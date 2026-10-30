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
    FailureKind,
    InvalidParameterError,
    classify_failure,
    fatal_status_message,
    sanitize_url_for_logging,
)
from geoparquet_io.core.logging_config import debug, info, progress, success, warn

#: Every URL scheme gpio reads or writes, and the store family it belongs to.
#: The one scheme table (#1367): :func:`is_remote_url` and every family test
#: below read it, and nothing outside this module spells a scheme out.
#: Matching is case-sensitive, as DuckDB's own is.
REMOTE_SCHEMES = {
    "http": "http",
    "https": "http",
    "s3": "s3",
    "s3a": "s3",
    "gs": "gcs",
    "gcs": "gcs",
    "az": "azure",
    "azure": "azure",
    "abfs": "azure",
    "abfss": "azure",
}


def url_scheme(path) -> str | None:
    """The scheme of a remote URL as written (``"s3a"``, ``"https"``), else None.

    None for a local path, ``file://``, an unknown scheme and anything that is
    not a string.
    """
    if not isinstance(path, str):
        return None
    scheme, separator, _ = path.partition("://")
    return scheme if separator and scheme in REMOTE_SCHEMES else None


def remote_family(path) -> str | None:
    """The store family of a remote URL: ``http``, ``s3``, ``gcs`` or ``azure``."""
    scheme = url_scheme(path)
    return REMOTE_SCHEMES[scheme] if scheme else None


def has_http_scheme(url: str) -> bool:
    """Whether *url* parses with an ``http``/``https`` scheme, in any case.

    For a service endpoint (CARTO's SQL API) that must be handed to an HTTP
    client and nothing else: an empty, ``file://`` or object-store scheme fails.
    """
    from urllib.parse import urlparse

    return REMOTE_SCHEMES.get(urlparse(url).scheme) == "http"


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
    return url_scheme(path) is not None


def is_s3_url(path):
    """
    Check if path is an S3 URL.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is S3
    """
    return remote_family(path) == "s3"


def is_azure_url(path):
    """
    Check if path is an Azure Blob Storage URL.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is Azure
    """
    return remote_family(path) == "azure"


def is_gcs_url(path):
    """
    Check if path is a Google Cloud Storage URL.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if path is GCS
    """
    return remote_family(path) == "gcs"


def needs_httpfs(path):
    """
    Check if path requires httpfs extension (S3, Azure, GCS).

    HTTP/HTTPS work without httpfs, but cloud storage protocols need it.

    Args:
        path: File path or URL to check

    Returns:
        bool: True if httpfs extension is needed
    """
    return remote_family(path) not in (None, "http")


def protocol_label(path, default: str = "HTTP") -> str:
    """The upper-cased scheme a message names a URL by (``S3``), else *default*."""
    return path.split("://")[0].upper() if "://" in path else default


def display_path(path):
    """*path* as a message may show it: a remote URL with its secrets stripped."""
    return sanitize_url_for_logging(path) if is_remote_url(path) else path


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
            protocol = protocol_label(url, "unknown")
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

    protocol = protocol_label(file_path)
    if verbose:
        info(f"Reading from {protocol}: {sanitize_url_for_logging(file_path)}")
    else:
        info(f"Reading from {protocol} (network operations may take time)...")


#: The remedy for a 401/403, per object store: a fact about the store, while *which*
#: store a path is on is `is_s3_url` / `is_azure_url` / `is_gcs_url`'s decision.
_S3_CREDENTIALS = (
    "S3: check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or configure ~/.aws/credentials."
)
_AZURE_CREDENTIALS = (
    "Azure: check AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY, "
    "or set AZURE_STORAGE_SAS_TOKEN for SAS token auth."
)
_GCS_CREDENTIALS = "GCS: check GOOGLE_APPLICATION_CREDENTIALS points to service account JSON."

#: The guidance for the failures that are not an HTTP status, keyed by the kind
#: ``classify_failure`` reports -- the same classification everything else uses.
_REMOTE_HINTS = {
    FailureKind.TIMEOUT: (
        "Connection timed out:\n  - Check network connectivity\n"
        "  - File may be very large - try a smaller file first\n"
        "  - Remote server may be slow or overloaded"
    ),
    FailureKind.UNREACHABLE: (
        "Cannot connect to remote server:\n  - Check network connectivity\n"
        "  - Verify the hostname/URL is correct\n  - Server may be down or unreachable"
    ),
}
_GENERIC_REMOTE_HINT = (
    "Remote file access failed:\n  - Check network connectivity\n"
    "  - Verify file URL and access permissions"
)


def _store_credentials(file_path: str) -> str:
    if is_s3_url(file_path):
        return _S3_CREDENTIALS
    if is_azure_url(file_path):
        return _AZURE_CREDENTIALS
    if is_gcs_url(file_path):
        return _GCS_CREDENTIALS
    return "The file may be private or require authentication."


def get_remote_error_hint(error: BaseException, file_path: str = "") -> str:
    """Guidance for a failed remote read, from the one failure classification.

    Args:
        error: The exception DuckDB (or another library) raised.
        file_path: The remote file path/URL that failed

    Returns:
        str: User-friendly message with troubleshooting hints. A 401/403/404 uses
        the shared status sentence (:func:`~geoparquet_io.core.exceptions.fatal_status_message`)
        with the object store's own credential remedy.
    """
    verdict = classify_failure(error)
    if verdict.kind is FailureKind.HTTP_STATUS and verdict.status is not None:
        message = fatal_status_message(
            verdict.status,
            subject="the remote file",
            credentials=_store_credentials(file_path),
        )
        if message is not None and verdict.status == 404 and file_path:
            return f"{message}\n  - URL: {sanitize_url_for_logging(file_path)}"
        if message is not None:
            return message
    return _REMOTE_HINTS.get(verdict.kind, _GENERIC_REMOTE_HINT)


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
