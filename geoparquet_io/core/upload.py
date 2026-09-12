"""Upload GeoParquet files to cloud object storage."""

import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import obstore as obs
from obstore.store import AzureStore, S3Store

from geoparquet_io.core.aws_credentials import (
    CREDENTIAL_CHAIN_SOURCES,
    resolve_aws_credentials,
    resolve_aws_region,
)
from geoparquet_io.core.exceptions import InvalidParameterError, RemoteAccessError
from geoparquet_io.core.logging_config import error, progress, success


def _try_infer_region_from_bucket(bucket: str) -> str | None:
    """Try to infer AWS region from bucket name.

    Some S3-compatible services include region in bucket name, e.g.:
    - us-west-2.opendata.source.coop -> us-west-2
    - eu-central-1.example.com -> eu-central-1

    This is a best-effort heuristic and should not be relied upon.

    Args:
        bucket: S3 bucket name

    Returns:
        Region string if detected, None otherwise
    """
    # Pattern matches AWS region format at start of bucket name
    region_pattern = r"^(us|eu|ap|sa|ca|me|af)-(north|south|east|west|central|northeast|southeast|northwest|southwest)-\d"
    match = re.match(region_pattern, bucket)
    if match:
        # Extract full region (e.g., "us-west-2" from "us-west-2.opendata.source.coop")
        region_end = bucket.find(".")
        if region_end > 0:
            return bucket[:region_end]
    return None


def _missing_profile_hint(profile: str) -> str:
    """Hint shown when ``--aws-profile`` names a profile the chain cannot use."""
    hints = [
        f"AWS profile '{profile}' not found or incomplete.",
        "",
        "Ensure your ~/.aws/credentials file has this profile:",
        f"  [{profile}]",
        "  aws_access_key_id = YOUR_ACCESS_KEY",
        "  aws_secret_access_key = YOUR_SECRET_KEY",
        "",
        "Or that ~/.aws/config configures it another way, e.g.:",
        f"  [profile {profile}]",
        f"  sso_session = my-sso        # then: aws sso login --profile {profile}",
        "  # or role_arn + source_profile, or credential_process",
        "",
        "Or use environment variables instead:",
        "  export AWS_ACCESS_KEY_ID=your_access_key",
        "  export AWS_SECRET_ACCESS_KEY=your_secret_key",
    ]
    return "\n".join(hints)


def _missing_credentials_hint() -> str:
    """Hint shown when no source in botocore's chain produced credentials."""
    hints = [
        "S3 credentials not found. To configure credentials:",
        "",
        "Option 1: Set environment variables",
        "  export AWS_ACCESS_KEY_ID=your_access_key",
        "  export AWS_SECRET_ACCESS_KEY=your_secret_key",
        "  export AWS_REGION=us-west-2  # required for most buckets",
        "",
        "Option 2: Use --aws-profile with a configured profile",
        "  gpio publish upload file.parquet s3://bucket/path --aws-profile myprofile",
        "",
        "Option 3: Configure the AWS CLI",
        "  aws configure          # static keys",
        "  aws sso login          # SSO / IAM Identity Center",
        "",
        "Credentials are resolved through the standard AWS chain:",
    ]
    hints.extend(f"  - {source}" for source in CREDENTIAL_CHAIN_SOURCES)
    return "\n".join(hints)


def _check_s3_credentials(profile: str | None = None) -> tuple[bool, str]:
    """Check if S3 credentials are available.

    Resolution goes through botocore's chain (see
    :mod:`geoparquet_io.core.aws_credentials`), so an SSO or assume-role user
    passes this gate rather than being told to write static keys (#865).

    Args:
        profile: AWS profile name to check (optional)

    Returns:
        Tuple of (credentials_found, hint_message)
    """
    if resolve_aws_credentials(profile):
        return True, ""
    return False, _missing_profile_hint(profile) if profile else _missing_credentials_hint()


def _check_gcs_credentials() -> tuple[bool, str]:
    """Check if GCS credentials are available.

    Returns:
        Tuple of (credentials_found, hint_message)
    """
    # Check for application default credentials or service account key
    gcloud_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if gcloud_creds and os.path.exists(gcloud_creds):
        return True, ""

    # Check if running in GCP (metadata service available)
    # For now, we'll assume credentials might be available via metadata

    hints = []
    hints.append("GCS credentials not found. To configure credentials:")
    hints.append("")
    hints.append("Option 1: Set service account key")
    hints.append("  export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json")
    hints.append("")
    hints.append("Option 2: Use application default credentials")
    hints.append("  gcloud auth application-default login")

    return False, "\n".join(hints)


# Every credential-bearing environment variable obstore's AzureStore reads
# (see obstore's AzureConfig): the aliases are equivalent, so any one of them
# is enough for the pre-upload gate. AZURE_USE_AZURE_CLI is checked separately
# because it is a boolean opt-in, not a secret.
_AZURE_CREDENTIAL_ENV_VARS = (
    "AZURE_STORAGE_ACCOUNT_KEY",
    "AZURE_STORAGE_ACCESS_KEY",
    "AZURE_STORAGE_MASTER_KEY",
    "AZURE_STORAGE_SAS_TOKEN",
    "AZURE_STORAGE_SAS_KEY",
    "AZURE_STORAGE_TOKEN",
    "AZURE_STORAGE_CLIENT_ID",
    "AZURE_CLIENT_ID",
)


def _check_azure_credentials() -> tuple[bool, str]:
    """Check if Azure credentials are available.

    The gate must accept everything obstore itself would authenticate with,
    or it blocks uploads that would have succeeded: obstore honours several
    aliases for each credential (``AZURE_STORAGE_ACCESS_KEY`` for
    ``AZURE_STORAGE_ACCOUNT_KEY``, ``AZURE_STORAGE_SAS_KEY`` for
    ``AZURE_STORAGE_SAS_TOKEN``, ...) plus the ``AZURE_USE_AZURE_CLI=true``
    opt-in for ``az login`` sessions.

    Returns:
        Tuple of (credentials_found, hint_message)
    """
    if any(os.environ.get(name) for name in _AZURE_CREDENTIAL_ENV_VARS):
        return True, ""

    # obstore only uses an `az login` session when explicitly opted in.
    if os.environ.get("AZURE_USE_AZURE_CLI", "").strip().lower() in ("1", "true", "yes", "on"):
        return True, ""

    hints = []
    hints.append("Azure credentials not found. To configure credentials:")
    hints.append("")
    hints.append("Option 1: Set storage account key")
    hints.append("  export AZURE_STORAGE_ACCOUNT_KEY=your_key")
    hints.append("")
    hints.append("Option 2: Set SAS token")
    hints.append("  export AZURE_STORAGE_SAS_TOKEN=your_token")
    hints.append("")
    hints.append("Option 3: Use Azure CLI (the opt-in is required; az login alone is not used)")
    hints.append("  az login")
    hints.append("  export AZURE_USE_AZURE_CLI=true")

    return False, "\n".join(hints)


def check_credentials(destination: str, profile: str | None = None) -> tuple[bool, str]:
    """Check if credentials are available for the destination.

    Args:
        destination: Object store URL (s3://, gs://, az://)
        profile: AWS profile name (for S3 only)

    Returns:
        Tuple of (credentials_ok, hint_message)
    """
    if destination.startswith("s3://"):
        return _check_s3_credentials(profile)
    elif destination.startswith("gs://"):
        return _check_gcs_credentials()
    elif destination.startswith("az://"):
        return _check_azure_credentials()
    else:
        # HTTP or other - assume ok
        return True, ""


def _print_single_file_dry_run(
    source: Path, destination: str, target_key: str, size_mb: float, profile: str | None
) -> None:
    """Print dry-run information for single file upload."""
    print("\n=== DRY RUN MODE - No files will be uploaded ===\n")
    print("Would upload:")
    print(f"  Source:      {source}")
    print(f"  Size:        {size_mb:.2f} MB")
    print(f"  Destination: {destination}")
    print(f"  Target key:  {target_key}")
    if profile:
        print(f"  AWS Profile: {profile}")
    print()


def _print_directory_dry_run(
    files: list[Path],
    source: Path,
    destination: str,
    prefix: str,
    total_size_mb: float,
    pattern: str | None,
    profile: str | None,
) -> None:
    """Print dry-run information for directory upload."""
    print("\n=== DRY RUN MODE - No files will be uploaded ===\n")
    print(f"Would upload {len(files)} file(s) ({total_size_mb:.2f} MB total)")
    print(f"  Source:      {source}")
    print(f"  Destination: {destination}")
    if pattern:
        print(f"  Pattern:     {pattern}")
    if profile:
        print(f"  AWS Profile: {profile}")
    print("\nFiles that would be uploaded:")
    for f in files[:10]:  # Show first 10 files
        rel_path = f.relative_to(source)
        target_key = f"{prefix.rstrip('/')}/{rel_path}" if prefix else str(rel_path)
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  • {f.name} ({size_mb:.2f} MB) → {target_key}")
    if len(files) > 10:
        print(f"  ... and {len(files) - 10} more file(s)")
    print()


AZURE_URL_FORM = "az://<account>/<container>/<path>"


def _split_azure_url(url: str) -> tuple[str, str, str]:
    """Split ``az://<account>/<container>[/<path>]`` into (account, container, path).

    gpio's Azure URLs name the storage account first and the container second.
    obstore's own ``az://`` convention is container-first, with the account taken
    from the environment, so a gpio URL can never be handed to
    ``obs.store.from_url``: with no account in the environment it refuses to build
    ("Account must be specified"), with ``AZURE_STORAGE_ACCOUNT_NAME`` set it
    panics inside Rust, and were it to build at all it would read the account
    segment as the container and write to the wrong place (#864). Splitting the
    URL here, and building the store from the parts, is what makes the account in
    the URL authoritative.

    Args:
        url: An ``az://`` URL

    Returns:
        Tuple of (account, container, path within the container)

    Raises:
        InvalidParameterError: If the URL names no account or no container
    """
    parts = url[len("az://") :].split("/", 2)
    account = parts[0]
    container = parts[1] if len(parts) > 1 else ""
    if not account or not container:
        raise InvalidParameterError("url", f"invalid Azure URL '{url}'. Expected {AZURE_URL_FORM}")
    return account, container, parts[2] if len(parts) > 2 else ""


def _build_azure_store(url: str) -> AzureStore:
    """Build an AzureStore for an ``az://<account>/<container>[/<path>]`` URL.

    Only the account and container are pinned from the URL. Credentials still come
    from the environment exactly as they did through ``obs.store.from_url``: every
    obstore constructor reads the same ``AZURE_STORAGE_*`` variables
    (``AZURE_STORAGE_ACCOUNT_KEY``/``AZURE_STORAGE_ACCESS_KEY``,
    ``AZURE_STORAGE_SAS_TOKEN``/``AZURE_STORAGE_SAS_KEY``, the client-credential
    ones, ``AZURE_USE_AZURE_CLI``). The account in the URL simply overrides
    ``AZURE_STORAGE_ACCOUNT_NAME``.
    """
    account, container, prefix = _split_azure_url(url)
    prefix_kwarg = {"prefix": prefix} if prefix else {}
    return AzureStore(container_name=container, account_name=account, **prefix_kwarg)


def _setup_store_and_kwargs(
    bucket_url: str,
    profile: str | None,
    chunk_concurrency: int,
    chunk_size: int | None,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_use_ssl: bool = True,
):
    """
    Setup object store and upload kwargs.

    Args:
        bucket_url: The object store bucket URL (e.g., s3://bucket)
        profile: AWS profile name (loads credentials from ~/.aws/credentials)
        chunk_concurrency: Max concurrent chunks per file
        chunk_size: Chunk size in bytes for multipart uploads
        s3_endpoint: Custom S3-compatible endpoint (e.g., "minio.example.com:9000")
        s3_region: S3 region (auto-detected from env var or profile config)
        s3_use_ssl: Whether to use HTTPS for S3 endpoint (default: True)

    Note: For S3, credentials come from botocore's standard chain (see
    :mod:`geoparquet_io.core.aws_credentials`), the same chain DuckDB reads use
    via ``PROVIDER credential_chain``: environment variables, the shared
    credentials file, ``--aws-profile``, SSO, assume-role, ``credential_process``
    and instance metadata. Naming a profile makes that profile win over
    environment keys, as it always has. When the chain finds nothing, no
    credential kwargs are passed and obstore resolves for itself.
    """
    if bucket_url.startswith("s3://"):
        bucket = bucket_url.replace("s3://", "").split("/")[0]

        credentials = resolve_aws_credentials(profile)

        # Determine region: explicit flag > env var > profile config > bucket heuristic
        region = s3_region
        if not region:
            region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        if not region:
            region = resolve_aws_region(profile)
        if not region:
            region = _try_infer_region_from_bucket(bucket)

        # Build S3Store with appropriate configuration
        store_kwargs = {"region": region} if region else {}

        if credentials:
            store_kwargs.update(credentials)

        if s3_endpoint:
            protocol = "https" if s3_use_ssl else "http"
            store_kwargs["endpoint"] = f"{protocol}://{s3_endpoint}"
            if not region:
                store_kwargs["region"] = "us-east-1"  # Default for custom endpoints

        store = S3Store(bucket, **store_kwargs)
    elif bucket_url.startswith("az://"):
        # Azure is built explicitly: obstore reads az:// as container-first, so
        # from_url() cannot serve gpio's account-first URLs (#864).
        store = _build_azure_store(bucket_url)
    else:
        # Other stores (GCS, HTTP)
        store = obs.store.from_url(bucket_url)

    kwargs = {"max_concurrency": chunk_concurrency}
    if chunk_size:
        kwargs["chunk_size"] = chunk_size
    return store, kwargs


def _upload_file_sync(store, source: Path, target_key: str, **kwargs) -> None:
    """Upload a single file synchronously and report progress."""
    file_size = source.stat().st_size
    size_mb = file_size / (1024 * 1024)

    progress(f"Uploading {source.name} ({size_mb:.2f} MB) → {target_key}")

    start_time = time.time()
    obs.put(store, target_key, source, max_concurrency=kwargs.get("max_concurrency", 12))
    elapsed = time.time() - start_time

    speed_mbps = size_mb / elapsed if elapsed > 0 else 0
    success(f"Upload complete ({speed_mbps:.2f} MB/s)")


class _UploadNotAttempted(Exception):
    """Marker for a file ``--fail-fast`` stopped before it was ever tried.

    It rides in the error slot of a result tuple so a not-attempted file is
    neither a success nor a failure when the summary is counted (#1019).
    """


def _upload_one_file(
    store,
    file_path: Path,
    source: Path,
    prefix: str,
    stop_requested: threading.Event | None = None,
    **kwargs,
) -> tuple[Path, Exception | None]:
    """Upload a single file and return result tuple for parallel processing.

    ``stop_requested`` is set only when ``--fail-fast`` is in force. Checking it
    here is what makes the stop reliable: cancelling the queued futures from the
    consumer loop races a worker that is already pulling the next file off the
    queue, and loses often enough that the same failing run cancelled six files
    on one machine and none on another. The first failure sets the flag before
    it returns, so every file a worker picks up afterwards stops here instead.
    """
    if stop_requested is not None and stop_requested.is_set():
        return file_path, _UploadNotAttempted()

    try:
        target_key = _build_target_key(file_path, source, prefix)
        file_size = file_path.stat().st_size
        size_mb = file_size / (1024 * 1024)
        start_time = time.time()

        progress(f"Uploading {file_path.name} ({size_mb:.2f} MB) → {target_key}")

        obs.put(store, target_key, file_path, max_concurrency=kwargs.get("max_concurrency", 12))

        elapsed = time.time() - start_time
        speed_mbps = size_mb / elapsed if elapsed > 0 else 0

        success(f"{file_path.name} ({speed_mbps:.2f} MB/s)")
        return file_path, None
    except Exception as e:
        error(f"{file_path.name}: {e}")
        if stop_requested is not None:
            stop_requested.set()
        return file_path, e


def _upload_directory_sync(
    store,
    source: Path,
    destination: str,
    prefix: str,
    files: list[Path],
    max_files: int,
    fail_fast: bool,
    **kwargs,
) -> None:
    """Upload all files in a directory with parallel uploads using threads.

    Args:
        store: obstore ObjectStore instance
        source: Source directory path
        destination: Object store URL the files are going to, named in the error
            raised when some of them do not get there
        prefix: S3/GCS/Azure prefix for uploaded files
        files: List of files to upload
        max_files: Max number of concurrent file uploads (must be >= 1)
        fail_fast: Stop on first error if True
        **kwargs: Additional arguments passed to obs.put

    Raises:
        RemoteAccessError: If any file failed or was never attempted
    """
    # Ensure max_files is at least 1 to avoid ThreadPoolExecutor ValueError
    max_files = max(1, max_files)

    total_size = sum(f.stat().st_size for f in files)
    total_size_mb = total_size / (1024 * 1024)
    progress(f"Found {len(files)} file(s) to upload ({total_size_mb:.2f} MB total)")

    stop_requested = threading.Event() if fail_fast else None
    results: list[tuple[Path, Exception | None]] = []
    cancelled: list[Path] = []
    with ThreadPoolExecutor(max_workers=max_files) as executor:
        futures = {
            executor.submit(
                _upload_one_file, store, f, source, prefix, stop_requested=stop_requested, **kwargs
            ): f
            for f in files
        }
        pending = set(futures)

        for future in as_completed(futures):
            pending.discard(future)
            result = future.result()
            results.append(result)
            if fail_fast and result[1] is not None:
                cancelled = _drain_remaining_uploads(futures, pending, results)
                break

    counts = _classify_upload_results(results, cancelled)
    _print_upload_summary(counts, len(files))
    _raise_if_upload_incomplete(destination, counts, len(files))


def _drain_remaining_uploads(
    futures: dict,
    pending: set,
    results: list[tuple[Path, Exception | None]],
) -> list[Path]:
    """End a ``--fail-fast`` run and return the files no worker ever saw.

    ``Future.cancel()`` only takes a file still queued. An upload already in
    flight cannot be called back -- its bytes are on their way -- so it is
    waited on and appended to ``results``, and counted by whether it actually
    reached the store. What comes back from here is only what was cancelled
    outright, which the summary reports as not attempted rather than as
    uploaded (#1019).
    """
    for future in pending:
        future.cancel()

    in_flight = [future for future in pending if not future.cancelled()]
    results.extend(future.result() for future in in_flight)
    return [futures[future] for future in pending if future.cancelled()]


def _classify_upload_results(
    results: list[tuple[Path, Exception | None]],
    cancelled: list[Path],
) -> tuple[int, int, int]:
    """Split what happened into (uploaded, failed, not attempted).

    A file is counted as uploaded only if it returned without an exception.
    Files stopped by ``--fail-fast`` -- cancelled while queued, or turned back
    at the start of their worker -- are their own category, because calling
    them either uploaded or failed misreports the state of the bucket.
    """
    uploaded = failed = 0
    not_attempted = len(cancelled)
    for _path, err in results:
        if err is None:
            uploaded += 1
        elif isinstance(err, _UploadNotAttempted):
            not_attempted += 1
        else:
            failed += 1
    return uploaded, failed, not_attempted


def _upload_single_file(
    source: Path,
    destination: str,
    bucket_url: str,
    prefix: str,
    profile: str | None,
    chunk_concurrency: int,
    chunk_size: int | None,
    dry_run: bool,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_use_ssl: bool = True,
) -> None:
    """Upload a single file."""
    target_key = _get_target_key(source, prefix, destination.endswith("/"))
    file_size = source.stat().st_size
    size_mb = file_size / (1024 * 1024)

    if dry_run:
        _print_single_file_dry_run(source, destination, target_key, size_mb, profile)
        return

    store, kwargs = _setup_store_and_kwargs(
        bucket_url, profile, chunk_concurrency, chunk_size, s3_endpoint, s3_region, s3_use_ssl
    )
    _upload_file_sync(store, source, target_key, **kwargs)


def _upload_directory(
    source: Path,
    destination: str,
    bucket_url: str,
    prefix: str,
    profile: str | None,
    pattern: str | None,
    max_files: int,
    chunk_concurrency: int,
    chunk_size: int | None,
    fail_fast: bool,
    dry_run: bool,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_use_ssl: bool = True,
) -> None:
    """Upload a directory of files.

    Raises:
        RemoteAccessError: If any file failed or was never attempted
    """
    files = list(source.rglob(pattern) if pattern else source.rglob("*"))
    files = [f for f in files if f.is_file()]

    if not files:
        print(f"No files found in {source}")
        return

    total_size = sum(f.stat().st_size for f in files)
    total_size_mb = total_size / (1024 * 1024)

    if dry_run:
        _print_directory_dry_run(
            files, source, destination, prefix, total_size_mb, pattern, profile
        )
        return

    store, kwargs = _setup_store_and_kwargs(
        bucket_url, profile, chunk_concurrency, chunk_size, s3_endpoint, s3_region, s3_use_ssl
    )
    _upload_directory_sync(
        store=store,
        source=source,
        destination=destination,
        prefix=prefix,
        files=files,
        max_files=max_files,
        fail_fast=fail_fast,
        **kwargs,
    )


def upload(
    source: Path,
    destination: str,
    profile: str | None = None,
    pattern: str | None = None,
    max_files: int = 4,
    chunk_concurrency: int = 12,
    chunk_size: int | None = None,
    fail_fast: bool = False,
    dry_run: bool = False,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_use_ssl: bool = True,
) -> None:
    """Upload file(s) to remote object storage using obstore.

    Args:
        source: Local file or directory path
        destination: Object store URL (e.g., s3://bucket/prefix/)
        profile: AWS profile name (only used for S3)
        pattern: Optional glob pattern for filtering files (e.g., "*.parquet")
        max_files: Max number of files to upload in parallel (for directories)
        chunk_concurrency: Max concurrent chunks per file (passed to obstore)
        chunk_size: Chunk size in bytes for multipart uploads (optional)
        fail_fast: If True, stop on first error; otherwise continue and report at end
        dry_run: If True, show what would be uploaded without actually uploading
        s3_endpoint: Custom S3-compatible endpoint (e.g., "minio.example.com:9000")
        s3_region: S3 region (default: us-east-1 when using custom endpoint)
        s3_use_ssl: Whether to use HTTPS for S3 endpoint (default: True)

    Raises:
        RemoteAccessError: If a directory upload left any file out of the
            destination -- failed, or never attempted because ``fail_fast``
            stopped the run. The summary printed first says how many of each.

    Examples:
        # Single file
        upload(Path("data.parquet"), "s3://bucket/data.parquet", profile="source-coop")

        # Directory (all files)
        upload(Path("output/"), "s3://bucket/dataset/", profile="source-coop")

        # Directory (only parquet)
        upload(Path("output/"), "s3://bucket/dataset/", pattern="*.parquet")

        # Custom S3 endpoint (MinIO, Rook/Ceph, source.coop)
        upload(
            Path("data.parquet"),
            "s3://bucket/data.parquet",
            s3_endpoint="minio.example.com:9000",
            s3_use_ssl=False,
        )
    """
    bucket_url, prefix = parse_object_store_url(destination)

    if source.is_file():
        _upload_single_file(
            source,
            destination,
            bucket_url,
            prefix,
            profile,
            chunk_concurrency,
            chunk_size,
            dry_run,
            s3_endpoint,
            s3_region,
            s3_use_ssl,
        )
    else:
        _upload_directory(
            source,
            destination,
            bucket_url,
            prefix,
            profile,
            pattern,
            max_files,
            chunk_concurrency,
            chunk_size,
            fail_fast,
            dry_run,
            s3_endpoint,
            s3_region,
            s3_use_ssl,
        )


def _build_target_key(file_path: Path, source: Path, prefix: str) -> str:
    """Build target key preserving directory structure."""
    rel_path = file_path.relative_to(source)
    if prefix:
        return f"{prefix.rstrip('/')}/{rel_path}"
    return str(rel_path)


def _print_upload_summary(counts: tuple[int, int, int], total_files: int) -> None:
    """Print summary of upload results.

    The uploaded count is counted from the files that actually returned without
    an exception, never derived as ``total_files - errors``: deriving it
    reported every file ``--fail-fast`` stopped as uploaded (#1019).
    """
    uploaded, failed, not_attempted = counts

    print(f"\n{'=' * 50}")
    print(f"✓ {uploaded}/{total_files} file(s) uploaded successfully")
    if failed:
        print(f"✗ {failed} file(s) failed")
    if not_attempted:
        print(f"⊘ {not_attempted} file(s) not attempted (stopped on first error)")


def _raise_if_upload_incomplete(
    destination: str,
    counts: tuple[int, int, int],
    total_files: int,
) -> None:
    """Fail the run when any file did not reach the store.

    Without this a directory upload printed its errors and still exited 0, so
    ``gpio publish upload … && echo ok`` printed ``ok`` over a bucket missing
    data (#1019). Partial and total failure raise alike: a caller branching on
    ``$?`` needs "the dataset is not all there", and the summary above says how
    much of it is there. Giving them separate exit codes would invent a
    convention gpio does not have -- 1 is a failure and 2 is Click's usage
    error, and nothing else is in use.
    """
    _uploaded, failed, not_attempted = counts
    if not failed and not not_attempted:
        return

    reason = f"{failed} of {total_files} file(s) failed to upload"
    if not_attempted:
        reason += f"; {not_attempted} not attempted (stopped on first error)"
    raise RemoteAccessError(destination, f"{reason}. See the errors above.")


def _get_target_key(source: Path, prefix: str, is_dir_destination: bool) -> str:
    """Determine the target key for a single file upload.

    Args:
        source: Source file path
        prefix: Prefix extracted from destination URL
        is_dir_destination: True if destination ends with '/'

    Returns:
        Target key for the object store
    """
    if is_dir_destination:
        # Destination is a directory, append filename
        return f"{prefix}/{source.name}".strip("/")
    else:
        # Destination is the exact key
        return prefix.strip("/")


def parse_object_store_url(url: str) -> tuple[str, str]:
    """Parse object store URL into (bucket_url, prefix).

    The bucket_url is what obstore needs to create a store.
    The prefix is the path within that bucket.

    Examples:
        s3://bucket/prefix/path -> (s3://bucket, prefix/path)
        gs://bucket/path -> (gs://bucket, path)
        az://account/container/path -> (az://account/container, path)

    Args:
        url: Full object store URL

    Returns:
        Tuple of (bucket_url, prefix)

    Raises:
        InvalidParameterError: If URL scheme is not supported, or an Azure URL
            is malformed or uses a spelling gpio cannot parse account-first
            (``abfs://``, ``abfss://``, ``azure://``)
    """
    if url.startswith("s3://"):
        parts = url[5:].split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return f"s3://{bucket}", prefix

    elif url.startswith("gs://"):
        parts = url[5:].split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return f"gs://{bucket}", prefix

    elif url.startswith("az://"):
        account, container, prefix = _split_azure_url(url)
        return f"az://{account}/{container}", prefix

    elif url.startswith(("https://", "http://")):
        # HTTP stores - may need different handling
        # For now, return as-is
        return url, ""

    elif url.startswith(("abfs://", "abfss://", "azure://")):
        # These spellings put the account and container in a different order
        # (or in a host name), so parsing them account-first would target the
        # wrong place. Name the supported form instead of guessing.
        raise InvalidParameterError(
            "url",
            f"cannot parse '{url}': gpio addresses Azure Blob Storage as "
            f"{AZURE_URL_FORM}. Rewrite the URL in that form.",
        )

    else:
        raise InvalidParameterError("url", f"unsupported URL scheme: {url}")
