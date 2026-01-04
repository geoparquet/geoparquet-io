"""Upload GeoParquet files to cloud object storage."""

import asyncio
import configparser
import os
import re
import time
from pathlib import Path

import obstore as obs
from obstore.store import S3Store


def _load_aws_credentials_from_profile(
    profile: str = "default",
) -> tuple[str | None, str | None, str | None]:
    """Load AWS credentials from ~/.aws/credentials file.

    Uses Python's built-in configparser to read credentials without requiring boto3.

    Args:
        profile: AWS profile name (default: "default")

    Returns:
        Tuple of (access_key_id, secret_access_key, region)
        Any value may be None if not found.
    """
    creds_file = Path.home() / ".aws" / "credentials"
    config_file = Path.home() / ".aws" / "config"

    access_key = None
    secret_key = None
    region = None

    # Read credentials
    if creds_file.exists():
        parser = configparser.ConfigParser()
        parser.read(creds_file)

        if profile in parser.sections():
            section = parser[profile]
            access_key = section.get("aws_access_key_id")
            secret_key = section.get("aws_secret_access_key")
        elif profile == "default" and "DEFAULT" in parser:
            access_key = parser["DEFAULT"].get("aws_access_key_id")
            secret_key = parser["DEFAULT"].get("aws_secret_access_key")

    # Read region from config
    if config_file.exists():
        config = configparser.ConfigParser()
        config.read(config_file)

        # Profile sections in config are named "profile <name>" except for default
        profile_section = profile if profile == "default" else f"profile {profile}"
        if profile_section in config.sections():
            region = config[profile_section].get("region")
        elif profile == "default" and "DEFAULT" in config:
            region = config["DEFAULT"].get("region")

    return access_key, secret_key, region


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


def _check_s3_credentials(profile: str | None = None) -> tuple[bool, str]:
    """Check if S3 credentials are available.

    Args:
        profile: AWS profile name to check (optional)

    Returns:
        Tuple of (credentials_found, hint_message)
    """
    # If profile specified, check credentials file
    if profile:
        access_key, secret_key, _ = _load_aws_credentials_from_profile(profile)
        if access_key and secret_key:
            return True, ""
        else:
            hints = []
            hints.append(f"AWS profile '{profile}' not found or incomplete.")
            hints.append("")
            hints.append("Ensure your ~/.aws/credentials file has this profile:")
            hints.append(f"  [{profile}]")
            hints.append("  aws_access_key_id = YOUR_ACCESS_KEY")
            hints.append("  aws_secret_access_key = YOUR_SECRET_KEY")
            hints.append("")
            hints.append("Or use environment variables instead:")
            hints.append("  export AWS_ACCESS_KEY_ID=your_access_key")
            hints.append("  export AWS_SECRET_ACCESS_KEY=your_secret_key")
            return False, "\n".join(hints)

    # Check environment variables first
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    if access_key and secret_key:
        return True, ""

    # Fall back to default profile in ~/.aws/credentials
    access_key, secret_key, _ = _load_aws_credentials_from_profile("default")
    if access_key and secret_key:
        return True, ""

    hints = []
    hints.append("S3 credentials not found. To configure credentials:")
    hints.append("")
    hints.append("Option 1: Set environment variables")
    hints.append("  export AWS_ACCESS_KEY_ID=your_access_key")
    hints.append("  export AWS_SECRET_ACCESS_KEY=your_secret_key")
    hints.append("  export AWS_REGION=us-west-2  # required for most buckets")
    hints.append("")
    hints.append("Option 2: Use --profile flag with AWS credentials file")
    hints.append("  gpio publish upload file.parquet s3://bucket/path --profile myprofile")
    hints.append("")
    hints.append("Option 3: Configure AWS CLI")
    hints.append("  aws configure")

    return False, "\n".join(hints)


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


def _check_azure_credentials() -> tuple[bool, str]:
    """Check if Azure credentials are available.

    Returns:
        Tuple of (credentials_found, hint_message)
    """
    # Check for various Azure credential env vars
    account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
    sas_token = os.environ.get("AZURE_STORAGE_SAS_TOKEN")
    client_id = os.environ.get("AZURE_CLIENT_ID")

    if account_key or sas_token or client_id:
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
    hints.append("Option 3: Use Azure CLI")
    hints.append("  az login")

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


async def _upload_file_with_progress(store, source: Path, target_key: str, **kwargs) -> None:
    """Upload a single file and report results."""
    file_size = source.stat().st_size
    size_mb = file_size / (1024 * 1024)

    print(f"⬆ Uploading {source.name} ({size_mb:.2f} MB) → {target_key}")

    start_time = time.time()
    await obs.put_async(
        store, target_key, source, max_concurrency=kwargs.get("max_concurrency", 12)
    )
    elapsed = time.time() - start_time

    speed_mbps = size_mb / elapsed if elapsed > 0 else 0
    print(f"✓ Upload complete ({speed_mbps:.2f} MB/s)")


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

    Note: For S3, credentials are loaded from (in order):
    1. --profile flag (reads ~/.aws/credentials)
    2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    3. Default profile in ~/.aws/credentials (automatic fallback)
    """
    if bucket_url.startswith("s3://"):
        bucket = bucket_url.replace("s3://", "").split("/")[0]

        # Load credentials from profile, environment, or default profile
        access_key = None
        secret_key = None
        profile_region = None

        if profile:
            access_key, secret_key, profile_region = _load_aws_credentials_from_profile(profile)
        else:
            # Try environment variables first
            access_key = os.environ.get("AWS_ACCESS_KEY_ID")
            secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

            # Fall back to default profile if no env vars
            if not (access_key and secret_key):
                access_key, secret_key, profile_region = _load_aws_credentials_from_profile(
                    "default"
                )

        # Determine region: explicit flag > env var > profile config > bucket heuristic
        region = s3_region
        if not region:
            region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        if not region and profile_region:
            region = profile_region
        if not region:
            region = _try_infer_region_from_bucket(bucket)

        # Build S3Store with appropriate configuration
        store_kwargs = {"region": region} if region else {}

        if access_key and secret_key:
            store_kwargs["access_key_id"] = access_key
            store_kwargs["secret_access_key"] = secret_key

        if s3_endpoint:
            protocol = "https" if s3_use_ssl else "http"
            store_kwargs["endpoint"] = f"{protocol}://{s3_endpoint}"
            if not region:
                store_kwargs["region"] = "us-east-1"  # Default for custom endpoints

        store = S3Store(bucket, **store_kwargs)
    else:
        # Non-S3 stores (GCS, Azure, HTTP)
        store = obs.store.from_url(bucket_url)

    kwargs = {"max_concurrency": chunk_concurrency}
    if chunk_size:
        kwargs["chunk_size"] = chunk_size
    return store, kwargs


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
    asyncio.run(_upload_file_with_progress(store, source, target_key, **kwargs))


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
    """Upload a directory of files."""
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
    asyncio.run(
        upload_directory_async(
            store=store,
            source=source,
            prefix=prefix,
            pattern=pattern,
            max_files=max_files,
            fail_fast=fail_fast,
            **kwargs,
        )
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


async def upload_file_async(
    store, source: Path, target_key: str, **kwargs
) -> tuple[Path, Exception | None]:
    """Upload a single file asynchronously.

    Returns:
        Tuple of (source_path, error). Error is None on success.
    """
    file_size = source.stat().st_size
    size_mb = file_size / (1024 * 1024)

    try:
        print(f"⬆ Uploading {source.name} ({size_mb:.2f} MB) → {target_key}")
        start_time = time.time()

        await obs.put_async(store, target_key, source, **kwargs)

        elapsed = time.time() - start_time
        speed_mbps = size_mb / elapsed if elapsed > 0 else 0

        print(f"✓ {source.name} ({speed_mbps:.2f} MB/s)")
        return source, None
    except Exception as e:
        print(f"✗ {source.name}: {e}")
        return source, e


def _find_files(source: Path, pattern: str | None) -> list[Path]:
    """Find all files in directory matching pattern."""
    files = list(source.rglob(pattern) if pattern else source.rglob("*"))
    return [f for f in files if f.is_file()]


def _build_target_key(file_path: Path, source: Path, prefix: str) -> str:
    """Build target key preserving directory structure."""
    rel_path = file_path.relative_to(source)
    if prefix:
        return f"{prefix.rstrip('/')}/{rel_path}"
    return str(rel_path)


async def _upload_files_parallel(
    store, files: list[Path], source: Path, prefix: str, max_files: int, fail_fast: bool, **kwargs
):
    """Upload files in parallel with semaphore."""
    semaphore = asyncio.Semaphore(max_files)

    async def upload_with_semaphore(file_path: Path):
        async with semaphore:
            target_key = _build_target_key(file_path, source, prefix)
            return await upload_file_async(store, file_path, target_key, **kwargs)

    if fail_fast:
        results = await asyncio.gather(*[upload_with_semaphore(f) for f in files])
    else:
        results = await asyncio.gather(
            *[upload_with_semaphore(f) for f in files], return_exceptions=False
        )
    return results


def _print_upload_summary(results: list, total_files: int) -> None:
    """Print summary of upload results."""
    errors = [(path, err) for path, err in results if err is not None]
    success_count = total_files - len(errors)

    print(f"\n{'=' * 50}")
    print(f"✓ {success_count}/{total_files} file(s) uploaded successfully")
    if errors:
        print(f"✗ {len(errors)} file(s) failed")


async def upload_directory_async(
    store,
    source: Path,
    prefix: str,
    pattern: str | None,
    max_files: int,
    fail_fast: bool,
    **kwargs,
):
    """Upload all files in a directory with parallel uploads.

    Args:
        store: obstore ObjectStore instance
        source: Source directory path
        prefix: S3/GCS/Azure prefix for uploaded files
        pattern: Optional glob pattern (e.g., "*.parquet", "**/*.json")
        max_files: Max number of concurrent file uploads
        fail_fast: Stop on first error if True
        **kwargs: Additional arguments passed to obs.put_async
    """
    files = _find_files(source, pattern)

    if not files:
        print(f"No files found in {source}")
        return

    total_size = sum(f.stat().st_size for f in files)
    total_size_mb = total_size / (1024 * 1024)
    print(f"Found {len(files)} file(s) to upload ({total_size_mb:.2f} MB total)")
    print()

    results = await _upload_files_parallel(
        store, files, source, prefix, max_files, fail_fast, **kwargs
    )
    _print_upload_summary(results, len(files))


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
        ValueError: If URL scheme is not supported
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
        # Azure: az://account/container/path
        parts = url[5:].split("/", 2)
        if len(parts) < 2:
            raise ValueError(f"Invalid Azure URL: {url}. Expected az://account/container/path")
        account, container = parts[0], parts[1]
        prefix = parts[2] if len(parts) > 2 else ""
        return f"az://{account}/{container}", prefix

    elif url.startswith(("https://", "http://")):
        # HTTP stores - may need different handling
        # For now, return as-is
        return url, ""

    else:
        raise ValueError(f"Unsupported URL scheme: {url}")
