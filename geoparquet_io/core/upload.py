"""Upload GeoParquet files to cloud object storage."""

import asyncio
import time
from pathlib import Path
from typing import Optional

import obstore as obs


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
    source: Path, destination: str, target_key: str, size_mb: float, profile: Optional[str]
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
    pattern: Optional[str],
    profile: Optional[str],
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
    bucket_url: str, profile: Optional[str], chunk_concurrency: int, chunk_size: Optional[int]
):
    """
    Setup object store and upload kwargs.

    Note: Profile handling is done via AWS_PROFILE env var set by the caller
    (see setup_aws_profile_if_needed in common.py). The obstore library
    automatically respects AWS_PROFILE along with other standard AWS SDK
    credential sources.
    """
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
    profile: Optional[str],
    chunk_concurrency: int,
    chunk_size: Optional[int],
    dry_run: bool,
) -> None:
    """Upload a single file."""
    target_key = _get_target_key(source, prefix, destination.endswith("/"))
    file_size = source.stat().st_size
    size_mb = file_size / (1024 * 1024)

    if dry_run:
        _print_single_file_dry_run(source, destination, target_key, size_mb, profile)
        return

    store, kwargs = _setup_store_and_kwargs(bucket_url, profile, chunk_concurrency, chunk_size)
    asyncio.run(_upload_file_with_progress(store, source, target_key, **kwargs))


def _upload_directory(
    source: Path,
    destination: str,
    bucket_url: str,
    prefix: str,
    profile: Optional[str],
    pattern: Optional[str],
    max_files: int,
    chunk_concurrency: int,
    chunk_size: Optional[int],
    fail_fast: bool,
    dry_run: bool,
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

    store, kwargs = _setup_store_and_kwargs(bucket_url, profile, chunk_concurrency, chunk_size)
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
    profile: Optional[str] = None,
    pattern: Optional[str] = None,
    max_files: int = 4,
    chunk_concurrency: int = 12,
    chunk_size: Optional[int] = None,
    fail_fast: bool = False,
    dry_run: bool = False,
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

    Examples:
        # Single file
        upload(Path("data.parquet"), "s3://bucket/data.parquet", profile="source-coop")

        # Directory (all files)
        upload(Path("output/"), "s3://bucket/dataset/", profile="source-coop")

        # Directory (only parquet)
        upload(Path("output/"), "s3://bucket/dataset/", pattern="*.parquet")
    """
    bucket_url, prefix = parse_object_store_url(destination)

    if source.is_file():
        _upload_single_file(
            source, destination, bucket_url, prefix, profile, chunk_concurrency, chunk_size, dry_run
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
        )


async def upload_file_async(
    store, source: Path, target_key: str, **kwargs
) -> tuple[Path, Optional[Exception]]:
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


def _find_files(source: Path, pattern: Optional[str]) -> list[Path]:
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
    pattern: Optional[str],
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
