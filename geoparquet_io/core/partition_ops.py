"""Partition-level operations for applying transformations to multiple files."""

import asyncio
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional

import click
import obstore as obs

from geoparquet_io.core.common import is_remote_url, is_s3_url, is_gcs_url, is_azure_url


@dataclass
class PartitionResult:
    """Result of applying an operation to a partition."""

    total_files: int
    successful: int
    failed: int
    errors: Dict[str, Exception] = field(default_factory=dict)
    elapsed_time: float = 0.0
    output_path: Optional[str] = None

    def raise_if_any_failed(self):
        """Raise an exception if any files failed to process."""
        if self.failed > 0:
            error_summary = "\n".join(
                f"  - {path}: {err}" for path, err in list(self.errors.items())[:5]
            )
            if self.failed > 5:
                error_summary += f"\n  ... and {self.failed - 5} more errors"
            raise click.ClickException(
                f"Partition operation failed for {self.failed}/{self.total_files} files:\n{error_summary}"
            )


def detect_input_type(path: str) -> Literal["file", "local_partition", "remote_partition"]:
    """
    Detect whether a path is a single file, local directory, or remote partition.

    For remote URLs, a trailing '/' or lack of file extension indicates a partition.
    For local paths, checks if the path is a directory.

    Args:
        path: File path or URL to check

    Returns:
        One of: 'file', 'local_partition', 'remote_partition'
    """
    if is_remote_url(path):
        # Remote: if ends with '/' or has no extension in final component, treat as partition
        final_component = path.rstrip("/").split("/")[-1]
        if path.endswith("/") or "." not in final_component:
            return "remote_partition"
        return "file"
    else:
        # Local: check if path is a directory
        if os.path.isdir(path):
            return "local_partition"
        return "file"


def _list_local_files(partition_path: str, pattern: str = "**/*.parquet") -> List[str]:
    """List files in a local directory matching the pattern."""
    path = Path(partition_path)
    files = list(path.glob(pattern))
    return [str(f) for f in files if f.is_file()]


def _get_store_and_prefix(url: str, profile: Optional[str] = None):
    """Get obstore and prefix from a remote URL."""
    from geoparquet_io.core.upload import parse_object_store_url

    # Set AWS profile if needed
    if profile and is_s3_url(url):
        os.environ["AWS_PROFILE"] = profile

    bucket_url, prefix = parse_object_store_url(url)
    store = obs.store.from_url(bucket_url)
    return store, prefix


async def _list_remote_files_async(
    partition_path: str, pattern: str = "*.parquet", profile: Optional[str] = None
) -> List[str]:
    """List files in a remote partition matching the pattern."""
    store, prefix = _get_store_and_prefix(partition_path, profile)

    # Ensure prefix ends without trailing slash for listing
    prefix = prefix.rstrip("/")

    # List all objects under the prefix
    all_files = []

    # Use list_with_delimiter for recursive listing
    result = await obs.list_async(store, prefix=prefix if prefix else None)

    for obj in result:
        obj_path = obj["path"] if isinstance(obj, dict) else obj.path
        # Filter by pattern (simple suffix matching for now)
        if pattern == "*.parquet" or pattern == "**/*.parquet":
            if obj_path.endswith(".parquet"):
                # Reconstruct full URL
                if partition_path.startswith("s3://"):
                    bucket = partition_path.split("/")[2]
                    all_files.append(f"s3://{bucket}/{obj_path}")
                elif partition_path.startswith("gs://") or partition_path.startswith("gcs://"):
                    bucket = partition_path.split("/")[2]
                    scheme = "gs" if partition_path.startswith("gs://") else "gcs"
                    all_files.append(f"{scheme}://{bucket}/{obj_path}")
                elif partition_path.startswith(("az://", "azure://", "abfs://", "abfss://")):
                    # Azure URLs are more complex, preserve original scheme
                    scheme = partition_path.split("://")[0]
                    parts = partition_path.split("/")
                    account_container = "/".join(parts[2:4])
                    all_files.append(f"{scheme}://{account_container}/{obj_path}")

    return all_files


def discover_partition_files(
    partition_path: str,
    pattern: str = "**/*.parquet",
    profile: Optional[str] = None,
) -> List[str]:
    """
    Discover all parquet files in a partition (local or remote).

    Args:
        partition_path: Path to the partition directory (local or remote URL)
        pattern: Glob pattern for matching files (default: **/*.parquet)
        profile: AWS profile name for S3 access (optional)

    Returns:
        List of file paths/URLs in the partition
    """
    input_type = detect_input_type(partition_path)

    if input_type == "file":
        raise click.ClickException(
            f"Expected a partition directory, but got a file: {partition_path}"
        )
    elif input_type == "local_partition":
        return _list_local_files(partition_path, pattern)
    else:  # remote_partition
        return asyncio.run(_list_remote_files_async(partition_path, pattern, profile))


def _get_output_path_for_file(
    input_file: str,
    input_partition: str,
    output_partition: Optional[str],
    preserve_structure: bool,
) -> str:
    """Calculate the output path for a single file."""
    if output_partition is None:
        # In-place modification
        return input_file

    if not preserve_structure:
        # Flat output - just use the filename
        filename = os.path.basename(input_file)
        if is_remote_url(output_partition):
            return f"{output_partition.rstrip('/')}/{filename}"
        else:
            return os.path.join(output_partition, filename)

    # Preserve directory structure
    input_partition_normalized = input_partition.rstrip("/")
    output_partition_normalized = output_partition.rstrip("/")

    if is_remote_url(input_file):
        # Remote file - extract relative path
        # Find where input_partition ends in the file path
        rel_path = input_file[len(input_partition_normalized) :].lstrip("/")
    else:
        # Local file
        rel_path = os.path.relpath(input_file, input_partition_normalized)

    if is_remote_url(output_partition):
        return f"{output_partition_normalized}/{rel_path}"
    else:
        return os.path.join(output_partition_normalized, rel_path)


def _ensure_output_dir(output_path: str) -> None:
    """Ensure the parent directory exists for local output paths."""
    if not is_remote_url(output_path):
        parent_dir = os.path.dirname(output_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)


def _apply_operation_single_sync(
    operation_fn: Callable,
    input_file: str,
    output_file: str,
    verbose: bool,
    **operation_kwargs: Any,
) -> tuple[str, Optional[Exception]]:
    """Apply an operation to a single file synchronously."""
    try:
        # Ensure output directory exists
        _ensure_output_dir(output_file)

        # Run the operation
        operation_fn(input_file, output_file, **operation_kwargs)

        if verbose:
            click.echo(f"✓ Processed: {os.path.basename(input_file)}")

        return input_file, None
    except Exception as e:
        click.echo(f"✗ Failed: {os.path.basename(input_file)}: {e}")
        return input_file, e


def apply_operation_to_partition(
    operation_fn: Callable,
    input_partition: str,
    output_partition: Optional[str] = None,
    file_pattern: str = "**/*.parquet",
    concurrency: int = 4,
    error_handling: Literal["fail_fast", "continue"] = "fail_fast",
    preserve_structure: bool = True,
    profile: Optional[str] = None,
    verbose: bool = False,
    **operation_kwargs: Any,
) -> PartitionResult:
    """
    Apply an operation to all files in a partition with parallel processing.

    Uses sequential processing to avoid DuckDB thread-safety issues.
    For true parallelism with DuckDB operations, use concurrency=1 or
    consider using multiprocessing.

    Args:
        operation_fn: Function to apply to each file. Must have signature:
                     fn(input_path, output_path, **kwargs)
        input_partition: Path to input partition (local dir or remote URL ending in /)
        output_partition: Path to output partition. If None, modifies files in-place.
        file_pattern: Glob pattern for matching files (default: **/*.parquet)
        concurrency: Number of files to process in parallel (default: 4)
                    Note: Currently processes sequentially due to DuckDB thread safety.
        error_handling: 'fail_fast' stops on first error, 'continue' processes all files
        preserve_structure: If True, maintains subdirectory structure in output
        profile: AWS profile name for S3 access (optional)
        verbose: Whether to print progress for each file
        **operation_kwargs: Additional arguments passed to operation_fn

    Returns:
        PartitionResult with processing statistics

    Example:
        from geoparquet_io.core.add_bbox_column import _add_bbox_column_single

        result = apply_operation_to_partition(
            operation_fn=_add_bbox_column_single,
            input_partition="./partitions/",
            output_partition="./output/",
            concurrency=8,
            bbox_column_name="bbox",
        )
        print(f"Processed {result.successful} files")
    """
    start_time = time.time()

    # Discover files
    files = discover_partition_files(input_partition, file_pattern, profile)

    if not files:
        click.echo(f"No files found matching pattern '{file_pattern}' in {input_partition}")
        return PartitionResult(
            total_files=0,
            successful=0,
            failed=0,
            elapsed_time=time.time() - start_time,
            output_path=output_partition,
        )

    click.echo(f"Found {len(files)} file(s) to process")

    # Build list of (input, output) pairs
    file_pairs = []
    for input_file in files:
        output_file = _get_output_path_for_file(
            input_file, input_partition, output_partition, preserve_structure
        )
        file_pairs.append((input_file, output_file))

    # Process files sequentially to avoid DuckDB thread-safety issues
    # DuckDB connections are not thread-safe when used concurrently
    results = []
    for input_file, output_file in file_pairs:
        result = _apply_operation_single_sync(
            operation_fn, input_file, output_file, verbose, **operation_kwargs
        )
        results.append(result)

        # Check for fail-fast
        if error_handling == "fail_fast" and result[1] is not None:
            break

    # Collect results
    errors = {path: err for path, err in results if err is not None}
    successful = len(results) - len(errors)

    elapsed = time.time() - start_time

    # Print summary
    click.echo(f"\n{'=' * 50}")
    click.echo(f"✓ {successful}/{len(files)} file(s) processed successfully")
    if errors:
        click.echo(f"✗ {len(errors)} file(s) failed")
    click.echo(f"Time: {elapsed:.1f}s")

    return PartitionResult(
        total_files=len(files),
        successful=successful,
        failed=len(errors),
        errors=errors,
        elapsed_time=elapsed,
        output_path=output_partition,
    )
