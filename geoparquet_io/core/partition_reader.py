"""
Partition reading utilities for GeoParquet files.

Provides unified interface for reading partitioned datasets (directories,
glob patterns, hive-style) across all gpio commands.
"""

import os

from geoparquet_io.core.common import (
    get_all_parquet_files,
    get_first_parquet_file,
    has_glob_pattern,
    is_partition_path,
    is_remote_url,
    resolve_partition_path,
    safe_file_url,
)
from geoparquet_io.core.logging_config import debug


def build_read_parquet_expr(
    path: str,
    allow_schema_diff: bool = False,
    hive_input: bool | None = None,
    verbose: bool = False,
) -> str:
    """
    Build a DuckDB read_parquet() expression for the given path.

    Handles:
    - Single files
    - Glob patterns
    - Directories (auto-converts to glob)
    - Hive-style partitioning
    - Union by name for schema differences

    Args:
        path: File path, directory, or glob pattern (local or remote)
        allow_schema_diff: If True, add union_by_name=true for schema merging
        hive_input: Explicitly enable/disable hive partitioning. None = auto-detect.
        verbose: Print debug messages

    Returns:
        str: DuckDB read_parquet() expression like
             "read_parquet('path/*.parquet', hive_partitioning=true)"
    """
    # Check if this is a partition path and resolve it
    if is_partition_path(path):
        resolved_path, auto_options = resolve_partition_path(path, hive_input)
        safe_path = safe_file_url(resolved_path, verbose=False)

        if verbose:
            debug(f"Resolved partition path: {resolved_path}")
            if auto_options:
                debug(f"Auto-detected options: {auto_options}")
    else:
        safe_path = safe_file_url(path, verbose=False)
        auto_options = {}

    # Build options list
    options = []

    # Hive partitioning
    if hive_input is True or auto_options.get("hive_partitioning"):
        options.append("hive_partitioning=true")

    # Union by name for schema differences
    if allow_schema_diff:
        options.append("union_by_name=true")

    # Build expression
    if options:
        options_str = ", ".join(options)
        return f"read_parquet('{safe_path}', {options_str})"
    else:
        return f"read_parquet('{safe_path}')"


def get_partition_info(path: str, verbose: bool = False) -> dict:
    """
    Get information about a partitioned dataset.

    Args:
        path: File path, directory, or glob pattern
        verbose: Print debug messages

    Returns:
        dict with:
            - is_partition: bool - True if this is a partitioned dataset
            - file_count: int - Number of parquet files (1 for remote globs)
            - first_file: str|None - Path to first file for metadata
            - all_files: list[str] - All parquet file paths
            - partition_type: str - 'single', 'flat', 'hive', 'glob', or 'remote_glob'
            - resolved_path: str - Resolved path/glob for DuckDB
    """
    info_dict = {
        "is_partition": False,
        "file_count": 1,
        "first_file": path,
        "all_files": [path],
        "partition_type": "single",
        "resolved_path": path,
    }

    if not is_partition_path(path):
        return info_dict

    info_dict["is_partition"] = True

    # Handle remote paths
    if is_remote_url(path):
        if has_glob_pattern(path):
            info_dict["partition_type"] = "remote_glob"
            info_dict["first_file"] = path  # Can't enumerate remote
            info_dict["all_files"] = [path]
            info_dict["file_count"] = 1  # Unknown for remote
            info_dict["resolved_path"] = path
        return info_dict

    # Handle local paths
    info_dict["first_file"] = get_first_parquet_file(path)
    info_dict["all_files"] = get_all_parquet_files(path)
    info_dict["file_count"] = len(info_dict["all_files"])

    # Determine partition type
    if has_glob_pattern(path):
        info_dict["partition_type"] = "glob"
        info_dict["resolved_path"] = path
    elif os.path.isdir(path):
        resolved, options = resolve_partition_path(path)
        info_dict["resolved_path"] = resolved
        if options.get("hive_partitioning"):
            info_dict["partition_type"] = "hive"
        else:
            info_dict["partition_type"] = "flat"

    if verbose:
        debug(f"Partition info: {info_dict['partition_type']}, {info_dict['file_count']} files")

    return info_dict


def require_single_file(path: str, command_name: str) -> None:
    """
    Check if path is a partition and raise helpful error if so.

    Used by commands that don't support partition input to provide
    guidance to users.

    Args:
        path: Input file path to check
        command_name: Name of the command for error message

    Raises:
        click.ClickException: If path is a partition
    """
    import click

    if is_partition_path(path):
        raise click.ClickException(
            f"Partitioned input detected: {path}\n\n"
            f"The '{command_name}' command requires a single parquet file as input.\n"
            "To work with partitioned data, first consolidate using:\n\n"
            f'    gpio extract "{path}" consolidated.parquet\n\n'
            "Then run this command on the consolidated file."
        )


def get_files_to_check(
    path: str,
    check_all: bool = False,
    check_sample: int | None = None,
    verbose: bool = False,
) -> tuple[list[str], str]:
    """
    Get list of files to check from a partitioned dataset.

    Args:
        path: File path, directory, or glob pattern
        check_all: If True, return all files
        check_sample: If set, return first N files
        verbose: Print debug messages

    Returns:
        tuple: (files_to_check, notice_message)
            - files_to_check: List of file paths to check
            - notice_message: Informational message about what's being checked (or empty)
    """
    partition_info = get_partition_info(path, verbose)

    if not partition_info["is_partition"]:
        # Single file - just return it
        return [partition_info["first_file"]], ""

    all_files = partition_info["all_files"]
    file_count = partition_info["file_count"]

    # For remote globs, we can only check the glob pattern as a whole
    if partition_info["partition_type"] == "remote_glob":
        notice = "Checking remote glob pattern (file enumeration not supported)"
        return [path], notice

    if check_all:
        notice = f"Checking all {file_count} files in partition"
        return all_files, notice

    if check_sample is not None:
        sample_count = min(check_sample, file_count)
        files = all_files[:sample_count]
        notice = f"Checking sample of {sample_count} files (out of {file_count} total)"
        return files, notice

    # Default: check first file only
    first_file = partition_info["first_file"]
    if first_file:
        notice = f"Checking first file (of {file_count} total). Use --all-files or --sample-files N for more."
        return [first_file], notice

    return [], "No parquet files found in partition"
