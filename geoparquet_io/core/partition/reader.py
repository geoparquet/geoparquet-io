"""
Partition reading utilities for GeoParquet files.

Provides unified interface for reading partitioned datasets (directories,
glob patterns, hive-style) across all gpio commands.
"""

import os

from geoparquet_io.core.duckdb_utils import sql_path
from geoparquet_io.core.file_utils import (
    get_all_parquet_files,
    get_first_parquet_file,
    has_glob_pattern,
    is_partition_path,
    resolve_file_url,
    resolve_partition_path,
)
from geoparquet_io.core.logging_config import debug
from geoparquet_io.core.remote import is_remote_url


def require_parquet_files(path: str) -> None:
    """Name the empty dataset, rather than letting DuckDB blame its reader.

    An empty directory reached DuckDB as the directory itself and came back as
    ``Cannot read file: <dir> ... is a directory`` -- true, and no help at all
    in finding out that the directory simply holds nothing to read (#867). A
    glob that matches nothing is the same question with a different spelling.

    Remote paths are left alone: :func:`get_all_parquet_files` cannot enumerate
    them, and reports the URL itself as the single match.
    """
    from geoparquet_io.core.exceptions import GeoParquetError

    if is_remote_url(path):
        return
    if not get_all_parquet_files(path):
        raise GeoParquetError(f"No .parquet files found in: {path}")


def resolve_read_path(
    path: str,
    hive_input: bool | None = None,
    verbose: bool = False,
) -> tuple[str, dict]:
    """Return a RAW path DuckDB can actually read, plus the options it implies.

    A bare directory is not something DuckDB's reader can open -- it has to
    become the glob over the parquet files it holds, or the read dies with a
    ``Catalog Error`` naming the directory (#817). Single files and globs pass
    straight through.

    The pass-through is guarded by :func:`is_partition_path` rather than applied
    unconditionally, because ``resolve_partition_path`` also turns on hive
    partitioning for any path with a ``key=value`` directory in it: a single
    file at ``data/country=US/x.parquet`` must keep reading as itself, without
    the partition keys appearing as extra columns.

    The result is RAW, so the caller still owes it exactly one escape, and
    ``sql_path`` is where that happens.

    Args:
        path: File path, directory, or glob pattern (local or remote)
        hive_input: Explicitly enable/disable hive partitioning. None = auto-detect.
        verbose: Print debug messages

    Returns:
        tuple: (raw path for DuckDB, read_parquet options dict)

    Raises:
        GeoParquetError: When a local directory or glob holds no parquet files.
    """
    if not is_partition_path(path):
        return path, {}

    require_parquet_files(path)
    resolved_path, auto_options = resolve_partition_path(path, hive_input)
    if verbose:
        debug(f"Resolved partition path: {resolved_path}")
        if auto_options:
            debug(f"Auto-detected options: {auto_options}")
    return resolved_path, auto_options


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
    resolved_path, auto_options = resolve_read_path(path, hive_input, verbose=verbose)
    raw_path = resolve_file_url(resolved_path, verbose=False)

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
        return f"read_parquet({sql_path(raw_path)}, {options_str})"
    else:
        return f"read_parquet({sql_path(raw_path)})"


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
        GeoParquetError: If path is a partition
    """
    from geoparquet_io.core.exceptions import GeoParquetError

    if is_partition_path(path):
        raise GeoParquetError(
            f"Partitioned input detected: {path}\n\n"
            f"The '{command_name}' command requires a single parquet file as input.\n"
            "To work with partitioned data, first consolidate using:\n\n"
            f'    gpio extract "{path}" consolidated.parquet\n\n'
            "Then run this command on the consolidated file."
        )


def raise_for_schema_mismatch(exc: BaseException, path: str) -> None:
    """Re-raise DuckDB's multi-file schema-mismatch as a user-facing error.

    A directory whose parquet files disagree on their columns dies in DuckDB
    with an ``InvalidInputException`` ("schema mismatch in glob ... try
    setting union_by_name=True"). Reading with ``union_by_name`` implicitly
    would NULL-fill the union -- for a renamed geometry column that fabricates
    empty geometries -- so gpio points at the explicit reconciliation instead.

    A no-op for single-file inputs and for unrelated errors, so callers can
    invoke it first and fall through to their own handling.

    Args:
        exc: The DuckDB exception that interrupted the read
        path: The RAW input path the user gave (directory or glob)

    Raises:
        GeoParquetError: When ``path`` is multi-file and ``exc`` is DuckDB's
            schema-mismatch complaint.
    """
    from geoparquet_io.core.exceptions import GeoParquetError

    if not is_partition_path(path):
        return
    text = str(exc)
    if "schema mismatch" not in text.lower() and "union_by_name" not in text:
        return
    raise GeoParquetError(
        f"The parquet files matched by '{path}' do not share one schema, so "
        "they cannot be read together.\n\n"
        "Reconcile them into a single file first:\n\n"
        f'    gpio extract "{path}" merged.parquet --allow-schema-diff\n\n'
        f"then run this command on the merged file.\n\nOriginal error: {exc}"
    ) from exc


def get_files_to_check(
    path: str,
    check_all: bool = False,
    check_sample: int | None = None,
    verbose: bool = False,
    fix: bool = False,
) -> tuple[list[str], str]:
    """
    Get list of files to check from a partitioned dataset.

    Args:
        path: File path, directory, or glob pattern
        check_all: If True, return all files
        check_sample: If set, return first N files
        verbose: Print debug messages
        fix: If True, the caller is about to *repair* these files, so the
            first-file default below does not apply. Sampling is the right
            default for a read-only look at a partition that may hold ten
            thousand files; it is the wrong one for a repair, which silently
            fixed ``a.parquet`` of ``a``, ``b``, ``d`` and reported success
            (#1041). An explicit ``--sample-files N`` still wins -- that is the
            user naming the subset, not gpio guessing at one.

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

    if fix:
        notice = f"Fixing all {file_count} files in the input (--fix never samples)"
        return all_files, notice

    # Default: check first file only
    first_file = partition_info["first_file"]
    if first_file:
        notice = f"Checking first file (of {file_count} total). Use --all-files or --sample-files N for more."
        return [first_file], notice

    return [], "No parquet files found in partition"
