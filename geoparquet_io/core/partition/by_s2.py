#!/usr/bin/env python3

"""
Partition GeoParquet files by S2 cell IDs.

Uses DuckDB's geography extension to compute S2 cell IDs and partition data
into separate files based on cell boundaries.
"""

from __future__ import annotations

import os
import tempfile
import uuid

from geoparquet_io.core.add.s2 import add_s2_column
from geoparquet_io.core.constants import (
    DEFAULT_S2_COLUMN_NAME,
    DEFAULT_S2_COMPRESSION_LEVEL,
)
from geoparquet_io.core.duckdb_metadata import get_column_names
from geoparquet_io.core.duckdb_utils import require_community_extension
from geoparquet_io.core.exceptions import InvalidParameterError, PartitionError
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    info,
    progress,
    success,
    warn,
)
from geoparquet_io.core.partition.auto_resolution import calculate_auto_resolution
from geoparquet_io.core.partition.common import (
    calculate_partition_stats,
    partition_by_column,
    preview_partition,
    raise_if_no_rows,
)
from geoparquet_io.core.streaming import is_stdin, read_stdin_to_temp_file


def _ensure_s2_column(input_parquet, s2_column_name, level, verbose):
    """Ensure S2 column exists, adding it if needed.

    Returns:
        tuple: (input_file_to_use, column_existed, temp_file_or_none)
    """

    column_names = get_column_names(input_parquet)

    if s2_column_name in column_names:
        if verbose:
            debug(f"Using existing S2 column '{s2_column_name}'")
        return input_parquet, True, None

    if verbose:
        debug(f"S2 column '{s2_column_name}' not found. Adding it now...")

    temp_file = os.path.join(
        tempfile.gettempdir(),
        f"s2_enriched_{uuid.uuid4().hex}_{os.path.basename(input_parquet)}",
    )

    try:
        add_s2_column(
            input_parquet=input_parquet,
            output_parquet=temp_file,
            s2_column_name=s2_column_name,
            s2_level=level,
            verbose=verbose,
            compression="ZSTD",
            compression_level=DEFAULT_S2_COMPRESSION_LEVEL,
        )
        if verbose:
            debug(f"S2 column added successfully at level {level}")
        return temp_file, False, temp_file
    except Exception as e:
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise PartitionError(f"Failed to add S2 column: {str(e)}") from e


def _run_preview(input_parquet, s2_column_name, preview_limit, verbose):
    """Run partition preview and analysis."""
    from geoparquet_io.core.partition.common import (
        PartitionAnalysisError,
        analyze_partition_strategy,
    )

    try:
        analyze_partition_strategy(
            input_parquet=input_parquet,
            column_name=s2_column_name,
            column_prefix_length=None,
            verbose=True,
        )
    except PartitionAnalysisError:
        pass
    except Exception as e:
        warn(f"\nAnalysis error: {e}")

    progress("\n" + "=" * 70)
    preview_partition(
        input_parquet=input_parquet,
        column_name=s2_column_name,
        column_prefix_length=None,
        limit=preview_limit,
        verbose=verbose,
    )


def _partition_with_temp_file(
    working_parquet: str,
    temp_file: str | None,
    output_folder: str,
    s2_column_name: str,
    hive: bool,
    overwrite: bool,
    verbose: bool,
    keep_s2_column: bool,
    force: bool,
    skip_analysis: bool,
    filename_prefix: str | None,
    profile: str | None,
    geoparquet_version: str | None,
    compression: str,
    compression_level: int,
    row_group_size_mb: int | None,
    row_group_rows: int | None,
    memory_limit: str | None,
) -> int:
    """Execute partitioning and return partition count.

    Handles temp file cleanup automatically.
    """
    try:
        num_partitions = partition_by_column(
            input_parquet=working_parquet,
            output_folder=output_folder,
            column_name=s2_column_name,
            column_prefix_length=None,
            hive=hive,
            overwrite=overwrite,
            verbose=verbose,
            keep_partition_column=keep_s2_column,
            force=force,
            skip_analysis=skip_analysis,
            filename_prefix=filename_prefix,
            profile=profile,
            geoparquet_version=geoparquet_version,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            memory_limit=memory_limit,
        )
        return num_partitions
    finally:
        if temp_file and os.path.exists(temp_file):
            if verbose:
                debug("Cleaning up temporary S2-enriched file...")
            os.remove(temp_file)


def partition_by_s2(
    input_parquet: str,
    output_folder: str,
    s2_column_name: str = DEFAULT_S2_COLUMN_NAME,
    level: int | None = None,
    hive: bool = False,
    overwrite: bool = False,
    preview: bool = False,
    preview_limit: int = 15,
    verbose: bool = False,
    keep_s2_column: bool | None = None,
    force: bool = False,
    skip_analysis: bool = False,
    filename_prefix: str | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    compression: str = "ZSTD",
    compression_level: int = 15,
    row_group_size_mb: int | None = None,
    row_group_rows: int | None = None,
    memory_limit: str | None = None,
    auto: bool = False,
    target_rows: int = 100000,
    max_partitions: int = 10000,
) -> None:
    """
    Partition a GeoParquet file by S2 cells at specified level.

    Supports Arrow IPC streaming for input:
    - Input "-" reads from stdin (output is always a directory)

    Auto-resolution mode:
    - Use --auto to automatically calculate optimal level based on data size
    - Specify --target-rows to control partition size (default: 100,000 rows)
    - Specify --max-partitions to limit total partition count (default: 10,000)

    Args:
        input_parquet: Input GeoParquet file (local, remote URL, or "-" for stdin)
        output_folder: Output directory (always writes to directory, no stdout support)
        s2_column_name: Name of the S2 column (default: 's2_cell')
        level: S2 level (0-30). Required unless --auto is used
        hive: Use Hive-style partitioning (column=value directories)
        overwrite: Overwrite existing output directory
        preview: Preview partition distribution without writing
        preview_limit: Max number of partitions to show in preview
        verbose: Print verbose output
        keep_s2_column: Keep S2 column in output partitions
        force: Force operation even if analysis suggests issues
        skip_analysis: Skip partition analysis
        filename_prefix: Prefix for output filenames
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write
        compression: Compression codec (default: ZSTD)
        compression_level: Compression level (default: 15)
        row_group_size_mb: Row group size in MB (mutually exclusive with row_group_rows)
        row_group_rows: Row group size in number of rows (mutually exclusive with row_group_size_mb)
        memory_limit: DuckDB memory limit for write operations (e.g., "2GB")
        auto: Automatically calculate optimal level (default: False)
        target_rows: Target rows per partition for auto mode (default: 100000)
        max_partitions: Maximum partitions for auto mode (default: 10000)
    """
    configure_verbose(verbose)

    # Validate level parameter before consuming stdin or probing extensions.
    if auto and level is not None:
        raise InvalidParameterError("auto", "cannot specify both --auto and --level")

    if not auto and level is None:
        raise InvalidParameterError("level", "must specify either --level or --auto")

    # S2 cell computation is impossible without 'geography'; fail before reading
    # input (or draining stdin) rather than partway through the pipeline (#737).
    require_community_extension("geography", feature="gpio partition s2")

    # Handle stdin input first (before level calculation)
    stdin_temp_file = None
    actual_input = input_parquet

    if is_stdin(input_parquet):
        stdin_temp_file = read_stdin_to_temp_file(verbose)
        actual_input = stdin_temp_file

    # Wrap all operations in try/finally to ensure stdin temp file cleanup
    try:
        # Nothing to partition: stop before rewriting the whole input just
        # to add an index column to it (#823).
        raise_if_no_rows(actual_input)

        # Calculate auto level if requested
        if auto:
            level = calculate_auto_resolution(
                input_parquet=actual_input,
                spatial_index_type="s2",
                target_rows_per_partition=target_rows,
                max_partitions=max_partitions,
                min_resolution=0,
                max_resolution=30,
                verbose=verbose,
                profile=profile,
            )
            info(f"Auto-calculated S2 level: {level}")

        # Validate user-provided level (auto mode always returns valid range)
        # This check is defensive for auto mode but necessary for user-provided levels
        if not 0 <= level <= 30:
            raise InvalidParameterError("level", f"must be between 0 and 30, got {level}")

        if keep_s2_column is None:
            keep_s2_column = hive
        working_parquet, column_existed, temp_file = _ensure_s2_column(
            actual_input, s2_column_name, level, verbose
        )

        if preview:
            try:
                _run_preview(working_parquet, s2_column_name, preview_limit, verbose)
            finally:
                if temp_file and os.path.exists(temp_file):
                    os.remove(temp_file)
            return

        progress(f"Partitioning by S2 cells at level {level} (column: '{s2_column_name}')")

        num_partitions = _partition_with_temp_file(
            working_parquet,
            temp_file,
            output_folder,
            s2_column_name,
            hive,
            overwrite,
            verbose,
            keep_s2_column,
            force,
            skip_analysis,
            filename_prefix,
            profile,
            geoparquet_version,
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            memory_limit,
        )

        total_size_mb, avg_size_mb = calculate_partition_stats(output_folder, num_partitions)
        success(
            f"\nCreated {num_partitions} partition(s) in {output_folder} "
            f"(total: {total_size_mb:.2f} MB, avg: {avg_size_mb:.2f} MB)"
        )
    finally:
        # Clean up stdin temp file
        if stdin_temp_file and os.path.exists(stdin_temp_file):
            os.remove(stdin_temp_file)
