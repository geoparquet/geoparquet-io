#!/usr/bin/env python3

from __future__ import annotations

import os
import tempfile
import uuid

from geoparquet_io.core.add.kdtree import (
    ITERATIONS_CONFLICT,
    add_kdtree_column,
    require_iterations_or_auto,
)
from geoparquet_io.core.exceptions import InvalidParameterError, PartitionError
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    info,
    progress,
    success,
    warn,
)
from geoparquet_io.core.parquet_writer import WriteOptions
from geoparquet_io.core.partition.common import (
    partition_by_column,
    preview_partition,
    raise_if_no_rows,
)
from geoparquet_io.core.streaming import is_stdin, read_stdin_to_temp_file


def _cleanup_temp_file(temp_file: str | None, verbose: bool = False) -> None:
    """Clean up a temporary file if it exists."""
    if temp_file and os.path.exists(temp_file):
        if verbose:
            debug("Cleaning up temporary file...")
        os.remove(temp_file)


def _add_kdtree_column_to_temp(
    input_file: str,
    kdtree_column_name: str,
    iterations: int | None,
    verbose: bool,
    force: bool,
    sample_size: int,
    auto_target_rows: tuple | None,
) -> str:
    """Add KD-tree column to input and return path to temp file.

    ``iterations`` is None in auto mode; `add_kdtree_column` then sizes the tree
    from the row count and reports the partition count it settled on.

    Raises:
        PartitionError: If column addition fails
    """
    if verbose and iterations is not None:
        debug(f"Adding KD-tree column '{kdtree_column_name}' with {2**iterations} partitions...")

    temp_dir = tempfile.gettempdir()
    temp_file = os.path.join(
        temp_dir, f"kdtree_enriched_{uuid.uuid4()}_{os.path.basename(input_file)}"
    )

    try:
        add_kdtree_column(
            input_parquet=input_file,
            output_parquet=temp_file,
            kdtree_column_name=kdtree_column_name,
            iterations=iterations,
            dry_run=False,
            verbose=verbose,
            compression=None,
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            force=force,
            sample_size=sample_size,
            auto_target_rows=auto_target_rows,
        )
        return temp_file
    except InvalidParameterError:
        # A parameter fault is the caller's to fix, not a partition failure:
        # let it surface under its own type, as it does through `add kdtree`.
        _cleanup_temp_file(temp_file)
        raise
    except Exception as e:
        _cleanup_temp_file(temp_file)
        raise PartitionError(f"Failed to add KD-tree column: {str(e)}") from e


def _show_partition_preview(
    working_input: str,
    kdtree_column_name: str,
    preview_limit: int,
    verbose: bool,
) -> None:
    """Show partition analysis and preview."""
    try:
        from geoparquet_io.core.partition.common import (
            PartitionAnalysisError,
            analyze_partition_strategy,
        )

        analyze_partition_strategy(
            input_parquet=working_input,
            column_name=kdtree_column_name,
            column_prefix_length=None,
            verbose=True,
        )
    except PartitionAnalysisError:
        pass
    except Exception as e:
        warn(f"\nAnalysis error: {e}")

    progress("\n" + "=" * 70)
    preview_partition(
        input_parquet=working_input,
        column_name=kdtree_column_name,
        column_prefix_length=None,
        limit=preview_limit,
        verbose=verbose,
    )


def partition_by_kdtree(
    input_parquet: str,
    output_folder: str,
    kdtree_column_name: str = "kdtree_cell",
    iterations: int | None = None,
    hive: bool = False,
    overwrite: bool = False,
    preview: bool = False,
    preview_limit: int = 15,
    verbose: bool = False,
    keep_kdtree_column: bool | None = None,
    force: bool = False,
    skip_analysis: bool = False,
    sample_size: int = 100000,
    auto_target_rows: tuple | None = None,
    filename_prefix: str | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    compression: str | None = None,
    compression_level: int | None = None,
    row_group_size_mb: int | None = None,
    row_group_rows: int | None = None,
    memory_limit: str | None = None,
) -> None:
    """
    Partition a GeoParquet file by KD-tree cells.

    Supports Arrow IPC streaming for input:
    - Input "-" reads from stdin (output is always a directory)

    If the KD-tree column doesn't exist, it will be automatically added before
    partitioning.

    Performance Note: Approximate mode is O(n), exact mode is O(n × iterations).

    Args:
        input_parquet: Input GeoParquet file (local, remote URL, or "-" for stdin)
        output_folder: Output directory
        kdtree_column_name: Name of KD-tree column (default: 'kdtree_cell')
        iterations: Number of recursive splits (1-20). Required unless
            auto_target_rows is given, which sizes the tree from the row count,
            or the input already carries the kdtree column (nothing is added,
            so there is no tree to size).
        hive: Use Hive-style partitioning
        overwrite: Overwrite existing files
        preview: Show preview of partitions without creating files
        preview_limit: Maximum number of partitions to show in preview (default: 15)
        verbose: Verbose output
        keep_kdtree_column: Whether to keep KD-tree column in output files
        force: Force partitioning even if analysis detects issues
        skip_analysis: Skip partition strategy analysis (for performance)
        sample_size: Number of points to sample for computing boundaries
        filename_prefix: Prefix for output filenames
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write
        compression: Compression codec (default: the compression owner's)
        compression_level: Compression level (default: 15)
        row_group_size_mb: Row group size in MB (mutually exclusive with row_group_rows)
        row_group_rows: Row group size in number of rows (mutually exclusive with row_group_size_mb)
        memory_limit: DuckDB memory limit for write operations (e.g., "2GB")
    """
    # Configure logging verbosity
    configure_verbose(verbose)

    # Validate iterations
    if iterations is not None and not 1 <= iterations <= 20:
        raise InvalidParameterError("iterations", f"must be between 1 and 20, got {iterations}")

    # Determine default for keep_kdtree_column
    if keep_kdtree_column is None:
        keep_kdtree_column = hive

    # Handle stdin input
    stdin_temp_file = None
    actual_input = input_parquet

    if is_stdin(input_parquet):
        stdin_temp_file = read_stdin_to_temp_file(verbose)
        actual_input = stdin_temp_file

    try:
        # Nothing to partition: stop before rewriting the whole input just
        # to add an index column to it (#823).
        raise_if_no_rows(actual_input)

        # Check if KD-tree column exists and get row count for dataset size validation
        from geoparquet_io.core.duckdb_metadata import get_column_names, get_row_count

        column_names = get_column_names(actual_input)
        total_rows = get_row_count(actual_input)

        column_exists = kdtree_column_name in column_names

        # Auto mode leaves `iterations` None on purpose: `add_kdtree_column` sizes
        # the tree from the row count below. Defaulting it to 9 here (as this did
        # until #813) silently overrode `--auto` with 512 partitions whatever the
        # input -- the same guess the Python API used to make.
        #
        # An input already carrying the kdtree column needs no sizing at all:
        # the add step is skipped and the existing cells drive the partition, so
        # only the contradiction of naming both is still refused.
        if not column_exists:
            require_iterations_or_auto(iterations, auto_target_rows)
        elif iterations is not None and auto_target_rows is not None:
            raise InvalidParameterError("auto", ITERATIONS_CONFLICT)

        # Note: With approximate mode (default), large datasets are handled efficiently
        if not column_exists and verbose and total_rows > 10_000_000:
            info(f"Processing {total_rows:,} rows - this may take several minutes...")

        # If column doesn't exist, add it
        partition_count = 2**iterations if iterations is not None else None
        working_input = actual_input
        temp_file = None

        if not column_exists:
            temp_file = _add_kdtree_column_to_temp(
                input_file=actual_input,
                kdtree_column_name=kdtree_column_name,
                iterations=iterations,
                verbose=verbose,
                force=force,
                sample_size=sample_size,
                auto_target_rows=auto_target_rows,
            )
            working_input = temp_file
        elif verbose:
            debug(f"Using existing KD-tree column '{kdtree_column_name}'")

        # If preview mode, show analysis and preview, then exit
        if preview:
            try:
                _show_partition_preview(working_input, kdtree_column_name, preview_limit, verbose)
            finally:
                _cleanup_temp_file(temp_file)
            return

        # Build description for user feedback. In auto mode the count is not known
        # here -- `add_kdtree_column` reported the one it picked.
        cell_count = "auto-selected" if partition_count is None else partition_count
        progress(f"Partitioning into {cell_count} KD-tree cells (column: '{kdtree_column_name}')")

        try:
            num_partitions = partition_by_column(
                input_parquet=working_input,
                output_folder=output_folder,
                column_name=kdtree_column_name,
                column_prefix_length=None,
                hive=hive,
                overwrite=overwrite,
                verbose=verbose,
                keep_partition_column=keep_kdtree_column,
                force=force,
                skip_analysis=skip_analysis,
                filename_prefix=filename_prefix,
                write=WriteOptions.pick(locals()),
            )

            if verbose:
                success(f"\nCreated {num_partitions} partition(s) in {output_folder}")

        finally:
            _cleanup_temp_file(temp_file, verbose)
    finally:
        # Clean up stdin temp file
        _cleanup_temp_file(stdin_temp_file)
