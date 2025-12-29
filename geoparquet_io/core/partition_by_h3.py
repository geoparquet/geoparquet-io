#!/usr/bin/env python3

from __future__ import annotations

import os
import tempfile
import uuid

import click

from geoparquet_io.core.add_h3_column import add_h3_column
from geoparquet_io.core.common import safe_file_url
from geoparquet_io.core.constants import DEFAULT_H3_COLUMN_NAME
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success, warn
from geoparquet_io.core.partition_common import (
    calculate_partition_stats,
    partition_by_column,
    preview_partition,
)
from geoparquet_io.core.streaming import is_stdin, read_stdin_to_temp_file


def _ensure_h3_column(input_parquet, h3_column_name, resolution, verbose):
    """Ensure H3 column exists, adding it if needed.

    Returns:
        tuple: (input_file_to_use, column_existed, temp_file_or_none)
    """
    from geoparquet_io.core.duckdb_metadata import get_column_names

    safe_url = safe_file_url(input_parquet, verbose)
    column_names = get_column_names(safe_url)

    if h3_column_name in column_names:
        if verbose:
            debug(f"Using existing H3 column '{h3_column_name}'")
        return input_parquet, True, None

    if verbose:
        debug(f"H3 column '{h3_column_name}' not found. Adding it now...")

    temp_file = os.path.join(
        tempfile.gettempdir(), f"h3_enriched_{uuid.uuid4().hex}_{os.path.basename(input_parquet)}"
    )

    try:
        add_h3_column(
            input_parquet=input_parquet,
            output_parquet=temp_file,
            h3_column_name=h3_column_name,
            h3_resolution=resolution,
            dry_run=False,
            verbose=verbose,
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
        )
        if verbose:
            debug(f"H3 column added successfully at resolution {resolution}")
        return temp_file, False, temp_file
    except Exception as e:
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise click.ClickException(f"Failed to add H3 column: {str(e)}") from e


def _run_preview(input_parquet, h3_column_name, preview_limit, verbose):
    """Run partition preview and analysis."""
    from geoparquet_io.core.partition_common import (
        PartitionAnalysisError,
        analyze_partition_strategy,
    )

    try:
        analyze_partition_strategy(
            input_parquet=input_parquet,
            column_name=h3_column_name,
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
        column_name=h3_column_name,
        column_prefix_length=None,
        limit=preview_limit,
        verbose=verbose,
    )


def partition_by_h3(
    input_parquet: str,
    output_folder: str,
    h3_column_name: str = DEFAULT_H3_COLUMN_NAME,
    resolution: int = 9,
    hive: bool = False,
    overwrite: bool = False,
    preview: bool = False,
    preview_limit: int = 15,
    verbose: bool = False,
    keep_h3_column: bool | None = None,
    force: bool = False,
    skip_analysis: bool = False,
    filename_prefix: str | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
) -> None:
    """
    Partition a GeoParquet file by H3 cells at specified resolution.

    Supports Arrow IPC streaming for input:
    - Input "-" reads from stdin (output is always a directory)

    Args:
        input_parquet: Input GeoParquet file (local, remote URL, or "-" for stdin)
        output_folder: Output directory (always writes to directory, no stdout support)
        h3_column_name: Name of the H3 column (default: 'h3_cell')
        resolution: H3 resolution level (0-15). Default: 9
        hive: Use Hive-style partitioning (column=value directories)
        overwrite: Overwrite existing output directory
        preview: Preview partition distribution without writing
        preview_limit: Max number of partitions to show in preview
        verbose: Print verbose output
        keep_h3_column: Keep H3 column in output partitions
        force: Force operation even if analysis suggests issues
        skip_analysis: Skip partition analysis
        filename_prefix: Prefix for output filenames
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write
    """
    configure_verbose(verbose)

    if not 0 <= resolution <= 15:
        raise click.UsageError(f"H3 resolution must be between 0 and 15, got {resolution}")

    if keep_h3_column is None:
        keep_h3_column = hive

    # Handle stdin input
    stdin_temp_file = None
    actual_input = input_parquet

    if is_stdin(input_parquet):
        stdin_temp_file = read_stdin_to_temp_file(verbose)
        actual_input = stdin_temp_file

    try:
        working_parquet, column_existed, temp_file = _ensure_h3_column(
            actual_input, h3_column_name, resolution, verbose
        )

        if preview:
            try:
                _run_preview(working_parquet, h3_column_name, preview_limit, verbose)
            finally:
                if temp_file and os.path.exists(temp_file):
                    os.remove(temp_file)
            return

        progress(
            f"Partitioning by H3 cells at resolution {resolution} (column: '{h3_column_name}')"
        )

        try:
            num_partitions = partition_by_column(
                input_parquet=working_parquet,
                output_folder=output_folder,
                column_name=h3_column_name,
                column_prefix_length=None,
                hive=hive,
                overwrite=overwrite,
                verbose=verbose,
                keep_partition_column=keep_h3_column,
                force=force,
                skip_analysis=skip_analysis,
                filename_prefix=filename_prefix,
                profile=profile,
                geoparquet_version=geoparquet_version,
            )

            total_size_mb, avg_size_mb = calculate_partition_stats(output_folder, num_partitions)
            success(
                f"\nCreated {num_partitions} partition(s) in {output_folder} "
                f"(total: {total_size_mb:.2f} MB, avg: {avg_size_mb:.2f} MB)"
            )
        finally:
            if temp_file and os.path.exists(temp_file):
                if verbose:
                    debug("Cleaning up temporary H3-enriched file...")
                os.remove(temp_file)
    finally:
        # Clean up stdin temp file
        if stdin_temp_file and os.path.exists(stdin_temp_file):
            os.remove(stdin_temp_file)
