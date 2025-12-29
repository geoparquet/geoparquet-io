#!/usr/bin/env python3

from __future__ import annotations

import os
import tempfile
import uuid

import click

from geoparquet_io.core.add_quadkey_column import add_quadkey_column
from geoparquet_io.core.common import safe_file_url
from geoparquet_io.core.constants import (
    DEFAULT_QUADKEY_COLUMN_NAME,
    DEFAULT_QUADKEY_PARTITION_RESOLUTION,
    DEFAULT_QUADKEY_RESOLUTION,
)
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success, warn
from geoparquet_io.core.partition_common import (
    calculate_partition_stats,
    partition_by_column,
    preview_partition,
)
from geoparquet_io.core.streaming import is_stdin, read_stdin_to_temp_file


def _validate_resolutions(resolution, partition_resolution):
    """Validate resolution parameters."""
    if not 0 <= resolution <= 23:
        raise click.UsageError(f"Resolution must be between 0 and 23, got {resolution}")
    if not 0 <= partition_resolution <= 23:
        raise click.UsageError(
            f"Partition resolution must be between 0 and 23, got {partition_resolution}"
        )
    if partition_resolution > resolution:
        raise click.UsageError(
            f"Partition resolution ({partition_resolution}) cannot exceed "
            f"column resolution ({resolution})"
        )


def _ensure_quadkey_column(
    input_parquet, quadkey_column_name, resolution, use_centroid, verbose, profile
):
    """Ensure quadkey column exists, adding it if needed.

    Returns:
        tuple: (input_file_to_use, temp_file_or_none)
    """
    from geoparquet_io.core.duckdb_metadata import get_column_names

    safe_url = safe_file_url(input_parquet, verbose)
    column_names = get_column_names(safe_url)

    if quadkey_column_name in column_names:
        if verbose:
            debug(f"Using existing quadkey column '{quadkey_column_name}'")
        return input_parquet, None

    if verbose:
        debug(f"Quadkey column '{quadkey_column_name}' not found. Adding it now...")

    temp_file = os.path.join(
        tempfile.gettempdir(),
        f"quadkey_enriched_{uuid.uuid4().hex}_{os.path.basename(input_parquet)}",
    )

    try:
        add_quadkey_column(
            input_parquet=input_parquet,
            output_parquet=temp_file,
            quadkey_column_name=quadkey_column_name,
            resolution=resolution,
            use_centroid=use_centroid,
            dry_run=False,
            verbose=verbose,
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            profile=profile,
        )
        if verbose:
            debug(f"Quadkey column added successfully at resolution {resolution}")
        return temp_file, temp_file
    except Exception as e:
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise click.ClickException(f"Failed to add quadkey column: {str(e)}") from e


def _run_quadkey_preview(
    input_parquet, quadkey_column_name, partition_resolution, preview_limit, verbose
):
    """Run partition preview and analysis for quadkey."""
    from geoparquet_io.core.partition_common import (
        PartitionAnalysisError,
        analyze_partition_strategy,
    )

    try:
        analyze_partition_strategy(
            input_parquet=input_parquet,
            column_name=quadkey_column_name,
            column_prefix_length=partition_resolution,
            verbose=True,
        )
    except PartitionAnalysisError:
        pass
    except Exception as e:
        warn(f"\nAnalysis error: {e}")

    progress("\n" + "=" * 70)
    preview_partition(
        input_parquet=input_parquet,
        column_name=quadkey_column_name,
        column_prefix_length=partition_resolution,
        limit=preview_limit,
        verbose=verbose,
    )


def partition_by_quadkey(
    input_parquet: str,
    output_folder: str,
    quadkey_column_name: str = DEFAULT_QUADKEY_COLUMN_NAME,
    resolution: int = DEFAULT_QUADKEY_RESOLUTION,
    partition_resolution: int = DEFAULT_QUADKEY_PARTITION_RESOLUTION,
    use_centroid: bool = False,
    hive: bool = False,
    overwrite: bool = False,
    preview: bool = False,
    preview_limit: int = 15,
    verbose: bool = False,
    keep_quadkey_column: bool | None = None,
    force: bool = False,
    skip_analysis: bool = False,
    filename_prefix: str | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
) -> None:
    """
    Partition a GeoParquet file by quadkey cells.

    Supports Arrow IPC streaming for input:
    - Input "-" reads from stdin (output is always a directory)
    """
    configure_verbose(verbose)
    _validate_resolutions(resolution, partition_resolution)

    if keep_quadkey_column is None:
        keep_quadkey_column = hive

    # Handle stdin input
    stdin_temp_file = None
    actual_input = input_parquet

    if is_stdin(input_parquet):
        stdin_temp_file = read_stdin_to_temp_file(verbose)
        actual_input = stdin_temp_file

    try:
        working_parquet, temp_file = _ensure_quadkey_column(
            actual_input, quadkey_column_name, resolution, use_centroid, verbose, profile
        )

        if preview:
            try:
                _run_quadkey_preview(
                    working_parquet,
                    quadkey_column_name,
                    partition_resolution,
                    preview_limit,
                    verbose,
                )
            finally:
                if temp_file and os.path.exists(temp_file):
                    os.remove(temp_file)
            return

        progress(
            f"Partitioning by quadkey at resolution {partition_resolution} "
            f"(column: '{quadkey_column_name}')"
        )

        try:
            num_partitions = partition_by_column(
                input_parquet=working_parquet,
                output_folder=output_folder,
                column_name=quadkey_column_name,
                column_prefix_length=partition_resolution,
                hive=hive,
                overwrite=overwrite,
                verbose=verbose,
                keep_partition_column=keep_quadkey_column,
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
                    debug("Cleaning up temporary quadkey-enriched file...")
                os.remove(temp_file)
    finally:
        # Clean up stdin temp file
        if stdin_temp_file and os.path.exists(stdin_temp_file):
            os.remove(stdin_temp_file)
