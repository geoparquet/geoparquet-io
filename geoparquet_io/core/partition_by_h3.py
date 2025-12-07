#!/usr/bin/env python3

import os
import tempfile

import click

from geoparquet_io.core.add_h3_column import add_h3_column
from geoparquet_io.core.common import safe_file_url
from geoparquet_io.core.partition_common import partition_by_column, preview_partition


def partition_by_h3(
    input_parquet: str,
    output_folder: str,
    h3_column_name: str = "h3_cell",
    resolution: int = 9,
    hive: bool = False,
    overwrite: bool = False,
    preview: bool = False,
    preview_limit: int = 15,
    verbose: bool = False,
    keep_h3_column: bool = None,
    force: bool = False,
    skip_analysis: bool = False,
    filename_prefix: str = None,
    profile: str = None,
    geoparquet_version: str = None,
):
    """
    Partition a GeoParquet file by H3 cells at specified resolution.

    If the H3 column doesn't exist, it will be automatically added at the specified
    resolution before partitioning.

    Args:
        input_parquet: Input GeoParquet file
        output_folder: Output directory
        h3_column_name: Name of H3 column (default: 'h3_cell')
        resolution: H3 resolution for partitioning (0-15, default: 9)
        hive: Use Hive-style partitioning
        overwrite: Overwrite existing files
        preview: Show preview of partitions without creating files
        preview_limit: Maximum number of partitions to show in preview (default: 15)
        verbose: Verbose output
        keep_h3_column: Whether to keep H3 column in output files. If None (default),
                       keeps the column for Hive partitioning but excludes it otherwise.
        force: Force partitioning even if analysis detects issues
        skip_analysis: Skip partition strategy analysis (for performance)
    """
    # Validate resolution
    if not 0 <= resolution <= 15:
        raise click.UsageError(f"H3 resolution must be between 0 and 15, got {resolution}")

    # Determine default for keep_h3_column
    # For Hive partitioning, keep the column by default (standard practice)
    # Otherwise, exclude it by default (avoid redundancy since it's in the partition path)
    if keep_h3_column is None:
        keep_h3_column = hive

    safe_url = safe_file_url(input_parquet, verbose)

    # Check if H3 column exists using DuckDB
    from geoparquet_io.core.duckdb_metadata import get_column_names

    column_names = get_column_names(safe_url)
    column_exists = h3_column_name in column_names

    # If column doesn't exist, add it
    if not column_exists:
        if verbose:
            click.echo(f"H3 column '{h3_column_name}' not found. Adding it now...")

        # Create temporary file for H3-enriched data
        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(temp_dir, f"h3_enriched_{os.path.basename(input_parquet)}")

        try:
            # Add H3 column at the specified resolution
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

            # Use the temp file as input for partitioning
            input_parquet = temp_file
            if verbose:
                click.echo(f"H3 column added successfully at resolution {resolution}")

        except Exception as e:
            # Clean up temp file on error
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise click.ClickException(f"Failed to add H3 column: {str(e)}") from e

    elif verbose:
        click.echo(f"Using existing H3 column '{h3_column_name}'")

    # If preview mode, show analysis and preview, then exit
    if preview:
        try:
            # Run analysis first to show recommendations
            try:
                from geoparquet_io.core.partition_common import (
                    PartitionAnalysisError,
                    analyze_partition_strategy,
                )

                analyze_partition_strategy(
                    input_parquet=input_parquet,
                    column_name=h3_column_name,
                    column_prefix_length=resolution,
                    verbose=True,
                )
            except PartitionAnalysisError:
                # Analysis already displayed the errors, just continue to preview
                pass
            except Exception as e:
                # If analysis fails unexpectedly, show error but continue to preview
                click.echo(click.style(f"\nAnalysis error: {e}", fg="yellow"))

            # Then show partition preview
            click.echo("\n" + "=" * 70)
            preview_partition(
                input_parquet=input_parquet,
                column_name=h3_column_name,
                column_prefix_length=resolution,
                limit=preview_limit,
                verbose=verbose,
            )
        finally:
            # Clean up temp file if we created one
            if not column_exists and os.path.exists(input_parquet):
                os.remove(input_parquet)
        return

    # Build description for user feedback
    click.echo(f"Partitioning by H3 cells at resolution {resolution} (column: '{h3_column_name}')")

    try:
        # Use common partition function with H3 resolution as prefix length
        num_partitions = partition_by_column(
            input_parquet=input_parquet,
            output_folder=output_folder,
            column_name=h3_column_name,
            column_prefix_length=resolution,
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

        if verbose:
            click.echo(f"\nSuccessfully created {num_partitions} partition(s) in {output_folder}")

    finally:
        # Clean up temp file if we created one
        if not column_exists and os.path.exists(input_parquet):
            if verbose:
                click.echo("Cleaning up temporary H3-enriched file...")
            os.remove(input_parquet)
