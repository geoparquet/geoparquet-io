#!/usr/bin/env python3

import os
import tempfile

import click
import fsspec
import pyarrow.parquet as pq

from geoparquet_io.core.add_kdtree_column import add_kdtree_column
from geoparquet_io.core.common import safe_file_url
from geoparquet_io.core.partition_common import partition_by_column, preview_partition


def partition_by_kdtree(
    input_parquet: str,
    output_folder: str,
    kdtree_column_name: str = "kdtree_cell",
    iterations: int = None,
    hive: bool = False,
    overwrite: bool = False,
    preview: bool = False,
    preview_limit: int = 15,
    verbose: bool = False,
    keep_kdtree_column: bool = None,
    force: bool = False,
    skip_analysis: bool = False,
    sample_size: int = 100000,
    auto_target_rows: tuple = None,
    filename_prefix: str = None,
    profile: str = None,
    geoparquet_version: str = "1.1",
    keep_bbox: bool = None,
):
    """
    Partition a GeoParquet file by KD-tree cells.

    If the KD-tree column doesn't exist, it will be automatically added before
    partitioning.

    Performance Note: Approximate mode is O(n), exact mode is O(n × iterations).

    Args:
        input_parquet: Input GeoParquet file
        output_folder: Output directory
        kdtree_column_name: Name of KD-tree column (default: 'kdtree_cell')
        iterations: Number of recursive splits (1-20, default: 9). Determines partition count: 2^iterations
        hive: Use Hive-style partitioning
        overwrite: Overwrite existing files
        preview: Show preview of partitions without creating files
        preview_limit: Maximum number of partitions to show in preview (default: 15)
        verbose: Verbose output
        keep_kdtree_column: Whether to keep KD-tree column in output files. If None (default),
                           keeps the column for Hive partitioning but excludes it otherwise.
        force: Force partitioning even if analysis detects issues
        skip_analysis: Skip partition strategy analysis (for performance)
        sample_size: Number of points to sample for computing boundaries. None for exact mode (default: 100,000)
    """
    # Validate iterations
    if not 1 <= iterations <= 20:
        raise click.UsageError(f"Iterations must be between 1 and 20, got {iterations}")

    # Determine default for keep_kdtree_column
    # For Hive partitioning, keep the column by default (standard practice)
    # Otherwise, exclude it by default (avoid redundancy since it's in the partition path)
    if keep_kdtree_column is None:
        keep_kdtree_column = hive

    safe_url = safe_file_url(input_parquet, verbose)

    # Check if KD-tree column exists and get row count for dataset size validation
    with fsspec.open(safe_url, "rb") as f:
        pf = pq.ParquetFile(f)
        schema = pf.schema_arrow
        total_rows = pf.metadata.num_rows

    column_exists = kdtree_column_name in schema.names

    # Note: With approximate mode (default), large datasets are handled efficiently in O(n)
    # Only exact mode is expensive for very large datasets

    if not column_exists and verbose and total_rows > 10_000_000:
        click.echo(
            click.style(
                f"Processing {total_rows:,} rows - this may take several minutes...",
                fg="cyan",
            )
        )

    # If column doesn't exist, add it
    partition_count = 2**iterations
    if not column_exists:
        if verbose:
            click.echo(
                f"Adding KD-tree column '{kdtree_column_name}' with {partition_count} partitions..."
            )

        # Create temporary file for KD-tree-enriched data
        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(temp_dir, f"kdtree_enriched_{os.path.basename(input_parquet)}")

        try:
            # Add KD-tree column at the specified iterations
            add_kdtree_column(
                input_parquet=input_parquet,
                output_parquet=temp_file,
                kdtree_column_name=kdtree_column_name,
                iterations=iterations,
                dry_run=False,
                verbose=verbose,
                compression="ZSTD",
                compression_level=15,
                row_group_size_mb=None,
                row_group_rows=None,
                force=force,
                sample_size=sample_size,
                auto_target_rows=auto_target_rows,
            )

            # Use the temp file as input for partitioning
            input_parquet = temp_file

        except Exception as e:
            # Clean up temp file on error
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise click.ClickException(f"Failed to add KD-tree column: {str(e)}") from e

    elif verbose:
        click.echo(f"Using existing KD-tree column '{kdtree_column_name}'")

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
                    column_name=kdtree_column_name,
                    column_prefix_length=None,  # Use full column value
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
                column_name=kdtree_column_name,
                column_prefix_length=None,  # Use full column value
                limit=preview_limit,
                verbose=verbose,
            )
        finally:
            # Clean up temp file if we created one
            if not column_exists and os.path.exists(input_parquet):
                os.remove(input_parquet)
        return

    # Build description for user feedback
    click.echo(
        f"Partitioning into {partition_count} KD-tree cells (column: '{kdtree_column_name}')"
    )

    try:
        # Use common partition function - partition by full column value (not prefix)
        # KD-tree generates partition IDs that ARE the partition keys
        num_partitions = partition_by_column(
            input_parquet=input_parquet,
            output_folder=output_folder,
            column_name=kdtree_column_name,
            column_prefix_length=None,  # Use full column value, not prefix
            hive=hive,
            overwrite=overwrite,
            verbose=verbose,
            keep_partition_column=keep_kdtree_column,
            force=force,
            skip_analysis=skip_analysis,
            filename_prefix=filename_prefix,
            profile=profile,
            geoparquet_version=geoparquet_version,
            keep_bbox=keep_bbox,
        )

        if verbose:
            click.echo(f"\nCreated {num_partitions} partition(s) in {output_folder}")

    finally:
        # Clean up temp file if we created one
        if not column_exists and os.path.exists(input_parquet):
            if verbose:
                click.echo("Cleaning up temporary KD-tree-enriched file...")
            os.remove(input_parquet)
