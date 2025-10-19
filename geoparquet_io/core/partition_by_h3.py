#!/usr/bin/env python3

import os
import tempfile

import click
import fsspec
import pyarrow.parquet as pq

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
    """
    # Validate resolution
    if not 0 <= resolution <= 15:
        raise click.UsageError(f"H3 resolution must be between 0 and 15, got {resolution}")

    safe_url = safe_file_url(input_parquet, verbose)

    # Check if H3 column exists
    with fsspec.open(safe_url, "rb") as f:
        pf = pq.ParquetFile(f)
        schema = pf.schema_arrow

    column_exists = h3_column_name in schema.names

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

    # If preview mode, show preview and exit
    if preview:
        try:
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
        )

        if verbose:
            click.echo(f"\nSuccessfully created {num_partitions} partition(s) in {output_folder}")

    finally:
        # Clean up temp file if we created one
        if not column_exists and os.path.exists(input_parquet):
            if verbose:
                click.echo("Cleaning up temporary H3-enriched file...")
            os.remove(input_parquet)
