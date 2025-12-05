#!/usr/bin/env python3

from typing import Optional

import click

from geoparquet_io.core.common import add_computed_column, find_primary_geometry_column
from geoparquet_io.core.partition_ops import (
    PartitionResult,
    apply_operation_to_partition,
    detect_input_type,
)


def _add_bbox_column_single(
    input_parquet,
    output_parquet,
    bbox_column_name="bbox",
    dry_run=False,
    verbose=False,
    compression="ZSTD",
    compression_level=None,
    row_group_size_mb=None,
    row_group_rows=None,
    profile=None,
):
    """
    Add a bbox struct column to a single GeoParquet file.

    This is the single-file implementation. Use add_bbox_column() for the
    auto-detecting wrapper that handles both files and partitions.
    """
    # Get geometry column for the SQL expression
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # Define the SQL expression (the only unique part)
    sql_expression = f"""STRUCT_PACK(
        xmin := ST_XMin({geom_col}),
        ymin := ST_YMin({geom_col}),
        xmax := ST_XMax({geom_col}),
        ymax := ST_YMax({geom_col})
    )"""

    # Use the generic helper for all boilerplate
    # Note: write_parquet_with_metadata automatically adds bbox covering metadata
    # when a bbox column is detected
    add_computed_column(
        input_parquet=input_parquet,
        output_parquet=output_parquet,
        column_name=bbox_column_name,
        sql_expression=sql_expression,
        extensions=None,  # Only needs spatial, which is loaded by default
        dry_run=dry_run,
        verbose=verbose,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        dry_run_description="Bounding box struct (xmin, ymin, xmax, ymax)",
        profile=profile,
    )

    if not dry_run:
        click.echo(f"Successfully added bbox column '{bbox_column_name}' to: {output_parquet}")


def add_bbox_column(
    input_parquet,
    output_parquet,
    bbox_column_name="bbox",
    dry_run=False,
    verbose=False,
    compression="ZSTD",
    compression_level=None,
    row_group_size_mb=None,
    row_group_rows=None,
    profile=None,
    # Partition-specific options
    concurrency: int = 4,
    error_handling: str = "fail_fast",
) -> Optional[PartitionResult]:
    """
    Add a bbox struct column to a GeoParquet file or partition.

    Automatically detects whether input is a single file or a partition directory
    and handles accordingly. For partitions, processes files in parallel.

    Args:
        input_parquet: Path to input file or partition (local or remote URL)
        output_parquet: Path to output file or partition (local or remote URL)
        bbox_column_name: Name for the bbox column (default: 'bbox')
        dry_run: Whether to print SQL commands without executing them
        verbose: Whether to print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name (S3 only, optional)
        concurrency: Number of files to process in parallel (partitions only)
        error_handling: 'fail_fast' or 'continue' (partitions only)

    Returns:
        None for single files, PartitionResult for partitions

    Note:
        Bbox covering metadata is automatically added when the file is written.
    """
    input_type = detect_input_type(input_parquet)

    if input_type == "file":
        # Single file - use original logic
        _add_bbox_column_single(
            input_parquet=input_parquet,
            output_parquet=output_parquet,
            bbox_column_name=bbox_column_name,
            dry_run=dry_run,
            verbose=verbose,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            profile=profile,
        )
        return None
    else:
        # Partition - apply to all files
        if dry_run:
            click.echo("Dry-run mode is not supported for partition operations.")
            return None

        click.echo(f"Processing partition: {input_parquet}")
        return apply_operation_to_partition(
            operation_fn=_add_bbox_column_single,
            input_partition=input_parquet,
            output_partition=output_parquet,
            concurrency=concurrency,
            error_handling=error_handling,
            profile=profile,
            verbose=verbose,
            # Pass operation-specific kwargs
            bbox_column_name=bbox_column_name,
            dry_run=False,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
        )


if __name__ == "__main__":
    add_bbox_column()
