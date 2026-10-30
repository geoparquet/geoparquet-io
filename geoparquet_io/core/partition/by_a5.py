#!/usr/bin/env python3

from __future__ import annotations

from geoparquet_io.core.constants import DEFAULT_A5_COLUMN_NAME
from geoparquet_io.core.index_ops import get_index_spec, partition_index

SPEC = get_index_spec("a5")


def partition_by_a5(
    input_parquet: str,
    output_folder: str,
    a5_column_name: str = DEFAULT_A5_COLUMN_NAME,
    resolution: int | None = None,
    hive: bool = False,
    overwrite: bool = False,
    preview: bool = False,
    preview_limit: int = 15,
    verbose: bool = False,
    keep_a5_column: bool | None = None,
    force: bool = False,
    skip_analysis: bool = False,
    filename_prefix: str | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    compression: str | None = None,
    compression_level: int | None = None,
    row_group_size_mb: int | None = None,
    row_group_rows: int | None = None,
    memory_limit: str | None = None,
    auto: bool = False,
    target_rows: int = 100000,
    max_partitions: int = 10000,
) -> None:
    """
    Partition a GeoParquet file by A5 cells at specified resolution.

    Supports Arrow IPC streaming for input:
    - Input "-" reads from stdin (output is always a directory)

    Auto-resolution mode:
    - Use --auto to automatically calculate optimal resolution based on data size
    - Specify --target-rows to control partition size (default: 100,000 rows)
    - Specify --max-partitions to limit total partition count (default: 10,000)

    Args:
        input_parquet: Input GeoParquet file (local, remote URL, or "-" for stdin)
        output_folder: Output directory (always writes to directory, no stdout support)
        a5_column_name: Name of the A5 column (default: 'a5_cell')
        resolution: A5 resolution level (see index bounds). Required unless --auto is used
        hive: Use Hive-style partitioning (column=value directories)
        overwrite: Overwrite existing output directory
        preview: Preview partition distribution without writing
        preview_limit: Max number of partitions to show in preview
        verbose: Print verbose output
        keep_a5_column: Keep A5 column in output partitions
        force: Force operation even if analysis suggests issues
        skip_analysis: Skip partition analysis
        filename_prefix: Prefix for output filenames
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write
        compression: Compression codec (default: the compression owner's)
        compression_level: Compression level (default: 15)
        row_group_size_mb: Row group size in MB (mutually exclusive with row_group_rows)
        row_group_rows: Row group size in number of rows (mutually exclusive with row_group_size_mb)
        memory_limit: DuckDB memory limit for write operations (e.g., "2GB")
        auto: Automatically calculate optimal resolution (default: False)
        target_rows: Target rows per partition for auto mode (default: 100000)
        max_partitions: Maximum partitions for auto mode (default: 10000)
    """
    return partition_index(
        SPEC,
        input_parquet,
        output_folder,
        a5_column_name,
        resolution,
        hive,
        overwrite,
        preview,
        preview_limit,
        verbose,
        keep_a5_column,
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
        auto,
        target_rows,
        max_partitions,
    )
