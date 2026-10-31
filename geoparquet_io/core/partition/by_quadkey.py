#!/usr/bin/env python3

from __future__ import annotations

from geoparquet_io.core.constants import (
    DEFAULT_QUADKEY_COLUMN_NAME,
)
from geoparquet_io.core.index_ops import get_index_spec, partition_index
from geoparquet_io.core.parquet_writer import WriteOptions

SPEC = get_index_spec("quadkey")


def partition_by_quadkey(
    input_parquet: str,
    output_folder: str,
    quadkey_column_name: str = DEFAULT_QUADKEY_COLUMN_NAME,
    resolution: int | None = None,
    partition_resolution: int | None = None,
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
    Partition a GeoParquet file by quadkey cells.

    Supports Arrow IPC streaming for input:
    - Input "-" reads from stdin (output is always a directory)

    Auto-resolution mode:
    - Use --auto to automatically calculate optimal zoom level based on data size
    - Specify --target-rows to control partition size (default: 100,000 rows)
    - Specify --max-partitions to limit total partition count (default: 10,000)

    Args:
        auto: Automatically calculate optimal resolution (default: False)
        target_rows: Target rows per partition for auto mode (default: 100000)
        max_partitions: Maximum partitions for auto mode (default: 10000)
        resolution: Quadkey resolution for column. Required unless auto is enabled.
        partition_resolution: Prefix length for partitioning. Required unless auto is enabled.
    """
    write = WriteOptions.pick(locals())
    return partition_index(
        SPEC,
        input_parquet,
        output_folder,
        quadkey_column_name,
        resolution,
        hive,
        overwrite,
        preview,
        preview_limit,
        verbose,
        keep_quadkey_column,
        force,
        skip_analysis,
        filename_prefix,
        write,
        auto,
        target_rows,
        max_partitions,
        partition_resolution=partition_resolution,
        use_centroid=use_centroid,
    )
