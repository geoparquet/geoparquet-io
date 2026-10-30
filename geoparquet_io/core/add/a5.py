#!/usr/bin/env python3

from __future__ import annotations

import pyarrow as pa

from geoparquet_io.core.constants import DEFAULT_A5_COLUMN_NAME
from geoparquet_io.core.index_ops import IndexSpec, add_index_column, add_index_table


def _get_resolution_size(resolution):
    """Get approximate cell size for a given A5 resolution."""
    sizes = {
        0: "42.5M km²",
        1: "10.6M km²",
        2: "2.7M km²",
        3: "666K km²",
        4: "167K km²",
        5: "41.7K km²",
        6: "10.4K km²",
        7: "2,600 km²",
        8: "650 km²",
        9: "163 km²",
        10: "40.7 km²",
        11: "10.2 km²",
        12: "2.5 km²",
        13: "630 m²",
        14: "158 m²",
        15: "39.4 m²",
        16: "9.9 m²",
        17: "2.5 m²",
        18: "616 mm²",
        19: "154 mm²",
        20: "38.5 mm²",
        21: "9.6 mm²",
        22: "2.4 mm²",
        23: "0.6 mm²",
        24: "0.15 mm²",
        25: "38 μm²",
        26: "9.5 μm²",
        27: "2.4 μm²",
        28: "0.6 μm²",
        29: "0.15 μm²",
        30: "37 nm²",
    }
    return sizes.get(resolution, f"resolution {resolution}")


def _cell_expression(lon, lat, resolution):
    return f"""a5_lonlat_to_cell(
        {lon},
        {lat},
        {resolution}
    )"""


SPEC = IndexSpec(
    "a5",
    DEFAULT_A5_COLUMN_NAME,
    0,
    30,
    _cell_expression,
    6,
    4,
    "a5",
    _get_resolution_size,
    label="A5",
    column_option="--a5-name",
)
from geoparquet_io.core.write_funnels import collect_nonplanar_edges


def add_a5_table(
    table: pa.Table,
    a5_column_name: str = DEFAULT_A5_COLUMN_NAME,
    resolution: int = 15,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add an A5 cell ID column to an Arrow Table.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table
        a5_column_name: Name for the A5 column (default: 'a5_cell')
        resolution: A5 resolution level (0-30). Default: 15
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with A5 column added
    """
    return add_index_table(SPEC, table, a5_column_name, resolution, geometry_column)


def add_a5_column(
    input_parquet: str,
    output_parquet: str | None = None,
    a5_column_name: str = DEFAULT_A5_COLUMN_NAME,
    a5_resolution: int = 15,
    dry_run: bool = False,
    verbose: bool = False,
    compression: str | None = None,
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
    memory_limit: str | None = None,
) -> None:
    """
    Add an A5 cell ID column to a GeoParquet file.

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    Computes A5 cell IDs based on geometry centroids using the A5
    discrete global grid system. The cell ID is stored as a UBIGINT
    (unsigned 64-bit integer) for efficient storage and indexing.

    Args:
        input_parquet: Path to the input parquet file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
        a5_column_name: Name for the A5 column (default: 'a5_cell')
        a5_resolution: A5 resolution level (0-30)
                      Res 10: ~40.7 km², Res 15: ~39.4 m², Res 20: ~38.5 mm²,
                      Res 25: ~38 μm², Res 30: ~37 nm²
                      Default: 15 (good balance for most use cases)
        dry_run: Whether to print SQL commands without executing them
        verbose: Whether to print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        memory_limit: DuckDB memory limit for the write (e.g., '2GB', '512MB')
    """
    return add_index_column(
        SPEC,
        input_parquet,
        output_parquet,
        a5_column_name,
        a5_resolution,
        dry_run,
        verbose,
        compression,
        compression_level,
        row_group_size_mb,
        row_group_rows,
        profile,
        geoparquet_version,
        overwrite,
        memory_limit,
    )
