#!/usr/bin/env python3

"""
Add S2 cell ID column to GeoParquet files.

Uses DuckDB's geography extension to compute S2 cell IDs from geometry centroids.
S2 (Google's Spherical Geometry library) provides a hierarchical spatial index
that divides Earth's surface into cells at various levels (0-30).
"""

from __future__ import annotations

import pyarrow as pa

from geoparquet_io.core.constants import (
    DEFAULT_S2_COLUMN_NAME,
    DEFAULT_S2_LEVEL,
)
from geoparquet_io.core.index_ops import IndexSpec, add_index_column, add_index_table


def _get_level_size(level):
    """Get approximate cell size for a given S2 level."""
    # S2 cell areas at various levels (approximate, varies by location)
    # Formula: Earth surface ~510M km², 6 base cells, each level divides by 4
    sizes = {
        0: "85M km²",
        1: "21M km²",
        2: "5.3M km²",
        3: "1.3M km²",
        4: "324,000 km²",
        5: "81,000 km²",
        6: "20,000 km²",
        7: "5,100 km²",
        8: "1,250 km²",
        9: "313 km²",
        10: "78 km²",
        11: "20 km²",
        12: "4.9 km²",
        13: "1.2 km²",
        14: "0.31 km²",
        15: "77,000 m²",
        16: "19,000 m²",
        17: "4,800 m²",
        18: "1,200 m²",
        19: "300 m²",
        20: "75 m²",
        21: "19 m²",
        22: "4.7 m²",
        23: "1.2 m²",
        24: "0.29 m²",
        25: "0.07 m²",
        26: "0.02 m²",
        27: "0.005 m²",
        28: "0.001 m²",
        29: "0.0003 m²",
        30: "0.00007 m²",
    }
    return sizes.get(level, f"level {level}")


def _cell_expression(lon, lat, resolution):
    return f"""s2_cell_token(
        s2_cell_parent(
            s2_cellfromlonlat(
                {lon},
                {lat}
            ),
            {resolution}
        )
    )"""


SPEC = IndexSpec(
    "s2",
    DEFAULT_S2_COLUMN_NAME,
    0,
    30,
    _cell_expression,
    6,
    4,
    "geography",
    _get_level_size,
    label="S2",
    column_option="--s2-name",
    unit="level",
    extension_preflight=True,
    wrap_auto_error=False,
)


def add_s2_table(
    table: pa.Table,
    s2_column_name: str = DEFAULT_S2_COLUMN_NAME,
    level: int = DEFAULT_S2_LEVEL,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add an S2 cell ID column to an Arrow Table.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table
        s2_column_name: Name for the S2 column (default: 's2_cell')
        level: S2 level (0-30). Default: 13 (~1.2 km² cells)
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with S2 column added
    """
    return add_index_table(SPEC, table, s2_column_name, level, geometry_column)


def add_s2_column(
    input_parquet: str,
    output_parquet: str | None = None,
    s2_column_name: str = DEFAULT_S2_COLUMN_NAME,
    s2_level: int = DEFAULT_S2_LEVEL,
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
    Add an S2 cell ID column to a GeoParquet file.

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    Computes S2 cell IDs based on geometry centroids using Google's S2
    hierarchical spherical grid system. The cell ID is stored as a
    token (hex string) for maximum portability.

    Args:
        input_parquet: Path to the input parquet file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
        s2_column_name: Name for the S2 column (default: 's2_cell')
        s2_level: S2 level (0-30)
                  Level 8: ~1,250 km², Level 13: ~1.2 km², Level 18: ~4,500 m²
                  Default: 13 (good balance for most use cases)
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
        s2_column_name,
        s2_level,
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
