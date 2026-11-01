#!/usr/bin/env python3

from __future__ import annotations

import pyarrow as pa

from geoparquet_io.core.constants import DEFAULT_H3_COLUMN_NAME
from geoparquet_io.core.index_ops import IndexSpec, add_index_column, add_index_table
from geoparquet_io.core.parquet_writer import WriteOptions


def _get_resolution_size(resolution):
    """Get approximate cell size for a given H3 resolution."""
    sizes = {
        0: "4,357 km²",
        1: "609 km²",
        2: "87 km²",
        3: "12 km²",
        4: "1.8 km²",
        5: "0.26 km²",
        6: "36,000 m²",
        7: "5,200 m²",
        8: "730 m²",
        9: "105 m²",
        10: "15 m²",
        11: "2.2 m²",
        12: "0.31 m²",
        13: "0.04 m²",
        14: "0.006 m²",
        15: "0.0009 m²",
    }
    return sizes.get(resolution, f"resolution {resolution}")


def _cell_expression(lon, lat, resolution):
    return f"""h3_latlng_to_cell_string(
        {lat},
        {lon},
        {resolution}
    )"""


SPEC = IndexSpec(
    "h3",
    DEFAULT_H3_COLUMN_NAME,
    0,
    15,
    _cell_expression,
    122,
    7,
    "h3",
    _get_resolution_size,
    resolution_function="h3_get_resolution",
    parent_template="h3_cell_to_parent({cell}, {level})",
    longitude_first=False,
    default_resolution=9,
    label="H3",
    # H3 string cell ids are 15 hex chars (e.g. "85283473fffffff").
    cell_pattern=r"^[0-9a-fA-F]{15}$",
    column_option="--h3-name",
)


def add_h3_table(
    table: pa.Table,
    h3_column_name: str = DEFAULT_H3_COLUMN_NAME,
    resolution: int = SPEC.default_resolution,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add an H3 cell ID column to an Arrow Table.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table
        h3_column_name: Name for the H3 column (default: 'h3_cell')
        resolution: H3 resolution level (0-15). Default: 9
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with H3 column added
    """
    return add_index_table(SPEC, table, h3_column_name, resolution, geometry_column)


def add_h3_column(
    input_parquet: str,
    output_parquet: str | None = None,
    h3_column_name: str = DEFAULT_H3_COLUMN_NAME,
    h3_resolution: int = SPEC.default_resolution,
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
    Add an H3 cell ID column to a GeoParquet file.

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    Computes H3 cell IDs based on geometry centroids using the H3
    hierarchical hexagonal grid system. The cell ID is stored as a
    VARCHAR (string) for maximum portability.

    Args:
        input_parquet: Path to the input parquet file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
        h3_column_name: Name for the H3 column (default: 'h3_cell')
        h3_resolution: H3 resolution level (0-15)
                      Res 7: ~5 km², Res 9: ~0.1 km², Res 11: ~1,770 m²,
                      Res 13: ~44 m², Res 15: ~0.9 m²
                      Default: 9 (good balance for most use cases)
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
    write = WriteOptions.pick(locals())
    return add_index_column(
        SPEC,
        input_parquet,
        output_parquet,
        h3_column_name,
        h3_resolution,
        dry_run,
        verbose,
        write,
        overwrite,
    )
