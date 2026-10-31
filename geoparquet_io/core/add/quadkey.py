#!/usr/bin/env python3

from __future__ import annotations

import mercantile
import pyarrow as pa

from geoparquet_io.core.constants import DEFAULT_QUADKEY_COLUMN_NAME, DEFAULT_QUADKEY_RESOLUTION
from geoparquet_io.core.crs_utils import (
    crs_of,
    get_crs_display_name,
    is_geographic,
    is_projected,
)
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.index_ops import IndexSpec, add_index_column, add_index_table
from geoparquet_io.core.logging_config import (
    debug,
)
from geoparquet_io.core.parquet_writer import WriteOptions


def _validate_crs(source, geom_col: str, verbose: bool, source_description: str = "data") -> None:
    """Refuse a projected source when no index transform is available."""
    crs_info = crs_of(source, geometry_column=geom_col, quiet=True)
    if is_projected(crs_info):
        crs_name = get_crs_display_name(crs_info)
        raise GeoParquetError(
            f"Quadkeys require geographic coordinates (lat/lon), but this {source_description} "
            f"uses a projected CRS: {crs_name}\n\n"
            f"Reproject to WGS84 first using:\n"
            f"  gpio convert reproject <input> <output> --dst-crs EPSG:4326"
        )

    if verbose and is_geographic(crs_info):
        debug("CRS validated as geographic (lat/lon coordinates)")


def _get_resolution_size(resolution):
    return f"resolution {resolution}"


def _cell_expression(lon, lat, resolution):
    return f"lat_lon_to_quadkey({lat}, {lon}, {resolution})"


#: Past Web Mercator's edge, short of the pole mercantile cannot take.
_POLE_SAFE_LAT = 89.9


def _lat_lon_to_quadkey(lat: float, lon: float, level: int) -> str:
    """Convert latitude and longitude to a quadkey string using mercantile.

    Web Mercator stops at about 85.05 degrees, and every latitude past it keys to
    the edge row. mercantile raises at a pole itself, where a spherical polar cap's
    point lies (#1293), so the latitude is held to 89.9, which keys to that row.
    """
    lat = max(-_POLE_SAFE_LAT, min(_POLE_SAFE_LAT, lat))
    tile = mercantile.tile(lon, lat, level)
    return mercantile.quadkey(tile)


def register_quadkey_udf(con) -> None:
    """Register :func:`_lat_lon_to_quadkey` as ``lat_lon_to_quadkey(lat, lon, level)``.

    The one quadkey keyer and its one latitude bound, for every path that keys
    a quadkey in SQL: add, sort and partition (through ``SPEC.register``), the
    partition resolution probe, and the overview tile probe.
    """
    con.create_function(
        "lat_lon_to_quadkey",
        _lat_lon_to_quadkey,
        ["DOUBLE", "DOUBLE", "INTEGER"],
        "VARCHAR",
    )


SPEC = IndexSpec(
    "quadkey",
    DEFAULT_QUADKEY_COLUMN_NAME,
    0,
    23,
    _cell_expression,
    1,
    4,
    None,
    _get_resolution_size,
    default_resolution=DEFAULT_QUADKEY_RESOLUTION,
    label="quadkey",
    column_option="--quadkey-column",
    register=register_quadkey_udf,
    bbox_midpoint=True,
    crs_validator=_validate_crs,
    prefix_partition=True,
    coerce_resolution=True,
    display_unit="zoom level",
    multiple_files=True,
    report_count=False,
)


def add_quadkey_table(
    table: pa.Table,
    quadkey_column_name: str = DEFAULT_QUADKEY_COLUMN_NAME,
    resolution: int = SPEC.default_resolution,
    use_centroid: bool = False,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add a quadkey column to an Arrow Table.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table
        quadkey_column_name: Name for the quadkey column (default: 'quadkey')
        resolution: Quadkey zoom level (0-23). Default: 13
        use_centroid: Force using geometry centroid even if bbox exists
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with quadkey column added

    Raises:
        ValueError: If resolution is not an integer between 0 and 23
        GeoParquetError: If CRS is detected as projected (quadkeys require lat/lon)
    """
    return add_index_table(
        SPEC, table, quadkey_column_name, resolution, geometry_column, use_centroid=use_centroid
    )


def add_quadkey_column(
    input_parquet: str,
    output_parquet: str | None = None,
    quadkey_column_name: str = DEFAULT_QUADKEY_COLUMN_NAME,
    resolution: int = SPEC.default_resolution,
    use_centroid: bool = False,
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
    allow_schema_diff: bool = False,
    hive_input: bool | None = None,
) -> None:
    """
    Add a quadkey column to a GeoParquet file.

    Computes quadkey tile IDs based on geometry location. By default, uses the
    bbox column midpoint if available, otherwise falls back to geometry centroid.

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    Args:
        input_parquet: Path to the input parquet file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
        quadkey_column_name: Name for the quadkey column (default: 'quadkey')
        resolution: Quadkey zoom level (0-23). Default: 13
        use_centroid: Force using geometry centroid even if bbox exists
        dry_run: Whether to print SQL commands without executing them
        verbose: Whether to print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        memory_limit: DuckDB memory limit for the write (e.g., '2GB', '512MB')
        allow_schema_diff: Read a multi-file input as the union of its columns
            (DuckDB ``union_by_name``) rather than the first file's schema
        hive_input: Read a multi-file input with hive partitioning on, so the
            ``key=value`` directory names come through as columns
    """
    write = WriteOptions.pick(locals())
    return add_index_column(
        SPEC,
        input_parquet,
        output_parquet,
        quadkey_column_name,
        resolution,
        dry_run,
        verbose,
        write,
        overwrite,
        use_centroid=use_centroid,
        allow_schema_diff=allow_schema_diff,
        hive_input=hive_input,
    )
