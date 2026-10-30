#!/usr/bin/env python3

from __future__ import annotations

import mercantile
import pyarrow as pa

from geoparquet_io.core.constants import DEFAULT_QUADKEY_COLUMN_NAME, DEFAULT_QUADKEY_RESOLUTION
from geoparquet_io.core.crs_utils import (
    get_crs_display_name,
    is_empty_crs_value,
    warn_empty_crs_once,
)
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.index_ops import IndexSpec, add_index_column, add_index_table
from geoparquet_io.core.logging_config import (
    debug,
)


def _is_geographic_crs(crs_info: dict | str | None) -> bool | None:
    """
    Check if CRS is geographic (lat/long) vs projected.

    Returns:
        True if geographic, False if projected, None if unknown (also for an
        empty value such as ``""`` or ``{}``, #1267)
    """
    if crs_info is None or is_empty_crs_value(crs_info):
        return None

    if isinstance(crs_info, str):
        crs_upper = crs_info.upper()
        # Common geographic CRS codes
        if any(
            code in crs_upper for code in ["4326", "CRS84", "CRS:84", "OGC:CRS84", "4269", "4267"]
        ):
            return True
        return None

    if isinstance(crs_info, dict):
        # Check PROJJSON type
        crs_type = crs_info.get("type", "")
        if crs_type == "GeographicCRS":
            return True
        if crs_type == "ProjectedCRS":
            return False

        # Check EPSG code
        crs_id = crs_info.get("id", {})
        if isinstance(crs_id, dict):
            code = crs_id.get("code")
            if code in [4326, 4269, 4267]:  # Common geographic codes
                return True

    return None


def _validate_crs_from_geo_metadata(
    geo_meta: dict | None,
    geom_col: str,
    verbose: bool,
    source_description: str = "data",
) -> None:
    """
    Validate CRS from geo metadata dictionary.

    Common helper used by file-based, streaming, and table-based paths.

    Args:
        geo_meta: Parsed geo metadata dict (from GeoParquet schema)
        geom_col: Name of the geometry column
        verbose: Whether to print debug output
        source_description: Description for error messages (e.g., "file", "stream", "table")

    Raises:
        GeoParquetError: If CRS is detected as projected
    """
    if not geo_meta:
        if verbose:
            debug("No GeoParquet metadata found, assuming WGS84 coordinates")
        return

    columns_meta = geo_meta.get("columns", {})
    if geom_col not in columns_meta:
        if verbose:
            debug(f"Geometry column '{geom_col}' not found in metadata, assuming WGS84")
        return

    crs_info = columns_meta[geom_col].get("crs")

    # An empty value names no CRS: unknown, like null, and said so (#1267).
    if is_empty_crs_value(crs_info):
        warn_empty_crs_once(crs_info)
        return

    # No CRS specified means default (WGS84)
    if crs_info is None:
        if verbose:
            debug("No CRS specified in metadata, using default WGS84")
        return

    is_geographic = _is_geographic_crs(crs_info)

    if is_geographic is False:
        crs_name = get_crs_display_name(crs_info)
        raise GeoParquetError(
            f"Quadkeys require geographic coordinates (lat/lon), but this {source_description} "
            f"uses a projected CRS: {crs_name}\n\n"
            f"Reproject to WGS84 first using:\n"
            f"  gpio convert reproject <input> <output> --dst-crs EPSG:4326"
        )

    if verbose and is_geographic:
        debug("CRS validated as geographic (lat/lon coordinates)")


def _get_resolution_size(resolution):
    return f"resolution {resolution}"


def _cell_expression(lon, lat, resolution):
    return f"lat_lon_to_quadkey({lat}, {lon}, {resolution})"


def _lat_lon_to_quadkey(lat: float, lon: float, level: int) -> str:
    return str(mercantile.quadkey(mercantile.tile(lon, lat, level)))


def _register(con):
    con.create_function(
        "lat_lon_to_quadkey", _lat_lon_to_quadkey, ["DOUBLE", "DOUBLE", "INTEGER"], "VARCHAR"
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
    register=_register,
    bbox_midpoint=True,
    crs_validator=_validate_crs_from_geo_metadata,
    prefix_partition=True,
    coerce_resolution=True,
    display_unit="zoom level",
    multiple_files=True,
    report_count=False,
)


def register_quadkey_udf(con) -> None:
    """Register :func:`_lat_lon_to_quadkey` as ``lat_lon_to_quadkey(lat, lon, level)``.

    The one quadkey keyer and its one latitude bound, for every path that keys
    a quadkey in SQL: add, sort and partition, the partition resolution probe,
    and the overview tile probe.
    """
    con.create_function(
        "lat_lon_to_quadkey",
        _lat_lon_to_quadkey,
        ["DOUBLE", "DOUBLE", "INTEGER"],
        "VARCHAR",
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
    return add_index_column(
        SPEC,
        input_parquet,
        output_parquet,
        quadkey_column_name,
        resolution,
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
        use_centroid=use_centroid,
        allow_schema_diff=allow_schema_diff,
        hive_input=hive_input,
    )
