"""
Python API for raquet (raster parquet) operations.

Provides functional API for working with raquet files:

    from geoparquet_io.api import raster

    # Check if file is raquet
    if raster.is_raquet('file.parquet'):
        meta = raster.read_metadata('file.parquet')
        print(meta.bounds)

    # Convert GeoTIFF to raquet
    table = raster.geotiff_to_table('input.tif', block_size=256)
    raster.convert_geotiff('input.tif', 'output.parquet')

    # Export raquet to GeoTIFF
    raster.export_geotiff('raster.parquet', 'output.tif')

Raquet is a specification for storing raster data in Parquet format using QUADBIN
spatial indexing. See https://github.com/CartoDB/raquet for the specification.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from geoparquet_io.core.raquet import (
    RaquetMetadata,
    geotiff_to_raquet,
    geotiff_to_raquet_table,
    get_band_columns,
    is_raquet_file,
    raquet_to_geotiff,
    read_raquet_metadata,
)


def is_raquet(parquet_file: str) -> bool:
    """
    Check if a parquet file is a valid raquet file.

    A valid raquet file has:
    - A 'block' column (uint64) for QUADBIN cell IDs
    - A 'metadata' column with JSON metadata at block=0
    - One or more band columns with pixel data

    Args:
        parquet_file: Path to parquet file

    Returns:
        True if file has raquet structure
    """
    return is_raquet_file(parquet_file)


def read_metadata(parquet_file: str) -> RaquetMetadata | None:
    """
    Read raquet metadata from a parquet file.

    Args:
        parquet_file: Path to raquet parquet file

    Returns:
        RaquetMetadata dataclass with:
        - version: Raquet version string
        - compression: "gzip" or None
        - bounds: (west, south, east, north) in WGS84
        - bands: List of band info dicts
        - block_width, block_height: Block dimensions
        - num_blocks, num_pixels: Data counts
        - And more...

        Returns None if not a valid raquet file.
    """
    return read_raquet_metadata(parquet_file)


def get_bands(parquet_file: str) -> list[str]:
    """
    Get list of band column names from a raquet file.

    Args:
        parquet_file: Path to raquet parquet file

    Returns:
        List of band column names (e.g., ['band_1', 'band_2'])
    """
    return get_band_columns(parquet_file)


def geotiff_to_table(
    input_geotiff: str,
    *,
    block_size: int = 256,
    compression: str | None = "gzip",
    target_resolution: int | None = None,
    include_overviews: bool = False,
    skip_empty_blocks: bool = True,
    calculate_stats: bool = True,
) -> pa.Table:
    """
    Convert a GeoTIFF to a raquet PyArrow Table.

    The GeoTIFF is reprojected to Web Mercator (EPSG:3857) and tiled
    according to the QUADBIN spatial indexing system.

    Args:
        input_geotiff: Path to GeoTIFF file
        block_size: Tile size in pixels (256 or 512, must be divisible by 16)
        compression: Block compression ("gzip" or None)
        target_resolution: QUADBIN pixel resolution 0-26 (auto if None)
        include_overviews: Include overview pyramids (not yet implemented)
        skip_empty_blocks: Skip blocks with all nodata values
        calculate_stats: Calculate per-band statistics

    Returns:
        PyArrow Table with raquet structure:
        - block (uint64): QUADBIN cell ID, block=0 for metadata
        - metadata (string): JSON metadata in row where block=0
        - band_1, band_2, ... (binary): Compressed pixel data
    """
    return geotiff_to_raquet_table(
        input_geotiff,
        block_size=block_size,
        compression=compression,
        target_resolution=target_resolution,
        include_overviews=include_overviews,
        skip_empty_blocks=skip_empty_blocks,
        calculate_stats=calculate_stats,
    )


def convert_geotiff(
    input_geotiff: str,
    output_parquet: str,
    *,
    block_size: int = 256,
    compression: str | None = "gzip",
    parquet_compression: str = "ZSTD",
    target_resolution: int | None = None,
    include_overviews: bool = False,
    skip_empty_blocks: bool = True,
    calculate_stats: bool = True,
    verbose: bool = False,
) -> dict[str, Any]:
    """
    Convert a GeoTIFF to raquet parquet file.

    The GeoTIFF is reprojected to Web Mercator (EPSG:3857) and tiled
    according to the QUADBIN spatial indexing system.

    Args:
        input_geotiff: Path to GeoTIFF file
        output_parquet: Path for output parquet file
        block_size: Tile size in pixels (256 or 512)
        compression: Block compression ("gzip" or None)
        parquet_compression: Parquet file compression (ZSTD, GZIP, SNAPPY, etc.)
        target_resolution: QUADBIN pixel resolution 0-26 (auto if None)
        include_overviews: Include overview pyramids (not yet implemented)
        skip_empty_blocks: Skip blocks with all nodata values
        calculate_stats: Calculate per-band statistics
        verbose: Print progress

    Returns:
        dict with conversion stats:
        - num_blocks: Number of blocks written
        - num_bands: Number of bands
        - num_pixels: Total pixel count
        - block_size: Block size used
        - compression: Compression used
    """
    return geotiff_to_raquet(
        input_geotiff,
        output_parquet,
        block_size=block_size,
        compression=compression,
        parquet_compression=parquet_compression,
        target_resolution=target_resolution,
        include_overviews=include_overviews,
        skip_empty_blocks=skip_empty_blocks,
        calculate_stats=calculate_stats,
        verbose=verbose,
    )


def to_array(
    parquet_file: str,
    *,
    bands: list[str] | None = None,
    resolution: int | None = None,
) -> tuple:
    """
    Convert raquet to numpy array and metadata.

    Note: For large rasters, consider using export_geotiff() to write
    directly to disk instead of loading into memory.

    Args:
        parquet_file: Path to raquet parquet file
        bands: Band names to include (all if None)
        resolution: Resolution level (max if None)

    Returns:
        tuple of (numpy array, metadata dict with transform, crs, etc.)
    """
    import tempfile
    import uuid

    import rasterio

    # Export to temporary GeoTIFF and read it back
    # This is simpler than reconstructing the array directly
    temp_path = f"{tempfile.gettempdir()}/raquet_temp_{uuid.uuid4()}.tif"

    try:
        export_geotiff(parquet_file, temp_path, bands=bands, resolution=resolution)

        with rasterio.open(temp_path) as src:
            data = src.read()
            metadata = {
                "transform": src.transform,
                "crs": src.crs,
                "bounds": src.bounds,
                "nodata": src.nodata,
                "width": src.width,
                "height": src.height,
                "count": src.count,
                "dtype": src.dtypes[0],
            }
        return data, metadata
    finally:
        import os

        if os.path.exists(temp_path):
            os.unlink(temp_path)


def export_geotiff(
    input_parquet: str,
    output_geotiff: str,
    *,
    bands: list[str] | None = None,
    resolution: int | None = None,
    verbose: bool = False,
) -> dict[str, Any]:
    """
    Export raquet parquet to GeoTIFF file.

    The output GeoTIFF will be in Web Mercator (EPSG:3857) coordinate system.

    Args:
        input_parquet: Path to raquet parquet file
        output_geotiff: Path for output GeoTIFF
        bands: Band names to export (all if None)
        resolution: Resolution level (max if None)
        verbose: Print progress

    Returns:
        dict with export stats:
        - width: Output width in pixels
        - height: Output height in pixels
        - num_bands: Number of bands exported
        - resolution: Resolution level used
    """
    return raquet_to_geotiff(
        input_parquet,
        output_geotiff,
        bands=bands,
        resolution=resolution,
        verbose=verbose,
    )


# Re-export the metadata class for type hints
__all__ = [
    "RaquetMetadata",
    "is_raquet",
    "read_metadata",
    "get_bands",
    "geotiff_to_table",
    "convert_geotiff",
    "to_array",
    "export_geotiff",
]
