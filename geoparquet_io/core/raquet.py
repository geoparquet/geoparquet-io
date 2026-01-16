"""
Core raquet (raster parquet) functionality.

This module provides the business logic for:
- Detecting raquet files
- Reading raquet metadata
- Converting GeoTIFF to raquet format
- Exporting raquet to GeoTIFF

Raquet is a specification for storing raster data in Parquet format using QUADBIN
spatial indexing. See https://github.com/CartoDB/raquet for the specification.
"""

from __future__ import annotations

import gzip
import json
import math
from dataclasses import dataclass
from typing import Any

import mercantile
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import quadbin

from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success, warn

# Constants
RAQUET_VERSION = "0.1.0"
METADATA_BLOCK_ID = 0  # block=0 is reserved for metadata
DEFAULT_BLOCK_SIZE = 256
WEB_MERCATOR_CRS = "EPSG:3857"


@dataclass
class BandInfo:
    """Information about a single raster band."""

    type: str
    name: str
    stats: dict[str, Any] | None = None
    colorinterp: str | None = None
    nodata: str | None = None
    colortable: dict[str, list[int]] | None = None


@dataclass
class RaquetMetadata:
    """Parsed raquet metadata structure."""

    version: str
    compression: str | None  # "gzip" or None
    block_resolution: int
    minresolution: int
    maxresolution: int
    nodata: float | int | None
    bounds: tuple[float, float, float, float]  # [west, south, east, north]
    center: tuple[float, float, int]  # [lon, lat, resolution]
    width: int
    height: int
    block_width: int
    block_height: int
    num_blocks: int
    num_pixels: int
    pixel_resolution: int
    bands: list[dict[str, Any]]


def is_raquet_file(parquet_file: str) -> bool:
    """
    Detect if a parquet file is a raquet file.

    Checks for:
    - 'block' column (uint64 or int64)
    - 'metadata' column (string)
    - Row where block=0 with non-null metadata

    Args:
        parquet_file: Path to parquet file

    Returns:
        True if file has raquet structure
    """
    try:
        pf = pq.ParquetFile(parquet_file)
        schema = pf.schema_arrow

        # Check for required columns
        column_names = schema.names
        if "block" not in column_names or "metadata" not in column_names:
            return False

        # Check block column type (should be uint64 or int64)
        block_field = schema.field("block")
        if block_field.type not in (pa.uint64(), pa.int64()):
            return False

        # Check for metadata row at block=0
        table = pq.read_table(
            parquet_file, columns=["block", "metadata"], filters=[("block", "=", 0)]
        )

        if len(table) == 0:
            return False

        # Check that metadata is not null
        metadata_value = table.column("metadata")[0].as_py()
        if metadata_value is None:
            return False

        # Try to parse as JSON
        try:
            json.loads(metadata_value)
            return True
        except (json.JSONDecodeError, TypeError):
            return False

    except Exception:
        return False


def read_raquet_metadata(parquet_file: str) -> RaquetMetadata | None:
    """
    Read and parse raquet metadata from a parquet file.

    Args:
        parquet_file: Path to raquet parquet file

    Returns:
        RaquetMetadata dataclass or None if not valid raquet
    """
    if not is_raquet_file(parquet_file):
        return None

    try:
        table = pq.read_table(
            parquet_file, columns=["block", "metadata"], filters=[("block", "=", 0)]
        )

        metadata_json = table.column("metadata")[0].as_py()
        meta = json.loads(metadata_json)

        return RaquetMetadata(
            version=meta.get("version", "0.1.0"),
            compression=meta.get("compression"),
            block_resolution=meta.get("block_resolution", 0),
            minresolution=meta.get("minresolution", 0),
            maxresolution=meta.get("maxresolution", 0),
            nodata=meta.get("nodata"),
            bounds=tuple(meta.get("bounds", [0, 0, 0, 0])),
            center=tuple(meta.get("center", [0, 0, 0])),
            width=meta.get("width", 0),
            height=meta.get("height", 0),
            block_width=meta.get("block_width", DEFAULT_BLOCK_SIZE),
            block_height=meta.get("block_height", DEFAULT_BLOCK_SIZE),
            num_blocks=meta.get("num_blocks", 0),
            num_pixels=meta.get("num_pixels", 0),
            pixel_resolution=meta.get("pixel_resolution", 0),
            bands=meta.get("bands", []),
        )
    except Exception:
        return None


def get_band_columns(parquet_file: str) -> list[str]:
    """
    Get list of band column names from a raquet file.

    Args:
        parquet_file: Path to raquet parquet file

    Returns:
        List of band column names (e.g., ['band_1', 'band_2'])
    """
    pf = pq.ParquetFile(parquet_file)
    schema = pf.schema_arrow

    # Band columns are all columns except 'block' and 'metadata'
    return [name for name in schema.names if name not in ("block", "metadata")]


def format_raquet_metadata(metadata: RaquetMetadata, verbose: bool = False) -> None:
    """
    Format and print raquet metadata to terminal.

    Args:
        metadata: RaquetMetadata dataclass
        verbose: Show additional details
    """
    from rich.console import Console
    from rich.table import Table

    console = Console()

    # Header
    console.print("\n[bold cyan]Raquet Metadata[/bold cyan]")
    console.print("=" * 50)

    # Basic info table
    info_table = Table(show_header=False, box=None)
    info_table.add_column("Property", style="dim")
    info_table.add_column("Value")

    info_table.add_row("Version", metadata.version)
    info_table.add_row("Compression", metadata.compression or "None")
    info_table.add_row(
        "Bounds (WSEN)",
        f"[{metadata.bounds[0]:.6f}, {metadata.bounds[1]:.6f}, "
        f"{metadata.bounds[2]:.6f}, {metadata.bounds[3]:.6f}]",
    )
    info_table.add_row("Center", f"[{metadata.center[0]:.6f}, {metadata.center[1]:.6f}]")
    info_table.add_row("Dimensions", f"{metadata.width} x {metadata.height} pixels")
    info_table.add_row("Block Size", f"{metadata.block_width} x {metadata.block_height} pixels")
    info_table.add_row("Number of Blocks", str(metadata.num_blocks))
    info_table.add_row("Total Pixels", f"{metadata.num_pixels:,}")

    console.print(info_table)

    # Resolution info
    console.print("\n[bold]Resolution[/bold]")
    res_table = Table(show_header=False, box=None)
    res_table.add_column("Property", style="dim")
    res_table.add_column("Value")
    res_table.add_row("Block Resolution", str(metadata.block_resolution))
    res_table.add_row("Pixel Resolution", str(metadata.pixel_resolution))
    res_table.add_row("Min Resolution", str(metadata.minresolution))
    res_table.add_row("Max Resolution", str(metadata.maxresolution))
    console.print(res_table)

    # Bands info
    console.print(f"\n[bold]Bands ({len(metadata.bands)})[/bold]")

    for i, band in enumerate(metadata.bands):
        band_table = Table(
            show_header=False, box=None, title=f"Band {i + 1}: {band.get('name', 'unnamed')}"
        )
        band_table.add_column("Property", style="dim")
        band_table.add_column("Value")

        band_table.add_row("Type", band.get("type", "unknown"))
        if band.get("colorinterp"):
            band_table.add_row("Color Interpretation", band["colorinterp"])
        if band.get("nodata"):
            band_table.add_row("NoData", str(band["nodata"]))

        # Stats
        if band.get("stats") and verbose:
            stats = band["stats"]
            if "min" in stats:
                band_table.add_row("Min", f"{stats['min']}")
            if "max" in stats:
                band_table.add_row("Max", f"{stats['max']}")
            if "mean" in stats:
                band_table.add_row("Mean", f"{stats['mean']:.4f}")
            if "stddev" in stats:
                band_table.add_row("Std Dev", f"{stats['stddev']:.4f}")
            if "count" in stats:
                band_table.add_row("Valid Pixel Count", f"{stats['count']:,}")

        console.print(band_table)


# === GeoTIFF to Raquet Conversion ===


def _get_numpy_dtype(rasterio_dtype: str) -> str:
    """Map rasterio dtype to raquet type string."""
    dtype_map = {
        "uint8": "uint8",
        "int8": "int8",
        "uint16": "uint16",
        "int16": "int16",
        "uint32": "uint32",
        "int32": "int32",
        "uint64": "uint64",
        "int64": "int64",
        "float32": "float32",
        "float64": "float64",
    }
    return dtype_map.get(str(rasterio_dtype), "float32")


def _calculate_target_resolution(
    bounds: tuple[float, float, float, float],
    width: int,
    height: int,
    block_size: int,
) -> int:
    """
    Calculate the appropriate QUADBIN resolution for the raster.

    Based on the raster's resolution in Web Mercator coordinates.

    Args:
        bounds: (west, south, east, north) in Web Mercator
        width: Raster width in pixels
        height: Raster height in pixels
        block_size: Block size (256 or 512)

    Returns:
        QUADBIN pixel resolution (0-26)
    """
    # Calculate meters per pixel
    x_res = (bounds[2] - bounds[0]) / width
    y_res = (bounds[3] - bounds[1]) / height
    resolution_m = (x_res + y_res) / 2

    # Web Mercator circumference
    circumference = mercantile.CE  # ~40075016.68 meters

    # Calculate zoom level based on resolution
    # At zoom z, tile size is circumference / 2^z
    # Pixel resolution = tile_size / block_size
    block_zoom = int(math.log2(block_size))

    # pixel_res = circumference / (2^z * block_size)
    # 2^z = circumference / (pixel_res * block_size)
    # z = log2(circumference / (pixel_res * block_size))
    raw_zoom = math.log2(circumference / (resolution_m * block_size))
    zoom = max(0, min(26 - block_zoom, round(raw_zoom)))

    # Pixel resolution is zoom + block_zoom
    return zoom + block_zoom


def _calculate_band_statistics(
    data: np.ndarray,
    nodata: float | int | None,
) -> dict[str, Any]:
    """
    Calculate band statistics.

    Args:
        data: Numpy array of pixel values
        nodata: NoData value to exclude

    Returns:
        dict with min, max, mean, stddev, sum, sum_squares, count
    """
    # Create mask for valid data
    if nodata is not None:
        mask = (data != nodata) & ~np.isnan(data.astype(float))
    else:
        mask = ~np.isnan(data.astype(float))

    valid_data = data[mask]

    if len(valid_data) == 0:
        return {"count": 0}

    return {
        "min": float(np.min(valid_data)),
        "max": float(np.max(valid_data)),
        "mean": float(np.mean(valid_data)),
        "stddev": float(np.std(valid_data)),
        "sum": float(np.sum(valid_data)),
        "sum_squares": float(np.sum(valid_data.astype(float) ** 2)),
        "count": int(len(valid_data)),
        "approximated_stats": False,
    }


def _compress_block_data(data: np.ndarray, compression: str | None) -> bytes:
    """
    Compress block pixel data.

    Args:
        data: Numpy array (2D) in row-major order
        compression: "gzip" or None

    Returns:
        Binary data, optionally gzip compressed
    """
    # Ensure row-major order (C order)
    raw_bytes = np.ascontiguousarray(data).tobytes()

    if compression == "gzip":
        return gzip.compress(raw_bytes)
    return raw_bytes


def _is_block_empty(data: np.ndarray, nodata: float | int | None) -> bool:
    """Check if block contains only nodata values."""
    if nodata is None:
        return False
    return np.all(data == nodata)


def geotiff_to_raquet_table(
    input_geotiff: str,
    *,
    block_size: int = DEFAULT_BLOCK_SIZE,
    compression: str | None = "gzip",
    target_resolution: int | None = None,
    include_overviews: bool = False,
    skip_empty_blocks: bool = True,
    calculate_stats: bool = True,
    verbose: bool = False,
) -> pa.Table:
    """
    Convert a GeoTIFF file to a raquet PyArrow Table.

    This is the core conversion function used by both CLI and Python API.

    Args:
        input_geotiff: Path to GeoTIFF file
        block_size: Tile size in pixels (must be divisible by 16, default 256)
        compression: Block compression ("gzip" or None)
        target_resolution: Target QUADBIN pixel resolution (auto if None)
        include_overviews: Include overview pyramids (not yet implemented)
        skip_empty_blocks: Skip blocks with all nodata values
        calculate_stats: Calculate per-band statistics

    Returns:
        PyArrow Table with raquet structure
    """
    import rasterio
    from rasterio.crs import CRS
    from rasterio.warp import Resampling, calculate_default_transform, reproject

    configure_verbose(verbose)

    if block_size % 16 != 0:
        raise ValueError("Block size must be divisible by 16")

    with rasterio.open(input_geotiff) as src:
        # Get source info
        src_crs = src.crs
        src_bounds = src.bounds
        src_width = src.width
        src_height = src.height
        num_bands = src.count
        src_dtype = src.dtypes[0]
        nodata = src.nodata

        debug(f"Source: {src_width}x{src_height}, {num_bands} bands, CRS: {src_crs}")

        # Calculate transform to Web Mercator
        dst_crs = CRS.from_epsg(3857)
        transform, dst_width, dst_height = calculate_default_transform(
            src_crs, dst_crs, src_width, src_height, *src_bounds
        )

        # Reproject all bands to Web Mercator
        reprojected_data = np.zeros((num_bands, dst_height, dst_width), dtype=src_dtype)

        for i in range(num_bands):
            reproject(
                source=rasterio.band(src, i + 1),
                destination=reprojected_data[i],
                src_transform=src.transform,
                src_crs=src_crs,
                dst_transform=transform,
                dst_crs=dst_crs,
                resampling=Resampling.nearest,
            )

        # Get bounds in Web Mercator
        dst_bounds = rasterio.transform.array_bounds(dst_height, dst_width, transform)

        # Calculate target resolution if not specified
        if target_resolution is None:
            target_resolution = _calculate_target_resolution(
                dst_bounds, dst_width, dst_height, block_size
            )

        block_zoom = int(math.log2(block_size))
        tile_zoom = target_resolution - block_zoom

        debug(f"Target resolution: {target_resolution}, tile zoom: {tile_zoom}")

        # Get tiles that cover the bounds
        # Convert Web Mercator bounds to lat/lon for mercantile
        from pyproj import Transformer

        transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
        lon_min, lat_min = transformer.transform(dst_bounds[0], dst_bounds[1])
        lon_max, lat_max = transformer.transform(dst_bounds[2], dst_bounds[3])

        # Clamp to valid ranges
        lon_min = max(-180, min(180, lon_min))
        lon_max = max(-180, min(180, lon_max))
        lat_min = max(-85.051129, min(85.051129, lat_min))
        lat_max = max(-85.051129, min(85.051129, lat_max))

        tiles = list(mercantile.tiles(lon_min, lat_min, lon_max, lat_max, zooms=tile_zoom))

        debug(f"Processing {len(tiles)} tiles at zoom {tile_zoom}")

        # Prepare band names and schema
        band_names = [f"band_{i + 1}" for i in range(num_bands)]

        # Collect rows
        rows = []
        band_stats = [[] for _ in range(num_bands)]

        # Track bounds for metadata
        xmin, ymin, xmax, ymax = float("inf"), float("inf"), float("-inf"), float("-inf")

        for tile in tiles:
            # Get tile bounds in Web Mercator
            tile_bounds = mercantile.xy_bounds(tile)

            # Calculate pixel coordinates for this tile in the reprojected array
            # Transform from Web Mercator to pixel coordinates
            col_start = int((tile_bounds.left - dst_bounds[0]) / transform.a)
            row_start = int((dst_bounds[3] - tile_bounds.top) / (-transform.e))
            col_end = col_start + block_size
            row_end = row_start + block_size

            # Skip if completely outside
            if col_start >= dst_width or row_start >= dst_height or col_end <= 0 or row_end <= 0:
                continue

            # Clamp to array bounds
            src_col_start = max(0, col_start)
            src_row_start = max(0, row_start)
            src_col_end = min(dst_width, col_end)
            src_row_end = min(dst_height, row_end)

            # Create block array (filled with nodata if partial)
            block_data = {}
            all_empty = True

            for band_idx in range(num_bands):
                block_array = np.full(
                    (block_size, block_size), nodata if nodata else 0, dtype=src_dtype
                )

                # Calculate destination offsets for partial tiles
                dst_col_start = src_col_start - col_start
                dst_row_start = src_row_start - row_start
                dst_col_end = dst_col_start + (src_col_end - src_col_start)
                dst_row_end = dst_row_start + (src_row_end - src_row_start)

                # Copy data
                block_array[dst_row_start:dst_row_end, dst_col_start:dst_col_end] = (
                    reprojected_data[band_idx, src_row_start:src_row_end, src_col_start:src_col_end]
                )

                # Check if empty
                if skip_empty_blocks and _is_block_empty(block_array, nodata):
                    continue

                all_empty = False

                # Compress block data
                compressed = _compress_block_data(block_array, compression)
                block_data[band_names[band_idx]] = compressed

                # Collect stats
                if calculate_stats:
                    stats = _calculate_band_statistics(block_array, nodata)
                    if stats.get("count", 0) > 0:
                        band_stats[band_idx].append(stats)

            if all_empty and skip_empty_blocks:
                continue

            # Get QUADBIN cell ID
            quadbin_id = quadbin.tile_to_cell((tile.x, tile.y, tile.z))

            # Add row
            row = {
                "block": quadbin_id,
                "metadata": None,
                **{name: block_data.get(name) for name in band_names},
            }
            rows.append(row)

            # Update bounds
            xmin = min(xmin, tile.x)
            ymin = min(ymin, tile.y)
            xmax = max(xmax, tile.x)
            ymax = max(ymax, tile.y)

        progress(f"Processed {len(rows)} blocks")

        # Aggregate band statistics
        aggregated_stats = []
        for band_idx in range(num_bands):
            if band_stats[band_idx]:
                total_count = sum(s["count"] for s in band_stats[band_idx])
                if total_count > 0:
                    aggregated_stats.append(
                        {
                            "min": min(s["min"] for s in band_stats[band_idx]),
                            "max": max(s["max"] for s in band_stats[band_idx]),
                            "mean": sum(s["mean"] * s["count"] for s in band_stats[band_idx])
                            / total_count,
                            "stddev": math.sqrt(
                                sum(s["stddev"] ** 2 * s["count"] for s in band_stats[band_idx])
                                / total_count
                            ),
                            "count": total_count,
                            "approximated_stats": True,
                        }
                    )
                else:
                    aggregated_stats.append(None)
            else:
                aggregated_stats.append(None)

        # Get color interpretation
        color_interps = []
        for i in range(num_bands):
            ci = src.colorinterp[i]
            color_interps.append(ci.name.lower() if ci else None)

        # Create metadata
        if xmin == float("inf"):
            # No tiles processed
            final_bounds = [lon_min, lat_min, lon_max, lat_max]
        else:
            # Calculate bounds from processed tiles
            ul_tile = mercantile.Tile(x=int(xmin), y=int(ymin), z=tile_zoom)
            lr_tile = mercantile.Tile(x=int(xmax), y=int(ymax), z=tile_zoom)
            ul_bounds = mercantile.bounds(ul_tile)
            lr_bounds = mercantile.bounds(lr_tile)
            final_bounds = [ul_bounds.west, lr_bounds.south, lr_bounds.east, ul_bounds.north]

        metadata_dict = {
            "version": RAQUET_VERSION,
            "compression": compression,
            "block_resolution": tile_zoom,
            "minresolution": tile_zoom,  # No overviews for now
            "maxresolution": tile_zoom,
            "nodata": nodata,
            "bounds": final_bounds,
            "center": [
                (final_bounds[0] + final_bounds[2]) / 2,
                (final_bounds[1] + final_bounds[3]) / 2,
                tile_zoom,
            ],
            "width": dst_width,
            "height": dst_height,
            "block_width": block_size,
            "block_height": block_size,
            "num_blocks": len(rows),
            "num_pixels": len(rows) * block_size * block_size,
            "pixel_resolution": target_resolution,
            "bands": [
                {
                    "type": _get_numpy_dtype(src_dtype),
                    "name": band_names[i],
                    "colorinterp": color_interps[i],
                    "nodata": str(nodata) if nodata is not None else None,
                    "stats": aggregated_stats[i],
                    "colortable": None,
                }
                for i in range(num_bands)
            ],
        }

        # Add metadata row
        metadata_row = {
            "block": METADATA_BLOCK_ID,
            "metadata": json.dumps(metadata_dict),
            **dict.fromkeys(band_names),
        }
        rows.insert(0, metadata_row)

        # Create schema
        schema = pa.schema(
            [
                ("block", pa.uint64()),
                ("metadata", pa.string()),
                *[(name, pa.binary()) for name in band_names],
            ]
        )

        # Build table
        table_dict = {key: [row[key] for row in rows] for key in schema.names}
        table = pa.Table.from_pydict(table_dict, schema=schema)

        return table


def geotiff_to_raquet(
    input_geotiff: str,
    output_parquet: str,
    *,
    block_size: int = DEFAULT_BLOCK_SIZE,
    compression: str | None = "gzip",
    parquet_compression: str = "ZSTD",
    target_resolution: int | None = None,
    include_overviews: bool = False,
    skip_empty_blocks: bool = True,
    calculate_stats: bool = True,
    verbose: bool = False,
) -> dict[str, Any]:
    """
    Convert a GeoTIFF file to raquet parquet format.

    This is the file-based wrapper called by the CLI.

    Args:
        input_geotiff: Path to GeoTIFF file
        output_parquet: Path for output parquet file
        block_size: Tile size in pixels (default 256)
        compression: Block compression ("gzip" or None)
        parquet_compression: Parquet file compression (ZSTD, GZIP, etc.)
        target_resolution: Target QUADBIN pixel resolution (auto if None)
        include_overviews: Include overview pyramids
        skip_empty_blocks: Skip nodata-only blocks
        calculate_stats: Calculate band statistics
        verbose: Show progress

    Returns:
        dict with conversion stats (num_blocks, num_bands, etc.)
    """
    configure_verbose(verbose)

    progress(f"Converting {input_geotiff} to raquet format...")

    table = geotiff_to_raquet_table(
        input_geotiff,
        block_size=block_size,
        compression=compression,
        target_resolution=target_resolution,
        include_overviews=include_overviews,
        skip_empty_blocks=skip_empty_blocks,
        calculate_stats=calculate_stats,
        verbose=verbose,
    )

    # Write to parquet
    pq.write_table(
        table,
        output_parquet,
        compression=parquet_compression.lower() if parquet_compression else None,
    )

    # Get stats from metadata
    metadata = read_raquet_metadata(output_parquet)

    success(f"Written to {output_parquet}")

    return {
        "num_blocks": metadata.num_blocks if metadata else len(table) - 1,
        "num_bands": len(metadata.bands) if metadata else 0,
        "num_pixels": metadata.num_pixels if metadata else 0,
        "block_size": block_size,
        "compression": compression,
    }


# === Raquet to GeoTIFF Export ===


def _decompress_block_data(
    data: bytes,
    dtype: np.dtype,
    shape: tuple[int, int],
    compression: str | None,
) -> np.ndarray:
    """
    Decompress block pixel data back to numpy array.

    Args:
        data: Binary data (possibly gzip compressed)
        dtype: Numpy dtype
        shape: (height, width) tuple
        compression: "gzip" or None

    Returns:
        Numpy array
    """
    if compression == "gzip":
        raw_bytes = gzip.decompress(data)
    else:
        raw_bytes = data

    return np.frombuffer(raw_bytes, dtype=dtype).reshape(shape)


def raquet_to_geotiff(
    input_parquet: str,
    output_geotiff: str,
    *,
    bands: list[str] | None = None,
    resolution: int | None = None,
    verbose: bool = False,
) -> dict[str, Any]:
    """
    Export a raquet parquet file to GeoTIFF format.

    Args:
        input_parquet: Path to raquet parquet file
        output_geotiff: Path for output GeoTIFF
        bands: Band names to export (all if None)
        resolution: Resolution level (max if None)
        verbose: Show progress

    Returns:
        dict with export stats
    """
    import rasterio
    from rasterio.crs import CRS
    from rasterio.transform import from_bounds

    configure_verbose(verbose)

    if not is_raquet_file(input_parquet):
        raise ValueError(f"File is not a valid raquet file: {input_parquet}")

    metadata = read_raquet_metadata(input_parquet)
    if metadata is None:
        raise ValueError(f"Could not read raquet metadata from: {input_parquet}")

    progress("Exporting raquet to GeoTIFF...")

    # Determine resolution to use
    target_res = resolution if resolution is not None else metadata.maxresolution

    # Determine bands to export
    if bands is None:
        band_names = [b["name"] for b in metadata.bands]
    else:
        band_names = bands

    # Read the raquet table
    # Filter to only the resolution we want
    table = pq.read_table(input_parquet, columns=["block", "metadata"] + band_names)

    # Filter out metadata row and get blocks at target resolution
    block_data = []
    for i in range(len(table)):
        block_id = table.column("block")[i].as_py()
        if block_id == 0:
            continue

        # Get tile from quadbin
        x, y, z = quadbin.cell_to_tile(block_id)
        if z != target_res:
            continue

        row_data = {"tile": mercantile.Tile(x, y, z), "bands": {}}
        for band_name in band_names:
            row_data["bands"][band_name] = table.column(band_name)[i].as_py()
        block_data.append(row_data)

    if not block_data:
        raise ValueError(f"No blocks found at resolution {target_res}")

    debug(f"Found {len(block_data)} blocks at resolution {target_res}")

    # Calculate output dimensions
    tiles = [d["tile"] for d in block_data]
    min_x = min(t.x for t in tiles)
    max_x = max(t.x for t in tiles)
    min_y = min(t.y for t in tiles)
    max_y = max(t.y for t in tiles)

    num_tiles_x = max_x - min_x + 1
    num_tiles_y = max_y - min_y + 1

    output_width = num_tiles_x * metadata.block_width
    output_height = num_tiles_y * metadata.block_height

    # Calculate bounds from tiles
    ul_bounds = mercantile.xy_bounds(mercantile.Tile(min_x, min_y, target_res))
    lr_bounds = mercantile.xy_bounds(mercantile.Tile(max_x, max_y, target_res))
    output_bounds = (ul_bounds.left, lr_bounds.bottom, lr_bounds.right, ul_bounds.top)

    # Get dtype from first band
    band_info = next((b for b in metadata.bands if b["name"] == band_names[0]), None)
    if band_info is None:
        raise ValueError(f"Band {band_names[0]} not found in metadata")

    dtype_str = band_info["type"]
    dtype = np.dtype(dtype_str)

    # Create output raster
    transform = from_bounds(*output_bounds, output_width, output_height)

    with rasterio.open(
        output_geotiff,
        "w",
        driver="GTiff",
        height=output_height,
        width=output_width,
        count=len(band_names),
        dtype=dtype,
        crs=CRS.from_epsg(3857),
        transform=transform,
        nodata=metadata.nodata,
        compress="DEFLATE",
        tiled=True,
        blockxsize=metadata.block_width,
        blockysize=metadata.block_height,
    ) as dst:
        # Write each block
        for row_data in block_data:
            tile = row_data["tile"]

            # Calculate pixel offset
            x_offset = (tile.x - min_x) * metadata.block_width
            y_offset = (tile.y - min_y) * metadata.block_height

            for band_idx, band_name in enumerate(band_names):
                band_bytes = row_data["bands"][band_name]
                if band_bytes is None:
                    continue

                # Decompress
                block_array = _decompress_block_data(
                    band_bytes,
                    dtype,
                    (metadata.block_height, metadata.block_width),
                    metadata.compression,
                )

                # Write to raster
                window = rasterio.windows.Window(
                    x_offset, y_offset, metadata.block_width, metadata.block_height
                )
                dst.write(block_array, band_idx + 1, window=window)

    success(f"Exported to {output_geotiff}")

    return {
        "width": output_width,
        "height": output_height,
        "num_bands": len(band_names),
        "resolution": target_res,
    }


# === ArcGIS ImageServer to Raquet Conversion ===


@dataclass
class ImageServerMetadata:
    """Metadata about an ArcGIS ImageServer service."""

    name: str
    description: str
    extent: dict
    spatial_reference: dict
    pixel_type: str
    band_count: int
    min_values: list[float] | None
    max_values: list[float] | None
    mean_values: list[float] | None
    stddev_values: list[float] | None
    nodata: float | int | None
    pixel_size_x: float
    pixel_size_y: float
    rows: int
    columns: int


def _get_http_client():
    """Get HTTP client for making requests."""
    try:
        import httpx

        return httpx.Client(timeout=120.0, follow_redirects=True)
    except ImportError as e:
        raise ValueError(
            "httpx is required for ImageServer conversion. Install with: pip install httpx"
        ) from e


def _make_imageserver_request(
    url: str,
    params: dict | None = None,
    token: str | None = None,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    return_bytes: bool = False,
) -> dict | bytes:
    """Make HTTP request to ImageServer with retry logic."""
    import httpx

    if params is None:
        params = {}

    if token:
        params["token"] = token

    last_exception = None

    for attempt in range(max_retries):
        try:
            with _get_http_client() as client:
                response = client.get(url, params=params)
                response.raise_for_status()

                if return_bytes:
                    return response.content
                return response.json()
        except httpx.TimeoutException as e:
            last_exception = e
            if attempt < max_retries - 1:
                import time

                time.sleep(retry_delay * (attempt + 1))
        except httpx.NetworkError as e:
            last_exception = e
            if attempt < max_retries - 1:
                import time

                time.sleep(retry_delay * (attempt + 1))
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status == 429 or (500 <= status < 600):
                last_exception = e
                if attempt < max_retries - 1:
                    import time

                    retry_after = e.response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        delay = float(retry_after)
                    else:
                        delay = retry_delay * (attempt + 1)
                    time.sleep(delay)
                    continue
            elif status == 401:
                raise ValueError(
                    "Authentication required. Use --token or --username/--password."
                ) from None
            elif status == 403:
                raise ValueError(
                    "Access denied. Check your credentials and service permissions."
                ) from None
            elif status == 404:
                raise ValueError(f"Service not found (404). Check the URL: {url}") from None
            raise ValueError(f"HTTP error {status}: {e}") from e

    raise ValueError(f"Request failed after {max_retries} attempts: {last_exception}")


def get_imageserver_metadata(
    service_url: str,
    token: str | None = None,
    verbose: bool = False,
) -> ImageServerMetadata:
    """
    Fetch metadata from an ArcGIS ImageServer service.

    Args:
        service_url: ImageServer REST URL (e.g., .../ImageServer)
        token: Optional authentication token
        verbose: Show debug output

    Returns:
        ImageServerMetadata with service information
    """
    configure_verbose(verbose)

    # Clean URL
    service_url = service_url.rstrip("/")

    debug(f"Fetching ImageServer metadata from {service_url}")

    # Fetch service info
    params = {"f": "json"}
    data = _make_imageserver_request(service_url, params, token)

    if "error" in data:
        error = data["error"]
        raise ValueError(f"ImageServer error: {error.get('message', 'Unknown error')}")

    # Parse extent
    extent = data.get("extent", {})
    spatial_ref = data.get("spatialReference", extent.get("spatialReference", {}))

    # Parse pixel type to numpy-compatible type
    pixel_type = data.get("pixelType", "U8")
    pixel_type_map = {
        "U1": "uint8",
        "U2": "uint8",
        "U4": "uint8",
        "U8": "uint8",
        "S8": "int8",
        "U16": "uint16",
        "S16": "int16",
        "U32": "uint32",
        "S32": "int32",
        "F32": "float32",
        "F64": "float64",
    }
    numpy_type = pixel_type_map.get(pixel_type, "float32")

    # Get band statistics if available
    min_vals = data.get("minValues")
    max_vals = data.get("maxValues")
    mean_vals = data.get("meanValues")
    stddev_vals = data.get("stdDevValues")

    # Get pixel size
    pixel_size_x = data.get("pixelSizeX", 1.0)
    pixel_size_y = data.get("pixelSizeY", 1.0)

    # Get nodata value
    nodata = None
    nodata_values = data.get("noDataValues")
    if nodata_values and len(nodata_values) > 0:
        nodata = nodata_values[0]

    # Get rows/columns or calculate from extent and pixel size
    rows = data.get("rows")
    columns = data.get("columns")

    if not rows or not columns:
        # Calculate from extent and pixel size
        xmin = extent.get("xmin", 0)
        xmax = extent.get("xmax", 0)
        ymin = extent.get("ymin", 0)
        ymax = extent.get("ymax", 0)

        if pixel_size_x > 0 and pixel_size_y > 0:
            columns = int((xmax - xmin) / pixel_size_x)
            rows = int((ymax - ymin) / pixel_size_y)
        else:
            columns = 0
            rows = 0

    return ImageServerMetadata(
        name=data.get("name", "Unknown"),
        description=data.get("description", ""),
        extent=extent,
        spatial_reference=spatial_ref,
        pixel_type=numpy_type,
        band_count=data.get("bandCount", 1),
        min_values=min_vals,
        max_values=max_vals,
        mean_values=mean_vals,
        stddev_values=stddev_vals,
        nodata=nodata,
        pixel_size_x=pixel_size_x,
        pixel_size_y=pixel_size_y,
        rows=rows,
        columns=columns,
    )


def _transform_bounds_with_crs(
    bounds: tuple[float, float, float, float],
    src_crs: str,
    dst_crs: str,
) -> tuple[float, float, float, float]:
    """Transform bounds from source to destination CRS (accepts any CRS string)."""
    from pyproj import Transformer

    transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)

    xmin, ymin = transformer.transform(bounds[0], bounds[1])
    xmax, ymax = transformer.transform(bounds[2], bounds[3])

    return (xmin, ymin, xmax, ymax)


def _transform_bounds(
    bounds: tuple[float, float, float, float],
    src_crs: int,
    dst_crs: int,
) -> tuple[float, float, float, float]:
    """Transform bounds from source to destination CRS (EPSG codes)."""
    return _transform_bounds_with_crs(bounds, f"EPSG:{src_crs}", f"EPSG:{dst_crs}")


def _get_crs_from_spatial_reference(spatial_ref: dict) -> str:
    """Extract CRS string from ArcGIS spatial reference (EPSG or WKT)."""
    # Check for wkid (well-known ID)
    wkid = spatial_ref.get("wkid") or spatial_ref.get("latestWkid")

    if wkid:
        # Handle special cases (Web Mercator variants)
        wkid_to_epsg = {102100: 3857, 102113: 3785}
        epsg = wkid_to_epsg.get(wkid, wkid)
        return f"EPSG:{epsg}"

    # Check for WKT
    wkt = spatial_ref.get("wkt") or spatial_ref.get("wkt2")
    if wkt:
        return wkt

    # Default to WGS84 if no spatial reference
    return "EPSG:4326"


def _get_epsg_from_spatial_reference(spatial_ref: dict) -> int:
    """Extract EPSG code from ArcGIS spatial reference."""
    # Check for wkid (well-known ID)
    wkid = spatial_ref.get("wkid") or spatial_ref.get("latestWkid")

    if wkid:
        # Handle special cases (Web Mercator variants)
        wkid_to_epsg = {102100: 3857, 102113: 3785}
        return wkid_to_epsg.get(wkid, wkid)

    # Default to WGS84 if no spatial reference
    return 4326


def fetch_imageserver_tile(
    service_url: str,
    bounds: tuple[float, float, float, float],
    size: int,
    token: str | None = None,
    bands: list[int] | None = None,
) -> np.ndarray | None:
    """
    Fetch a single tile from an ArcGIS ImageServer.

    Args:
        service_url: ImageServer REST URL
        bounds: Bounding box in Web Mercator (xmin, ymin, xmax, ymax)
        size: Output size in pixels (width and height)
        token: Optional authentication token
        bands: Optional list of band indices to fetch (1-based)

    Returns:
        Numpy array of shape (bands, height, width), or None if empty
    """
    import io

    import rasterio

    export_url = f"{service_url}/exportImage"

    params = {
        "bbox": f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}",
        "bboxSR": "3857",
        "imageSR": "3857",
        "size": f"{size},{size}",
        "format": "tiff",
        "f": "image",
        "interpolation": "RSP_NearestNeighbor",
    }

    if bands:
        params["bandIds"] = ",".join(str(b) for b in bands)

    # Fetch tile as TIFF bytes
    image_bytes = _make_imageserver_request(export_url, params, token, return_bytes=True)

    if not image_bytes or len(image_bytes) < 100:
        return None

    # Read TIFF with rasterio
    try:
        with rasterio.open(io.BytesIO(image_bytes)) as src:
            data = src.read()
            return data
    except Exception:
        return None


def imageserver_to_raquet_table(
    service_url: str,
    *,
    token: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    block_size: int = DEFAULT_BLOCK_SIZE,
    compression: str | None = "gzip",
    target_resolution: int | None = None,
    skip_empty_blocks: bool = True,
    calculate_stats: bool = True,
    verbose: bool = False,
) -> pa.Table:
    """
    Convert an ArcGIS ImageServer to raquet PyArrow Table.

    This fetches raster tiles from the ImageServer and converts them to
    the raquet format with QUADBIN spatial indexing.

    Args:
        service_url: ImageServer REST URL (e.g., .../ImageServer)
        token: Optional authentication token
        bbox: Optional bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        block_size: Tile size in pixels (256 or 512, default 256)
        compression: Block compression ("gzip" or None)
        target_resolution: Target QUADBIN pixel resolution (auto if None)
        skip_empty_blocks: Skip blocks with all nodata values
        calculate_stats: Calculate per-band statistics
        verbose: Show progress

    Returns:
        PyArrow Table with raquet structure
    """
    configure_verbose(verbose)

    if block_size % 16 != 0:
        raise ValueError("Block size must be divisible by 16")

    progress(f"Fetching metadata from {service_url}...")

    # Get service metadata
    metadata = get_imageserver_metadata(service_url, token, verbose)

    debug(
        f"ImageServer: {metadata.name}, {metadata.columns}x{metadata.rows}, "
        f"{metadata.band_count} bands, type: {metadata.pixel_type}"
    )

    # Get source CRS and bounds
    src_crs = _get_crs_from_spatial_reference(metadata.spatial_reference)
    src_extent = metadata.extent

    src_bounds = (
        src_extent.get("xmin", 0),
        src_extent.get("ymin", 0),
        src_extent.get("xmax", 0),
        src_extent.get("ymax", 0),
    )

    debug(f"Source CRS: {src_crs[:50]}..." if len(src_crs) > 50 else f"Source CRS: {src_crs}")
    debug(f"Source bounds: {src_bounds}")

    # Apply bbox filter if provided (bbox is in WGS84)
    if bbox:
        # Transform bbox from WGS84 to source CRS
        bbox_src = _transform_bounds_with_crs(bbox, "EPSG:4326", src_crs)
        # Intersect with service bounds
        src_bounds = (
            max(src_bounds[0], bbox_src[0]),
            max(src_bounds[1], bbox_src[1]),
            min(src_bounds[2], bbox_src[2]),
            min(src_bounds[3], bbox_src[3]),
        )

        if src_bounds[0] >= src_bounds[2] or src_bounds[1] >= src_bounds[3]:
            raise ValueError("Bounding box does not intersect with ImageServer extent")

    # Transform bounds to Web Mercator
    bounds_3857 = _transform_bounds_with_crs(src_bounds, src_crs, "EPSG:3857")

    debug(f"Web Mercator bounds: {bounds_3857}")

    # Calculate dimensions in Web Mercator
    width_m = bounds_3857[2] - bounds_3857[0]
    height_m = bounds_3857[3] - bounds_3857[1]

    # Estimate pixel resolution from service native resolution
    # The native resolution is in the source CRS units
    # For projected CRS (UTM, etc.) it's already in meters
    # For geographic CRS, we need to convert degrees to meters
    native_res_m = max(metadata.pixel_size_x, metadata.pixel_size_y)

    # Check if source CRS is geographic (degrees)
    # PROJCS = projected (already in linear units like meters)
    # GEOGCS at start = geographic (in degrees)
    # EPSG:4326 = WGS84 geographic
    src_crs_upper = src_crs.upper()
    is_projected = (
        src_crs_upper.startswith("PROJCS")
        or src_crs.startswith("EPSG:")
        and not src_crs.startswith("EPSG:4326")
    )
    is_geographic = not is_projected and (
        src_crs.startswith("EPSG:4326") or src_crs_upper.startswith("GEOGCS")
    )

    debug(f"CRS type: {'projected' if is_projected else 'geographic'}")

    if is_geographic:
        # Rough approximation: for geographic coords, convert degrees to meters at center
        center_lat = (src_bounds[1] + src_bounds[3]) / 2
        # 1 degree ≈ 111,320 meters at equator, less at higher latitudes
        native_res_m = native_res_m * 111320 * math.cos(math.radians(center_lat))
        debug(f"Converted geographic resolution: {native_res_m:.2f}m")

    # Estimate dimensions
    est_width = int(width_m / native_res_m) if native_res_m > 0 else metadata.columns
    est_height = int(height_m / native_res_m) if native_res_m > 0 else metadata.rows

    # Calculate target resolution if not specified
    if target_resolution is None:
        target_resolution = _calculate_target_resolution(
            bounds_3857, est_width, est_height, block_size
        )

    block_zoom = int(math.log2(block_size))
    tile_zoom = target_resolution - block_zoom

    # Clamp zoom level
    tile_zoom = max(0, min(22, tile_zoom))
    target_resolution = tile_zoom + block_zoom

    debug(f"Target resolution: {target_resolution}, tile zoom: {tile_zoom}")

    # Convert bounds to WGS84 for mercantile
    from pyproj import Transformer

    transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    lon_min, lat_min = transformer.transform(bounds_3857[0], bounds_3857[1])
    lon_max, lat_max = transformer.transform(bounds_3857[2], bounds_3857[3])

    # Clamp to valid ranges
    lon_min = max(-180, min(180, lon_min))
    lon_max = max(-180, min(180, lon_max))
    lat_min = max(-85.051129, min(85.051129, lat_min))
    lat_max = max(-85.051129, min(85.051129, lat_max))

    tiles = list(mercantile.tiles(lon_min, lat_min, lon_max, lat_max, zooms=tile_zoom))

    progress(f"Fetching {len(tiles)} tiles at zoom {tile_zoom}...")

    # Prepare band names
    band_names = [f"band_{i + 1}" for i in range(metadata.band_count)]
    dtype = np.dtype(metadata.pixel_type)
    nodata = metadata.nodata

    # Collect rows and stats
    rows = []
    band_stats = [[] for _ in range(metadata.band_count)]

    # Track bounds for metadata
    xmin_tile, ymin_tile, xmax_tile, ymax_tile = (
        float("inf"),
        float("inf"),
        float("-inf"),
        float("-inf"),
    )

    tiles_fetched = 0
    for tile in tiles:
        # Get tile bounds in Web Mercator
        tile_bounds = mercantile.xy_bounds(tile)
        tile_bounds_tuple = (
            tile_bounds.left,
            tile_bounds.bottom,
            tile_bounds.right,
            tile_bounds.top,
        )

        # Fetch tile from ImageServer
        tile_data = fetch_imageserver_tile(
            service_url,
            tile_bounds_tuple,
            block_size,
            token,
        )

        if tile_data is None:
            continue

        tiles_fetched += 1
        if tiles_fetched % 10 == 0:
            progress(f"Fetched {tiles_fetched}/{len(tiles)} tiles...")

        # Process each band
        block_data = {}
        all_empty = True

        for band_idx in range(min(tile_data.shape[0], metadata.band_count)):
            band_array = tile_data[band_idx]

            # Ensure correct size (pad or crop if needed)
            if band_array.shape != (block_size, block_size):
                resized = np.full((block_size, block_size), nodata if nodata else 0, dtype=dtype)
                h, w = min(band_array.shape[0], block_size), min(band_array.shape[1], block_size)
                resized[:h, :w] = band_array[:h, :w]
                band_array = resized

            # Check if empty
            if skip_empty_blocks and _is_block_empty(band_array, nodata):
                continue

            all_empty = False

            # Compress block data
            compressed = _compress_block_data(band_array.astype(dtype), compression)
            block_data[band_names[band_idx]] = compressed

            # Collect stats
            if calculate_stats:
                stats = _calculate_band_statistics(band_array, nodata)
                if stats.get("count", 0) > 0:
                    band_stats[band_idx].append(stats)

        if all_empty and skip_empty_blocks:
            continue

        # Get QUADBIN cell ID
        quadbin_id = quadbin.tile_to_cell((tile.x, tile.y, tile.z))

        # Add row
        row = {
            "block": quadbin_id,
            "metadata": None,
            **{name: block_data.get(name) for name in band_names},
        }
        rows.append(row)

        # Update tile bounds
        xmin_tile = min(xmin_tile, tile.x)
        ymin_tile = min(ymin_tile, tile.y)
        xmax_tile = max(xmax_tile, tile.x)
        ymax_tile = max(ymax_tile, tile.y)

    progress(f"Processed {len(rows)} blocks from {tiles_fetched} tiles")

    if not rows:
        warn("No valid tiles were fetched from the ImageServer")

    # Aggregate band statistics
    aggregated_stats = []
    for band_idx in range(metadata.band_count):
        if band_stats[band_idx]:
            total_count = sum(s["count"] for s in band_stats[band_idx])
            if total_count > 0:
                aggregated_stats.append(
                    {
                        "min": min(s["min"] for s in band_stats[band_idx]),
                        "max": max(s["max"] for s in band_stats[band_idx]),
                        "mean": sum(s["mean"] * s["count"] for s in band_stats[band_idx])
                        / total_count,
                        "stddev": math.sqrt(
                            sum(s["stddev"] ** 2 * s["count"] for s in band_stats[band_idx])
                            / total_count
                        ),
                        "count": total_count,
                        "approximated_stats": True,
                    }
                )
            else:
                aggregated_stats.append(None)
        else:
            aggregated_stats.append(None)

    # Calculate final bounds
    if xmin_tile == float("inf"):
        final_bounds = [lon_min, lat_min, lon_max, lat_max]
    else:
        ul_tile = mercantile.Tile(x=int(xmin_tile), y=int(ymin_tile), z=tile_zoom)
        lr_tile = mercantile.Tile(x=int(xmax_tile), y=int(ymax_tile), z=tile_zoom)
        ul_bounds = mercantile.bounds(ul_tile)
        lr_bounds = mercantile.bounds(lr_tile)
        final_bounds = [ul_bounds.west, lr_bounds.south, lr_bounds.east, ul_bounds.north]

    # Create metadata
    metadata_dict = {
        "version": RAQUET_VERSION,
        "compression": compression,
        "block_resolution": tile_zoom,
        "minresolution": tile_zoom,
        "maxresolution": tile_zoom,
        "nodata": nodata,
        "bounds": final_bounds,
        "center": [
            (final_bounds[0] + final_bounds[2]) / 2,
            (final_bounds[1] + final_bounds[3]) / 2,
            tile_zoom,
        ],
        "width": est_width,
        "height": est_height,
        "block_width": block_size,
        "block_height": block_size,
        "num_blocks": len(rows),
        "num_pixels": len(rows) * block_size * block_size,
        "pixel_resolution": target_resolution,
        "source": {
            "type": "ArcGIS ImageServer",
            "url": service_url,
            "name": metadata.name,
        },
        "bands": [
            {
                "type": metadata.pixel_type,
                "name": band_names[i],
                "colorinterp": None,
                "nodata": str(nodata) if nodata is not None else None,
                "stats": aggregated_stats[i] if i < len(aggregated_stats) else None,
                "colortable": None,
            }
            for i in range(metadata.band_count)
        ],
    }

    # Add metadata row
    metadata_row = {
        "block": METADATA_BLOCK_ID,
        "metadata": json.dumps(metadata_dict),
        **dict.fromkeys(band_names),
    }
    rows.insert(0, metadata_row)

    # Create schema
    schema = pa.schema(
        [
            ("block", pa.uint64()),
            ("metadata", pa.string()),
            *[(name, pa.binary()) for name in band_names],
        ]
    )

    # Build table
    table_dict = {key: [row.get(key) for row in rows] for key in schema.names}
    table = pa.Table.from_pydict(table_dict, schema=schema)

    return table


def imageserver_to_raquet(
    service_url: str,
    output_parquet: str,
    *,
    token: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    block_size: int = DEFAULT_BLOCK_SIZE,
    compression: str | None = "gzip",
    parquet_compression: str = "ZSTD",
    target_resolution: int | None = None,
    skip_empty_blocks: bool = True,
    calculate_stats: bool = True,
    verbose: bool = False,
) -> dict[str, Any]:
    """
    Convert an ArcGIS ImageServer to raquet parquet format.

    Args:
        service_url: ImageServer REST URL (e.g., .../ImageServer)
        output_parquet: Path for output parquet file
        token: Optional authentication token
        bbox: Optional bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        block_size: Tile size in pixels (256 or 512, default 256)
        compression: Block compression ("gzip" or None)
        parquet_compression: Parquet file compression (ZSTD, GZIP, etc.)
        target_resolution: Target QUADBIN pixel resolution (auto if None)
        skip_empty_blocks: Skip nodata-only blocks
        calculate_stats: Calculate band statistics
        verbose: Show progress

    Returns:
        dict with conversion stats (num_blocks, num_bands, etc.)
    """
    configure_verbose(verbose)

    progress("Converting ImageServer to raquet format...")

    table = imageserver_to_raquet_table(
        service_url,
        token=token,
        bbox=bbox,
        block_size=block_size,
        compression=compression,
        target_resolution=target_resolution,
        skip_empty_blocks=skip_empty_blocks,
        calculate_stats=calculate_stats,
        verbose=verbose,
    )

    # Write to parquet
    pq.write_table(
        table,
        output_parquet,
        compression=parquet_compression.lower() if parquet_compression else None,
    )

    # Get stats from metadata
    raquet_meta = read_raquet_metadata(output_parquet)

    success(f"Written to {output_parquet}")

    return {
        "num_blocks": raquet_meta.num_blocks if raquet_meta else len(table) - 1,
        "num_bands": len(raquet_meta.bands) if raquet_meta else 0,
        "num_pixels": raquet_meta.num_pixels if raquet_meta else 0,
        "block_size": block_size,
        "compression": compression,
    }
