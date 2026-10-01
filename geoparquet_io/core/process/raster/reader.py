"""Read one raster band for the contourrs-backed commands.

rasterio is the only raster reader gpio touches and it stays behind the
``raster`` extra's lazy shim (ADR-0007). What comes back is exactly what
contourrs consumes: the 2D array, a True-is-valid mask (only when the raster
actually masks something), the effective nodata value, the affine transform
as a 6-tuple, and the CRS as PROJJSON for the output's ``geo`` block.

Docs: docs/guide/process-raster.md
"""

from __future__ import annotations

from dataclasses import dataclass

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.logging_config import debug
from geoparquet_io.core.optional_deps import require_rasterio


@dataclass
class RasterBand:
    """One band's data and georeferencing, ready for contourrs."""

    array: object  # 2D numpy array
    mask: object | None  # boolean array, True = valid, or None
    nodata: float | None
    transform: tuple[float, float, float, float, float, float]
    crs: dict | None  # PROJJSON


def read_band(
    raster_path: str,
    *,
    band: int = 1,
    nodata: float | None = None,
    use_mask: bool = True,
) -> RasterBand:
    """Read ``band`` of ``raster_path``; ``nodata`` overrides the file's own."""
    rasterio = require_rasterio()
    with rasterio.open(raster_path) as src:
        if band < 1 or band > src.count:
            raise InvalidParameterError(
                "band", f"raster has {src.count} band(s), asked for band {band}"
            )
        array = src.read(band)
        effective_nodata = nodata if nodata is not None else src.nodata
        mask = None
        if use_mask:
            valid = src.read_masks(band) != 0
            if not valid.all():
                mask = valid
        crs = None
        if src.crs is not None:
            from pyproj import CRS

            crs = CRS.from_wkt(src.crs.to_wkt()).to_json_dict()
        transform = tuple(src.transform)[:6]
        debug(
            f"Read band {band} of {raster_path}: {array.shape}, "
            f"nodata={effective_nodata}, masked={mask is not None}"
        )
        return RasterBand(array, mask, effective_nodata, transform, crs)
