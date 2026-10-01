"""Contour bands from an elevation raster with contourrs (``gpio process contour``).

Docs: docs/guide/process-raster.md
"""

from __future__ import annotations

import math
from bisect import bisect_left

import pyarrow as pa

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.logging_config import info
from geoparquet_io.core.optional_deps import require_contourrs
from geoparquet_io.core.process.raster.output import finalize_raster_table
from geoparquet_io.core.process.raster.reader import read_band
from geoparquet_io.core.write_funnels import write_geoparquet_table


def resolve_levels(
    levels: list[float] | None,
    interval: float | None,
    base: float,
    band_min: float,
    band_max: float,
) -> list[float]:
    """The explicit break values, or a ``base + k*interval`` ladder that
    brackets the band's [min, max] (gdal_contour-style)."""
    if levels is not None and interval is not None:
        raise InvalidParameterError(
            "levels", "give either explicit levels or an interval, not both"
        )
    if levels is not None:
        if len(levels) < 2:
            raise InvalidParameterError(
                "levels", "at least two values are needed to form a band"
            )
        if any(b <= a for a, b in zip(levels, levels[1:])):
            raise InvalidParameterError("levels", "values must be strictly increasing")
        return [float(v) for v in levels]
    if interval is None:
        raise InvalidParameterError("levels", "one of levels or interval is required")
    if interval <= 0:
        raise InvalidParameterError("interval", "must be > 0")
    start = base + math.floor((band_min - base) / interval) * interval
    stop = base + math.ceil((band_max - base) / interval) * interval
    if stop <= start:
        stop = start + interval
    count = round((stop - start) / interval)
    return [start + k * interval for k in range(count + 1)]


def _band_max_for(levels: list[float], lower: float) -> float:
    """The upper break of the band whose lower break is ``lower``."""
    idx = bisect_left(levels, lower)
    if idx >= len(levels) - 1 or levels[idx] != lower:
        # contourrs returned a lower bound we did not ask for; nearest wins.
        idx = min(
            range(len(levels) - 1), key=lambda k: abs(levels[k] - lower)
        )
    return levels[idx + 1]


def contour_array(
    array,
    levels: list[float],
    *,
    mask=None,
    nodata: float | None = None,
    transform: tuple | None = None,
    crs: dict | None = None,
    min_column: str = "min",
    max_column: str = "max",
    verbose: bool = False,
) -> pa.Table:
    """Extract filled contour bands into a GeoParquet-ready table.

    Each output row is one band polygon attributed with the band's
    ``[min, max)`` break values.
    """
    levels = resolve_levels(levels, None, 0.0, 0.0, 0.0)  # validates shape
    contourrs = require_contourrs()
    table = contourrs.contours_arrow(
        array, thresholds=list(levels), mask=mask, transform=transform, nodata=nodata
    )
    maxs = [_band_max_for(levels, lower) for lower in table.column("value").to_pylist()]
    table = table.append_column(
        pa.field(max_column, pa.float64(), nullable=False),
        pa.array(maxs, pa.float64()),
    )
    table = finalize_raster_table(table, crs=crs, rename={"value": min_column})
    if verbose:
        info(f"Extracted {table.num_rows} contour bands from {len(levels)} levels")
    return table


def _valid_range(array, mask, nodata) -> tuple[float, float]:
    """Min/max of the band's valid pixels, for interval-mode level resolution."""
    import numpy as np

    values = np.asarray(array, dtype=np.float64)
    keep = np.isfinite(values)
    if mask is not None:
        keep &= np.asarray(mask, dtype=bool)
    if nodata is not None:
        keep &= values != float(nodata)
    if not keep.any():
        raise InvalidParameterError("input_raster", "no valid pixels in the band")
    valid = values[keep]
    return float(valid.min()), float(valid.max())


def contour_raster(
    input_raster: str,
    *,
    levels: list[float] | None = None,
    interval: float | None = None,
    base: float = 0.0,
    band: int = 1,
    nodata: float | None = None,
    use_mask: bool = True,
    min_column: str = "min",
    max_column: str = "max",
    verbose: bool = False,
) -> pa.Table:
    """Extract contour bands from one raster band into a GeoParquet-ready table."""
    src = read_band(input_raster, band=band, nodata=nodata, use_mask=use_mask)
    if levels is None:
        band_min, band_max = _valid_range(src.array, src.mask, src.nodata)
        resolved = resolve_levels(None, interval, base, band_min, band_max)
    else:
        resolved = resolve_levels(levels, interval, base, 0.0, 0.0)
    return contour_array(
        src.array,
        resolved,
        mask=src.mask,
        nodata=src.nodata,
        transform=src.transform,
        crs=src.crs,
        min_column=min_column,
        max_column=max_column,
        verbose=verbose,
    )


def contour_file(
    input_raster: str,
    output_parquet: str,
    *,
    levels: list[float] | None = None,
    interval: float | None = None,
    base: float = 0.0,
    band: int = 1,
    nodata: float | None = None,
    use_mask: bool = True,
    min_column: str = "min",
    max_column: str = "max",
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    geoparquet_version: str | None = None,
    verbose: bool = False,
) -> None:
    """Extract contour bands from one raster band into a GeoParquet file."""
    table = contour_raster(
        input_raster,
        levels=levels,
        interval=interval,
        base=base,
        band=band,
        nodata=nodata,
        use_mask=use_mask,
        min_column=min_column,
        max_column=max_column,
        verbose=verbose,
    )
    write_geoparquet_table(
        table,
        output_parquet,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        geoparquet_version=geoparquet_version,
        verbose=verbose,
    )
