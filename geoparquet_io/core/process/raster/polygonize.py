"""Polygonize a categorical raster with contourrs (``gpio process polygonize``).

Docs: docs/guide/process-raster.md
"""

from __future__ import annotations

import pyarrow as pa

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.logging_config import info
from geoparquet_io.core.optional_deps import require_contourrs
from geoparquet_io.core.process.raster.output import finalize_raster_table
from geoparquet_io.core.process.raster.reader import read_band
from geoparquet_io.core.write_funnels import write_geoparquet_table


def polygonize_array(
    array,
    *,
    mask=None,
    nodata: float | None = None,
    transform: tuple | None = None,
    crs: dict | None = None,
    values: list[float] | None = None,
    value_column: str = "value",
    connectivity: int = 4,
    verbose: bool = False,
) -> pa.Table:
    """Polygonize a 2D array's classes into a GeoParquet-ready table.

    ``values`` keeps only the listed class values (default: every class that
    is not nodata/masked). ``mask`` is True-where-valid.
    """
    if value_column == "geometry":
        raise InvalidParameterError(
            "value_column", "collides with the output's geometry column name"
        )
    contourrs = require_contourrs()
    table = contourrs.shapes_arrow(
        array, mask=mask, connectivity=connectivity, transform=transform, nodata=nodata
    )
    rename = {"value": value_column} if value_column != "value" else None
    table = finalize_raster_table(table, crs=crs, rename=rename)
    if values is not None:
        import pyarrow.compute as pc

        table = table.filter(
            pc.is_in(table[value_column], value_set=pa.array(values, pa.float64()))
        )
    if verbose:
        info(f"Polygonized {table.num_rows} features")
    return table


def polygonize_raster(
    input_raster: str,
    *,
    band: int = 1,
    values: list[float] | None = None,
    value_column: str = "value",
    nodata: float | None = None,
    use_mask: bool = True,
    connectivity: int = 4,
    verbose: bool = False,
) -> pa.Table:
    """Polygonize one raster band into a GeoParquet-ready table."""
    src = read_band(input_raster, band=band, nodata=nodata, use_mask=use_mask)
    return polygonize_array(
        src.array,
        mask=src.mask,
        nodata=src.nodata,
        transform=src.transform,
        crs=src.crs,
        values=values,
        value_column=value_column,
        connectivity=connectivity,
        verbose=verbose,
    )


def polygonize_file(
    input_raster: str,
    output_parquet: str,
    *,
    band: int = 1,
    values: list[float] | None = None,
    value_column: str = "value",
    nodata: float | None = None,
    use_mask: bool = True,
    connectivity: int = 4,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    geoparquet_version: str | None = None,
    verbose: bool = False,
) -> None:
    """Polygonize one raster band into a GeoParquet file."""
    table = polygonize_raster(
        input_raster,
        band=band,
        values=values,
        value_column=value_column,
        nodata=nodata,
        use_mask=use_mask,
        connectivity=connectivity,
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
