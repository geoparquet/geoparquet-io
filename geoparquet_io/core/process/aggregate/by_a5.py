#!/usr/bin/env python3
"""A5-cell aggregation for `gpio process aggregate a5`.

Thin scheme definition over the shared engine in ``grid_common``.
"""

from __future__ import annotations

import pyarrow as pa

from geoparquet_io.core.constants import DEFAULT_A5_COLUMN_NAME
from geoparquet_io.core.process.aggregate.grid_common import (
    GridScheme,
    aggregate_grid_file,
    aggregate_grid_table,
)

A5_SCHEME = GridScheme(
    name="a5",
    extension="a5",
    min_resolution=0,
    max_resolution=30,
    default_column=DEFAULT_A5_COLUMN_NAME,
    key_template="a5_lonlat_to_cell(ST_X({pt}), ST_Y({pt}), {res})",
    # a5_cell_to_boundary returns an open DOUBLE[2][] ring that stays contiguous
    # but lets longitudes run outside [-180, 180] (267.0 is a real value), so the
    # shared builder still has to bring the polygon back into range.
    boundary_template=(
        "ST_MakePolygon(ST_MakeLine(list_transform("
        "list_append(a5_cell_to_boundary({cell}), a5_cell_to_boundary({cell})[1]), "
        "p -> ST_Point(p[1], p[2]))))"
    ),
    latlng_template="a5_cell_to_lonlat({cell})",
    # a5_cell_to_lonlat returns [lon, lat].
    centroid_wkb_template="ST_AsWKB(ST_Point({ll}[1], {ll}[2]))",
)


def aggregate_by_a5(
    input_parquet: str,
    output_parquet: str,
    resolution: int | None = None,
    auto: bool = False,
    target_per_cell: int = 10000,
    max_cells: int = 500000,
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
    a5_column_name: str = DEFAULT_A5_COLUMN_NAME,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    geoparquet_version: str | None = None,
    verbose: bool = False,
    show_sql: bool = False,
    where: str | None = None,
    metric_nodata: str | None = None,
    bucket_point: str = "geometry",
    bbox_column: str | None = None,
) -> None:
    """Aggregate a GeoParquet file into A5 cells. Writes the output file."""
    aggregate_grid_file(
        A5_SCHEME,
        input_parquet,
        output_parquet,
        resolution=resolution,
        auto=auto,
        target_per_cell=target_per_cell,
        max_cells=max_cells,
        metric=metric,
        breakdown=breakdown,
        breakdown_limit=breakdown_limit,
        out_geometry=out_geometry,
        cell_column=a5_column_name,
        compression=compression,
        compression_level=compression_level,
        geoparquet_version=geoparquet_version,
        verbose=verbose,
        show_sql=show_sql,
        where=where,
        metric_nodata=metric_nodata,
        bucket_point=bucket_point,
        bbox_column=bbox_column,
    )


def aggregate_a5_table(
    table,
    resolution: int,
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
    a5_column_name: str = DEFAULT_A5_COLUMN_NAME,
    geometry_column: str | None = None,
    where: str | None = None,
    metric_nodata: str | None = None,
    bucket_point: str = "geometry",
    bbox_column: str | None = None,
) -> pa.Table:
    """Aggregate an in-memory Arrow table by a5 cell. Returns a new Arrow table."""
    return aggregate_grid_table(
        A5_SCHEME,
        table,
        resolution=resolution,
        metric=metric,
        breakdown=breakdown,
        breakdown_limit=breakdown_limit,
        out_geometry=out_geometry,
        cell_column=a5_column_name,
        geometry_column=geometry_column,
        where=where,
        metric_nodata=metric_nodata,
        bucket_point=bucket_point,
        bbox_column=bbox_column,
    )
