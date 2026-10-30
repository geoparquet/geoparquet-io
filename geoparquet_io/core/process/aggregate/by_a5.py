#!/usr/bin/env python3
"""A5-cell aggregation for `gpio process aggregate a5`.

Thin scheme definition over the shared engine in ``grid_common``.
"""

from __future__ import annotations

import pyarrow as pa

from geoparquet_io.core.add.a5 import SPEC
from geoparquet_io.core.compression import resolve_process_compression_level
from geoparquet_io.core.constants import DEFAULT_A5_COLUMN_NAME
from geoparquet_io.core.process.aggregate.grid_common import (
    GridScheme,
    aggregate_grid_file,
    aggregate_grid_table,
)

assert SPEC.extension is not None

A5_SCHEME = GridScheme(
    name=SPEC.name,
    extension=SPEC.extension,
    min_resolution=SPEC.min_resolution,
    max_resolution=SPEC.max_resolution,
    default_column=SPEC.column_name,
    key_template=SPEC.expression("ST_X({pt})", "ST_Y({pt})", "{res}"),
    # a5_cell_to_boundary returns an open DOUBLE[2][] ring that stays contiguous
    # but lets longitudes run outside [-180, 180] (267.0 is a real value), so the
    # shared builder still has to bring the polygon back into range.
    boundary_template=(
        "ST_MakePolygon(ST_MakeLine(list_transform("
        "list_append(a5_cell_to_boundary({cell}), a5_cell_to_boundary({cell})[1]), "
        "lambda p: ST_Point(p[1], p[2]))))"
    ),
    latlng_template="a5_cell_to_lonlat({cell})",
    # a5_cell_to_lonlat returns [lon, lat].
    centroid_wkb_template="ST_AsWKB(ST_Point({ll}[1], {ll}[2]))",
    # A5 is equal-area by construction: every cell at a resolution covers the
    # same number of square metres, so the denominator of `pct_cell` is one
    # constant and does not need the cell id (#1181).
    cell_area_template="a5_cell_area({res})",
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
    compression: str | None = None,
    compression_level: int | None = None,
    geoparquet_version: str | None = None,
    verbose: bool = False,
    show_sql: bool = False,
    where: str | None = None,
    metric_nodata: str | None = None,
    bucket_point: str = "geometry",
    bbox_column: str | None = None,
    breakdown_metric: str | None = None,
    memory_limit: str | None = None,
) -> None:
    """Aggregate a GeoParquet file into A5 cells. Writes the output file."""
    compression_level = resolve_process_compression_level(compression, compression_level)
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
        breakdown_metric=breakdown_metric,
        memory_limit=memory_limit,
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
    breakdown_metric: str | None = None,
    memory_limit: str | None = None,
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
        breakdown_metric=breakdown_metric,
        memory_limit=memory_limit,
    )
