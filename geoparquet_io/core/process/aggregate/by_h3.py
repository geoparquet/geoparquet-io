#!/usr/bin/env python3
"""H3-cell aggregation for `gpio process aggregate h3`.

Thin scheme definition over the shared engine in ``grid_common``.
"""

from __future__ import annotations

import pyarrow as pa

from geoparquet_io.core.constants import DEFAULT_H3_COLUMN_NAME
from geoparquet_io.core.process.aggregate.grid_common import (
    GridScheme,
    aggregate_grid_file,
    aggregate_grid_table,
)

H3_SCHEME = GridScheme(
    name="h3",
    extension="h3",
    min_resolution=0,
    max_resolution=15,
    default_column=DEFAULT_H3_COLUMN_NAME,
    # h3_latlng_to_cell_string takes (lat, lng) -> note Y before X.
    key_template="h3_latlng_to_cell_string(ST_Y({pt}), ST_X({pt}), {res})",
    # The WKB form parses straight to a GEOMETRY -- no WKT round trip. Every
    # vertex comes back wrapped into [-180, 180], so a cell straddling the
    # antimeridian arrives torn; the shared builder repairs it.
    boundary_template="ST_GeomFromWKB(h3_cell_to_boundary_wkb({cell}))",
    latlng_template="h3_cell_to_latlng({cell})",
    # h3_cell_to_latlng returns [lat, lng]; ST_Point wants (lng, lat).
    centroid_wkb_template="ST_AsWKB(ST_Point({ll}[2], {ll}[1]))",
)


def aggregate_by_h3(
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
    h3_column_name: str = DEFAULT_H3_COLUMN_NAME,
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
    """Aggregate a GeoParquet file into H3 cells. Writes the output file."""
    aggregate_grid_file(
        H3_SCHEME,
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
        cell_column=h3_column_name,
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


def aggregate_h3_table(
    table,
    resolution: int,
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
    h3_column_name: str = DEFAULT_H3_COLUMN_NAME,
    geometry_column: str | None = None,
    where: str | None = None,
    metric_nodata: str | None = None,
    bucket_point: str = "geometry",
    bbox_column: str | None = None,
) -> pa.Table:
    """Aggregate an in-memory Arrow table by h3 cell. Returns a new Arrow table."""
    return aggregate_grid_table(
        H3_SCHEME,
        table,
        resolution=resolution,
        metric=metric,
        breakdown=breakdown,
        breakdown_limit=breakdown_limit,
        out_geometry=out_geometry,
        cell_column=h3_column_name,
        geometry_column=geometry_column,
        where=where,
        metric_nodata=metric_nodata,
        bucket_point=bucket_point,
        bbox_column=bbox_column,
    )
