#!/usr/bin/env python3
"""H3-cell aggregation for `gpio process aggregate h3`.

Thin scheme definition over the shared engine in ``grid_common``.
"""

from __future__ import annotations

import pyarrow as pa

from geoparquet_io.core.compression import resolve_process_compression_level
from geoparquet_io.core.constants import DEFAULT_H3_COLUMN_NAME
from geoparquet_io.core.duckdb_utils import as_wkb_sql, geom_from_wkb_sql
from geoparquet_io.core.index_ops import get_index_spec
from geoparquet_io.core.process.aggregate.grid_common import (
    GridScheme,
    aggregate_grid_file,
    aggregate_grid_table,
)
from geoparquet_io.core.process.aggregate.options import AggregateOptions

SPEC = get_index_spec("h3")

H3_SCHEME = GridScheme(
    name=SPEC.name,
    spec=SPEC,
    default_column=SPEC.column_name,
    # h3_latlng_to_cell_string takes (lat, lng) -> note Y before X.
    key_template=SPEC.expression("ST_X({pt})", "ST_Y({pt})", "{res}"),
    # The WKB form parses straight to a GEOMETRY -- no WKT round trip. Every
    # vertex comes back wrapped into [-180, 180], so a cell straddling the
    # antimeridian arrives torn; the shared builder repairs it.
    boundary_template=geom_from_wkb_sql("h3_cell_to_boundary_wkb({cell})"),
    latlng_template="h3_cell_to_latlng({cell})",
    # h3_cell_to_latlng returns [lat, lng]; ST_Point wants (lng, lat).
    centroid_wkb_template=as_wkb_sql("ST_Point({ll}[2], {ll}[1])"),
    # H3 is *not* equal-area -- a cell near a pole is about a quarter smaller
    # than one at the equator -- so `pct_cell` divides each cell by its own
    # measured area rather than a per-resolution average (#1181).
    cell_area_template="h3_cell_area({cell}, 'm^2')",
)


def aggregate_by_h3(
    input_parquet: str,
    output_parquet: str,
    resolution: int | None = None,
    auto: bool = False,
    target_per_cell: int = 10000,
    max_cells: int = 500000,
    options: AggregateOptions | None = None,
    h3_column_name: str = DEFAULT_H3_COLUMN_NAME,
    compression: str | None = None,
    compression_level: int | None = None,
    geoparquet_version: str | None = None,
    verbose: bool = False,
    show_sql: bool = False,
) -> None:
    """Aggregate a GeoParquet file into H3 cells. Writes the output file."""
    compression_level = resolve_process_compression_level(compression, compression_level)
    aggregate_grid_file(
        H3_SCHEME,
        input_parquet,
        output_parquet,
        options=options,
        resolution=resolution,
        auto=auto,
        target_per_cell=target_per_cell,
        max_cells=max_cells,
        cell_column=h3_column_name,
        compression=compression,
        compression_level=compression_level,
        geoparquet_version=geoparquet_version,
        verbose=verbose,
        show_sql=show_sql,
    )


def aggregate_h3_table(
    table,
    resolution: int,
    options: AggregateOptions | None = None,
    h3_column_name: str = DEFAULT_H3_COLUMN_NAME,
    geometry_column: str | None = None,
) -> pa.Table:
    """Aggregate an in-memory Arrow table by h3 cell. Returns a new Arrow table."""
    return aggregate_grid_table(
        H3_SCHEME,
        table,
        resolution=resolution,
        options=options,
        cell_column=h3_column_name,
        geometry_column=geometry_column,
    )
