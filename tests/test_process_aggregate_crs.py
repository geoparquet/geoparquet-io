"""CRS-awareness for `gpio process aggregate` (#525 follow-up).

Grid keying (a5/h3) needs lon/lat, and the admin join compares against OGC:CRS84
boundaries, so a non-CRS84 input must be reprojected first. These exercise the
projected (EPSG:5070) test fixture.
"""

import duckdb
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.geometry_detection import primary_geometry_column
from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5
from geoparquet_io.core.process.aggregate.by_h3 import aggregate_by_h3


def _reprojected_cells(parquet_file, key_sql_template, res):
    """Cells computed from the input geometries reprojected EPSG:5070 -> CRS84."""
    geom = primary_geometry_column(parquet_file)
    con = duckdb.connect()
    con.execute(
        "INSTALL spatial; LOAD spatial; INSTALL a5 FROM community; LOAD a5; "
        "INSTALL h3 FROM community; LOAD h3; SET geometry_always_xy=true"
    )
    rows = con.execute(
        f"""
        SELECT DISTINCT {key_sql_template.format(x="ST_X(c)", y="ST_Y(c)", res=res)}
        FROM (
            SELECT ST_Centroid(ST_Transform({geom}, 'EPSG:5070', 'OGC:CRS84')) AS c
            FROM read_parquet('{parquet_file}')
        )
        WHERE c IS NOT NULL
        """
    ).fetchall()
    con.close()
    return {r[0] for r in rows}


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_reprojects_projected_input(fields_5070_file, tmp_path):
    out = tmp_path / "cells.parquet"
    aggregate_by_a5(fields_5070_file, str(out), resolution=6)
    cells = set(pq.read_table(out).column("a5_cell").to_pylist())
    assert None not in cells  # every feature keyed; none scattered to unassigned
    # a5_lonlat_to_cell takes (lon, lat).
    expected = _reprojected_cells(fields_5070_file, "a5_lonlat_to_cell({x}, {y}, {res})", 6)
    assert cells == expected


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_h3_reprojects_projected_input(fields_5070_file, tmp_path):
    out = tmp_path / "cells.parquet"
    aggregate_by_h3(fields_5070_file, str(out), resolution=6)
    cells = set(pq.read_table(out).column("h3_cell").to_pylist())
    assert None not in cells
    # h3_latlng_to_cell_string takes (lat, lng).
    expected = _reprojected_cells(fields_5070_file, "h3_latlng_to_cell_string({y}, {x}, {res})", 6)
    assert cells == expected
