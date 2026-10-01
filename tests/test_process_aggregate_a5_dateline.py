"""A5 cells astride the antimeridian, at the resolutions real catalogs use (#1180).

`a5_cell_to_boundary` keeps a cell ring contiguous but lets its longitudes run
past the valid range: the Fiji cell pinned below reports vertices at -180.20,
and the Chukotka one at +180.20. Tile exporters read those as outside the world
and drop the whole cell, so dateline cells vanish from any map built on the
aggregate.

The shared seam repair in ``wrap_grid_geometry`` already cuts such a ring into a
MultiPolygon (#911). These tests hold it to the acceptance the field report
asked for, which the ring-case suite in
``test_process_aggregate_antimeridian.py`` does not cover: the *a5* grid at the
resolutions the FTW global runs use (r7 base, r5 overview parents), one pinned
cell shape, the invariance of counts and metrics across the repair, and that a
cell nowhere near the seam is handed through untouched.
"""

import pytest

from geoparquet_io.core.duckdb_utils import get_duckdb_connection, load_community_extension
from geoparquet_io.core.process.aggregate.by_a5 import A5_SCHEME, aggregate_by_a5
from geoparquet_io.core.process.aggregate.common import geometry_to_geom_expr
from geoparquet_io.core.process.aggregate.grid_common import wrap_grid_geometry

# a5 r7 cell over Fiji. `a5_cell_to_boundary` reports it spanning
# -180.2016 .. -179.6540, i.e. with vertices west of -180.
FIJI_CELL_R7 = 10914438512560308224
# a5 r7 cell over the Chukotka side, reported spanning 179.0883 .. 180.1986.
CHUKOTKA_CELL_R7 = 130780311054188544
# ... and one nowhere near the seam, which the repair must not touch.
NULL_ISLAND_CELL_R7 = 5694203594484482048

# The cut shape for FIJI_CELL_R7, at 1e-6 degrees. Pinned so a change to the
# seam repair has to state what it did to a real dateline cell.
FIJI_CELL_R7_WKT = (
    "MULTIPOLYGON ((("
    "179.798413 -16.208029, 180 -15.96654, 180 -16.465104, "
    "179.949732 -16.508113, 179.798413 -16.208029)), (("
    "-179.654003 -15.950447, -179.786505 -16.282441, -180 -16.465104, "
    "-180 -15.96654, -179.976092 -15.9379, -179.654003 -15.950447)))"
)

# Points either side of the seam near Fiji, plus one far from it. `area` stands
# in for the per-feature metric an FTW-style coverage aggregate carries.
DATELINE_POINTS = [
    (179.95, -16.20, 100.0),
    (179.80, -16.40, 200.0),
    (-179.90, -16.10, 300.0),
    (-179.75, -16.30, 400.0),
    (179.99, 66.00, 500.0),
    (-179.99, 66.00, 600.0),
    (0.10, 0.10, 700.0),
]


def _a5_connection():
    con = get_duckdb_connection(load_spatial=True)
    load_community_extension(con, "a5", feature="a5 dateline tests")
    con.execute("SET geometry_always_xy = true")
    return con


def _write_points(path, points):
    con = get_duckdb_connection(load_spatial=True)
    con.execute("SET geometry_always_xy = true")
    values = ", ".join(f"(ST_Point({x}, {y}), {area})" for x, y, area in points)
    con.execute(
        f"COPY (SELECT * FROM (VALUES {values}) AS t(geometry, area)) TO '{path}' (FORMAT PARQUET)"
    )
    con.close()
    return str(path)


@pytest.fixture
def dateline_parquet(tmp_path):
    return _write_points(tmp_path / "dateline.parquet", DATELINE_POINTS)


def _vertex_report(parquet_file):
    """(cells, invalid, out_of_range, multipolygons) over an aggregate's cells."""
    con = _a5_connection()
    try:
        relation = f"read_parquet('{parquet_file}')"
        geom = geometry_to_geom_expr(con, relation, "geometry")
        cells = f"SELECT {geom} AS g FROM {relation} WHERE geometry IS NOT NULL"
        return con.execute(
            f"""
            SELECT count(*),
                   count(*) FILTER (WHERE NOT ST_IsValid(g)),
                   count(*) FILTER (WHERE ST_XMin(g) < -180.0 OR ST_XMax(g) > 180.0),
                   count(*) FILTER (WHERE ST_GeometryType(g) = 'MULTIPOLYGON')
            FROM ({cells})
            """
        ).fetchone()
    finally:
        con.close()


# ---------------------------------------------------------------------------
# What the grid library reports, before anything is done about it.
# ---------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.network
@pytest.mark.parametrize(
    "cell,side", [(FIJI_CELL_R7, "west of -180"), (CHUKOTKA_CELL_R7, "east of 180")]
)
def test_the_raw_a5_boundary_leaves_the_valid_range(cell, side):
    """The defect these cells are pinned for: without the repair the polygon
    written for them carries vertices outside [-180, 180]."""
    con = _a5_connection()
    try:
        lo, hi = con.execute(
            f"SELECT list_min(list_transform(a5_cell_to_boundary({cell}::UBIGINT), p -> p[1])), "
            f"list_max(list_transform(a5_cell_to_boundary({cell}::UBIGINT), p -> p[1]))"
        ).fetchone()
    finally:
        con.close()
    assert lo < -180.0 or hi > 180.0, f"cell {cell} was expected to run {side}"


# ---------------------------------------------------------------------------
# What the aggregate writes.
# ---------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.network
def test_a5_r7_dateline_cells_stay_inside_the_world(dateline_parquet, tmp_path):
    out = tmp_path / "a5_r7.parquet"
    aggregate_by_a5(dateline_parquet, str(out), resolution=7, metric="sum:area")
    total, invalid, out_of_range, multi = _vertex_report(out)
    assert total > 0
    assert invalid == 0, f"{invalid} of {total} cell polygons are invalid"
    assert out_of_range == 0, f"{out_of_range} of {total} cells have vertices outside [-180, 180]"
    assert multi > 0, "no dateline cell was cut into a MultiPolygon"


@pytest.mark.slow
@pytest.mark.network
def test_the_fiji_cell_keeps_its_pinned_multipolygon():
    """One known dateline cell, pinned shape. Built straight through
    ``wrap_grid_geometry`` so the pin covers aggregate and overview alike."""
    sql = wrap_grid_geometry(
        f"SELECT {FIJI_CELL_R7}::UBIGINT AS a5_cell, 1 AS count",
        A5_SCHEME,
        "a5_cell",
        "polygon",
    )
    con = _a5_connection()
    try:
        (wkt,) = con.execute(
            f"SELECT ST_AsText(ST_ReducePrecision(ST_GeomFromWKB(geometry), 0.000001)) FROM ({sql})"
        ).fetchone()
    finally:
        con.close()
    assert wkt == FIJI_CELL_R7_WKT


@pytest.mark.slow
@pytest.mark.network
def test_a_cell_away_from_the_seam_is_handed_through_untouched():
    """A ring that is already contiguous and in range must reach the output as
    the grid library drew it -- same WKB, still a Polygon, not rebuilt."""
    sql = wrap_grid_geometry(
        f"SELECT {NULL_ISLAND_CELL_R7}::UBIGINT AS a5_cell, 1 AS count",
        A5_SCHEME,
        "a5_cell",
        "polygon",
    )
    raw = A5_SCHEME.boundary_template.format(cell=f"{NULL_ISLAND_CELL_R7}::UBIGINT")
    con = _a5_connection()
    try:
        written, untouched = con.execute(
            f"SELECT (SELECT geometry FROM ({sql})), ST_AsWKB({raw})"
        ).fetchone()
    finally:
        con.close()
    assert bytes(written) == bytes(untouched)


@pytest.mark.slow
@pytest.mark.network
def test_the_seam_repair_leaves_counts_and_metrics_alone(dateline_parquet, tmp_path):
    """The repair rewrites geometry only: the same run with no geometry at all
    must produce the same cells, the same counts and the same metric."""
    with_geom = tmp_path / "with_geom.parquet"
    without = tmp_path / "without.parquet"
    aggregate_by_a5(dateline_parquet, str(with_geom), resolution=7, metric="sum:area")
    aggregate_by_a5(
        dateline_parquet, str(without), resolution=7, metric="sum:area", out_geometry="none"
    )
    con = _a5_connection()
    try:
        rows = con.execute(
            f"""
            SELECT a5_cell, count, sum_area FROM read_parquet('{with_geom}') ORDER BY a5_cell
            """
        ).fetchall()
        bare = con.execute(
            f"""
            SELECT a5_cell, count, sum_area FROM read_parquet('{without}') ORDER BY a5_cell
            """
        ).fetchall()
    finally:
        con.close()
    assert rows == bare
    assert sum(r[1] for r in rows) == len(DATELINE_POINTS)
    assert sum(float(r[2]) for r in rows) == pytest.approx(sum(p[2] for p in DATELINE_POINTS))


# ---------------------------------------------------------------------------
# ... and what the overview writes for the same cells' parents.
# ---------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.network
def test_a5_overview_parents_are_dateline_safe(dateline_parquet, tmp_path):
    """`process overview` regenerates parent polygons through the same builder,
    so an r5 parent of a dateline r7 cell must be cut too -- and must keep the
    rolled-up metric while it is at it."""
    from geoparquet_io.core.process.overview.run import create_overviews

    base = tmp_path / "a5_base.parquet"
    aggregate_by_a5(dateline_parquet, str(base), resolution=7, metric="sum:area")
    written = create_overviews(str(base), levels=[5])
    assert len(written) == 1
    _level, parent_path = written[0]

    total, invalid, out_of_range, multi = _vertex_report(parent_path)
    assert total > 0
    assert invalid == 0, f"{invalid} of {total} parent polygons are invalid"
    assert out_of_range == 0, f"{out_of_range} of {total} parents leave [-180, 180]"
    assert multi > 0, "no parent cell was cut at the antimeridian"

    con = _a5_connection()
    try:
        rolled_count, rolled_area = con.execute(
            f"SELECT SUM(count), SUM(sum_area) FROM read_parquet('{parent_path}')"
        ).fetchone()
    finally:
        con.close()
    assert rolled_count == len(DATELINE_POINTS)
    assert float(rolled_area) == pytest.approx(sum(p[2] for p in DATELINE_POINTS))
