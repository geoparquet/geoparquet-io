"""Grid-cell output must be antimeridian-safe.

A cell ring is a closed curve on the sphere, and the two grid libraries disagree
about how to flatten it: `h3_cell_to_boundary_wkb` wraps every vertex into
[-180, 180] (so a cell straddling the antimeridian tears into a band across the
whole map), while `a5_cell_to_boundary` keeps the ring contiguous but lets
longitudes run past the valid range. Both are repaired in one shared builder,
which cuts a crossing cell into a MultiPolygon at +/-180 (RFC 7946 3.1.9) and
closes a pole-enclosing ring through an explicit seam.

The invariant these tests hold every scheme to: **every cell polygon is valid,
contiguous, and inside [-180, 180]**, and the centroid the same row carries
falls inside it.
"""

import json

import duckdb
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.process.aggregate.by_a5 import A5_SCHEME, aggregate_by_a5
from geoparquet_io.core.process.aggregate.by_h3 import H3_SCHEME, aggregate_by_h3
from geoparquet_io.core.process.aggregate.common import (
    antimeridian_aware_bbox,
    geometry_to_geom_expr,
)
from geoparquet_io.core.process.aggregate.grid_common import GridScheme, wrap_grid_geometry

# Fiji sits on the antimeridian, so cells here straddle it at low resolutions.
ANTIMERIDIAN_POINTS = [
    (179.95, -16.2),
    (179.80, -16.4),
    (-179.90, -16.1),
    (-179.75, -16.3),
    (179.60, -16.0),
    (-179.60, -16.5),
]
# Both poles, so the pole-enclosing cells are exercised too.
POLE_POINTS = [(0.0, 89.9), (120.0, 89.5), (-120.0, 88.5), (0.0, -89.9), (60.0, -88.5)]


def _spatial_connection():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    return con


def _write_points(path, points):
    con = _spatial_connection()
    values = ", ".join(f"(ST_Point({x}, {y}), 1)" for x, y in points)
    con.execute(
        f"COPY (SELECT * FROM (VALUES {values}) AS t(geometry, n)) TO '{path}' (FORMAT PARQUET)"
    )
    con.close()
    return str(path)


@pytest.fixture
def antimeridian_parquet(tmp_path):
    return _write_points(tmp_path / "antimeridian.parquet", ANTIMERIDIAN_POINTS)


@pytest.fixture
def pole_parquet(tmp_path):
    return _write_points(tmp_path / "poles.parquet", POLE_POINTS)


# ---------------------------------------------------------------------------
# Fast lane: no community extension, so these run where diff-cover reads.
# ---------------------------------------------------------------------------

# Real rings, taken from the grid libraries, keyed by what is wrong with them.
# "plain" needs nothing done; the rest are what the repair exists for.
_RING_CASES = {
    "plain": "POLYGON ((10 10, 11 10, 11 11, 10 11, 10 10))",
    # h3 819b7ffffffffff near Fiji: vertices on both sides of the seam.
    "torn": (
        "POLYGON ((-178.00242433 -22.06717569, -175.76570558 -18.09884526, "
        "-178.00946172 -14.54728283, 177.80629501 -14.86964530, "
        "176.70743517 -16.71898041, 175.52711991 -18.29733290, "
        "177.51613499 -22.19754139, -178.00242433 -22.06717569))"
    ),
    # a5-style: contiguous, but running past +180.
    "beyond_180": "POLYGON ((175 10, 185 10, 185 20, 175 20, 175 10))",
    # h3 81033ffffffffff, which contains the north pole.
    "north_pole": (
        "POLYGON ((8.38436067 87.82361750, 145.55819769 87.36469532, "
        "-161.89171801 84.22628942, -125.29445893 82.48154131, "
        "-90.38109747 82.85349004, -51.70487752 84.93555635, "
        "8.38436067 87.82361750))"
    ),
    # The same ring reflected, so it contains the south pole instead.
    "south_pole": (
        "POLYGON ((8.38436067 -87.82361750, 145.55819769 -87.36469532, "
        "-161.89171801 -84.22628942, -125.29445893 -82.48154131, "
        "-90.38109747 -82.85349004, -51.70487752 -84.93555635, "
        "8.38436067 -87.82361750))"
    ),
}


# The cell centres the grid libraries report for those rings -- h3_cell_to_latlng
# for the two real cells, the obvious centre for the synthetic ones. They stand
# in for the scheme's own centre function, which reports lon/lat in range.
_RING_CENTRES = {
    "plain": (10.5, 10.5),
    "torn": (179.80304066, -18.38547805),
    "beyond_180": (180.0, 15.0),
    "north_pole": (-110.58483359, 86.88988502),
    "south_pole": (-110.58483359, -86.88988502),
}


def _ring_case_scheme():
    """A scheme whose boundary is a literal WKT ring picked by the cell id.

    Keeps the whole repair testable on a plain DuckDB + spatial connection, with
    no h3/a5 community extension and no network.
    """
    cases = " ".join(f"WHEN '{name}' THEN '{wkt}'" for name, wkt in _RING_CASES.items())
    centres = " ".join(
        f"WHEN '{name}' THEN [{lon}, {lat}]" for name, (lon, lat) in _RING_CENTRES.items()
    )
    return GridScheme(
        name="ringcase",
        extension="",
        min_resolution=0,
        max_resolution=0,
        default_column="cell",
        key_template="{pt}",
        boundary_template=f"ST_GeomFromText(CASE {{cell}} {cases} END)",
        latlng_template=f"CASE {{cell}} {centres} END",
        centroid_wkb_template="ST_AsWKB(ST_Point({ll}[1], {ll}[2]))",
    )


@pytest.fixture
def repaired_rings():
    """{case name: row} for every ring case, put through the shared builder."""
    scheme = _ring_case_scheme()
    names = ", ".join(f"('{name}')" for name in _RING_CASES)
    sql = wrap_grid_geometry(f"SELECT * FROM (VALUES {names}) AS t(cell)", scheme, "cell", "both")
    con = _spatial_connection()
    try:
        rows = con.execute(
            f"""
            SELECT cell,
                   ST_GeometryType(g) AS type,
                   ST_IsValid(g) AS is_valid,
                   ST_XMin(g) AS xmin, ST_XMax(g) AS xmax,
                   ST_YMin(g) AS ymin, ST_YMax(g) AS ymax,
                   ST_Area(g) AS area,
                   ST_Intersects(g, ST_GeomFromWKB(centroid)) AS holds_centroid
            FROM (SELECT cell, ST_GeomFromWKB(geometry) AS g, centroid FROM ({sql}))
            """
        ).fetchall()
    finally:
        con.close()
    cols = (
        "type",
        "is_valid",
        "xmin",
        "xmax",
        "ymin",
        "ymax",
        "area",
        "holds_centroid",
    )
    return {row[0]: dict(zip(cols, row[1:], strict=True)) for row in rows}


@pytest.mark.parametrize("case", sorted(_RING_CASES))
def test_repaired_ring_is_valid_and_in_range(repaired_rings, case):
    row = repaired_rings[case]
    assert row["is_valid"], f"{case}: repaired ring is not a valid polygon"
    assert row["xmin"] >= -180.0, f"{case}: xmin {row['xmin']} is west of -180"
    assert row["xmax"] <= 180.0, f"{case}: xmax {row['xmax']} is east of 180"
    assert -90.0 <= row["ymin"] <= row["ymax"] <= 90.0


def test_untouched_ring_stays_a_single_polygon(repaired_rings):
    """A ring that is already contiguous and in range must not be rewritten."""
    row = repaired_rings["plain"]
    assert row["type"] == "POLYGON"
    assert row["area"] == pytest.approx(1.0)


@pytest.mark.parametrize("case", ["torn", "beyond_180", "north_pole", "south_pole"])
def test_crossing_ring_becomes_a_multipolygon(repaired_rings, case):
    assert repaired_rings[case]["type"] == "MULTIPOLYGON"


def test_cut_preserves_the_ring_area(repaired_rings):
    """Cutting at the seam moves vertices, it does not lose area: the 10x10 box
    at 175..185 keeps its 100 square degrees on both sides of the line."""
    assert repaired_rings["beyond_180"]["area"] == pytest.approx(100.0)


@pytest.mark.parametrize("case,pole", [("north_pole", 90.0), ("south_pole", -90.0)])
def test_pole_enclosing_ring_gets_a_seam_to_the_pole(repaired_rings, case, pole):
    """A ring that encircles a pole spans every longitude, so it is closed
    through the pole rather than unwrapped."""
    row = repaired_rings[case]
    assert row["ymax" if pole > 0 else "ymin"] == pytest.approx(pole)
    # Both halves of the cut are present, so the cap covers the full sweep.
    assert row["xmin"] == pytest.approx(-180.0)
    assert row["xmax"] == pytest.approx(180.0)


@pytest.mark.parametrize("case", sorted(_RING_CASES))
def test_polygon_and_centroid_share_one_frame(repaired_rings, case):
    """--out-geometry both must not put the centroid outside its own cell."""
    assert repaired_rings[case]["holds_centroid"], (
        f"{case}: the centroid falls outside the polygon written for the same cell"
    )


def test_polygon_sql_carries_the_seam_repair():
    """The generated SQL keeps its shape: guard, cut, and no leaked internals."""
    sql = wrap_grid_geometry("SELECT * FROM t", H3_SCHEME, "h3_cell", "polygon")
    assert "EXCLUDE (__bnd, __ll, __ring, __fring)" in sql
    # The repair is skipped for rings that are already contiguous and in range.
    assert "ST_XMax(__bnd) - ST_XMin(__bnd) > 180.0" in sql
    assert "WHEN __ring IS NULL THEN ST_AsWKB(__bnd)" in sql
    # ... and cuts at the antimeridian for the ones that are not.
    assert "ST_MakeEnvelope(-180.0, -90.0, 180.0, 90.0)" in sql
    assert "ST_MakeEnvelope(180.0, -90.0, 540.0, 90.0)" in sql
    assert "ST_CollectionExtract(" in sql
    # The scheme's cell function is evaluated once, not once per repair layer.
    assert sql.count("h3_cell_to_boundary_wkb") == 1


def test_centroid_only_sql_builds_no_boundary():
    """--out-geometry centroid needs no polygon, so it must not pay for one."""
    sql = wrap_grid_geometry("SELECT * FROM t", A5_SCHEME, "a5_cell", "centroid")
    assert "a5_cell_to_boundary" not in sql
    assert "__ring" not in sql
    assert "ST_Intersection" not in sql


def test_both_sql_repairs_the_polygon_and_keeps_the_centroid():
    sql = wrap_grid_geometry("SELECT * FROM t", A5_SCHEME, "a5_cell", "both")
    assert "AS geometry, " in sql and " AS centroid " in sql
    assert "ST_CollectionExtract(" in sql


# ---------------------------------------------------------------------------
# geo bbox: RFC 7946 5.2 wrap form
# ---------------------------------------------------------------------------


def _bbox_of(wkts):
    con = _spatial_connection()
    try:
        values = ", ".join(f"(ST_AsWKB(ST_GeomFromText('{w}')))" for w in wkts)
        con.execute(f"CREATE TEMP TABLE g AS SELECT * FROM (VALUES {values}) t(geometry)")
        return antimeridian_aware_bbox(con, "g", "geometry")
    finally:
        con.close()


def test_bbox_stays_plain_for_data_that_does_not_cross():
    bbox = _bbox_of(["POLYGON ((-100 30, -90 30, -90 40, -100 40, -100 30))"])
    assert bbox == pytest.approx([-100.0, 30.0, -90.0, 40.0])


def test_bbox_uses_the_wrap_form_for_data_astride_the_seam():
    """Parts at both -180 and 180 with nothing in between: a plain min/max would
    claim the whole globe, so the extent is written xmin > xmax instead."""
    bbox = _bbox_of(
        [
            "MULTIPOLYGON (((179 -17, 180 -17, 180 -16, 179 -16, 179 -17)), "
            "((-180 -17, -179 -17, -179 -16, -180 -16, -180 -17)))"
        ]
    )
    assert bbox[0] > bbox[2], f"expected an antimeridian wrap form, got {bbox}"
    assert bbox == pytest.approx([179.0, -17.0, -179.0, -16.0])


def test_bbox_stays_plain_for_genuinely_global_data():
    bbox = _bbox_of(
        [
            "POLYGON ((-180 -10, 0 -10, 0 10, -180 10, -180 -10))",
            "POLYGON ((0 -10, 180 -10, 180 10, 0 10, 0 -10))",
        ]
    )
    assert bbox == pytest.approx([-180.0, -10.0, 180.0, 10.0])


def test_result_bbox_reads_an_arrow_table():
    """The aggregate writer takes its bbox off the finished Arrow result."""
    from geoparquet_io.core.process.aggregate.grid_common import _result_geo_bbox

    con = _spatial_connection()
    try:
        result = (
            con.execute(
                """
            SELECT ST_AsWKB(ST_GeomFromText(w)) AS geometry FROM (VALUES
              ('POLYGON ((179 -17, 180 -17, 180 -16, 179 -16, 179 -17))'),
              ('POLYGON ((-180 -17, -179 -17, -179 -16, -180 -16, -180 -17))')
            ) t(w)
            """
            )
            .arrow()
            .read_all()
        )
        assert _result_geo_bbox(con, result) == pytest.approx([179.0, -17.0, -179.0, -16.0])
        # The registration is cleaned up, so a second result can reuse the name.
        assert _result_geo_bbox(con, result) is not None
    finally:
        con.close()


def test_bbox_is_none_without_geometry():
    con = _spatial_connection()
    try:
        con.execute("CREATE TEMP TABLE g AS SELECT NULL::BLOB AS geometry WHERE false")
        assert antimeridian_aware_bbox(con, "g", "geometry") is None
    finally:
        con.close()


# ---------------------------------------------------------------------------
# The real grids. These need the h3 / a5 community extensions.
# ---------------------------------------------------------------------------


def _cell_report(parquet_file):
    """Validity, longitude range and widest part over every cell in an output.

    The widest *part*, not the widest geometry: a cell cut at the seam has parts
    at both -180 and 180, so its own envelope spans the globe by construction. A
    torn ring is the case where one part does.
    """
    con = _spatial_connection()
    relation = f"read_parquet('{parquet_file}')"
    try:
        geom = geometry_to_geom_expr(con, relation, "geometry")
        cells = f"SELECT {geom} AS g FROM {relation} WHERE geometry IS NOT NULL"
        return con.execute(
            f"""
            SELECT count(*),
                   count(*) FILTER (WHERE NOT ST_IsValid(g)),
                   count(*) FILTER (WHERE ST_XMin(g) < -180.0 OR ST_XMax(g) > 180.0),
                   count(*) FILTER (WHERE ST_GeometryType(g) = 'MULTIPOLYGON'),
                   (SELECT max(ST_XMax(p) - ST_XMin(p))
                    FROM (SELECT UNNEST(ST_Dump(g)).geom AS p FROM ({cells})))
            FROM ({cells})
            """
        ).fetchone()
    finally:
        con.close()


def _assert_cells_are_antimeridian_safe(out, expect_multipolygon=True, max_part_width=None):
    from geoparquet_io.core.validate import validate_geoparquet

    total, invalid, out_of_range, multi, widest = _cell_report(out)
    assert total > 0
    assert invalid == 0, f"{invalid} of {total} cell polygons are invalid"
    assert out_of_range == 0, f"{out_of_range} of {total} cells leave [-180, 180]"
    if expect_multipolygon:
        assert multi > 0, "no cell was cut at the antimeridian"
    if max_part_width is not None:
        assert widest < max_part_width, (
            f"a cell part spans {widest:.3f} degrees of longitude, so its ring tore"
        )
    result = validate_geoparquet(str(out), validate_data=True)
    assert result.failed_count == 0, [c.name for c in result.checks if c.status.value == "failed"]


def _pole_cells(parquet_file):
    """Cells reaching a pole: (ymin, ymax, xmin, xmax) for each."""
    con = _spatial_connection()
    relation = f"read_parquet('{parquet_file}')"
    try:
        geom = geometry_to_geom_expr(con, relation, "geometry")
        return con.execute(
            f"""
            SELECT ST_YMin(g), ST_YMax(g), ST_XMin(g), ST_XMax(g)
            FROM (SELECT {geom} AS g FROM {relation} WHERE geometry IS NOT NULL)
            WHERE ST_YMax(g) >= 89.999999 OR ST_YMin(g) <= -89.999999
            """
        ).fetchall()
    finally:
        con.close()


def _assert_pole_cells_reach_the_pole(out, full_width):
    """Cells at a pole must reach it -- the seam runs the boundary up to +/-90.

    ``full_width`` says whether the grid's polar cell *encloses* the pole, as
    H3's does: such a ring spans every longitude, so its seamed polygon runs the
    whole [-180, 180]. A5's polar cells only touch the pole at a vertex and keep
    their own longitude wedge.
    """
    cells = _pole_cells(out)
    assert cells, "no cell reached a pole"
    for ymin, ymax, xmin, xmax in cells:
        assert ymax == pytest.approx(90.0) or ymin == pytest.approx(-90.0)
        assert -180.0 <= xmin <= xmax <= 180.0
        if full_width:
            assert xmin == pytest.approx(-180.0), f"pole cell starts at {xmin}, not -180"
            assert xmax == pytest.approx(180.0), f"pole cell ends at {xmax}, not 180"


@pytest.mark.slow
@pytest.mark.network
@pytest.mark.parametrize("resolution", [1, 3])
def test_h3_cells_are_antimeridian_safe(antimeridian_parquet, tmp_path, resolution):
    out = tmp_path / f"h3_{resolution}.parquet"
    aggregate_by_h3(antimeridian_parquet, str(out), resolution=resolution)
    _assert_cells_are_antimeridian_safe(out, max_part_width=180.0)


@pytest.mark.slow
@pytest.mark.network
@pytest.mark.parametrize("resolution", [1, 3])
def test_a5_cells_are_antimeridian_safe(antimeridian_parquet, tmp_path, resolution):
    """a5_cell_to_boundary reports longitudes outside [-180, 180] (267.0 is a
    real value), so this fails before the fix on the range check alone."""
    out = tmp_path / f"a5_{resolution}.parquet"
    aggregate_by_a5(antimeridian_parquet, str(out), resolution=resolution)
    _assert_cells_are_antimeridian_safe(out, max_part_width=180.0)


@pytest.mark.slow
@pytest.mark.network
@pytest.mark.parametrize("resolution", [0, 1, 2])
def test_a5_polar_cells_stay_valid_and_in_range(pole_parquet, tmp_path, resolution):
    """The two pole-touching A5 cells are the ones a naive unwrap ruins: their
    rings span far more than 180 degrees for real, so shifting vertices relative
    to the first tears them."""
    out = tmp_path / f"a5_pole_{resolution}.parquet"
    aggregate_by_a5(pole_parquet, str(out), resolution=resolution)
    _assert_cells_are_antimeridian_safe(out, expect_multipolygon=False)
    _assert_pole_cells_reach_the_pole(out, full_width=False)


@pytest.mark.slow
@pytest.mark.network
@pytest.mark.parametrize("resolution", [1, 3, 5])
def test_h3_pole_cells_stay_valid_and_in_range(pole_parquet, tmp_path, resolution):
    """The cells that contain a pole span every longitude, so they need a seam
    rather than an unwrap."""
    out = tmp_path / f"h3_pole_{resolution}.parquet"
    aggregate_by_h3(pole_parquet, str(out), resolution=resolution)
    _assert_cells_are_antimeridian_safe(out, expect_multipolygon=False)
    _assert_pole_cells_reach_the_pole(out, full_width=True)


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_writes_the_wrap_form_bbox(antimeridian_parquet, tmp_path):
    """A Fiji-only aggregate must not claim the whole globe in its geo bbox."""
    out = tmp_path / "h3_bbox.parquet"
    aggregate_by_h3(antimeridian_parquet, str(out), resolution=1)
    col = json.loads(pq.read_schema(out).metadata[b"geo"])["columns"]["geometry"]
    assert col["bbox"][0] > col["bbox"][2], f"expected the wrap form, got {col['bbox']}"
    assert "MultiPolygon" in col["geometry_types"]


@pytest.mark.slow
@pytest.mark.network
def test_out_geometry_both_keeps_one_frame(antimeridian_parquet, tmp_path):
    out = tmp_path / "h3_both.parquet"
    aggregate_by_h3(antimeridian_parquet, str(out), resolution=3, out_geometry="both")
    con = _spatial_connection()
    relation = f"read_parquet('{out}')"
    try:
        geom = geometry_to_geom_expr(con, relation, "geometry")
        centroid = geometry_to_geom_expr(con, relation, "centroid")
        outside = con.execute(
            f"""
            SELECT count(*) FROM {relation}
            WHERE geometry IS NOT NULL AND NOT ST_Intersects({geom}, {centroid})
            """
        ).fetchone()[0]
    finally:
        con.close()
    assert outside == 0, f"{outside} centroids fall outside their own cell polygon"


@pytest.mark.slow
@pytest.mark.network
def test_overview_rollup_is_antimeridian_safe(antimeridian_parquet, tmp_path):
    """`process overview` regenerates parent geometry through the same builder,
    so a rolled-up parent cell must be seam-safe too."""
    from geoparquet_io.core.process.overview.run import create_overviews

    base = tmp_path / "h3_base.parquet"
    aggregate_by_h3(antimeridian_parquet, str(base), resolution=4)
    written = create_overviews(str(base), levels=[1, 2])
    assert len(written) == 2
    for _level, path in written:
        _assert_cells_are_antimeridian_safe(path, max_part_width=180.0)
