"""`--metric pct_cell:<column>`: percent of the cell's area covered (#1181).

"How much of each cell is covered" is the choropleth polygon layers aggregated
to a grid are usually made for, and every catalog used to compute it
downstream, re-measuring the cell area itself because `ST_Area_Spheroid`
returns nonsense on a5 cell polygons. The grid extensions know their own cell
areas, so gpio computes the metric in the aggregate:

- a5 is equal-area by construction: `a5_cell_area(resolution)` is one constant
  per resolution;
- h3 is not, so its denominator is the exact per-cell `h3_cell_area(cell)`.

`pct_cell:x` implies `sum:x`, which is what lets `process overview` recompute a
parent's percentage from the rolled-up total instead of averaging its
children's percentages.
"""

from __future__ import annotations

import duckdb
import pyarrow as pa
import pytest

from geoparquet_io.core.duckdb_utils import load_community_extension
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.memory_limits import open_bounded_connection
from geoparquet_io.core.process.aggregate.by_a5 import A5_SCHEME
from geoparquet_io.core.process.aggregate.by_h3 import H3_SCHEME
from geoparquet_io.core.process.aggregate.common import (
    PCT_CELL_FUNC,
    VALID_METRIC_SPECS,
    MetricSpec,
    build_metric_select,
    parse_breakdown_metric,
    parse_metrics,
    validate_metric_nodata,
)
from geoparquet_io.core.process.aggregate.grid_common import (
    build_grid_query,
    cell_area_expr,
    needed_source_columns,
    read_grid_source_sql,
)
from geoparquet_io.core.process.overview.detect import RollupColumn, _classify_columns
from geoparquet_io.core.process.overview.rollup import build_rollup_agg_parts

# The measured a5 cell areas the issue cross-checks against, in km^2. a5 is an
# equal-area grid, so these are exact per resolution rather than averages.
A5_AREA_KM2 = {5: 33208.0, 7: 2075.5, 8: 518.9}


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def test_pct_cell_is_an_accepted_metric_spec():
    assert PCT_CELL_FUNC in VALID_METRIC_SPECS
    (spec,) = parse_metrics("pct_cell:area")
    assert spec == MetricSpec(func="pct_cell", column="area", output_name="pct_area")


def test_the_unknown_function_error_offers_pct_cell():
    with pytest.raises(InvalidParameterError) as exc:
        parse_metrics("median:area")
    assert "pct_cell" in str(exc.value)


def test_pct_cell_needs_a_column():
    with pytest.raises(InvalidParameterError, match="missing a column name"):
        parse_metrics("pct_cell:")


def test_pct_cell_implies_the_underlying_sum():
    """The sum is what an overview recomputes the parent percentage from, so it
    is emitted whether or not it was asked for -- right before its percentage."""
    metrics, _ = validate_metric_nodata("pct_cell:area", None)
    assert [m.output_name for m in metrics] == ["sum_area", "pct_area"]


def test_an_explicit_sum_is_not_duplicated():
    metrics, _ = validate_metric_nodata("sum:area,pct_cell:area", None)
    assert [m.output_name for m in metrics] == ["sum_area", "pct_area"]


def test_the_implied_sum_matches_the_columns_own_spelling():
    """A metric names the column as the user spelled it; the implied sum must
    not become a second, differently-spelled read of the same column."""
    metrics, _ = validate_metric_nodata("avg:AREA,pct_cell:AREA", None)
    assert [m.output_name for m in metrics] == ["avg_AREA", "sum_AREA", "pct_AREA"]
    assert needed_source_columns(metrics, None, None) == ("AREA",)


def test_pct_cell_is_refused_as_a_breakdown_metric():
    """A per-category percentage of the cell has no rollup rule: the overview
    would have to carry each category's own sum, which the pivot replaces."""
    with pytest.raises(InvalidParameterError, match="pct_cell"):
        parse_breakdown_metric("pct_cell:area")


# ---------------------------------------------------------------------------
# The SELECT expression
# ---------------------------------------------------------------------------


def test_build_metric_select_divides_by_the_cell_area():
    sql = build_metric_select(parse_metrics("pct_cell:area"), cell_area_expr="a5_cell_area(7)")
    assert sql == '100.0 * SUM("area") / NULLIF(a5_cell_area(7), 0) AS "pct_area"'


def test_pct_cell_honours_nodata_sentinels():
    """The numerator is a SUM like any other, so sentinel values drop out of it."""
    sql = build_metric_select(
        parse_metrics("pct_cell:area"),
        nodata_values=["-999"],
        column_types={"area": "DOUBLE"},
        cell_area_expr="a5_cell_area(7)",
    )
    assert 'SUM(NULLIF("area", -999))' in sql


def test_pct_cell_without_a_cell_area_is_refused():
    """Admin regions have no fixed cell area, so the metric cannot mean anything
    there -- and the message has to say so rather than fail in the binder."""
    with pytest.raises(InvalidParameterError, match="a5 or h3"):
        build_metric_select(parse_metrics("pct_cell:area"))


def test_pct_cell_on_a_non_numeric_column_is_refused():
    with pytest.raises(InvalidParameterError, match="numeric"):
        build_metric_select(
            parse_metrics("pct_cell:label"),
            column_types={"label": "VARCHAR"},
            cell_area_expr="a5_cell_area(7)",
        )


def test_the_a5_cell_area_is_a_resolution_constant():
    assert cell_area_expr(A5_SCHEME, 7, "__key") == (
        "CASE WHEN __key IS NULL THEN NULL ELSE a5_cell_area(7) END"
    )


def test_the_h3_cell_area_is_measured_per_cell():
    """h3 cells are not equal-area, so the denominator is the cell's own area."""
    assert cell_area_expr(H3_SCHEME, 7, "__key") == (
        "CASE WHEN __key IS NULL THEN NULL ELSE h3_cell_area(__key, 'm^2') END"
    )


def test_a_scheme_with_no_area_function_has_no_cell_area():
    bare = A5_SCHEME.__class__(
        name="bare",
        extension="",
        min_resolution=0,
        max_resolution=0,
        default_column="cell",
        key_template="{pt}",
        boundary_template="{cell}",
        latlng_template="{cell}",
        centroid_wkb_template="{ll}",
    )
    assert cell_area_expr(bare, 7, "__key") is None


# ---------------------------------------------------------------------------
# The rollup
# ---------------------------------------------------------------------------


def test_a_pct_column_rolls_up_from_its_sum():
    columns = [("a5_cell", "UBIGINT"), ("count", "BIGINT")]
    columns += [("sum_area", "DOUBLE"), ("pct_area", "DOUBLE")]
    rollups, dropped = _classify_columns(columns, "a5_cell", "a5")
    assert dropped == ()
    assert RollupColumn("pct_area", "pct_cell", source_column="sum_area") in rollups


def test_a_pct_column_without_its_sum_cannot_roll_up():
    """Averaging child percentages is wrong, so a percentage with no total
    behind it is dropped rather than rolled up incorrectly."""
    columns = [("a5_cell", "UBIGINT"), ("count", "BIGINT"), ("pct_area", "DOUBLE")]
    rollups, dropped = _classify_columns(columns, "a5_cell", "a5")
    assert dropped == ("pct_area",)
    assert rollups == ()


def test_an_admin_pct_column_cannot_roll_up():
    columns = [("admin_code", "VARCHAR"), ("count", "BIGINT")]
    columns += [("sum_area", "DOUBLE"), ("pct_area", "DOUBLE")]
    rollups, dropped = _classify_columns(columns, "admin_code", "admin")
    assert dropped == ("pct_area",)
    assert [c.name for c in rollups] == ["sum_area"]


def test_build_rollup_agg_parts_recomputes_the_percentage():
    from geoparquet_io.core.process.overview.detect import AggregateInfo

    info = AggregateInfo(
        scheme="a5",
        cell_column="a5_cell",
        base_level=7,
        rollup_columns=(
            RollupColumn("sum_area", "sum"),
            RollupColumn("pct_area", "pct_cell", source_column="sum_area"),
        ),
        out_geometry="polygon",
    )
    parts = build_rollup_agg_parts(info, cell_area_expr="a5_cell_area(5)")
    assert 'SUM("sum_area") AS "sum_area"' in parts
    assert '100.0 * SUM("sum_area") / NULLIF(a5_cell_area(5), 0) AS "pct_area"' in parts
    # Never the mean of the children's percentages.
    assert not any('AVG("pct_area")' in p or 'SUM("pct_area")' in p for p in parts)


def test_a_pct_column_is_dropped_when_no_cell_area_is_available():
    from geoparquet_io.core.process.overview.detect import AggregateInfo

    info = AggregateInfo(
        scheme="a5",
        cell_column="a5_cell",
        base_level=7,
        rollup_columns=(RollupColumn("pct_area", "pct_cell", source_column="sum_area"),),
        out_geometry="none",
    )
    parts = build_rollup_agg_parts(info, cell_area_expr=None)
    assert parts == ["CAST(SUM(count) AS BIGINT) AS count"]


# ---------------------------------------------------------------------------
# End to end, against the real grid extensions
# ---------------------------------------------------------------------------

SCHEMES = {"a5": A5_SCHEME, "h3": H3_SCHEME}
CELL_COLUMN = {"a5": "a5_cell", "h3": "h3_cell"}


@pytest.fixture
def con():
    connection = open_bounded_connection(load_spatial=True, load_httpfs=False)
    for extension in ("a5", "h3"):
        load_community_extension(connection, extension, feature=f"{extension} aggregation")
    connection.execute("SET geometry_always_xy = true")
    yield connection
    connection.close()


# Three features inside one small patch of ocean, so they land in one a5 r7
# cell, with per-feature areas in m^2 that add to a round 20,754,623.404 -- one
# percent of the r7 cell area, so the expected answer is legible.
PATCH_LON, PATCH_LAT = -30.0, 12.0
PATCH_AREAS = [10_000_000.0, 7_000_000.0, 3_754_623.404]


@pytest.fixture
def patch_parquet(tmp_path):
    path = tmp_path / "patch.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(
        f"(ST_Point({PATCH_LON} + {i} * 0.0001, {PATCH_LAT}), {area})"
        for i, area in enumerate(PATCH_AREAS)
    )
    con.execute(
        f"COPY (SELECT * FROM (VALUES {values}) AS t(geometry, area_m2)) "
        f"TO '{path}' (FORMAT PARQUET)"
    )
    con.close()
    return path


def _aggregate(con, scheme: str, src, *, resolution: int, metric: str) -> pa.Table:
    source_sql = read_grid_source_sql(
        con,
        str(src),
        "geometry",
        None,
        keep_columns=needed_source_columns(*_parsed(metric)),
    )
    sql = build_grid_query(
        con,
        SCHEMES[scheme],
        source_sql,
        resolution,
        CELL_COLUMN[scheme],
        metric,
        None,
        20,
        "none",
    )
    return con.execute(sql).arrow().read_all()


def _parsed(metric: str):
    metrics, _ = validate_metric_nodata(metric, None)
    return metrics, None, None


@pytest.mark.parametrize("resolution", [5, 7, 8])
def test_the_a5_cell_area_matches_the_measured_constant(con, resolution):
    """The denominator is the extension's own equal-area constant; these are the
    geodesically measured values the issue cross-checks it against."""
    (area_m2,) = con.execute(f"SELECT a5_cell_area({resolution})").fetchone()
    assert area_m2 / 1e6 == pytest.approx(A5_AREA_KM2[resolution], rel=1e-3)


def test_pct_cell_is_the_percentage_of_the_a5_cell_covered(con, patch_parquet):
    table = _aggregate(con, "a5", patch_parquet, resolution=7, metric="pct_cell:area_m2")
    assert table.column_names == ["a5_cell", "count", "sum_area_m2", "pct_area_m2"]
    assert table.num_rows == 1
    assert table.column("count")[0].as_py() == len(PATCH_AREAS)
    assert float(table.column("sum_area_m2")[0].as_py()) == pytest.approx(sum(PATCH_AREAS))

    (cell_area,) = con.execute("SELECT a5_cell_area(7)").fetchone()
    expected = 100.0 * sum(PATCH_AREAS) / cell_area
    assert table.column("pct_area_m2")[0].as_py() == pytest.approx(expected)
    assert expected == pytest.approx(1.0, rel=1e-3)


def test_pct_cell_reproduces_the_downstream_derivation(con, patch_parquet):
    """The cross-check the catalogs did by hand: `100 * sum / cell_area`, taken
    from the emitted sum column and the extension's area, must be what the
    metric wrote."""
    table = _aggregate(
        con, "a5", patch_parquet, resolution=7, metric="sum:area_m2,pct_cell:area_m2"
    )
    con.register("__agg", table)
    try:
        mismatches = con.execute(
            "SELECT count(*) FROM __agg "
            "WHERE abs(pct_area_m2 - 100.0 * sum_area_m2 / a5_cell_area(7)) > 1e-9"
        ).fetchone()[0]
    finally:
        con.unregister("__agg")
    assert mismatches == 0


def test_h3_divides_by_each_cells_own_area(con, tmp_path):
    """h3 cells shrink towards the poles, so two cells holding the same area
    must report different percentages."""
    path = tmp_path / "two_latitudes.parquet"
    src = duckdb.connect()
    src.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    src.execute(
        f"COPY (SELECT * FROM (VALUES (ST_Point(0.0, 0.0), 1000000.0), "
        f"(ST_Point(0.0, 80.0), 1000000.0)) AS t(geometry, area_m2)) "
        f"TO '{path}' (FORMAT PARQUET)"
    )
    src.close()

    table = _aggregate(con, "h3", path, resolution=7, metric="pct_cell:area_m2")
    con.register("__agg", table)
    try:
        rows = con.execute(
            "SELECT pct_area_m2, 100.0 * sum_area_m2 / h3_cell_area(h3_cell, 'm^2') "
            "FROM __agg ORDER BY pct_area_m2"
        ).fetchall()
    finally:
        con.unregister("__agg")
    assert len(rows) == 2
    for written, derived in rows:
        assert written == pytest.approx(derived)
    assert rows[0][0] != pytest.approx(rows[1][0]), (
        "both h3 cells got the same denominator, so the per-cell area was not used"
    )


def test_the_unassigned_bucket_has_no_percentage(con, tmp_path):
    """A feature with no keying point lands in the NULL-cell bucket, which is no
    cell and so has no area to be a percentage of."""
    path = tmp_path / "with_null.parquet"
    src = duckdb.connect()
    src.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    src.execute(
        f"COPY (SELECT * FROM (VALUES (ST_Point(-30.0, 12.0), 1000000.0), "
        f"(CAST(NULL AS GEOMETRY), 5000000.0)) AS t(geometry, area_m2)) "
        f"TO '{path}' (FORMAT PARQUET)"
    )
    src.close()

    table = _aggregate(con, "a5", path, resolution=7, metric="pct_cell:area_m2")
    rows = dict(
        zip(
            table.column("a5_cell").to_pylist(),
            table.column("pct_area_m2").to_pylist(),
            strict=True,
        )
    )
    assert rows[None] is None
    assert all(v is not None for k, v in rows.items() if k is not None)


def test_an_unknown_pct_cell_column_is_reported_as_missing(con, patch_parquet):
    with pytest.raises(InvalidParameterError, match="nope"):
        _aggregate(con, "a5", patch_parquet, resolution=7, metric="pct_cell:nope")


def test_the_scan_reads_only_the_pct_cell_column(con, patch_parquet):
    """The narrowed projection (#1179) must know about the new metric's column."""
    metrics, _ = validate_metric_nodata("pct_cell:area_m2", None)
    assert needed_source_columns(metrics, None, None) == ("area_m2",)


def test_overview_parents_recompute_the_percentage_from_the_total(con, patch_parquet, tmp_path):
    """An r5 parent's percentage is its rolled-up total over the r5 cell area --
    a much smaller number than its children's, which averaging would miss."""
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5
    from geoparquet_io.core.process.overview.run import create_overviews

    base = tmp_path / "base.parquet"
    aggregate_by_a5(str(patch_parquet), str(base), resolution=7, metric="pct_cell:area_m2")
    written = create_overviews(str(base), levels=[5])
    assert len(written) == 1
    _level, parent_path = written[0]

    child_pct, child_sum = con.execute(
        f"SELECT pct_area_m2, sum_area_m2 FROM read_parquet('{base}') WHERE a5_cell IS NOT NULL"
    ).fetchone()
    parent_pct, parent_sum = con.execute(
        f"SELECT pct_area_m2, sum_area_m2 FROM read_parquet('{parent_path}') "
        "WHERE a5_cell IS NOT NULL"
    ).fetchone()
    (r5_area,) = con.execute("SELECT a5_cell_area(5)").fetchone()

    assert float(parent_sum) == pytest.approx(float(child_sum))
    assert parent_pct == pytest.approx(100.0 * float(parent_sum) / r5_area)
    # 16 r7 cells fit in an r5 cell, so the parent's percentage is far below the
    # child's -- which is exactly what averaging the children would have given.
    assert parent_pct == pytest.approx(child_pct / 16.0, rel=1e-3)


def test_admin_aggregation_refuses_pct_cell(tmp_path, patch_parquet):
    """An admin region is not a grid cell, so there is no cell area to divide by."""
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    with pytest.raises(InvalidParameterError, match="a5 or h3"):
        aggregate_by_admin(
            str(patch_parquet),
            str(tmp_path / "admin.parquet"),
            metric="pct_cell:area_m2",
        )
