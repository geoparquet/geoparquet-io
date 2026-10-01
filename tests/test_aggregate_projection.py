"""The aggregate scan and its materializations read only the columns they use (#1179).

`gpio process aggregate` reduces a file to one row per bucket, and the only
input columns that reach that row are the keying source, the ``--metric``
columns and the ``--breakdown`` column. The scan nevertheless projected
``SELECT *``, and with ``--breakdown`` it materialized every one of those
columns into the ``__agg_keyed`` / ``__agg_joined`` temp table -- one full-width
copy of the input for a 42 GB / 134M-row file.

These tests pin the narrowing and, more importantly, that it changes nothing
about the answer: every case is computed both ways -- the pre-change
``SELECT *`` source and the narrowed one -- and the two tables must be equal.
"""

from __future__ import annotations

import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.duckdb_utils import load_community_extension, sql_path
from geoparquet_io.core.memory_limits import open_bounded_connection
from geoparquet_io.core.process.aggregate.by_a5 import A5_SCHEME, aggregate_by_a5
from geoparquet_io.core.process.aggregate.by_h3 import H3_SCHEME, aggregate_by_h3
from geoparquet_io.core.process.aggregate.common import (
    parse_breakdown_metric,
    validate_metric_nodata,
)
from geoparquet_io.core.process.aggregate.grid_common import (
    build_grid_query,
    needed_source_columns,
    read_grid_source_sql,
)

SCHEMES = {"a5": A5_SCHEME, "h3": H3_SCHEME}
CELL_COLUMN = {"a5": "a5_cell", "h3": "h3_cell"}


def _write_points(path: str, rows: int = 1500) -> None:
    """Geometry, a bbox covering, two aggregated attributes and one that is never read."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"""COPY (
            SELECT i AS id,
                   ST_Point((i % 97) * 0.5 - 20, (i % 89) * 0.5 - 20) AS geometry,
                   {{'xmin': (i % 97) * 0.5 - 20, 'ymin': (i % 89) * 0.5 - 20,
                     'xmax': (i % 97) * 0.5 - 19.99,
                     'ymax': (i % 89) * 0.5 - 19.99}} AS bbox,
                   (i % 7) * 1.5 AS height,
                   'crop' || (i % 4) AS crop,
                   repeat('padding', 20) AS note
            FROM range({rows}) t(i)
        ) TO '{path}' (FORMAT PARQUET)"""
    )
    con.close()


@pytest.fixture
def points_parquet(tmp_path):
    path = tmp_path / "points.parquet"
    _write_points(str(path))
    return path


@pytest.fixture
def con():
    connection = open_bounded_connection(load_spatial=True, load_httpfs=False)
    for extension in ("a5", "h3"):
        load_community_extension(connection, extension, feature=f"{extension} aggregation")
    connection.execute("SET geometry_always_xy = true")
    yield connection
    connection.close()


def _keep(metric, breakdown, breakdown_metric):
    spec = parse_breakdown_metric(breakdown_metric)
    metrics, _ = validate_metric_nodata(metric, None, spec)
    return needed_source_columns(metrics, breakdown, spec)


def _grid_result(
    con,
    scheme: str,
    src,
    *,
    narrow: bool,
    resolution: int = 4,
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_metric: str | None = None,
    where: str | None = None,
    bucket_point: str = "geometry",
    bbox_column: str | None = None,
    out_geometry: str = "polygon",
) -> pa.Table:
    """One aggregation, with the narrowed projection or the pre-change ``SELECT *``."""
    source_sql = read_grid_source_sql(
        con,
        str(src),
        "geometry",
        None,
        where=where,
        bucket_point=bucket_point,
        bbox_column=bbox_column,
        keep_columns=_keep(metric, breakdown, breakdown_metric) if narrow else None,
    )
    sql = build_grid_query(
        con,
        SCHEMES[scheme],
        source_sql,
        resolution,
        CELL_COLUMN[scheme],
        metric,
        breakdown,
        20,
        out_geometry,
        breakdown_spec=parse_breakdown_metric(breakdown_metric),
    )
    table = con.execute(sql).arrow().read_all()
    con.execute("DROP TABLE IF EXISTS __agg_keyed")
    return table


def _sorted(table: pa.Table, key: str) -> pa.Table:
    return table.sort_by([(key, "ascending")]).combine_chunks()


def _scan_projections(con, sql: str) -> set[str]:
    """Columns the Parquet reader is asked for, from the optimized plan."""
    plan = json.loads(con.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchall()[0][1])
    found: set[str] = set()

    def walk(node):
        info = node.get("extra_info", {})
        if node.get("name") == "READ_PARQUET" and "Projections" in info:
            raw = info["Projections"]
            parts = raw if isinstance(raw, list) else str(raw).strip("[]").split(",")
            found.update(
                str(part).strip().strip("'\"").split(".")[0] for part in parts if str(part).strip()
            )
        for child in node.get("children", []):
            walk(child)

    for root in plan:
        walk(root)
    return found


# ---------------------------------------------------------------------------
# What the narrowed source relation exposes
# ---------------------------------------------------------------------------


class TestTheNarrowedSource:
    def test_projects_only_the_keying_point_and_the_aggregated_columns(self, con, points_parquet):
        sql = read_grid_source_sql(
            con,
            str(points_parquet),
            "geometry",
            keep_columns=_keep("sum:height", "crop", None),
        )
        columns = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM ({sql})").fetchall()}
        assert columns == {"height", "crop", "__pt"}

    def test_an_aggregate_with_no_metrics_projects_only_the_keying_point(self, con, points_parquet):
        sql = read_grid_source_sql(
            con, str(points_parquet), "geometry", keep_columns=_keep(None, None, None)
        )
        columns = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM ({sql})").fetchall()}
        assert columns == {"__pt"}

    def test_the_default_still_passes_every_column_through(self, con, points_parquet):
        """``keep_columns=None`` is the pre-change relation; other callers rely on it."""
        sql = read_grid_source_sql(con, str(points_parquet), "geometry")
        columns = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM ({sql})").fetchall()}
        assert {"id", "note", "geometry"} <= columns

    def test_a_requested_column_is_matched_the_way_duckdb_matches_it(self, con, points_parquet):
        """``sum:HEIGHT`` binds to a ``height`` column, so the projection must keep it."""
        sql = read_grid_source_sql(
            con, str(points_parquet), "geometry", keep_columns=_keep("sum:HEIGHT", None, None)
        )
        columns = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM ({sql})").fetchall()}
        assert columns == {"height", "__pt"}

    def test_a_missing_column_still_reports_as_missing_not_as_a_binder_error(
        self, points_parquet, tmp_path
    ):
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match="nosuch"):
            aggregate_by_a5(
                str(points_parquet),
                str(tmp_path / "out.parquet"),
                resolution=4,
                metric="sum:nosuch",
            )

    def test_the_missing_column_error_still_lists_the_whole_input(self, points_parquet, tmp_path):
        """Narrowing must not shrink the 'Available columns' hint to what was asked for."""
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError) as excinfo:
            aggregate_by_a5(
                str(points_parquet),
                str(tmp_path / "out.parquet"),
                resolution=4,
                metric="sum:nosuch",
            )
        assert "note" in str(excinfo.value) and "crop" in str(excinfo.value)


# ---------------------------------------------------------------------------
# What actually reaches the Parquet reader and the temp tables
# ---------------------------------------------------------------------------


class TestWhatIsRead:
    def test_projection_reaches_the_parquet_scan_through_a_where_clause(self, con, points_parquet):
        """``--where`` sits on the same SELECT as the projection, so both push down."""
        source_sql = read_grid_source_sql(
            con,
            str(points_parquet),
            "geometry",
            where="height > 1",
            bucket_point="bbox",
            bbox_column="bbox",
            keep_columns=_keep("sum:height", None, None),
        )
        sql = build_grid_query(
            con, A5_SCHEME, source_sql, 4, "a5_cell", "sum:height", None, 20, "polygon"
        )
        projections = _scan_projections(con, sql)
        assert "height" in projections and "bbox" in projections
        assert "geometry" not in projections
        assert "note" not in projections and "id" not in projections and "crop" not in projections

    def test_breakdown_materialization_holds_only_the_aggregated_columns(self, con, points_parquet):
        source_sql = read_grid_source_sql(
            con,
            str(points_parquet),
            "geometry",
            keep_columns=_keep("sum:height", "crop", None),
        )
        build_grid_query(
            con, A5_SCHEME, source_sql, 4, "a5_cell", "sum:height", "crop", 20, "polygon"
        )
        try:
            columns = [r[0] for r in con.execute("DESCRIBE __agg_keyed").fetchall()]
        finally:
            con.execute("DROP TABLE IF EXISTS __agg_keyed")
        assert sorted(columns) == ["__key", "crop", "height"]

    @pytest.mark.parametrize(
        "kwargs,expected",
        [
            pytest.param({"metric": "sum:height"}, '"height", ', id="metric"),
            pytest.param({"metric": "sum:height,avg:height"}, '"height", ', id="two-metrics"),
            pytest.param({}, "", id="count-only"),
        ],
    )
    def test_the_command_builds_the_narrowed_scan(self, points_parquet, tmp_path, kwargs, expected):
        """The entry point, not just the builder, asks for the narrow relation.

        Parity alone cannot see this -- the two paths agree by construction --
        so the generated SQL is read back from ``--show-sql``.
        """
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        args = ["process", "aggregate", "a5", str(points_parquet), str(tmp_path / "o.parquet")]
        args += ["--resolution", "4", "--verbose", "--show-sql"]
        for flag, value in kwargs.items():
            args += [f"--{flag}", value]
        result = CliRunner().invoke(cli, args)
        assert result.exit_code == 0, result.output
        assert f"SELECT {expected}ST_Centroid" in result.output
        assert '"note"' not in result.output and '"id"' not in result.output


PARITY_CASES = [
    pytest.param({}, id="plain"),
    pytest.param({"metric": "sum:height,avg:height"}, id="metrics"),
    pytest.param({"breakdown": "crop"}, id="breakdown"),
    pytest.param({"metric": "sum:height", "breakdown": "crop"}, id="metrics+breakdown"),
    pytest.param(
        {"metric": "sum:height", "breakdown": "crop", "breakdown_metric": "sum:height"},
        id="breakdown-metric",
    ),
    pytest.param({"metric": "sum:height", "where": "height > 1"}, id="where"),
    pytest.param(
        {"metric": "sum:height", "breakdown": "crop", "where": "crop <> 'crop0'"},
        id="where+breakdown",
    ),
    pytest.param(
        {"metric": "sum:height", "bucket_point": "bbox", "bbox_column": "bbox"},
        id="bucket-point-bbox",
    ),
    pytest.param(
        {
            "metric": "sum:height",
            "breakdown": "crop",
            "bucket_point": "bbox",
            "bbox_column": "bbox",
            "where": "height > 1",
        },
        id="bbox+breakdown+where",
    ),
    pytest.param({"metric": "sum:height", "out_geometry": "none"}, id="no-geometry"),
    pytest.param({"metric": "sum:height", "out_geometry": "both"}, id="both-geometries"),
]


@pytest.mark.parametrize("scheme", ["a5", "h3"])
@pytest.mark.parametrize("case", PARITY_CASES)
def test_narrowed_output_is_identical_to_the_wide_one(con, points_parquet, scheme, case):
    wide = _grid_result(con, scheme, points_parquet, narrow=False, **case)
    narrow = _grid_result(con, scheme, points_parquet, narrow=True, **case)
    key = CELL_COLUMN[scheme]
    assert narrow.column_names == wide.column_names
    assert _sorted(narrow, key).equals(_sorted(wide, key))


@pytest.mark.parametrize(
    "aggregate,scheme",
    [(aggregate_by_a5, "a5"), (aggregate_by_h3, "h3")],
    ids=["a5", "h3"],
)
def test_the_command_itself_produces_the_wide_answer(
    con, points_parquet, tmp_path, aggregate, scheme
):
    """The entry point, end to end, against the pre-change SQL."""
    out = tmp_path / f"{scheme}.parquet"
    aggregate(
        str(points_parquet),
        str(out),
        resolution=4,
        metric="sum:height,avg:height",
        breakdown="crop",
        where="height > 1",
    )
    wide = _grid_result(
        con,
        scheme,
        points_parquet,
        narrow=False,
        metric="sum:height,avg:height",
        breakdown="crop",
        where="height > 1",
    )
    key = CELL_COLUMN[scheme]
    written = pq.read_table(out)
    # Values, not Arrow types: the writer may re-encode geometry and widens
    # string types on the round trip. Geometry parity is pinned above, on the
    # two SQL paths themselves.
    stats = [c for c in wide.column_names if c not in ("geometry", "centroid")]
    assert [c for c in written.column_names if c not in ("geometry", "centroid")] == stats
    assert _sorted(written.select(stats), key).to_pylist() == (
        _sorted(wide.select(stats), key).to_pylist()
    )


# ---------------------------------------------------------------------------
# The admin engine narrows the same way (no Overture download: a local stand-in)
# ---------------------------------------------------------------------------


def _write_admin(path: str) -> None:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"""COPY (
            SELECT code, ST_MakeEnvelope(xmin, ymin, xmax, ymax) AS geometry
            FROM (VALUES ('AA', -25.0, -25.0, 5.0, 5.0), ('BB', 5.0, -25.0, 40.0, 30.0))
                 AS t(code, xmin, ymin, xmax, ymax)
        ) TO '{path}' (FORMAT PARQUET)"""
    )
    con.close()


def _admin_result(con, src, admin_path, *, narrow: bool, breakdown=None, where=None) -> pa.Table:
    from geoparquet_io.core.process.aggregate.by_admin import (
        _build_agg_sql,
        _build_joined_sql,
        _wrap_admin_geometry,
    )
    from geoparquet_io.core.process.aggregate.common import (
        aggregate_source_relation,
        build_breakdown_pivot,
    )
    from geoparquet_io.core.process.aggregate.grid_common import (
        bucket_point_expr,
        build_exclude_clause,
        needed_source_columns,
        source_select_list,
    )

    read_rel = aggregate_source_relation(str(src))
    metrics, _ = validate_metric_nodata("sum:height", None, None)
    pt_expr, exclude = bucket_point_expr(con, read_rel, "geometry", None, "geometry", None)
    keep = needed_source_columns(metrics, breakdown, None) if narrow else None
    select_list = (
        source_select_list(con, read_rel, keep, exclude)
        if narrow
        else f"*{build_exclude_clause(con, read_rel, exclude)}, "
    )
    joined = _build_joined_sql(
        str(src),
        pt_expr,
        f"read_parquet({sql_path(str(admin_path))})",
        "code",
        "code",
        "geometry",
        None,
        where=where,
        select_list=select_list,
    )
    breakdown_select = ""
    if breakdown:
        materialized = f"SELECT * EXCLUDE (__cen) FROM ({joined})" if narrow else joined
        con.execute(f"CREATE TEMP TABLE __agg_joined AS {materialized}")
        joined = "SELECT * FROM __agg_joined"
        breakdown_select = build_breakdown_pivot(
            con,
            joined,
            breakdown,
            20,
            metrics=metrics,
            spec=None,
            nodata_values=None,
            column_types=None,
        )
    sql = _wrap_admin_geometry(_build_agg_sql(joined, metrics, breakdown_select), "polygon")
    table = con.execute(sql).arrow().read_all()
    con.execute("DROP TABLE IF EXISTS __agg_joined")
    return table


@pytest.mark.parametrize(
    "case",
    [
        pytest.param({}, id="plain"),
        pytest.param({"breakdown": "crop"}, id="breakdown"),
        pytest.param({"where": "height > 1"}, id="where"),
        pytest.param({"breakdown": "crop", "where": "height > 1"}, id="breakdown+where"),
    ],
)
def test_admin_narrowed_output_is_identical_to_the_wide_one(con, points_parquet, tmp_path, case):
    admin_path = tmp_path / "admin.parquet"
    _write_admin(str(admin_path))
    wide = _admin_result(con, points_parquet, admin_path, narrow=False, **case)
    narrow = _admin_result(con, points_parquet, admin_path, narrow=True, **case)
    assert narrow.column_names == wide.column_names
    assert _sorted(narrow, "admin_code").equals(_sorted(wide, "admin_code"))


def test_admin_join_materialization_holds_only_the_aggregated_columns(
    con, points_parquet, tmp_path
):
    """``__agg_joined`` is one row per input feature; it must not be full width."""
    from geoparquet_io.core.process.aggregate.by_admin import _build_joined_sql
    from geoparquet_io.core.process.aggregate.common import aggregate_source_relation
    from geoparquet_io.core.process.aggregate.grid_common import (
        bucket_point_expr,
        needed_source_columns,
        source_select_list,
    )

    admin_path = tmp_path / "admin.parquet"
    _write_admin(str(admin_path))
    read_rel = aggregate_source_relation(str(points_parquet))
    metrics, _ = validate_metric_nodata("sum:height", None, None)
    pt_expr, exclude = bucket_point_expr(con, read_rel, "geometry", None, "geometry", None)
    joined = _build_joined_sql(
        str(points_parquet),
        pt_expr,
        f"read_parquet({sql_path(str(admin_path))})",
        "code",
        "code",
        "geometry",
        None,
        select_list=source_select_list(
            con, read_rel, needed_source_columns(metrics, "crop", None), exclude
        ),
    )
    con.execute(f"CREATE TEMP TABLE __agg_joined AS SELECT * EXCLUDE (__cen) FROM ({joined})")
    try:
        columns = sorted(r[0] for r in con.execute("DESCRIBE __agg_joined").fetchall())
    finally:
        con.execute("DROP TABLE IF EXISTS __agg_joined")
    assert columns == ["__admin_code", "__admin_geom", "__admin_name", "crop", "height"]


# ---------------------------------------------------------------------------
# A requested column that collides with an internal alias
# ---------------------------------------------------------------------------


def _write_points_with_reserved_name(path: str) -> None:
    """An input carrying a real column named like one of the internal aliases."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"""COPY (
            SELECT ST_Point((i % 13) * 0.5, (i % 11) * 0.5) AS geometry,
                   (i % 7) * 1.5 AS __key,
                   'crop' || (i % 4) AS crop
            FROM range(200) t(i)
        ) TO '{path}' (FORMAT PARQUET)"""
    )
    con.close()


@pytest.mark.parametrize("engine", ["grid", "admin"])
def test_a_metric_named_like_an_internal_alias_reports_cleanly(tmp_path, engine):
    """``--metric sum:__key`` must not reach DuckDB as an unbindable column.

    ``needed_source_columns`` drops a requested name that collides with an
    internal alias, so validating against the raw column set would wave the
    request through and the narrowed query would then fail to bind it. Both
    engines must refuse it up front instead (#1179 review).
    """
    from geoparquet_io.core.exceptions import InvalidParameterError

    src = tmp_path / "reserved.parquet"
    _write_points_with_reserved_name(str(src))

    with pytest.raises(InvalidParameterError, match="__key"):
        if engine == "grid":
            aggregate_by_a5(
                str(src), str(tmp_path / "out.parquet"), resolution=4, metric="sum:__key"
            )
        else:
            from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

            aggregate_by_admin(
                str(src), str(tmp_path / "out.parquet"), level="country", metric="sum:__key"
            )
