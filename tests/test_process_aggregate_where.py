"""Tests for --where row filtering on `gpio process aggregate` (issue #568).

The clause must apply to the source scan feeding keying, metrics, breakdowns,
and --auto resolution sizing, mirroring `gpio extract --where` semantics.
"""

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import ValidationError
from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5
from geoparquet_io.core.process.aggregate.by_h3 import aggregate_by_h3


def _write_points_geoparquet(path, rows):
    """rows: list of (lon, lat, crop, area). Writes a tiny GeoParquet of points."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(f"({lon}, {lat}, '{crop}', {area})" for lon, lat, crop, area in rows)
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry, crop, area
            FROM (VALUES {values}) AS t(lon, lat, crop, area)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


FOUR_POINTS = [
    (10.00, 50.00, "wheat", 4.0),
    (10.001, 50.001, "wheat", 6.0),
    (10.002, 50.002, "corn", 2.0),
    (-120.0, 40.0, "wheat", 1.0),
]


def _points_arrow_table():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    tbl = (
        con.execute("SELECT ST_AsWKB(ST_Point(10.0, 50.0)) AS geometry, 'wheat' AS crop")
        .arrow()
        .read_all()
    )
    con.close()
    return tbl


# ---------------------------------------------------------------------------
# SQL construction units (no grid extension, no network)
# ---------------------------------------------------------------------------


def test_read_grid_source_sql_appends_where(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import read_grid_source_sql

    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        sql = read_grid_source_sql(con, str(src), "geometry", where="area > 3")
        # The clause is closed on its own line so a trailing `--` comment in it
        # cannot swallow the paren (#612).
        assert "WHERE (area > 3\n)" in sql
        count = con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]
        assert count == 2  # areas 4.0 and 6.0
        # No clause -> unchanged behavior
        sql_all = read_grid_source_sql(con, str(src), "geometry")
        assert "WHERE" not in sql_all
    finally:
        con.close()


def test_build_joined_sql_injects_where_into_inner_scan():
    from geoparquet_io.core.process.aggregate.by_admin import _build_joined_sql

    sql = _build_joined_sql(
        "in.parquet",
        "geometry",
        "read_parquet('admin.parquet')",
        "country",
        "country",
        "geometry",
        where="\"crop:name\" = 'wheat'",
    )
    inner = sql.split(") s")[0]  # the input-scan subquery
    assert "WHERE (\"crop:name\" = 'wheat'\n)" in inner


def test_where_validation_rejects_dangerous_keywords(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS[:1])
    with pytest.raises(ValidationError, match="dangerous"):
        aggregate_by_a5(
            str(src), str(tmp_path / "o.parquet"), resolution=5, where="1=1; DROP TABLE x"
        )


def test_cli_where_dangerous_keywords_fail_cleanly(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS[:1])
    runner = CliRunner()
    for sub, extra in (
        ("a5", ["--resolution", "5"]),
        ("h3", ["--resolution", "5"]),
        ("admin", ["--level", "country"]),
    ):
        result = runner.invoke(
            cli,
            [
                "process",
                "aggregate",
                sub,
                str(src),
                str(tmp_path / f"o_{sub}.parquet"),
                *extra,
                "--where",
                "DROP TABLE x",
            ],
        )
        assert result.exit_code != 0, f"{sub} accepted a dangerous clause"
        assert "dangerous" in result.output.lower(), f"{sub}: {result.output}"


def test_admin_where_validation_rejects_dangerous_keywords(tmp_path):
    """Core admin path validates the clause before any dataset setup."""
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS[:1])
    with pytest.raises(ValidationError, match="dangerous"):
        aggregate_by_admin(
            str(src), str(tmp_path / "o.parquet"), level="country", where="DELETE FROM x"
        )


def test_table_where_validation_rejects_dangerous_keywords():
    """In-memory table path validates the clause before touching DuckDB."""
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_a5_table

    tbl = _points_arrow_table()
    with pytest.raises(ValidationError, match="dangerous"):
        aggregate_a5_table(tbl, resolution=5, where="TRUNCATE x")


# ---------------------------------------------------------------------------
# --auto integration: the filter must drive resolution sizing
# ---------------------------------------------------------------------------


def test_auto_row_count_respects_where(tmp_path):
    from geoparquet_io.core.partition.auto_resolution import _get_total_row_count

    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS)
    assert _get_total_row_count(str(src)) == 4
    assert _get_total_row_count(str(src), where="area > 3") == 2


def test_aggregate_auto_threads_where_to_auto_resolution(tmp_path, monkeypatch):
    """aggregate --auto must size the grid on the *filtered* row count (#568)."""
    import geoparquet_io.core.partition.auto_resolution as auto_res

    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS)
    captured = {}

    def fake_calc(input_parquet, spatial_index_type, **kwargs):
        captured.update(kwargs)
        raise RuntimeError("stop-after-capture")

    monkeypatch.setattr(auto_res, "calculate_auto_resolution", fake_calc)
    with pytest.raises(RuntimeError, match="stop-after-capture"):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"), auto=True, where="area > 3")
    assert captured.get("where") == "area > 3"


def test_count_query_nests_where_in_subquery():
    """The clause must never trail a top-level statement (#612 injection)."""
    from geoparquet_io.core.partition.auto_resolution import _count_query

    sql = _count_query("f.parquet", "area > 3")
    assert "FROM (SELECT 1 FROM 'f.parquet'" in sql
    assert sql.rstrip().endswith("AS __filtered")
    assert _count_query("f.parquet", None) == "SELECT COUNT(*) FROM 'f.parquet'"


def test_auto_resolution_rejects_statement_separator(tmp_path):
    """A ``;`` payload must be refused before any SQL runs (#612)."""
    from geoparquet_io.core.partition.auto_resolution import calculate_auto_resolution

    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS)
    pwned = tmp_path / "pwned.csv"
    payload = f"1=1); COPY (SELECT 42 AS x) TO '{pwned}'; SELECT COUNT(*) FROM '{src}' WHERE (1=1"
    with pytest.raises(ValidationError):
        calculate_auto_resolution(str(src), "quadkey", target_rows_per_partition=1, where=payload)
    assert not pwned.exists()


def test_cli_auto_where_injection_writes_no_file(tmp_path):
    """End-to-end: --auto --where cannot be used to run a second statement (#612)."""
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS)
    pwned = tmp_path / "pwned_cli.csv"
    payload = f"1=1); COPY (SELECT 42 AS x) TO '{pwned}'; SELECT COUNT(*) FROM '{src}' WHERE (1=1"
    result = CliRunner().invoke(
        cli,
        [
            "process",
            "aggregate",
            "a5",
            str(src),
            str(tmp_path / "o.parquet"),
            "--auto",
            "--where",
            payload,
        ],
    )
    assert result.exit_code != 0, result.output
    assert not pwned.exists()


def test_auto_empty_filter_result_names_the_filter(tmp_path):
    """An empty filter result must blame the filter, not the file (#612)."""
    from geoparquet_io.core.partition.auto_resolution import calculate_auto_resolution

    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS)
    with pytest.raises(ValueError, match="--where"):
        calculate_auto_resolution(
            str(src), "quadkey", target_rows_per_partition=1, where="area > 1000"
        )


def test_probe_distinct_cell_counts_respects_where(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.partition.auto_resolution import (
        _probe_distinct_cell_counts,
        _register_quadkey_udf,
    )

    src = tmp_path / "f.parquet"
    # Two far-apart points -> 2 distinct quadkeys; filter selects one.
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 4.0), (-120.0, 40.0, "corn", 1.0)])
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        _register_quadkey_udf(con)
        counts_all = _probe_distinct_cell_counts(con, str(src), "quadkey", "geometry", "", [5])
        counts_filtered = _probe_distinct_cell_counts(
            con, str(src), "quadkey", "geometry", "", [5], where="crop = 'wheat'"
        )
        assert counts_all == [2]
        assert counts_filtered == [1]
    finally:
        con.close()


def test_probe_filters_beneath_the_sample(tmp_path):
    """The sample must be drawn from the *filtered* rows, not filtered afterwards.

    DuckDB puts the FILTER above RESERVOIR_SAMPLE when the clause sits next to
    ``USING SAMPLE``, so a selective filter left only a fraction of the sample
    and --auto over-resolved (#612).
    """
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.partition.auto_resolution import (
        _probe_distinct_cell_counts,
        _register_quadkey_udf,
    )

    src = tmp_path / "f.parquet"
    # 100 wheat rows at 100 distinct locations + 900 corn rows stacked on one spot.
    rows = [(-170.0 + 3.0 * i, 10.0 + (i % 20), "wheat", 1.0) for i in range(100)]
    rows += [(10.0, 50.0, "corn", 1.0)] * 900
    _write_points_geoparquet(src, rows)

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        _register_quadkey_udf(con)
        counts = _probe_distinct_cell_counts(
            con,
            str(src),
            "quadkey",
            "geometry",
            " USING SAMPLE 200 ROWS",
            [8],
            where="crop = 'wheat'",
        )
    finally:
        con.close()
    # All 100 matching rows fit the 200-row budget, so every distinct cell is seen.
    assert counts == [100]


def test_read_grid_source_sql_tolerates_trailing_sql_comment(tmp_path):
    """A trailing ``--`` comment must not swallow what follows the clause (#612)."""
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import read_grid_source_sql

    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        sql = read_grid_source_sql(con, str(src), "geometry", where="area > 3 -- big only")
        assert con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0] == 2
    finally:
        con.close()


def test_where_error_message_names_the_where_option():
    """An invalid clause must point at --where, not dump the generated SQL (#612)."""
    import duckdb as _duckdb

    from geoparquet_io.cli.commands.process import _aggregate_error

    exc = _duckdb.Error(
        'Binder Error: Referenced column "yr" not found in FROM clause!\n'
        "LINE 1: SELECT * EXCLUDE (...) FROM read_parquet('x.parquet') WHERE (yr = 2025)"
    )
    err = _aggregate_error(exc, "yr = 2025")
    assert "--where" in str(err)
    assert "yr = 2025" in str(err)
    assert "LINE 1" not in str(err)
    # Non-DuckDB errors pass through untouched.
    assert str(_aggregate_error(ValueError("plain"), "yr = 2025")) == "plain"


# ---------------------------------------------------------------------------
# Hive-partitioned input: --where must be able to filter on partition columns
# ---------------------------------------------------------------------------


def _write_hive_points(root):
    """Write year=2024/ and year=2025/ partitions of two points each."""
    for year, rows in (
        (2024, [(10.0, 50.0, "wheat", 1.0), (10.001, 50.001, "corn", 2.0)]),
        (2025, [(10.002, 50.002, "wheat", 3.0)]),
    ):
        part = root / f"year={year}"
        part.mkdir(parents=True, exist_ok=True)
        _write_points_geoparquet(part / "data.parquet", rows)
    return str(root / "**" / "*.parquet")


def test_read_grid_source_sql_can_filter_on_hive_partition_column(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import read_grid_source_sql

    glob = _write_hive_points(tmp_path / "hive")
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        sql = read_grid_source_sql(con, glob, "geometry", where="year = 2025")
        assert con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0] == 1
        sql_all = read_grid_source_sql(con, glob, "geometry")
        assert con.execute(f"SELECT COUNT(*) FROM ({sql_all})").fetchone()[0] == 3
    finally:
        con.close()


def test_grid_aggregation_output_has_no_hive_partition_column(tmp_path):
    """Enabling hive partitioning must not leak partition columns into the output."""
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import (
        GridScheme,
        build_grid_query,
        read_grid_source_sql,
    )

    # A scheme keyed by plain SQL so the test needs no community extension.
    scheme = GridScheme(
        name="test",
        extension="",
        min_resolution=0,
        max_resolution=10,
        default_column="test_cell",
        key_template="CAST(floor(ST_X({pt}) * {res}) AS VARCHAR)",
        boundary_template="{cell}",
        latlng_template="{cell}",
        poly_wkb_template="ST_AsWKB(ST_Point(0, 0))",
        centroid_wkb_template="ST_AsWKB(ST_Point(0, 0))",
    )
    glob = _write_hive_points(tmp_path / "hive")
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        source_sql = read_grid_source_sql(con, glob, "geometry")
        sql = build_grid_query(con, scheme, source_sql, 1, "test_cell", None, "crop", 20, "polygon")
        cols = set(con.execute(sql).arrow().read_all().column_names)
        assert "year" not in cols
        assert {"test_cell", "count", "geometry"} <= cols
    finally:
        con.close()


def test_admin_joined_sql_can_filter_on_hive_partition_column(tmp_path):
    """The admin input scan exposes hive partition columns to --where too."""
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.by_admin import _build_joined_sql

    glob = _write_hive_points(tmp_path / "hive")
    admin = tmp_path / "admin.parquet"
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.execute(
            f"""
            COPY (SELECT 'XX' AS country,
                         ST_GeomFromText('POLYGON((0 0, 20 0, 20 60, 0 60, 0 0))') AS geometry)
            TO '{admin}' (FORMAT PARQUET)
            """
        )
        sql = _build_joined_sql(
            glob,
            "geometry",
            f"read_parquet('{admin}')",
            "country",
            "country",
            "geometry",
            where="year = 2025",
        )
        assert con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0] == 1
    finally:
        con.close()


# ---------------------------------------------------------------------------
# End-to-end per scheme (needs grid community extensions)
# ---------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_where_filters_rows(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS)
    out_all = tmp_path / "all.parquet"
    out_filtered = tmp_path / "filtered.parquet"
    aggregate_by_a5(str(src), str(out_all), resolution=5)
    aggregate_by_a5(str(src), str(out_filtered), resolution=5, where="crop = 'wheat'")
    total_all = sum(pq.read_table(out_all).column("count").to_pylist())
    total_filtered = sum(pq.read_table(out_filtered).column("count").to_pylist())
    assert total_all == 4
    assert total_filtered == 3  # three wheat rows


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_where_with_breakdown_and_metric(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS)
    out = tmp_path / "o.parquet"
    aggregate_by_a5(
        str(src),
        str(out),
        resolution=5,
        metric="sum:area",
        breakdown="crop",
        where="area > 1",
    )
    df = pq.read_table(out).to_pandas()
    # The (-120, 40) wheat row (area=1) is filtered out entirely.
    assert int(df["count"].sum()) == 3
    assert float(df["sum_area"].sum()) == 12.0
    assert int(df["count_wheat"].sum()) == 2
    assert int(df["count_corn"].sum()) == 1


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_h3_where_filters_rows(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS)
    out = tmp_path / "o.parquet"
    aggregate_by_h3(str(src), str(out), resolution=5, where="crop = 'corn'")
    assert sum(pq.read_table(out).column("count").to_pylist()) == 1


@pytest.mark.slow
@pytest.mark.network
def test_cli_aggregate_a5_where_special_char_column(tmp_path):
    src = tmp_path / "f.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry, crop AS "crop:name"
            FROM (VALUES (10.0, 50.0, 'wheat'), (10.001, 50.001, 'corn')) AS t(lon, lat, crop)
        ) TO '{src}' (FORMAT PARQUET)
        """
    )
    con.close()
    out = tmp_path / "o.parquet"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "process",
            "aggregate",
            "a5",
            str(src),
            str(out),
            "--resolution",
            "5",
            "--where",
            "\"crop:name\" = 'wheat'",
        ],
    )
    assert result.exit_code == 0, result.output
    assert sum(pq.read_table(out).column("count").to_pylist()) == 1


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_admin_where_filters_rows(tmp_path):
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    src = tmp_path / "pts.parquet"
    out = tmp_path / "by_country.parquet"
    # Two points in France (one wheat, one corn), one ocean wheat point.
    _write_points_geoparquet(
        src,
        [(2.35, 48.85, "wheat", 1.0), (4.85, 45.75, "corn", 2.0), (-30.0, 0.0, "wheat", 3.0)],
    )
    aggregate_by_admin(str(src), str(out), level="country", where="crop = 'wheat'")
    df = pq.read_table(out).to_pandas()
    assert int(df["count"].sum()) == 2  # corn row filtered out


@pytest.mark.slow
@pytest.mark.network
def test_table_aggregate_a5_where():
    from geoparquet_io.api.table import Table

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    tbl = (
        con.execute(
            """
            SELECT ST_AsWKB(ST_Point(lon, lat)) AS geometry, crop FROM (VALUES
                (10.0, 50.0, 'wheat'), (10.001, 50.001, 'corn')
            ) AS t(lon, lat, crop)
            """
        )
        .arrow()
        .read_all()
    )
    con.close()
    result = Table(tbl).aggregate_a5(resolution=5, where="crop = 'wheat'")
    assert sum(result.table.column("count").to_pylist()) == 1
