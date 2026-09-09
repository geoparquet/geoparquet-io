"""Tests for --metric-nodata sentinel handling in `gpio process aggregate` (issue #566).

Sentinel values (e.g. -999) are mapped to NULL for metric computation only, so
sum/avg/min/max ignore them while `count` still counts every feature.
"""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5
from geoparquet_io.core.process.aggregate.common import (
    build_metric_select,
    parse_metric_nodata,
    parse_metrics,
    resolve_metric_column_types,
    validate_metric_nodata,
)
from geoparquet_io.core.process.aggregate.grid_common import GridScheme, build_grid_query

# A scheme with trivial SQL templates so the shared grid engine can be exercised
# end-to-end on a plain DuckDB connection (no community extension, no network).
# key_template ignores the geometry and keys every row to the resolution value.
_DUMMY_SCHEME = GridScheme(
    name="dummy",
    extension="none",
    min_resolution=0,
    max_resolution=10,
    default_column="cell",
    key_template="{res}",
    boundary_template="ST_MakeEnvelope(0.0, 0.0, 1.0, 1.0)",
    latlng_template="{cell}",
    centroid_wkb_template="{ll}",
)


def _write_points_geoparquet(path, rows):
    """rows: list of (lon, lat, height). Writes a tiny GeoParquet of points."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(f"({lon}, {lat}, {height})" for lon, lat, height in rows)
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry, height
            FROM (VALUES {values}) AS t(lon, lat, height)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def test_parse_metric_nodata_single():
    assert parse_metric_nodata("-999") == ["-999"]


def test_parse_metric_nodata_multiple_with_whitespace():
    assert parse_metric_nodata(" -999, -9999 ,9999") == ["-999", "-9999", "9999"]


def test_parse_metric_nodata_float():
    assert parse_metric_nodata("-999.0") == ["-999.0"]


def test_parse_metric_nodata_none_passthrough():
    assert parse_metric_nodata(None) == []


def test_parse_metric_nodata_rejects_non_numeric():
    with pytest.raises(InvalidParameterError):
        parse_metric_nodata("NA")
    with pytest.raises(InvalidParameterError):
        parse_metric_nodata("-999,oops")
    with pytest.raises(InvalidParameterError):
        parse_metric_nodata("")


def test_parse_metric_nodata_rejects_non_finite():
    """inf/Infinity/overflowing values pass float() but must be rejected (P1)."""
    for bad in ("inf", "-inf", "Infinity", "-Infinity", "INF", "1e999"):
        with pytest.raises(InvalidParameterError):
            parse_metric_nodata(bad)


def test_parse_metric_nodata_rejects_non_ascii_and_underscore_digits():
    """float() accepts Unicode digits and underscores; SQL would misparse them (P1)."""
    for bad in ("٤٢", "1_000", "１２"):
        with pytest.raises(InvalidParameterError):
            parse_metric_nodata(bad)


def test_parse_metric_nodata_normalizes_nan():
    """NaN is an explicitly supported sentinel, normalized to a canonical token."""
    assert parse_metric_nodata("nan") == ["nan"]
    assert parse_metric_nodata("NaN") == ["nan"]
    assert parse_metric_nodata("-nan") == ["nan"]
    assert parse_metric_nodata("nan,-999") == ["nan", "-999"]


# ---------------------------------------------------------------------------
# SQL generation (shared by grid + admin paths)
# ---------------------------------------------------------------------------


def test_build_metric_select_single_sentinel():
    metrics = parse_metrics("avg:height,max:height")
    sql = build_metric_select(metrics, nodata_values=["-999"])
    assert sql == (
        'AVG(NULLIF("height", -999)) AS "avg_height", MAX(NULLIF("height", -999)) AS "max_height"'
    )


def test_build_metric_select_multiple_sentinels():
    metrics = parse_metrics("min:var")
    sql = build_metric_select(metrics, nodata_values=["-999", "-9999"])
    assert sql == ('MIN(CASE WHEN "var" IN (-999, -9999) THEN NULL ELSE "var" END) AS "min_var"')


def test_build_metric_select_without_nodata_unchanged():
    metrics = parse_metrics("sum:area_ha,avg:yield")
    assert build_metric_select(metrics) == (
        'SUM("area_ha") AS "sum_area_ha", AVG("yield") AS "avg_yield"'
    )


def test_metric_nodata_verified_against_duckdb():
    """The generated SQL computes metrics over non-sentinel values only."""
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1.0), (-999.0), (2.0)) v(height)")
    metrics = parse_metrics("avg:height,min:height")
    sql = build_metric_select(metrics, nodata_values=["-999"])
    row = con.execute(f"SELECT COUNT(*) AS count, {sql} FROM t").fetchone()
    assert row[0] == 3  # count unaffected
    assert row[1] == 1.5  # avg of 1.0, 2.0
    assert row[2] == 1.0  # min ignores -999
    con.close()


# ---------------------------------------------------------------------------
# Column-type-aware sentinel literals (P1 NaN, P2 REAL/float32, integer columns)
# ---------------------------------------------------------------------------


def test_resolve_metric_column_types():
    con = duckdb.connect()
    metrics = parse_metrics("avg:height,min:name")
    types = resolve_metric_column_types(
        con, "SELECT CAST(1 AS REAL) AS height, 'x' AS name", metrics
    )
    con.close()
    assert types == {"height": "FLOAT", "name": "VARCHAR"}


def test_resolve_metric_column_types_missing_column_is_lenient():
    """Unknown/missing columns resolve to no type info (error surfaces later)."""
    con = duckdb.connect()
    metrics = parse_metrics("avg:nope")
    assert resolve_metric_column_types(con, "SELECT 1 AS height", metrics) == {}
    con.close()


def test_nan_sentinel_double_column():
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES "
        "(1.0::DOUBLE), ('NaN'::DOUBLE), (3.0::DOUBLE)) v(height)"
    )
    sql = build_metric_select(
        parse_metrics("avg:height,sum:height"),
        nodata_values=parse_metric_nodata("nan"),
        column_types={"height": "DOUBLE"},
    )
    row = con.execute(f"SELECT {sql} FROM t").fetchone()
    con.close()
    assert row[0] == 2.0  # avg of 1.0, 3.0
    assert row[1] == 4.0


def test_nan_sentinel_real_column():
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES (2.0::REAL), ('NaN'::REAL), (4.0::REAL)) v(height)"
    )
    sql = build_metric_select(
        parse_metrics("avg:height"),
        nodata_values=parse_metric_nodata("nan"),
        column_types={"height": "FLOAT"},
    )
    row = con.execute(f"SELECT {sql} FROM t").fetchone()
    con.close()
    assert row[0] == 3.0


def test_nan_sentinel_without_type_info_still_matches():
    """With no schema info the NaN literal defaults to DOUBLE and still matches REAL."""
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (2.0::REAL), ('NaN'::REAL)) v(height)")
    sql = build_metric_select(parse_metrics("avg:height"), nodata_values=["nan"])
    row = con.execute(f"SELECT {sql} FROM t").fetchone()
    con.close()
    assert row[0] == 2.0


def test_real_column_scientific_sentinel_cast_to_real():
    """The classic float32 nodata (-3.4028235e+38) must match a REAL column (P2)."""
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES (CAST(1.0 AS REAL)), "
        "(CAST(-3.4028235e+38 AS REAL)), (CAST(3.0 AS REAL))) v(height)"
    )
    sql = build_metric_select(
        parse_metrics("avg:height"),
        nodata_values=["-3.4028235e+38"],
        column_types={"height": "FLOAT"},
    )
    assert "CAST(-3.4028235e+38 AS REAL)" in sql
    row = con.execute(f"SELECT {sql} FROM t").fetchone()
    con.close()
    assert row[0] == 2.0  # sentinel excluded; avg of 1.0, 3.0


def test_real_column_decimal_sentinel_multiple():
    """Decimal-form sentinels keep matching REAL columns via the CAST path."""
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES (CAST(1.5 AS REAL)), "
        "(CAST(-999.9 AS REAL)), (CAST(-9999 AS REAL)), (CAST(2.5 AS REAL))) v(height)"
    )
    sql = build_metric_select(
        parse_metrics("avg:height"),
        nodata_values=["-999.9", "-9999"],
        column_types={"height": "FLOAT"},
    )
    row = con.execute(f"SELECT {sql} FROM t").fetchone()
    con.close()
    assert row[0] == 2.0


def test_integer_column_with_int_sentinel():
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1), (-999), (3)) v(height)")
    sql = build_metric_select(
        parse_metrics("sum:height,avg:height"),
        nodata_values=["-999"],
        column_types={"height": "INTEGER"},
    )
    row = con.execute(f"SELECT {sql} FROM t").fetchone()
    con.close()
    assert row[0] == 4
    assert row[1] == 2.0


def test_integer_column_with_float_sentinel():
    """A float-form sentinel with an integral value matches int columns via promotion;
    a fractional sentinel matches nothing (no int equals -999.5) and never rounds."""
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1), (-999), (-1000), (3)) v(height)")
    metrics = parse_metrics("sum:height")
    sql = build_metric_select(metrics, nodata_values=["-999.0"], column_types={"height": "INTEGER"})
    assert con.execute(f"SELECT {sql} FROM t").fetchone()[0] == -996  # -999 nulled
    sql = build_metric_select(metrics, nodata_values=["-999.5"], column_types={"height": "INTEGER"})
    # Must NOT round to -1000 and null out a real value.
    assert con.execute(f"SELECT {sql} FROM t").fetchone()[0] == -1995
    con.close()


def test_all_sentinel_group_returns_null_not_zero():
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (-999.0), (-999.0)) v(height)")
    sql = build_metric_select(
        parse_metrics("sum:height,avg:height"),
        nodata_values=["-999"],
        column_types={"height": "DOUBLE"},
    )
    row = con.execute(f"SELECT COUNT(*), {sql} FROM t").fetchone()
    con.close()
    assert row[0] == 2  # count still counts sentinel rows
    assert row[1] is None  # SUM over all-NULL is NULL, not 0
    assert row[2] is None


def test_varchar_metric_with_nodata_raises():
    """Applying numeric sentinels to a VARCHAR metric column fails up-front (P3)."""
    with pytest.raises(InvalidParameterError, match="numeric"):
        build_metric_select(
            parse_metrics("min:name"),
            nodata_values=["-999"],
            column_types={"name": "VARCHAR"},
        )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_metric_nodata_requires_metric(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, 5.0)])
    with pytest.raises(InvalidParameterError, match="metric"):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"), resolution=5, metric_nodata="-999")


def test_admin_metric_nodata_requires_metric(tmp_path):
    """Admin path validates before any dataset setup or download."""
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, 5.0)])
    with pytest.raises(InvalidParameterError, match="metric"):
        aggregate_by_admin(
            str(src), str(tmp_path / "o.parquet"), level="country", metric_nodata="-999"
        )


def test_admin_agg_sql_wraps_metrics_with_nodata():
    """_build_agg_sql applies the sentinel wrap in the admin GROUP BY select."""
    from geoparquet_io.core.process.aggregate.by_admin import _build_agg_sql

    metrics = parse_metrics("avg:height")
    sql = _build_agg_sql("SELECT 1", metrics, "", ["-999"])
    assert 'AVG(NULLIF("height", -999)) AS "avg_height"' in sql


def test_build_grid_query_wraps_metrics_with_nodata():
    """The grid SQL builder threads metric_nodata into the metric select."""
    from geoparquet_io.core.process.aggregate.by_a5 import A5_SCHEME
    from geoparquet_io.core.process.aggregate.grid_common import build_grid_query

    con = duckdb.connect()
    sql = build_grid_query(
        con,
        A5_SCHEME,
        "SELECT 1 AS height, NULL AS __geom",
        5,
        "a5_cell",
        "avg:height",
        None,
        20,
        "none",
        metric_nodata="-999",
    )
    con.close()
    assert 'AVG(NULLIF("height", -999)) AS "avg_height"' in sql


def test_validate_metric_nodata_shared_and_flag_neutral():
    """One shared validator, worded without CLI flag spellings (P3)."""
    metrics, nodata = validate_metric_nodata("avg:height", "-999")
    assert [m.output_name for m in metrics] == ["avg_height"]
    assert nodata == ["-999"]
    assert validate_metric_nodata(None, None) == ([], [])
    with pytest.raises(InvalidParameterError, match="metric") as exc:
        validate_metric_nodata(None, "-999")
    assert "--" not in str(exc.value)


def test_table_api_metric_nodata_requires_metric_flag_neutral():
    """aggregate_a5_table validates before touching DuckDB extensions, with a
    message that does not hard-code CLI flag spellings."""
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_a5_table

    tbl = pa.table({"geometry": pa.array([b"\x00"], type=pa.binary()), "height": [1.0]})
    with pytest.raises(InvalidParameterError, match="metric") as exc:
        aggregate_a5_table(tbl, resolution=5, metric_nodata="-999")
    assert "--" not in str(exc.value)


def test_grid_validates_nodata_before_auto_resolution(tmp_path, monkeypatch):
    """Grid path must reject bad metric/nodata combos before --auto scanning,
    CRS reads, or extension installs (P3: early validation)."""
    import geoparquet_io.core.partition.auto_resolution as auto_resolution

    def _boom(*args, **kwargs):
        raise AssertionError("auto-resolution scan ran before metric-nodata validation")

    monkeypatch.setattr(auto_resolution, "calculate_auto_resolution", _boom)
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, 5.0)])
    with pytest.raises(InvalidParameterError, match="metric"):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"), auto=True, metric_nodata="-999")


def test_build_grid_query_varchar_metric_with_nodata_raises():
    """The grid builder resolves column types and rejects VARCHAR metrics up-front
    instead of failing mid-query with a conversion error (P3)."""
    con = duckdb.connect()
    with pytest.raises(InvalidParameterError, match="numeric"):
        build_grid_query(
            con,
            _DUMMY_SCHEME,
            "SELECT 'alpha' AS name, NULL AS __geom",
            5,
            "cell",
            "min:name",
            None,
            20,
            "none",
            metric_nodata="-999",
        )
    con.close()


def test_build_grid_query_real_column_sentinel_executes():
    """Grid wiring resolves the REAL column type so the sentinel actually matches."""
    con = duckdb.connect()
    source = (
        "SELECT * FROM (VALUES (CAST(2.0 AS REAL), NULL), "
        "(CAST(-3.4028235e+38 AS REAL), NULL), (CAST(4.0 AS REAL), NULL)) t(height, __geom)"
    )
    sql = build_grid_query(
        con,
        _DUMMY_SCHEME,
        source,
        3,
        "cell",
        "avg:height",
        None,
        20,
        "none",
        metric_nodata="-3.4028235e+38",
    )
    tbl = con.execute(sql).arrow().read_all()
    con.close()
    assert tbl.num_rows == 1
    assert tbl.column("count")[0].as_py() == 3
    assert tbl.column("avg_height")[0].as_py() == 3.0


def test_build_grid_query_breakdown_with_nodata():
    """Breakdown counting and nodata-wrapped metrics coexist in one grid query."""
    con = duckdb.connect()
    source = (
        "SELECT * FROM (VALUES (4.0, 'a', NULL), (6.0, 'b', NULL), "
        "(-999.0, 'a', NULL)) t(height, cat, __geom)"
    )
    sql = build_grid_query(
        con,
        _DUMMY_SCHEME,
        source,
        5,
        "cell",
        "avg:height",
        "cat",
        20,
        "none",
        metric_nodata="-999",
    )
    tbl = con.execute(sql).arrow().read_all()
    con.close()
    assert tbl.num_rows == 1
    assert tbl.column("count")[0].as_py() == 3  # sentinel row still counted
    assert tbl.column("avg_height")[0].as_py() == 5.0  # avg of 4.0, 6.0
    assert tbl.column("count_a")[0].as_py() == 2  # breakdown counts sentinel rows too
    assert tbl.column("count_b")[0].as_py() == 1


def test_admin_agg_sql_casts_real_sentinel():
    """The admin builder threads resolved column types into the sentinel literals."""
    from geoparquet_io.core.process.aggregate.by_admin import _build_agg_sql

    sql = _build_agg_sql(
        "SELECT 1", parse_metrics("avg:height"), "", ["-3.4028235e+38"], {"height": "FLOAT"}
    )
    assert "CAST(-3.4028235e+38 AS REAL)" in sql


class _FakeAdminDataset:
    """Local stand-in for an admin boundary dataset (no download, no S3)."""

    def __init__(self, admin_path):
        self._path = str(admin_path)

    def supports_per_level_sources(self):
        return True

    def get_source_for_level(self, level):
        return self._path

    def get_level_column_mapping(self):
        return {"country": "country"}

    def get_geometry_column(self):
        return "geometry"

    def get_bbox_column(self):
        return None

    def configure_s3(self, con):
        pass


@pytest.fixture
def fake_admin_dataset(tmp_path, monkeypatch):
    """Patch admin setup to a tiny local boundary file covering lon 0-20, lat 40-60."""
    admin_path = tmp_path / "admin.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"""
        COPY (
            SELECT 'AA' AS country,
                   ST_GeomFromText('POLYGON((0 40, 20 40, 20 60, 0 60, 0 40))') AS geometry
        ) TO '{admin_path}' (FORMAT PARQUET)
        """
    )
    con.close()
    monkeypatch.setattr(
        "geoparquet_io.core.process.aggregate.by_admin._setup_admin_dataset",
        lambda dataset, verbose, levels: (_FakeAdminDataset(admin_path), None),
    )


def _write_real_points(path, rows):
    """rows: list of (lon, lat, height, cat). Writes points with a REAL height column."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(f"({lon}, {lat}, {h}, '{cat}')" for lon, lat, h, cat in rows)
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry, CAST(h AS REAL) AS height, cat
            FROM (VALUES {values}) AS t(lon, lat, h, cat)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


@pytest.mark.usefixtures("fake_admin_dataset")
def test_admin_real_column_sentinel_end_to_end(tmp_path):
    """Admin path resolves REAL column types so the float32 sentinel matches."""
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    src = tmp_path / "pts.parquet"
    out = tmp_path / "out.parquet"
    _write_real_points(
        src,
        [(10.0, 50.0, 4.0, "a"), (10.1, 50.1, 6.0, "b"), (10.2, 50.2, -3.4028235e38, "a")],
    )
    aggregate_by_admin(
        str(src),
        str(out),
        level="country",
        metric="avg:height",
        metric_nodata="-3.4028235e+38",
        out_geometry="none",
    )
    df = pq.read_table(out).to_pandas()
    row = df[df["admin_code"] == "AA"].iloc[0]
    assert int(row["count"]) == 3  # sentinel row still counted
    assert float(row["avg_height"]) == 5.0  # avg of 4.0, 6.0


@pytest.mark.usefixtures("fake_admin_dataset")
def test_admin_breakdown_with_nodata_end_to_end(tmp_path):
    """Breakdown counting and nodata-wrapped metrics coexist on the admin path."""
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    src = tmp_path / "pts.parquet"
    out = tmp_path / "out.parquet"
    _write_real_points(
        src,
        [(10.0, 50.0, 4.0, "a"), (10.1, 50.1, 6.0, "b"), (10.2, 50.2, -999.0, "a")],
    )
    aggregate_by_admin(
        str(src),
        str(out),
        level="country",
        metric="avg:height",
        metric_nodata="-999",
        breakdown="cat",
        out_geometry="none",
    )
    df = pq.read_table(out).to_pandas()
    row = df[df["admin_code"] == "AA"].iloc[0]
    assert int(row["count"]) == 3
    assert float(row["avg_height"]) == 5.0
    assert int(row["count_a"]) == 2
    assert int(row["count_b"]) == 1


def test_cli_metric_nodata_requires_metric(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, 5.0)])
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "process",
            "aggregate",
            "a5",
            str(src),
            str(tmp_path / "o.parquet"),
            "--resolution",
            "5",
            "--metric-nodata",
            "-999",
        ],
    )
    assert result.exit_code != 0
    assert "metric" in result.output.lower()


# ---------------------------------------------------------------------------
# End-to-end (needs grid community extensions)
# ---------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_metric_nodata(tmp_path):
    src = tmp_path / "f.parquet"
    out = tmp_path / "o.parquet"
    # One cluster: heights 4.0, 6.0, and a -999 NoData sentinel.
    _write_points_geoparquet(
        src, [(10.0, 50.0, 4.0), (10.001, 50.001, 6.0), (10.002, 50.002, -999.0)]
    )
    aggregate_by_a5(
        str(src),
        str(out),
        resolution=5,
        metric="avg:height,min:height",
        metric_nodata="-999",
    )
    df = pq.read_table(out).to_pandas()
    assert int(df["count"].sum()) == 3  # count includes the sentinel row
    assert float(df["avg_height"].iloc[0]) == 5.0  # avg of 4.0, 6.0
    assert float(df["min_height"].iloc[0]) == 4.0  # min ignores -999


@pytest.mark.slow
@pytest.mark.network
def test_cli_aggregate_a5_metric_nodata(tmp_path):
    src = tmp_path / "f.parquet"
    out = tmp_path / "o.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, 4.0), (10.001, 50.001, -999.0)])
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
            "--metric",
            "avg:height",
            "--metric-nodata",
            "-999",
        ],
    )
    assert result.exit_code == 0, result.output
    df = pq.read_table(out).to_pandas()
    assert float(df["avg_height"].iloc[0]) == 4.0


@pytest.mark.slow
@pytest.mark.network
def test_table_aggregate_a5_metric_nodata():
    from geoparquet_io.api.table import Table

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    tbl = (
        con.execute(
            """
            SELECT ST_AsWKB(ST_Point(lon, lat)) AS geometry, height FROM (VALUES
                (10.0, 50.0, 2.0), (10.001, 50.001, -999.0)
            ) AS t(lon, lat, height)
            """
        )
        .arrow()
        .read_all()
    )
    con.close()
    result = Table(tbl).aggregate_a5(resolution=5, metric="avg:height", metric_nodata="-999")
    df = result.table.to_pandas()
    assert int(df["count"].sum()) == 2
    assert float(df["avg_height"].iloc[0]) == 2.0
