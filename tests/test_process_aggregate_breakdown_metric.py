"""`--breakdown-metric`: a breakdown pivot carrying an aggregate other than COUNT (#1100).

The column name is the contract with `gpio process overview`: `detect.py`
recognises roll-up behaviour by the `sum_`/`min_`/`max_`/`count_` prefix and
drops what it does not recognise, so every pivot is named
``<func>_<col>_<value>``. The round-trip at the bottom runs a real aggregate
through a real rollup to pin that.
"""

from __future__ import annotations

import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.aggregate import by_admin
from geoparquet_io.core.process.aggregate.common import (
    MetricSpec,
    build_breakdown_column_names,
    build_breakdown_pivot,
    parse_breakdown_metric,
    parse_metrics,
    sql_literal,
    validate_agg_columns,
    validate_breakdown_metric,
    validate_metric_nodata,
)
from geoparquet_io.core.process.aggregate.grid_common import build_grid_query
from geoparquet_io.core.process.overview import rollup_table
from tests.test_process_aggregate_metric_nodata import _DUMMY_SCHEME

# --- parsing ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (None, None),
        ("", None),
        ("count", None),  # the explicit spelling of the default
        (" COUNT ", None),
        ("sum:peak", MetricSpec("sum", "peak", "sum_peak")),
        ("MAX:peak", MetricSpec("max", "peak", "max_peak")),
        ("peak", MetricSpec("sum", "peak", "sum_peak")),  # bare column is sum, as for --metric
    ],
)
def test_parse_breakdown_metric_accepts(spec, expected):
    assert parse_breakdown_metric(spec) == expected


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        ("avg:peak", "cannot be a breakdown metric"),
        ("median:peak", "Unknown metric function"),
        ("sum:", "missing a column name"),
        ("sum:peak,max:peak", "more than one metric"),
        ("count:peak", "Unknown metric function 'count'"),
    ],
)
def test_parse_breakdown_metric_rejects(spec, message):
    with pytest.raises(InvalidParameterError, match=message) as exc:
        parse_breakdown_metric(spec)
    assert "'breakdown-metric'" in str(exc.value)


def test_a_breakdown_metric_needs_a_breakdown():
    with pytest.raises(InvalidParameterError, match="needs a breakdown"):
        validate_breakdown_metric(None, MetricSpec("sum", "peak", "sum_peak"))
    validate_breakdown_metric("month", MetricSpec("sum", "peak", "sum_peak"))
    validate_breakdown_metric(None, None)


def test_nodata_no_longer_needs_a_plain_metric_when_a_breakdown_metric_is_set():
    metrics, nodata = validate_metric_nodata(None, "-999", MetricSpec("sum", "peak", "sum_peak"))
    assert metrics == [] and nodata == ["-999"]
    with pytest.raises(InvalidParameterError, match="require at least one metric"):
        validate_metric_nodata(None, "-999", None)


def test_validate_agg_columns_rejects_a_missing_breakdown_metric_column():
    with pytest.raises(InvalidParameterError, match="Breakdown metric column 'peak' not found"):
        validate_agg_columns({"month"}, [], "month", MetricSpec("sum", "peak", "sum_peak"))


# --- names -----------------------------------------------------------------


def test_a_pivot_never_lands_on_a_metric_output_name(caplog):
    """`--metric sum:peak_202605` wants the literal name the May pivot would take."""
    reserved = {m.output_name for m in parse_metrics("sum:peak_202605")}
    mapping = build_breakdown_column_names(["202605", "202606"], reserved, prefix="sum_peak")
    assert mapping == [("202605", "sum_peak_202605_2"), ("202606", "sum_peak_202606")]
    assert "already a --metric output column" in caplog.text


def test_a_long_value_is_a_bounded_column_name():
    ((value, name),) = build_breakdown_column_names(["x" * 5000], prefix="sum_peak")
    assert len(name) == len("sum_peak_") + 64


@pytest.mark.parametrize(
    ("value", "literal"),
    [
        (0.699999988079071, "CAST('0.699999988079071' AS DOUBLE)"),  # REAL categories must match
        (float("nan"), "CAST('nan' AS DOUBLE)"),
        (3, "3"),
        (True, "TRUE"),
        ("nul\x00byte", "'nul' || chr(0) || 'byte'"),
        ("O'Brien", "'O''Brien'"),
    ],
)
def test_sql_literal_spells_every_category_value(value, literal):
    assert sql_literal(value) == literal


# --- the grid engine ------------------------------------------------------

_DETECTIONS_SQL = """
SELECT * FROM (VALUES
    ('202605', 2.0), ('202605', 3.0), ('202606', 7.0), ('202607', 1.0)
) AS t(month, peak)
"""


def _grid_query(con, source=_DETECTIONS_SQL, **kwargs):
    params = {"metric": None, "breakdown": "month", "breakdown_limit": 20}
    params.update(kwargs)
    return build_grid_query(
        con,
        _DUMMY_SCHEME,
        source,
        1,
        "cell",
        params.pop("metric"),
        params.pop("breakdown"),
        params.pop("breakdown_limit"),
        "none",
        **params,
    )


def _columns(con, sql):
    return [r[0] for r in con.execute(f"DESCRIBE {sql}").fetchall()]


@pytest.mark.parametrize(
    ("spec", "row"),
    [
        ("sum:peak", (1, 4, 5.0, 7.0, 1.0)),
        ("min:peak", (1, 4, 2.0, 7.0, 1.0)),
        ("max:peak", (1, 4, 3.0, 7.0, 1.0)),
    ],
)
def test_grid_engine_pivots_the_metric_per_category(spec, row):
    con = duckdb.connect()
    sql = _grid_query(con, breakdown_spec=parse_breakdown_metric(spec))
    func = spec.split(":")[0]
    assert _columns(con, sql) == [
        "cell",
        "count",
        *(f"{func}_peak_2026{m}" for m in "05 06 07".split()),
    ]
    assert con.execute(sql).fetchone() == row


def test_grid_engine_without_a_breakdown_metric_is_unchanged():
    con = duckdb.connect()
    sql = _grid_query(con)
    assert _columns(con, sql) == ["cell", "count", "count_202605", "count_202606", "count_202607"]
    assert con.execute(sql).fetchone() == (1, 4, 2, 1, 1)


def test_grid_engine_names_the_remainder_bucket_for_the_metric():
    con = duckdb.connect()
    sql = _grid_query(con, breakdown_limit=1, breakdown_spec=parse_breakdown_metric("sum:peak"))
    assert _columns(con, sql) == ["cell", "count", "sum_peak_202605", "sum_peak_other"]
    assert con.execute(sql).fetchone() == (1, 4, 5.0, 8.0)  # the rest, summed


def test_grid_engine_keeps_a_pivot_off_a_metric_column_of_the_same_name():
    con = duckdb.connect()
    source = """
    SELECT * FROM (VALUES ('202605', 2.0, 10.0), ('202606', 7.0, 20.0))
    AS t(month, peak, peak_202605)
    """
    sql = _grid_query(
        con, source, metric="sum:peak_202605", breakdown_spec=parse_breakdown_metric("sum:peak")
    )
    assert _columns(con, sql) == [
        "cell",
        "count",
        "sum_peak_202605",  # the --metric
        "sum_peak_202605_2",  # the May pivot, not merged into it
        "sum_peak_202606",
    ]
    assert con.execute(sql).fetchone() == (1, 2, 30.0, 2.0, 7.0)


def test_the_remainder_bucket_keeps_off_a_metric_output_name_too():
    con = duckdb.connect()
    source = "SELECT * FROM (VALUES ('a', 1.0, 5.0), ('b', 2.0, 6.0)) AS t(month, peak, peak_other)"
    sql = _grid_query(
        con,
        source,
        metric="sum:peak_other",
        breakdown_limit=1,
        breakdown_spec=parse_breakdown_metric("sum:peak"),
    )
    assert _columns(con, sql) == [
        "cell",
        "count",
        "sum_peak_other",
        "sum_peak_a",
        "sum_peak_other_2",
    ]


@pytest.mark.parametrize(("func", "empty"), [("sum", None), ("min", None), ("max", None)])
def test_an_empty_or_all_null_bucket_is_null_like_the_per_cell_metric(func, empty):
    """The same cell reports `sum_peak` NULL for an all-NULL column; the pivot agrees."""
    con = duckdb.connect()
    source = "SELECT * FROM (VALUES ('a', 1.0), ('b', NULL)) AS t(month, peak)"
    sql = _grid_query(
        con, source, metric=f"{func}:peak", breakdown_spec=parse_breakdown_metric(f"{func}:peak")
    )
    cell, count, per_cell, a, b = con.execute(sql).fetchone()
    assert (per_cell, a, b) == (1.0, 1.0, empty)


def test_grid_engine_applies_nodata_sentinels_to_the_pivot():
    con = duckdb.connect()
    source = "SELECT * FROM (VALUES ('202605', 2.0), ('202605', -999.0), ('202606', 7.0)) AS t(month, peak)"
    sql = _grid_query(
        con, source, metric_nodata="-999", breakdown_spec=parse_breakdown_metric("sum:peak")
    )
    # -999 is a sentinel, not a magnitude: it must not be summed into May.
    assert con.execute(sql).fetchone() == (1, 3, 2.0, 7.0)


@pytest.mark.parametrize("spelling", ["sum:peak", "sum:PEAK"])
def test_nodata_casts_a_real_breakdown_metric_column(spelling):
    """A REAL column's sentinel needs the REAL cast (#613), whichever case the spec uses (#1104)."""
    con = duckdb.connect()
    source = (
        "SELECT * FROM (VALUES ('a', CAST(2.0 AS REAL)), ('a', CAST(-3.4028235e+38 AS REAL)), "
        "('b', CAST(4.0 AS REAL))) AS t(month, peak)"
    )
    sql = _grid_query(
        con, source, metric_nodata="-3.4028235e+38", breakdown_spec=parse_breakdown_metric(spelling)
    )
    assert con.execute(sql).fetchone() == (1, 3, 2.0, 4.0)


def test_nodata_rejects_a_non_numeric_breakdown_metric_column():
    con = duckdb.connect()
    source = "SELECT * FROM (VALUES ('a', 'x')) AS t(month, label)"
    with pytest.raises(InvalidParameterError, match="numeric"):
        _grid_query(
            con, source, metric_nodata="-999", breakdown_spec=parse_breakdown_metric("sum:label")
        )


def test_real_category_values_reach_their_pivot():
    """`str(0.7f)` is a DECIMAL literal DuckDB never equates with a REAL column (security pass)."""
    con = duckdb.connect()
    source = (
        "SELECT * FROM (VALUES (CAST(0.7 AS REAL), 8.0), (CAST(0.1 AS REAL), 1.0)) AS t(cat, peak)"
    )
    sql = _grid_query(
        con, source, breakdown="cat", breakdown_spec=parse_breakdown_metric("sum:peak")
    )
    assert con.execute(sql).fetchone() == (1, 2, 1.0, 8.0)


def test_a_blob_breakdown_column_is_refused():
    con = duckdb.connect()
    source = "SELECT * FROM (VALUES ('\\xFF\\xFE'::BLOB, 8.0)) AS t(cat, peak)"
    with pytest.raises(InvalidParameterError, match="type BLOB"):
        _grid_query(con, source, breakdown="cat")


def test_a_null_byte_in_a_category_still_matches():
    con = duckdb.connect()
    source = "SELECT * FROM (VALUES ('nul' || chr(0) || 'byte', 8.0), ('b', 1.0)) AS t(cat, peak)"
    sql = _grid_query(
        con, source, breakdown="cat", breakdown_spec=parse_breakdown_metric("sum:peak")
    )
    assert con.execute(sql).fetchone() == (1, 2, 1.0, 8.0)


def test_the_pivot_builder_is_the_one_both_engines_use():
    con = duckdb.connect()
    con.execute(f"CREATE TABLE d AS {_DETECTIONS_SQL}")
    select = build_breakdown_pivot(
        con,
        "SELECT * FROM d",
        "month",
        1,
        metrics=parse_metrics("sum:peak_other"),
        spec=parse_breakdown_metric("sum:peak"),
        nodata_values=None,
        column_types=None,
    )
    assert '"sum_peak_202605"' in select and '"sum_peak_other_2"' in select


# --- the admin engine, offline ---------------------------------------------


class _SquareAdmin:
    """A one-country admin dataset: the unit square, code 'AA'."""

    def __init__(self, path):
        self._path = path

    def get_level_column_mapping(self):
        return {"country": "country"}

    def get_geometry_column(self):
        return "geometry"

    def get_bbox_column(self):
        return None

    def configure_s3(self, con):
        pass

    def supports_per_level_sources(self):
        return True

    def get_source_for_level(self, level):
        return str(self._path)


def test_admin_engine_pivots_and_keeps_off_a_metric_name(tmp_path, monkeypatch):
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    con.execute(
        f"COPY (SELECT 'AA' AS country, ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))') AS geometry) "
        f"TO '{tmp_path / 'admin.parquet'}' (FORMAT PARQUET)"
    )
    con.execute(
        f"COPY (SELECT * FROM (VALUES ('a', 2.0, 10.0, ST_Point(0.2, 0.2)), ('a', 3.0, 20.0, ST_Point(0.3, 0.3)), "
        f"('b', 7.0, 30.0, ST_Point(0.4, 0.4))) AS t(month, peak, peak_a, geometry)) "
        f"TO '{tmp_path / 'in.parquet'}' (FORMAT PARQUET)"
    )
    con.close()
    monkeypatch.setattr(
        by_admin,
        "_setup_admin_dataset",
        lambda *a, **k: (_SquareAdmin(tmp_path / "admin.parquet"), []),
    )
    monkeypatch.setattr(by_admin, "extract_crs_from_parquet", lambda *a, **k: None)

    out = tmp_path / "out.parquet"
    by_admin.aggregate_by_admin(
        str(tmp_path / "in.parquet"),
        str(out),
        level="country",
        metric="sum:peak_a",
        breakdown="month",
        breakdown_metric="sum:peak",
        out_geometry="none",
    )
    result = pq.read_table(out)
    assert set(result.column_names) >= {"sum_peak_a", "sum_peak_a_2", "sum_peak_b"}
    (row,) = result.to_pylist()
    assert (row["count"], row["sum_peak_a"], row["sum_peak_a_2"], row["sum_peak_b"]) == (
        3,
        60.0,
        5.0,
        7.0,
    )


# --- the CLI and the overview round-trip ------------------------------------


def _points_file(path):
    table = pa.table(
        {
            "month": ["202605", "202605", "202606"],
            "peak": [2.0, 3.0, 7.0],
            "geometry": pa.array(
                [bytes.fromhex("0101000000" + ("0000000000000000" * 2))] * 3, pa.binary()
            ),
        }
    ).replace_schema_metadata(
        {
            b"geo": json.dumps(
                {
                    "version": "1.1.0",
                    "primary_column": "geometry",
                    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
                }
            ).encode()
        }
    )
    pq.write_table(table, path)
    return str(path)


def test_cli_pivots_a_metric_and_the_overview_rolls_it_up(tmp_path):
    """The whole point of the naming: a real aggregate through a real rollup."""
    src = _points_file(tmp_path / "pts.parquet")
    out = tmp_path / "agg.parquet"
    result = CliRunner().invoke(
        cli,
        [
            "process",
            "aggregate",
            "h3",
            src,
            str(out),
            "--resolution",
            "8",
            "--breakdown",
            "month",
            "--breakdown-metric",
            "sum:peak",
            "--out-geometry",
            "none",
        ],
    )
    assert result.exit_code == 0, result.output
    agg = pq.read_table(out)
    assert agg.column_names == ["h3_cell", "count", "sum_peak_202605", "sum_peak_202606"]
    assert agg.column("sum_peak_202605").to_pylist() == [5.0]

    rolled = rollup_table(agg, 3)
    assert rolled.column("sum_peak_202605").to_pylist() == [5.0]
    assert rolled.column("sum_peak_202606").to_pylist() == [7.0]


def test_cli_rejects_a_breakdown_metric_without_a_breakdown(tmp_path):
    src = _points_file(tmp_path / "pts.parquet")
    result = CliRunner().invoke(
        cli,
        [
            "process",
            "aggregate",
            "h3",
            src,
            str(tmp_path / "o.parquet"),
            "--resolution",
            "8",
            "--breakdown-metric",
            "sum:peak",
        ],
    )
    assert result.exit_code != 0
    assert "needs a breakdown" in result.output


def test_a_breakdown_limit_below_one_is_refused():
    """A negative limit used to keep all-but-one values through slice semantics."""
    con = duckdb.connect()
    with pytest.raises(InvalidParameterError, match="at least 1"):
        _grid_query(con, breakdown_limit=0)
