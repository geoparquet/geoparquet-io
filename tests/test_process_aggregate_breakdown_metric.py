"""`--breakdown-metric`: a breakdown pivot carrying an aggregate other than COUNT (#1100).

The unit tests here pin the parser, the column naming and the generated SQL.
The naming is the load-bearing part: `overview/detect.py` recognises roll-up
behaviour by prefix, so a pivot column has to land on `sum_`/`min_`/`max_` or it
is dropped from every overview level. `test_process_overview.py` holds the
round-trip that proves it actually survives.
"""

import inspect

import duckdb
import pyarrow as pa
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.aggregate.common import (
    MetricSpec,
    breakdown_prefix,
    build_breakdown_column_names,
    build_breakdown_select,
    parse_breakdown_metric,
    parse_metrics,
    validate_agg_columns,
    validate_metric_nodata,
)
from geoparquet_io.core.process.aggregate.grid_common import GridScheme, build_grid_query

# --- parsing ---------------------------------------------------------------


def test_parse_breakdown_metric_defaults_to_count():
    assert parse_breakdown_metric(None) is None
    assert parse_breakdown_metric("") is None
    # `count` is the explicit spelling of the default: no column to name.
    assert parse_breakdown_metric("count") is None
    assert parse_breakdown_metric(" COUNT ") is None


def test_parse_breakdown_metric_func_and_bare():
    assert parse_breakdown_metric("sum:peak") == MetricSpec("sum", "peak", "sum_peak")
    assert parse_breakdown_metric("max:peak") == MetricSpec("max", "peak", "max_peak")
    # Bare column defaults to sum, matching --metric.
    assert parse_breakdown_metric("peak") == MetricSpec("sum", "peak", "sum_peak")


def test_parse_breakdown_metric_rejects_avg():
    # avg_* rolls up count-weighted by the row's total count, but a bucket mean
    # needs the bucket's count -- so an avg pivot would be silently wrong at
    # every overview level rather than merely imprecise.
    with pytest.raises(InvalidParameterError) as exc:
        parse_breakdown_metric("avg:peak")
    assert "avg" in str(exc.value)
    assert "roll" in str(exc.value).lower()


def test_parse_breakdown_metric_rejects_unknown_func():
    with pytest.raises(InvalidParameterError):
        parse_breakdown_metric("median:peak")


def test_parse_breakdown_metric_rejects_missing_column():
    with pytest.raises(InvalidParameterError):
        parse_breakdown_metric("sum:")


def test_parse_breakdown_metric_rejects_a_list():
    # One breakdown, one metric: a comma list would have no defined column names.
    with pytest.raises(InvalidParameterError) as exc:
        parse_breakdown_metric("sum:peak,max:peak")
    assert "one" in str(exc.value).lower()


def test_parse_breakdown_metric_rejects_count_with_column():
    with pytest.raises(InvalidParameterError):
        parse_breakdown_metric("count:peak")


# --- naming ----------------------------------------------------------------


def test_breakdown_prefix_is_count_without_a_metric():
    assert breakdown_prefix(None) == "count"
    assert breakdown_prefix(MetricSpec("sum", "peak", "sum_peak")) == "sum_peak"


def test_build_breakdown_column_names_uses_the_metric_prefix():
    mapping = build_breakdown_column_names(["202605", "202606"], prefix="sum_peak")
    assert mapping == [("202605", "sum_peak_202605"), ("202606", "sum_peak_202606")]


def test_build_breakdown_column_names_defaults_to_count_prefix():
    # The default keeps every existing invocation byte-identical.
    mapping = build_breakdown_column_names(["wheat"])
    assert mapping == [("wheat", "count_wheat")]


def test_pivot_never_merges_with_a_plain_metric_column():
    # `--metric sum:peak_202605 --breakdown month --breakdown-metric sum:peak`
    # wants the same name for two different numbers; the reserved set splits them.
    metrics = parse_metrics("sum:peak_202605")
    reserved = {"sum_peak_other"} | {m.output_name for m in metrics}
    mapping = build_breakdown_column_names(["202605", "202606"], reserved, prefix="sum_peak")
    assert mapping == [("202605", "sum_peak_202605_2"), ("202606", "sum_peak_202606")]


# --- SQL -------------------------------------------------------------------


def _peaks_con():
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE detections AS
        SELECT * FROM (VALUES
            ('wheat', 1.0), ('wheat', 2.0), ('wheat', 3.0),
            ('corn', 10.0), ('corn', 20.0),
            ('rice', 5.0), ('barley', 5.0), (NULL, 5.0)
        ) AS t(crop, peak)
        """
    )
    return con


def test_build_breakdown_select_without_a_metric_is_unchanged():
    spec = None
    colmap = build_breakdown_column_names(["wheat"], prefix=breakdown_prefix(spec))
    select = build_breakdown_select("crop", colmap, has_other=False, spec=spec)
    assert select == 'COUNT(*) FILTER (WHERE "crop" = \'wheat\') AS "count_wheat"'


def test_build_breakdown_select_sums_per_bucket():
    con = _peaks_con()
    spec = parse_breakdown_metric("sum:peak")
    colmap = build_breakdown_column_names(
        ["wheat", "corn"], reserved={"sum_peak_other"}, prefix=breakdown_prefix(spec)
    )
    select = build_breakdown_select("crop", colmap, has_other=True, spec=spec)
    row = con.execute(f"SELECT {select} FROM detections").fetchone()
    # wheat=1+2+3, corn=10+20, other(rice+barley+null)=5+5+5
    assert row == (6.0, 30.0, 15.0)


def test_build_breakdown_select_names_the_other_bucket_for_the_metric():
    spec = parse_breakdown_metric("sum:peak")
    colmap = build_breakdown_column_names(
        ["wheat"], reserved={"sum_peak_other"}, prefix=breakdown_prefix(spec)
    )
    select = build_breakdown_select("crop", colmap, has_other=True, spec=spec)
    assert '"sum_peak_other"' in select
    assert "count_other" not in select


def test_empty_sum_bucket_is_zero_not_null():
    # A count pivot reports 0 for a bucket with no rows; a sum has to agree, or
    # the running-total pattern the pivot exists for reads NULL instead of a
    # number.
    con = _peaks_con()
    spec = parse_breakdown_metric("sum:peak")
    colmap = build_breakdown_column_names(["soy"], prefix=breakdown_prefix(spec))
    select = build_breakdown_select("crop", colmap, has_other=False, spec=spec)
    assert con.execute(f"SELECT {select} FROM detections").fetchone() == (0.0,)


def test_empty_max_bucket_stays_null():
    # There is no identity value for a maximum: NULL is the honest answer.
    con = _peaks_con()
    spec = parse_breakdown_metric("max:peak")
    colmap = build_breakdown_column_names(["soy"], prefix=breakdown_prefix(spec))
    select = build_breakdown_select("crop", colmap, has_other=False, spec=spec)
    assert con.execute(f"SELECT {select} FROM detections").fetchone() == (None,)


def test_build_breakdown_select_max_per_bucket():
    con = _peaks_con()
    spec = parse_breakdown_metric("max:peak")
    colmap = build_breakdown_column_names(["wheat", "corn"], prefix=breakdown_prefix(spec))
    select = build_breakdown_select("crop", colmap, has_other=False, spec=spec)
    assert con.execute(f"SELECT {select} FROM detections").fetchone() == (3.0, 20.0)


def test_nodata_sentinels_apply_to_the_breakdown_metric_column():
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE detections AS
        SELECT * FROM (VALUES
            ('wheat', 1.0), ('wheat', -999.0), ('wheat', 3.0)
        ) AS t(crop, peak)
        """
    )
    spec = parse_breakdown_metric("sum:peak")
    colmap = build_breakdown_column_names(["wheat"], prefix=breakdown_prefix(spec))
    select = build_breakdown_select(
        "crop",
        colmap,
        has_other=False,
        spec=spec,
        nodata_values=["-999"],
        column_types={"peak": "DOUBLE"},
    )
    assert con.execute(f"SELECT {select} FROM detections").fetchone() == (4.0,)


def test_nodata_rejects_a_non_numeric_breakdown_metric_column():
    spec = parse_breakdown_metric("max:label")
    colmap = build_breakdown_column_names(["wheat"], prefix=breakdown_prefix(spec))
    with pytest.raises(InvalidParameterError) as exc:
        build_breakdown_select(
            "crop",
            colmap,
            has_other=False,
            spec=spec,
            nodata_values=["-999"],
            column_types={"label": "VARCHAR"},
        )
    assert "label" in str(exc.value)


# --- validation ------------------------------------------------------------


def test_nodata_no_longer_needs_a_plain_metric_when_a_breakdown_metric_is_set():
    metrics, nodata = validate_metric_nodata(
        None, "-999", breakdown_metric=MetricSpec("sum", "peak", "sum_peak")
    )
    assert metrics == []
    assert nodata == ["-999"]


def test_nodata_still_needs_some_metric():
    with pytest.raises(InvalidParameterError):
        validate_metric_nodata(None, "-999")


def test_validate_agg_columns_rejects_a_missing_breakdown_metric_column():
    with pytest.raises(InvalidParameterError) as exc:
        validate_agg_columns(
            {"crop", "month"},
            metrics=[],
            breakdown="month",
            breakdown_metric=MetricSpec("sum", "peak", "sum_peak"),
        )
    assert "peak" in str(exc.value)


def test_validate_agg_columns_accepts_a_present_breakdown_metric_column():
    validate_agg_columns(
        {"crop", "month", "peak"},
        metrics=[],
        breakdown="month",
        breakdown_metric=MetricSpec("sum", "peak", "sum_peak"),
    )


def test_validate_agg_columns_rejects_a_breakdown_metric_without_a_breakdown():
    with pytest.raises(InvalidParameterError) as exc:
        validate_agg_columns(
            {"peak"},
            metrics=[],
            breakdown=None,
            breakdown_metric=MetricSpec("sum", "peak", "sum_peak"),
        )
    assert "--breakdown" in str(exc.value)


# --- the grid engine -------------------------------------------------------
#
# `_DUMMY_SCHEME` keys every row to the resolution value, so the shared grid
# engine runs end-to-end on a plain DuckDB connection: no community extension,
# no network, fast lane.

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

_DETECTIONS_SQL = """
SELECT * FROM (VALUES
    ('202605', 2.0), ('202605', 3.0), ('202606', 7.0), ('202607', 1.0)
) AS t(month, peak)
"""


def _grid_query(con, **kwargs):
    params = {
        "metric": None,
        "breakdown": "month",
        "breakdown_limit": 20,
        "out_geometry": "none",
    }
    params.update(kwargs)
    return build_grid_query(
        con,
        _DUMMY_SCHEME,
        _DETECTIONS_SQL,
        1,
        "cell",
        params.pop("metric"),
        params.pop("breakdown"),
        params.pop("breakdown_limit"),
        params.pop("out_geometry"),
        **params,
    )


def test_grid_engine_pivots_a_sum_per_category():
    con = duckdb.connect()
    sql = _grid_query(con, breakdown_metric="sum:peak")
    cols = [r[0] for r in con.execute(f"DESCRIBE {sql}").fetchall()]
    assert cols == ["cell", "count", "sum_peak_202605", "sum_peak_202606", "sum_peak_202607"]
    row = con.execute(sql).fetchone()
    # One cell: 4 rows, and the per-month sums 2+3 / 7 / 1.
    assert row == (1, 4, 5.0, 7.0, 1.0)


def test_grid_engine_without_a_breakdown_metric_is_unchanged():
    con = duckdb.connect()
    sql = _grid_query(con)
    cols = [r[0] for r in con.execute(f"DESCRIBE {sql}").fetchall()]
    assert cols == ["cell", "count", "count_202605", "count_202606", "count_202607"]


def test_grid_engine_names_the_remainder_bucket_for_the_metric():
    con = duckdb.connect()
    sql = _grid_query(con, breakdown_limit=1, breakdown_metric="sum:peak")
    cols = [r[0] for r in con.execute(f"DESCRIBE {sql}").fetchall()]
    assert cols == ["cell", "count", "sum_peak_202605", "sum_peak_other"]
    assert con.execute(sql).fetchone() == (1, 4, 5.0, 8.0)


def test_grid_engine_keeps_a_pivot_off_a_metric_column_of_the_same_name():
    con = duckdb.connect()
    # A column literally named `peak_202605` makes `--metric sum:peak_202605`
    # want the same output name as the May pivot of `--breakdown-metric sum:peak`.
    source = """
    SELECT * FROM (VALUES
        ('202605', 2.0, 100.0), ('202606', 7.0, 200.0)
    ) AS t(month, peak, peak_202605)
    """
    sql = build_grid_query(
        con,
        _DUMMY_SCHEME,
        source,
        1,
        "cell",
        "sum:peak_202605",
        "month",
        20,
        "none",
        breakdown_metric="sum:peak",
    )
    cols = [r[0] for r in con.execute(f"DESCRIBE {sql}").fetchall()]
    assert cols == [
        "cell",
        "count",
        "sum_peak_202605",  # the --metric column
        "sum_peak_202605_2",  # the May pivot, not merged into it
        "sum_peak_202606",
    ]
    # 300 is the metric (100+200); 2.0 is May's peak. Two different numbers.
    assert con.execute(sql).fetchone() == (1, 2, 300.0, 2.0, 7.0)


def test_grid_engine_rejects_a_breakdown_metric_without_a_breakdown():
    con = duckdb.connect()
    with pytest.raises(InvalidParameterError, match="--breakdown"):
        _grid_query(con, breakdown=None, breakdown_metric="sum:peak")


def test_grid_engine_rejects_a_missing_breakdown_metric_column():
    con = duckdb.connect()
    with pytest.raises(InvalidParameterError, match="nope"):
        _grid_query(con, breakdown_metric="sum:nope")


def test_grid_engine_applies_nodata_sentinels_to_the_pivot():
    con = duckdb.connect()
    source = """
    SELECT * FROM (VALUES
        ('202605', 2.0), ('202605', -999.0), ('202606', 7.0)
    ) AS t(month, peak)
    """
    sql = build_grid_query(
        con,
        _DUMMY_SCHEME,
        source,
        1,
        "cell",
        None,
        "month",
        20,
        "none",
        metric_nodata="-999",
        breakdown_metric="sum:peak",
    )
    # -999 is a sentinel, not a magnitude: it must not be summed into May.
    assert con.execute(sql).fetchone() == (1, 3, 2.0, 7.0)


# --- overviews -------------------------------------------------------------


def test_a_sum_pivot_rolls_up_through_an_overview_level():
    """The whole point of the `sum_<metric>_<value>` naming.

    `overview/detect.py` recognises roll-up behaviour by prefix and drops what it
    does not recognise, so a pivot on an unrecognised prefix would survive the
    base band and vanish from every overview -- exactly where wide time
    selections hurt most.
    """
    from geoparquet_io.core.process.overview import rollup_table

    # An admin aggregate (no geometry -> no country cache needed) carrying a
    # breakdown-metric pivot rather than counts.
    src = pa.table(
        {
            "admin_code": ["US-CA", "US-NV", "FR-IDF"],
            "count": [2, 3, 4],
            "sum_peak_202605": [1.5, 2.5, 4.0],
            "sum_peak_other": [10.0, 20.0, 30.0],
        }
    )
    result = rollup_table(src, "country").sort_by("admin_code")
    assert result.column("admin_code").to_pylist() == ["FR", "US"]
    # US is CA + NV summed; FR passes through.
    assert result.column("sum_peak_202605").to_pylist() == [4.0, 4.0]
    assert result.column("sum_peak_other").to_pylist() == [30.0, 30.0]


def test_a_max_pivot_rolls_up_as_a_max():
    from geoparquet_io.core.process.overview import rollup_table

    src = pa.table(
        {
            "admin_code": ["US-CA", "US-NV"],
            "count": [2, 3],
            "max_peak_202605": [1.5, 9.0],
        }
    )
    result = rollup_table(src, "country")
    assert result.column("max_peak_202605").to_pylist() == [9.0]


# --- the three front doors -------------------------------------------------


@pytest.mark.parametrize("subcommand", ["a5", "h3", "admin"])
def test_cli_exposes_breakdown_metric(subcommand):
    result = CliRunner().invoke(cli, ["process", "aggregate", subcommand, "--help"])
    assert result.exit_code == 0
    assert "--breakdown-metric" in result.output


@pytest.mark.parametrize("func", ["aggregate_a5", "aggregate_h3", "aggregate_admin"])
def test_ops_accept_breakdown_metric(func):
    from geoparquet_io.api import ops

    assert "breakdown_metric" in inspect.signature(getattr(ops, func)).parameters


@pytest.mark.parametrize("method", ["aggregate_a5", "aggregate_h3", "aggregate_admin"])
def test_table_methods_accept_breakdown_metric(method):
    from geoparquet_io.api.table import Table

    assert "breakdown_metric" in inspect.signature(getattr(Table, method)).parameters
