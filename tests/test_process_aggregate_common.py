import duckdb
import pytest

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.aggregate.common import (
    MetricSpec,
    build_breakdown_column_names,
    build_breakdown_select,
    build_metric_select,
    parse_metrics,
    resolve_breakdown_values,
    sanitize_value_for_column,
    sql_literal,
)


def test_parse_metrics_empty():
    assert parse_metrics(None) == []
    assert parse_metrics("") == []


def test_parse_metrics_func_and_bare():
    specs = parse_metrics("sum:area_ha, avg:yield, population")
    assert specs == [
        MetricSpec("sum", "area_ha", "sum_area_ha"),
        MetricSpec("avg", "yield", "avg_yield"),
        MetricSpec("sum", "population", "sum_population"),  # bare -> sum
    ]


def test_parse_metrics_rejects_unknown_func():
    with pytest.raises(InvalidParameterError):
        parse_metrics("median:area_ha")


def test_parse_metrics_rejects_missing_column():
    with pytest.raises(InvalidParameterError):
        parse_metrics("sum:")


def test_build_metric_select():
    specs = parse_metrics("sum:area_ha, avg:yield")
    sql = build_metric_select(specs)
    assert sql == 'SUM("area_ha") AS "sum_area_ha", AVG("yield") AS "avg_yield"'
    assert build_metric_select([]) == ""


def test_sanitize_value_for_column():
    assert sanitize_value_for_column("Wheat") == "wheat"
    assert sanitize_value_for_column("row crop / cereal") == "row_crop_cereal"
    assert sanitize_value_for_column("2021") == "2021"
    assert sanitize_value_for_column(None) == "null"
    assert sanitize_value_for_column("!!!") == "value"
    # Test leading/trailing underscores are stripped
    assert sanitize_value_for_column("  wheat  ") == "wheat"
    # Test with multiple consecutive special chars
    assert sanitize_value_for_column("crop---production") == "crop_production"


def test_build_breakdown_column_names_disambiguates_collisions():
    # "a/b" and "a.b" both sanitize to "a_b" -> must not merge
    mapping = build_breakdown_column_names(["a/b", "a.b"])
    names = [n for _, n in mapping]
    assert names == ["count_a_b", "count_a_b_2"]
    # Test multiple collisions
    mapping = build_breakdown_column_names(["a/b", "a.b", "a-b"])
    names = [n for _, n in mapping]
    assert names == ["count_a_b", "count_a_b_2", "count_a_b_3"]


def test_build_breakdown_column_names_respects_reserved():
    mapping = build_breakdown_column_names(["other"], reserved={"count_other"})
    assert mapping == [("other", "count_other_2")]


def test_sql_literal():
    assert sql_literal("wheat") == "'wheat'"
    assert sql_literal("O'Brien") == "'O''Brien'"
    assert sql_literal(2021) == "2021"
    assert sql_literal(3.14) == "CAST('3.14' AS DOUBLE)"
    assert sql_literal(True) == "TRUE"
    assert sql_literal(False) == "FALSE"


def _crop_con():
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE features AS
        SELECT * FROM (VALUES
            ('wheat'), ('wheat'), ('wheat'),
            ('corn'), ('corn'),
            ('rice'), ('barley'), (NULL)
        ) AS t(crop)
        """
    )
    return con


def test_resolve_breakdown_values_top_n_and_other():
    con = _crop_con()
    top, has_other = resolve_breakdown_values(con, "SELECT * FROM features", "crop", limit=2)
    assert top == ["wheat", "corn"]  # most frequent first
    assert has_other is True


def test_resolve_breakdown_values_no_other():
    con = _crop_con()
    top, has_other = resolve_breakdown_values(con, "SELECT * FROM features", "crop", limit=10)
    assert has_other is False


def test_build_breakdown_select_counts_and_other():
    con = _crop_con()
    colmap = build_breakdown_column_names(["wheat", "corn"], reserved={"count_other"})
    select = build_breakdown_select("crop", colmap, has_other=True)
    row = con.execute(f"SELECT {select} FROM features").fetchone()
    # wheat=3, corn=2, other(rice+barley+null)=3
    assert row == (3, 2, 3)
