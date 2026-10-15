"""GeoJSON datetimes keep their UTC offset through convert (#1152).

DuckDB spatial 1.5 bundles GDAL 3.8.5, which stores the local clock time of a
GeoJSON datetime as UTC whenever a column has one offset throughout. These
tests assert epochs, never wall clock, and run under two session time zones.
"""

import json
import sys

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.geojson_datetimes import DATE_AS_STRING, plan_datetime_read

# 2026-01-01T00:00:00 at +01:00, Z and -05:00.
PLUS_ONE = 1767222000
UTC = 1767225600
MINUS_FIVE = 1767243600


@pytest.fixture(autouse=True)
def no_pytz(monkeypatch):
    """gpio does not depend on pytz; the read must not need it either."""
    monkeypatch.setitem(sys.modules, "pytz", None)


@pytest.fixture(params=["UTC", "Asia/Tokyo"])
def session_tz(request, monkeypatch):
    """Run under more than one time zone: the epochs must not move."""
    monkeypatch.setenv("TZ", request.param)
    return request.param


def _geojson(path, rows):
    features = [
        {
            "type": "Feature",
            "properties": props,
            "geometry": {"type": "Point", "coordinates": [index, index]},
        }
        for index, props in enumerate(rows)
    ]
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))
    return str(path)


@pytest.fixture
def uniform(tmp_path):
    """One offset per column, the shape that triggers the GDAL bug."""
    return _geojson(
        tmp_path / "uniform.geojson",
        [
            {
                "ts": "2026-01-01T00:00:00+01:00",
                "utc": "2026-01-01T00:00:00Z",
                "neg": "2026-01-01T00:00:00-05:00",
            }
        ],
    )


def _epochs(table, column):
    values = table.column(column).cast("int64").to_pylist()
    unit = table.schema.field(column).type.unit
    scale = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}[unit]
    return [None if v is None else v // scale for v in values]


def _cli_convert(src, tmp_path):
    out = str(tmp_path / "out.parquet")
    result = CliRunner().invoke(cli, ["convert", "geoparquet", src, out])
    assert result.exit_code == 0, result.output
    return pq.read_table(out)


@pytest.mark.usefixtures("session_tz")
def test_python_api_keeps_offsets(uniform):
    table = gpio.convert(uniform).to_arrow()
    assert _epochs(table, "ts") == [PLUS_ONE]
    assert _epochs(table, "utc") == [UTC]
    assert _epochs(table, "neg") == [MINUS_FIVE]


@pytest.mark.usefixtures("session_tz")
def test_cli_keeps_offsets(uniform, tmp_path):
    table = _cli_convert(uniform, tmp_path)
    assert _epochs(table, "ts") == [PLUS_ONE]
    assert _epochs(table, "utc") == [UTC]
    assert _epochs(table, "neg") == [MINUS_FIVE]
    assert table.schema.field("ts").type.tz is not None


@pytest.mark.usefixtures("session_tz")
def test_mixed_offsets_in_one_column(tmp_path):
    src = _geojson(
        tmp_path / "mixed.geojson",
        [
            {"ts": "2026-01-01T00:00:00+01:00"},
            {"ts": "2026-01-01T00:00:00Z"},
            {"ts": "2026-01-01T00:00:00-05:00"},
            {"ts": None},
        ],
    )
    table = _cli_convert(src, tmp_path)
    assert sorted(_epochs(table, "ts"), key=lambda v: (v is None, v)) == [
        PLUS_ONE,
        UTC,
        MINUS_FIVE,
        None,
    ]


@pytest.mark.usefixtures("session_tz")
def test_date_time_and_naive_columns_keep_their_types(tmp_path):
    src = _geojson(
        tmp_path / "dates.geojson",
        [
            {"d": "2026-01-01", "t": "12:34:56", "naive": "2026-01-01T00:00:00", "s": "x"},
            {"d": None, "t": "01:02:03", "naive": "2026-01-02T00:00:00", "s": "y"},
        ],
    )
    table = _cli_convert(src, tmp_path).sort_by("s")
    schema = table.schema
    assert str(schema.field("d").type) == "date32[day]"
    assert str(schema.field("t").type).startswith("time64")
    assert schema.field("naive").type.tz is None
    assert schema.field("s").type == "string"
    assert [str(v) for v in table.column("d").to_pylist()] == ["2026-01-01", "None"]
    assert [str(v) for v in table.column("t").to_pylist()] == ["12:34:56", "01:02:03"]
    assert _epochs(table, "naive") == [UTC, UTC + 86400]


def test_no_datetime_columns(tmp_path):
    src = _geojson(tmp_path / "plain.geojson", [{"name": "a", "n": 1}])
    table = _cli_convert(src, tmp_path)
    assert table.column("name").to_pylist() == ["a"]
    assert table.column("n").to_pylist() == [1]


class TestPlan:
    def test_not_geojson_is_unchanged(self):
        plan = plan_datetime_read(False, ["ENCODING=CP1252"], [("d", "DATE")])
        assert plan.open_options == ["ENCODING=CP1252"]
        assert plan.wrap("src") == "src"

    def test_no_temporal_column_is_unchanged(self):
        plan = plan_datetime_read(True, None, [("name", "VARCHAR")])
        assert plan.open_options is None
        assert plan.wrap("src") == "src"

    def test_existing_option_is_respected(self):
        plan = plan_datetime_read(True, ["date_as_string=NO"], [("d", "DATE")])
        assert plan.open_options == ["date_as_string=NO"]
        assert plan.casts == ()

    def test_merges_open_options_and_quotes_names(self):
        plan = plan_datetime_read(
            True, ["ENCODING=UTF-8"], [('we"ird', "TIMESTAMP WITH TIME ZONE"), ("n", "BIGINT")]
        )
        assert plan.open_options == ["ENCODING=UTF-8", DATE_AS_STRING]
        assert plan.wrap("src") == (
            '(SELECT * REPLACE (CAST("we""ird" AS TIMESTAMP WITH TIME ZONE) AS "we""ird") FROM src)'
        )
