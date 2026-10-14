"""inspect stats on TIMESTAMPTZ columns, with and without pytz.

DuckDB's Python row conversion (``fetchone``) of a TIMESTAMP WITH TIME ZONE
value imports ``pytz``. gpio does not depend on pytz, so these tests hide it
(``sys.modules['pytz'] = None``) to prove the stats path never needs it.
"""

import json
import sys
from datetime import date, datetime, timezone

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core import inspect_utils
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
from geoparquet_io.core.inspect import inspect_stats
from geoparquet_io.core.inspect_utils import (
    _create_stats_table,
    format_markdown_output,
    get_column_statistics,
)

# Noon UTC keeps the calendar date stable in any session time zone within +-12h.
TZ_MIN = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
TZ_MAX = datetime(2024, 6, 30, 12, 30, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def no_pytz(monkeypatch):
    """Make ``import pytz`` fail in every test here, as on an install without pandas 2."""
    monkeypatch.setitem(sys.modules, "pytz", None)


@pytest.fixture
def temporal_file(tmp_path):
    """A GeoParquet file with TIMESTAMPTZ, TIMESTAMP and DATE columns, one NULL row each."""
    path = tmp_path / "temporal.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            f"""
            COPY (
                SELECT * FROM (VALUES
                    (TIMESTAMPTZ '2024-01-01 12:00:00+00', TIMESTAMP '2024-01-01 12:00:00',
                     DATE '2024-01-01', ST_Point(0, 0)),
                    (TIMESTAMPTZ '2024-06-30 12:30:00+00', TIMESTAMP '2024-06-30 12:30:00',
                     DATE '2024-06-30', ST_Point(1, 1)),
                    (NULL, NULL, NULL, ST_Point(2, 2))
                ) t(ts_tz, ts, d, geometry)
            ) TO {sql_path(str(path))} (FORMAT parquet)
            """
        )
    finally:
        con.close()
    return str(path)


def _stats(path):
    result = inspect_stats(path)
    return result["columns_info"], result["statistics"]


def test_get_column_statistics_timestamptz_without_pytz(temporal_file):
    _, stats = _stats(temporal_file)

    tz = stats["ts_tz"]
    assert tz["nulls"] == 1
    assert tz["min"] == TZ_MIN
    assert tz["max"] == TZ_MAX
    assert tz["min"].tzinfo is not None
    assert tz["unique"] == 2

    assert stats["ts"] == {
        "nulls": 1,
        "min": datetime(2024, 1, 1, 12, 0),
        "max": datetime(2024, 6, 30, 12, 30),
        "unique": 2,
    }
    assert stats["d"] == {
        "nulls": 1,
        "min": date(2024, 1, 1),
        "max": date(2024, 6, 30),
        "unique": 2,
    }
    assert stats["geometry"]["nulls"] == 0


def test_cli_inspect_stats_text_timestamptz_without_pytz(temporal_file):
    result = CliRunner().invoke(cli, ["inspect", "stats", temporal_file])

    assert result.exit_code == 0, result.output
    tz_row = next(line for line in result.output.splitlines() if "ts_tz" in line and "│ 1" in line)
    assert "2024-01-01" in tz_row
    assert "2024-06-30" in tz_row
    assert "~2" in tz_row


def test_cli_inspect_stats_json_temporal_without_pytz(temporal_file):
    result = CliRunner().invoke(cli, ["inspect", "stats", temporal_file, "--json"])

    assert result.exit_code == 0, result.output
    stats = json.loads(result.output)["statistics"]

    tz = stats["ts_tz"]
    assert tz["nulls"] == 1
    assert tz["unique"] == 2
    assert datetime.fromisoformat(tz["min"]) == TZ_MIN
    assert datetime.fromisoformat(tz["max"]) == TZ_MAX

    assert stats["ts"]["min"] == "2024-01-01 12:00:00"
    assert stats["ts"]["max"] == "2024-06-30 12:30:00"
    assert stats["d"]["min"] == "2024-01-01"
    assert stats["d"]["max"] == "2024-06-30"


def test_cli_inspect_stats_markdown_timestamptz_without_pytz(temporal_file):
    result = CliRunner().invoke(cli, ["inspect", "stats", temporal_file, "--markdown"])

    assert result.exit_code == 0, result.output
    # The columns and compression tables also have a "| ts_tz |" row.
    lines = result.output.splitlines()
    stats_section = lines[lines.index("### Statistics") :]
    tz_row = next(line for line in stats_section if line.startswith("| ts_tz |"))
    assert "2024-01-01" in tz_row
    assert "2024-06-30" in tz_row


def test_api_table_stats_timestamptz_without_pytz(temporal_file):
    import geoparquet_io as gpio

    stats = gpio.read(temporal_file).stats()

    assert stats["ts_tz"]["nulls"] == 1
    assert stats["ts_tz"]["min"] == TZ_MIN
    assert stats["ts_tz"]["max"] == TZ_MAX
    assert stats["ts_tz"]["unique"] == 2


def test_fetch_row_returns_tz_aware_without_pytz():
    from geoparquet_io.core.duckdb_utils import fetch_row

    con = duckdb.connect()
    try:
        row = fetch_row(con, "SELECT TIMESTAMPTZ '2024-01-01 12:00:00+00' AS t, 1 AS n")
        assert row == (TZ_MIN, 1)
        assert fetch_row(con, "SELECT 1 AS n WHERE false") is None
    finally:
        con.close()


def test_stats_query_failure_reports_unknown_not_zero_nulls(monkeypatch, temporal_file):
    def boom(con, query, params=None):
        raise duckdb.InvalidInputException("simulated conversion failure")

    messages = []
    monkeypatch.setattr(inspect_utils, "fetch_row", boom)
    monkeypatch.setattr(inspect_utils, "debug", messages.append)

    _, stats = _stats(temporal_file)

    assert stats["ts_tz"] == {"nulls": None, "min": None, "max": None, "unique": None}
    assert any("ts_tz" in m and "simulated conversion failure" in m for m in messages)


def test_stats_query_interrupt_is_not_swallowed(monkeypatch, temporal_file):
    def interrupted(con, query, params=None):
        raise duckdb.InterruptException("interrupted")

    columns_info, _ = _stats(temporal_file)
    monkeypatch.setattr(inspect_utils, "fetch_row", interrupted)

    with pytest.raises(duckdb.InterruptException):
        get_column_statistics(temporal_file, columns_info)


def test_unknown_nulls_render_as_dash():
    columns_info = [{"name": "c", "type": "INTEGER", "is_geometry": False}]
    stats = {"c": {"nulls": None, "min": None, "max": None, "unique": None}}

    table = _create_stats_table(columns_info, stats)
    assert list(table.columns[1].cells) == ["-"]

    md = format_markdown_output(
        {"file_path": "x.parquet", "size_human": "1 B", "rows": 0, "row_groups": 0},
        {"has_geo_metadata": False, "bbox": None, "crs": None, "geometry_types": []},
        columns_info,
        None,
        None,
        stats,
    )
    assert "| c | - | - | - | - |" in md


def test_stats_table_keeps_full_timestamptz():
    columns_info = [{"name": "t", "type": "TIMESTAMP WITH TIME ZONE", "is_geometry": False}]
    stats = {"t": {"nulls": 0, "min": TZ_MIN, "max": TZ_MAX, "unique": 2}}

    table = _create_stats_table(columns_info, stats)
    assert list(table.columns[2].cells) == [str(TZ_MIN)]
    assert list(table.columns[3].cells) == [str(TZ_MAX)]


# Other paths that materialize a user column's values in Python.


def test_partition_string_preview_timestamptz_without_pytz(temporal_file):
    from geoparquet_io.core.partition.common import preview_partition

    result = preview_partition(temporal_file, "ts_tz")

    assert result["total_partitions"] == 2
    assert result["total_records"] == 2
    assert {value for value, _ in result["partitions"]} == {TZ_MIN, TZ_MAX}


def test_aggregate_breakdown_values_timestamptz_without_pytz(temporal_file):
    from geoparquet_io.core.process.aggregate.common import (
        resolve_breakdown_values,
        sql_literal,
    )

    con = get_duckdb_connection(load_spatial=True)
    try:
        source = f"SELECT * FROM {sql_path(temporal_file)}"
        top, has_other = resolve_breakdown_values(con, source, "ts_tz", limit=2)
        assert set(top) == {TZ_MIN, TZ_MAX}
        assert has_other is True  # the NULL row is the third value
        # The kept values must still select their own rows when spelled as literals.
        (matched,) = con.execute(
            f"SELECT COUNT(*) FROM ({source}) WHERE ts_tz = {sql_literal(top[0])}"
        ).fetchone()
        assert matched == 1
    finally:
        con.close()
