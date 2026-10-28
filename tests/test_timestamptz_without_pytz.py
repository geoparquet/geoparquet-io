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

    assert stats["ts"]["min"] == "2024-01-01T12:00:00"
    assert stats["ts"]["max"] == "2024-06-30T12:30:00"
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


def test_fetch_helpers_return_intervals_as_timedelta_like_fetchone():
    """Arrow hands INTERVAL back as MonthDayNano. The helpers keep fetchone's timedelta."""
    from geoparquet_io.core.duckdb_utils import fetch_row, fetch_rows

    query = (
        "SELECT * FROM (VALUES (INTERVAL 3 DAY, 1),"
        " (INTERVAL '1 month 2 days 3 seconds 5 microseconds', 2),"
        " (INTERVAL '-1 month', 3), (NULL, 4)) t(i, n)"
    )
    con = duckdb.connect()
    try:
        expected = con.execute(query).fetchall()
        assert fetch_rows(con, query) == expected
        assert fetch_row(con, query) == expected[0]
    finally:
        con.close()


def test_stats_query_failure_reports_unknown_not_zero_nulls(monkeypatch, temporal_file):
    def boom(con, query, params=None, **_):
        raise duckdb.InvalidInputException("simulated conversion failure")

    messages = []
    monkeypatch.setattr(inspect_utils, "fetch_row", boom)
    monkeypatch.setattr(inspect_utils, "debug", messages.append)

    _, stats = _stats(temporal_file)

    assert stats["ts_tz"] == {"nulls": None, "min": None, "max": None, "unique": None}
    assert any("ts_tz" in m and "simulated conversion failure" in m for m in messages)


def _interrupt_as_error():
    """A duckdb.Error that is an interrupt only through its cause chain."""
    try:
        raise duckdb.InterruptException("interrupted")
    except duckdb.InterruptException as cause:
        error = duckdb.InvalidInputException("conversion failed")
        error.__cause__ = cause
        return error


# DuckDB raises a SIGINT as InterruptException, as a plain error reading
# "Query interrupted", or wrapped in another error. All of them must propagate.
INTERRUPTS = [
    lambda: duckdb.InterruptException("interrupted"),
    lambda: duckdb.InvalidInputException("Query interrupted"),
    _interrupt_as_error,
]
INTERRUPT_IDS = ["interrupt_exception", "query_interrupted_text", "chained"]


@pytest.mark.parametrize("make_interrupt", INTERRUPTS, ids=INTERRUPT_IDS)
def test_stats_query_interrupt_is_not_swallowed(monkeypatch, temporal_file, make_interrupt):
    interrupt = make_interrupt()

    def interrupted(con, query, params=None, **_):
        raise interrupt

    columns_info, _ = _stats(temporal_file)
    monkeypatch.setattr(inspect_utils, "fetch_row", interrupted)

    with pytest.raises(type(interrupt)):
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


def test_api_table_stats_falls_back_per_column_when_batch_fails(monkeypatch, temporal_file):
    import geoparquet_io as gpio
    from geoparquet_io.core import duckdb_utils

    real_fetch_row = duckdb_utils.fetch_row
    calls = []

    def fail_batched_once(con, query, params=None, **kwargs):
        calls.append(query)
        if len(calls) == 1:
            raise duckdb.BinderException("simulated batched failure")
        return real_fetch_row(con, query, params, **kwargs)

    monkeypatch.setattr(duckdb_utils, "fetch_row", fail_batched_once)

    stats = gpio.read(temporal_file).stats()

    assert len(calls) == 1  # per-column fallback goes through inspect_utils
    assert stats["ts_tz"]["min"] == TZ_MIN
    assert stats["ts_tz"]["nulls"] == 1
    assert stats["d"]["max"] == date(2024, 6, 30)


@pytest.mark.parametrize("make_interrupt", INTERRUPTS, ids=INTERRUPT_IDS)
def test_api_table_stats_interrupt_is_not_swallowed(monkeypatch, temporal_file, make_interrupt):
    import geoparquet_io as gpio
    from geoparquet_io.core import duckdb_utils

    interrupt = make_interrupt()

    def interrupted(con, query, params=None, **_):
        raise interrupt

    table = gpio.read(temporal_file)
    monkeypatch.setattr(duckdb_utils, "fetch_row", interrupted)

    with pytest.raises(type(interrupt)):
        table.stats()


# Values Python's datetime cannot hold. fetchone() clamps infinities to
# datetime.max/min and returns BC or year 10000+ values as DuckDB's text, and
# the stats must keep doing so rather than crash.

EDGE_COLUMNS = "ts_inf, d_ninf, d_bc, d_far, tstz_inf, tstz_bc, tstz_far"
TZ_BC = "0100-01-01 (BC) 00:00:00+00"
TZ_FAR = "12000-01-01 00:00:00+00"


@pytest.fixture
def edge_file(tmp_path):
    """A GeoParquet file whose temporal columns hold infinities, BC and year 10000 values."""
    path = tmp_path / "edge.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            f"""
            COPY (
                SELECT * FROM (VALUES
                    (TIMESTAMP 'infinity', DATE '-infinity', DATE '0044-03-15 (BC)',
                     DATE '10000-01-01', TIMESTAMPTZ 'infinity', TIMESTAMPTZ '{TZ_BC}',
                     TIMESTAMPTZ '{TZ_FAR}', ST_Point(0, 0)),
                    (TIMESTAMP '2024-01-01 12:00:00', DATE '2024-01-01', DATE '2024-01-01',
                     DATE '2024-01-01', TIMESTAMPTZ '2024-01-01 12:00:00+00',
                     TIMESTAMPTZ '2024-01-01 12:00:00+00', TIMESTAMPTZ '2024-01-01 12:00:00+00',
                     ST_Point(1, 1))
                ) t({EDGE_COLUMNS}, geometry)
            ) TO {sql_path(str(path))} (FORMAT parquet)
            """
        )
    finally:
        con.close()
    return str(path)


EDGE_EXPECTED = {
    "ts_inf": (datetime(2024, 1, 1, 12, 0), datetime.max),
    "d_ninf": (date.min, date(2024, 1, 1)),
    "d_bc": ("0044-03-15 (BC)", date(2024, 1, 1)),
    "d_far": (date(2024, 1, 1), "10000-01-01"),
    "tstz_inf": (TZ_MIN, datetime.max),
    # fetchone() gives DuckDB's UTC text with its "+00", so it still reads back right.
    "tstz_bc": (TZ_BC, TZ_MIN),
    "tstz_far": (TZ_MIN, TZ_FAR),
}


def test_get_column_statistics_out_of_range_temporal_values(edge_file):
    _, stats = _stats(edge_file)

    for name, (low, high) in EDGE_EXPECTED.items():
        assert (stats[name]["min"], stats[name]["max"]) == (low, high), name
        assert stats[name]["nulls"] == 0, name


def test_cli_inspect_stats_out_of_range_temporal_values(edge_file):
    text = CliRunner().invoke(cli, ["inspect", "stats", edge_file])
    assert text.exit_code == 0, text.output
    assert "0044-03-15 (BC)" in text.output
    assert "10000-01-01" in text.output

    as_json = CliRunner().invoke(cli, ["inspect", "stats", edge_file, "--json"])
    assert as_json.exit_code == 0, as_json.output
    stats = json.loads(as_json.output)["statistics"]
    assert stats["ts_inf"]["max"] == datetime.max.isoformat()
    assert stats["d_ninf"]["min"] == date.min.isoformat()
    assert stats["d_bc"]["min"] == "0044-03-15 (BC)"
    assert stats["d_far"]["max"] == "10000-01-01"
    assert stats["tstz_inf"]["max"] == datetime.max.isoformat()
    assert stats["tstz_bc"]["min"] == TZ_BC
    assert stats["tstz_far"]["max"] == TZ_FAR


def test_api_table_stats_out_of_range_temporal_values(edge_file):
    import geoparquet_io as gpio

    stats = gpio.read(edge_file).stats()

    for name, (low, high) in EDGE_EXPECTED.items():
        assert (stats[name]["min"], stats[name]["max"]) == (low, high), name


def test_partition_preview_out_of_range_temporal_values(edge_file):
    from geoparquet_io.core.partition.common import preview_partition

    for name, values in EDGE_EXPECTED.items():
        result = preview_partition(edge_file, name)
        assert {value for value, _ in result["partitions"]} == set(values), name


def test_aggregate_breakdown_out_of_range_temporal_values(edge_file):
    from geoparquet_io.core.process.aggregate.common import resolve_breakdown_values

    con = get_duckdb_connection(load_spatial=True)
    try:
        source = f"SELECT * FROM {sql_path(edge_file)}"
        for name, values in EDGE_EXPECTED.items():
            top, has_other = resolve_breakdown_values(con, source, name, limit=2)
            assert set(top) == set(values), name
            assert has_other is False
    finally:
        con.close()


@pytest.mark.parametrize("zone", ["UTC", "America/New_York", "Africa/Cairo"])
@pytest.mark.parametrize("column", ["tstz_bc", "tstz_far"])
def test_breakdown_out_of_range_timestamptz_counts_its_rows_in_any_zone(edge_file, zone, column):
    """The breakdown spells each kept value back as a SQL literal, read in the session zone."""
    from geoparquet_io.core.process.aggregate.common import (
        resolve_breakdown_values,
        sql_literal,
    )

    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(f"SET TimeZone = '{zone}'")
        source = f"SELECT * FROM {sql_path(edge_file)}"
        top, _ = resolve_breakdown_values(con, source, column, limit=2)
        (out_of_range,) = [value for value in top if isinstance(value, str)]
        (matched,) = con.execute(
            f"SELECT COUNT(*) FROM ({source}) WHERE {column} = {sql_literal(out_of_range)}"
        ).fetchone()
        assert matched == 1
    finally:
        con.close()


@pytest.mark.parametrize("zone", ["UTC", "America/New_York", "Asia/Kolkata"])
def test_fetch_row_out_of_range_timestamptz_keeps_utc_offset(zone):
    from geoparquet_io.core.duckdb_utils import fetch_row

    con = duckdb.connect()
    try:
        con.execute(f"SET TimeZone = '{zone}'")
        row = fetch_row(con, f"SELECT TIMESTAMPTZ '{TZ_BC}', TIMESTAMPTZ '{TZ_FAR}'")
        assert row == (TZ_BC, TZ_FAR)
    finally:
        con.close()


def test_fetch_helpers_accept_a_query_ending_in_a_comment():
    from geoparquet_io.core.duckdb_utils import fetch_row, fetch_rows

    query = "SELECT TIMESTAMPTZ '2024-01-01 12:00:00+00' AS t, 1 AS n -- trailing note"
    con = duckdb.connect()
    try:
        assert fetch_rows(con, query) == [(TZ_MIN, 1)]
        assert fetch_row(con, query) == (TZ_MIN, 1)
    finally:
        con.close()


class _RecordingConnection:
    """A DuckDB connection that records each statement it runs."""

    def __init__(self, con):
        self._con = con
        self.statements = []

    def execute(self, query, params=None):
        self.statements.append(query)
        return self._con.execute(query, params)

    def __getattr__(self, name):
        return getattr(self._con, name)


def test_get_column_statistics_describes_the_file_once(monkeypatch, temporal_file):
    """One DESCRIBE per file, not one per column, keeps remote footer reads flat."""
    recorded = []
    real = inspect_utils.get_duckdb_connection

    def recording(*args, **kwargs):
        con = _RecordingConnection(real(*args, **kwargs))
        recorded.append(con)
        return con

    monkeypatch.setattr(inspect_utils, "get_duckdb_connection", recording)

    _, stats = _stats(temporal_file)

    describes = [q for con in recorded for q in con.statements if q.startswith("DESCRIBE")]
    assert len(describes) == 1
    assert stats["ts_tz"]["min"] == TZ_MIN
    assert stats["ts"]["max"] == datetime(2024, 6, 30, 12, 30)


class _FailingConnection:
    def __init__(self, error):
        self.error = error

    def execute(self, query, params=None):
        raise self.error


def test_source_types_fall_back_to_per_query_describe(monkeypatch):
    messages = []
    monkeypatch.setattr(inspect_utils, "debug", messages.append)
    con = _FailingConnection(duckdb.CatalogException("no such table"))

    assert inspect_utils._source_column_types(con, "missing") == {}
    assert any("no such table" in m for m in messages)


def test_source_types_let_an_interrupt_through():
    con = _FailingConnection(duckdb.InvalidInputException("Query interrupted"))

    with pytest.raises(duckdb.InvalidInputException):
        inspect_utils._source_column_types(con, "t")


def test_compute_column_stats_describes_when_the_type_is_unknown(temporal_file):
    con = duckdb.connect()
    try:
        stats = inspect_utils.compute_column_stats(con, "ts_tz", False, sql_path(temporal_file))
        assert (stats["min"], stats["max"], stats["nulls"]) == (TZ_MIN, TZ_MAX, 1)
    finally:
        con.close()


# Every other type comes back exactly as fetchall() gives it, also when a
# TIMESTAMPTZ column in the same row sends the query through the rewrite.

PARITY_QUERY = """
    SELECT * FROM (VALUES
        (UUID 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
         170141183460469231731687303715884105727::HUGEINT,
         340282366920938463463374607431768211455::UHUGEINT,
         '0101'::BIT, '123456789012345678901234567890'::BIGNUM,
         TIMETZ '12:34:56+05:30', INTERVAL '1 month 2 days',
         TIMESTAMP_NS '2024-01-01 12:00:01.123456789',
         MAP([1, 2], ['one', 'two']), [1, 2, 3]::INTEGER[3],
         TIMESTAMP '-infinity', DATE '0044-03-15 (BC)', 1.5::DECIMAL(4, 1)),
        (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    ) t(u, h, uh, b, big, ttz, i, tns, m, arr, ts, d, dec)
"""


def test_fetch_helpers_match_fetchall_for_other_types():
    from geoparquet_io.core.duckdb_utils import fetch_row, fetch_rows

    con = duckdb.connect()
    try:
        expected = con.execute(PARITY_QUERY).fetchall()
        assert fetch_rows(con, PARITY_QUERY) == expected
        assert fetch_row(con, PARITY_QUERY) == expected[0]

        with_tz = f"SELECT *, TIMESTAMPTZ '2024-01-01 12:00:00+00' AS tz FROM ({PARITY_QUERY})"
        assert fetch_rows(con, with_tz) == [(*row, TZ_MIN) for row in expected]
        assert fetch_row(con, with_tz) == (*expected[0], TZ_MIN)
    finally:
        con.close()


def test_admin_partition_preview_timestamptz_without_pytz():
    from geoparquet_io.core.partition.admin_hierarchical import _get_preview_partitions

    con = duckdb.connect()
    try:
        con.execute(
            "CREATE TABLE t AS SELECT * FROM (VALUES (TIMESTAMPTZ '2024-01-01 12:00:00+00'),"
            " (TIMESTAMPTZ '2024-01-01 12:00:00+00'), (NULL)) v(ts_tz)"
        )
        assert _get_preview_partitions(con, "t", ["ts_tz"], ["level"]) == [(TZ_MIN, 2)]
    finally:
        con.close()


def test_overview_scheme_detection_timestamptz_without_pytz():
    from geoparquet_io.core.process.overview.detect import _scheme_for_column

    con = duckdb.connect()
    try:
        relation = "(SELECT TIMESTAMPTZ '2024-01-01 12:00:00+00' AS cell)"
        assert _scheme_for_column(con, relation, "cell", "TIMESTAMP WITH TIME ZONE") == "admin"
    finally:
        con.close()


@pytest.mark.parametrize("zone", ["UTC", "America/New_York", "Asia/Kolkata", "Pacific/Chatham"])
def test_fetch_row_keeps_session_time_zone(zone):
    from geoparquet_io.core.duckdb_utils import fetch_row

    con = duckdb.connect()
    try:
        con.execute(f"SET TimeZone = '{zone}'")
        (value,) = fetch_row(con, "SELECT TIMESTAMPTZ '2024-01-01 12:00:00+00'")
        assert value == TZ_MIN
        assert value.tzinfo.key == zone
    finally:
        con.close()
