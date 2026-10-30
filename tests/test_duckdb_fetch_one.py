"""``fetch_one`` reads one row from an ``execute()`` result and leaves no result open.

On DuckDB 2.0 a result read with a bare ``fetchone()`` stays open until a fetch
returns no rows, and while it is open ``con.sql()``, ``con.register()``,
``create_function`` and every relation read on that connection raise
``ResourceInUseException``. A one-row query (a count, a ``LIMIT 1``, a
``current_setting``) is exactly the case that hits it: the single ``fetchone()``
gets the row and never sees the end. On 1.5 the next statement silently
replaced the result, so these tests pass trivially there; the DuckDB 2.0 CI leg
is the one that holds them.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from geoparquet_io.core.duckdb_utils import fetch_one, get_duckdb_connection
from tests.test_duckdb_capabilities import pretend_duckdb_capabilities


@pytest.fixture
def con():
    connection = get_duckdb_connection(load_spatial=False, load_httpfs=False)
    yield connection
    connection.close()


def test_returns_the_row(con):
    assert fetch_one(con.execute("SELECT 42, 'x'")) == (42, "x")


def test_returns_none_for_an_empty_result(con):
    assert fetch_one(con.execute("SELECT 1 WHERE false")) is None


def test_binds_params(con):
    assert fetch_one(con.execute("SELECT ? + 1", [41])) == (42,)


@pytest.mark.parametrize(
    "query",
    ["SELECT count(*) FROM range(10)", "SELECT * FROM range(5)"],
    ids=["one-row", "many-rows"],
)
def test_the_connection_is_free_for_a_relation_afterwards(con, query):
    fetch_one(con.execute(query))
    assert con.sql("SELECT 7").fetchall() == [(7,)]


def test_the_connection_is_free_for_register_afterwards(con):
    fetch_one(con.execute("SELECT 1"))
    con.register("t", pa.table({"a": [1, 2]}))
    assert fetch_one(con.execute("SELECT sum(a) FROM t")) == (3,)


def test_the_remaining_rows_are_run_not_cancelled(con):
    """The rest is completed, not aborted, so a write statement keeps its effect."""
    con.execute("CREATE TABLE src AS SELECT * FROM range(3) t(i)")
    con.execute("CREATE TABLE dst (i BIGINT)")
    fetch_one(con.execute("INSERT INTO dst SELECT i FROM src RETURNING i"))
    assert con.sql("SELECT count(*) FROM dst").fetchall() == [(3,)]


class _Result:
    """An execute() result stand-in, with or without 2.0's ``complete()``."""

    def __init__(self, rows, has_complete):
        self._rows = list(rows)
        self.completed = 0
        if has_complete:
            self.complete = self._complete

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def _complete(self):
        self.completed += 1


@pytest.mark.parametrize("has_complete", [True, False], ids=["duckdb-2.0", "duckdb-1.5"])
def test_completes_the_result_only_where_duckdb_can(has_complete):
    result = _Result([(1,), (2,)], has_complete)
    with pretend_duckdb_capabilities(result_complete=has_complete):
        assert fetch_one(result) == (1,)
    assert getattr(result, "completed", 0) == (1 if has_complete else 0)


def test_an_exhausted_result_is_not_completed_again():
    result = _Result([], has_complete=True)
    assert fetch_one(result) is None
    assert result.completed == 0
