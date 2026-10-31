"""Fast-lane checks of the worker database's connection and teardown boundaries."""

import duckdb
import pytest

from geoparquet_io.core.duckdb_utils import get_duckdb_connection


def test_factory_restores_default_limits():
    settings = """SELECT current_setting('memory_limit'), current_setting('threads'),
                         current_setting('geometry_always_xy')"""
    with get_duckdb_connection.__wrapped__() as fresh:
        expected = fresh.execute(settings).fetchall()[0]
        with get_duckdb_connection() as changed:
            changed.execute("SET memory_limit = '123MB'; SET threads = 7")
            changed.execute("SET geometry_always_xy = false")
            assert changed.execute(settings).fetchall()[0][0] != expected[0]
            with get_duckdb_connection() as shared:
                assert shared.execute(settings).fetchall()[0] == expected
                assert fresh.execute(settings).fetchall()[0] == expected


def test_teardown_drops_owned_schema(worker_duckdb_database, request):
    # Exercise a complete test-fixture lifecycle within one test so xdist cannot
    # send the before/after assertions to different workers.
    fixture = request._get_active_fixturedef("_isolate_package_caches").func
    lifecycle = fixture(worker_duckdb_database, request)
    next(lifecycle)
    try:
        retained = get_duckdb_connection()
        retained.execute("CREATE TEMP TABLE temporary_probe AS SELECT 1")
        assert retained.execute("SELECT * FROM temporary_probe").fetchall()[0] == (1,)
        with get_duckdb_connection() as first:
            schema = first.execute("SELECT current_schema()").fetchall()[0][0]
            first.execute("CREATE TABLE teardown_probe AS SELECT 1 AS value")
            assert first.execute(f"SELECT * FROM {schema}.teardown_probe").fetchall()[0] == (1,)
    finally:
        with pytest.raises(StopIteration):
            next(lifecycle)
    with pytest.raises(duckdb.ConnectionException, match="Connection already closed"):
        retained.execute("SELECT * FROM temporary_probe")
    retained.close()  # Closing an already-closed cursor is harmless.
    with get_duckdb_connection() as second:
        assert second.execute(
            "SELECT count(*) FROM duckdb_schemas() WHERE schema_name = ?", [schema]
        ).fetchall()[0] == (0,)
        assert second.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE schema_name = ?", [schema]
        ).fetchall()[0] == (0,)


@pytest.mark.fresh_duckdb
def test_marker_opens_independent_database(worker_duckdb_database):
    root = worker_duckdb_database["root"]
    # Establish a root even when this test is the first scheduled on its worker.
    if root is None:
        root = get_duckdb_connection.__wrapped__()
        worker_duckdb_database["root"] = root
    root.execute("CREATE TABLE main.root_only_probe AS SELECT 1")
    try:
        with get_duckdb_connection() as independent:
            assert independent is not root
            assert independent.execute(
                "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'root_only_probe'"
            ).fetchall()[0] == (0,)
    finally:
        root.execute("DROP TABLE main.root_only_probe")
