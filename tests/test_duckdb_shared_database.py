"""Production database ownership contracts."""

import gc
import json
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from functools import partial
from threading import Barrier, Event, RLock, Thread
from unittest.mock import Mock

import duckdb
import pyarrow as pa
import pytest

from geoparquet_io.core import duckdb_utils as utils
from geoparquet_io.core.memory_limits import scoped_bounded_connection, scoped_write_memory_limit


@pytest.fixture
def production_factory():
    """Give ownership probes a fresh process root and clear it afterwards."""
    utils._reset_shared_database_for_tests()
    yield utils.get_duckdb_connection
    utils._reset_shared_database_for_tests()


@pytest.fixture
def spatial_loads(monkeypatch, production_factory):
    loads = []
    real = utils._install_and_load_extension

    def spy(con, name):
        if name == "spatial":
            loads.append(name)
        return real(con, name)

    monkeypatch.setattr(utils, "_install_and_load_extension", spy)
    return loads


def _settings(con):
    return utils.fetch_one(
        con.execute("SELECT current_setting('memory_limit'), current_setting('threads')")
    )


def test_cursor_objects_settings_and_catalog_isolation(production_factory):
    from tests.conftest import _isolate_shared_database, _shared_database_snapshot

    # Exercise teardown with both a pre-existing root and one created in the test.
    for existing in (False, True):
        utils._reset_shared_database_for_tests()
        if existing:
            with production_factory(load_spatial=False) as con:
                con.execute("CREATE TABLE retained_baseline AS SELECT 42")
        with _isolate_shared_database():
            with production_factory(load_spatial=False) as con:
                root = utils._shared_state().root
                baseline = _shared_database_snapshot(root)
                con.execute("SET threads = 1")
                assert _shared_database_snapshot(root)[1]["threads"] == 1
                con.execute(f"SET threads = {baseline[1]['threads']}")
                con.execute("CREATE TABLE teardown_marker AS SELECT 1")
                con.execute('CREATE SCHEMA "test schema"')
                con.execute('CREATE TABLE "test schema".t AS SELECT 1 AS value')
                con.execute('CREATE VIEW "test schema".v AS SELECT * FROM "test schema".t')
                con.execute('CREATE MACRO "test schema".m(x) AS x + 1')
                con.execute('CREATE MACRO "test schema".tm() AS TABLE SELECT 1')
                con.execute('CREATE SEQUENCE "test schema".s')
                con.execute("SET memory_limit = '128MB'; SET threads = 1")
                con.execute("SET preserve_insertion_order = false")
        assert utils._shared_state().root is root
        assert _shared_database_snapshot(root) == baseline

    with production_factory() as first, production_factory() as second:
        first.execute("CREATE TEMP TABLE private_table AS SELECT 1 AS value")
        first.register("private_arrow", pa.table({"value": [2]}))
        for name in ("private_table", "private_arrow"):
            with pytest.raises(duckdb.CatalogException):
                second.execute(f"SELECT * FROM {name}")
        second.execute("CREATE TEMP TABLE private_table AS SELECT 3 AS value")
        second.register("private_arrow", pa.table({"value": [4]}))
        assert utils.fetch_one(first.execute("SELECT * FROM private_arrow")) == (2,)
        assert utils.fetch_one(second.execute("SELECT * FROM private_table")) == (3,)

    with production_factory() as shared:
        shared.execute("CREATE TABLE shared_catalog_marker AS SELECT 1")
        with production_factory(isolated=True) as isolated:
            assert not isolated.sql(
                "SELECT 1 FROM duckdb_tables() WHERE table_name = 'shared_catalog_marker'"
            ).fetchall()
            isolated.execute("CREATE TABLE isolated_catalog_marker AS SELECT 2")
        with production_factory() as sibling:
            assert utils.fetch_one(sibling.execute("SELECT * FROM shared_catalog_marker")) == (1,)
            assert not sibling.sql(
                "SELECT 1 FROM duckdb_tables() WHERE table_name = 'isolated_catalog_marker'"
            ).fetchall()

    with production_factory() as first:
        first.execute("SET geometry_always_xy = false")
        with production_factory() as second:
            assert utils.fetch_one(
                second.execute("SELECT current_setting('geometry_always_xy')")
            ) == (True,)
        assert utils.fetch_one(first.execute("SELECT current_setting('geometry_always_xy')")) == (
            False,
        )


def test_overlapping_thread_scopes_do_not_cross_restore(production_factory):
    for read in (False, True):
        start = Barrier(2)
        attempted = Event()
        with production_factory() as observer:
            before = _settings(observer)

            def worker(index, start=start, attempted=attempted, read=read):
                with production_factory() as con, production_factory() as peer:
                    start.wait(timeout=5)
                    if index:
                        attempted.set()
                    scope = (
                        scoped_bounded_connection(memory_limit="256MB" if index == 0 else "1GiB")
                        if read
                        else scoped_write_memory_limit(
                            con,
                            "256MB" if index == 0 else "512MB",
                            False,
                            pinned={"threads": index + 1},
                        )
                    )
                    with scope as bounded:
                        active = bounded if read else con
                        assert attempted.wait(timeout=5)
                        assert _settings(peer) == _settings(active)
                        assert _settings(active)[1] == index + 1

            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(worker, index) for index in range(2)]
                for future in futures:
                    future.result(timeout=10)
            assert _settings(observer) == before

    with production_factory() as first, production_factory() as observer:
        before = _settings(first)
        with scoped_write_memory_limit(first, "256MB", False, pinned={"threads": 1}):
            assert _settings(first)[1] == 1
            assert _settings(observer) == _settings(first)
        assert _settings(first) == _settings(observer) == before


@pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork is unavailable on Windows")
def test_fork_child_gets_a_new_root(production_factory, monkeypatch):
    with production_factory() as parent:
        parent.execute("CREATE TABLE parent_catalog_marker AS SELECT 1")
        # A sibling must share the catalog before checking fork isolation.
        with production_factory() as sibling:
            assert utils.fetch_one(sibling.execute("SELECT * FROM parent_catalog_marker")) == (1,)
        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(read_fd)
            try:
                with production_factory() as child:
                    rows = child.sql(
                        "SELECT table_name FROM duckdb_tables() "
                        "WHERE table_name = 'parent_catalog_marker'"
                    ).fetchall()
                os.write(write_fd, b"fresh" if not rows else b"inherited")
                os._exit(0)
            except BaseException:
                os._exit(1)
        os.close(write_fd)
        try:
            report = os.read(read_fd, 32)
        finally:
            os.close(read_fd)
            _, status = os.waitpid(pid, 0)
        assert status == 0
        assert report == b"fresh"

        # Exercise the PID fallback in-process too: child coverage is not merged.
        from geoparquet_io.core import memory_limits

        previous = utils._shared_state()
        with monkeypatch.context() as case_patch:
            for name in (
                "_shared_database",
                "_shared_cursors",
                "_s3_cache_lock",
                "_spill_locks_guard",
            ):
                case_patch.setattr(utils, name, getattr(utils, name))
            case_patch.setattr(utils, "_inherited_databases", [])
            old_lock = memory_limits._settings_lock
            case_patch.setattr(memory_limits, "_settings_lock", old_lock)
            case_patch.setattr(utils.os, "getpid", lambda: previous.pid + 1)
            fresh = utils._shared_state()
            assert fresh.pid == previous.pid + 1
            assert fresh.root is None and fresh.spill is None and not fresh.loaded
            assert utils._shared_state() is fresh
            assert utils._inherited_databases == [previous]
            assert not utils._shared_cursors
            assert memory_limits._settings_lock is not old_lock
            assert utils.fetch_one(parent.execute("SELECT * FROM parent_catalog_marker")) == (1,)


def test_owner_registered_udf_survives_cursor_close(production_factory, monkeypatch, tmp_path):
    for profiled in [False, True]:
        utils._reset_shared_database_for_tests()
        with monkeypatch.context() as case_patch:
            if profiled:
                case_patch.setenv("GPIO_DUCKDB_PROFILE", str(tmp_path / "profiles"))

            def increment(value: int) -> int:
                return value + 1

            con = production_factory()
            try:
                utils.register_shared_function(con, "shared_increment", increment)
                utils.register_shared_function(con, "shared_increment", increment)
            finally:
                con.close()
            with production_factory() as sibling:
                assert utils.fetch_one(sibling.execute("SELECT shared_increment(41)")) == (42,)


def test_spill_release_belongs_to_root(monkeypatch, production_factory):
    release = Mock(wraps=utils._release_spill_lock)
    monkeypatch.setattr(utils, "_release_spill_lock", release)
    con = production_factory()
    spill = utils.fetch_one(con.execute("SELECT current_setting('temp_directory')"))[0]
    con.close()
    del con
    gc.collect()
    assert spill not in [call.args[0] for call in release.call_args_list]
    utils._reset_shared_database_for_tests()
    gc.collect()
    assert spill in [call.args[0] for call in release.call_args_list]


@pytest.fixture
def one_point_geojson(tmp_path):
    path = tmp_path / "point.geojson"
    path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"id": 1},
                        "geometry": {"type": "Point", "coordinates": [1, 2]},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_factory_and_command_chains_load_spatial_once(
    production_factory, spatial_loads, one_point_geojson, tmp_path, monkeypatch
):
    from click.testing import CliRunner

    import geoparquet_io as gpio
    from geoparquet_io.cli.main import cli
    from geoparquet_io.core import convert

    bounded = Mock(wraps=convert.scoped_bounded_connection)
    monkeypatch.setattr(convert, "scoped_bounded_connection", bounded)
    connect = Mock(wraps=duckdb.connect)
    monkeypatch.setattr(duckdb, "connect", connect)
    for _ in range(4):
        with production_factory() as con:
            utils.load_spatial_extension(con)
            assert utils.fetch_one(con.execute("SELECT ST_X(ST_Point(1, 2))")) == (1.0,)

    # A WKB write needs measured statistics, then eager conversion and fixes must
    # reuse the same root. One database is enough for this sequential local chain.
    source = tmp_path / "source.parquet"
    with production_factory() as observer:
        before = _settings(observer)
        gpio.convert(str(one_point_geojson)).write(
            str(source), geoparquet_version="1.0", compression="SNAPPY"
        )
        for index in range(2):
            out = tmp_path / f"chain_{index}.parquet"
            gpio.read(str(source)).add_bbox().write(str(out))
            gpio.convert(str(source)).add_bbox().sort_hilbert().write(str(out), overwrite=True)
            fixed = tmp_path / f"fixed_{index}.parquet"
            result = CliRunner().invoke(
                cli, ["check", "all", str(source), "--fix", "--fix-output", str(fixed)]
            )
            assert result.exit_code == 0, result.output
            assert fixed.exists()
            assert bounded.call_args.kwargs["per_thread"] == 16 * 1024**3
            assert _settings(observer) == before
    assert spatial_loads == ["spatial"]
    assert connect.call_count == 1


def test_gc_only_after_isolated_gdal_read(
    monkeypatch, production_factory, one_point_geojson, tmp_path
):
    for gdal in [False, True]:
        for isolated in [False, True]:
            for profiled in [False, True]:
                with monkeypatch.context() as case_patch:
                    import geoparquet_io as gpio
                    from geoparquet_io.core import convert

                    source = one_point_geojson
                    if not gdal:
                        source = tmp_path / "source.parquet"
                        gpio.convert(str(one_point_geojson)).write(str(source))
                    if profiled:
                        case_patch.setenv("GPIO_DUCKDB_PROFILE", str(tmp_path / "profiles"))
                    case_patch.setattr(
                        convert,
                        "scoped_bounded_connection",
                        lambda isolated=isolated, **kwargs: production_factory(
                            isolated=isolated,
                            load_spatial=kwargs["load_spatial"],
                            load_httpfs=kwargs["load_httpfs"],
                        ),
                    )
                    collect = Mock()
                    case_patch.setattr(convert.gc, "collect", collect)
                    result = gpio.convert(str(source))
                    assert result.num_rows == 1
                    assert collect.call_count == int(gdal and isolated)


def test_connection_limits_restore_on_exit_and_allow_out_of_order_close(
    production_factory, monkeypatch, tmp_path
):
    for opening in ["factory", "bounded", "scoped", "scoped_bounded"]:
        for exit_mode in ["close", "exception"]:
            from geoparquet_io.core.memory_limits import (
                open_bounded_connection,
                scoped_bounded_connection,
            )

            with production_factory() as observer:
                before = (
                    *_settings(observer),
                    utils._current_setting(observer, "preserve_insertion_order"),
                )
                lifetime = ExitStack()
                if opening == "factory":
                    con = production_factory(memory_limit="256MB", threads=1)
                elif opening == "bounded":
                    con = open_bounded_connection(memory_limit="256MB")
                elif opening == "scoped_bounded":
                    con = lifetime.enter_context(scoped_bounded_connection(memory_limit="256MB"))
                else:
                    con = lifetime.enter_context(production_factory())
                    lifetime.enter_context(scoped_write_memory_limit(con, "256MB", False))
                if opening not in ("scoped", "scoped_bounded"):
                    lifetime.enter_context(con)
                assert _settings(con)[0] != before[0]
                if opening not in ("scoped", "scoped_bounded"):
                    assert _settings(observer) == before[:2]
                if opening in ("bounded", "scoped_bounded"):
                    assert utils._current_setting(con, "preserve_insertion_order") is False
                if exit_mode == "close":
                    lifetime.close()
                else:
                    with pytest.raises(RuntimeError), lifetime:
                        raise RuntimeError("stop")
                assert (
                    *_settings(observer),
                    utils._current_setting(observer, "preserve_insertion_order"),
                ) == before

    for opening in ["factory", "bounded"]:
        from geoparquet_io.core.memory_limits import open_bounded_connection

        factory = production_factory if opening == "factory" else open_bounded_connection
        with production_factory(load_spatial=False) as observer:
            before = _settings(observer)
            first = factory(memory_limit="256MB", load_spatial=False)
            second = factory(memory_limit="512MB", load_spatial=False)
            try:
                second_limit = _settings(second)
                first.close()
                remaining_limit = _settings(second)
            finally:
                first.close()
                second.close()
            assert _settings(observer) == before
            assert remaining_limit == second_limit

    from geoparquet_io.core import admin_datasets, memory_limits

    # A setup error must close the newly opened connection and preserve the cause.
    for opening in ("factory", "bounded", "admin"):
        with monkeypatch.context() as case_patch:
            raw = production_factory(load_spatial=False, isolated=True)
            con = Mock(wraps=raw)
            failure = RuntimeError("connection setup failed")
            if opening == "factory":
                case_patch.setattr(utils, "_open_shared_cursor", Mock(return_value=con))
                case_patch.setattr(utils, "_apply_s3_settings", Mock(side_effect=failure))
                factory = partial(production_factory, load_spatial=False)
            else:
                con.execute.side_effect = failure
                case_patch.setattr(utils, "get_duckdb_connection", Mock(return_value=con))
                if opening == "bounded":
                    case_patch.setattr(
                        memory_limits, "get_duckdb_connection", Mock(return_value=con)
                    )
                    factory = memory_limits.open_bounded_connection
                else:
                    case_patch.setattr(admin_datasets, "get_cache_dir", lambda: tmp_path)
                    factory = partial(
                        admin_datasets.open_admin_join_connection, spill_to_cache=True
                    )
            with pytest.raises(RuntimeError) as caught:
                factory()
            assert caught.value is failure
            con.close.assert_called_once_with()
            with pytest.raises(duckdb.ConnectionException):
                raw.execute("SELECT 1")


def test_limited_connection_cross_thread_cleanup_does_not_strand_settings_lock(
    production_factory, monkeypatch
):
    for opening in ["factory", "bounded", "threads"]:
        for cleanup in ["close", "gc"]:
            with monkeypatch.context() as case_patch:
                _assert_cross_thread_cleanup(production_factory, case_patch, opening, cleanup)


def _assert_cross_thread_cleanup(production_factory, monkeypatch, opening, cleanup):
    from geoparquet_io.core import memory_limits

    lock = RLock()
    monkeypatch.setattr(memory_limits, "_settings_lock", lock)
    if opening == "bounded":
        con = memory_limits.open_bounded_connection(memory_limit="256MB", load_spatial=False)
    else:
        kwargs = {"threads": 1} if opening == "threads" else {"memory_limit": "256MB"}
        con = production_factory(load_spatial=False, **kwargs)
    pending = [con]
    if cleanup == "gc":
        cycle = [con]
        cycle.append(cycle)
        pending = [cycle]
        del cycle
    del con
    errors = []

    def dispose():
        try:
            if cleanup == "close":
                pending.pop().close()
            else:
                pending.clear()
                gc.collect()
        except Exception as exc:
            errors.append(exc)

    disposer = Thread(target=dispose, daemon=True)
    disposer.start()
    disposer.join(timeout=5)
    assert not disposer.is_alive()
    wrote = Event()

    def write():
        with production_factory(load_spatial=False) as writer:
            with scoped_write_memory_limit(writer, "256MB", False):
                wrote.set()

    writer = Thread(target=write, daemon=True)
    writer.start()
    writer.join(timeout=2)
    completed = not writer.is_alive()
    # Repair a stranded lock so the failing regression can exit cleanly.
    while lock._is_owned():
        lock.release()
    writer.join(timeout=5)
    assert completed, "cross-thread cleanup stranded the settings lock"
    assert wrote.is_set()
    assert not errors


def test_remote_crs_and_validator_reuse_database(production_factory, monkeypatch):
    from pathlib import Path

    from geoparquet_io.core.crs_utils import crs_of
    from geoparquet_io.core.validate import validate_geoparquet
    from tests._fake_s3 import fake_s3

    source = Path(__file__).parent / "data" / "buildings_test.parquet"
    connections = Mock(wraps=duckdb.connect)
    loads = Mock(wraps=utils._install_and_load_extension)
    monkeypatch.setattr(duckdb, "connect", connections)
    monkeypatch.setattr(utils, "_install_and_load_extension", loads)

    with fake_s3({"bucket/input.parquet": source.read_bytes()}) as (endpoint, _):
        url = f"http://{endpoint}/bucket/input.parquet"
        expected_crs = crs_of(url)
        expected_validation = validate_geoparquet(str(source))
        initial_connections = connections.call_count
        initial_loads = loads.call_count
        assert initial_connections == 1
        for _ in range(5):
            assert crs_of(url) == expected_crs
            assert validate_geoparquet(str(source)) == expected_validation
        assert connections.call_count == initial_connections
        assert loads.call_count == initial_loads
