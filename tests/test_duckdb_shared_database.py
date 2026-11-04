"""Production database ownership contracts."""

import gc
import json
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from threading import Barrier, Event, RLock, Thread
from unittest.mock import Mock

import duckdb
import pyarrow as pa
import pytest

from geoparquet_io.core import duckdb_utils as utils
from geoparquet_io.core.memory_limits import scoped_write_memory_limit


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


def test_factory_loads_spatial_once(production_factory, spatial_loads):
    for _ in range(4):
        with production_factory() as con:
            utils.load_spatial_extension(con)
            assert utils.fetch_one(con.execute("SELECT ST_X(ST_Point(1, 2))")) == (1.0,)
    assert spatial_loads == ["spatial"]


def test_temp_tables_and_registered_names_are_cursor_local(production_factory):
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


def test_write_scope_restores_database_settings(production_factory):
    with production_factory() as first, production_factory() as observer:
        before = _settings(first)
        with scoped_write_memory_limit(first, "256MB", False, pinned={"threads": 1}):
            assert _settings(first)[1] == 1
            assert _settings(observer) == _settings(first)
        assert _settings(first) == _settings(observer) == before


def test_overlapping_thread_scopes_do_not_cross_restore(production_factory):
    start = Barrier(2)
    attempted = Event()
    with production_factory() as observer:
        before = _settings(observer)

        def worker(index):
            with production_factory() as con, production_factory() as peer:
                start.wait(timeout=5)
                if index:
                    attempted.set()
                with scoped_write_memory_limit(
                    con,
                    "256MB" if index == 0 else "512MB",
                    False,
                    pinned={"threads": index + 1},
                ):
                    assert attempted.wait(timeout=5)
                    assert _settings(peer) == _settings(con)
                    assert _settings(con)[1] == index + 1

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(worker, index) for index in range(2)]
            for future in futures:
                future.result(timeout=10)
        assert _settings(observer) == before


@pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork is unavailable on Windows")
def test_fork_child_gets_a_new_root(production_factory):
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


@pytest.mark.parametrize("profiled", [False, True])
def test_owner_registered_udf_survives_cursor_close(
    production_factory, monkeypatch, tmp_path, profiled
):
    if profiled:
        monkeypatch.setenv("GPIO_DUCKDB_PROFILE", str(tmp_path / "profiles"))

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


def test_isolated_database_has_independent_catalog(production_factory):
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


def test_geometry_always_xy_on_each_cursor(production_factory):
    with production_factory() as first:
        first.execute("SET geometry_always_xy = false")
        with production_factory() as second:
            assert utils.fetch_one(
                second.execute("SELECT current_setting('geometry_always_xy')")
            ) == (True,)
        assert utils.fetch_one(first.execute("SELECT current_setting('geometry_always_xy')")) == (
            False,
        )


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
        )
    )
    return path


def test_two_api_chains_load_spatial_once(
    production_factory, spatial_loads, one_point_geojson, tmp_path
):
    import geoparquet_io as gpio

    for index in range(2):
        out = tmp_path / f"chain_{index}.parquet"
        gpio.convert(str(one_point_geojson)).add_bbox().sort_hilbert().write(str(out))
        assert out.exists()
    assert spatial_loads == ["spatial"]


@pytest.mark.parametrize("gdal", [False, True], ids=["parquet", "gdal"])
@pytest.mark.parametrize("isolated", [False, True], ids=["shared", "isolated"])
@pytest.mark.parametrize("profiled", [False, True], ids=["plain", "profiled"])
def test_gc_only_after_isolated_gdal_read(
    monkeypatch, production_factory, one_point_geojson, tmp_path, gdal, isolated, profiled
):
    import geoparquet_io as gpio
    from geoparquet_io.core import convert

    source = one_point_geojson
    if not gdal:
        source = tmp_path / "source.parquet"
        gpio.convert(str(one_point_geojson)).write(str(source))
    if profiled:
        monkeypatch.setenv("GPIO_DUCKDB_PROFILE", str(tmp_path / "profiles"))
    monkeypatch.setattr(
        convert,
        "get_duckdb_connection",
        lambda **kwargs: production_factory(isolated=isolated, **kwargs),
    )
    collect = Mock()
    monkeypatch.setattr(convert.gc, "collect", collect)
    result = gpio.convert(str(source))
    assert result.num_rows == 1
    assert collect.call_count == int(gdal and isolated)


@pytest.mark.parametrize("opening", ["factory", "bounded", "scoped"])
@pytest.mark.parametrize("exit_mode", ["close", "exception"])
def test_connection_limits_isolate_or_restore_on_exit(production_factory, opening, exit_mode):
    from geoparquet_io.core.memory_limits import open_bounded_connection

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
        else:
            con = lifetime.enter_context(production_factory())
            lifetime.enter_context(scoped_write_memory_limit(con, "256MB", False))
        if opening != "scoped":
            lifetime.enter_context(con)
        assert _settings(con)[0] != before[0]
        if opening != "scoped":
            assert _settings(observer) == before[:2]
        if opening == "bounded":
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


@pytest.mark.parametrize("opening", ["factory", "bounded", "threads"])
@pytest.mark.parametrize("cleanup", ["close", "gc"])
def test_limited_connection_cross_thread_cleanup_does_not_strand_settings_lock(
    production_factory, monkeypatch, opening, cleanup
):
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


@pytest.mark.parametrize("opening", ["factory", "bounded"])
def test_limited_connections_closed_out_of_order_leave_shared_defaults(production_factory, opening):
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
