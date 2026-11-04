"""Production database ownership contracts, bypassing the temporary pytest shim."""

import gc
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from unittest.mock import Mock

import duckdb
import pyarrow as pa
import pytest

from geoparquet_io.core import duckdb_utils as utils
from geoparquet_io.core.memory_limits import scoped_write_memory_limit


@pytest.fixture
def production_factory(monkeypatch):
    """Unwrap only the conftest prototype; fresh_duckdb will mean isolated later."""
    factory = utils.get_duckdb_connection
    real = getattr(factory, "__wrapped__", factory)
    for module in tuple(sys.modules.values()):
        if module and module.__name__.startswith("geoparquet_io."):
            if getattr(module, "get_duckdb_connection", None) is factory:
                monkeypatch.setattr(module, "get_duckdb_connection", real)
    reset = getattr(utils, "_reset_shared_database_for_tests", None)
    if reset:
        reset()
    yield real
    if reset:
        reset()


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


def test_owner_registered_udf_survives_cursor_close(production_factory):
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
def test_gc_only_after_gdal_read(
    monkeypatch, production_factory, one_point_geojson, tmp_path, gdal
):
    import geoparquet_io as gpio
    from geoparquet_io.core import convert

    source = one_point_geojson
    if not gdal:
        source = tmp_path / "source.parquet"
        gpio.convert(str(one_point_geojson)).write(str(source))
    collect = Mock()
    monkeypatch.setattr(convert.gc, "collect", collect)
    result = gpio.convert(str(source))
    assert result.num_rows == 1
    assert collect.call_count == int(gdal)
