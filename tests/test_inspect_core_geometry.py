"""`gpio inspect` head/tail/meta skip the spatial extension on DuckDB >= 2.0 (#1353).

DuckDB 2.0's core ships the GEOMETRY type and the serialize/deserialize
functions the preview and metadata paths use (ST_AsText, ST_GeomFromWKB, ...),
so those read-only paths open their connections without installing or loading
spatial: faster start-up, and no failure on hosts that cannot download
extensions. On DuckDB 1.5 nothing changes: spatial is loaded exactly as before.
"""

import os

import duckdb
import pytest
from click.testing import CliRunner

import geoparquet_io.core.duckdb_utils as duckdb_utils
from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import (
    get_duckdb_connection,
    has_core_geometry,
)
from geoparquet_io.core.inspect_utils import _preview_table_from_query

DUCKDB_MAJOR = int(duckdb.__version__.split(".")[0])

requires_core_geometry = pytest.mark.skipif(
    DUCKDB_MAJOR < 2, reason="DuckDB without core GEOMETRY (needs >= 2.0)"
)
requires_no_core_geometry = pytest.mark.skipif(
    DUCKDB_MAJOR >= 2, reason="DuckDB with core GEOMETRY (needs < 2.0)"
)


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def test_file():
    return os.path.join(os.path.dirname(__file__), "data", "places_test.parquet")


@pytest.fixture
def extension_loads(monkeypatch):
    """Record every extension name `_install_and_load_extension` is asked for."""
    loaded = []
    original = duckdb_utils._install_and_load_extension

    def recording(con, name, *args, **kwargs):
        loaded.append(name)
        return original(con, name, *args, **kwargs)

    monkeypatch.setattr(duckdb_utils, "_install_and_load_extension", recording)
    return loaded


@pytest.fixture
def spatial_unavailable(monkeypatch):
    """Sabotage the install path: any attempt to load spatial raises.

    Simulates an air-gapped host where the extension cannot be downloaded.
    """
    original = duckdb_utils._install_and_load_extension

    def refusing(con, name, *args, **kwargs):
        if name == "spatial":
            raise RuntimeError("spatial extension unavailable (sabotaged for test)")
        return original(con, name, *args, **kwargs)

    monkeypatch.setattr(duckdb_utils, "_install_and_load_extension", refusing)


# --- the version gate itself -------------------------------------------------


def test_has_core_geometry_true_on_duckdb_2(monkeypatch):
    monkeypatch.setattr(duckdb, "__version__", "2.0.0")
    assert has_core_geometry() is True


def test_has_core_geometry_true_past_2(monkeypatch):
    monkeypatch.setattr(duckdb, "__version__", "2.1.3")
    assert has_core_geometry() is True


def test_has_core_geometry_false_on_duckdb_1(monkeypatch):
    monkeypatch.setattr(duckdb, "__version__", "1.5.6")
    assert has_core_geometry() is False


def test_has_core_geometry_matches_installed_duckdb():
    assert has_core_geometry() is (DUCKDB_MAJOR >= 2)


# --- inspect paths never load spatial on DuckDB >= 2.0 ------------------------


@requires_core_geometry
@pytest.mark.parametrize(
    "args",
    [
        ["inspect", "head"],
        ["inspect", "tail"],
        ["inspect", "meta"],
        ["inspect", "meta", "--geo-stats"],
    ],
    ids=["head", "tail", "meta", "meta-geo-stats"],
)
def test_inspect_skips_spatial_on_core_geometry(runner, test_file, extension_loads, args):
    result = runner.invoke(cli, [*args, test_file], catch_exceptions=False)

    assert result.exit_code == 0
    assert "spatial" not in extension_loads


@requires_core_geometry
def test_inspect_head_output_intact_without_spatial(runner, test_file, extension_loads):
    result = runner.invoke(cli, ["inspect", "head", test_file, "3"], catch_exceptions=False)

    assert result.exit_code == 0
    assert "spatial" not in extension_loads
    # Geometry still renders as WKT, which needs ST_AsText from DuckDB core.
    assert "POINT" in result.output


@requires_core_geometry
@pytest.mark.parametrize(
    "args",
    [
        ["inspect", "head"],
        ["inspect", "tail"],
        ["inspect", "meta"],
        ["inspect", "meta", "--geo-stats"],
    ],
    ids=["head", "tail", "meta", "meta-geo-stats"],
)
@pytest.mark.usefixtures("spatial_unavailable")
def test_inspect_works_with_spatial_genuinely_unavailable(runner, test_file, args):
    """The air-gap case: the spatial extension cannot be installed at all."""
    result = runner.invoke(cli, [*args, test_file], catch_exceptions=False)

    assert result.exit_code == 0


# --- DuckDB 1.5 behavior is unchanged -----------------------------------------


@requires_no_core_geometry
def test_inspect_head_still_loads_spatial_without_core_geometry(runner, test_file, extension_loads):
    result = runner.invoke(cli, ["inspect", "head", test_file], catch_exceptions=False)

    assert result.exit_code == 0
    assert "spatial" in extension_loads


# --- the on-demand fallback ----------------------------------------------------


def test_preview_query_loads_spatial_on_demand():
    """A function outside DuckDB's core set loads spatial and retries (any version)."""
    con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
    try:
        # ST_Area is not in DuckDB core on any supported version.
        table = _preview_table_from_query(
            con,
            "SELECT ST_Area('POLYGON((0 0, 1 0, 1 1, 0 0))'::GEOMETRY) AS area",
            has_spatial=False,
        )
        assert table.column("area")[0].as_py() == pytest.approx(0.5)
    finally:
        con.close()


def test_preview_query_with_spatial_reraises():
    """When spatial is already loaded a failure is real and propagates."""
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        with pytest.raises(duckdb.Error):
            _preview_table_from_query(con, "SELECT ST_NoSuchFunction(1) AS x", has_spatial=True)
    finally:
        con.close()
