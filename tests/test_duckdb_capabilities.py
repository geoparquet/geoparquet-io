"""One owner for the DuckDB 1.5 / 2.0 differences (#1367, concepts 10 and 20).

``duckdb_utils.duckdb_capabilities()`` is the only place gpio reads the DuckDB
version or probes for a version-dependent feature; every call site asks it.
Tests that need another version use :func:`pretend_duckdb_version` (or
:func:`pretend_duckdb_capabilities` for one field). Both patch the owner alone --
the capabilities it caches -- and leave ``duckdb.__version__`` real, so a call site
that reads the version for itself instead of asking the owner fails its test.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest

import geoparquet_io.core.duckdb_utils as duckdb_utils
from geoparquet_io.core import geojson_stream
from geoparquet_io.core.benchmark import get_environment_info
from geoparquet_io.core.duckdb_utils import (
    DuckDBCapabilities,
    complete_without_fetch,
    duckdb_capabilities,
    fetch_one,
    get_duckdb_connection,
)

REPO = Path(__file__).resolve().parents[1]
PACKAGE = REPO / "geoparquet_io"
OWNER = PACKAGE / "core" / "duckdb_utils.py"

V2_FEATURES = ("is_v2", "core_geometry", "native_geojson_writer", "a5_from_query_farm")


@contextmanager
def _owner_reports(caps: DuckDBCapabilities) -> Iterator[DuckDBCapabilities]:
    """Make ``duckdb_capabilities()`` return ``caps`` for the duration of the block.

    Replaces what the owner caches (its ``_capabilities_for`` step) and nothing
    else: ``duckdb.__version__`` stays the installed one. Clears the cache after
    patching and again after restoring, so neither the fake nor a stale real
    value leaks into another test.
    """
    original = duckdb_utils._capabilities_for
    duckdb_utils._capabilities_for = lambda version: caps
    duckdb_capabilities.cache_clear()
    try:
        yield duckdb_capabilities()
    finally:
        duckdb_utils._capabilities_for = original
        duckdb_capabilities.cache_clear()


@contextmanager
def pretend_duckdb_version(version: str) -> Iterator[DuckDBCapabilities]:
    """Make the owner report what ``version`` implies, for the duration of the block."""
    with _owner_reports(duckdb_utils._capabilities_for(version)) as caps:
        yield caps


@contextmanager
def pretend_duckdb_capabilities(**fields: object) -> Iterator[DuckDBCapabilities]:
    """The installed capabilities with ``fields`` replaced, e.g. ``native_geojson_writer=True``."""
    with _owner_reports(dataclasses.replace(duckdb_capabilities(), **fields)) as caps:
        yield caps


# --- version parsing -----------------------------------------------------------


@pytest.mark.parametrize(
    ("version", "major", "minor"),
    [
        ("1.5.6", 1, 5),
        ("2.0.0", 2, 0),
        ("2", 2, None),
        ("2.0.0-dev123", 2, 0),
        ("2.1.0-dev123", 2, 1),
        ("v2.0.0", 2, 0),
        ("10.0.0", 10, 0),
        ("unknown", None, None),
        ("", None, None),
    ],
)
def test_version_parsing(version, major, minor):
    with pretend_duckdb_version(version) as caps:
        assert caps.version == version
        assert (caps.major, caps.minor) == (major, minor)
        expected_v2 = major is not None and major >= 2
        assert all(getattr(caps, name) is expected_v2 for name in V2_FEATURES)


def test_unknown_version_is_conservative():
    """An unreadable version keeps every 2.0-only path off."""
    with pretend_duckdb_version("not-a-version") as caps:
        assert caps.major is None and caps.minor is None
        assert not any(getattr(caps, name) for name in V2_FEATURES)


def test_capabilities_are_frozen():
    with pytest.raises(dataclasses.FrozenInstanceError):
        duckdb_capabilities().is_v2 = True  # type: ignore[misc]


def test_installed_version_matches_the_library():
    caps = duckdb_capabilities()
    assert caps.version == duckdb.__version__
    assert caps.major == int(duckdb.__version__.split(".")[0])


# --- the cache and its reset hook ------------------------------------------------


def test_capabilities_are_cached():
    assert duckdb_capabilities() is duckdb_capabilities()


def test_cache_clear_rereads_the_version():
    real = duckdb_capabilities()
    with pretend_duckdb_version("9.9.9") as fake:
        assert fake.major == 9
        assert duckdb_capabilities() is fake
    assert duckdb_capabilities().version == real.version


def test_pretending_leaves_the_library_version_alone():
    """Only the owner is patched, so a direct ``duckdb.__version__`` read is not fooled."""
    real = duckdb.__version__
    with pretend_duckdb_version("9.9.9") as caps:
        assert caps.version == "9.9.9"
        assert duckdb.__version__ == real
    with pretend_duckdb_capabilities(native_geojson_writer=True) as caps:
        assert caps.native_geojson_writer is True
        assert caps.version == real


def test_without_cache_clear_a_patched_version_is_not_seen(monkeypatch):
    """The cache is real: patching the version alone changes nothing."""
    before = duckdb_capabilities()
    monkeypatch.setattr(duckdb, "__version__", "9.9.9")
    assert duckdb_capabilities() is before


# --- complete_without_fetch -------------------------------------------------------


def test_complete_without_fetch_leaves_the_connection_usable():
    """After closing an open multi-row result, the connection takes relation reads."""
    con = duckdb.connect()
    try:
        result = con.execute("SELECT * FROM range(100000)")
        complete_without_fetch(result)
        assert con.sql("SELECT 41 + 1").fetchall() == [(42,)]
        con.register("t", con.sql("SELECT 1 AS a"))
        assert fetch_one(con.execute("SELECT count(*) FROM t")) == (1,)
    finally:
        con.close()


def test_complete_without_fetch_keeps_a_write(tmp_path):
    """complete() runs the statement to the end: a COPY read for nothing still writes."""
    out = tmp_path / "out.parquet"
    con = duckdb.connect()
    try:
        result = con.execute(f"COPY (SELECT 1 AS a) TO '{out}' (FORMAT PARQUET)")
        complete_without_fetch(result)
        assert con.sql(f"SELECT a FROM read_parquet('{out}')").fetchall() == [(1,)]
    finally:
        con.close()


@pytest.mark.parametrize("has_complete", [True, False], ids=["duckdb-2.0", "duckdb-1.5"])
def test_complete_without_fetch_never_fetches(has_complete):
    """complete() is called where the result has it, and no row is ever fetched."""
    calls: list[str] = []

    class Result:
        def fetchone(self):
            calls.append("fetchone")

    result = Result()
    if has_complete:
        result.complete = lambda: calls.append("complete")
    complete_without_fetch(result)
    assert calls == (["complete"] if has_complete else [])


@pytest.mark.parametrize("version", ["1.5.6", "2.0.0"])
def test_fetch_one_reads_a_sqlite_cursor_on_every_client(version):
    """GeoPackage metadata is read with ``fetch_one`` on a sqlite3 cursor (no ``complete()``)."""
    import sqlite3

    con = sqlite3.connect(":memory:")
    try:
        with pretend_duckdb_version(version):
            assert fetch_one(con.execute("SELECT 1, 'a'")) == (1, "a")
    finally:
        con.close()


# --- routed call sites read the owner ---------------------------------------------


@pytest.fixture
def extension_loads(monkeypatch):
    loaded: list[str] = []
    original = duckdb_utils._install_and_load_extension

    def recording(con, name, *args, **kwargs):
        loaded.append(name)
        return original(con, name, *args, **kwargs)

    monkeypatch.setattr(duckdb_utils, "_install_and_load_extension", recording)
    return loaded


@pytest.mark.parametrize(("version", "loads_spatial"), [("1.5.6", True), ("2.0.0", False)])
def test_auto_spatial_reads_core_geometry(extension_loads, version, loads_spatial):
    """get_duckdb_connection(load_spatial=None) follows ``core_geometry``."""
    with pretend_duckdb_version(version):
        get_duckdb_connection(load_spatial=None, load_httpfs=False).close()
    assert ("spatial" in extension_loads) is loads_spatial


@pytest.mark.parametrize(("version", "expected"), [("1.5.6", False), ("2.0.0", True)])
def test_geojson_gate_reads_the_owner(monkeypatch, tmp_path, version, expected):
    """``gpio convert geojson`` to a file tries the native writer exactly where the owner says.

    The real native writer runs (wrapped, not replaced); on an engine without
    ``FORMAT GEOJSON`` it declines and the hand-built writer finishes the file.
    """
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    tried: list[bool] = []
    original = geojson_stream._native_feature_collection_to_file

    def recording(*args, **kwargs):
        tried.append(True)
        return original(*args, **kwargs)

    monkeypatch.setattr(geojson_stream, "_native_feature_collection_to_file", recording)
    sample = str(REPO / "tests" / "data" / "places_test.parquet")
    out = tmp_path / "out.geojson"
    with pretend_duckdb_version(version):
        result = CliRunner().invoke(cli, ["convert", "geojson", sample, str(out)])
    assert result.exit_code == 0, result.output
    assert out.exists()
    assert bool(tried) is expected


def test_benchmark_environment_reports_the_owner_version():
    with pretend_duckdb_version("7.7.7-test"):
        assert get_environment_info()["duckdb_version"] == "7.7.7-test"


def test_extension_error_reports_the_owner_version():
    class RefusingConnection:
        def execute(self, sql):
            raise duckdb.IOException("HTTP 404 (offline stand-in)")

    with (
        pretend_duckdb_version("7.7.7-test"),
        pytest.raises(duckdb_utils.ExtensionUnavailableError) as excinfo,
    ):
        duckdb_utils.load_community_extension(RefusingConnection(), "h3")
    assert excinfo.value.duckdb_version == "7.7.7-test"


@pytest.mark.parametrize(("version", "has_spatial"), [("1.5.6", True), ("2.0.0", False)])
def test_inspect_preview_reads_the_owner(monkeypatch, version, has_spatial):
    """``gpio inspect head`` tells the preview what its connection was opened with.

    The real preview runs (wrapped, not replaced); the wrapper records the
    ``has_spatial`` the call site passed and whether the connection actually has
    spatial loaded. Both must follow ``core_geometry``.
    """
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli
    from geoparquet_io.core import inspect_utils

    seen: list[tuple[bool, bool]] = []
    original = inspect_utils._preview_table_from_query

    def recording(con, query, has_spatial):
        loaded = con.execute(
            "SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'spatial'"
        ).fetchall()
        seen.append((has_spatial, bool(loaded and loaded[0][0])))
        return original(con, query, has_spatial)

    monkeypatch.setattr(inspect_utils, "_preview_table_from_query", recording)
    sample = str(REPO / "tests" / "data" / "places_test.parquet")
    with pretend_duckdb_version(version):
        result = CliRunner().invoke(cli, ["inspect", "head", sample, "5"])
    assert result.exit_code == 0, result.output
    assert seen == [(has_spatial, has_spatial)]


def test_wkt_detection_closes_its_probe_through_the_owner(monkeypatch, tmp_path):
    """``gpio convert`` of a WKT CSV closes its parse probe with ``complete_without_fetch``.

    The probe's GEOMETRY row is never fetched (1.5 cannot serialize it); on 2.0
    an unclosed probe blocks the connection. The real helper runs, wrapped.
    """
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli
    from geoparquet_io.core import convert

    closed: list[object] = []
    original = convert.complete_without_fetch

    def recording(result):
        closed.append(result)
        return original(result)

    monkeypatch.setattr(convert, "complete_without_fetch", recording)
    csv = tmp_path / "in.csv"
    csv.write_text("id,wkt\n1,POINT (1 2)\n2,POINT (3 4)\n")
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(cli, ["convert", "geoparquet", str(csv), str(out)])
    assert result.exit_code == 0, result.output
    assert len(closed) == 1
    assert duckdb.sql(f"SELECT count(*) FROM read_parquet('{out}')").fetchall() == [(2,)]
