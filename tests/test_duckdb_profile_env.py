"""GPIO_DUCKDB_PROFILE writes one DuckDB JSON profile per query (#1268).

DuckDB's ``profiling_output`` names a single file that every profiled
statement overwrites, so the connection wrapper must rotate the target
before each statement — these tests pin that per-query behaviour, the
graceful degradation on an unusable path, and that an unset variable
leaves profiling off entirely.
"""

import json
import logging
import os
import sys

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import get_duckdb_connection

BUILDINGS = os.path.join(os.path.dirname(__file__), "data", "buildings_test.parquet")


def _connection():
    return get_duckdb_connection(load_spatial=False, load_httpfs=False)


def _profiles(directory):
    """All JSON profile files in *directory*, parsed."""
    out = []
    for name in sorted(os.listdir(directory)):
        if name.endswith(".json"):
            with open(os.path.join(directory, name), encoding="utf-8") as f:
                out.append(json.load(f))
    return out


def test_each_query_writes_its_own_profile(tmp_path, monkeypatch):
    prof_dir = tmp_path / "prof"  # does not exist yet: gpio must create it
    monkeypatch.setenv("GPIO_DUCKDB_PROFILE", str(prof_dir))

    con = _connection()
    con.execute("SELECT 1").fetchall()
    con.execute("SELECT 2 + 2").fetchall()
    con.execute("SELECT range FROM range(5)").fetchall()

    profiles = _profiles(prof_dir)
    assert len(profiles) == 3
    for profile in profiles:
        assert isinstance(profile, dict)
        assert profile  # non-empty: a real DuckDB profile, not a stub


def test_parallel_connections_do_not_collide(tmp_path, monkeypatch):
    monkeypatch.setenv("GPIO_DUCKDB_PROFILE", str(tmp_path))

    first = _connection()
    second = _connection()
    first.execute("SELECT 1").fetchall()
    second.execute("SELECT 2").fetchall()
    first.execute("SELECT 3").fetchall()

    assert len(_profiles(tmp_path)) == 3


def test_wrapper_covers_sql_executemany_and_context_manager(tmp_path, monkeypatch):
    monkeypatch.setenv("GPIO_DUCKDB_PROFILE", str(tmp_path))

    with get_duckdb_connection(load_spatial=False, load_httpfs=False) as con:
        assert con.sql("SELECT 7").fetchall() == [(7,)]
        con.execute("CREATE TABLE t (a INTEGER)")
        con.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
        # __getattr__ delegation: fetchone is not wrapped explicitly
        assert con.execute("SELECT count(*) FROM t").fetchone() == (2,)

    assert len(_profiles(tmp_path)) >= 2


def test_cli_convert_writes_a_profile_per_query(tmp_path, monkeypatch):
    """The issue's acceptance case: a profiled `gpio convert geoparquet` run."""
    prof_dir = tmp_path / "prof"
    monkeypatch.setenv("GPIO_DUCKDB_PROFILE", str(prof_dir))
    output = tmp_path / "out.parquet"

    result = CliRunner().invoke(cli, ["convert", "geoparquet", BUILDINGS, str(output)])

    assert result.exit_code == 0, result.output
    assert output.exists()
    profiles = _profiles(prof_dir)
    assert profiles, "no DuckDB profiles were written"
    for profile in profiles:
        assert isinstance(profile, dict) and profile


def test_path_that_is_a_file_degrades_gracefully(tmp_path, monkeypatch, caplog):
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("occupied")
    monkeypatch.setenv("GPIO_DUCKDB_PROFILE", str(blocker))

    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        con = _connection()
    assert con.execute("SELECT 1").fetchall() == [(1,)]
    assert type(con) is duckdb.DuckDBPyConnection  # profiling never enabled
    assert "GPIO_DUCKDB_PROFILE" in caplog.text


@pytest.mark.skipif(sys.platform == "win32", reason="chmod is advisory on Windows")
@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions")
def test_unwritable_directory_degrades_gracefully(tmp_path, monkeypatch, caplog):
    locked = tmp_path / "locked"
    locked.mkdir()
    locked.chmod(0o500)
    monkeypatch.setenv("GPIO_DUCKDB_PROFILE", str(locked))
    try:
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            con = _connection()
        assert con.execute("SELECT 1").fetchall() == [(1,)]
        assert "GPIO_DUCKDB_PROFILE" in caplog.text
        assert _profiles(locked) == []
    finally:
        locked.chmod(0o700)


@pytest.mark.parametrize("value", [None, "", "   "])
def test_unset_or_blank_env_leaves_profiling_off(tmp_path, monkeypatch, value):
    if value is None:
        monkeypatch.delenv("GPIO_DUCKDB_PROFILE", raising=False)
    else:
        monkeypatch.setenv("GPIO_DUCKDB_PROFILE", value)

    con = _connection()
    assert type(con) is duckdb.DuckDBPyConnection
    con.execute("SELECT 1").fetchall()
    assert con.execute("SELECT current_setting('enable_profiling')").fetchone() == (None,)
