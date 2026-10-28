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
import re
import shutil
import subprocess
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
@pytest.mark.skipif(
    getattr(os, "geteuid", lambda: -1)() == 0, reason="root ignores directory permissions"
)
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


def _profile_files(directory):
    return sorted(n for n in os.listdir(directory) if n.endswith(".json"))


def _query_names(directory):
    return [profile.get("query_name") for profile in _profiles(directory)]


def test_sql_and_executemany_each_write_one_named_profile(tmp_path, monkeypatch):
    monkeypatch.setenv("GPIO_DUCKDB_PROFILE", str(tmp_path))
    con = _connection()
    con.execute("CREATE TABLE t (a INTEGER)")  # DDL: DuckDB writes no profile

    assert con.sql("SELECT 'from-sql'").fetchall() == [("from-sql",)]
    assert _query_names(tmp_path) == ["SELECT 'from-sql'"]

    con.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
    assert _query_names(tmp_path) == ["SELECT 'from-sql'", "INSERT INTO t VALUES (?)"]
    assert len(_profile_files(tmp_path)) == 2


def test_quote_in_directory_name(tmp_path, monkeypatch):
    prof_dir = tmp_path / "o'brien"
    monkeypatch.setenv("GPIO_DUCKDB_PROFILE", str(prof_dir))
    con = _connection()
    assert con.execute("SELECT 'quoted'").fetchall() == [("quoted",)]
    assert _query_names(prof_dir) == ["SELECT 'quoted'"]


def test_fetchone_profile_is_written_only_once_the_result_is_exhausted(tmp_path, monkeypatch):
    """Pins the documented limit: DuckDB writes a profile when a result is
    fully consumed. A one-row result read with a single fetchone() is not
    exhausted, so the next statement cancels it and no profile is written."""
    monkeypatch.setenv("GPIO_DUCKDB_PROFILE", str(tmp_path))
    con = _connection()

    assert con.execute("SELECT 'fetched-once'").fetchone() == ("fetched-once",)
    con.execute("SELECT 'next'").fetchall()
    assert _query_names(tmp_path) == ["SELECT 'next'"]

    result = con.execute("SELECT 'drained'")
    assert result.fetchone() == ("drained",)
    assert result.fetchone() is None  # exhausted: now DuckDB writes it
    assert "SELECT 'drained'" in _query_names(tmp_path)


# These run in a subprocess: tests may not change pytest's own CWD (the
# no-cwd-change-in-tests hook), and both cases depend on the CWD.
_CHDIR_SCRIPT = """
import json, os, sys
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
os.chdir(sys.argv[1])  # allow-cwd-change: a child process, never pytest's CWD
print(json.dumps({"type": type(con).__name__, "rows": con.execute("SELECT 'q'").fetchall()}))
"""


def _run_in(start, then, env_value):
    env = {**os.environ, "GPIO_DUCKDB_PROFILE": env_value}
    proc = subprocess.run(
        [sys.executable, "-c", _CHDIR_SCRIPT, str(then)],
        cwd=start,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout.strip().splitlines()[-1]), proc.stderr


def test_relative_directory_survives_chdir(tmp_path):
    start = tmp_path / "start"
    elsewhere = tmp_path / "elsewhere"
    start.mkdir()
    elsewhere.mkdir()

    result, _ = _run_in(start, elsewhere, "relprof")

    assert result == {"type": "ProfiledConnection", "rows": [["q"]]}
    assert _query_names(start / "relprof") == ["SELECT 'q'"]
    assert not (elsewhere / "relprof").exists()


def test_deleted_directory_is_recreated(tmp_path, monkeypatch):
    prof_dir = tmp_path / "prof"
    monkeypatch.setenv("GPIO_DUCKDB_PROFILE", str(prof_dir))
    con = _connection()
    con.execute("SELECT 1").fetchall()

    shutil.rmtree(prof_dir)
    assert con.execute("SELECT 'recreated'").fetchall() == [("recreated",)]
    assert _query_names(prof_dir) == ["SELECT 'recreated'"]


def test_directory_that_becomes_unusable_disables_profiling_once(tmp_path, monkeypatch, caplog):
    prof_dir = tmp_path / "prof"
    monkeypatch.setenv("GPIO_DUCKDB_PROFILE", str(prof_dir))
    con = _connection()
    con.execute("SELECT 1").fetchall()

    shutil.rmtree(prof_dir)
    prof_dir.write_text("a file now sits where the directory was")
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        assert con.execute("SELECT 2").fetchall() == [(2,)]
        assert con.execute("SELECT 3").fetchall() == [(3,)]
        assert con.sql("SELECT 4").fetchall() == [(4,)]
    warnings = [r for r in caplog.records if "GPIO_DUCKDB_PROFILE" in r.getMessage()]
    assert len(warnings) == 1
    assert con.execute("SELECT current_setting('enable_profiling')").fetchone() == (None,)
    # a cursor of a connection whose profiling is off is a plain cursor
    assert type(con.cursor()) is duckdb.DuckDBPyConnection


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on", "0", "false", "off"])
def test_boolean_looking_value_warns_and_creates_nothing(monkeypatch, caplog, value):
    assert not os.path.exists(value)  # relative to the CWD, which the test leaves alone
    monkeypatch.setenv("GPIO_DUCKDB_PROFILE", value)
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        con = _connection()
    assert type(con) is duckdb.DuckDBPyConnection
    assert con.execute("SELECT 1").fetchall() == [(1,)]
    assert "directory" in caplog.text
    assert not os.path.exists(value)


def test_boolean_looking_name_of_an_existing_directory_is_used(tmp_path):
    (tmp_path / "on").mkdir()

    result, _ = _run_in(tmp_path, tmp_path, "on")

    assert result["type"] == "ProfiledConnection"
    assert _query_names(tmp_path / "on") == ["SELECT 'q'"]


def test_lazy_relation_never_lands_in_an_unrelated_new_file(tmp_path, monkeypatch):
    """Documented caveat: a con.sql() relation runs when it is fetched, so its
    profile goes to whatever file is current then. Every file still holds a
    real profile and no name is reused for a second rotation."""
    monkeypatch.setenv("GPIO_DUCKDB_PROFILE", str(tmp_path))
    con = _connection()
    rel = con.sql("SELECT 'lazy'")
    con.execute("SELECT 'eager'").fetchall()
    assert rel.fetchall() == [("lazy",)]
    names = _query_names(tmp_path)
    assert names and set(names) <= {"SELECT 'lazy'", "SELECT 'eager'"}


def test_existing_file_is_never_overwritten(tmp_path, monkeypatch):
    """Two connections forced onto the same filename prefix (a PID-1
    container, the same instant) still never overwrite each other's files."""
    from geoparquet_io.core import duckdb_utils

    monkeypatch.setattr(duckdb_utils, "_profile_file_prefix", lambda: "gpio-profile-fixed")
    monkeypatch.setenv("GPIO_DUCKDB_PROFILE", str(tmp_path))
    first = _connection()
    second = _connection()
    first.execute("SELECT 'a'").fetchall()
    second.execute("SELECT 'b'").fetchall()
    first.execute("SELECT 'c'").fetchall()
    second.execute("SELECT 'd'").fetchall()

    assert sorted(_query_names(tmp_path)) == [
        "SELECT 'a'",
        "SELECT 'b'",
        "SELECT 'c'",
        "SELECT 'd'",
    ]


def test_filename_prefix_is_unique_per_connection():
    from geoparquet_io.core.duckdb_utils import _profile_file_prefix

    prefixes = {_profile_file_prefix() for _ in range(50)}
    assert len(prefixes) == 50
    # Microsecond time + PID + random token: the PID alone repeats across
    # containers (often PID 1) and second resolution repeats within a run.
    shape = re.compile(rf"gpio-profile-\d{{8}}T\d{{12}}-p{os.getpid()}-[0-9a-f]{{6}}-c\d+")
    assert all(shape.fullmatch(p) for p in prefixes), prefixes
    assert len({p.split("-")[4] for p in prefixes}) > 1  # the token varies


def test_cursor_queries_are_profiled(tmp_path, monkeypatch):
    monkeypatch.setenv("GPIO_DUCKDB_PROFILE", str(tmp_path))
    con = _connection()
    cur = con.cursor()
    assert cur.execute("SELECT 'via-cursor'").fetchall() == [("via-cursor",)]
    assert _query_names(tmp_path) == ["SELECT 'via-cursor'"]
