"""Where DuckDB spills intermediate results (Deep Review 2.1).

DuckDB's out-of-the-box ``temp_directory`` is the relative path ``.tmp``, so
every spill lands under whatever directory the process happens to be running
in. Two things follow, and both are pinned here:

* a read-only (or full) working directory turns a large sort into a hard
  failure even when the input and output volumes are perfectly writable;
* a *shared* spill directory is not safe. DuckDB names its spill files after
  the block size alone (``duckdb_temp_storage_S192K-0.tmp``), with nothing
  identifying the connection or the process, so two connections pointed at one
  directory overwrite each other's blocks and fail with ``IO Error: Could not
  read enough bytes`` or corrupt statistics.

The fix is central: :func:`get_duckdb_connection` gives every connection its
own private spill directory under the OS temp directory unless the caller names
one.

A private *unique* directory has a cost the fixed ``.tmp`` did not, and it is
pinned here too. DuckDB removes its temp directory when the **query** finishes,
so a run killed mid-spill (SIGINT, SIGTERM, SIGKILL) leaves the leaf behind
however carefully the caller closes the connection. With a fixed name that leak
is self-limiting -- the next run reuses the same path and the same
``duckdb_temp_storage_*.tmp`` filenames -- while unique names would let three
killed runs leave three multi-GB directories. So ``spill_directory()`` sweeps
the base it is about to write into, removing leaves whose owning process is
gone, and ``--clear-cache`` does the same for the admin cache directory.
"""

from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import uuid
from pathlib import Path

import click
import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.core.duckdb_utils import (
    get_duckdb_connection,
    spill_directory,
    spill_space_hint,
    sweep_orphaned_spill_dirs,
)

# A sort that comfortably exceeds the memory limit below, and still runs in
# well under a second.
SPILL_ROWS = 400_000
SPILL_MEMORY_LIMIT = "128MB"
SPILL_QUERY = (
    "CREATE TABLE spilled AS "
    f"SELECT i, repeat('x', 400) s FROM range({SPILL_ROWS}) t(i) ORDER BY hash(i)"
)


def _force_spill(con) -> None:
    """Run a query that cannot finish without writing to temp_directory."""
    con.execute("SET threads = 1")
    con.execute(f"SET memory_limit = '{SPILL_MEMORY_LIMIT}'")
    con.execute(SPILL_QUERY)


def _temp_directory(con) -> str:
    return con.execute("SELECT current_setting('temp_directory')").fetchone()[0]


#: Rows in the fixture below: ~300MB of payload, so a sort of it cannot fit in
#: WRITE_MEMORY_LIMIT, yet it builds and writes in about a second. The limit has
#: headroom over the payload because duckdb-kv's COPY needs working space of its
#: own beyond the sort it is draining.
WIDE_ROWS = 200_000
WRITE_MEMORY_LIMIT = "320MB"


def _wide_geoparquet(path: Path) -> str:
    """A GeoParquet file whose sort does not fit in ``SPILL_MEMORY_LIMIT``."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    import shapely
    import shapely.wkb

    point = shapely.wkb.dumps(shapely.Point(1.0, 2.0))
    table = pa.table(
        {
            "geometry": pa.array([point] * WIDE_ROWS, pa.binary()),
            "sort_key": pa.array([(i * 2654435761) % WIDE_ROWS for i in range(WIDE_ROWS)]),
            "padding": pa.array(["x" * 1500] * WIDE_ROWS),
        }
    )
    pq.write_table(table, path)
    return str(path)


requires_unprivileged_posix = pytest.mark.skipif(
    sys.platform == "win32" or (hasattr(os, "geteuid") and os.geteuid() == 0),
    reason="needs POSIX permission bits and a non-root user to make a directory read-only",
)


class TestSpillDirectoryHelper:
    """``spill_directory()`` picks the location; it never creates it."""

    def test_defaults_under_the_os_temp_directory(self):
        path = Path(spill_directory())
        assert path.parent == Path(tempfile.gettempdir())

    def test_honours_an_explicit_base(self, tmp_path):
        path = Path(spill_directory(tmp_path))
        assert path.parent == tmp_path

    def test_is_not_created_eagerly(self, tmp_path):
        # Nothing is written unless DuckDB actually spills, and DuckDB removes
        # a directory it created itself when the connection closes.
        assert not Path(spill_directory(tmp_path)).exists()
        assert list(tmp_path.iterdir()) == []

    def test_is_unique_per_call(self, tmp_path):
        paths = {spill_directory(tmp_path) for _ in range(50)}
        assert len(paths) == 50


class TestConnectionDefaults:
    """Every connection gets a private spill directory, named or not."""

    def test_default_is_absolute_and_outside_the_working_directory(self):
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        try:
            value = _temp_directory(con)
            assert value != ".tmp"
            assert Path(value).is_absolute()
            assert Path(tempfile.gettempdir()) in Path(value).parents
        finally:
            con.close()

    def test_two_connections_never_share_a_spill_directory(self):
        first = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        second = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        try:
            assert _temp_directory(first) != _temp_directory(second)
        finally:
            first.close()
            second.close()

    def test_an_explicit_temp_directory_is_used_verbatim(self, tmp_path):
        chosen = str(tmp_path / "chosen")
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False, temp_directory=chosen)
        try:
            assert _temp_directory(con) == chosen
        finally:
            con.close()

    def test_memory_limit_stays_opt_in(self):
        """No default cap: DuckDB's own 80%-of-RAM limit is the right one.

        A limit only helps when spilling works, and forcing a lower one would
        push queries to disk that fit in RAM today.
        """
        configured = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        bare = get_duckdb_connection(load_spatial=False, load_httpfs=False, memory_limit=None)
        try:
            assert _temp_directory(configured) != ".tmp"  # spill dir was set
            assert (
                configured.execute("SELECT current_setting('memory_limit')").fetchone()[0]
                == bare.execute("SELECT current_setting('memory_limit')").fetchone()[0]
            )
        finally:
            configured.close()
            bare.close()

    def test_threads_and_insertion_order_stay_at_duckdb_defaults(self):
        """``threads=1`` + ``preserve_insertion_order=false`` are write-time knobs.

        They are what makes a *COPY* spill reliably (duckdb-kv clamps both for
        the duration of a write), but as a global default they would serialise
        every read gpio does and change result ordering for callers that never
        write a file.
        """
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        try:
            assert int(con.execute("SELECT current_setting('threads')").fetchone()[0]) > 0
            assert (
                con.execute("SELECT current_setting('preserve_insertion_order')").fetchone()[0]
                is True
            )
        finally:
            con.close()


class TestSpillsActuallyLandThere:
    """The setting is not the point; surviving the spill is."""

    def test_spill_files_land_in_the_private_directory(self):
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        spill_dir = Path(_temp_directory(con))
        try:
            _force_spill(con)
            assert spill_dir.is_dir()
            assert list(spill_dir.glob("duckdb_temp_storage*"))
        finally:
            con.close()
        # DuckDB removes a temp directory it created itself -- when the query
        # finished, which it did here. On Windows a still-open handle can defeat
        # that removal (this repo has a long history of WinError 32 on
        # unlink/replace), and a leftover is not the thing this test is about:
        # what bounds leftovers is the sweep in ``spill_directory()``, pinned in
        # TestOrphanedSpillDirectoriesAreReaped below.
        if os.name != "nt":
            assert not spill_dir.exists()

    def test_concurrent_connections_do_not_corrupt_each_other(self):
        """Regression guard for the shared-directory hazard in the docstring."""
        errors: list[BaseException] = []

        def run() -> None:
            con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
            try:
                _force_spill(con)
                assert con.execute("SELECT count(*) FROM spilled").fetchone()[0] == SPILL_ROWS
            except BaseException as exc:  # pragma: no cover - only on regression
                errors.append(exc)
            finally:
                con.close()

        threads = [threading.Thread(target=run) for _ in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert not errors


#: Body of the subprocess below. Run from a read-only working directory, it
#: proves the failure Deep Review 2.1 names and then proves gpio's default
#: clears it, on one interpreter start.
_READ_ONLY_CWD_SCRIPT = f"""
import os, pathlib, sys
import duckdb
import pyarrow.parquet as pq
from geoparquet_io.core.common import write_parquet_with_metadata
from geoparquet_io.core.duckdb_utils import get_duckdb_connection

source, output = sys.argv[1], sys.argv[2]
assert not os.access(os.getcwd(), os.W_OK), "working directory is still writable"

def force_spill(con):
    con.execute("SET threads = 1")
    con.execute("SET memory_limit = '{SPILL_MEMORY_LIMIT}'")
    con.execute({SPILL_QUERY!r})

# 1. Stock DuckDB spills into ".tmp" under the working directory, and dies here.
bare = duckdb.connect()
try:
    force_spill(bare)
    raise AssertionError("expected stock DuckDB to fail on a read-only CWD")
except duckdb.IOException as exc:
    assert ".tmp" in str(exc), exc
finally:
    bare.close()

# 2. A gpio connection spills somewhere writable instead.
con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
try:
    force_spill(con)
    assert con.execute("SELECT count(*) FROM spilled").fetchone()[0] == {SPILL_ROWS}
finally:
    con.close()

# 3. And so does a real GeoParquet write whose sort does not fit in memory.
con = get_duckdb_connection(load_httpfs=False)
spill_dir = pathlib.Path(con.execute("SELECT current_setting('temp_directory')").fetchone()[0])
try:
    write_parquet_with_metadata(
        con=con,
        query=(
            "SELECT * REPLACE (ST_GeomFromWKB(geometry) AS geometry) "
            "FROM read_parquet('" + source + "') ORDER BY sort_key"
        ),
        output_file=output,
        compression="ZSTD",
        compression_level=1,
        geoparquet_version="1.1",
        memory_limit="{WRITE_MEMORY_LIMIT}",
    )
    # The payload is larger than the limit, so the sort must have gone to disk
    # -- and it went to gpio's private directory, not to ".tmp".
    assert spill_dir.is_dir(), "the write never spilled; the test proves nothing"
finally:
    con.close()

assert pq.ParquetFile(output).metadata.num_rows == {WIDE_ROWS}
assert os.listdir(".") == [], os.listdir(".")
print("OK")
"""


@requires_unprivileged_posix
def test_spilling_write_survives_a_read_only_working_directory(tmp_path):
    """The failure Deep Review 2.1 names, reproduced and then fixed.

    Runs in a subprocess because the working directory is the whole point and
    this suite may not move its own (see the ``no-cwd-change-in-tests`` hook).
    """
    cwd = tmp_path / "read-only-cwd"
    cwd.mkdir()
    source = _wide_geoparquet(tmp_path / "in.parquet")
    output = str(tmp_path / "out.parquet")

    cwd.chmod(stat.S_IRUSR | stat.S_IXUSR)
    try:
        result = subprocess.run(
            [sys.executable, "-c", _READ_ONLY_CWD_SCRIPT, source, output],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=300,
        )
    finally:
        cwd.chmod(stat.S_IRWXU)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "OK" in result.stdout


ONE_MB = 1024 * 1024


def _dead_pid() -> int:
    """A pid that has certainly exited and been reaped."""
    proc = subprocess.Popen([sys.executable, "-c", ""])
    proc.wait()
    return proc.pid


def _plant_leaf(base: Path, pid: int, payload: int = ONE_MB) -> Path:
    """A spill leaf shaped exactly like one gpio would leave behind."""
    leaf = base / f"gpio-spill-{pid}-{uuid.uuid4().hex[:12]}"
    leaf.mkdir(parents=True)
    (leaf / "duckdb_temp_storage_S192K-0.tmp").write_bytes(b"\0" * payload)
    return leaf


def _bytes_under(base: Path) -> int:
    return sum(p.stat().st_size for p in base.rglob("*") if p.is_file())


class TestOrphanedSpillDirectoriesAreReaped:
    """A killed run leaves its leaf behind; the next run takes it away.

    DuckDB removes a temp directory when the *query* completes, not when the
    process dies, so an interrupted spill orphans it no matter what the caller
    does in a ``finally``. Unique names remove the accidental self-healing the
    fixed ``.tmp`` had, which is why the sweep exists.
    """

    def test_a_leaf_whose_owner_is_gone_is_removed(self, tmp_path):
        leaf = _plant_leaf(tmp_path, _dead_pid())

        spill_directory(tmp_path)

        assert not leaf.exists()

    def test_a_leaf_belonging_to_a_live_process_survives(self, tmp_path):
        """Never take a *sibling's* scratch space away mid-query."""
        leaf = _plant_leaf(tmp_path, os.getpid())

        spill_directory(tmp_path)

        assert leaf.is_dir()

    def test_an_unparseable_pid_is_left_alone(self, tmp_path):
        """When ownership cannot be decided, keep the directory."""
        odd = tmp_path / "gpio-spill-notapid-abcdef123456"
        odd.mkdir()

        spill_directory(tmp_path)

        assert odd.is_dir()

    def test_neighbours_are_never_touched(self, tmp_path):
        cached = tmp_path / "overture-2025-10-22.0.parquet"
        cached.write_bytes(b"cached dataset")
        unrelated = tmp_path / "someone-elses-scratch"
        unrelated.mkdir()
        # A *file* whose name happens to match: gpio only ever makes directories.
        decoy = tmp_path / f"gpio-spill-{_dead_pid()}-abcdef123456"
        decoy.write_bytes(b"not a directory")

        spill_directory(tmp_path)

        assert cached.exists()
        assert unrelated.is_dir()
        assert decoy.exists()

    def test_the_sweep_recognises_the_names_gpio_actually_mints(self, tmp_path):
        """Couples the minting and the matching so they cannot drift apart."""
        minted = Path(spill_directory(tmp_path)).name
        prefix, pid, suffix = minted.rsplit("-", 2)[0], os.getpid(), minted.rsplit("-", 1)[1]
        assert f"{prefix}-{pid}-{suffix}" == minted

        orphan = tmp_path / f"{prefix}-{_dead_pid()}-{suffix}"
        orphan.mkdir()

        spill_directory(tmp_path)

        assert not orphan.exists()

    def test_repeated_interrupted_runs_do_not_accumulate(self, tmp_path):
        """The accumulation the reviewer measured: 28MB -> 133MB -> 202MB.

        Each iteration is one gpio run that picks a spill path, spills, and is
        killed before DuckDB can clean up. Without a sweep the base grows by a
        leaf per run; with one it plateaus at the single leaf the *last* run
        left, because every run reaps its dead predecessors first.
        """
        sizes = []
        for _ in range(3):
            leaf = Path(spill_directory(tmp_path))
            # Pretend this run is a separate process, since the sweep -- rightly
            # -- refuses to delete a live process's directory.
            leaf = leaf.parent / leaf.name.replace(f"-{os.getpid()}-", f"-{_dead_pid()}-")
            leaf.mkdir()
            (leaf / "duckdb_temp_storage_S192K-0.tmp").write_bytes(b"\0" * ONE_MB)
            sizes.append(_bytes_under(tmp_path))

        assert sizes == [ONE_MB, ONE_MB, ONE_MB]

    def test_an_unremovable_leaf_does_not_fail_the_connection(self, tmp_path, monkeypatch):
        """Best effort: a permission error on someone else's leftovers is not fatal."""
        leaf = _plant_leaf(tmp_path, _dead_pid())

        def refuse(*args, **kwargs):
            raise PermissionError(13, "Permission denied", str(leaf))

        monkeypatch.setattr(shutil, "rmtree", refuse)

        con = get_duckdb_connection(
            load_spatial=False, load_httpfs=False, temp_directory=spill_directory(tmp_path)
        )
        try:
            assert con.execute("SELECT 1").fetchone()[0] == 1
        finally:
            con.close()
        assert leaf.is_dir()

    def test_a_missing_base_directory_is_not_an_error(self, tmp_path):
        """The default base always exists; an explicit one need not yet."""
        assert spill_directory(tmp_path / "not-created-yet")

    def test_opening_a_default_connection_reaps_the_temp_directory(self, tmp_path, monkeypatch):
        """The sweep is on the path every gpio connection already takes."""
        monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
        leaf = _plant_leaf(tmp_path, _dead_pid())

        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        try:
            assert Path(_temp_directory(con)).parent == tmp_path
        finally:
            con.close()

        assert not leaf.exists()

    def test_sweep_reports_what_it_removed(self, tmp_path):
        dead = _plant_leaf(tmp_path, _dead_pid())
        alive = _plant_leaf(tmp_path, os.getpid())

        removed = sweep_orphaned_spill_dirs(tmp_path)

        assert [Path(p) for p in removed] == [dead]
        assert alive.is_dir()


#: The real DuckDB error, provoked with a tiny ``max_temp_directory_size``
#: rather than a tiny volume: the message is byte-for-byte what a user on a
#: 64MB or RAM-backed ``/tmp`` sees.
def _out_of_spill_space_error(tmp_path) -> duckdb.Error:
    con = duckdb.connect()
    try:
        con.execute(f"SET temp_directory = '{tmp_path.as_posix()}'")
        con.execute("SET max_temp_directory_size = '16MB'")
        con.execute("SET threads = 1")
        con.execute(f"SET memory_limit = '{SPILL_MEMORY_LIMIT}'")
        with pytest.raises(duckdb.Error) as excinfo:
            con.execute(SPILL_QUERY)
        return excinfo.value
    finally:
        con.close()


class TestOutOfSpillSpaceIsExplained:
    """DuckDB says "Out of Memory Error" for a *disk* shortage, and never says TMPDIR."""

    def test_duckdbs_own_message_says_memory_and_never_says_tmpdir(self, tmp_path):
        text = str(_out_of_spill_space_error(tmp_path))
        assert "Out of Memory Error" in text
        assert "TMPDIR" not in text

    def test_the_real_error_is_recognised(self, tmp_path):
        hint = spill_space_hint(_out_of_spill_space_error(tmp_path))
        assert hint is not None
        assert "TMPDIR" in hint
        assert "tmpfs" in hint

    def test_an_ordinary_memory_error_is_not_claimed(self):
        con = duckdb.connect()
        try:
            con.execute("SET memory_limit = '10MB'")
            con.execute("SET temp_directory = ''")
            with pytest.raises(duckdb.Error) as excinfo:
                con.execute(SPILL_QUERY)
        finally:
            con.close()
        assert "max_temp_directory_size" not in str(excinfo.value)
        assert spill_space_hint(excinfo.value) is None

    def test_nothing_and_unrelated_errors_return_none(self):
        assert spill_space_hint(None) is None
        assert spill_space_hint(ValueError("no such file")) is None

    def test_raw_error_text_is_accepted_too(self):
        assert spill_space_hint("... the 'max_temp_directory_size' setting.") is not None
        assert spill_space_hint("IO Error: no such file") is None

    def test_a_wrapped_error_is_found_through_the_cause_chain(self):
        inner = duckdb.Error("Out of Memory Error: ... the 'max_temp_directory_size' setting.")
        outer = RuntimeError("write failed")
        outer.__cause__ = inner
        assert spill_space_hint(outer) is not None


class TestTheCliNamesTmpdir:
    """The hint has to reach the user, on every command, not just the ones with a decorator."""

    def test_the_group_replaces_an_out_of_spill_space_message(self):
        from geoparquet_io.cli.decorators import ErrorBoundaryGroup

        @click.group(cls=ErrorBoundaryGroup)
        def root():
            pass

        @root.command()
        def boom():
            raise duckdb.OutOfMemoryException(
                "Out of Memory Error: failed to offload data block of size 256.0 KiB.\n"
                "This limit was set by the 'max_temp_directory_size' setting."
            )

        result = CliRunner().invoke(root, ["boom"])

        assert result.exit_code != 0
        assert "TMPDIR" in result.output
        # The original text is kept: it names the sizes involved.
        assert "failed to offload data block" in result.output

    def test_another_duckdb_error_keeps_its_own_message(self):
        """The spill hint is added to the one failure it explains and to nothing
        else. Another DuckDB error still becomes an error line (#983) -- it just
        does not grow a ``TMPDIR`` paragraph that has nothing to do with it."""
        from geoparquet_io.cli.decorators import ErrorBoundaryGroup

        @click.group(cls=ErrorBoundaryGroup)
        def root():
            pass

        @root.command()
        def boom():
            raise duckdb.IOException("IO Error: No files found that match the pattern")

        result = CliRunner().invoke(root, ["boom"])

        assert "IO Error: No files found that match the pattern" in result.output
        assert "TMPDIR" not in result.output

    def test_the_real_cli_group_uses_it(self):
        from geoparquet_io.cli.decorators import ErrorBoundaryGroup
        from geoparquet_io.cli.main import cli

        assert isinstance(cli, ErrorBoundaryGroup)


def test_the_reaper_pattern_is_not_hand_written_twice():
    """One regex owns the naming convention; ``--clear-cache`` reuses it."""
    from geoparquet_io.core import admin_datasets, duckdb_utils

    assert isinstance(duckdb_utils._SPILL_DIR_RE, re.Pattern)
    assert admin_datasets.sweep_orphaned_spill_dirs is duckdb_utils.sweep_orphaned_spill_dirs
