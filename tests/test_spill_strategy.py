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
"""

from __future__ import annotations

import os
import stat
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

import pytest

from geoparquet_io.core.duckdb_utils import get_duckdb_connection, spill_directory

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
        # DuckDB removes a temp directory it created itself.
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
