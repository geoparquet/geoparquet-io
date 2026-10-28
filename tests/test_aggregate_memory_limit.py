"""`gpio process aggregate` and `gpio process overview` run inside a memory limit (#1179).

`gpio process aggregate a5` on a 42 GB / 134M-row GeoParquet peaked at 115.2 GiB
MaxRSS to produce 37,206 cells. The five DuckDB connections these commands open
passed no ``memory_limit``, so DuckDB sized itself for the node -- blind to the
Slurm job cgroup -- exactly the family #1153/#1154 closed for writes.

The aggregate paths end in ``con.execute(sql).arrow().read_all()``: there is no
COPY for ``scoped_write_memory_limit`` to wrap, so the cap has to land on the
connection when it is opened. These tests pin that:

  * every one of the five sites opens its connection through the shared
    bounded-connection helper, and forwards the caller's ``memory_limit``;
  * an explicit limit is really SET on the connection, not merely logged;
  * without one, the default is half the process's memory *ceiling*, which
    follows the job cgroup (Slurm nests the cap above the process's own group);
  * threads shrink with the limit so DuckDB spills rather than raising;
  * row order is not preserved -- a GROUP BY decides the output order anyway;
  * `--write-memory` reaches all four commands from the CLI.
"""

from __future__ import annotations

import inspect
import os

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core import memory_limits
from geoparquet_io.core.process.aggregate import by_admin, grid_common
from geoparquet_io.core.process.aggregate.by_a5 import aggregate_a5_table, aggregate_by_a5
from geoparquet_io.core.process.overview import detect, rollup

GIB = 1024**3


# ---------------------------------------------------------------------------
# fixtures and helpers
# ---------------------------------------------------------------------------


def _write_points(path: str, rows: int = 2000) -> None:
    """A small GeoParquet with a geometry, a bbox covering, and two attributes."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"""COPY (
            SELECT i AS id,
                   ST_Point((i % 97) * 0.5, (i % 89) * 0.5) AS geometry,
                   {{'xmin': (i % 97) * 0.5, 'ymin': (i % 89) * 0.5,
                     'xmax': (i % 97) * 0.5 + 0.01, 'ymax': (i % 89) * 0.5 + 0.01}} AS bbox,
                   (i % 7) * 1.5 AS height,
                   'crop' || (i % 3) AS crop
            FROM range({rows}) t(i)
        ) TO '{path}' (FORMAT PARQUET)"""
    )
    con.close()


@pytest.fixture
def points_parquet(tmp_path):
    path = tmp_path / "points.parquet"
    _write_points(str(path))
    return path


@pytest.fixture
def cells_parquet(tmp_path, points_parquet):
    """An aggregate output, the input `gpio process overview` takes."""
    out = tmp_path / "cells.parquet"
    aggregate_by_a5(str(points_parquet), str(out), resolution=5, metric="sum:height")
    return out


def _setting(con, key):
    return con.execute(f"SELECT current_setting('{key}')").fetchone()[0]


def _duckdb_renders(value: str) -> str:
    """How DuckDB displays ``SET memory_limit = value`` (it truncates to MiB/GiB)."""
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit = '{value}'")
        return _setting(con, "memory_limit")
    finally:
        con.close()


def _duckdb_default(key: str):
    con = duckdb.connect()
    try:
        return _setting(con, key)
    finally:
        con.close()


@pytest.fixture
def connection_spy(monkeypatch):
    """Record the settings every bounded aggregate connection is opened with.

    Patched on each owning module by name, with ``raising=True``: a site that
    stops using the shared helper (or never started) fails here immediately.
    """
    records: list[dict] = []
    real = memory_limits.open_bounded_connection

    def spy(**kwargs):
        con = real(**kwargs)
        records.append(
            {
                "requested": kwargs.get("memory_limit"),
                "memory_limit": _setting(con, "memory_limit"),
                "threads": int(_setting(con, "threads")),
                "preserve_insertion_order": _setting(con, "preserve_insertion_order"),
            }
        )
        return con

    for module in (grid_common, by_admin, detect, rollup):
        monkeypatch.setattr(module, "open_bounded_connection", spy, raising=True)
    return records


def _fake_cgroups(tmp_path, proc_lines: list[str], files: dict[str, str]):
    proc = tmp_path / "proc_self_cgroup"
    proc.write_text("\n".join(proc_lines) + "\n")
    root = tmp_path / "cgroup"
    for rel, value in files.items():
        f = root / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(value + "\n")
    return str(proc), str(root)


# ---------------------------------------------------------------------------
# Layer 0: no aggregate/overview site may open an unbounded connection
# ---------------------------------------------------------------------------

SITES = [
    pytest.param(grid_common, "aggregate_grid_file", id="aggregate-grid-file"),
    pytest.param(grid_common, "aggregate_grid_table", id="aggregate-grid-table"),
    pytest.param(by_admin, "aggregate_by_admin", id="aggregate-by-admin"),
    pytest.param(detect, "aggregate_connection", id="overview-aggregate-connection"),
    pytest.param(rollup, "rollup_table", id="overview-rollup-table"),
]


@pytest.mark.parametrize("module,func", SITES)
def test_site_opens_a_bounded_connection_and_forwards_the_limit(module, func):
    """A hand-kept list of call sites goes stale; reading their source cannot."""
    source = inspect.getsource(inspect.unwrap(getattr(module, func)))
    assert "open_bounded_connection(" in source, f"{func} does not use the bounded helper"
    assert "get_duckdb_connection(" not in source, f"{func} still opens a raw connection"
    assert "memory_limit=memory_limit" in source, f"{func} drops its memory_limit argument"


# ---------------------------------------------------------------------------
# The cap really lands on the connection
# ---------------------------------------------------------------------------


class TestTheLimitIsSet:
    def test_file_aggregate_uses_the_requested_limit(
        self, points_parquet, tmp_path, connection_spy
    ):
        aggregate_by_a5(
            str(points_parquet),
            str(tmp_path / "out.parquet"),
            resolution=5,
            metric="sum:height",
            memory_limit="523MB",
        )
        assert [r["memory_limit"] for r in connection_spy] == [_duckdb_renders("523MB")]

    def test_table_aggregate_uses_the_requested_limit(self, points_parquet, connection_spy):
        import pyarrow.parquet as pq

        aggregate_a5_table(pq.read_table(points_parquet), resolution=5, memory_limit="523MB")
        assert [r["memory_limit"] for r in connection_spy] == [_duckdb_renders("523MB")]

    def test_overview_uses_the_requested_limit(self, cells_parquet, tmp_path, connection_spy):
        from geoparquet_io.core.process.overview import create_overviews

        create_overviews(
            str(cells_parquet),
            levels=[3],
            output_dir=str(tmp_path / "ov"),
            memory_limit="523MB",
        )
        assert connection_spy, "process overview opened no bounded connection"
        assert {r["memory_limit"] for r in connection_spy} == {_duckdb_renders("523MB")}

    def test_rollup_table_uses_the_requested_limit(self, cells_parquet, connection_spy):
        import pyarrow.parquet as pq

        from geoparquet_io.core.process.overview.rollup import rollup_table

        rollup_table(pq.read_table(cells_parquet), 3, memory_limit="523MB")
        assert [r["memory_limit"] for r in connection_spy] == [_duckdb_renders("523MB")]

    def test_an_impossible_limit_raises_rather_than_growing(
        self, points_parquet, tmp_path, monkeypatch
    ):
        """The limit is enforced by DuckDB, not just recorded."""
        with pytest.raises(duckdb.OutOfMemoryException):
            aggregate_by_a5(
                str(points_parquet),
                str(tmp_path / "out.parquet"),
                resolution=12,
                metric="sum:height",
                breakdown="crop",
                memory_limit="1MB",
            )


class TestTheDefaultLimit:
    def test_default_is_half_the_memory_ceiling(
        self, points_parquet, tmp_path, monkeypatch, connection_spy
    ):
        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: 10 * GIB)
        aggregate_by_a5(str(points_parquet), str(tmp_path / "out.parquet"), resolution=5)
        assert connection_spy[0]["memory_limit"] == _duckdb_renders("5.0GB")

    def test_a_slurm_job_cgroup_cap_drives_the_default(
        self, points_parquet, tmp_path, monkeypatch, connection_spy
    ):
        """The cap sits on the job cgroup, above the process's own group (#1153)."""
        proc, root = _fake_cgroups(
            tmp_path,
            ["0::/system.slice/slurmstepd.scope/job_205790/step_batch/user/task_0"],
            {
                "memory.max": "max",
                "system.slice/slurmstepd.scope/job_205790/memory.max": str(2 * GIB),
                "system.slice/slurmstepd.scope/job_205790/step_batch/user/task_0/memory.max": (
                    "max"
                ),
            },
        )
        monkeypatch.setattr(memory_limits, "_PROC_SELF_CGROUP", proc)
        monkeypatch.setattr(memory_limits, "_CGROUP_ROOT", root)
        aggregate_by_a5(str(points_parquet), str(tmp_path / "out.parquet"), resolution=5)
        assert connection_spy[0]["memory_limit"] == _duckdb_renders("1.0GB")

    def test_unknown_ceiling_leaves_duckdbs_own_default(
        self, points_parquet, tmp_path, monkeypatch, connection_spy
    ):
        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: None)
        aggregate_by_a5(str(points_parquet), str(tmp_path / "out.parquet"), resolution=5)
        assert connection_spy[0]["requested"] is None
        assert connection_spy[0]["memory_limit"] == _duckdb_default("memory_limit")

    def test_an_explicit_limit_wins_over_the_default(
        self, points_parquet, tmp_path, monkeypatch, connection_spy
    ):
        """Mirrors ``_resolve_limit``: the caller's value is used as given."""
        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: 10 * GIB)
        aggregate_by_a5(
            str(points_parquet),
            str(tmp_path / "out.parquet"),
            resolution=5,
            memory_limit="700MB",
        )
        assert connection_spy[0]["memory_limit"] == _duckdb_renders("700MB")


class TestThreadsAndOrdering:
    @pytest.mark.skipif((os.cpu_count() or 1) <= 3, reason="needs more than 3 cores to shrink to 3")
    def test_threads_shrink_with_a_small_limit(self, points_parquet, tmp_path, connection_spy):
        """A small limit spread over many threads makes DuckDB raise instead of spill."""
        expected = (2 * 1000**3) // memory_limits._BYTES_PER_THREAD
        aggregate_by_a5(
            str(points_parquet),
            str(tmp_path / "out.parquet"),
            resolution=5,
            memory_limit="2GB",
        )
        assert connection_spy[0]["threads"] == expected

    def test_a_generous_limit_keeps_every_thread(
        self, points_parquet, tmp_path, monkeypatch, connection_spy
    ):
        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: 4096 * GIB)
        aggregate_by_a5(str(points_parquet), str(tmp_path / "out.parquet"), resolution=5)
        assert connection_spy[0]["threads"] == int(_duckdb_default("threads"))

    def test_row_order_is_not_preserved(self, points_parquet, tmp_path, connection_spy):
        """The output order comes from the GROUP BY, so buffering for it is waste."""
        aggregate_by_a5(str(points_parquet), str(tmp_path / "out.parquet"), resolution=5)
        assert connection_spy[0]["preserve_insertion_order"] is False


# ---------------------------------------------------------------------------
# The CLI flag
# ---------------------------------------------------------------------------


class TestTheCliFlag:
    @pytest.mark.parametrize("scheme", ["a5", "h3"])
    def test_grid_command_reports_the_limit(self, points_parquet, tmp_path, scheme):
        result = CliRunner().invoke(
            cli,
            [
                "process",
                "aggregate",
                scheme,
                str(points_parquet),
                str(tmp_path / f"{scheme}.parquet"),
                "--resolution",
                "5",
                "--write-memory",
                "523MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 523MB" in result.output

    def test_overview_reports_the_limit(self, cells_parquet, tmp_path):
        result = CliRunner().invoke(
            cli,
            [
                "process",
                "overview",
                str(cells_parquet),
                "--levels",
                "3",
                "--output-dir",
                str(tmp_path / "ov"),
                "--write-memory",
                "523MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 523MB" in result.output

    @pytest.mark.parametrize(
        "args",
        [
            ["process", "aggregate", "a5"],
            ["process", "aggregate", "h3"],
            ["process", "aggregate", "admin"],
            ["process", "overview"],
        ],
        ids=["a5", "h3", "admin", "overview"],
    )
    def test_command_declares_the_flag(self, args):
        result = CliRunner().invoke(cli, [*args, "--help"])
        assert result.exit_code == 0, result.output
        assert "--write-memory" in result.output

    def test_a_bad_value_is_refused_before_any_work(self, points_parquet, tmp_path):
        result = CliRunner().invoke(
            cli,
            [
                "process",
                "aggregate",
                "a5",
                str(points_parquet),
                str(tmp_path / "out.parquet"),
                "--resolution",
                "5",
                "--write-memory",
                "512MB'; ATTACH 'evil.db",
            ],
        )
        assert result.exit_code != 0
        assert not (tmp_path / "out.parquet").exists()
