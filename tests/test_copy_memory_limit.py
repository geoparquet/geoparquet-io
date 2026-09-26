"""The plain-COPY write path is memory-bounded like the duckdb-kv strategy (#1153).

`gpio convert geoparquet big.parquet out.parquet --geoparquet-version 2.0` on a
91 GB input was OOM-killed at a 120 GiB Slurm cgroup cap. A 2.0 write takes the
funnel's plain-COPY fast path, which never set a memory limit: `--write-memory`
was silently dropped there, and DuckDB ran with its own default of 80% of the
cap. DuckDB also allocates outside that limit (measured 30-40% on a Hilbert
sort), so the cap was reached before DuckDB ever spilled or raised.

These tests pin the four pieces of the fix: the fast path honours
`--write-memory`, it defaults to a limit that leaves headroom under the
machine's memory ceiling, that ceiling follows the process's own cgroup (Slurm
puts a job in a nested cgroup; the root cgroup has no limit), and convert sorts
the input once, inside that limit, instead of also sorting it to count invalid
geometries.
"""

from __future__ import annotations

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.write_strategies import duckdb_kv

GIB = 1024**3


def _points_parquet(con, path: str, rows: int = 2000) -> str:
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(
        f"""COPY (SELECT i AS id, ST_Point((i % 500) * 0.001, (i % 499) * 0.001) AS geometry
            FROM range({rows}) t(i))
            TO '{path}' (FORMAT PARQUET)"""
    )
    return f"SELECT * FROM read_parquet('{path}')"


class TestFastPathMemoryLimit:
    def test_convert_to_2_0_write_memory_reaches_engine(self, test_data_dir, tmp_path):
        """2.0 output skips the duckdb-kv strategy; the flag must not vanish there."""
        result = CliRunner().invoke(
            cli,
            [
                "convert",
                "geoparquet",
                str(test_data_dir / "buildings_test.geojson"),
                str(tmp_path / "out.parquet"),
                "--geoparquet-version",
                "2.0",
                "--write-memory",
                "523MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 523MB" in result.output

    def test_fast_path_limit_is_enforced(self, tmp_path):
        """The limit is really SET for the COPY, not only logged."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.write_funnels import write_parquet_with_metadata

        con = get_duckdb_connection()
        query = _points_parquet(con, str(tmp_path / "src.parquet"), rows=200_000)
        with pytest.raises(duckdb.OutOfMemoryException):
            write_parquet_with_metadata(
                con,
                f"{query} ORDER BY id DESC",
                str(tmp_path / "out.parquet"),
                geoparquet_version="2.0",
                memory_limit="1MB",
            )

    def test_fast_path_default_leaves_headroom_under_the_ceiling(
        self, tmp_path, monkeypatch, caplog
    ):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.write_funnels import write_parquet_with_metadata

        monkeypatch.setattr(duckdb_kv, "memory_ceiling", lambda: 10 * GIB)
        con = get_duckdb_connection()
        query = _points_parquet(con, str(tmp_path / "src.parquet"))
        write_parquet_with_metadata(
            con, query, str(tmp_path / "out.parquet"), geoparquet_version="2.0", verbose=True
        )
        assert "DuckDB memory limit: 5.0GB" in caplog.text

    def test_small_ceiling_formats_in_megabytes(self, monkeypatch):
        monkeypatch.setattr(duckdb_kv, "memory_ceiling", lambda: GIB)
        assert duckdb_kv.default_copy_memory_limit() == "512MB"

    def test_unknown_ceiling_leaves_duckdb_default(self, tmp_path, monkeypatch, caplog):
        """No ceiling to measure: keep DuckDB's own limit rather than invent one."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.write_funnels import write_parquet_with_metadata

        monkeypatch.setattr(duckdb_kv, "memory_ceiling", lambda: None)
        con = get_duckdb_connection()
        query = _points_parquet(con, str(tmp_path / "src.parquet"))
        write_parquet_with_metadata(
            con, query, str(tmp_path / "out.parquet"), geoparquet_version="2.0", verbose=True
        )
        assert "DuckDB memory limit" not in caplog.text
        assert (tmp_path / "out.parquet").exists()

    def test_fast_path_restores_the_session_limit(self, tmp_path):
        """The connection is the caller's; one write must not leave its limit behind."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.write_funnels import write_parquet_with_metadata

        con = get_duckdb_connection()
        query = _points_parquet(con, str(tmp_path / "src.parquet"))

        def current():
            return con.execute("SELECT current_setting('memory_limit')").fetchone()[0]

        before = current()
        write_parquet_with_metadata(
            con,
            query,
            str(tmp_path / "out.parquet"),
            geoparquet_version="2.0",
            memory_limit="700MB",
        )
        assert current() == before


def _fake_cgroups(tmp_path, proc_lines: list[str], files: dict[str, str]):
    proc = tmp_path / "proc_self_cgroup"
    proc.write_text("\n".join(proc_lines) + "\n")
    root = tmp_path / "cgroup"
    for rel, value in files.items():
        f = root / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(value + "\n")
    return str(proc), str(root)


class TestCgroupMemory:
    def test_v2_limit_on_an_ancestor_of_the_process_cgroup(self, tmp_path):
        """Slurm (cgroup v2): the job cgroup carries the cap, the task cgroup says max."""
        proc, root = _fake_cgroups(
            tmp_path,
            ["0::/system.slice/slurmstepd.scope/job_195428/step_batch/user/task_0"],
            {
                "memory.max": "max",
                "system.slice/slurmstepd.scope/job_195428/memory.max": str(120 * GIB),
                "system.slice/slurmstepd.scope/job_195428/memory.current": str(20 * GIB),
                "system.slice/slurmstepd.scope/job_195428/step_batch/user/task_0/memory.max": (
                    "max"
                ),
            },
        )
        assert duckdb_kv._cgroup_memory(proc, root) == (120 * GIB, 20 * GIB)

    def test_v1_memory_controller_path(self, tmp_path):
        """Slurm on Rocky 8 (cgroup v1): the limit lives under the memory hierarchy."""
        proc, root = _fake_cgroups(
            tmp_path,
            [
                "11:cpuset:/slurm/uid_1000/job_195428/step_batch",
                "4:memory:/slurm/uid_1000/job_195428/step_batch",
                "1:name=systemd:/system.slice/slurmd.service",
            ],
            {
                "memory/memory.limit_in_bytes": str(2**63 - 4096),
                "memory/slurm/uid_1000/job_195428/memory.limit_in_bytes": str(120 * GIB),
                "memory/slurm/uid_1000/job_195428/memory.usage_in_bytes": str(GIB),
                "memory/slurm/uid_1000/job_195428/step_batch/memory.limit_in_bytes": str(
                    2**63 - 4096
                ),
            },
        )
        assert duckdb_kv._cgroup_memory(proc, root) == (120 * GIB, GIB)

    def test_tightest_ancestor_wins(self, tmp_path):
        proc, root = _fake_cgroups(
            tmp_path,
            ["0::/outer/inner"],
            {"outer/memory.max": str(64 * GIB), "outer/inner/memory.max": str(8 * GIB)},
        )
        assert duckdb_kv._cgroup_memory(proc, root) == (8 * GIB, None)

    def test_container_root_cgroup_still_detected(self, tmp_path):
        """A cgroup namespace (Docker, Kubernetes) shows the process at the root."""
        proc, root = _fake_cgroups(tmp_path, ["0::/"], {"memory.max": str(4 * GIB)})
        assert duckdb_kv._cgroup_memory(proc, root) == (4 * GIB, None)

    def test_no_cgroup_information(self, tmp_path):
        assert (
            duckdb_kv._cgroup_memory(str(tmp_path / "missing"), str(tmp_path / "missing_root"))
            is None
        )

    def test_unlimited_everywhere(self, tmp_path):
        proc, root = _fake_cgroups(
            tmp_path, ["0::/a"], {"memory.max": "max", "a/memory.max": "max"}
        )
        assert duckdb_kv._cgroup_memory(proc, root) is None

    def test_available_memory_uses_the_process_cgroup(self, tmp_path, monkeypatch):
        proc, root = _fake_cgroups(
            tmp_path,
            ["0::/job"],
            {"job/memory.max": str(16 * GIB), "job/memory.current": str(6 * GIB)},
        )
        monkeypatch.setattr(duckdb_kv, "_PROC_SELF_CGROUP", proc)
        monkeypatch.setattr(duckdb_kv, "_CGROUP_ROOT", root)
        assert duckdb_kv._get_available_memory() == 10 * GIB

    def test_ceiling_is_the_smaller_of_cgroup_and_ram(self, tmp_path, monkeypatch):
        import psutil

        proc, root = _fake_cgroups(tmp_path, ["0::/job"], {"job/memory.max": str(2 * GIB)})
        monkeypatch.setattr(duckdb_kv, "_PROC_SELF_CGROUP", proc)
        monkeypatch.setattr(duckdb_kv, "_CGROUP_ROOT", root)
        assert duckdb_kv.memory_ceiling() == min(2 * GIB, psutil.virtual_memory().total)


class TestConvertSortsOnce:
    """The invalid-geometry count ran over the Hilbert-ordered query (#1153).

    DuckDB keeps an ORDER BY inside a COUNT subquery, so convert sorted the
    whole input once to count invalid geometries -- outside any memory limit --
    and again to write it.
    """

    @pytest.mark.parametrize(
        "source", ["buildings_test.geojson", "buildings_test.parquet", "points_geometry.csv"]
    )
    def test_repair_count_query_is_not_sorted(self, test_data_dir, tmp_path, monkeypatch, source):
        from geoparquet_io.core import convert as convert_mod
        from geoparquet_io.core.geometry_repair import repair_query_geometry

        src = test_data_dir / source
        seen = []

        def spy(con, query, geometry_column, *, repair=True):
            seen.append(query)
            return repair_query_geometry(con, query, geometry_column, repair=repair)

        monkeypatch.setattr(convert_mod, "repair_query_geometry", spy)
        out = tmp_path / "out.parquet"
        convert_mod.convert_to_geoparquet(str(src), str(out), geoparquet_version="2.0")
        assert seen, "repair_query_geometry was not called"
        assert all("ORDER BY" not in q.upper() for q in seen)
        assert out.exists()
