"""The plain-COPY write path is memory-bounded like the duckdb-kv strategy (#1153).

`gpio convert geoparquet big.parquet out.parquet --geoparquet-version 2.0` on a
91 GB input was OOM-killed at a 120 GiB Slurm cgroup cap. A 2.0 write takes the
funnel's plain-COPY fast path, which never set a memory limit: `--write-memory`
was silently dropped there, and DuckDB ran with its own default of 80% of the
cap. DuckDB also allocates outside that limit (measured 30-40% on a Hilbert
sort), so the cap was reached before DuckDB ever spilled or raised.

These tests pin the pieces of the fix: the fast path honours `--write-memory`,
every write defaults to a limit that leaves headroom under the machine's memory
ceiling (never a share of "free" memory, which in a job cgroup counts page
cache), that ceiling follows the process's own cgroup (Slurm puts a job in a
nested cgroup; the root cgroup has no limit), threads shrink with the limit so
DuckDB spills instead of raising, the caller's own session settings survive the
write, and convert sorts the input once, inside that limit, instead of also
sorting it to count invalid geometries.
"""

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core import memory_limits
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, restore_duckdb_settings, sql_path
from geoparquet_io.core.write_funnels import write_parquet_with_metadata

GIB = 1024**3


def _points_parquet(con, path: str, rows: int = 2000) -> str:
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(
        f"""COPY (SELECT i AS id, ST_Point((i % 500) * 0.001, (i % 499) * 0.001) AS geometry
            FROM range({rows}) t(i))
            TO {sql_path(path)} (FORMAT PARQUET)"""
    )
    return f"SELECT * FROM read_parquet({sql_path(path)})"


def _setting(con, key):
    return con.execute(f"SELECT current_setting('{key}')").fetchone()[0]


@pytest.fixture
def con():
    connection = get_duckdb_connection()
    yield connection
    connection.close()


class TestFastPathMemoryLimit:
    def test_convert_to_2_0_write_memory_is_used(self, test_data_dir, tmp_path):
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

    def test_fast_path_limit_is_enforced(self, tmp_path, con):
        """The limit is really SET for the COPY, not only logged."""
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
        self, tmp_path, monkeypatch, caplog, con
    ):
        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: 10 * GIB)
        query = _points_parquet(con, str(tmp_path / "src.parquet"))
        write_parquet_with_metadata(
            con, query, str(tmp_path / "out.parquet"), geoparquet_version="2.0", verbose=True
        )
        assert "DuckDB memory limit: 5.0GB" in caplog.text

    def test_small_ceiling_formats_in_megabytes(self, monkeypatch):
        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: GIB)
        assert memory_limits.default_memory_limit() == "512MB"

    def test_unknown_ceiling_leaves_duckdb_default(self, tmp_path, monkeypatch, caplog, con):
        """No ceiling to measure: keep DuckDB's own limit rather than invent one."""
        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: None)
        query = _points_parquet(con, str(tmp_path / "src.parquet"))
        write_parquet_with_metadata(
            con, query, str(tmp_path / "out.parquet"), geoparquet_version="2.0", verbose=True
        )
        assert "DuckDB memory limit" not in caplog.text
        assert (tmp_path / "out.parquet").exists()

    def test_limit_holds_during_the_write_and_is_restored_after(self, con):
        """The connection is the caller's; one write must not leave its limit behind."""
        before = _setting(con, "memory_limit")
        with memory_limits.scoped_write_memory_limit(con, "700MB", verbose=False):
            assert _setting(con, "memory_limit") == "667.5 MiB"
        assert _setting(con, "memory_limit") == before

    @pytest.mark.parametrize("caller_limit", ["3GB", "1000MB", "123456789B"])
    def test_caller_set_limit_survives_a_write(self, tmp_path, con, caller_limit):
        """DuckDB displays sizes truncated, so a naive restore reset them to its default."""
        query = _points_parquet(con, str(tmp_path / "src.parquet"))
        con.execute(f"SET memory_limit = '{caller_limit}'")
        before = _setting(con, "memory_limit")
        for i in range(3):  # a partition loop: no drift from one write to the next
            write_parquet_with_metadata(
                con,
                query,
                str(tmp_path / f"out{i}.parquet"),
                geoparquet_version="2.0",
                memory_limit="700MB",
            )
            assert _setting(con, "memory_limit") == before

    def test_default_does_not_loosen_a_stricter_caller_limit(self, con, monkeypatch):
        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: 100 * GIB)
        con.execute("SET memory_limit = '1GB'")
        with memory_limits.scoped_write_memory_limit(con, None, verbose=False):
            assert _setting(con, "memory_limit") == "953.6 MiB"

    def test_default_tightens_duckdbs_own_default(self, con, monkeypatch):
        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: GIB)
        with memory_limits.scoped_write_memory_limit(con, None, verbose=False):
            assert _setting(con, "memory_limit") == "488.2 MiB"  # 512MB

    def test_threads_shrink_with_the_limit_and_come_back(self, con):
        """Under ~250MB a thread, DuckDB raises OutOfMemory instead of spilling."""
        con.execute("SET threads = 8")
        with memory_limits.scoped_write_memory_limit(con, "2GiB", verbose=False):
            assert _setting(con, "threads") == 4
        assert _setting(con, "threads") == 8

    def test_threads_untouched_when_the_limit_affords_them(self, con):
        con.execute("SET threads = 2")
        with memory_limits.scoped_write_memory_limit(con, "8GiB", verbose=False):
            assert _setting(con, "threads") == 2

    def test_settings_restored_when_the_write_fails(self, con):
        con.execute("SET threads = 8")
        before = _setting(con, "memory_limit")
        with pytest.raises(RuntimeError):
            with memory_limits.scoped_write_memory_limit(con, "600MB", verbose=False):
                raise RuntimeError("boom")
        assert (_setting(con, "memory_limit"), _setting(con, "threads")) == (before, 8)


class TestRestoreSettings:
    def test_engine_default_comes_back(self, con):
        before = _setting(con, "memory_limit")
        con.execute("SET memory_limit = '1GB'")
        restore_duckdb_settings(con, {"memory_limit": before})
        assert _setting(con, "memory_limit") == before

    def test_setting_names_are_checked(self, con):
        with pytest.raises(ValueError, match="setting name"):
            restore_duckdb_settings(con, {"threads; DROP TABLE x": 1})


class TestParseSize:
    @pytest.mark.parametrize(
        "text, expected",
        [
            ("2GB", 2 * 10**9),
            ("14.3 GiB", int(14.3 * GIB)),
            ("512MB", 512 * 10**6),
            ("0 bytes", 0),
            ("1.5GIB", int(1.5 * GIB)),
            ("lots", None),
            ("5 parsecs", None),
        ],
    )
    def test_units(self, text, expected):
        assert memory_limits.parse_size(text) == expected


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
        assert memory_limits._cgroup_limit(proc, root) == 120 * GIB

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
        assert memory_limits._cgroup_limit(proc, root) == 120 * GIB

    def test_tightest_ancestor_wins(self, tmp_path):
        proc, root = _fake_cgroups(
            tmp_path,
            ["0::/outer/inner"],
            {"outer/memory.max": str(64 * GIB), "outer/inner/memory.max": str(8 * GIB)},
        )
        assert memory_limits._cgroup_limit(proc, root) == 8 * GIB

    def test_container_root_cgroup_still_detected(self, tmp_path):
        """A cgroup namespace (Docker, Kubernetes) shows the process at the root."""
        proc, root = _fake_cgroups(tmp_path, ["0::/"], {"memory.max": str(4 * GIB)})
        assert memory_limits._cgroup_limit(proc, root) == 4 * GIB

    def test_no_cgroup_information(self, tmp_path):
        assert (
            memory_limits._cgroup_limit(str(tmp_path / "missing"), str(tmp_path / "missing_root"))
            is None
        )

    def test_unlimited_everywhere(self, tmp_path):
        proc, root = _fake_cgroups(
            tmp_path, ["0::/a"], {"memory.max": "max", "a/memory.max": "max"}
        )
        assert memory_limits._cgroup_limit(proc, root) is None

    def _point_at(self, monkeypatch, proc, root, ram):
        monkeypatch.setattr(memory_limits, "_PROC_SELF_CGROUP", proc)
        monkeypatch.setattr(memory_limits, "_CGROUP_ROOT", root)
        monkeypatch.setattr(
            memory_limits.psutil, "virtual_memory", lambda: SimpleNamespace(total=ram)
        )

    def test_ceiling_is_the_cgroup_when_it_is_lower(self, tmp_path, monkeypatch):
        proc, root = _fake_cgroups(tmp_path, ["0::/job"], {"job/memory.max": str(2 * GIB)})
        self._point_at(monkeypatch, proc, root, ram=64 * GIB)
        assert memory_limits.memory_ceiling() == 2 * GIB

    def test_ceiling_is_ram_when_it_is_lower(self, tmp_path, monkeypatch):
        proc, root = _fake_cgroups(tmp_path, ["0::/job"], {"job/memory.max": str(64 * GIB)})
        self._point_at(monkeypatch, proc, root, ram=8 * GIB)
        assert memory_limits.memory_ceiling() == 8 * GIB

    def test_busy_job_cgroup_still_gets_half_its_cap(self, tmp_path, monkeypatch):
        """A Slurm job that has read its input shows usage at the cap (page cache).

        The duckdb-kv and partition default used to be half of limit - usage,
        which collapsed to the 128MB floor and failed a 1.x convert (#1153).
        """
        proc, root = _fake_cgroups(
            tmp_path,
            ["0::/slurm/job_1/step_batch"],
            {
                "slurm/job_1/memory.max": str(120 * GIB),
                "slurm/job_1/memory.current": str(int(119.9 * GIB)),
            },
        )
        self._point_at(monkeypatch, proc, root, ram=512 * GIB)
        assert memory_limits.get_default_memory_limit() == "60.0GB"


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


class TestDirectCopyMemoryLimit:
    """Direct ``COPY ... TO`` sites run inside ``scoped_write_memory_limit`` (#1156).

    The admin dataset cache downloads, the bbox-metadata rewrite fallback and
    duckdb-kv's no-geometry write bypassed every limit -- DuckDB's own default,
    80% of RAM and blind to a Slurm job cgroup (#1153). The single-pass
    PARTITION_BY staging COPY (partition string/index drivers, admin
    hierarchical, pmtiles chunking) already set the ceiling-based limit, but
    left it -- with ``threads = 1`` -- on the caller's connection and loosened a
    stricter limit the caller had set; it now restores both.
    """

    @staticmethod
    def _spy_scoped_limit(monkeypatch, module, record):
        """Route ``module``'s scoped_write_memory_limit through a recorder.

        Records ``(memory_limit_arg, displayed_limit, threads)`` as they stand
        right after the wrapped COPY ran, before the scope restores anything.
        """
        from contextlib import contextmanager

        real = module.scoped_write_memory_limit

        @contextmanager
        def wrapper(con, memory_limit, verbose, **kwargs):
            with real(con, memory_limit, verbose, **kwargs):
                try:
                    yield
                finally:
                    record.append(
                        (
                            memory_limit,
                            str(_setting(con, "memory_limit")),
                            int(str(_setting(con, "threads"))),
                        )
                    )

        monkeypatch.setattr(module, "scoped_write_memory_limit", wrapper)

    def test_partition_staging_copy_holds_the_limit_and_restores_settings(
        self, tmp_path, con, monkeypatch
    ):
        """--write-memory bounds the PARTITION_BY COPY; the caller's session survives it."""
        from geoparquet_io.core.partition import staging

        query = _points_parquet(con, str(tmp_path / "src.parquet"))
        con.execute("SET threads = 4")
        keys = ("memory_limit", "threads", "preserve_insertion_order")
        before = {key: _setting(con, key) for key in keys}
        seen = []
        self._spy_scoped_limit(monkeypatch, staging, seen)

        staging.run_partitioned_copy(
            con,
            f"SELECT *, id % 3 AS part FROM ({query})",
            ["part"],
            str(tmp_path / "staging"),
            True,  # the verbose COPY echo moved inside the scope with the COPY
            "700MB",
        )

        assert seen == [("700MB", "667.5 MiB", 1)]
        assert {key: _setting(con, key) for key in keys} == before
        parts = sorted(p.name for p in (tmp_path / "staging").iterdir())
        assert parts == ["part=0", "part=1", "part=2"]

    def test_pmtiles_split_copy_is_memory_bounded(self, tmp_path, monkeypatch, test_data_dir):
        """pmtiles chunking has no --write-memory, so the ceiling-based default applies."""
        from geoparquet_io.core import pmtiles_chunks as pc
        from geoparquet_io.core.common import get_dataset_bounds
        from geoparquet_io.core.partition import staging

        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: 10 * GIB)
        seen = []
        self._spy_scoped_limit(monkeypatch, staging, seen)

        buildings = str(test_data_dir / "buildings_test.parquet")
        parts_dir = tmp_path / "out.pmtiles.parts"
        parts_dir.mkdir()
        pc._split_input(
            buildings,
            str(parts_dir),
            geometry_column="geometry",
            key_sql=pc.chunk_key_sql(
                "geometry", tuple(get_dataset_bounds(buildings, geometry_column="geometry")), 2, 2
            ),
            where=None,
            projection="*",
            scratch=str(tmp_path),
            verbose=False,
        )

        ((memory_limit_arg, during, _threads),) = seen
        assert memory_limit_arg is None
        limit_bytes = memory_limits.parse_size(during)
        assert limit_bytes is not None and limit_bytes <= 5 * 10**9  # half the ceiling
        assert list(parts_dir.glob("chunk_*.parquet"))

    @pytest.mark.parametrize("write_memory", [None, "700MB"])
    def test_admin_cache_download_copy_is_memory_bounded(
        self, tmp_path, monkeypatch, con, write_memory
    ):
        """The cache download COPY of a multi-GB admin dataset is bounded too,
        by --write-memory when the command was given one."""
        from geoparquet_io.core import admin_datasets as ad

        _points_parquet(con, str(tmp_path / "src.parquet"))
        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: 10 * GIB)
        dataset = ad.CurrentAdminDataset()
        dataset.memory_limit = write_memory
        monkeypatch.setattr(dataset, "get_default_source", lambda: str(tmp_path / "src.parquet"))
        monkeypatch.setattr(dataset, "get_s3_config", lambda: {})
        # No remote read in this test: keep the connection free of httpfs.
        monkeypatch.setattr(ad, "get_duckdb_connection", lambda **kwargs: get_duckdb_connection())
        seen = []
        self._spy_scoped_limit(monkeypatch, ad, seen)

        cache_path = tmp_path / "cache" / "dataset.parquet"
        assert dataset._download_to_cache(cache_path) == cache_path
        assert cache_path.exists()
        ((memory_limit_arg, during, _threads),) = seen
        assert memory_limit_arg == write_memory
        if write_memory is not None:
            assert during == "667.5 MiB"
        limit_bytes = memory_limits.parse_size(during)
        assert limit_bytes is not None and limit_bytes <= 5 * 10**9

    def test_admin_per_level_cache_copy_is_memory_bounded(self, tmp_path, monkeypatch):
        """The Overture per-level cache COPY runs inside the scoped limit too.

        Fully offline: the connection is a mock, so this pins that each level's
        COPY happens inside one ``scoped_write_memory_limit`` scope with no
        explicit limit (the ceiling-based default), not what DuckDB does with it.
        """
        from contextlib import contextmanager
        from unittest.mock import MagicMock

        from geoparquet_io.core import admin_datasets as ad

        monkeypatch.setattr(ad, "get_cache_dir", lambda: tmp_path)
        mock_con = MagicMock()
        monkeypatch.setattr(ad, "get_duckdb_connection", lambda **kwargs: mock_con)
        dataset = ad.OvertureAdminDataset()
        monkeypatch.setattr(dataset, "get_version", lambda: "2026-07-22.0")
        monkeypatch.setattr(dataset, "get_default_source", lambda: "s3://stub/divisions")

        scopes = []

        @contextmanager
        def fake_scope(con, memory_limit, verbose):
            before = mock_con.execute.call_count
            yield
            during = mock_con.execute.call_args_list[before:]
            assert any("COPY" in str(call.args[0]) for call in during), (
                "no COPY ran inside the memory-limit scope"
            )
            scopes.append(memory_limit)

        monkeypatch.setattr(ad, "scoped_write_memory_limit", fake_scope)

        dataset._download_per_level_caches()

        assert scopes == [None] * len(dataset.get_available_levels())

    def test_bbox_metadata_rewrite_fallback_is_memory_bounded(self, monkeypatch, places_v11_file):
        """The full-file rewrite behind an unpatchable footer is bounded too."""
        from geoparquet_io.core.add import bbox_metadata as bm
        from geoparquet_io.core.parquet_footer import FooterPatchUnsupported

        def refuse(*args, **kwargs):
            raise FooterPatchUnsupported("forced for the test")

        monkeypatch.setattr(bm, "patch_footer_kv", refuse)
        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: 10 * GIB)
        seen = []
        self._spy_scoped_limit(monkeypatch, bm, seen)

        bm.add_bbox_metadata(places_v11_file)

        ((memory_limit_arg, during, _threads),) = seen
        assert memory_limit_arg is None
        limit_bytes = memory_limits.parse_size(during)
        assert limit_bytes is not None and limit_bytes <= 5 * 10**9


class TestDirectCopyRegressions:
    """What #1156's restore exposed, and what it must keep doing."""

    def test_a_stricter_caller_limit_survives_the_staging_copy(self, tmp_path, con):
        """The split must not loosen a limit the caller set (main set the default)."""
        from geoparquet_io.core.partition import staging

        query = _points_parquet(con, str(tmp_path / "src.parquet"))
        con.execute("SET memory_limit = '300MB'")
        before = _setting(con, "memory_limit")
        during = []
        real_execute = con.execute

        class Spy:
            def execute(self, sql, *args, **kwargs):
                if sql.lstrip().upper().startswith("COPY"):
                    during.append(
                        real_execute("SELECT current_setting('memory_limit')").fetchone()[0]
                    )
                return real_execute(sql, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(con, name)

        staging.run_partitioned_copy(
            Spy(),
            f"SELECT *, id % 3 AS part FROM ({query})",
            ["part"],
            str(tmp_path / "staging"),
            False,
        )

        assert during == [before]
        assert _setting(con, "memory_limit") == before

    def test_the_no_geometry_duckdb_kv_write_honours_write_memory(self, tmp_path, monkeypatch):
        """Partitioning a table with no geometry: the finalize writes are bounded.

        They used to inherit the staging COPY's leaked settings; once those are
        restored, duckdb-kv's no-geometry branch must bound its own COPY.
        """
        from geoparquet_io.core.partition.common import partition_by_column
        from geoparquet_io.core.write_strategies import duckdb_kv

        source = tmp_path / "plain.parquet"
        con = get_duckdb_connection()
        try:
            con.execute(
                f"""COPY (SELECT i AS id, CAST(i % 3 AS VARCHAR) AS cat FROM range(300) t(i))
                    TO {sql_path(str(source))} (FORMAT PARQUET)"""
            )
        finally:
            con.close()
        seen = []
        TestDirectCopyMemoryLimit._spy_scoped_limit(monkeypatch, duckdb_kv, seen)

        partition_by_column(
            str(source),
            str(tmp_path / "out"),
            column_name="cat",
            skip_analysis=True,
            memory_limit="700MB",
        )

        assert seen, "the no-geometry write ran outside scoped_write_memory_limit"
        assert {(arg, during) for arg, during, _threads in seen} == {("700MB", "667.5 MiB")}

    def test_partition_admin_hands_write_memory_to_the_dataset(
        self, tmp_path, monkeypatch, places_test_file
    ):
        """--write-memory reaches the admin dataset whose downloads it bounds."""
        from geoparquet_io.core.partition import admin_hierarchical as ah

        class Stop(Exception):
            pass

        dataset = SimpleNamespace(memory_limit=None)
        monkeypatch.setattr(ah, "_setup_admin_dataset", lambda *a, **k: (dataset, ["country"]))

        def stop(*args, **kwargs):
            raise Stop

        monkeypatch.setattr(ah, "_get_input_file_info", stop)

        with pytest.raises(Stop):
            ah.partition_by_admin_hierarchical(
                places_test_file,
                str(tmp_path / "out"),
                dataset_name="gaul",
                levels=["country"],
                memory_limit="700MB",
            )
        assert dataset.memory_limit == "700MB"

    def test_add_admin_divisions_hands_write_memory_to_the_dataset(
        self, monkeypatch, places_test_file
    ):
        """The same for `add admin-divisions`, before its first download."""
        from geoparquet_io.core.add import admin_divisions as ad

        class Stop(Exception):
            pass

        seen = []

        class FakeDataset:
            memory_limit = None

            def get_dataset_name(self):
                return "fake"

            def validate_levels(self, levels):
                seen.append(self.memory_limit)
                raise Stop

        monkeypatch.setattr(ad.AdminDatasetFactory, "create", lambda *a, **k: FakeDataset())

        with pytest.raises(Stop):
            ad._setup_dataset_and_columns(
                places_test_file, "gaul", None, ["country"], False, memory_limit="700MB"
            )
        assert seen == ["700MB"]
