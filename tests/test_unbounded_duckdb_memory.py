"""Every DuckDB connection gpio opens is memory-bounded, and no write loosens a
stricter caller limit (#1174).

#1156 (fixed by #1166) bounded the direct ``COPY ... TO`` sites. The DuckDB work
*around* those statements still ran at DuckDB's own default of 80% of host RAM, blind
to a Slurm job cgroup: ``gpio partition admin``'s enrichment join (measured at
14.3 GiB / 12 threads under ``--write-memory 700MB``), the same join behind
``gpio add admin-divisions``, ``disk-rewrite``'s first-phase COPY and the format
exports behind ``gpio convert csv``/``flatgeobuf``/...

And duckdb-kv did the opposite of the documented contract: it SET the
percentage default unconditionally, so a Python API caller who had set
``memory_limit = '300MB'`` on their own connection had it *raised* for the
duration of the write.

These tests pin: the ceiling-based default on every connection
``get_duckdb_connection`` opens, an explicit value still winning, the admin
joins honouring ``--write-memory``, the remaining COPY sites running inside the
shared scope, and a stricter caller limit surviving a duckdb-kv write.
"""

from __future__ import annotations

from contextlib import contextmanager

import duckdb
import pytest

from geoparquet_io.core import memory_limits
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
from geoparquet_io.core.write_funnels import write_parquet_with_metadata

GIB = 1024**3


def _setting(con, key):
    return con.execute(f"SELECT current_setting('{key}')").fetchone()[0]


def _points_parquet(con, path: str, rows: int = 500) -> str:
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(
        f"""COPY (SELECT i AS id, ST_Point((i % 50) * 0.001, (i % 49) * 0.001) AS geometry
            FROM range({rows}) t(i))
            TO {sql_path(path)} (FORMAT PARQUET)"""
    )
    return f"SELECT * FROM read_parquet({sql_path(path)})"


@pytest.fixture
def con():
    connection = get_duckdb_connection()
    yield connection
    connection.close()


@contextmanager
def _recording_scope(monkeypatch, module, record):
    """Record ``(memory_limit_arg, limit_during, threads_during)`` per scope entered.

    Wraps the real ``scoped_write_memory_limit`` as the module sees it, so the
    assertions are about what DuckDB actually had in force while the wrapped
    statement ran.
    """
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
    yield record


class _CopySpy:
    """A connection proxy that records a setting as each matching statement runs."""

    def __init__(self, con, keyword: str, key: str = "memory_limit"):
        self._con = con
        self._keyword = keyword.upper()
        self._key = key
        self.seen: list[object] = []

    def execute(self, sql, *args, **kwargs):
        if self._keyword in str(sql).upper():
            self.seen.append(_setting(self._con, self._key))
        return self._con.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._con, name)


class TestConnectionDefault:
    """The ceiling-based default is applied once, where connections are opened."""

    def test_a_new_connection_is_bounded_by_the_ceiling(self, monkeypatch):
        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: 10 * GIB)
        connection = get_duckdb_connection(load_spatial=False)
        try:
            limit = memory_limits.parse_size(str(_setting(connection, "memory_limit")))
        finally:
            connection.close()
        assert limit is not None and limit <= 5 * 10**9  # half the ceiling

    def test_an_explicit_limit_is_not_overridden(self, monkeypatch):
        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: 10 * GIB)
        connection = get_duckdb_connection(load_spatial=False, memory_limit="700MB")
        try:
            assert _setting(connection, "memory_limit") == "667.5 MiB"
        finally:
            connection.close()

    def test_unknown_ceiling_leaves_duckdbs_own_default(self, monkeypatch):
        """Nothing to measure: keep DuckDB's limit rather than invent one."""
        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: None)
        bare = duckdb.connect()
        connection = get_duckdb_connection(load_spatial=False)
        try:
            assert _setting(connection, "memory_limit") == _setting(bare, "memory_limit")
        finally:
            connection.close()
            bare.close()


class TestDuckDBKvRespectsTheCaller:
    """Sub-item 4: a write must never loosen a limit the caller set (correctness)."""

    def test_a_stricter_caller_limit_survives_the_write(self, tmp_path, con):
        query = _points_parquet(con, str(tmp_path / "src.parquet"))
        con.execute("SET memory_limit = '300MB'")
        before = _setting(con, "memory_limit")
        spy = _CopySpy(con, "COPY")

        write_parquet_with_metadata(
            spy, query, str(tmp_path / "out.parquet"), geoparquet_version="1.1"
        )

        assert spy.seen == [before]
        assert _setting(con, "memory_limit") == before

    def test_write_memory_still_wins_over_the_caller_setting(self, tmp_path, con):
        """Precedence: an explicit limit beats the connection's own setting."""
        query = _points_parquet(con, str(tmp_path / "src.parquet"))
        con.execute("SET memory_limit = '300MB'")
        before = _setting(con, "memory_limit")
        spy = _CopySpy(con, "COPY")

        write_parquet_with_metadata(
            spy,
            query,
            str(tmp_path / "out.parquet"),
            geoparquet_version="1.1",
            memory_limit="700MB",
        )

        assert spy.seen == ["667.5 MiB"]
        assert _setting(con, "memory_limit") == before

    def test_the_write_still_runs_single_threaded_and_restores_threads(self, tmp_path, con):
        """``threads = 1`` is pinned for memory control (DuckDB #8270)."""
        con.execute("SET threads = 3")
        query = _points_parquet(con, str(tmp_path / "src.parquet"))
        spy = _CopySpy(con, "COPY", key="threads")

        write_parquet_with_metadata(
            spy, query, str(tmp_path / "out.parquet"), geoparquet_version="1.1"
        )

        assert [int(str(v)) for v in spy.seen] == [1]
        assert int(str(_setting(con, "threads"))) == 3


def _admin_fixture(tmp_path, name="admin.parquet"):
    """An Overture-shaped, offline admin dataset: one country, two regions."""
    path = str(tmp_path / name)
    connection = duckdb.connect()
    connection.execute("INSTALL spatial; LOAD spatial;")
    connection.execute(
        f"""
        COPY (
            SELECT subtype, country, region, geometry,
                {{'xmin': ST_XMin(geometry), 'xmax': ST_XMax(geometry),
                  'ymin': ST_YMin(geometry), 'ymax': ST_YMax(geometry)}} AS bbox
            FROM (VALUES
                ('country', 'XX', NULL,
                 ST_GeomFromText('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))')),
                ('region', 'XX', 'XX-W',
                 ST_GeomFromText('POLYGON((0 0, 2 0, 2 4, 0 4, 0 0))')),
                ('region', 'XX', 'XX-E',
                 ST_GeomFromText('POLYGON((2 0, 4 0, 4 4, 2 4, 2 0))'))
            ) AS t(subtype, country, region, geometry)
        ) TO {sql_path(path)} (FORMAT PARQUET)
        """
    )
    connection.close()
    return path


def _points_input(tmp_path, name="input.parquet"):
    path = str(tmp_path / name)
    connection = duckdb.connect()
    connection.execute("INSTALL spatial; LOAD spatial;")
    connection.execute(
        f"""COPY (SELECT ST_Point(1 + (i % 2) * 2, 1 + (i % 3) * 0.5) AS geometry
            FROM range(20) t(i))
            TO {sql_path(path)} (FORMAT PARQUET)"""
    )
    connection.close()
    return path


class TestAdminJoinIsBounded:
    """The full-input spatial join ran before the bounded split and ignored it."""

    def test_partition_admin_single_source_join_honours_write_memory(self, tmp_path, monkeypatch):
        from geoparquet_io.core.admin_datasets import CurrentAdminDataset
        from geoparquet_io.core.partition import admin_hierarchical as ah

        admin_file = _admin_fixture(tmp_path)
        input_file = _points_input(tmp_path)
        monkeypatch.setattr(
            ah.AdminDatasetFactory,
            "create",
            staticmethod(
                lambda dataset_name, source_path=None, verbose=False: CurrentAdminDataset(
                    source_path=admin_file, verbose=verbose
                )
            ),
        )
        seen: list[tuple] = []
        with _recording_scope(monkeypatch, ah, seen):
            ah.partition_by_admin_hierarchical(
                input_file,
                str(tmp_path / "out"),
                dataset_name="current",
                levels=["country"],
                hive=True,
                memory_limit="700MB",
            )

        assert [(arg, during) for arg, during, _threads in seen] == [("700MB", "667.5 MiB")]

    def test_partition_admin_per_level_joins_honour_write_memory(self, tmp_path, monkeypatch):
        from geoparquet_io.core.admin_datasets import OvertureAdminDataset
        from geoparquet_io.core.partition import admin_hierarchical as ah

        admin_file = _admin_fixture(tmp_path)
        input_file = _points_input(tmp_path)
        monkeypatch.setattr(
            ah.AdminDatasetFactory,
            "create",
            staticmethod(
                lambda dataset_name, source_path=None, verbose=False: OvertureAdminDataset(
                    source_path=admin_file, verbose=verbose
                )
            ),
        )
        seen: list[tuple] = []
        with _recording_scope(monkeypatch, ah, seen):
            ah.partition_by_admin_hierarchical(
                input_file,
                str(tmp_path / "out"),
                dataset_name="overture",
                levels=["country", "region"],
                hive=True,
                memory_limit="700MB",
            )

        # One scope per level's CREATE TEMP TABLE join.
        assert [(arg, during) for arg, during, _threads in seen] == [("700MB", "667.5 MiB")] * 2

    def test_add_admin_divisions_per_level_temp_table_is_bounded(self, tmp_path, monkeypatch):
        """``gpio add admin-divisions`` chains the same join through temp tables."""
        from geoparquet_io.core.add import admin_divisions as ad

        admin_file = _admin_fixture(tmp_path)
        input_file = _points_input(tmp_path)
        seen: list[tuple] = []
        with _recording_scope(monkeypatch, ad, seen):
            ad.add_admin_divisions_multi(
                input_parquet=input_file,
                output_parquet=str(tmp_path / "out.parquet"),
                dataset_name="overture",
                levels=["country", "region"],
                dataset_source=admin_file,
                memory_limit="700MB",
            )

        # The last level writes through the funnel; the earlier one makes a temp table.
        assert [(arg, during) for arg, during, _threads in seen] == [("700MB", "667.5 MiB")]

    def test_the_join_runs_inside_the_scope_not_beside_it(self, tmp_path, monkeypatch):
        """Order matters: the limit must be in force while the join runs."""
        from unittest.mock import MagicMock

        from geoparquet_io.core.partition import admin_hierarchical as ah

        mock_con = MagicMock()
        inside: list[list[str]] = []

        @contextmanager
        def fake_scope(con, memory_limit, verbose):
            before = mock_con.execute.call_count
            yield
            inside.append([str(call.args[0]) for call in mock_con.execute.call_args_list[before:]])

        monkeypatch.setattr(ah, "scoped_write_memory_limit", fake_scope)
        ah._perform_enrichment_join(
            mock_con,
            "enriched",
            str(tmp_path / "in.parquet"),
            sql_path(str(tmp_path / "admin.parquet")),
            "",
            "b.country AS _admin_country",
            "geometry",
            None,
            ["country"],
            "geometry",
            None,
            memory_limit="700MB",
        )

        assert len(inside) == 1
        assert any("CREATE TEMP TABLE" in sql for sql in inside[0])


class TestRemainingCopySitesAreBounded:
    """disk-rewrite's first phase and the format exports (no ``--write-memory``)."""

    def test_disk_rewrite_first_phase_copy_is_bounded(self, tmp_path, monkeypatch, con):
        from geoparquet_io.core.write_strategies import disk_rewrite

        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: 10 * GIB)
        query = _points_parquet(con, str(tmp_path / "src.parquet"))
        seen: list[tuple] = []
        with _recording_scope(monkeypatch, disk_rewrite, seen):
            write_parquet_with_metadata(
                con,
                query,
                str(tmp_path / "out.parquet"),
                geoparquet_version="1.1",
                write_strategy="disk-rewrite",
            )

        assert seen, "the disk-rewrite COPY ran outside scoped_write_memory_limit"
        for arg, during, _threads in seen:
            assert arg is None
            limit = memory_limits.parse_size(during)
            assert limit is not None and limit <= 5 * 10**9

    @pytest.mark.parametrize("fmt", ["csv", "flatgeobuf"])
    def test_format_export_copy_is_bounded(self, tmp_path, monkeypatch, places_test_file, fmt):
        from geoparquet_io.core import format_writers

        monkeypatch.setattr(memory_limits, "memory_ceiling", lambda: 10 * GIB)
        seen: list[tuple] = []
        with _recording_scope(monkeypatch, format_writers, seen):
            format_writers.write_format(places_test_file, str(tmp_path / f"out.{fmt}"), format=fmt)

        assert seen, f"the {fmt} export COPY ran outside scoped_write_memory_limit"
        for arg, during, _threads in seen:
            assert arg is None
            limit = memory_limits.parse_size(during)
            assert limit is not None and limit <= 5 * 10**9
