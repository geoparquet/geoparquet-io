"""Tests for `gpio partition admin --vecorel`.

Vecorel mode mirrors `gpio add admin-divisions --vecorel`: it forces the
Overture dataset with country,region levels, names the admin columns with
Vecorel-compliant names, includes those columns in each output partition,
and writes Vecorel collection metadata.
"""

import json

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import partition
from geoparquet_io.core import duckdb_utils


class TestVecorelPartitionSelectClause:
    """Unit tests for the partition admin SELECT clause builder."""

    def test_non_vecorel_uses_temp_column_names(self):
        from geoparquet_io.core.admin_datasets import OvertureAdminDataset
        from geoparquet_io.core.partition.admin_hierarchical import (
            _build_admin_select_for_partitioning,
        )

        dataset = OvertureAdminDataset()
        levels = ["country", "region"]
        cols = dataset.get_partition_columns(levels)

        _clause, output_names = _build_admin_select_for_partitioning(levels, cols)

        assert output_names == ["_admin_country", "_admin_region"]

    def test_vecorel_uses_vecorel_column_names(self):
        from geoparquet_io.core.admin_datasets import OvertureAdminDataset
        from geoparquet_io.core.partition.admin_hierarchical import (
            _build_admin_select_for_partitioning,
        )

        dataset = OvertureAdminDataset()
        levels = ["country", "region"]
        cols = dataset.get_partition_columns(levels)

        clause, output_names = _build_admin_select_for_partitioning(
            levels, cols, dataset=dataset, vecorel=True
        )

        assert output_names == ["admin:country_code", "admin:subdivision_code"]
        assert '"admin:country_code"' in clause
        assert '"admin:subdivision_code"' in clause

    def test_vecorel_applies_region_transform(self):
        """Overture region codes must be stripped of the country prefix."""
        from geoparquet_io.core.admin_datasets import OvertureAdminDataset
        from geoparquet_io.core.partition.admin_hierarchical import (
            _build_admin_select_for_partitioning,
        )

        dataset = OvertureAdminDataset()
        levels = ["country", "region"]
        cols = dataset.get_partition_columns(levels)

        clause, _output_names = _build_admin_select_for_partitioning(
            levels, cols, dataset=dataset, vecorel=True
        )

        assert "split_part" in clause


class TestVecorelPartitionCLI:
    """CLI surface tests (no network)."""

    def test_requires_levels_or_vecorel(self, places_test_file, temp_output_dir):
        """Without --levels and without --vecorel the command should error."""
        runner = CliRunner()
        result = runner.invoke(partition, ["admin", places_test_file, temp_output_dir])
        assert result.exit_code != 0
        assert "vecorel" in result.output.lower() or "levels" in result.output.lower()


def _write_geoparquet(con, query, output_file):
    from geoparquet_io.core.write_funnels import write_parquet_with_metadata

    write_parquet_with_metadata(con, query, output_file)


@pytest.mark.integration
class TestVecorelPartitionLocalIntegration:
    """End-to-end vecorel partitioning against a small LOCAL Overture-shaped
    admin file (no network), by monkeypatching the dataset factory."""

    def test_vecorel_partitions_have_admin_columns_and_metadata(self, monkeypatch, temp_output_dir):
        import json
        import os
        from pathlib import Path

        import duckdb
        import pyarrow.parquet as pq

        from geoparquet_io.core.admin_datasets import OvertureAdminDataset
        from geoparquet_io.core.constants import VECOREL_ADMIN_SCHEMA
        from geoparquet_io.core.partition.admin_hierarchical import (
            partition_by_admin_hierarchical,
        )

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        admin_file = os.path.join(temp_output_dir, "admin.parquet")
        input_file = os.path.join(temp_output_dir, "input.parquet")
        out_dir = os.path.join(temp_output_dir, "out")

        # Overture-shaped admin file. Per-level joins read each level's own
        # polygons, so the country level needs a country-subtype polygon (real
        # Overture has these; region rows alone are not enough). One country
        # polygon covering both regions, plus the two region polygons.
        _write_geoparquet(
            con,
            """
            SELECT subtype, country, region, geometry,
                {'xmin': ST_XMin(geometry), 'xmax': ST_XMax(geometry),
                 'ymin': ST_YMin(geometry), 'ymax': ST_YMax(geometry)} AS bbox
            FROM (VALUES
                ('country', 'US', NULL,
                 ST_GeomFromText('POLYGON((0 0, 0 10, 20 10, 20 0, 0 0))')),
                ('region', 'US', 'US-CA',
                 ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')),
                ('region', 'US', 'US-NV',
                 ST_GeomFromText('POLYGON((10 0, 10 10, 20 10, 20 0, 10 0))'))
            ) AS t(subtype, country, region, geometry)
            """,
            admin_file,
        )

        # Input points: one inside each region
        _write_geoparquet(
            con,
            """
            SELECT id, geometry FROM (VALUES
                (1, ST_GeomFromText('POINT(5 5)')),
                (2, ST_GeomFromText('POINT(15 5)'))
            ) AS t(id, geometry)
            """,
            input_file,
        )

        def fake_create(dataset_name, source_path=None, verbose=False):
            return OvertureAdminDataset(source_path=admin_file, verbose=verbose)

        monkeypatch.setattr(
            "geoparquet_io.core.partition.admin_hierarchical.AdminDatasetFactory.create",
            staticmethod(fake_create),
        )

        count = partition_by_admin_hierarchical(
            input_file,
            out_dir,
            dataset_name="overture",
            levels=["country", "region"],
            vecorel=True,
            hive=True,
        )

        assert count == 2

        files = list(Path(out_dir).rglob("*.parquet"))
        assert len(files) == 2

        # No row multiplication: per-level chaining must emit one row per input
        # feature, not one per (country × region) polygon match (PR #474).
        total_rows = sum(pq.ParquetFile(str(f)).metadata.num_rows for f in files)
        assert total_rows == 2

        for f in files:
            pf = pq.ParquetFile(str(f))
            names = pf.schema_arrow.names
            assert "admin:country_code" in names
            assert "admin:subdivision_code" in names
            # Region codes stripped of country prefix (US-CA -> CA)
            table = pf.read()
            subdivisions = set(table.column("admin:subdivision_code").to_pylist())
            assert subdivisions <= {"CA", "NV"}
            # Vecorel metadata + non-nullable country code
            meta = pf.schema_arrow.metadata
            assert b"collection" in meta
            collection = json.loads(meta[b"collection"])
            assert VECOREL_ADMIN_SCHEMA in collection["schemas"]["default"]
            assert pf.schema_arrow.field("admin:country_code").nullable is False


@pytest.mark.integration
class TestNonVecorelPartitionLocalIntegration:
    """End-to-end NON-vecorel admin partitioning against a local admin file.

    Pins the single-pass invariants for the admin path (the new file only covered
    partition_by_column): exactly one PARTITION_BY scan, row reconciliation with
    no multiplication, admin columns dropped from output, nested layout.
    """

    def test_single_scan_reconciliation_and_dropped_admin_columns(
        self, monkeypatch, temp_output_dir
    ):
        import os
        from pathlib import Path

        import duckdb
        import pyarrow.parquet as pq

        from geoparquet_io.core.admin_datasets import OvertureAdminDataset
        from geoparquet_io.core.partition.admin_hierarchical import (
            partition_by_admin_hierarchical,
        )

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        admin_file = os.path.join(temp_output_dir, "admin.parquet")
        input_file = os.path.join(temp_output_dir, "input.parquet")
        out_dir = os.path.join(temp_output_dir, "out")

        _write_geoparquet(
            con,
            """
            SELECT subtype, country, region, geometry,
                {'xmin': ST_XMin(geometry), 'xmax': ST_XMax(geometry),
                 'ymin': ST_YMin(geometry), 'ymax': ST_YMax(geometry)} AS bbox
            FROM (VALUES
                ('country', 'US', NULL,
                 ST_GeomFromText('POLYGON((0 0, 0 10, 20 10, 20 0, 0 0))')),
                ('region', 'US', 'US-CA',
                 ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')),
                ('region', 'US', 'US-NV',
                 ST_GeomFromText('POLYGON((10 0, 10 10, 20 10, 20 0, 10 0))'))
            ) AS t(subtype, country, region, geometry)
            """,
            admin_file,
        )
        _write_geoparquet(
            con,
            """
            SELECT id, geometry FROM (VALUES
                (1, ST_GeomFromText('POINT(5 5)')),
                (2, ST_GeomFromText('POINT(15 5)'))
            ) AS t(id, geometry)
            """,
            input_file,
        )
        con.close()

        def fake_create(dataset_name, source_path=None, verbose=False):
            return OvertureAdminDataset(source_path=admin_file, verbose=verbose)

        monkeypatch.setattr(
            "geoparquet_io.core.partition.admin_hierarchical.AdminDatasetFactory.create",
            staticmethod(fake_create),
        )

        # Spy the DuckDB connection factory to count partitioned scans.
        executed: list[str] = []
        real_get_conn = duckdb_utils.get_duckdb_connection

        class SpyCon:
            def __init__(self, con):
                self._con = con

            def execute(self, sql, *args, **kwargs):
                executed.append(sql)
                return self._con.execute(sql, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(self._con, name)

        def spy_get_conn(*args, **kwargs):
            return SpyCon(real_get_conn(*args, **kwargs))

        monkeypatch.setattr(duckdb_utils, "get_duckdb_connection", spy_get_conn)

        count = partition_by_admin_hierarchical(
            input_file,
            out_dir,
            dataset_name="overture",
            levels=["country", "region"],
            hive=True,
        )

        assert count == 2
        files = list(Path(out_dir).rglob("*.parquet"))
        assert len(files) == 2
        # No row multiplication from the country×region polygon matches.
        assert sum(pq.ParquetFile(str(f)).metadata.num_rows for f in files) == 2

        # Exactly one partitioned scan of the enriched data; no per-combination writes.
        partition_by_stmts = [s for s in executed if "PARTITION_BY" in s.upper()]
        assert len(partition_by_stmts) == 1, executed

        # Admin columns are dropped from non-vecorel output (via the alias trick).
        for f in files:
            names = pq.ParquetFile(str(f)).schema_arrow.names
            assert not any(n.startswith("_admin_") or n.startswith("__gpio_part") for n in names)
            assert "admin:country_code" not in names


@pytest.mark.network
@pytest.mark.slow
class TestVecorelPartitionEndToEnd:
    """End-to-end vecorel partitioning (requires network for Overture)."""

    def test_partition_admin_vecorel_writes_compliant_partitions(
        self, places_test_file, temp_output_dir
    ):
        import pyarrow.parquet as pq

        from geoparquet_io.core.constants import VECOREL_ADMIN_SCHEMA

        runner = CliRunner()
        result = runner.invoke(
            partition,
            ["admin", places_test_file, temp_output_dir, "--vecorel"],
        )
        assert result.exit_code == 0, result.output

        from pathlib import Path

        parquet_files = list(Path(temp_output_dir).rglob("*.parquet"))
        assert parquet_files, "no partitions were created"

        pf = pq.ParquetFile(str(parquet_files[0]))
        names = pf.schema_arrow.names
        assert "admin:country_code" in names
        assert "admin:subdivision_code" in names

        # Vecorel collection metadata present
        meta = pf.schema_arrow.metadata
        assert b"collection" in meta
        collection = json.loads(meta[b"collection"])
        assert VECOREL_ADMIN_SCHEMA in collection["schemas"]["default"]

        # admin:country_code must be non-nullable per Vecorel spec
        assert pf.schema_arrow.field("admin:country_code").nullable is False
