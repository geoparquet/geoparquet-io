"""Tests for gpio add geometry-metrics command."""

import json
import os

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import add
from geoparquet_io.core.add.geometry_metrics import (
    AREA_COLUMN,
    PERIMETER_COLUMN,
    _build_vecorel_metadata,
    add_geometry_metrics,
)
from geoparquet_io.core.constants import VECOREL_METRICS_SCHEMA
from geoparquet_io.core.duckdb_utils import fetch_one


class TestBuildVecorelMetadata:
    """Test Vecorel metadata generation."""

    def test_default_metadata(self):
        meta = _build_vecorel_metadata()
        assert "collection" in meta
        parsed = json.loads(meta["collection"])
        assert VECOREL_METRICS_SCHEMA in parsed["schemas"]["default"]
        assert (
            "https://vecorel.org/specification/v0.1.0/schema.yaml" in parsed["schemas"]["default"]
        )
        assert parsed["collection"] == "default"

    def test_merges_existing_metadata(self):
        existing = {
            "collection": json.dumps(
                {"schemas": {"mycoll": ["https://example.com/other.yaml"]}, "collection": "mycoll"}
            )
        }
        meta = _build_vecorel_metadata(existing)
        parsed = json.loads(meta["collection"])
        schemas = parsed["schemas"]["mycoll"]
        assert VECOREL_METRICS_SCHEMA in schemas
        assert "https://example.com/other.yaml" in schemas
        assert parsed["collection"] == "mycoll"

    def test_no_duplicate_schemas(self):
        existing = {
            "collection": json.dumps(
                {"schemas": {"default": [VECOREL_METRICS_SCHEMA]}, "collection": "default"}
            )
        }
        meta = _build_vecorel_metadata(existing)
        parsed = json.loads(meta["collection"])
        schemas = parsed["schemas"]["default"]
        assert schemas.count(VECOREL_METRICS_SCHEMA) == 1


class TestAddGeometryMetricsCli:
    """Test the CLI command for add geometry-metrics."""

    def test_add_metrics_to_buildings(self, buildings_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(add, ["geometry-metrics", buildings_test_file, temp_output_file])
        assert result.exit_code == 0, result.output
        assert os.path.exists(temp_output_file)

        con = duckdb.connect()
        con.execute("LOAD spatial;")
        columns = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{temp_output_file}')"
        ).fetchall()
        col_names = [col[0] for col in columns]
        assert AREA_COLUMN in col_names
        assert PERIMETER_COLUMN in col_names

        # Verify row count preserved
        input_count = fetch_one(
            con.execute(f"SELECT COUNT(*) FROM read_parquet('{buildings_test_file}')")
        )[0]
        output_count = fetch_one(
            con.execute(f"SELECT COUNT(*) FROM read_parquet('{temp_output_file}')")
        )[0]
        assert input_count == output_count
        con.close()

    def test_metrics_values_positive(self, buildings_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(add, ["geometry-metrics", buildings_test_file, temp_output_file])
        assert result.exit_code == 0

        con = duckdb.connect()
        con.execute("LOAD spatial;")
        min_area = fetch_one(
            con.execute(f"""SELECT MIN("{AREA_COLUMN}") FROM read_parquet('{temp_output_file}')""")
        )[0]
        min_perim = fetch_one(
            con.execute(
                f"""SELECT MIN("{PERIMETER_COLUMN}") FROM read_parquet('{temp_output_file}')"""
            )
        )[0]
        assert min_area > 0
        assert min_perim > 0
        con.close()

    def test_vecorel_metadata_written(self, buildings_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(add, ["geometry-metrics", buildings_test_file, temp_output_file])
        assert result.exit_code == 0

        pf = pq.ParquetFile(temp_output_file)
        meta = pf.schema_arrow.metadata
        assert b"collection" in meta
        vecorel = json.loads(meta[b"collection"])
        assert VECOREL_METRICS_SCHEMA in vecorel["schemas"]["default"]

    def test_no_vecorel_flag(self, buildings_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["geometry-metrics", buildings_test_file, temp_output_file, "--no-vecorel"],
        )
        assert result.exit_code == 0

        pf = pq.ParquetFile(temp_output_file)
        meta = pf.schema_arrow.metadata
        assert b"collection" not in meta

    def test_dry_run(self, buildings_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["geometry-metrics", buildings_test_file, temp_output_file, "--dry-run"],
        )
        assert result.exit_code == 0
        assert "ST_Area_Spheroid" in result.output
        assert not os.path.exists(temp_output_file)

    def test_no_output_arg_streams(self, buildings_test_file):
        runner = CliRunner()
        result = runner.invoke(add, ["geometry-metrics", buildings_test_file])
        assert result.exit_code == 0


class TestVecorelCompliance:
    """Test that vecorel mode produces spec-compliant output."""

    def test_id_column_preserved_when_exists(self, buildings_test_file, temp_output_file):
        """Buildings file has an id column — it should be kept."""
        add_geometry_metrics(buildings_test_file, temp_output_file, vecorel=True)

        con = duckdb.connect()
        con.execute("LOAD spatial;")
        cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{temp_output_file}')").fetchall()
        col_names = [c[0] for c in cols]
        assert "id" in col_names
        con.close()

    def test_id_column_added_when_missing(self, places_test_file, temp_output_file):
        """Places file has no 'id' column — vecorel mode should add one."""
        add_geometry_metrics(places_test_file, temp_output_file, vecorel=True)

        con = duckdb.connect()
        con.execute("LOAD spatial;")
        cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{temp_output_file}')").fetchall()
        col_names = [c[0] for c in cols]
        assert "id" in col_names

        # id values should be non-null strings
        nulls = fetch_one(
            con.execute(
                f"""SELECT COUNT(*) FROM read_parquet('{temp_output_file}') WHERE id IS NULL"""
            )
        )[0]
        assert nulls == 0
        con.close()

    def test_id_column_not_added_without_vecorel(self, places_test_file, temp_output_file):
        """Without vecorel flag, no id column should be added."""
        add_geometry_metrics(places_test_file, temp_output_file, vecorel=False)

        con = duckdb.connect()
        con.execute("LOAD spatial;")
        cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{temp_output_file}')").fetchall()
        col_names = [c[0] for c in cols]
        # places file doesn't have id, and without vecorel we shouldn't add one
        assert "id" not in col_names or "fsq_place_id" in col_names
        con.close()

    def test_id_and_geometry_non_nullable(self, buildings_test_file, temp_output_file):
        """Vecorel requires id and geometry to be non-nullable in the schema."""
        add_geometry_metrics(buildings_test_file, temp_output_file, vecorel=True)

        pf = pq.ParquetFile(temp_output_file)
        schema = pf.schema_arrow
        assert schema.field("id").nullable is False
        assert schema.field("geometry").nullable is False

    def test_null_geometries_filtered(self, buildings_test_file, temp_output_file):
        """Vecorel mode should filter out null geometries."""
        add_geometry_metrics(buildings_test_file, temp_output_file, vecorel=True)

        con = duckdb.connect()
        con.execute("LOAD spatial;")
        nulls = fetch_one(
            con.execute(
                f"""SELECT COUNT(*) FROM read_parquet('{temp_output_file}') WHERE geometry IS NULL"""
            )
        )[0]
        assert nulls == 0
        con.close()


class TestAddGeometryMetricsCore:
    """Test the core function directly."""

    def test_add_metrics_function(self, buildings_test_file, temp_output_file):
        add_geometry_metrics(
            buildings_test_file,
            temp_output_file,
            vecorel=True,
        )
        assert os.path.exists(temp_output_file)

        con = duckdb.connect()
        con.execute("LOAD spatial;")
        columns = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{temp_output_file}')"
        ).fetchall()
        col_names = [col[0] for col in columns]
        assert AREA_COLUMN in col_names
        assert PERIMETER_COLUMN in col_names
        con.close()

    def test_overwrite_protection(self, buildings_test_file, temp_output_file):
        from geoparquet_io.core.exceptions import GeoParquetError

        add_geometry_metrics(buildings_test_file, temp_output_file)
        with pytest.raises(GeoParquetError):
            add_geometry_metrics(buildings_test_file, temp_output_file, overwrite=False)

    def test_overwrite_allowed(self, buildings_test_file, temp_output_file):
        add_geometry_metrics(buildings_test_file, temp_output_file)
        add_geometry_metrics(buildings_test_file, temp_output_file, overwrite=True)
        assert os.path.exists(temp_output_file)
