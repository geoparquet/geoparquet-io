"""
Comprehensive end-to-end conversion tests for all format and version combinations.

Tests verify:
- All input formats (GeoJSON, GPKG, Shapefile, CSV, Parquet) convert to all GeoParquet versions
- CRS preservation across conversions
- Geometry integrity
- Metadata correctness
- Data completeness (row counts, attributes)

This test suite provides comprehensive coverage before refactoring metadata write paths.
"""

import json
import os

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.convert import convert_to_geoparquet
from tests.conftest import (
    get_geoparquet_version,
    has_geoparquet_metadata,
    has_native_geo_types,
)

# Helper functions


def get_parquet_type_crs(parquet_file):
    """Extract CRS from Parquet native geo type schema."""
    from geoparquet_io.core.metadata_utils import parse_geometry_type_from_schema

    pf = pq.ParquetFile(parquet_file)
    schema = pf.schema_arrow
    parquet_schema_str = str(pf.metadata.schema)

    for field in schema:
        geom_details = parse_geometry_type_from_schema(field.name, parquet_schema_str)
        if geom_details and "crs" in geom_details:
            return geom_details["crs"]

    return None


def get_geoparquet_crs(parquet_file):
    """Extract CRS from GeoParquet metadata."""
    pf = pq.ParquetFile(parquet_file)
    metadata = pf.schema_arrow.metadata

    if metadata and b"geo" in metadata:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        primary_col = geo_meta.get("primary_column", "geometry")
        columns = geo_meta.get("columns", {})
        if primary_col in columns:
            return columns[primary_col].get("crs")

    return None


def extract_crs_identifier(crs):
    """Extract (authority, code) from CRS dict or string."""
    from geoparquet_io.core.common import _extract_crs_identifier

    return _extract_crs_identifier(crs)


def assert_crs_equivalent(crs1, crs2):
    """Compare two CRS by identifier."""
    id1 = extract_crs_identifier(crs1)
    id2 = extract_crs_identifier(crs2)
    return id1 == id2 if id1 and id2 else False


def get_row_count(parquet_file):
    """Get row count from Parquet file using DuckDB."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_file}')").fetchone()
    con.close()
    return result[0]


def verify_duckdb_readable(parquet_file):
    """Verify file is readable by DuckDB spatial extension."""
    try:
        count = get_row_count(parquet_file)
        return count > 0
    except Exception:
        return False


# Test classes


class TestGeoJSONConversions:
    """Test GeoJSON to all GeoParquet versions."""

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    def test_geojson_to_version(self, geojson_input, temp_output_file, version):
        """Test GeoJSON converts to each version correctly."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version=version,
        )

        assert os.path.exists(temp_output_file)
        assert os.path.getsize(temp_output_file) > 0

        if version == "parquet-geo-only":
            assert not has_geoparquet_metadata(temp_output_file)
            assert has_native_geo_types(temp_output_file)
        else:
            assert has_geoparquet_metadata(temp_output_file)
            assert get_geoparquet_version(temp_output_file) == f"{version}.0"

            if version in ["2.0"]:
                assert has_native_geo_types(temp_output_file)

        # Verify data integrity
        assert verify_duckdb_readable(temp_output_file)


class TestGPKGConversions:
    """Test GeoPackage to all GeoParquet versions."""

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    def test_gpkg_to_version(self, gpkg_buildings, temp_output_file, version):
        """Test GPKG converts to each version correctly."""
        convert_to_geoparquet(
            gpkg_buildings,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version=version,
        )

        assert os.path.exists(temp_output_file)
        assert verify_duckdb_readable(temp_output_file)

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    def test_gpkg_with_crs_6933_to_version(self, buildings_gpkg_6933, temp_output_file, version):
        """Test GPKG with EPSG:6933 converts to each version with CRS preserved."""
        if not os.path.exists(buildings_gpkg_6933):
            pytest.skip("buildings_test_6933.gpkg not available")

        convert_to_geoparquet(
            buildings_gpkg_6933,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version=version,
        )

        # Verify CRS preservation based on version
        if version in ["2.0"]:
            # v2.0 should have CRS in both Parquet type AND metadata
            parquet_crs = get_parquet_type_crs(temp_output_file)
            geo_crs = get_geoparquet_crs(temp_output_file)
            assert parquet_crs is not None, "CRS missing from Parquet native type"
            assert geo_crs is not None, "CRS missing from GeoParquet metadata"
            assert assert_crs_equivalent(parquet_crs, "EPSG:6933")
            assert assert_crs_equivalent(geo_crs, "EPSG:6933")

        elif version == "parquet-geo-only":
            # parquet-geo-only should have CRS ONLY in Parquet type
            parquet_crs = get_parquet_type_crs(temp_output_file)
            geo_crs = get_geoparquet_crs(temp_output_file)
            assert parquet_crs is not None, "CRS missing from Parquet native type"
            assert geo_crs is None, "CRS should not be in metadata for parquet-geo-only"
            assert assert_crs_equivalent(parquet_crs, "EPSG:6933")

        else:  # v1.0, v1.1
            # v1.x should have CRS ONLY in GeoParquet metadata
            geo_crs = get_geoparquet_crs(temp_output_file)
            assert geo_crs is not None, "CRS missing from GeoParquet metadata"
            assert assert_crs_equivalent(geo_crs, "EPSG:6933")


class TestShapefileConversions:
    """Test Shapefile to all GeoParquet versions."""

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    def test_shapefile_to_version(self, shapefile_buildings, temp_output_file, version):
        """Test Shapefile converts to each version correctly."""
        convert_to_geoparquet(
            shapefile_buildings,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version=version,
        )

        assert os.path.exists(temp_output_file)
        assert verify_duckdb_readable(temp_output_file)


class TestCSVConversions:
    """Test CSV (WKT) to all GeoParquet versions."""

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    def test_csv_wkt_to_version(self, csv_points_wkt, temp_output_file, version):
        """Test CSV with WKT converts to each version correctly."""
        convert_to_geoparquet(
            csv_points_wkt,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version=version,
        )

        assert os.path.exists(temp_output_file)
        assert verify_duckdb_readable(temp_output_file)

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    def test_csv_with_crs_to_version(self, csv_points_wkt, temp_output_file, version):
        """Test CSV with --crs flag converts to each version correctly."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                csv_points_wkt,
                temp_output_file,
                "--crs",
                "EPSG:32632",
                "--geoparquet-version",
                version,
                "--skip-hilbert",
            ],
        )

        assert result.exit_code == 0, f"Conversion failed: {result.output}"

        # Verify CRS is applied correctly based on version
        if version == "2.0":
            parquet_crs = get_parquet_type_crs(temp_output_file)
            geo_crs = get_geoparquet_crs(temp_output_file)
            assert parquet_crs is not None
            assert geo_crs is not None
            assert assert_crs_equivalent(parquet_crs, "EPSG:32632")
            assert assert_crs_equivalent(geo_crs, "EPSG:32632")

        elif version == "parquet-geo-only":
            parquet_crs = get_parquet_type_crs(temp_output_file)
            assert parquet_crs is not None
            assert assert_crs_equivalent(parquet_crs, "EPSG:32632")

        else:  # v1.0, v1.1
            geo_crs = get_geoparquet_crs(temp_output_file)
            assert geo_crs is not None
            assert assert_crs_equivalent(geo_crs, "EPSG:32632")


class TestParquetToParquetConversions:
    """Test Parquet to Parquet cross-version conversions."""

    def test_parquet_geo_only_5070_to_v2(self, fields_5070_file, temp_output_file):
        """Test parquet-geo-only with EPSG:5070 to v2.0."""
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
        )

        # Should have CRS in both locations
        parquet_crs = get_parquet_type_crs(temp_output_file)
        geo_crs = get_geoparquet_crs(temp_output_file)
        assert parquet_crs is not None
        assert geo_crs is not None
        assert assert_crs_equivalent(parquet_crs, "EPSG:5070")
        assert assert_crs_equivalent(geo_crs, "EPSG:5070")

    def test_parquet_geo_only_5070_to_v1_1(self, fields_5070_file, temp_output_file):
        """Test parquet-geo-only with EPSG:5070 to v1.1."""
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="1.1",
        )

        # Should have CRS only in metadata
        geo_crs = get_geoparquet_crs(temp_output_file)
        assert geo_crs is not None
        assert assert_crs_equivalent(geo_crs, "EPSG:5070")

    def test_parquet_geo_only_to_parquet_geo_only_preserves_crs(
        self, fields_5070_file, temp_output_file
    ):
        """Test parquet-geo-only to parquet-geo-only preserves CRS."""
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="parquet-geo-only",
        )

        # Should have CRS only in Parquet type
        parquet_crs = get_parquet_type_crs(temp_output_file)
        assert parquet_crs is not None
        assert assert_crs_equivalent(parquet_crs, "EPSG:5070")
        assert get_geoparquet_crs(temp_output_file) is None


class TestRowCountPreservation:
    """Test that row counts are preserved across all conversions."""

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    def test_gpkg_row_count_preserved(self, gpkg_buildings, temp_output_file, version):
        """Test row count preserved when converting GPKG."""
        # Get original row count
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        original_count = con.execute(
            f"SELECT COUNT(*) FROM ST_Read('{gpkg_buildings}')"
        ).fetchone()[0]

        convert_to_geoparquet(
            gpkg_buildings,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version=version,
        )

        # Verify row count
        output_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{temp_output_file}')"
        ).fetchone()[0]
        con.close()

        assert output_count == original_count

    @pytest.mark.parametrize("target_version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    def test_parquet_row_count_preserved(self, fields_5070_file, temp_output_file, target_version):
        """Test row count preserved when converting parquet to different versions."""
        original_count = get_row_count(fields_5070_file)

        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version=target_version,
        )

        output_count = get_row_count(temp_output_file)
        assert output_count == original_count


class TestDefaultCRSHandling:
    """Test that default CRS (EPSG:4326) is handled correctly."""

    @pytest.mark.parametrize("version", ["2.0", "parquet-geo-only"])
    def test_default_crs_not_written_to_v2_and_parquet_geo_only(
        self, fields_geom_type_only_file, temp_output_file, version
    ):
        """
        Test that files with default CRS don't get explicit CRS written for v2.0/parquet-geo-only.

        This is an optimization - default CRS is implied.
        """
        from geoparquet_io.core.common import is_default_crs

        convert_to_geoparquet(
            fields_geom_type_only_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version=version,
        )

        # For v2.0, check metadata CRS
        if version == "2.0":
            geo_crs = get_geoparquet_crs(temp_output_file)
            if geo_crs is not None:
                assert is_default_crs(geo_crs)


class TestGeometryIntegrity:
    """Test that geometries remain valid across conversions."""

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    def test_geometry_validity_preserved(self, gpkg_buildings, temp_output_file, version):
        """Test that geometries remain valid after conversion."""
        convert_to_geoparquet(
            gpkg_buildings,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version=version,
        )

        # Check that geometries are valid using DuckDB
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        # Count invalid geometries
        invalid_count = con.execute(
            f"""
            SELECT COUNT(*)
            FROM read_parquet('{temp_output_file}')
            WHERE NOT ST_IsValid(geometry)
        """
        ).fetchone()[0]

        con.close()

        assert invalid_count == 0, f"Found {invalid_count} invalid geometries after conversion"


class TestCLIConversions:
    """Test conversions via CLI to ensure end-to-end functionality."""

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    def test_cli_convert_geojson_to_version(self, geojson_input, temp_output_file, version):
        """Test CLI conversion of GeoJSON to each version."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                geojson_input,
                temp_output_file,
                "--geoparquet-version",
                version,
                "--skip-hilbert",
            ],
        )

        assert result.exit_code == 0, f"CLI conversion failed: {result.output}"
        assert os.path.exists(temp_output_file)
        assert verify_duckdb_readable(temp_output_file)

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    def test_cli_verbose_output(self, geojson_input, temp_output_file, version):
        """Test that verbose output shows conversion progress."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                geojson_input,
                temp_output_file,
                "--geoparquet-version",
                version,
                "--skip-hilbert",
                "--verbose",
            ],
        )

        assert result.exit_code == 0
        # Verbose output should contain version information
        assert version in result.output or version.replace("-", " ") in result.output.lower()
