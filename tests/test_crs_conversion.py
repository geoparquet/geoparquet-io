"""
Tests for CRS-aware conversions between GeoParquet formats.

Tests verify that CRS metadata is properly preserved and converted:
- Parquet native geo type CRS (Geometry(crs={...}))
- GeoParquet metadata CRS (columns.<geom>.crs)

CRS Priority Rules:
1. User-specified --crs always wins
2. Auto-detect from input file
3. Default EPSG:4326

Test Matrix:
- parquet-geo-only → GeoParquet 2.0: CRS in both locations
- parquet-geo-only → GeoParquet 1.1: CRS in geo metadata only
- parquet-geo-only → parquet-geo-only: CRS in Parquet type only
- GPKG with non-default CRS → GeoParquet 2.0: CRS preserved
- Default CRS should NOT trigger extra rewrite
"""

import json
import os

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.common import (
    _extract_crs_identifier,
    extract_crs_from_parquet,
    is_default_crs,
)
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.metadata_utils import parse_geometry_type_from_schema

# Helper functions for CRS testing


def get_parquet_type_crs(parquet_file):
    """
    Extract CRS from Parquet native geo type schema.

    Args:
        parquet_file: Path to the parquet file

    Returns:
        dict: PROJJSON CRS dict, or None if not found
    """
    pf = pq.ParquetFile(parquet_file)
    schema = pf.schema_arrow
    parquet_schema_str = str(pf.metadata.schema)

    for field in schema:
        geom_details = parse_geometry_type_from_schema(field.name, parquet_schema_str)
        if geom_details and "crs" in geom_details:
            return geom_details["crs"]

    return None


def get_geoparquet_crs(parquet_file):
    """
    Extract CRS from GeoParquet metadata.

    Args:
        parquet_file: Path to the parquet file

    Returns:
        dict: PROJJSON CRS dict, or None if not found
    """
    pf = pq.ParquetFile(parquet_file)
    metadata = pf.schema_arrow.metadata

    if metadata and b"geo" in metadata:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        primary_col = geo_meta.get("primary_column", "geometry")
        columns = geo_meta.get("columns", {})
        if primary_col in columns:
            return columns[primary_col].get("crs")

    return None


def get_geoparquet_version(parquet_file):
    """Extract GeoParquet version from file metadata."""
    pf = pq.ParquetFile(parquet_file)
    metadata = pf.schema_arrow.metadata
    if metadata and b"geo" in metadata:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        return geo_meta.get("version")
    return None


def has_native_geo_types(parquet_file):
    """Check if file uses Parquet GEOMETRY/GEOGRAPHY logical types."""
    pf = pq.ParquetFile(parquet_file)
    schema_str = str(pf.metadata.schema)
    return "Geometry" in schema_str or "Geography" in schema_str


def assert_crs_equivalent(crs1, crs2):
    """
    Compare two CRS by EPSG code.

    Handles both PROJJSON dicts and EPSG strings.

    Args:
        crs1: First CRS (dict or string)
        crs2: Second CRS (dict or string)

    Returns:
        bool: True if CRS are equivalent
    """
    id1 = _extract_crs_identifier(crs1)
    id2 = _extract_crs_identifier(crs2)

    if id1 is None or id2 is None:
        return False

    return id1 == id2


# Fixtures


@pytest.fixture
def runner():
    """Provide a Click CLI runner."""
    return CliRunner()


@pytest.fixture
def test_data_dir():
    """Return the path to the test data directory."""
    return os.path.join(os.path.dirname(__file__), "data")


@pytest.fixture
def fields_5070_file(test_data_dir):
    """Return path to parquet-geo-only file with EPSG:5070 CRS."""
    return os.path.join(test_data_dir, "fields_pgo_5070_snappy.parquet")


@pytest.fixture
def fields_geom_type_only_file(test_data_dir):
    """Return path to parquet-geo-only file (default CRS)."""
    return os.path.join(test_data_dir, "fields_pgo_crs84_bbox_snappy.parquet")


@pytest.fixture
def buildings_gpkg_file(test_data_dir):
    """Return path to GeoPackage test file with EPSG:6933."""
    return os.path.join(test_data_dir, "buildings_test_6933.gpkg")


@pytest.fixture
def temp_output_dir(tmp_path):
    """Create a temporary directory for test outputs."""
    return str(tmp_path)


@pytest.fixture
def temp_output_file(temp_output_dir):
    """Create a temporary output file path."""
    return os.path.join(temp_output_dir, "output.parquet")


# Test CRS extraction functions


class TestCRSExtraction:
    """Test CRS extraction from various file types."""

    def test_extract_crs_from_parquet_5070(self, fields_5070_file):
        """Test extracting EPSG:5070 from parquet-geo-only file."""
        crs = extract_crs_from_parquet(fields_5070_file)

        assert crs is not None
        identifier = _extract_crs_identifier(crs)
        assert identifier == ("EPSG", 5070)

    def test_extract_crs_from_parquet_default(self, fields_geom_type_only_file):
        """Test extracting default CRS returns None (or default CRS)."""
        crs = extract_crs_from_parquet(fields_geom_type_only_file)

        # Default CRS should be filtered out (returns None)
        # or if returned, should be recognized as default
        if crs is not None:
            assert is_default_crs(crs)

    def test_get_parquet_type_crs(self, fields_5070_file):
        """Test getting CRS from Parquet native geo type."""
        crs = get_parquet_type_crs(fields_5070_file)

        assert crs is not None
        identifier = _extract_crs_identifier(crs)
        assert identifier == ("EPSG", 5070)


# Test CRS-aware conversions


class TestCRSConversion:
    """Test CRS preservation during format conversions."""

    def test_parquet_geo_only_to_geoparquet_2_preserves_crs(
        self, runner, fields_5070_file, temp_output_file
    ):
        """
        Test: parquet-geo-only with EPSG:5070 → GeoParquet 2.0

        Expected: CRS should be in BOTH Parquet native type AND geo metadata.
        """
        result = runner.invoke(
            cli,
            [
                "convert",
                fields_5070_file,
                temp_output_file,
                "--geoparquet-version",
                "2.0",
                "--skip-hilbert",
                "-v",
            ],
        )

        assert result.exit_code == 0, f"Conversion failed: {result.output}"

        # Check output file
        assert os.path.exists(temp_output_file)

        # Verify GeoParquet version
        version = get_geoparquet_version(temp_output_file)
        assert version == "2.0.0"

        # Verify CRS in Parquet native type
        parquet_crs = get_parquet_type_crs(temp_output_file)
        assert parquet_crs is not None, "CRS missing from Parquet native type"
        assert assert_crs_equivalent(parquet_crs, "EPSG:5070")

        # Verify CRS in GeoParquet metadata
        geoparquet_crs = get_geoparquet_crs(temp_output_file)
        assert geoparquet_crs is not None, "CRS missing from GeoParquet metadata"
        assert assert_crs_equivalent(geoparquet_crs, "EPSG:5070")

    def test_parquet_geo_only_to_geoparquet_1_preserves_crs(
        self, runner, fields_5070_file, temp_output_file
    ):
        """
        Test: parquet-geo-only with EPSG:5070 → GeoParquet 1.1

        Expected: CRS should be in geo metadata only (1.x doesn't use native types).
        """
        result = runner.invoke(
            cli,
            [
                "convert",
                fields_5070_file,
                temp_output_file,
                "--geoparquet-version",
                "1.1",
                "--skip-hilbert",
            ],
        )

        assert result.exit_code == 0, f"Conversion failed: {result.output}"

        # Verify GeoParquet version
        version = get_geoparquet_version(temp_output_file)
        assert version == "1.1.0"

        # Verify CRS in GeoParquet metadata
        geoparquet_crs = get_geoparquet_crs(temp_output_file)
        assert geoparquet_crs is not None, "CRS missing from GeoParquet metadata"
        assert assert_crs_equivalent(geoparquet_crs, "EPSG:5070")

    def test_parquet_geo_only_to_parquet_geo_only_preserves_crs(
        self, runner, fields_5070_file, temp_output_file
    ):
        """
        Test: parquet-geo-only with EPSG:5070 → parquet-geo-only

        Expected: CRS should be in Parquet native type only (no geo metadata).
        """
        result = runner.invoke(
            cli,
            [
                "convert",
                fields_5070_file,
                temp_output_file,
                "--geoparquet-version",
                "parquet-geo-only",
                "--skip-hilbert",
            ],
        )

        assert result.exit_code == 0, f"Conversion failed: {result.output}"

        # Verify no GeoParquet metadata
        version = get_geoparquet_version(temp_output_file)
        assert version is None, "parquet-geo-only should not have GeoParquet metadata"

        # Verify file has native geo types
        assert has_native_geo_types(temp_output_file)

        # Verify CRS in Parquet native type
        parquet_crs = get_parquet_type_crs(temp_output_file)
        assert parquet_crs is not None, "CRS missing from Parquet native type"
        assert assert_crs_equivalent(parquet_crs, "EPSG:5070")

    def test_gpkg_to_geoparquet_2_detects_crs(self, runner, buildings_gpkg_file, temp_output_file):
        """
        Test: GPKG with EPSG:6933 → GeoParquet 2.0

        Expected: CRS should be auto-detected and preserved in both locations.
        """
        if not os.path.exists(buildings_gpkg_file):
            pytest.skip("buildings_test_6933.gpkg not available")

        result = runner.invoke(
            cli,
            [
                "convert",
                buildings_gpkg_file,
                temp_output_file,
                "--geoparquet-version",
                "2.0",
                "--skip-hilbert",
                "-v",
            ],
        )

        assert result.exit_code == 0, f"Conversion failed: {result.output}"

        # Verify CRS in Parquet native type
        parquet_crs = get_parquet_type_crs(temp_output_file)
        assert parquet_crs is not None, "CRS missing from Parquet native type"
        assert assert_crs_equivalent(parquet_crs, "EPSG:6933")

        # Verify CRS in GeoParquet metadata
        geoparquet_crs = get_geoparquet_crs(temp_output_file)
        assert geoparquet_crs is not None, "CRS missing from GeoParquet metadata"
        assert assert_crs_equivalent(geoparquet_crs, "EPSG:6933")

    def test_default_crs_not_written(self, runner, fields_geom_type_only_file, temp_output_file):
        """
        Test: File with default CRS → GeoParquet 2.0

        Expected: No explicit CRS should be written (default is implied).
        """
        result = runner.invoke(
            cli,
            [
                "convert",
                fields_geom_type_only_file,
                temp_output_file,
                "--geoparquet-version",
                "2.0",
                "--skip-hilbert",
            ],
        )

        assert result.exit_code == 0, f"Conversion failed: {result.output}"

        # GeoParquet metadata CRS should be None or default
        geoparquet_crs = get_geoparquet_crs(temp_output_file)
        if geoparquet_crs is not None:
            assert is_default_crs(geoparquet_crs)


class TestCRSOverride:
    """Test --crs parameter behavior."""

    def test_crs_flag_errors_on_non_csv_files(self, runner, fields_5070_file, temp_output_file):
        """
        Test: --crs flag should error when used with non-CSV files.

        The --crs option is only valid for CSV/TSV files because other formats
        (Parquet, GeoPackage, Shapefile) already have CRS metadata embedded.
        """
        result = runner.invoke(
            cli,
            [
                "convert",
                fields_5070_file,
                temp_output_file,
                "--crs",
                "EPSG:32632",
                "--geoparquet-version",
                "2.0",
                "--skip-hilbert",
            ],
        )

        # Should fail because --crs is not valid for parquet files
        assert result.exit_code != 0
        assert "--crs option is only valid for CSV/TSV" in result.output

    def test_crs_flag_works_for_csv(self, runner, test_data_dir, temp_output_file):
        """
        Test: --crs flag should work correctly for CSV files.

        Expected: Output should have the user-specified CRS.
        """
        csv_file = os.path.join(test_data_dir, "points_wkt.csv")

        result = runner.invoke(
            cli,
            [
                "convert",
                csv_file,
                temp_output_file,
                "--crs",
                "EPSG:32632",
                "--geoparquet-version",
                "2.0",
                "--skip-hilbert",
            ],
        )

        assert result.exit_code == 0, f"Conversion failed: {result.output}"

        # Verify user-specified CRS is used
        parquet_crs = get_parquet_type_crs(temp_output_file)
        assert parquet_crs is not None
        assert assert_crs_equivalent(parquet_crs, "EPSG:32632")

        geoparquet_crs = get_geoparquet_crs(temp_output_file)
        assert geoparquet_crs is not None
        assert assert_crs_equivalent(geoparquet_crs, "EPSG:32632")


class TestCRSHelperFunctions:
    """Test CRS helper functions."""

    def test_is_default_crs_epsg_4326(self):
        """Test that EPSG:4326 is recognized as default."""
        assert is_default_crs({"id": {"authority": "EPSG", "code": 4326}})
        assert is_default_crs(None)
        assert is_default_crs({})

    def test_is_default_crs_non_default(self):
        """Test that non-default CRS is not recognized as default."""
        assert not is_default_crs({"id": {"authority": "EPSG", "code": 5070}})
        assert not is_default_crs({"id": {"authority": "EPSG", "code": 32632}})

    def test_extract_crs_identifier_projjson(self):
        """Test extracting identifier from PROJJSON."""
        projjson = {
            "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
            "type": "ProjectedCRS",
            "name": "NAD83 / Conus Albers",
            "id": {"authority": "EPSG", "code": 5070},
        }
        identifier = _extract_crs_identifier(projjson)
        assert identifier == ("EPSG", 5070)

    def test_extract_crs_identifier_epsg_string(self):
        """Test extracting identifier from EPSG string."""
        assert _extract_crs_identifier("EPSG:4326") == ("EPSG", 4326)
        assert _extract_crs_identifier("epsg:5070") == ("EPSG", 5070)

    def test_extract_crs_identifier_urn(self):
        """Test extracting identifier from URN format."""
        assert _extract_crs_identifier("urn:ogc:def:crs:EPSG::4326") == ("EPSG", 4326)
        assert _extract_crs_identifier("urn:ogc:def:crs:EPSG::5070") == ("EPSG", 5070)

    def test_assert_crs_equivalent(self):
        """Test CRS equivalence comparison."""
        projjson_5070 = {"id": {"authority": "EPSG", "code": 5070}}

        assert assert_crs_equivalent(projjson_5070, "EPSG:5070")
        assert assert_crs_equivalent("EPSG:5070", projjson_5070)
        assert not assert_crs_equivalent(projjson_5070, "EPSG:4326")


class TestAdditionalCRSEdgeCases:
    """Additional edge case tests for CRS handling."""

    def test_utm_crs_preservation(self, runner, temp_output_file):
        """Test EPSG:32632 (UTM Zone 32N) preservation through conversion."""
        # Create a temporary CSV with UTM CRS
        import os
        import tempfile

        csv_content = """geometry,name
"POINT (500000 4500000)",Point A
"POINT (500100 4500100)",Point B
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            csv_file = f.name

        try:
            result = runner.invoke(
                cli,
                [
                    "convert",
                    csv_file,
                    temp_output_file,
                    "--crs",
                    "EPSG:32632",
                    "--geoparquet-version",
                    "2.0",
                    "--skip-hilbert",
                ],
            )

            assert result.exit_code == 0, f"Conversion failed: {result.output}"

            # Verify CRS in both locations
            parquet_crs = get_parquet_type_crs(temp_output_file)
            geo_crs = get_geoparquet_crs(temp_output_file)

            assert parquet_crs is not None
            assert geo_crs is not None
            assert assert_crs_equivalent(parquet_crs, "EPSG:32632")
            assert assert_crs_equivalent(geo_crs, "EPSG:32632")

        finally:
            if os.path.exists(csv_file):
                os.unlink(csv_file)

    def test_parquet_geo_only_to_parquet_geo_only_different_crs(
        self, fields_5070_file, temp_output_file
    ):
        """
        Test parquet-geo-only → parquet-geo-only preserves CRS.

        This tests the single-rewrite path for parquet-geo-only format.
        """
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="parquet-geo-only",
        )

        # Verify CRS preserved
        parquet_crs = get_parquet_type_crs(temp_output_file)
        assert parquet_crs is not None
        assert assert_crs_equivalent(parquet_crs, "EPSG:5070")

        # Should have no metadata
        geo_crs = get_geoparquet_crs(temp_output_file)
        assert geo_crs is None

    def test_v2_to_v2_different_crs(self, temp_output_dir):
        """
        Test v2.0 → v2.0 conversion preserves CRS.

        This tests the metadata rewrite path for v2.0.
        """
        import os

        test_data_dir = os.path.join(os.path.dirname(__file__), "data")
        input_file = os.path.join(test_data_dir, "fields_gpq2_5070_brotli.parquet")
        output_file = os.path.join(temp_output_dir, "output.parquet")

        if not os.path.exists(input_file):
            pytest.skip("fields_gpq2_5070_brotli.parquet not available")

        convert_to_geoparquet(
            input_file,
            output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
        )

        # Both locations should have EPSG:5070
        parquet_crs = get_parquet_type_crs(output_file)
        geo_crs = get_geoparquet_crs(output_file)

        assert parquet_crs is not None
        assert geo_crs is not None
        assert assert_crs_equivalent(parquet_crs, "EPSG:5070")
        assert assert_crs_equivalent(geo_crs, "EPSG:5070")

    def test_multiple_epsg_codes(self, runner, temp_output_file):
        """Test various EPSG codes are handled correctly."""
        import os
        import tempfile

        epsg_codes = [3857, 4269, 6933]  # Web Mercator, NAD83, Cylindrical Equal Area

        for epsg in epsg_codes:
            csv_content = """geometry,name
"POINT (0 0)",Test Point
"""
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
                f.write(csv_content)
                csv_file = f.name

            try:
                output = temp_output_file.replace(".parquet", f"_{epsg}.parquet")
                result = runner.invoke(
                    cli,
                    [
                        "convert",
                        csv_file,
                        output,
                        "--crs",
                        f"EPSG:{epsg}",
                        "--geoparquet-version",
                        "2.0",
                        "--skip-hilbert",
                    ],
                )

                assert result.exit_code == 0, f"Failed for EPSG:{epsg}: {result.output}"

                # Verify CRS
                parquet_crs = get_parquet_type_crs(output)
                geo_crs = get_geoparquet_crs(output)

                assert assert_crs_equivalent(parquet_crs, f"EPSG:{epsg}")
                assert assert_crs_equivalent(geo_crs, f"EPSG:{epsg}")

            finally:
                if os.path.exists(csv_file):
                    os.unlink(csv_file)
                if os.path.exists(output):
                    os.unlink(output)

    def test_custom_projjson_crs(self, runner, temp_output_file):
        """
        Test that custom PROJJSON CRS is preserved.

        This tests CRS beyond simple EPSG codes.
        """
        # For now, skip this test - would require custom PROJJSON input
        pytest.skip("Custom PROJJSON test requires specialized test data")

    def test_crs_consistency_between_schema_and_metadata(self, fields_5070_file, temp_output_file):
        """
        Test that CRS is identical in both schema and metadata for v2.0.

        This is critical - the dual-write must produce identical CRS.
        """
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
        )

        parquet_crs = get_parquet_type_crs(temp_output_file)
        geo_crs = get_geoparquet_crs(temp_output_file)

        # Both should exist
        assert parquet_crs is not None
        assert geo_crs is not None

        # Extract EPSG codes
        parquet_epsg = _extract_crs_identifier(parquet_crs)
        geo_epsg = _extract_crs_identifier(geo_crs)

        # Should be identical
        assert parquet_epsg == geo_epsg, (
            f"CRS mismatch: schema has {parquet_epsg}, metadata has {geo_epsg}"
        )
