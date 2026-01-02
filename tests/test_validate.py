"""
Tests for the validate command.
"""

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli, validate
from geoparquet_io.core.common import is_geographic_crs
from geoparquet_io.core.validate import (
    CheckStatus,
    ValidationCheck,
    ValidationResult,
    _check_bbox_valid,
    _check_columns_present,
    _check_crs_valid,
    _check_edges_valid,
    _check_encoding_valid,
    _check_epoch_valid,
    _check_file_extension,
    _check_geo_key_exists,
    _check_geometry_types_list,
    _check_metadata_is_json,
    _check_orientation_valid,
    _check_primary_column_in_columns,
    _check_primary_column_present,
    _check_version_present,
    _crs_equals,
    format_json_output,
    validate_geoparquet,
)
from tests.conftest import _extract_json_from_output

# Test data directory
TEST_DATA_DIR = Path(__file__).parent / "data"


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def geoparquet_v1_file():
    """Return path to GeoParquet 1.x test file."""
    return str(TEST_DATA_DIR / "buildings_test.parquet")


@pytest.fixture
def geoparquet_v2_file():
    """Return path to GeoParquet 2.0 test file."""
    return str(TEST_DATA_DIR / "fields_gpq2_crs84_zstd.parquet")


@pytest.fixture
def parquet_geo_only_file():
    """Return path to parquet-geo-only test file."""
    return str(TEST_DATA_DIR / "fields_pgo_crs84_zstd.parquet")


@pytest.fixture
def geoparquet_v1_with_covering():
    """Return path to GeoParquet 1.1 file with bbox covering."""
    return str(TEST_DATA_DIR / "austria_bbox_covering.parquet")


# =============================================================================
# Test Data Structures
# =============================================================================


class TestCheckStatus:
    """Test CheckStatus enum."""

    def test_status_values(self):
        """Verify all status values exist."""
        assert CheckStatus.PASSED.value == "passed"
        assert CheckStatus.FAILED.value == "failed"
        assert CheckStatus.WARNING.value == "warning"
        assert CheckStatus.SKIPPED.value == "skipped"


class TestValidationCheck:
    """Test ValidationCheck dataclass."""

    def test_basic_creation(self):
        """Test creating a basic validation check."""
        check = ValidationCheck(
            name="test_check",
            status=CheckStatus.PASSED,
            message="Test passed",
            category="test",
        )
        assert check.name == "test_check"
        assert check.status == CheckStatus.PASSED
        assert check.message == "Test passed"
        assert check.category == "test"
        assert check.details is None

    def test_with_details(self):
        """Test creating check with details."""
        check = ValidationCheck(
            name="test_check",
            status=CheckStatus.FAILED,
            message="Test failed",
            category="test",
            details="Additional info",
        )
        assert check.details == "Additional info"


class TestValidationResult:
    """Test ValidationResult dataclass."""

    def test_empty_result(self):
        """Test empty validation result."""
        result = ValidationResult(
            file_path="/test/file.parquet",
            detected_version="1.0.0",
            target_version=None,
        )
        assert result.passed_count == 0
        assert result.failed_count == 0
        assert result.warning_count == 0
        assert result.is_valid is True

    def test_result_with_checks(self):
        """Test validation result with various checks."""
        result = ValidationResult(
            file_path="/test/file.parquet",
            detected_version="1.0.0",
            target_version=None,
            checks=[
                ValidationCheck("c1", CheckStatus.PASSED, "ok", "cat"),
                ValidationCheck("c2", CheckStatus.PASSED, "ok", "cat"),
                ValidationCheck("c3", CheckStatus.FAILED, "fail", "cat"),
                ValidationCheck("c4", CheckStatus.WARNING, "warn", "cat"),
            ],
        )
        assert result.passed_count == 2
        assert result.failed_count == 1
        assert result.warning_count == 1
        assert result.is_valid is False

    def test_is_valid_with_only_warnings(self):
        """Test that warnings don't affect is_valid."""
        result = ValidationResult(
            file_path="/test/file.parquet",
            detected_version="1.0.0",
            target_version=None,
            checks=[
                ValidationCheck("c1", CheckStatus.PASSED, "ok", "cat"),
                ValidationCheck("c2", CheckStatus.WARNING, "warn", "cat"),
            ],
        )
        assert result.is_valid is True


# =============================================================================
# Test Core Metadata Checks
# =============================================================================


class TestCoreMetadataChecks:
    """Test core metadata validation checks."""

    def test_geo_key_exists_pass(self):
        """Test geo key exists check passes."""
        check = _check_geo_key_exists({b"geo": b'{"version": "1.0.0"}'})
        assert check.status == CheckStatus.PASSED
        assert "geo" in check.message

    def test_geo_key_exists_fail(self):
        """Test geo key exists check fails."""
        check = _check_geo_key_exists({b"other": b"value"})
        assert check.status == CheckStatus.FAILED

    def test_metadata_is_json_pass(self):
        """Test metadata is JSON check passes."""
        check = _check_metadata_is_json({"version": "1.0.0"})
        assert check.status == CheckStatus.PASSED

    def test_metadata_is_json_fail(self):
        """Test metadata is JSON check fails for non-dict."""
        check = _check_metadata_is_json("not a dict")
        assert check.status == CheckStatus.FAILED

    def test_version_present_pass(self):
        """Test version present check passes."""
        check = _check_version_present({"version": "1.0.0"})
        assert check.status == CheckStatus.PASSED
        assert "1.0.0" in check.message

    def test_version_present_fail(self):
        """Test version present check fails."""
        check = _check_version_present({})
        assert check.status == CheckStatus.FAILED

    def test_primary_column_present_pass(self):
        """Test primary column present check passes."""
        check = _check_primary_column_present({"primary_column": "geometry"})
        assert check.status == CheckStatus.PASSED

    def test_primary_column_present_fail(self):
        """Test primary column present check fails."""
        check = _check_primary_column_present({})
        assert check.status == CheckStatus.FAILED

    def test_columns_present_pass(self):
        """Test columns present check passes."""
        check = _check_columns_present({"columns": {"geometry": {}}})
        assert check.status == CheckStatus.PASSED

    def test_columns_present_fail(self):
        """Test columns present check fails."""
        check = _check_columns_present({})
        assert check.status == CheckStatus.FAILED

    def test_primary_column_in_columns_pass(self):
        """Test primary column in columns check passes."""
        geo_meta = {
            "primary_column": "geometry",
            "columns": {"geometry": {}},
        }
        check = _check_primary_column_in_columns(geo_meta)
        assert check.status == CheckStatus.PASSED

    def test_primary_column_in_columns_fail(self):
        """Test primary column in columns check fails."""
        geo_meta = {
            "primary_column": "geometry",
            "columns": {"other_col": {}},
        }
        check = _check_primary_column_in_columns(geo_meta)
        assert check.status == CheckStatus.FAILED


# =============================================================================
# Test Column Metadata Checks
# =============================================================================


class TestColumnMetadataChecks:
    """Test column metadata validation checks."""

    def test_encoding_valid_pass(self):
        """Test valid encoding check passes."""
        check = _check_encoding_valid({"encoding": "WKB"}, "geometry")
        assert check.status == CheckStatus.PASSED

    def test_encoding_valid_fail(self):
        """Test invalid encoding check fails."""
        check = _check_encoding_valid({"encoding": "GeoJSON"}, "geometry")
        assert check.status == CheckStatus.FAILED

    def test_geometry_types_list_pass(self):
        """Test valid geometry types check passes."""
        check = _check_geometry_types_list({"geometry_types": ["Point", "Polygon"]}, "geometry")
        assert check.status == CheckStatus.PASSED

    def test_geometry_types_list_empty_pass(self):
        """Test empty geometry types list passes (means any type)."""
        check = _check_geometry_types_list({"geometry_types": []}, "geometry")
        assert check.status == CheckStatus.PASSED

    def test_geometry_types_list_invalid_type(self):
        """Test invalid geometry type fails."""
        check = _check_geometry_types_list({"geometry_types": ["Point", "InvalidType"]}, "geometry")
        assert check.status == CheckStatus.FAILED

    def test_crs_valid_none_pass(self):
        """Test null CRS passes (defaults to OGC:CRS84)."""
        check = _check_crs_valid({}, "geometry")
        assert check.status == CheckStatus.PASSED

    def test_crs_valid_projjson_pass(self):
        """Test valid PROJJSON CRS passes."""
        crs = {"$schema": "...", "type": "GeographicCRS", "name": "WGS 84"}
        check = _check_crs_valid({"crs": crs}, "geometry")
        assert check.status == CheckStatus.PASSED

    def test_crs_valid_invalid_fail(self):
        """Test invalid CRS fails."""
        check = _check_crs_valid({"crs": "EPSG:4326"}, "geometry")
        assert check.status == CheckStatus.FAILED

    def test_orientation_valid_pass(self):
        """Test valid orientation passes."""
        check = _check_orientation_valid({"orientation": "counterclockwise"}, "geometry")
        assert check.status == CheckStatus.PASSED

    def test_orientation_valid_none_pass(self):
        """Test no orientation passes."""
        check = _check_orientation_valid({}, "geometry")
        assert check.status == CheckStatus.PASSED

    def test_orientation_valid_fail(self):
        """Test invalid orientation fails."""
        check = _check_orientation_valid({"orientation": "clockwise"}, "geometry")
        assert check.status == CheckStatus.FAILED

    def test_edges_valid_pass(self):
        """Test valid edges passes."""
        check = _check_edges_valid({"edges": "planar"}, "geometry")
        assert check.status == CheckStatus.PASSED

    def test_edges_valid_spherical_pass(self):
        """Test spherical edges passes."""
        check = _check_edges_valid({"edges": "spherical"}, "geometry")
        assert check.status == CheckStatus.PASSED

    def test_edges_valid_fail(self):
        """Test invalid edges fails."""
        check = _check_edges_valid({"edges": "geodesic"}, "geometry")
        assert check.status == CheckStatus.FAILED

    def test_bbox_valid_4_elements_pass(self):
        """Test valid 4-element bbox passes."""
        check = _check_bbox_valid({"bbox": [0, 0, 1, 1]}, "geometry")
        assert check.status == CheckStatus.PASSED

    def test_bbox_valid_6_elements_pass(self):
        """Test valid 6-element bbox passes."""
        check = _check_bbox_valid({"bbox": [0, 0, 0, 1, 1, 1]}, "geometry")
        assert check.status == CheckStatus.PASSED

    def test_bbox_valid_wrong_count_fail(self):
        """Test wrong element count fails."""
        check = _check_bbox_valid({"bbox": [0, 0, 1]}, "geometry")
        assert check.status == CheckStatus.FAILED

    def test_bbox_valid_non_numeric_fail(self):
        """Test non-numeric elements fail."""
        check = _check_bbox_valid({"bbox": [0, 0, "one", 1]}, "geometry")
        assert check.status == CheckStatus.FAILED

    def test_epoch_valid_pass(self):
        """Test valid epoch passes."""
        check = _check_epoch_valid({"epoch": 2023.0}, "geometry")
        assert check.status == CheckStatus.PASSED

    def test_epoch_valid_none_pass(self):
        """Test no epoch passes."""
        check = _check_epoch_valid({}, "geometry")
        assert check.status == CheckStatus.PASSED

    def test_epoch_valid_fail(self):
        """Test non-numeric epoch fails."""
        check = _check_epoch_valid({"epoch": "2023"}, "geometry")
        assert check.status == CheckStatus.FAILED


# =============================================================================
# Test GeoParquet 1.1 Checks
# =============================================================================


class TestGeoParquet11Checks:
    """Test GeoParquet 1.1-specific validation checks."""

    def test_file_extension_parquet_pass(self):
        """Test .parquet extension passes."""
        check = _check_file_extension("/path/to/file.parquet")
        assert check.status == CheckStatus.PASSED

    def test_file_extension_geoparquet_warning(self):
        """Test .geoparquet extension gives warning."""
        check = _check_file_extension("/path/to/file.geoparquet")
        assert check.status == CheckStatus.WARNING

    def test_file_extension_other_warning(self):
        """Test unusual extension gives warning."""
        check = _check_file_extension("/path/to/file.pq")
        assert check.status == CheckStatus.WARNING


# =============================================================================
# Test Helper Functions
# =============================================================================


class TestHelperFunctions:
    """Test helper functions."""

    def test_crs_equals_both_none(self):
        """Test both CRS None equals."""
        assert _crs_equals(None, None) is True

    def test_crs_equals_one_none(self):
        """Test one CRS None not equals."""
        assert _crs_equals({"name": "WGS 84"}, None) is False

    def test_crs_equals_same_epsg(self):
        """Test same EPSG code equals."""
        crs1 = {"id": {"authority": "EPSG", "code": 4326}}
        crs2 = {"id": {"authority": "EPSG", "code": 4326}}
        assert _crs_equals(crs1, crs2) is True

    def test_crs_equals_different_epsg(self):
        """Test different EPSG code not equals."""
        crs1 = {"id": {"authority": "EPSG", "code": 4326}}
        crs2 = {"id": {"authority": "EPSG", "code": 3857}}
        assert _crs_equals(crs1, crs2) is False

    def test_is_geographic_crs_none(self):
        """Test None CRS is geographic (default)."""
        assert is_geographic_crs(None) is True

    def test_is_geographic_crs_4326(self):
        """Test EPSG:4326 is geographic."""
        crs = {"id": {"authority": "EPSG", "code": 4326}}
        assert is_geographic_crs(crs) is True

    def test_is_geographic_crs_wgs84_name(self):
        """Test WGS 84 name is geographic."""
        crs = {"name": "WGS 84"}
        assert is_geographic_crs(crs) is True

    def test_is_geographic_crs_projected(self):
        """Test projected CRS is not geographic."""
        crs = {"id": {"authority": "EPSG", "code": 3857}, "name": "Web Mercator"}
        assert is_geographic_crs(crs) is False


# =============================================================================
# Test Main Validation Function
# =============================================================================


class TestValidateGeoparquet:
    """Test the main validate_geoparquet function."""

    def test_validate_geoparquet_v1(self, geoparquet_v1_file):
        """Test validating GeoParquet 1.x file."""
        result = validate_geoparquet(geoparquet_v1_file)
        assert result.detected_version == "1.0.0"
        assert result.is_valid is True
        assert result.passed_count > 0

    def test_validate_geoparquet_v2(self, geoparquet_v2_file):
        """Test validating GeoParquet 2.0 file."""
        result = validate_geoparquet(geoparquet_v2_file)
        assert result.detected_version == "2.0.0"
        assert result.is_valid is True
        # Should have 2.0-specific checks
        categories = {c.category for c in result.checks}
        assert "geoparquet_2_0" in categories

    def test_validate_parquet_geo_only(self, parquet_geo_only_file):
        """Test validating parquet-geo-only file."""
        result = validate_geoparquet(parquet_geo_only_file)
        assert result.detected_version == "parquet-geo-only"
        assert result.is_valid is True
        # Should have parquet_geo_types checks (native geo types)
        categories = {c.category for c in result.checks}
        assert "parquet_geo_types" in categories

    def test_validate_with_skip_data_validation(self, geoparquet_v1_file):
        """Test validation with data validation disabled."""
        result = validate_geoparquet(geoparquet_v1_file, validate_data=False)
        # Should not have data_validation category
        data_validation_checks = [c for c in result.checks if c.category == "data_validation"]
        assert len(data_validation_checks) == 0

    def test_validate_with_data_validation(self, geoparquet_v1_file):
        """Test validation with data validation enabled."""
        result = validate_geoparquet(geoparquet_v1_file, validate_data=True)
        # Should have data_validation category
        data_validation_checks = [c for c in result.checks if c.category == "data_validation"]
        assert len(data_validation_checks) > 0

    def test_validate_v1_with_covering(self, geoparquet_v1_with_covering):
        """Test validating GeoParquet 1.1 file with bbox covering."""
        result = validate_geoparquet(geoparquet_v1_with_covering)
        assert result.is_valid is True
        # Should have 1.1 checks for covering
        categories = {c.category for c in result.checks}
        assert "geoparquet_1_1" in categories

    def test_validate_with_target_version_1_0(self, geoparquet_v1_file):
        """Test validation with target_version='1.0' to validate against GeoParquet 1.0."""
        result = validate_geoparquet(geoparquet_v1_file, target_version="1.0")
        assert result.target_version == "1.0"
        assert result.is_valid is True
        # 1.0 validation should not include 1.1 or 2.0 specific checks
        categories = {c.category for c in result.checks}
        assert "geoparquet_1_1" not in categories
        assert "geoparquet_2_0" not in categories

    def test_validate_with_target_version_2_0(self, geoparquet_v2_file):
        """Test validation with target_version='2.0' to validate against GeoParquet 2.0."""
        result = validate_geoparquet(geoparquet_v2_file, target_version="2.0")
        assert result.target_version == "2.0"
        assert result.is_valid is True
        # 2.0 validation should include 2.0 specific checks
        categories = {c.category for c in result.checks}
        assert "geoparquet_2_0" in categories

    def test_validate_v1_file_against_v2_target(self, geoparquet_v1_file):
        """Test validating a 1.0 file against 2.0 target version fails appropriately."""
        result = validate_geoparquet(geoparquet_v1_file, target_version="2.0")
        assert result.target_version == "2.0"
        assert result.detected_version == "1.0.0"
        # When detected version != target version, validation fails with version_check
        assert result.is_valid is False
        categories = {c.category for c in result.checks}
        assert "version_check" in categories
        # Should have failed version_match check
        version_checks = [c for c in result.checks if c.category == "version_check"]
        failed_checks = [c for c in version_checks if c.status == CheckStatus.FAILED]
        assert len(failed_checks) > 0


class TestValidateSampleSize:
    """Test sample size parameter."""

    def test_validate_small_sample(self, geoparquet_v1_file):
        """Test validation with small sample size."""
        result = validate_geoparquet(geoparquet_v1_file, validate_data=True, sample_size=10)
        assert result.is_valid is True

    def test_validate_all_rows(self, geoparquet_v1_file):
        """Test validation with all rows (sample_size=0)."""
        result = validate_geoparquet(geoparquet_v1_file, validate_data=True, sample_size=0)
        assert result.is_valid is True


# =============================================================================
# Test JSON Output
# =============================================================================


class TestJsonOutput:
    """Test JSON output formatting."""

    def test_format_json_output(self):
        """Test JSON output format."""
        result = ValidationResult(
            file_path="/test/file.parquet",
            detected_version="1.0.0",
            target_version=None,
            checks=[
                ValidationCheck("c1", CheckStatus.PASSED, "ok", "cat"),
                ValidationCheck("c2", CheckStatus.FAILED, "fail", "cat", "details"),
            ],
        )
        json_str = format_json_output(result)
        data = json.loads(json_str)

        assert data["file_path"] == "/test/file.parquet"
        assert data["detected_version"] == "1.0.0"
        assert data["is_valid"] is False
        assert data["summary"]["passed"] == 1
        assert data["summary"]["failed"] == 1
        assert len(data["checks"]) == 2


# =============================================================================
# Test CLI Command
# =============================================================================


class TestValidateCLI:
    """Test the validate CLI command."""

    def test_validate_basic(self, geoparquet_v1_file):
        """Test basic validate command."""
        runner = CliRunner()
        result = runner.invoke(validate, [geoparquet_v1_file])
        # Exit code 0 = all passed
        assert result.exit_code == 0
        assert "GeoParquet Validation Report" in result.output
        assert "passed" in result.output.lower()

    def test_validate_json_output(self, geoparquet_v1_file):
        """Test validate with JSON output."""
        runner = CliRunner()
        result = runner.invoke(validate, [geoparquet_v1_file, "--json"])
        assert result.exit_code == 0
        # Should be valid JSON (filter out deprecation warning)
        json_output = _extract_json_from_output(result.output)
        data = json.loads(json_output)
        assert "is_valid" in data
        assert data["is_valid"] is True

    def test_validate_skip_data_validation(self, geoparquet_v1_file):
        """Test validate with --skip-data-validation."""
        runner = CliRunner()
        result = runner.invoke(validate, [geoparquet_v1_file, "--skip-data-validation"])
        assert result.exit_code == 0
        # Should not mention data validation results
        assert "Data Validation" not in result.output

    def test_validate_verbose(self, geoparquet_v1_file):
        """Test validate with --verbose."""
        runner = CliRunner()
        result = runner.invoke(validate, [geoparquet_v1_file, "--verbose"])
        assert result.exit_code == 0

    def test_validate_sample_size(self, geoparquet_v1_file):
        """Test validate with --sample-size."""
        runner = CliRunner()
        result = runner.invoke(validate, [geoparquet_v1_file, "--sample-size", "50"])
        assert result.exit_code == 0

    def test_validate_parquet_geo_only(self, parquet_geo_only_file):
        """Test validate on parquet-geo-only file."""
        runner = CliRunner()
        result = runner.invoke(validate, [parquet_geo_only_file])
        # Exit code 0 (pass) or 2 (warnings only, e.g. missing row group stats)
        assert result.exit_code in [0, 2]
        assert "parquet-geo-only" in result.output.lower()

    def test_validate_geoparquet_v2(self, geoparquet_v2_file):
        """Test validate on GeoParquet 2.0 file."""
        runner = CliRunner()
        result = runner.invoke(validate, [geoparquet_v2_file])
        # Exit code 0 (pass) or 2 (warnings only, e.g. missing row group stats)
        assert result.exit_code in [0, 2]
        assert "2.0" in result.output

    def test_validate_help(self):
        """Test validate --help."""
        runner = CliRunner()
        result = runner.invoke(validate, ["--help"])
        assert result.exit_code == 0
        assert "Validate a GeoParquet file" in result.output

    def test_validate_with_geoparquet_version_1_0(self, geoparquet_v1_file):
        """Test validate with --geoparquet-version 1.0."""
        runner = CliRunner()
        result = runner.invoke(validate, [geoparquet_v1_file, "--geoparquet-version", "1.0"])
        assert result.exit_code == 0
        assert "1.0" in result.output

    def test_validate_with_geoparquet_version_2_0(self, geoparquet_v2_file):
        """Test validate with --geoparquet-version 2.0."""
        runner = CliRunner()
        result = runner.invoke(validate, [geoparquet_v2_file, "--geoparquet-version", "2.0"])
        # Exit code 0 (pass) or 2 (warnings only)
        assert result.exit_code in [0, 2]
        assert "2.0" in result.output

    def test_validate_with_invalid_geoparquet_version(self, geoparquet_v1_file):
        """Test validate with invalid --geoparquet-version value."""
        runner = CliRunner()
        result = runner.invoke(validate, [geoparquet_v1_file, "--geoparquet-version", "99.0"])
        # Should fail with error for invalid version
        assert result.exit_code != 0

    def test_validate_v1_file_with_v2_version_flag(self, geoparquet_v1_file):
        """Test validating a 1.0 file with --geoparquet-version 2.0 returns exit code 1."""
        runner = CliRunner()
        result = runner.invoke(validate, [geoparquet_v1_file, "--geoparquet-version", "2.0"])
        # Should fail because 1.0 file doesn't meet 2.0 requirements
        assert result.exit_code == 1


# =============================================================================
# Test Exit Codes
# =============================================================================


class TestExitCodes:
    """Test CLI exit codes."""

    def test_exit_code_0_on_pass(self, geoparquet_v1_file):
        """Test exit code 0 when all checks pass."""
        runner = CliRunner()
        result = runner.invoke(validate, [geoparquet_v1_file])
        assert result.exit_code == 0

    def test_exit_code_with_parquet_geo_only(self, parquet_geo_only_file):
        """Test exit code with parquet-geo-only file."""
        runner = CliRunner()
        result = runner.invoke(validate, [parquet_geo_only_file])
        # Should be 0 (pass) or 2 (warnings only, e.g. missing row group stats)
        assert result.exit_code in [0, 2]

    def test_exit_code_1_on_invalid_file(self, tmp_path):
        """Test that CLI returns exit code 1 for an invalid (non-parquet) file.

        Creates a file with arbitrary non-parquet bytes and verifies the CLI
        returns exit code 1 indicating validation failure.
        """
        invalid_file = tmp_path / "not_a_parquet.parquet"
        invalid_file.write_bytes(b"this is not a valid parquet file")

        runner = CliRunner()
        result = runner.invoke(validate, [str(invalid_file)])
        assert result.exit_code == 1


# =============================================================================
# Test Edge Cases
# =============================================================================


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_validate_nonexistent_file(self):
        """Test validation of non-existent file."""
        runner = CliRunner()
        result = runner.invoke(validate, ["/nonexistent/file.parquet"])
        assert result.exit_code != 0

    def test_validate_v1_1_checks_only_on_v1_1(self, geoparquet_v1_file):
        """Test that 1.1 checks run on 1.1+ files."""
        # The buildings_test.parquet is 1.0.0, so 1.1 checks shouldn't run
        result = validate_geoparquet(geoparquet_v1_file)
        categories = {c.category for c in result.checks}
        # 1.0.0 file should not have 1.1 checks
        assert "geoparquet_1_1" not in categories

    def test_validate_result_properties(self, geoparquet_v1_file):
        """Test ValidationResult computed properties."""
        result = validate_geoparquet(geoparquet_v1_file)
        # Properties should be consistent
        total = result.passed_count + result.failed_count + result.warning_count
        skipped = sum(1 for c in result.checks if c.status == CheckStatus.SKIPPED)
        assert total + skipped == len(result.checks)


class TestValidateWithCovering:
    """Test validation of files with bbox covering."""

    def test_validate_file_with_covering(self, geoparquet_v1_with_covering):
        """Test validation of file with bbox covering metadata."""
        result = validate_geoparquet(geoparquet_v1_with_covering)
        assert result.is_valid is True

        # Should have covering-related checks
        covering_checks = [c for c in result.checks if "covering" in c.name.lower()]
        assert len(covering_checks) > 0


# =============================================================================
# Test Deprecation and New check spec Command
# =============================================================================


class TestValidateDeprecation:
    """Test that validate command is deprecated."""

    def test_validate_shows_deprecation_warning(self, geoparquet_v1_file):
        """Test that validate command shows deprecation warning."""
        runner = CliRunner()
        # Use cli to invoke validate to test the full deprecation flow
        result = runner.invoke(cli, ["validate", geoparquet_v1_file])
        assert result.exit_code == 0
        assert "deprecated" in result.output.lower()
        assert "gpio check spec" in result.output


class TestCheckSpec:
    """Test the new check spec subcommand."""

    def test_check_spec_basic(self, geoparquet_v1_file):
        """Test basic check spec command."""
        runner = CliRunner()
        result = runner.invoke(cli, ["check", "spec", geoparquet_v1_file])
        assert result.exit_code == 0
        assert "GeoParquet Validation Report" in result.output

    def test_check_spec_json_output(self, geoparquet_v1_file):
        """Test check spec with --json flag produces valid JSON."""
        runner = CliRunner()
        result = runner.invoke(cli, ["check", "spec", geoparquet_v1_file, "--json"])
        assert result.exit_code == 0
        # No deprecation warning, should be pure JSON
        data = json.loads(result.output)
        assert "is_valid" in data
        assert data["is_valid"] is True

    def test_check_spec_skip_data_validation(self, geoparquet_v1_file):
        """Test check spec with --skip-data-validation."""
        runner = CliRunner()
        result = runner.invoke(cli, ["check", "spec", geoparquet_v1_file, "--skip-data-validation"])
        assert result.exit_code == 0
        assert "Data Validation" not in result.output

    def test_check_spec_with_version(self, geoparquet_v1_file):
        """Test check spec with --geoparquet-version."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["check", "spec", geoparquet_v1_file, "--geoparquet-version", "1.0"]
        )
        assert result.exit_code == 0
