"""
Tests for GeoParquet version writing support.

Tests verify:
- Version constants and configuration
- Writing GeoParquet 1.0, 1.1, 2.0
- Writing parquet-geo-only (no GeoParquet metadata)
- Version conversions between formats
- CLI options for version control
- Round-trip preservation of data
"""

import os

import duckdb
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.common import (
    DEFAULT_GEOPARQUET_VERSION,
    GEOPARQUET_VERSIONS,
)
from geoparquet_io.core.convert import convert_to_geoparquet
from tests.conftest import (
    get_geo_metadata,
    get_geoparquet_version,
    has_geoparquet_metadata,
    has_native_geo_types,
)


class TestGeoParquetVersionConstants:
    """Test version configuration constants."""

    def test_version_mapping_exists(self):
        """Verify all expected versions are defined."""
        assert "1.0" in GEOPARQUET_VERSIONS
        assert "1.1" in GEOPARQUET_VERSIONS
        assert "2.0" in GEOPARQUET_VERSIONS
        assert "parquet-geo-only" in GEOPARQUET_VERSIONS

    def test_default_version(self):
        """Verify default version is 1.1."""
        assert DEFAULT_GEOPARQUET_VERSION == "1.1"

    def test_version_config_structure(self):
        """Verify each version config has required keys."""
        for version, config in GEOPARQUET_VERSIONS.items():
            assert "duckdb_param" in config, f"{version} missing duckdb_param"
            assert "metadata_version" in config, f"{version} missing metadata_version"
            assert "rewrite_metadata" in config, f"{version} missing rewrite_metadata"

    def test_duckdb_params(self):
        """Verify DuckDB parameters are correct."""
        assert GEOPARQUET_VERSIONS["1.0"]["duckdb_param"] == "V1"
        assert GEOPARQUET_VERSIONS["1.1"]["duckdb_param"] == "V1"
        assert GEOPARQUET_VERSIONS["2.0"]["duckdb_param"] == "V2"
        assert GEOPARQUET_VERSIONS["parquet-geo-only"]["duckdb_param"] == "NONE"

    def test_metadata_versions(self):
        """Verify metadata version strings are correct."""
        assert GEOPARQUET_VERSIONS["1.0"]["metadata_version"] == "1.0.0"
        assert GEOPARQUET_VERSIONS["1.1"]["metadata_version"] == "1.1.0"
        assert GEOPARQUET_VERSIONS["2.0"]["metadata_version"] == "2.0.0"
        assert GEOPARQUET_VERSIONS["parquet-geo-only"]["metadata_version"] is None


class TestWriteGeoParquetV1:
    """Test writing GeoParquet 1.0 and 1.1."""

    def test_convert_default_version(self, geojson_input, temp_output_file):
        """Test default version is 1.1."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
        )

        assert os.path.exists(temp_output_file)
        assert get_geoparquet_version(temp_output_file) == "1.1.0"

    def test_convert_explicit_1_1(self, geojson_input, temp_output_file):
        """Test explicit version 1.1."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version="1.1",
        )

        assert get_geoparquet_version(temp_output_file) == "1.1.0"
        assert has_geoparquet_metadata(temp_output_file)

    def test_convert_1_0(self, geojson_input, temp_output_file):
        """Test version 1.0."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version="1.0",
        )

        assert get_geoparquet_version(temp_output_file) == "1.0.0"
        assert has_geoparquet_metadata(temp_output_file)

    def test_geometry_encoding_wkb(self, geojson_input, temp_output_file):
        """Verify geometry encoding is WKB for v1."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version="1.1",
        )

        geo_meta = get_geo_metadata(temp_output_file)
        assert geo_meta is not None
        primary_col = geo_meta.get("primary_column")
        assert geo_meta["columns"][primary_col]["encoding"] == "WKB"


class TestWriteGeoParquetV2:
    """Test writing GeoParquet 2.0 with native Parquet geo types."""

    def test_convert_2_0_creates_file(self, geojson_input, temp_output_file):
        """Test v2.0 conversion creates a valid file."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version="2.0",
        )

        assert os.path.exists(temp_output_file)
        assert os.path.getsize(temp_output_file) > 0

    def test_2_0_has_native_geo_types(self, geojson_input, temp_output_file):
        """Test v2.0 uses native Parquet Geometry type."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version="2.0",
        )

        assert has_native_geo_types(temp_output_file)

    def test_2_0_has_geoparquet_metadata(self, geojson_input, temp_output_file):
        """Test v2.0 has GeoParquet metadata."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version="2.0",
        )

        assert has_geoparquet_metadata(temp_output_file)

    def test_2_0_metadata_version(self, geojson_input, temp_output_file):
        """Test v2.0 metadata has correct version."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version="2.0",
        )

        assert get_geoparquet_version(temp_output_file) == "2.0.0"

    def test_2_0_geometry_encoding_wkb(self, geojson_input, temp_output_file):
        """Verify geometry encoding is WKB for v2 (native types use WKB under the hood)."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version="2.0",
        )

        geo_meta = get_geo_metadata(temp_output_file)
        assert geo_meta is not None
        primary_col = geo_meta.get("primary_column")
        assert geo_meta["columns"][primary_col]["encoding"] == "WKB"

    def test_2_0_has_bbox_in_metadata(self, geojson_input, temp_output_file):
        """Test v2.0 metadata includes bbox."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version="2.0",
        )

        geo_meta = get_geo_metadata(temp_output_file)
        primary_col = geo_meta.get("primary_column")
        assert "bbox" in geo_meta["columns"][primary_col]


class TestWriteParquetGeoOnly:
    """Test writing Parquet geo types without GeoParquet metadata."""

    def test_parquet_geo_only_creates_file(self, geojson_input, temp_output_file):
        """Test parquet-geo-only creates a valid file."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version="parquet-geo-only",
        )

        assert os.path.exists(temp_output_file)
        assert os.path.getsize(temp_output_file) > 0

    def test_parquet_geo_only_has_native_geo_types(self, geojson_input, temp_output_file):
        """Test parquet-geo-only uses native Parquet Geometry type."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version="parquet-geo-only",
        )

        assert has_native_geo_types(temp_output_file)

    def test_parquet_geo_only_no_geo_metadata(self, geojson_input, temp_output_file):
        """Test parquet-geo-only has NO GeoParquet metadata."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version="parquet-geo-only",
        )

        assert not has_geoparquet_metadata(temp_output_file)

    def test_parquet_geo_only_readable_by_duckdb(self, geojson_input, temp_output_file):
        """Test parquet-geo-only file is readable by DuckDB."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version="parquet-geo-only",
        )

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{temp_output_file}')").fetchone()
        assert result[0] > 0
        con.close()


class TestVersionCLI:
    """Test CLI options for version control."""

    def test_cli_default_version(self, geojson_input, temp_output_file):
        """Test CLI default version is 1.1."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", geojson_input, temp_output_file, "--skip-hilbert"])

        assert result.exit_code == 0
        assert get_geoparquet_version(temp_output_file) == "1.1.0"

    def test_cli_explicit_1_0(self, geojson_input, temp_output_file):
        """Test CLI with explicit version 1.0."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                geojson_input,
                temp_output_file,
                "--skip-hilbert",
                "--geoparquet-version",
                "1.0",
            ],
        )

        assert result.exit_code == 0
        assert get_geoparquet_version(temp_output_file) == "1.0.0"

    def test_cli_explicit_1_1(self, geojson_input, temp_output_file):
        """Test CLI with explicit version 1.1."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                geojson_input,
                temp_output_file,
                "--skip-hilbert",
                "--geoparquet-version",
                "1.1",
            ],
        )

        assert result.exit_code == 0
        assert get_geoparquet_version(temp_output_file) == "1.1.0"

    def test_cli_explicit_2_0(self, geojson_input, temp_output_file):
        """Test CLI with explicit version 2.0."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                geojson_input,
                temp_output_file,
                "--skip-hilbert",
                "--geoparquet-version",
                "2.0",
            ],
        )

        assert result.exit_code == 0
        assert get_geoparquet_version(temp_output_file) == "2.0.0"
        assert has_native_geo_types(temp_output_file)

    def test_cli_parquet_geo_only(self, geojson_input, temp_output_file):
        """Test CLI with parquet-geo-only."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                geojson_input,
                temp_output_file,
                "--skip-hilbert",
                "--geoparquet-version",
                "parquet-geo-only",
            ],
        )

        assert result.exit_code == 0
        assert not has_geoparquet_metadata(temp_output_file)
        assert has_native_geo_types(temp_output_file)

    def test_cli_invalid_version_rejected(self, geojson_input, temp_output_file):
        """Test CLI rejects invalid version."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                geojson_input,
                temp_output_file,
                "--skip-hilbert",
                "--geoparquet-version",
                "3.0",
            ],
        )

        assert result.exit_code != 0
        assert "Invalid value" in result.output or "invalid choice" in result.output.lower()

    def test_cli_verbose_shows_version(self, geojson_input, temp_output_file):
        """Test verbose output shows version being used."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                geojson_input,
                temp_output_file,
                "--skip-hilbert",
                "--verbose",
                "--geoparquet-version",
                "2.0",
            ],
        )

        assert result.exit_code == 0
        assert "2.0" in result.output


class TestVersionRoundTrip:
    """Test reading/writing round-trip for each version."""

    def test_v1_roundtrip_preserves_geometry(self, geojson_input, temp_output_dir):
        """Test v1 round-trip preserves geometry data."""
        output1 = os.path.join(temp_output_dir, "v1_output.parquet")

        convert_to_geoparquet(
            geojson_input,
            output1,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version="1.1",
        )

        # Read and count geometries
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output1}')").fetchone()
        con.close()

        assert result[0] > 0

    def test_v2_roundtrip_preserves_geometry(self, geojson_input, temp_output_dir):
        """Test v2 round-trip preserves geometry data."""
        output1 = os.path.join(temp_output_dir, "v2_output.parquet")

        convert_to_geoparquet(
            geojson_input,
            output1,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version="2.0",
        )

        # Read and count geometries
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output1}')").fetchone()
        con.close()

        assert result[0] > 0


class TestExistingTestFiles:
    """Test that existing test files have expected structure."""

    def test_fields_v2_has_geoparquet_metadata(self, fields_v2_file):
        """Test fields_v2.parquet has GeoParquet 2.0 metadata."""
        assert has_geoparquet_metadata(fields_v2_file)
        assert get_geoparquet_version(fields_v2_file) == "2.0.0"

    def test_fields_v2_has_native_geo_types(self, fields_v2_file):
        """Test fields_v2.parquet has native Parquet Geometry type."""
        assert has_native_geo_types(fields_v2_file)

    def test_fields_geom_type_only_has_native_geo_types(self, fields_geom_type_only_file):
        """Test fields_geom_type_only.parquet has native Parquet Geometry type."""
        assert has_native_geo_types(fields_geom_type_only_file)

    def test_fields_geom_type_only_no_geoparquet_metadata(self, fields_geom_type_only_file):
        """Test fields_geom_type_only.parquet has NO GeoParquet metadata."""
        assert not has_geoparquet_metadata(fields_geom_type_only_file)

    def test_fields_5070_has_native_geo_types(self, fields_geom_type_only_5070_file):
        """Test fields_geom_type_only_5070.parquet has native Parquet Geometry type."""
        assert has_native_geo_types(fields_geom_type_only_5070_file)

    def test_fields_5070_no_geoparquet_metadata(self, fields_geom_type_only_5070_file):
        """Test fields_geom_type_only_5070.parquet has NO GeoParquet metadata."""
        assert not has_geoparquet_metadata(fields_geom_type_only_5070_file)


class TestCheckBboxVersionAware:
    """Test version-aware bbox checking."""

    def test_check_bbox_v2_no_bbox_passes(self, fields_v2_file):
        """V2 file without bbox should pass (bbox not recommended)."""
        from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

        result = check_metadata_and_bbox(fields_v2_file, verbose=False, return_results=True)

        assert result["passed"] is True
        assert result["file_type"] == "geoparquet_v2"
        assert result["has_bbox_column"] is False
        assert result["needs_bbox_removal"] is False
        assert result["fix_available"] is False

    def test_check_bbox_parquet_geo_only_with_bbox_reports_warning(
        self, fields_geom_type_only_file
    ):
        """Parquet-geo-only file with bbox should warn it's unnecessary."""
        from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

        result = check_metadata_and_bbox(
            fields_geom_type_only_file, verbose=False, return_results=True
        )

        assert result["passed"] is False
        assert result["file_type"] == "parquet_geo_only"
        assert result["has_bbox_column"] is True
        assert result["needs_bbox_removal"] is True
        assert result["fix_available"] is True
        assert len(result["issues"]) > 0
        assert "not needed" in result["issues"][0].lower()

    def test_check_bbox_parquet_geo_only_no_bbox_passes(self, fields_geom_type_only_5070_file):
        """Parquet-geo-only file without bbox should pass."""
        from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

        result = check_metadata_and_bbox(
            fields_geom_type_only_5070_file, verbose=False, return_results=True
        )

        assert result["passed"] is True
        assert result["file_type"] == "parquet_geo_only"
        assert result["has_bbox_column"] is False
        assert result["needs_bbox_removal"] is False
        assert result["fix_available"] is False

    def test_check_bbox_parquet_geo_only_no_metadata_not_error(self, fields_geom_type_only_file):
        """Parquet-geo-only should not report 'no metadata' as a critical error."""
        from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

        result = check_metadata_and_bbox(
            fields_geom_type_only_file, verbose=False, return_results=True
        )

        # Should recognize it as parquet_geo_only, not unknown
        assert result["file_type"] == "parquet_geo_only"
        assert result["has_geo_metadata"] is False
        assert result["has_native_geo_types"] is True

    def test_check_bbox_v2_file_type_detection(self, fields_v2_file):
        """V2 file should be correctly detected as geoparquet_v2."""
        from geoparquet_io.core.common import detect_geoparquet_file_type

        result = detect_geoparquet_file_type(fields_v2_file, verbose=False)

        assert result["file_type"] == "geoparquet_v2"
        assert result["has_geo_metadata"] is True
        assert result["geo_version"] == "2.0.0"
        assert result["has_native_geo_types"] is True
        assert result["bbox_recommended"] is False

    def test_check_bbox_parquet_geo_only_file_type_detection(self, fields_geom_type_only_file):
        """Parquet-geo-only file should be correctly detected."""
        from geoparquet_io.core.common import detect_geoparquet_file_type

        result = detect_geoparquet_file_type(fields_geom_type_only_file, verbose=False)

        assert result["file_type"] == "parquet_geo_only"
        assert result["has_geo_metadata"] is False
        assert result["geo_version"] is None
        assert result["has_native_geo_types"] is True
        assert result["bbox_recommended"] is False


class TestCheckBboxFix:
    """Test bbox fix functionality for different versions."""

    def test_fix_removes_bbox_from_parquet_geo_only(
        self, fields_geom_type_only_file, temp_output_file
    ):
        """--fix on parquet-geo-only with bbox should remove it."""
        from geoparquet_io.core.check_fixes import fix_bbox_removal
        from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

        # Verify original has bbox
        original_result = check_metadata_and_bbox(
            fields_geom_type_only_file, verbose=False, return_results=True
        )
        assert original_result["has_bbox_column"] is True

        # Apply fix
        fix_bbox_removal(
            fields_geom_type_only_file,
            temp_output_file,
            bbox_column_name="bbox",
            verbose=False,
        )

        # Verify fixed file has no bbox
        fixed_result = check_metadata_and_bbox(temp_output_file, verbose=False, return_results=True)
        assert fixed_result["has_bbox_column"] is False
        assert fixed_result["passed"] is True

    def test_fix_preserves_native_geo_type(self, fields_geom_type_only_file, temp_output_file):
        """Bbox removal should preserve native Parquet Geometry type."""
        from geoparquet_io.core.check_fixes import fix_bbox_removal

        fix_bbox_removal(
            fields_geom_type_only_file,
            temp_output_file,
            bbox_column_name="bbox",
            verbose=False,
        )

        assert has_native_geo_types(temp_output_file)

    def test_fix_preserves_data(self, fields_geom_type_only_file, temp_output_file):
        """Bbox removal should preserve all other data."""
        from geoparquet_io.core.check_fixes import fix_bbox_removal

        # Get original row count
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        original_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{fields_geom_type_only_file}')"
        ).fetchone()[0]

        fix_bbox_removal(
            fields_geom_type_only_file,
            temp_output_file,
            bbox_column_name="bbox",
            verbose=False,
        )

        # Verify row count preserved
        fixed_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{temp_output_file}')"
        ).fetchone()[0]
        con.close()

        assert fixed_count == original_count

    def test_cli_check_bbox_fix_removes_bbox(self, fields_geom_type_only_file, temp_output_file):
        """CLI check bbox --fix should remove bbox from parquet-geo-only file."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "check",
                "bbox",
                fields_geom_type_only_file,
                "--fix",
                "--fix-output",
                temp_output_file,
            ],
        )

        assert result.exit_code == 0
        assert "removed" in result.output.lower()

        # Verify bbox was removed
        from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

        fixed_result = check_metadata_and_bbox(temp_output_file, verbose=False, return_results=True)
        assert fixed_result["has_bbox_column"] is False

    def test_fix_preserves_parquet_geo_only_format(
        self, fields_geom_type_only_file, temp_output_file
    ):
        """Fixing parquet-geo-only file should NOT add GeoParquet metadata."""
        from geoparquet_io.core.check_fixes import fix_bbox_removal

        fix_bbox_removal(
            fields_geom_type_only_file,
            temp_output_file,
            bbox_column_name="bbox",
            verbose=False,
        )

        # Should still be parquet-geo-only (no GeoParquet metadata)
        assert not has_geoparquet_metadata(temp_output_file)
        assert has_native_geo_types(temp_output_file)


class TestVersionPreservation:
    """Test that fixes preserve the original GeoParquet version."""

    def test_get_version_from_check_results_v2(self):
        """Test version detection for v2 files."""
        from geoparquet_io.core.check_fixes import get_geoparquet_version_from_check_results

        check_results = {"bbox": {"file_type": "geoparquet_v2", "version": "2.0.0"}}
        version = get_geoparquet_version_from_check_results(check_results)
        assert version == "2.0"

    def test_get_version_from_check_results_parquet_geo_only(self):
        """Test version detection for parquet-geo-only files."""
        from geoparquet_io.core.check_fixes import get_geoparquet_version_from_check_results

        check_results = {"bbox": {"file_type": "parquet_geo_only"}}
        version = get_geoparquet_version_from_check_results(check_results)
        assert version == "parquet-geo-only"

    def test_get_version_from_check_results_v1_1(self):
        """Test version detection for v1.1 files."""
        from geoparquet_io.core.check_fixes import get_geoparquet_version_from_check_results

        check_results = {"bbox": {"file_type": "geoparquet_v1", "version": "1.1.0"}}
        version = get_geoparquet_version_from_check_results(check_results)
        assert version == "1.1"

    def test_get_version_from_check_results_v1_0(self):
        """Test version detection for v1.0 files."""
        from geoparquet_io.core.check_fixes import get_geoparquet_version_from_check_results

        check_results = {"bbox": {"file_type": "geoparquet_v1", "version": "1.0.0"}}
        version = get_geoparquet_version_from_check_results(check_results)
        assert version == "1.0"


class TestConvertSkipsBbox:
    """Test that convert skips bbox for 2.0 and parquet-geo-only."""

    def test_convert_2_0_no_bbox_column(self, geojson_input, temp_output_file):
        """Converting to 2.0 should not add bbox column."""
        from geoparquet_io.core.common import check_bbox_structure

        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
        )

        bbox_info = check_bbox_structure(temp_output_file)
        assert bbox_info["has_bbox_column"] is False

    def test_convert_parquet_geo_only_no_bbox_column(self, geojson_input, temp_output_file):
        """Converting to parquet-geo-only should not add bbox column."""
        from geoparquet_io.core.common import check_bbox_structure

        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="parquet-geo-only",
        )

        bbox_info = check_bbox_structure(temp_output_file)
        assert bbox_info["has_bbox_column"] is False

    def test_convert_1_1_has_bbox_column(self, geojson_input, temp_output_file):
        """Converting to 1.1 should add bbox column."""
        from geoparquet_io.core.common import check_bbox_structure

        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="1.1",
        )

        bbox_info = check_bbox_structure(temp_output_file)
        assert bbox_info["has_bbox_column"] is True

    def test_convert_1_0_has_bbox_column(self, geojson_input, temp_output_file):
        """Converting to 1.0 should add bbox column."""
        from geoparquet_io.core.common import check_bbox_structure

        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="1.0",
        )

        bbox_info = check_bbox_structure(temp_output_file)
        assert bbox_info["has_bbox_column"] is True

    def test_convert_removes_existing_bbox_to_2_0(
        self, fields_geom_type_only_file, temp_output_file
    ):
        """Converting parquet with bbox to 2.0 should remove bbox."""
        from geoparquet_io.core.common import check_bbox_structure

        # Verify source has bbox
        source_bbox = check_bbox_structure(fields_geom_type_only_file)
        assert source_bbox["has_bbox_column"] is True

        convert_to_geoparquet(
            fields_geom_type_only_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
        )

        # Verify output has no bbox
        result_bbox = check_bbox_structure(temp_output_file)
        assert result_bbox["has_bbox_column"] is False

    def test_convert_removes_existing_bbox_to_parquet_geo_only(
        self, fields_geom_type_only_file, temp_output_file
    ):
        """Converting parquet with bbox to parquet-geo-only should remove bbox."""
        from geoparquet_io.core.common import check_bbox_structure

        # Verify source has bbox
        source_bbox = check_bbox_structure(fields_geom_type_only_file)
        assert source_bbox["has_bbox_column"] is True

        convert_to_geoparquet(
            fields_geom_type_only_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="parquet-geo-only",
        )

        # Verify output has no bbox
        result_bbox = check_bbox_structure(temp_output_file)
        assert result_bbox["has_bbox_column"] is False

    def test_convert_preserves_bbox_for_1_1_from_parquet(
        self, fields_geom_type_only_file, temp_output_file
    ):
        """Converting parquet with bbox to 1.1 should preserve bbox."""
        from geoparquet_io.core.common import check_bbox_structure

        convert_to_geoparquet(
            fields_geom_type_only_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="1.1",
        )

        result_bbox = check_bbox_structure(temp_output_file)
        assert result_bbox["has_bbox_column"] is True

    def test_convert_preserves_data_when_removing_bbox(
        self, fields_geom_type_only_file, temp_output_file
    ):
        """Converting to 2.0 should preserve all data except bbox."""
        # Get original row count (exclude bbox column)
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        original_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{fields_geom_type_only_file}')"
        ).fetchone()[0]

        convert_to_geoparquet(
            fields_geom_type_only_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
        )

        # Verify row count preserved
        result_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{temp_output_file}')"
        ).fetchone()[0]
        con.close()

        assert result_count == original_count
