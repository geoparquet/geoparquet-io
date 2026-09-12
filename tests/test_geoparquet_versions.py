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
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.common import DEFAULT_GEOPARQUET_VERSION, GEOPARQUET_VERSIONS
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.validate import CheckStatus
from tests.conftest import (
    get_geo_metadata,
    get_geoparquet_version,
    has_geoparquet_metadata,
    has_native_geo_types,
)


def _is_wkb_type(arrow_type) -> bool:
    """True when an Arrow type is WKB (plain/large binary or a geoarrow.wkb extension).

    Uses pyarrow type predicates rather than a string comparison so it also catches
    large_binary and geoarrow.wkb, which a ``str(type) != "binary"`` check would miss.
    """
    import pyarrow as pa

    return (
        pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
        or getattr(arrow_type, "extension_name", "") == "geoarrow.wkb"
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

    @pytest.mark.parametrize(
        "version,expected_version",
        [
            (None, "1.1.0"),  # default
            ("1.1", "1.1.0"),  # explicit 1.1
            ("1.0", "1.0.0"),  # explicit 1.0
        ],
        ids=["default", "explicit-1.1", "explicit-1.0"],
    )
    def test_convert_version(self, geojson_input, temp_output_file, version, expected_version):
        """Test conversion to various v1.x versions."""
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            verbose=False,
            geoparquet_version=version,
        )

        assert os.path.exists(temp_output_file)
        assert get_geoparquet_version(temp_output_file) == expected_version
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
    """Test CLI options for version control.

    One parametrized matrix over (flags, expected version, native types);
    every cell has a distinct expected outcome, so every former test survives
    as a param case (issue #666 item 10).
    """

    @pytest.mark.parametrize(
        ("extra_args", "expected_version", "expect_native_types", "output_contains"),
        [
            # expected_version: metadata version string; None = no geo metadata
            # expected at all; "invalid" = the CLI must reject the invocation.
            # "Done in" pins the non-verbose progress message: the dropped
            # test_cli_output_messages asserted it in default mode, and line
            # coverage alone cannot notice it moving behind --verbose.
            pytest.param([], "1.1.0", False, "Done in", id="default-1.1"),
            pytest.param(["--geoparquet-version", "1.0"], "1.0.0", False, None, id="explicit-1.0"),
            pytest.param(["--geoparquet-version", "1.1"], "1.1.0", False, None, id="explicit-1.1"),
            pytest.param(["--geoparquet-version", "2.0"], "2.0.0", True, None, id="explicit-2.0"),
            pytest.param(
                ["--geoparquet-version", "parquet-geo-only"],
                None,
                True,
                None,
                id="parquet-geo-only",
            ),
            pytest.param(
                ["--geoparquet-version", "3.0"], "invalid", False, None, id="invalid-3.0-rejected"
            ),
            pytest.param(
                ["--verbose", "--geoparquet-version", "2.0"],
                "2.0.0",
                True,
                "2.0",
                id="verbose-shows-version",
            ),
        ],
    )
    def test_cli_version_matrix(
        self,
        geojson_input,
        temp_output_file,
        extra_args,
        expected_version,
        expect_native_types,
        output_contains,
    ):
        """Each version flag produces its distinct expected output format."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["convert", geojson_input, temp_output_file, "--skip-hilbert", *extra_args],
        )

        if expected_version == "invalid":
            assert result.exit_code != 0
            assert "Invalid value" in result.output or "invalid choice" in result.output.lower()
            return

        assert result.exit_code == 0
        if expected_version is None:
            assert not has_geoparquet_metadata(temp_output_file)
        else:
            assert get_geoparquet_version(temp_output_file) == expected_version
        if expect_native_types:
            assert has_native_geo_types(temp_output_file)
        if output_contains is not None:
            assert output_contains in result.output


@pytest.mark.slow
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

    def test_austria_bbox_covering_has_geoparquet_metadata(self, austria_bbox_covering_file):
        """Test austria_bbox_covering.parquet has GeoParquet 1.1 metadata."""
        assert has_geoparquet_metadata(austria_bbox_covering_file)
        assert get_geoparquet_version(austria_bbox_covering_file) == "1.1.0"

    def test_austria_bbox_covering_has_nonstandard_bbox_name(self, austria_bbox_covering_file):
        """Test austria_bbox_covering.parquet has 'geometry_bbox' column (not 'bbox')."""
        from geoparquet_io.core.common import check_bbox_structure

        bbox_info = check_bbox_structure(austria_bbox_covering_file)
        assert bbox_info["has_bbox_column"] is True
        assert bbox_info["bbox_column_name"] == "geometry_bbox"
        assert bbox_info["has_bbox_metadata"] is True

    def test_austria_bbox_covering_has_proper_covering_metadata(self, austria_bbox_covering_file):
        """Test austria_bbox_covering.parquet has proper bbox covering in geo metadata."""
        geo_meta = get_geo_metadata(austria_bbox_covering_file)
        assert geo_meta is not None

        # Check that covering references the correct column
        geom_col_meta = geo_meta["columns"]["geometry"]
        assert "covering" in geom_col_meta
        assert "bbox" in geom_col_meta["covering"]

        bbox_covering = geom_col_meta["covering"]["bbox"]
        assert bbox_covering["xmin"] == ["geometry_bbox", "xmin"]
        assert bbox_covering["ymin"] == ["geometry_bbox", "ymin"]
        assert bbox_covering["xmax"] == ["geometry_bbox", "xmax"]
        assert bbox_covering["ymax"] == ["geometry_bbox", "ymax"]


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

    def test_check_bbox_v1_nonstandard_bbox_column_passes(self, austria_bbox_covering_file):
        """V1.1 file with non-standard bbox column name should pass if properly registered."""
        from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

        result = check_metadata_and_bbox(
            austria_bbox_covering_file, verbose=False, return_results=True
        )

        assert result["passed"] is True
        assert result["file_type"] == "geoparquet_v1"
        assert result["has_bbox_column"] is True
        assert result["bbox_column_name"] == "geometry_bbox"
        assert result["has_bbox_metadata"] is True


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


@pytest.mark.slow
class TestComprehensiveRoundTrips:
    """Comprehensive round-trip conversion tests between all version combinations."""

    def test_v1_0_to_v1_1_roundtrip(self, geojson_input, temp_output_dir):
        """Test v1.0 → v1.1 → v1.0 preserves data."""
        v1_0_file = os.path.join(temp_output_dir, "v1_0.parquet")
        v1_1_file = os.path.join(temp_output_dir, "v1_1.parquet")
        roundtrip_file = os.path.join(temp_output_dir, "roundtrip.parquet")

        # Create v1.0
        convert_to_geoparquet(geojson_input, v1_0_file, skip_hilbert=True, geoparquet_version="1.0")

        # Convert to v1.1
        convert_to_geoparquet(v1_0_file, v1_1_file, skip_hilbert=True, geoparquet_version="1.1")

        # Convert back to v1.0
        convert_to_geoparquet(
            v1_1_file, roundtrip_file, skip_hilbert=True, geoparquet_version="1.0"
        )

        # Verify versions
        assert get_geoparquet_version(v1_0_file) == "1.0.0"
        assert get_geoparquet_version(v1_1_file) == "1.1.0"
        assert get_geoparquet_version(roundtrip_file) == "1.0.0"

        # Verify row counts match
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        count_v1_0 = con.execute(f"SELECT COUNT(*) FROM read_parquet('{v1_0_file}')").fetchone()[0]
        count_roundtrip = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{roundtrip_file}')"
        ).fetchone()[0]

        con.close()

        assert count_v1_0 == count_roundtrip

    def test_v1_1_to_v2_0_roundtrip(self, geojson_input, temp_output_dir):
        """Test v1.1 → v2.0 → v1.1 preserves data."""
        v1_1_file = os.path.join(temp_output_dir, "v1_1.parquet")
        v2_0_file = os.path.join(temp_output_dir, "v2_0.parquet")
        roundtrip_file = os.path.join(temp_output_dir, "roundtrip.parquet")

        # Create v1.1
        convert_to_geoparquet(geojson_input, v1_1_file, skip_hilbert=True, geoparquet_version="1.1")

        # Convert to v2.0
        convert_to_geoparquet(v1_1_file, v2_0_file, skip_hilbert=True, geoparquet_version="2.0")

        # Convert back to v1.1
        convert_to_geoparquet(
            v2_0_file, roundtrip_file, skip_hilbert=True, geoparquet_version="1.1"
        )

        # Verify versions and types
        assert get_geoparquet_version(v1_1_file) == "1.1.0"
        assert get_geoparquet_version(v2_0_file) == "2.0.0"
        assert get_geoparquet_version(roundtrip_file) == "1.1.0"

        assert not has_native_geo_types(v1_1_file)
        assert has_native_geo_types(v2_0_file)
        assert not has_native_geo_types(roundtrip_file)

    def test_v2_0_to_parquet_geo_only_roundtrip(self, geojson_input, temp_output_dir):
        """Test v2.0 → parquet-geo-only → v2.0 preserves data."""
        v2_0_file = os.path.join(temp_output_dir, "v2_0.parquet")
        pgo_file = os.path.join(temp_output_dir, "parquet_geo_only.parquet")
        roundtrip_file = os.path.join(temp_output_dir, "roundtrip.parquet")

        # Create v2.0
        convert_to_geoparquet(geojson_input, v2_0_file, skip_hilbert=True, geoparquet_version="2.0")

        # Convert to parquet-geo-only
        convert_to_geoparquet(
            v2_0_file, pgo_file, skip_hilbert=True, geoparquet_version="parquet-geo-only"
        )

        # Convert back to v2.0
        convert_to_geoparquet(pgo_file, roundtrip_file, skip_hilbert=True, geoparquet_version="2.0")

        # Verify metadata presence
        assert has_geoparquet_metadata(v2_0_file)
        assert not has_geoparquet_metadata(pgo_file)
        assert has_geoparquet_metadata(roundtrip_file)

        # Both should have native types
        assert has_native_geo_types(v2_0_file)
        assert has_native_geo_types(pgo_file)
        assert has_native_geo_types(roundtrip_file)

    def test_parquet_geo_only_to_v1_1_roundtrip(self, temp_output_dir):
        """Test parquet-geo-only → v1.1 → parquet-geo-only."""
        test_data_dir = os.path.join(os.path.dirname(__file__), "data")
        pgo_input = os.path.join(test_data_dir, "fields_pgo_crs84_bbox_snappy.parquet")

        v1_1_file = os.path.join(temp_output_dir, "v1_1.parquet")
        pgo_output = os.path.join(temp_output_dir, "pgo_output.parquet")

        # Convert to v1.1
        convert_to_geoparquet(pgo_input, v1_1_file, skip_hilbert=True, geoparquet_version="1.1")

        # Convert back to parquet-geo-only
        convert_to_geoparquet(
            v1_1_file, pgo_output, skip_hilbert=True, geoparquet_version="parquet-geo-only"
        )

        # Verify types change correctly
        assert not has_native_geo_types(v1_1_file)
        assert has_native_geo_types(pgo_output)

        assert has_geoparquet_metadata(v1_1_file)
        assert not has_geoparquet_metadata(pgo_output)

    def test_all_versions_cycle(self, geojson_input, temp_output_dir):
        """Test complete cycle: v1.0 → v1.1 → v2.0 → parquet-geo-only → v2.0 → v1.1 → v1.0."""
        files = {}
        versions = ["1.0", "1.1", "2.0", "parquet-geo-only", "2.0", "1.1", "1.0"]

        current_input = geojson_input

        for i, version in enumerate(versions):
            output_file = os.path.join(
                temp_output_dir, f"step_{i}_{version.replace('.', '_')}.parquet"
            )
            convert_to_geoparquet(
                current_input,
                output_file,
                skip_hilbert=True,
                geoparquet_version=version,
            )
            files[i] = (version, output_file)
            current_input = output_file

        # Verify all conversions succeeded
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        counts = {}
        for i, (_version, file_path) in files.items():
            count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{file_path}')").fetchone()[0]
            counts[i] = count

        con.close()

        # All should have same row count
        unique_counts = set(counts.values())
        assert len(unique_counts) == 1, f"Row counts vary across conversions: {counts}"

    def test_roundtrip_with_crs_preservation(self, temp_output_dir):
        """Test that CRS is preserved through round-trip conversions."""
        test_data_dir = os.path.join(os.path.dirname(__file__), "data")
        input_file = os.path.join(test_data_dir, "fields_pgo_5070_snappy.parquet")

        v2_file = os.path.join(temp_output_dir, "v2.parquet")
        v1_file = os.path.join(temp_output_dir, "v1.parquet")
        v2_roundtrip = os.path.join(temp_output_dir, "v2_roundtrip.parquet")

        # parquet-geo-only → v2.0 → v1.1 → v2.0
        convert_to_geoparquet(input_file, v2_file, skip_hilbert=True, geoparquet_version="2.0")
        convert_to_geoparquet(v2_file, v1_file, skip_hilbert=True, geoparquet_version="1.1")
        convert_to_geoparquet(v1_file, v2_roundtrip, skip_hilbert=True, geoparquet_version="2.0")

        # Check CRS in final file
        from tests.test_crs_conversion import get_geoparquet_crs, get_parquet_type_crs

        parquet_crs = get_parquet_type_crs(v2_roundtrip)
        geo_crs = get_geoparquet_crs(v2_roundtrip)

        # Both should have EPSG:5070
        assert parquet_crs is not None
        assert geo_crs is not None

        from geoparquet_io.core.crs_utils import _extract_crs_identifier

        assert _extract_crs_identifier(parquet_crs) == ("EPSG", 5070)
        assert _extract_crs_identifier(geo_crs) == ("EPSG", 5070)

    def test_geometry_type_preservation_roundtrip(self, geojson_input, temp_output_dir):
        """Test that geometry types are preserved through conversions."""
        v1_file = os.path.join(temp_output_dir, "v1.parquet")
        v2_file = os.path.join(temp_output_dir, "v2.parquet")
        v1_roundtrip = os.path.join(temp_output_dir, "v1_roundtrip.parquet")

        # v1.1 → v2.0 → v1.1
        convert_to_geoparquet(geojson_input, v1_file, skip_hilbert=True, geoparquet_version="1.1")
        convert_to_geoparquet(v1_file, v2_file, skip_hilbert=True, geoparquet_version="2.0")
        convert_to_geoparquet(v2_file, v1_roundtrip, skip_hilbert=True, geoparquet_version="1.1")

        # Verify encoding in metadata
        geo_meta_v1 = get_geo_metadata(v1_file)
        geo_meta_roundtrip = get_geo_metadata(v1_roundtrip)

        primary_col = geo_meta_v1.get("primary_column", "geometry")

        assert geo_meta_v1["columns"][primary_col]["encoding"] == "WKB"
        assert geo_meta_roundtrip["columns"][primary_col]["encoding"] == "WKB"


class TestExtractGeoParquetVersion:
    """Test --geoparquet-version option on extract command."""

    def test_extract_preserves_input_version(self, fields_v2_file, temp_output_file):
        """Test extract preserves input version (2.0 input -> 2.0 output)."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["extract", fields_v2_file, temp_output_file, "--limit", "5"],
        )

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert os.path.exists(temp_output_file)
        # Input is GeoParquet 2.0, so output should also be 2.0
        assert get_geoparquet_version(temp_output_file) == "2.0.0"

    def test_extract_version_2_0(self, fields_v2_file, temp_output_file):
        """Test extract with explicit 2.0 version."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                fields_v2_file,
                temp_output_file,
                "--limit",
                "5",
                "--geoparquet-version",
                "2.0",
            ],
        )

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert get_geoparquet_version(temp_output_file) == "2.0.0"
        assert has_native_geo_types(temp_output_file)

    def test_extract_parquet_geo_only(self, fields_v2_file, temp_output_file):
        """Test extract with parquet-geo-only version."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                fields_v2_file,
                temp_output_file,
                "--limit",
                "5",
                "--geoparquet-version",
                "parquet-geo-only",
            ],
        )

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert has_native_geo_types(temp_output_file)
        assert not has_geoparquet_metadata(temp_output_file)


class TestSortHilbertGeoParquetVersion:
    """Test --geoparquet-version option on sort hilbert command."""

    def test_sort_hilbert_default_version(self, places_test_file, temp_output_file):
        """Test sort hilbert uses default version (1.1)."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["sort", "hilbert", places_test_file, temp_output_file],
        )

        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)
        assert get_geoparquet_version(temp_output_file) == "1.1.0"

    def test_sort_hilbert_version_2_0(self, places_test_file, temp_output_file):
        """Test sort hilbert with explicit 2.0 version."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "sort",
                "hilbert",
                places_test_file,
                temp_output_file,
                "--geoparquet-version",
                "2.0",
            ],
        )

        assert result.exit_code == 0
        assert get_geoparquet_version(temp_output_file) == "2.0.0"
        assert has_native_geo_types(temp_output_file)

    def test_sort_hilbert_version_1_0(self, places_test_file, temp_output_file):
        """Test sort hilbert with explicit 1.0 version."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "sort",
                "hilbert",
                places_test_file,
                temp_output_file,
                "--geoparquet-version",
                "1.0",
            ],
        )

        assert result.exit_code == 0
        assert get_geoparquet_version(temp_output_file) == "1.0.0"
        assert not has_native_geo_types(temp_output_file)


class TestAddBboxGeoParquetVersion:
    """Test --geoparquet-version option on add bbox command."""

    def test_add_bbox_default_version(self, buildings_test_file, temp_output_file):
        """Test add bbox uses default version (1.1)."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["add", "bbox", buildings_test_file, temp_output_file],
        )

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert os.path.exists(temp_output_file)
        assert get_geoparquet_version(temp_output_file) == "1.1.0"

    def test_add_bbox_version_1_0(self, buildings_test_file, temp_output_file):
        """Test add bbox with explicit 1.0 version."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "bbox",
                buildings_test_file,
                temp_output_file,
                "--geoparquet-version",
                "1.0",
            ],
        )

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert get_geoparquet_version(temp_output_file) == "1.0.0"


@pytest.mark.slow
class TestPartitionGeoParquetVersion:
    """Test --geoparquet-version option on partition commands."""

    def test_partition_string_default_version(self, buildings_test_file, temp_output_dir):
        """Test partition string uses default version (1.1)."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "partition",
                "string",
                buildings_test_file,
                temp_output_dir,
                "--column",
                "id",
                "--skip-analysis",
                "--force",
            ],
        )

        assert result.exit_code == 0, f"Failed with: {result.output}"

        # Find a partition file
        parquet_files = [f for f in os.listdir(temp_output_dir) if f.endswith(".parquet")]
        assert len(parquet_files) > 0

        partition_file = os.path.join(temp_output_dir, parquet_files[0])
        assert get_geoparquet_version(partition_file) == "1.1.0"

    def test_partition_string_version_2_0(self, buildings_test_file, temp_output_dir):
        """Test partition string with explicit 2.0 version."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "partition",
                "string",
                buildings_test_file,
                temp_output_dir,
                "--column",
                "id",
                "--skip-analysis",
                "--force",
                "--geoparquet-version",
                "2.0",
            ],
        )

        assert result.exit_code == 0, f"Failed with: {result.output}"

        # Find a partition file
        parquet_files = [f for f in os.listdir(temp_output_dir) if f.endswith(".parquet")]
        assert len(parquet_files) > 0

        partition_file = os.path.join(temp_output_dir, parquet_files[0])
        assert get_geoparquet_version(partition_file) == "2.0.0"
        assert has_native_geo_types(partition_file)


class TestGeoParquet11GeoArrow:
    """Tests for --geoparquet-version 1.1-geoarrow output.

    This version writes GeoParquet 1.1.0 metadata, skips the bbox column, and
    encodes geometry using native GeoArrow nested-coordinate types. Native input
    encoding is preserved; WKB/text inputs are converted to native GeoArrow
    (falling back to WKB only for geometry-type mixes that cannot be unified).
    """

    def test_version_constant_exists(self):
        """1.1-geoarrow must be a registered version."""
        assert "1.1-geoarrow" in GEOPARQUET_VERSIONS

    def test_version_constant_structure(self):
        """1.1-geoarrow config must have the required keys."""
        config = GEOPARQUET_VERSIONS["1.1-geoarrow"]
        assert config["metadata_version"] == "1.1.0"
        assert "duckdb_param" in config
        assert "rewrite_metadata" in config

    def test_should_skip_bbox(self):
        """should_skip_bbox must return True for 1.1-geoarrow."""
        from geoparquet_io.core.common import should_skip_bbox

        assert should_skip_bbox("1.1-geoarrow") is True

    def test_cli_accepts_version(self, geojson_input, temp_output_file):
        """CLI must accept --geoparquet-version 1.1-geoarrow without error."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["convert", geojson_input, temp_output_file, "--geoparquet-version", "1.1-geoarrow"],
        )
        assert result.exit_code == 0, f"CLI rejected 1.1-geoarrow: {result.output}"

    def test_output_has_1_1_metadata(self, geojson_input, temp_output_file):
        """Output must carry GeoParquet 1.1.0 version in the geo metadata."""
        convert_to_geoparquet(
            geojson_input, temp_output_file, geoparquet_version="1.1-geoarrow", verbose=False
        )
        assert get_geoparquet_version(temp_output_file) == "1.1.0"

    def test_output_has_no_bbox_column(self, geojson_input, temp_output_file):
        """Output must not have a bbox column (the key benefit over plain 1.1)."""
        from geoparquet_io.core.common import check_bbox_structure

        convert_to_geoparquet(
            geojson_input, temp_output_file, geoparquet_version="1.1-geoarrow", verbose=False
        )
        bbox_info = check_bbox_structure(temp_output_file)
        assert not bbox_info["has_bbox_column"]

    def test_plain_1_1_has_bbox_column(self, geojson_input, temp_output_file):
        """Sanity check: plain 1.1 DOES add a bbox column."""
        from geoparquet_io.core.common import check_bbox_structure

        convert_to_geoparquet(
            geojson_input, temp_output_file, geoparquet_version="1.1", verbose=False
        )
        bbox_info = check_bbox_structure(temp_output_file)
        assert bbox_info["has_bbox_column"]

    def test_native_input_preserves_encoding(self, test_data_dir, tmp_path):
        """Converting a native-encoded file with 1.1-geoarrow keeps native geometry."""
        import pyarrow.parquet as pq

        input_file = str(test_data_dir / "data-multipolygon-encoding_native.parquet")
        output_file = str(tmp_path / "output.parquet")

        convert_to_geoparquet(
            input_file, output_file, geoparquet_version="1.1-geoarrow", verbose=False
        )

        schema = pq.read_schema(output_file)
        geom_field = schema.field("geometry")
        # Native geometry should NOT be a plain binary blob
        assert not _is_wkb_type(geom_field.type), "geometry should remain native, not WKB blob"

    def test_native_input_metadata_encoding_preserved(self, test_data_dir, tmp_path):
        """GeoParquet metadata must record the actual native encoding, not WKB."""
        input_file = str(test_data_dir / "data-multipolygon-encoding_native.parquet")
        output_file = str(tmp_path / "output.parquet")

        convert_to_geoparquet(
            input_file, output_file, geoparquet_version="1.1-geoarrow", verbose=False
        )

        geo_meta = get_geo_metadata(output_file)
        encoding = geo_meta["columns"]["geometry"]["encoding"]
        assert encoding == "multipolygon", (
            f"Expected 'multipolygon' encoding in metadata, got '{encoding}'"
        )

    def test_wkb_input_writes_native_geometry(self, geojson_input, temp_output_file):
        """WKB-encoded input (GeoJSON) with 1.1-geoarrow is converted to native GeoArrow."""
        import pyarrow.parquet as pq

        convert_to_geoparquet(
            geojson_input, temp_output_file, geoparquet_version="1.1-geoarrow", verbose=False
        )
        geo_meta = get_geo_metadata(temp_output_file)
        geom_col = geo_meta["primary_column"]
        geom_field = pq.read_schema(temp_output_file).field(geom_col)
        assert not _is_wkb_type(geom_field.type), (
            f"WKB input should become native GeoArrow, got {geom_field.type}"
        )

    def test_wkb_input_metadata_encoding_is_native(self, geojson_input, temp_output_file):
        """WKB-encoded polygon input records a native encoding (not 'WKB')."""
        convert_to_geoparquet(
            geojson_input, temp_output_file, geoparquet_version="1.1-geoarrow", verbose=False
        )
        geo_meta = get_geo_metadata(temp_output_file)
        geom_col = geo_meta["primary_column"]
        assert geo_meta["columns"][geom_col]["encoding"] == "polygon"

    @pytest.mark.parametrize(
        "geom_type",
        ["point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon"],
    )
    def test_all_native_types_preserve_encoding(self, geom_type, test_data_dir, tmp_path):
        """All 6 native geometry types must preserve their encoding through 1.1-geoarrow."""
        import pyarrow.parquet as pq

        input_file = str(test_data_dir / f"data-{geom_type}-encoding_native.parquet")
        output_file = str(tmp_path / f"output_{geom_type}.parquet")

        convert_to_geoparquet(
            input_file, output_file, geoparquet_version="1.1-geoarrow", verbose=False
        )

        schema = pq.read_schema(output_file)
        geom_field = schema.field("geometry")
        assert not _is_wkb_type(geom_field.type), (
            f"{geom_type}: geometry should remain native, not WKB blob"
        )

        geo_meta = get_geo_metadata(output_file)
        encoding = geo_meta["columns"]["geometry"]["encoding"]
        assert encoding == geom_type, (
            f"Expected '{geom_type}' encoding in metadata, got '{encoding}'"
        )

    def test_wkb_query_via_arrow_streaming_produces_native(self, geojson_input, temp_output_file):
        """arrow-streaming + 1.1-geoarrow must emit nested GeoArrow, not WKB binary."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.convert import convert_to_geoparquet

        # buildings_test.geojson is polygons
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            geoparquet_version="1.1-geoarrow",
            verbose=False,
        )
        geo_meta = get_geo_metadata(temp_output_file)
        geom_col = geo_meta["primary_column"]
        schema = pq.read_schema(temp_output_file)
        assert not _is_wkb_type(schema.field(geom_col).type)
        assert geo_meta["columns"][geom_col]["encoding"] == "polygon"

    def test_wkb_parquet_input_converts_to_native(self, test_data_dir, tmp_path):
        """A WKB-encoded GeoParquet input must become native GeoArrow under 1.1-geoarrow."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = str(test_data_dir / "buildings_test.parquet")  # WKB-encoded
        output_file = str(tmp_path / "out.parquet")
        convert_to_geoparquet(
            input_file, output_file, geoparquet_version="1.1-geoarrow", verbose=False
        )

        geo_meta = get_geo_metadata(output_file)
        geom_col = geo_meta["primary_column"]
        assert geo_meta["columns"][geom_col]["encoding"] != "WKB"
        assert not _is_wkb_type(pq.read_schema(output_file).field(geom_col).type)

    @pytest.mark.parametrize(
        "fixture_name",
        [
            "buildings_test.geojson",
            "buildings_test.shp",
            "buildings_test.gpkg",
            "buildings_test.parquet",
        ],
    )
    def test_all_input_formats_produce_native_encoding(self, fixture_name, test_data_dir, tmp_path):
        """Every input format must yield native GeoArrow encoding + valid GeoParquet under 1.1-geoarrow."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.common import check_bbox_structure
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = str(test_data_dir / fixture_name)
        output_file = str(tmp_path / "out.parquet")
        convert_to_geoparquet(
            input_file, output_file, geoparquet_version="1.1-geoarrow", verbose=False
        )

        geo_meta = get_geo_metadata(output_file)
        geom_col = geo_meta["primary_column"]
        encoding = geo_meta["columns"][geom_col]["encoding"]
        # all buildings_test fixtures contain only Polygon geometries (verified)
        assert encoding == "polygon", f"{fixture_name}: expected polygon, got {encoding}"
        assert not _is_wkb_type(pq.read_schema(output_file).field(geom_col).type)
        # bbox column must be skipped for 1.1-geoarrow
        assert not check_bbox_structure(output_file)["has_bbox_column"]
        assert get_geoparquet_version(output_file) == "1.1.0"

    def test_mixed_geometry_falls_back_to_wkb(self, test_data_dir, tmp_path):
        """A column mixing incompatible geometry types (Point + Polygon) stays WKB under 1.1-geoarrow."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from geoparquet_io.core.convert import convert_to_geoparquet

        # mixed_geometries.csv has a 'geometry' WKT column with Point and Polygon rows
        input_file = str(test_data_dir / "mixed_geometries.csv")
        output_file = str(tmp_path / "out.parquet")
        convert_to_geoparquet(
            input_file, output_file, geoparquet_version="1.1-geoarrow", verbose=False
        )

        geo_meta = get_geo_metadata(output_file)
        geom_col = geo_meta["primary_column"]
        assert geo_meta["columns"][geom_col]["encoding"] == "WKB"

        # Verify the physical Arrow column type is binary/WKB (not native nested GeoArrow)
        schema = pq.read_schema(output_file)
        geom_type = schema.field(geom_col).type
        assert pa.types.is_binary(geom_type) or pa.types.is_large_binary(geom_type), (
            f"mixed geometry should stay binary WKB, got {geom_type}"
        )

    def test_native_output_is_valid_and_readable(self, geojson_input, temp_output_file):
        """1.1-geoarrow native output passes validation and round-trips through geopandas."""
        import geopandas as gpd

        from geoparquet_io.core.convert import convert_to_geoparquet

        convert_to_geoparquet(
            geojson_input, temp_output_file, geoparquet_version="1.1-geoarrow", verbose=False
        )
        gdf = gpd.read_parquet(temp_output_file)
        assert len(gdf) > 0
        assert gdf.geometry.notna().all()

    def test_python_api_write_produces_native_encoding(self, geojson_input, temp_output_file):
        """The public Python API (Table.write) must produce native GeoArrow, not WKB.

        Drives the Table.write path (gpio.convert(...).write(...)), which is a
        different code path from convert_to_geoparquet and was previously emitting WKB.
        """
        import json

        import pyarrow.parquet as pq

        import geoparquet_io as gpio

        table = gpio.convert(geojson_input)
        table.write(temp_output_file, geoparquet_version="1.1-geoarrow")

        schema = pq.read_schema(temp_output_file)
        geo_meta = json.loads(schema.metadata[b"geo"])
        geom_col = geo_meta["primary_column"]
        assert geo_meta["columns"][geom_col]["encoding"] == "polygon"
        assert not _is_wkb_type(schema.field(geom_col).type)

    def test_3d_input_preserves_z(self, tmp_path):
        """3D (Point Z) WKB input must keep its Z ordinate under 1.1-geoarrow.

        Regression for silent Z/M data loss: the native target type was always 2D,
        so a forced conversion dropped the Z. Either Z survives in a native XYZ type
        or the column falls back to WKB — never a silent 2D coercion.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq
        import shapely.wkb
        from shapely.geometry import Point

        import geoparquet_io as gpio

        arr = pa.array(
            [shapely.wkb.dumps(Point(1, 2, 3)), shapely.wkb.dumps(Point(4, 5, 6))],
            type=pa.binary(),
        )
        table = gpio.Table(pa.table({"geometry": arr}), geometry_column="geometry")

        output_file = str(tmp_path / "out_3d.parquet")
        table.write(output_file, geoparquet_version="1.1-geoarrow")

        schema = pq.read_schema(output_file)
        geom_type = schema.field("geometry").type
        if _is_wkb_type(geom_type):
            # WKB fallback is acceptable — Z is preserved verbatim in the WKB bytes.
            return

        # Native encoding must carry the Z ordinate (XYZ storage with a z field).
        import geoarrow.pyarrow as ga

        col = pq.read_table(output_file).column("geometry").combine_chunks()
        wkt = ga.format_wkt(ga.as_geoarrow(col))[0].as_py()
        assert "POINT Z" in wkt, f"Z ordinate dropped, got {wkt}"

    def test_python_api_write_preserves_crs(self, tmp_path):
        """A non-default CRS must survive Table.write(1.1-geoarrow) (no silent CRS84).

        Regression for dropped CRS: write_from_table was never given self.crs, so the
        native type and geo metadata both lost a non-default CRS.
        """
        import json

        import pyarrow as pa
        import shapely.wkb
        from pyproj import CRS
        from shapely.geometry import Point

        import geoparquet_io as gpio

        projjson = CRS.from_epsg(3857).to_json_dict()
        arr = pa.array(
            [shapely.wkb.dumps(Point(1, 2)), shapely.wkb.dumps(Point(3, 4))],
            type=pa.binary(),
        )
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                    "crs": projjson,
                }
            },
        }
        tbl = pa.table({"geometry": arr}).replace_schema_metadata(
            {b"geo": json.dumps(geo).encode("utf-8")}
        )
        table = gpio.Table(tbl, geometry_column="geometry")

        output_file = str(tmp_path / "out_crs.parquet")
        table.write(output_file, geoparquet_version="1.1-geoarrow")

        geo_out = get_geo_metadata(output_file)
        crs_out = geo_out["columns"]["geometry"].get("crs")
        assert crs_out is not None, "non-default CRS was dropped"
        assert crs_out.get("id", {}).get("code") == 3857


class TestWriteGeoParquetTableParquetGeoOnly:
    """write_geoparquet_table must honor an explicit parquet-geo-only request (#687).

    The table writer converts the geometry column to a native Parquet GEOMETRY
    logical type for parquet-geo-only, so any ``geo`` key carried in from the
    input declares a version whose spec forbids native geo types. The other
    write paths omit the key entirely; this one must too.
    """

    @staticmethod
    def _table_with_geo(version):
        """Build an in-memory table carrying a ``geo`` key of the given version."""
        import json

        import pyarrow as pa
        import shapely.wkb
        from shapely.geometry import Point

        geo = {
            "version": version,
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
        }
        tbl = pa.table(
            {
                "id": pa.array([1, 2]),
                "geometry": pa.array(
                    [shapely.wkb.dumps(Point(1, 2)), shapely.wkb.dumps(Point(3, 4))],
                    type=pa.binary(),
                ),
            }
        )
        return tbl.replace_schema_metadata({b"geo": json.dumps(geo).encode("utf-8")})

    @pytest.mark.parametrize("input_version", ["1.0.0", "1.1.0", "2.0.0"])
    def test_pgo_strips_carried_geo_key(self, input_version, tmp_path):
        """An explicit parquet-geo-only request must drop the input's geo key."""
        from geoparquet_io.core.common import write_geoparquet_table

        output_file = str(tmp_path / f"pgo_{input_version}.parquet")
        write_geoparquet_table(
            self._table_with_geo(input_version),
            output_file,
            geoparquet_version="parquet-geo-only",
        )

        assert not has_geoparquet_metadata(output_file), (
            f"geo key from input version {input_version} survived a parquet-geo-only write"
        )
        assert has_native_geo_types(output_file)

    def test_pgo_on_plain_input_unchanged(self, tmp_path):
        """parquet-geo-only on an input without geo metadata still writes no geo key."""
        import pyarrow as pa
        import shapely.wkb
        from shapely.geometry import Point

        from geoparquet_io.core.common import write_geoparquet_table

        tbl = pa.table(
            {
                "id": pa.array([1]),
                "geometry": pa.array([shapely.wkb.dumps(Point(5, 6))], type=pa.binary()),
            }
        )
        output_file = str(tmp_path / "pgo_plain.parquet")
        write_geoparquet_table(tbl, output_file, geoparquet_version="parquet-geo-only")

        assert not has_geoparquet_metadata(output_file)
        assert has_native_geo_types(output_file)

    def test_pgo_preserves_non_geo_kv_metadata(self, tmp_path):
        """Stripping the geo key must not take unrelated KV metadata with it."""
        import json

        import pyarrow.parquet as pq

        from geoparquet_io.core.common import write_geoparquet_table

        tbl = self._table_with_geo("1.1.0")
        metadata = dict(tbl.schema.metadata or {})
        metadata[b"vecorel"] = json.dumps({"collection": "test"}).encode("utf-8")
        tbl = tbl.replace_schema_metadata(metadata)

        output_file = str(tmp_path / "pgo_kv.parquet")
        write_geoparquet_table(
            tbl, output_file, geoparquet_version="parquet-geo-only", verbose=True
        )

        out_metadata = pq.ParquetFile(output_file).schema_arrow.metadata or {}
        assert b"geo" not in out_metadata
        assert b"vecorel" in out_metadata

    def test_pgo_validates_against_target_version_oracle(self, tmp_path):
        """The real validator, told to expect pgo, must find no failures."""
        from geoparquet_io.core.common import write_geoparquet_table
        from geoparquet_io.core.validate import validate_geoparquet

        output_file = str(tmp_path / "pgo_oracle.parquet")
        write_geoparquet_table(
            self._table_with_geo("1.0.0"),
            output_file,
            geoparquet_version="parquet-geo-only",
        )

        result = validate_geoparquet(output_file, target_version="parquet-geo-only")
        failed = [c.name for c in result.checks if c.status == CheckStatus.FAILED]
        assert not failed, f"validator failures: {failed}"
        assert result.detected_version == "parquet-geo-only"

    def test_pgo_drops_stale_arrow_schema_descriptor(self, tmp_path):
        """A carried ARROW:schema/pandas describes the input, not the output.

        pyarrow writes such a carried blob through verbatim rather than
        replacing it with a fresh one, so the output would otherwise ship a
        serialized schema naming a column the file does not have. Read-back
        typing must still be the native geoarrow extension type.
        """
        import geoarrow.pyarrow  # noqa: F401  (registers the extension types)
        import pyarrow as pa
        import pyarrow.parquet as pq
        import shapely.wkb
        from shapely.geometry import Point

        from geoparquet_io.core.common import write_geoparquet_table

        # Build a stale descriptor from a table with an extra "ghost" column.
        stale_src = tmp_path / "stale_src.parquet"
        pq.write_table(
            pa.table(
                {
                    "id": pa.array([1]),
                    "ghost": pa.array(["gone"]),
                    "geometry": pa.array([shapely.wkb.dumps(Point(1, 2))], type=pa.binary()),
                }
            ),
            stale_src,
        )
        stale_blob = pq.ParquetFile(stale_src).metadata.metadata[b"ARROW:schema"]

        tbl = self._table_with_geo("1.1.0")
        metadata = dict(tbl.schema.metadata or {})
        metadata[b"ARROW:schema"] = stale_blob
        metadata[b"pandas"] = b'{"index_columns": ["ghost"]}'
        tbl = tbl.replace_schema_metadata(metadata)

        output_file = str(tmp_path / "pgo_stale.parquet")
        write_geoparquet_table(tbl, output_file, geoparquet_version="parquet-geo-only")

        raw_kv = pq.ParquetFile(output_file).metadata.metadata or {}
        assert raw_kv.get(b"ARROW:schema") != stale_blob, (
            "stale ARROW:schema descriptor was carried into the output verbatim"
        )
        assert b"pandas" not in raw_kv or b"ghost" not in raw_kv[b"pandas"]

        read_back = pq.read_table(output_file)
        assert read_back.column_names == ["id", "geometry"]
        assert (
            getattr(read_back.schema.field("geometry").type, "extension_name", None)
            == "geoarrow.wkb"
        )

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0"])
    def test_other_versions_still_write_geo_key(self, version, tmp_path):
        """Non-pgo versions are unaffected: the geo key is written as before."""
        from geoparquet_io.core.common import write_geoparquet_table

        output_file = str(tmp_path / f"v{version}.parquet")
        write_geoparquet_table(
            self._table_with_geo("1.0.0"), output_file, geoparquet_version=version
        )

        assert has_geoparquet_metadata(output_file)
        assert get_geoparquet_version(output_file).startswith(version)


class TestParquetGeoOnlyWithoutGeometryColumn:
    """parquet-geo-only must drop the carried geo key with no geometry column too (#701).

    #687 fixed the geometry-present case. The strip sat inside geometry-present
    guards, so a table whose geometry column was dropped by a projection still
    wrote the carried `geo` key -- and that key names a `primary_column` the file
    does not contain.
    """

    @staticmethod
    def _attributes_only_table_with_geo(extra_keys=None):
        """Attributes only: the geometry column is gone, but the geo key rode along."""
        import json

        import pyarrow as pa

        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
        }
        metadata = {b"geo": json.dumps(geo).encode("utf-8")}
        metadata.update(extra_keys or {})
        return pa.table({"id": pa.array([1, 2])}).replace_schema_metadata(metadata)

    def test_pgo_strips_geo_key_when_geometry_column_is_absent(self, tmp_path):
        """The reproducer from #701."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.common import write_geoparquet_table

        output_file = str(tmp_path / "pgo_no_geom.parquet")
        write_geoparquet_table(
            self._attributes_only_table_with_geo(),
            output_file,
            geoparquet_version="parquet-geo-only",
        )

        assert pq.read_table(output_file).column_names == ["id"]
        out_metadata = pq.ParquetFile(output_file).schema_arrow.metadata or {}
        assert b"geo" not in out_metadata, (
            "parquet-geo-only was explicitly requested but the carried geo key was written"
        )

    def test_pgo_without_geometry_keeps_non_geo_kv_metadata(self, tmp_path):
        """Stripping takes the carried schema descriptors, and nothing else.

        `_strip_geo_metadata_key` drops the whole
        `_CARRIED_SCHEMA_METADATA_KEYS` set -- `geo`, `ARROW:schema` and
        `pandas` -- not just `geo`. That is deliberate and matches the
        geometry-present case: all three describe the *input's* schema, and a
        carried `pandas` block names columns and dtypes the output no longer
        has. Pinned here so the loss of a round-tripped `pandas` key is a
        stated behaviour rather than a surprise. Unrelated sidecar payloads
        (`fiboa`, `vecorel`, STAC fragments) must survive.
        """
        import base64
        import json

        import pyarrow as pa
        import pyarrow.parquet as pq

        from geoparquet_io.core.common import write_geoparquet_table

        # A real serialized schema for a shape the output does not have. It has
        # to be well-formed: pyarrow reconstructs `schema_arrow` from whatever
        # `ARROW:schema` holds, so garbage here fails the read rather than the
        # assertion, and the file-level KV block is read directly below.
        stale_arrow_schema = base64.b64encode(
            pa.schema([pa.field("stale_input_col", pa.float64())]).serialize()
        )
        table = self._attributes_only_table_with_geo(
            {
                b"fiboa": json.dumps({"schemas": ["example"]}).encode("utf-8"),
                b"pandas": json.dumps({"index_columns": ["geometry"]}).encode("utf-8"),
                b"ARROW:schema": bytes(stale_arrow_schema),
            }
        )
        output_file = str(tmp_path / "pgo_no_geom_kv.parquet")
        write_geoparquet_table(table, output_file, geoparquet_version="parquet-geo-only")

        out_metadata = pq.ParquetFile(output_file).metadata.metadata or {}
        assert b"geo" not in out_metadata
        assert b"fiboa" in out_metadata
        assert b"pandas" not in out_metadata, (
            "a carried pandas block describes the input's columns, so it is dropped"
        )
        # pyarrow regenerates ARROW:schema from the output schema, so the key is
        # present -- what must not survive is the *input's* value.
        assert out_metadata.get(b"ARROW:schema") != bytes(stale_arrow_schema), (
            "the input's stale ARROW:schema must not ride through to the output"
        )

    def test_pgo_helper_drops_every_carried_schema_key(self):
        """The in-memory view of the same rule, free of writer regeneration.

        `pq.write_table` regenerates `ARROW:schema` from the output schema, so
        the on-disk assertion above can only show the stale value is gone. At
        the helper boundary all three keys are simply absent.
        """
        import json

        from geoparquet_io.core.common import _apply_geoparquet_metadata

        result = _apply_geoparquet_metadata(
            self._attributes_only_table_with_geo(
                {
                    b"fiboa": json.dumps({"schemas": ["example"]}).encode("utf-8"),
                    b"pandas": json.dumps({"index_columns": ["geometry"]}).encode("utf-8"),
                    b"ARROW:schema": b"stale-serialized-input-schema",
                }
            ),
            geometry_column="geometry",
            geoparquet_version="parquet-geo-only",
        )

        metadata = result.schema.metadata or {}
        assert set(metadata) == {b"fiboa"}

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0"])
    def test_other_versions_without_geometry_are_unchanged(self, version, tmp_path):
        """Only parquet-geo-only strips; the other explicit versions still no-op.

        The geometry-absent branch is a no-op for them, and this pins that the
        new strip did not widen into one.
        """
        import pyarrow.parquet as pq

        from geoparquet_io.core.common import write_geoparquet_table

        output_file = str(tmp_path / f"nogeom_{version}.parquet")
        write_geoparquet_table(
            self._attributes_only_table_with_geo(),
            output_file,
            geoparquet_version=version,
        )

        out_metadata = pq.ParquetFile(output_file).schema_arrow.metadata or {}
        assert b"geo" in out_metadata

    def test_verbose_reports_the_drop(self, caplog):
        """The verbose path names what it dropped and why."""
        import logging

        from geoparquet_io.core.common import _apply_geoparquet_metadata

        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            _apply_geoparquet_metadata(
                self._attributes_only_table_with_geo(),
                geometry_column="geometry",
                geoparquet_version="parquet-geo-only",
                verbose=True,
            )

        # Assert on text only the new branch emits. The pre-existing
        # "not found in table, skipping metadata" line and the earlier
        # "Applying GeoParquet metadata for version: parquet-geo-only" line
        # are both reachable without the fix, so matching those would make
        # this test pass against the unfixed code.
        assert "dropping carried geo metadata for parquet-geo-only" in caplog.text
        assert "parquet-geo-only: dropped the input's geo metadata key(s)" in caplog.text

    def test_apply_metadata_helper_strips_directly(self):
        """The helper itself honors the request, so every caller inherits the fix."""
        from geoparquet_io.core.common import _apply_geoparquet_metadata

        result = _apply_geoparquet_metadata(
            self._attributes_only_table_with_geo(),
            geometry_column="geometry",
            geoparquet_version="parquet-geo-only",
        )

        assert b"geo" not in (result.schema.metadata or {})


class TestCarriedSchemaMetadataKeysHasOneDefinition:
    """The two write paths must exclude the same keys, structurally.

    `_strip_geo_metadata_key` (parquet-geo-only, sees `bytes` keys off a
    pyarrow schema) and `write_parquet_with_metadata`'s preserved-keys loop
    (sees them decoded) were two hand-maintained literals of the same set.
    A key added to one and not the other is a silent metadata leak: the
    descriptor rides into the output naming the input's columns and CRS.
    """

    def test_bytes_form_is_derived_from_the_string_form(self):
        from geoparquet_io.core.common import (
            _CARRIED_SCHEMA_METADATA_KEYS,
            _CARRIED_SCHEMA_METADATA_KEYS_BYTES,
        )

        assert _CARRIED_SCHEMA_METADATA_KEYS_BYTES == {
            key.encode("utf-8") for key in _CARRIED_SCHEMA_METADATA_KEYS
        }

    def test_no_second_literal_copy_of_the_set(self):
        """Guard against the literal being reintroduced next to the constant.

        Scanned across all of ``geoparquet_io/core`` rather than one module: the
        constant and its two consumers no longer share an address, and a guard
        that watched only the module the constant happens to live in would miss
        a copy pasted into the other consumer -- which is the drift.
        """
        import re
        from pathlib import Path

        import geoparquet_io.core

        core_dir = Path(geoparquet_io.core.__file__).parent
        # The keys spelled out as a literal tuple/set/list. Exactly one such
        # spelling is legitimate -- the constant's own definition; a second is
        # the duplicate this guards against.
        pattern = re.compile(r"""[\(\[\{]\s*["']geo["']\s*,\s*["']ARROW:schema["']""")
        sites = [
            f"{path.relative_to(core_dir)}"
            for path in sorted(core_dir.rglob("*.py"))
            for _ in pattern.findall(path.read_text(encoding="utf-8"))
        ]
        assert len(sites) == 1, (
            f"the carried-metadata key set is spelled out {len(sites)} times ({sites}); it "
            "must appear only in the _CARRIED_SCHEMA_METADATA_KEYS definition, so the two "
            "write paths cannot drift"
        )

    def test_both_write_paths_reference_the_constant(self):
        """Both paths must reach the one constant — directly or through the helper.

        The query path no longer names the constant itself: it filters through
        ``extract_preserved_kv_metadata``, which is where the comparison now
        lives. Following that indirection keeps the guard honest; asserting the
        literal name in ``write_parquet_with_metadata`` would only have been
        satisfiable by inlining the loop back into it.
        """
        import inspect

        from geoparquet_io.core.common import (
            _strip_geo_metadata_key,
            extract_preserved_kv_metadata,
            write_parquet_with_metadata,
        )

        # parquet-geo-only path: compares bytes keys straight off a pyarrow schema.
        assert "_CARRIED_SCHEMA_METADATA_KEYS_BYTES" in inspect.getsource(_strip_geo_metadata_key)
        # Query path: delegates to the helper, which owns the comparison.
        assert "extract_preserved_kv_metadata" in inspect.getsource(write_parquet_with_metadata)
        assert "_CARRIED_SCHEMA_METADATA_KEYS" in inspect.getsource(extract_preserved_kv_metadata)
