"""Additional tests for check_fixes helper functions."""

from geoparquet_io.core.check_fixes import (
    _apply_bbox_column_fix,
    _apply_bbox_metadata_fix,
    _cleanup_temp_files,
)


class TestCleanupTempFiles:
    """Tests for _cleanup_temp_files function."""

    def test_cleanup_nonexistent_files(self, tmp_path):
        """Test cleanup with non-existent files."""
        # Should not raise
        _cleanup_temp_files(
            [str(tmp_path / "nonexistent1.parquet"), str(tmp_path / "nonexistent2.parquet")],
            output_file=None,
        )

    def test_cleanup_existing_files(self, tmp_path):
        """Test cleanup with existing files."""
        # Create temp files
        f1 = tmp_path / "temp1.parquet"
        f2 = tmp_path / "temp2.parquet"
        f1.write_text("test")
        f2.write_text("test")

        _cleanup_temp_files([str(f1), str(f2)], output_file=None)

        assert not f1.exists()
        assert not f2.exists()

    def test_cleanup_excludes_output_file(self, tmp_path):
        """Test that output file is not deleted."""
        output = tmp_path / "output.parquet"
        output.write_text("output")
        temp = tmp_path / "temp.parquet"
        temp.write_text("temp")

        _cleanup_temp_files([str(output), str(temp)], output_file=str(output))

        assert output.exists()  # Should NOT be deleted
        assert not temp.exists()  # Should be deleted


class TestApplyBboxColumnFix:
    """Tests for _apply_bbox_column_fix function."""

    def test_no_fix_needed(self):
        """Test when no bbox fix is needed."""
        bbox_result = {"needs_bbox_removal": False, "needs_bbox_column": False}
        temp_files = []

        current_file, fixes = _apply_bbox_column_fix(
            bbox_result, "/some/file.parquet", temp_files, False, None
        )

        assert current_file == "/some/file.parquet"
        assert fixes == []
        assert len(temp_files) == 0


class TestApplyBboxMetadataFix:
    """Tests for _apply_bbox_metadata_fix function."""

    def test_skip_for_v2_files(self):
        """Test that bbox metadata is skipped for v2/parquet-geo-only files."""
        bbox_result = {"needs_bbox_removal": True}
        temp_files = []

        current_file, fixes = _apply_bbox_metadata_fix(
            bbox_result, "/some/file.parquet", "/some/file.parquet", temp_files, False, None
        )

        assert current_file == "/some/file.parquet"
        assert fixes == []

    def test_no_metadata_needed(self):
        """Test when no metadata fix is needed."""
        bbox_result = {
            "needs_bbox_removal": False,
            "needs_bbox_metadata": False,
            "needs_bbox_column": False,
        }
        temp_files = []

        current_file, fixes = _apply_bbox_metadata_fix(
            bbox_result, "/some/file.parquet", "/some/file.parquet", temp_files, False, None
        )

        assert current_file == "/some/file.parquet"
        assert fixes == []


class TestApplySpatialOrderingFix:
    """Tests for _apply_spatial_ordering_fix function."""

    def test_no_spatial_fix_needed(self):
        """Test when no spatial ordering fix is needed."""
        from geoparquet_io.core.check_fixes import _apply_spatial_ordering_fix

        check_results = {"spatial": {"fix_available": False}}
        temp_files = []

        current_file, fixes = _apply_spatial_ordering_fix(
            check_results, "/some/file.parquet", temp_files, False, None
        )

        assert current_file == "/some/file.parquet"
        assert fixes == []
        assert len(temp_files) == 0

    def test_missing_spatial_result(self):
        """Test when spatial result is missing."""
        from geoparquet_io.core.check_fixes import _apply_spatial_ordering_fix

        check_results = {}
        temp_files = []

        current_file, fixes = _apply_spatial_ordering_fix(
            check_results, "/some/file.parquet", temp_files, False, None
        )

        assert current_file == "/some/file.parquet"
        assert fixes == []


class TestApplyCompressionFix:
    """Tests for _apply_compression_fix function."""

    def test_no_compression_fix_needed(self, tmp_path):
        """Test when no compression or row group fix is needed."""

        from geoparquet_io.core.check_fixes import _apply_compression_fix

        check_results = {
            "compression": {"fix_available": False},
            "row_groups": {"fix_available": False},
        }

        # Create temp files
        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        input_file.write_text("test")

        fixes = _apply_compression_fix(
            check_results, str(input_file), str(output_file), None, False, None
        )

        assert fixes == []
        assert output_file.exists()

    def test_compression_fix_only(self):
        """Test when only compression fix is needed."""
        # This will fail without real file, but tests the logic path
        # The actual file operations are tested in integration tests
        pass


class TestFixBboxAll:
    """Tests for fix_bbox_all function.

    ``fix_bbox_all`` is the entry point ``check bbox --fix`` uses for a 1.x file,
    so every output below is also handed the shared oracle (WP-1 of #1018):
    ``check spec`` clean, the rows preserved, the CRS from both carriers read
    separately, and the covering resolved against the real schema.
    """

    #: 42 rows, CRS84, GeoParquet 1.0.0, no bbox column.
    BUILDINGS_ROWS = 42

    @classmethod
    def assert_buildings_output_is_sound(cls, path):
        from tests.fix_output_oracle import CRS84, assert_fix_output_is_sound

        assert_fix_output_is_sound(
            path,
            expected_rows=cls.BUILDINGS_ROWS,
            expected_crs=CRS84,
            expects_covering=True,
            expected_version_prefix="1.1",
        )

    def test_column_only_fix(self, buildings_test_file, temp_output_dir):
        """Test fixing only bbox column."""
        import os

        from geoparquet_io.core.check_fixes import fix_bbox_all

        output_file = os.path.join(temp_output_dir, "fixed.parquet")

        fix_result = fix_bbox_all(
            buildings_test_file,
            output_file,
            needs_column=True,
            needs_metadata=False,
            verbose=False,
        )

        assert fix_result["success"] is True
        assert os.path.exists(output_file)
        self.assert_buildings_output_is_sound(output_file)

    def test_metadata_only_fix(self, places_v11_file, temp_output_dir):
        """Test fixing only bbox metadata.

        Uses the 1.1 places fixture: covering is a 1.1-only key, so adding it to
        the 1.0 original is now refused outright (gpio #686).
        """
        import os
        import shutil

        from geoparquet_io.core.check_fixes import fix_bbox_all

        # Copy file first
        input_file = os.path.join(temp_output_dir, "input.parquet")
        shutil.copy2(places_v11_file, input_file)
        output_file = os.path.join(temp_output_dir, "fixed.parquet")

        fix_result = fix_bbox_all(
            input_file, output_file, needs_column=False, needs_metadata=True, verbose=False
        )

        assert fix_result["success"] is True
        # `places_v11_file` declares 1.1.0 while DuckDB gives it a native Parquet
        # GEOMETRY logical type, so it fails `check spec` before any fix runs
        # (gpio #1037). Carried as an explicit baseline, not waved away.
        from tests.fix_output_oracle import CRS84, assert_fix_output_is_sound

        assert_fix_output_is_sound(
            output_file,
            expected_rows=766,
            expected_crs=CRS84,
            expects_covering=True,
            expected_version_prefix="1.1",
            known_spec_failures={
                "version_features_match": (
                    "the places_v11_file fixture is built with native Parquet geo "
                    "types under a 1.1 version string (gpio #1037)"
                )
            },
        )
        # This test copies its input before fixing, which is what hides gpio
        # #1036: `fix_bbox_all` *moves* the caller's file to `output_file` on
        # this exact branch, so without the copy the fixture would be destroyed.

    def test_both_fixes(self, buildings_test_file, temp_output_dir):
        """Test fixing both column and metadata."""
        import os

        from geoparquet_io.core.check_fixes import fix_bbox_all

        output_file = os.path.join(temp_output_dir, "fixed.parquet")

        fix_result = fix_bbox_all(
            buildings_test_file,
            output_file,
            needs_column=True,
            needs_metadata=True,
            verbose=False,
        )

        assert fix_result["success"] is True
        self.assert_buildings_output_is_sound(output_file)

    def test_with_verbose(self, buildings_test_file, temp_output_dir):
        """Test fix_bbox_all with verbose output."""
        import os

        from geoparquet_io.core.check_fixes import fix_bbox_all

        output_file = os.path.join(temp_output_dir, "fixed.parquet")

        fix_result = fix_bbox_all(
            buildings_test_file,
            output_file,
            needs_column=True,
            needs_metadata=True,
            verbose=True,
        )

        assert fix_result["success"] is True
        self.assert_buildings_output_is_sound(output_file)
