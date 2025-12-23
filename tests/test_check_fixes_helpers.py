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
