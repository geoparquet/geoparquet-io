"""Additional tests for check_fixes helper functions."""

import contextlib

from geoparquet_io.core.check_fixes import (
    _apply_bbox_column_fix,
    _apply_bbox_metadata_fix,
)
from tests.fix_output_oracle import (
    PLACES_V11_FIXTURE_BASELINE,
    assert_buildings_output_is_sound,
    assert_places_output_is_sound,
)


class TestApplyBboxColumnFix:
    """Tests for _apply_bbox_column_fix function."""

    def test_no_fix_needed(self):
        """Test when no bbox fix is needed."""
        bbox_result = {"needs_bbox_removal": False, "needs_bbox_column": False}
        temp_files = contextlib.ExitStack()

        current_file, fixes = _apply_bbox_column_fix(
            bbox_result, "/some/file.parquet", temp_files, False, None
        )

        assert current_file == "/some/file.parquet"
        assert fixes == []


class TestApplyBboxMetadataFix:
    """Tests for _apply_bbox_metadata_fix function."""

    def test_skip_for_v2_files(self):
        """Test that bbox metadata is skipped for v2/parquet-geo-only files."""
        bbox_result = {"needs_bbox_removal": True}
        temp_files = contextlib.ExitStack()

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
        temp_files = contextlib.ExitStack()

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
        temp_files = contextlib.ExitStack()

        current_file, fixes = _apply_spatial_ordering_fix(
            check_results, "/some/file.parquet", temp_files, False, None
        )

        assert current_file == "/some/file.parquet"
        assert fixes == []

    def test_missing_spatial_result(self):
        """Test when spatial result is missing."""
        from geoparquet_io.core.check_fixes import _apply_spatial_ordering_fix

        check_results = {}
        temp_files = contextlib.ExitStack()

        current_file, fixes = _apply_spatial_ordering_fix(
            check_results, "/some/file.parquet", temp_files, False, None
        )

        assert current_file == "/some/file.parquet"
        assert fixes == []


class TestApplyCompressionFix:
    """Tests for _apply_compression_fix function."""

    def test_no_compression_fix_needed(self, tmp_path):
        """Test when no compression or row group fix is needed.

        Here the file to place *is* the user's input -- every earlier step
        declined to rewrite anything -- so it must be copied to the output and
        left where it was. Moving it is what deleted files under
        ``--fix-output`` (#1036).
        """

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
        assert input_file.exists(), "the user's input was moved to the output path"
        assert input_file.read_text() == "test"


class TestFixBboxAll:
    """Tests for fix_bbox_all function.

    ``fix_bbox_all`` is the entry point ``check bbox --fix`` uses for a 1.x file,
    so every output below is also handed the shared oracle (WP-1 of #1018):
    ``check spec`` clean, the rows preserved, the CRS from both carriers read
    separately, and the covering resolved against the real schema.
    """

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
        assert_buildings_output_is_sound(output_file)

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
        assert_places_output_is_sound(
            output_file,
            version="1.1",
            covering=True,
            known_spec_failures=PLACES_V11_FIXTURE_BASELINE,
        )

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
        assert_buildings_output_is_sound(output_file)

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
        assert_buildings_output_is_sound(output_file)
