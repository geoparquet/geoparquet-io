"""Tests for core/check_fixes.py module."""

import os
import shutil

import pyarrow.parquet as pq

from geoparquet_io.core.check_fixes import (
    fix_bbox_column,
    fix_bbox_metadata,
    fix_compression,
)
from geoparquet_io.core.check_parquet_structure import (
    check_compression,
    check_metadata_and_bbox,
)


class TestFixCompression:
    """Tests for fix_compression function."""

    def test_fixes_snappy_to_zstd(self, places_test_file, temp_output_dir):
        """Test fixing SNAPPY compression to ZSTD."""
        # Create a file with SNAPPY compression
        snappy_file = os.path.join(temp_output_dir, "snappy.parquet")
        output_file = os.path.join(temp_output_dir, "fixed.parquet")

        table = pq.read_table(places_test_file)
        pq.write_table(table, snappy_file, compression="SNAPPY")

        # Verify it has SNAPPY compression
        result = check_compression(snappy_file, verbose=False, return_results=True)
        assert result["current_compression"] == "SNAPPY"

        # Apply fix
        fix_result = fix_compression(snappy_file, output_file, verbose=False)

        assert fix_result["success"] is True
        assert "ZSTD" in fix_result["fix_applied"]
        assert os.path.exists(output_file)

        # Verify compression is now ZSTD
        final_result = check_compression(output_file, verbose=False, return_results=True)
        assert final_result["current_compression"] == "ZSTD"

    def test_fixes_with_verbose(self, places_test_file, temp_output_dir):
        """Test fix_compression with verbose flag."""
        snappy_file = os.path.join(temp_output_dir, "snappy.parquet")
        output_file = os.path.join(temp_output_dir, "fixed.parquet")

        table = pq.read_table(places_test_file)
        pq.write_table(table, snappy_file, compression="SNAPPY")

        fix_result = fix_compression(snappy_file, output_file, verbose=True)
        assert fix_result["success"] is True


class TestFixBboxColumn:
    """Tests for fix_bbox_column function."""

    def test_adds_missing_bbox_column(self, places_test_file, temp_output_dir):
        """Test adding bbox column to file without one."""
        # Create file without bbox column
        no_bbox_file = os.path.join(temp_output_dir, "no_bbox.parquet")
        output_file = os.path.join(temp_output_dir, "fixed.parquet")

        table = pq.read_table(places_test_file)
        if "bbox" in table.column_names:
            table = table.drop(["bbox"])
        pq.write_table(table, no_bbox_file)

        # Verify no bbox column
        result = check_metadata_and_bbox(no_bbox_file, verbose=False, return_results=True)
        assert result["has_bbox_column"] is False

        # Apply fix
        fix_result = fix_bbox_column(no_bbox_file, output_file, verbose=False)

        assert fix_result["success"] is True
        assert os.path.exists(output_file)

        # Verify bbox column now exists
        final_result = check_metadata_and_bbox(output_file, verbose=False, return_results=True)
        assert final_result["has_bbox_column"] is True

    def test_with_verbose(self, buildings_test_file, temp_output_dir):
        """Test fix_bbox_column with verbose flag."""
        output_file = os.path.join(temp_output_dir, "fixed.parquet")

        # Buildings file doesn't have bbox
        fix_result = fix_bbox_column(buildings_test_file, output_file, verbose=True)
        assert fix_result["success"] is True


class TestFixBboxMetadata:
    """Tests for fix_bbox_metadata function."""

    def test_adds_bbox_metadata(self, places_test_file, temp_output_dir):
        """Test adding bbox metadata to file with bbox column."""
        test_file = os.path.join(temp_output_dir, "test.parquet")
        shutil.copy2(places_test_file, test_file)

        fix_result = fix_bbox_metadata(test_file, test_file, verbose=False)

        assert fix_result["success"] is True
        assert "bbox" in fix_result["fix_applied"].lower()

    def test_copies_file_if_output_different(self, places_test_file, temp_output_dir):
        """Test that file is copied when output differs from input."""
        output_file = os.path.join(temp_output_dir, "output.parquet")

        fix_result = fix_bbox_metadata(places_test_file, output_file, verbose=False)

        assert fix_result["success"] is True
        assert os.path.exists(output_file)

    def test_with_verbose(self, places_test_file, temp_output_dir):
        """Test fix_bbox_metadata with verbose flag."""
        test_file = os.path.join(temp_output_dir, "test.parquet")
        shutil.copy2(places_test_file, test_file)

        fix_result = fix_bbox_metadata(test_file, test_file, verbose=True)
        assert fix_result["success"] is True


class TestFixRowGroups:
    """Tests for fix_row_groups function."""

    def test_optimizes_row_groups(self, places_test_file, temp_output_dir):
        """Test row group optimization."""
        from geoparquet_io.core.check_fixes import fix_row_groups

        output_file = os.path.join(temp_output_dir, "optimized.parquet")

        fix_result = fix_row_groups(places_test_file, output_file, verbose=False)

        assert fix_result["success"] is True
        assert "Optimized row groups" in fix_result["fix_applied"]
        assert os.path.exists(output_file)

    def test_with_geoparquet_version(self, places_test_file, temp_output_dir):
        """Test row group optimization preserving version."""
        from geoparquet_io.core.check_fixes import fix_row_groups

        output_file = os.path.join(temp_output_dir, "optimized.parquet")

        fix_result = fix_row_groups(
            places_test_file, output_file, verbose=False, geoparquet_version="1.1"
        )

        assert fix_result["success"] is True

    def test_with_verbose(self, places_test_file, temp_output_dir):
        """Test row group optimization with verbose output."""
        from geoparquet_io.core.check_fixes import fix_row_groups

        output_file = os.path.join(temp_output_dir, "optimized.parquet")

        fix_result = fix_row_groups(places_test_file, output_file, verbose=True)

        assert fix_result["success"] is True


class TestFixBboxRemoval:
    """Tests for fix_bbox_removal function."""

    def test_removes_bbox_column(self, places_test_file, temp_output_dir):
        """Test removing bbox column from file."""
        from geoparquet_io.core.check_fixes import fix_bbox_removal

        output_file = os.path.join(temp_output_dir, "no_bbox.parquet")

        # First ensure test file has bbox column
        table = pq.read_table(places_test_file)
        if "bbox" not in table.column_names:
            # Skip if no bbox column
            return

        fix_result = fix_bbox_removal(
            places_test_file, output_file, bbox_column_name="bbox", verbose=False
        )

        assert fix_result["success"] is True
        assert "Removed bbox column" in fix_result["fix_applied"]

        # Verify column removed
        result_table = pq.read_table(output_file)
        assert "bbox" not in result_table.column_names

    def test_with_verbose(self, places_test_file, temp_output_dir):
        """Test bbox removal with verbose output."""
        from geoparquet_io.core.check_fixes import fix_bbox_removal

        output_file = os.path.join(temp_output_dir, "no_bbox.parquet")

        table = pq.read_table(places_test_file)
        if "bbox" not in table.column_names:
            return

        fix_result = fix_bbox_removal(
            places_test_file, output_file, bbox_column_name="bbox", verbose=True
        )

        assert fix_result["success"] is True


class TestGetGeoparquetVersionFromCheckResults:
    """Tests for get_geoparquet_version_from_check_results function."""

    def test_v2_detection(self):
        """Test detecting GeoParquet v2."""
        from geoparquet_io.core.check_fixes import get_geoparquet_version_from_check_results

        check_results = {"bbox": {"file_type": "geoparquet_v2", "version": "2.0.0"}}

        version = get_geoparquet_version_from_check_results(check_results)

        assert version == "2.0"

    def test_parquet_geo_only_detection(self):
        """Test detecting parquet-geo-only files."""
        from geoparquet_io.core.check_fixes import get_geoparquet_version_from_check_results

        check_results = {"bbox": {"file_type": "parquet_geo_only"}}

        version = get_geoparquet_version_from_check_results(check_results)

        assert version == "parquet-geo-only"

    def test_v1_0_detection(self):
        """Test detecting GeoParquet v1.0."""
        from geoparquet_io.core.check_fixes import get_geoparquet_version_from_check_results

        check_results = {"bbox": {"file_type": "geoparquet_v1", "version": "1.0.0"}}

        version = get_geoparquet_version_from_check_results(check_results)

        assert version == "1.0"

    def test_v1_1_detection(self):
        """Test detecting GeoParquet v1.1."""
        from geoparquet_io.core.check_fixes import get_geoparquet_version_from_check_results

        check_results = {"bbox": {"file_type": "geoparquet_v1", "version": "1.1.0"}}

        version = get_geoparquet_version_from_check_results(check_results)

        assert version == "1.1"

    def test_unknown_defaults_to_none(self):
        """Test unknown file type defaults to None."""
        from geoparquet_io.core.check_fixes import get_geoparquet_version_from_check_results

        check_results = {"bbox": {"file_type": "unknown"}}

        version = get_geoparquet_version_from_check_results(check_results)

        assert version is None

    def test_missing_bbox_result(self):
        """Test handling missing bbox result."""
        from geoparquet_io.core.check_fixes import get_geoparquet_version_from_check_results

        check_results = {}

        version = get_geoparquet_version_from_check_results(check_results)

        assert version is None
