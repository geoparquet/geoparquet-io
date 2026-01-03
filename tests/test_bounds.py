"""
Tests for get_dataset_bounds function.
"""

import logging

import pytest

from geoparquet_io.core.common import get_dataset_bounds


class TestGetDatasetBounds:
    """Test suite for get_dataset_bounds function."""

    def test_get_bounds_without_bbox_column(self, buildings_test_file):
        """Test getting bounds from file without bbox column."""
        bounds = get_dataset_bounds(buildings_test_file, verbose=False)

        assert bounds is not None
        assert len(bounds) == 4
        xmin, ymin, xmax, ymax = bounds

        # Check that bounds are reasonable
        assert isinstance(xmin, float)
        assert isinstance(ymin, float)
        assert isinstance(xmax, float)
        assert isinstance(ymax, float)
        assert xmin < xmax
        assert ymin < ymax

        # Buildings test file is in Germany, so roughly these coords
        assert 5 < xmin < 7  # Longitude
        assert 49 < ymin < 51  # Latitude
        assert 5 < xmax < 7
        assert 49 < ymax < 51

    def test_get_bounds_with_bbox_column(self, places_test_file):
        """Test getting bounds from file with bbox column."""
        bounds = get_dataset_bounds(places_test_file, verbose=False)

        assert bounds is not None
        assert len(bounds) == 4
        xmin, ymin, xmax, ymax = bounds

        # Check that bounds are reasonable
        assert isinstance(xmin, float)
        assert isinstance(ymin, float)
        assert isinstance(xmax, float)
        assert isinstance(ymax, float)
        assert xmin < xmax
        assert ymin < ymax

    def test_get_bounds_with_specific_geometry_column(self, buildings_test_file):
        """Test getting bounds with specified geometry column."""
        bounds = get_dataset_bounds(buildings_test_file, geometry_column="geometry", verbose=False)

        assert bounds is not None
        assert len(bounds) == 4

    def test_get_bounds_verbose_output(self, buildings_test_file, caplog):
        """Test verbose output when calculating bounds."""

        # Capture log output at DEBUG level to see verbose messages
        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            bounds = get_dataset_bounds(buildings_test_file, verbose=True)

        # Should show debug message about dataset bounds
        log_text = " ".join(record.message.lower() for record in caplog.records)
        assert "bbox column" in log_text or "dataset bounds" in log_text

        assert bounds is not None

    def test_get_bounds_with_created_bbox_column(self, buildings_test_file, temp_output_file):
        """Test getting bounds after adding bbox column."""
        from geoparquet_io.core.add_bbox_column import add_bbox_column

        # First add bbox column
        add_bbox_column(buildings_test_file, temp_output_file, verbose=False)

        # Now get bounds from file with bbox column (should be faster)
        bounds = get_dataset_bounds(temp_output_file, verbose=False)

        assert bounds is not None
        assert len(bounds) == 4

        # Bounds should be the same as original
        original_bounds = get_dataset_bounds(buildings_test_file, verbose=False)
        xmin1, ymin1, xmax1, ymax1 = original_bounds
        xmin2, ymin2, xmax2, ymax2 = bounds

        # Allow small floating point differences
        assert abs(xmin1 - xmin2) < 0.0001
        assert abs(ymin1 - ymin2) < 0.0001
        assert abs(xmax1 - xmax2) < 0.0001
        assert abs(ymax1 - ymax2) < 0.0001

    def test_get_bounds_nonexistent_file(self):
        """Test getting bounds from nonexistent file."""
        import click

        # Should raise an exception for nonexistent file
        with pytest.raises(click.BadParameter):
            get_dataset_bounds("nonexistent.parquet", verbose=False)

    def test_get_bounds_performance_difference(self, buildings_test_file, temp_output_file, caplog):
        """Test that bbox column actually helps performance (via warnings)."""
        from geoparquet_io.core.add_bbox_column import add_bbox_column

        # Get bounds without bbox column - should show warning
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            bounds1 = get_dataset_bounds(buildings_test_file, verbose=False)

        # Should have warning about slow calculation
        log_text1 = " ".join(record.message.lower() for record in caplog.records)
        assert "slow" in log_text1 or "bbox" in log_text1

        # Add bbox column
        caplog.clear()
        add_bbox_column(buildings_test_file, temp_output_file, verbose=False)

        # Get bounds with bbox column - should not show warning
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            bounds2 = get_dataset_bounds(temp_output_file, verbose=False)

        # Should not have warning about slow calculation
        log_text2 = " ".join(record.message.lower() for record in caplog.records)
        assert "slow" not in log_text2 or len(log_text2) == 0

        # Results should be the same
        assert bounds1 is not None
        assert bounds2 is not None
