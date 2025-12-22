"""Tests for core/partition_by_string.py module."""

from unittest.mock import patch

import click
import pytest

from geoparquet_io.core.partition_by_string import (
    partition_by_string,
    validate_column_exists,
)


class TestValidateColumnExists:
    """Tests for validate_column_exists function."""

    def test_valid_column(self, places_test_file):
        """Test validation passes for existing column."""
        # Should not raise
        validate_column_exists(places_test_file, "name", verbose=False)

    def test_invalid_column(self, places_test_file):
        """Test validation fails for non-existent column."""
        with pytest.raises(click.UsageError) as exc_info:
            validate_column_exists(places_test_file, "nonexistent_column", verbose=False)
        assert "not found" in str(exc_info.value)
        assert "nonexistent_column" in str(exc_info.value)

    def test_with_verbose(self, places_test_file):
        """Test validation with verbose output."""
        # Should not raise
        validate_column_exists(places_test_file, "name", verbose=True)


class TestPartitionByString:
    """Tests for partition_by_string function."""

    def test_chars_zero_raises_error(self, places_test_file, tmp_path):
        """Test that chars=0 raises UsageError (line 86)."""
        with pytest.raises(click.UsageError) as exc_info:
            partition_by_string(
                input_parquet=places_test_file,
                output_folder=str(tmp_path),
                column="name",
                chars=0,
                verbose=False,
            )
        assert "--chars must be a positive integer" in str(exc_info.value)

    def test_chars_negative_raises_error(self, places_test_file, tmp_path):
        """Test that negative chars raises UsageError (line 86)."""
        with pytest.raises(click.UsageError) as exc_info:
            partition_by_string(
                input_parquet=places_test_file,
                output_folder=str(tmp_path),
                column="name",
                chars=-5,
                verbose=False,
            )
        assert "--chars must be a positive integer" in str(exc_info.value)

    def test_preview_with_partition_analysis_error(self, places_test_file, tmp_path):
        """Test preview mode when PartitionAnalysisError is raised (lines 103-105)."""
        # Import the actual exception class
        from geoparquet_io.core.partition_common import PartitionAnalysisError

        # Patch at the source module where it's defined
        with patch(
            "geoparquet_io.core.partition_common.analyze_partition_strategy"
        ) as mock_analyze:
            mock_analyze.side_effect = PartitionAnalysisError("Test analysis error")

            # Should not raise - exception is caught and preview continues
            partition_by_string(
                input_parquet=places_test_file,
                output_folder=str(tmp_path),
                column="name",
                preview=True,
                verbose=False,
            )

    def test_preview_with_generic_exception(self, places_test_file, tmp_path):
        """Test preview mode when generic Exception is raised (lines 106-108)."""
        # Patch at the source module where it's defined
        with patch(
            "geoparquet_io.core.partition_common.analyze_partition_strategy"
        ) as mock_analyze:
            mock_analyze.side_effect = Exception("Unexpected error")

            # Should not raise - exception is caught and preview continues
            partition_by_string(
                input_parquet=places_test_file,
                output_folder=str(tmp_path),
                column="name",
                preview=True,
                verbose=False,
            )

    def test_preview_mode_basic(self, places_test_file, tmp_path, capsys):
        """Test basic preview mode execution."""
        partition_by_string(
            input_parquet=places_test_file,
            output_folder=str(tmp_path),
            column="name",
            preview=True,
            verbose=False,
        )
        # Preview should not create any files
        output_files = list(tmp_path.glob("*.parquet"))
        assert len(output_files) == 0

    def test_invalid_column_raises_error(self, places_test_file, tmp_path):
        """Test that invalid column raises UsageError."""
        with pytest.raises(click.UsageError) as exc_info:
            partition_by_string(
                input_parquet=places_test_file,
                output_folder=str(tmp_path),
                column="nonexistent_column",
                verbose=False,
            )
        assert "not found" in str(exc_info.value)
