"""Tests for split_by_country helper functions."""

from pathlib import Path

import pytest


class TestCheckCountryCodeColumn:
    """Tests for check_country_code_column function."""

    @pytest.fixture
    def sample_file(self):
        """Return path to the sample file."""
        return str(Path(__file__).parent / "data" / "sample.parquet")

    def test_missing_column_raises_error(self, sample_file):
        """Test that missing column raises UsageError."""
        from click import UsageError

        from geoparquet_io.core.split_by_country import check_country_code_column

        with pytest.raises(UsageError):
            check_country_code_column(sample_file, "nonexistent_column")
