"""Tests for bloom filter detection functionality.

Tests cover:
- Core detection function in duckdb_metadata.py
- Integration with check_parquet_structure.py
- Integration with inspect metadata display

Fixtures used in this module:
    - places_test_file: Provided by conftest.py (no bloom filters)
    - buildings_test_file: Provided by conftest.py (no bloom filters)
"""

from unittest.mock import patch

import pyarrow.parquet as pq


class TestGetBloomFilterInfo:
    """Tests for get_bloom_filter_info in duckdb_metadata.py."""

    def test_returns_list(self, places_test_file):
        """Test that get_bloom_filter_info returns a list."""
        from geoparquet_io.core.duckdb_metadata import get_bloom_filter_info

        result = get_bloom_filter_info(places_test_file)
        schema = pq.ParquetFile(places_test_file).schema
        expected = {schema.column(i).path.replace(".", ", ") for i in range(len(schema))}
        assert {entry["column_name"] for entry in result} == expected

    def test_no_bloom_filters_in_places(self, places_test_file):
        """Test that places file (standard test file) has no bloom filters."""
        from geoparquet_io.core.duckdb_metadata import get_bloom_filter_info

        result = get_bloom_filter_info(places_test_file)
        for entry in result:
            assert entry["row_groups_with_bloom_filter"] == 0
            assert entry["bloom_filter_coverage_pct"] == 0.0

    def test_result_structure(self, places_test_file):
        """Test that each result entry has the expected keys."""
        from geoparquet_io.core.duckdb_metadata import get_bloom_filter_info

        result = get_bloom_filter_info(places_test_file)
        assert len(result) > 0
        for entry in result:
            assert "column_name" in entry
            assert "row_groups" in entry
            assert "row_groups_with_bloom_filter" in entry
            assert "bloom_filter_coverage_pct" in entry
            assert "total_bloom_filter_bytes" in entry

    def test_with_existing_connection(self, places_test_file):
        """Test that an existing DuckDB connection can be passed."""
        import duckdb

        from geoparquet_io.core.duckdb_metadata import get_bloom_filter_info

        con = duckdb.connect()
        result = get_bloom_filter_info(places_test_file, con=con)
        assert isinstance(result, list)
        assert len(result) > 0
        con.close()

    def test_buildings_file(self, buildings_test_file):
        """Test with the buildings test file."""
        from geoparquet_io.core.duckdb_metadata import get_bloom_filter_info

        result = get_bloom_filter_info(buildings_test_file)
        assert isinstance(result, list)
        assert len(result) > 0


class TestGetBloomFilterInfoWithMockedData:
    """Tests for bloom filter detection with mocked DuckDB data.

    These tests simulate files that have bloom filters since
    PyArrow 21 cannot write bloom filters.
    """

    def _mock_bloom_filter_query(self, parquet_file, con=None):
        """Return mocked bloom filter query results."""
        return [
            {
                "column_name": "city",
                "row_groups": 2,
                "row_groups_with_bloom_filter": 2,
                "bloom_filter_coverage_pct": 100.0,
                "total_bloom_filter_bytes": 256,
            },
            {
                "column_name": "state",
                "row_groups": 2,
                "row_groups_with_bloom_filter": 2,
                "bloom_filter_coverage_pct": 100.0,
                "total_bloom_filter_bytes": 128,
            },
            {
                "column_name": "value",
                "row_groups": 2,
                "row_groups_with_bloom_filter": 0,
                "bloom_filter_coverage_pct": 0.0,
                "total_bloom_filter_bytes": 0,
            },
        ]

    def test_detects_bloom_filters(self):
        """Test that columns with bloom filters are correctly detected."""
        result = self._mock_bloom_filter_query("fake.parquet")
        columns_with_bloom = [r for r in result if r["row_groups_with_bloom_filter"] > 0]
        column_names = {r["column_name"] for r in columns_with_bloom}
        assert "city" in column_names
        assert "state" in column_names

    def test_coverage_percentage(self):
        """Test that coverage percentage is 100 for fully bloom-filtered columns."""
        result = self._mock_bloom_filter_query("fake.parquet")
        city_entry = next(r for r in result if r["column_name"] == "city")
        assert city_entry["bloom_filter_coverage_pct"] == 100.0

    def test_bloom_filter_bytes_positive(self):
        """Test that bloom filter bytes is positive for filtered columns."""
        result = self._mock_bloom_filter_query("fake.parquet")
        city_entry = next(r for r in result if r["column_name"] == "city")
        assert city_entry["total_bloom_filter_bytes"] > 0

    def test_columns_without_bloom_have_zero_bytes(self):
        """Test that columns without bloom filters have zero bytes."""
        result = self._mock_bloom_filter_query("fake.parquet")
        value_entry = next(r for r in result if r["column_name"] == "value")
        assert value_entry["total_bloom_filter_bytes"] == 0


class TestCheckBloomFilters:
    """Tests for check_bloom_filters in check_parquet_structure.py."""

    def test_returns_dict_when_return_results(self, places_test_file):
        """Test that check_bloom_filters returns a dict when return_results=True."""
        from geoparquet_io.core.check_parquet_structure import check_bloom_filters

        result = check_bloom_filters(places_test_file, return_results=True, quiet=True)
        assert isinstance(result, dict)

    def test_info_when_no_bloom_filters(self, places_test_file):
        """Test that check returns info status when no bloom filters found."""
        from geoparquet_io.core.check_parquet_structure import check_bloom_filters

        result = check_bloom_filters(places_test_file, return_results=True, quiet=True)
        # No bloom filters is not a failure, just informational
        assert result["passed"] is True
        assert result["has_bloom_filters"] is False

    def test_result_structure(self, places_test_file):
        """Test result dict has expected keys."""
        from geoparquet_io.core.check_parquet_structure import check_bloom_filters

        result = check_bloom_filters(places_test_file, return_results=True, quiet=True)
        assert "passed" in result
        assert "has_bloom_filters" in result
        assert "columns_with_bloom_filters" in result
        assert "columns_without_bloom_filters" in result
        assert "bloom_filter_details" in result

    def test_quiet_mode_no_output(self, places_test_file, capsys):
        """Test that quiet mode suppresses output."""
        from geoparquet_io.core.check_parquet_structure import check_bloom_filters

        check_bloom_filters(places_test_file, return_results=True, quiet=True)
        captured = capsys.readouterr()
        assert captured.out == ""

    def test_passed_with_mocked_bloom_filters(self, places_test_file):
        """Test that check passes when bloom filters are present (mocked)."""
        from geoparquet_io.core.check_parquet_structure import check_bloom_filters

        mock_data = [
            {
                "column_name": "name",
                "row_groups": 1,
                "row_groups_with_bloom_filter": 1,
                "bloom_filter_coverage_pct": 100.0,
                "total_bloom_filter_bytes": 256,
            },
        ]

        with patch(
            "geoparquet_io.core.duckdb_metadata.get_bloom_filter_info",
            return_value=mock_data,
        ):
            result = check_bloom_filters(places_test_file, return_results=True, quiet=True)
        assert result["passed"] is True
        assert result["has_bloom_filters"] is True
        assert "name" in result["columns_with_bloom_filters"]

    def test_returns_none_when_not_return_results(self, places_test_file):
        """Test that function returns None when return_results is False."""
        from geoparquet_io.core.check_parquet_structure import check_bloom_filters

        result = check_bloom_filters(places_test_file, return_results=False, quiet=True)
        assert result is None


class TestCheckAllIncludesBloomFilters:
    """Tests that check_all includes bloom filter results."""

    def test_check_all_has_bloom_filter_key(self, places_test_file):
        """Test that check_all results include bloom_filters key."""
        from geoparquet_io.core.check_parquet_structure import check_all

        results = check_all(
            places_test_file,
            verbose=False,
            return_results=True,
            quiet=True,
        )
        assert "bloom_filters" in results

    def test_check_all_bloom_filter_results(self, places_test_file):
        """Test that check_all bloom filter results for places file."""
        from geoparquet_io.core.check_parquet_structure import check_all

        results = check_all(
            places_test_file,
            verbose=False,
            return_results=True,
            quiet=True,
        )
        assert results["bloom_filters"]["has_bloom_filters"] is False


class TestInspectMetaBloomFilters:
    """Tests for bloom filter info in inspect meta output."""

    def test_format_parquet_metadata_no_bloom_filters(self, places_test_file, capsys):
        """Test that inspect meta works when no bloom filters are present."""
        from geoparquet_io.core.metadata_utils import format_parquet_metadata_enhanced

        format_parquet_metadata_enhanced(places_test_file, json_output=False)
        captured = capsys.readouterr()
        # Should still show bloom filter section even if empty
        assert "Bloom" in captured.out or "bloom" in captured.out.lower()

    def test_format_parquet_metadata_with_bloom_filters(self, places_test_file, capsys):
        """Test that inspect meta shows bloom filter info when present (mocked)."""
        from geoparquet_io.core.metadata_utils import format_parquet_metadata_enhanced

        mock_data = [
            {
                "column_name": "name",
                "row_groups": 1,
                "row_groups_with_bloom_filter": 1,
                "bloom_filter_coverage_pct": 100.0,
                "total_bloom_filter_bytes": 256,
            },
            {
                "column_name": "geometry",
                "row_groups": 1,
                "row_groups_with_bloom_filter": 0,
                "bloom_filter_coverage_pct": 0.0,
                "total_bloom_filter_bytes": 0,
            },
        ]

        with patch(
            "geoparquet_io.core.duckdb_metadata.get_bloom_filter_info",
            return_value=mock_data,
        ):
            format_parquet_metadata_enhanced(places_test_file, json_output=False)
        captured = capsys.readouterr()
        assert "Bloom" in captured.out or "bloom" in captured.out.lower()

    def test_format_parquet_metadata_json_includes_bloom(self, places_test_file, capsys):
        """Test that JSON output includes bloom filter info."""
        from geoparquet_io.core.metadata_utils import format_parquet_metadata_enhanced

        mock_data = [
            {
                "column_name": "name",
                "row_groups": 1,
                "row_groups_with_bloom_filter": 1,
                "bloom_filter_coverage_pct": 100.0,
                "total_bloom_filter_bytes": 256,
            },
        ]

        with patch(
            "geoparquet_io.core.duckdb_metadata.get_bloom_filter_info",
            return_value=mock_data,
        ):
            format_parquet_metadata_enhanced(places_test_file, json_output=True)
        captured = capsys.readouterr()
        assert "bloom_filter" in captured.out.lower()
