"""Additional tests for metadata_utils helper functions."""

from geoparquet_io.core.metadata_utils import _build_row_group_json


class TestBuildRowGroupJson:
    """Tests for _build_row_group_json function."""

    def test_empty_columns(self):
        """Test with empty columns list."""
        result = _build_row_group_json(0, [], {})
        assert result["id"] == 0
        assert result["num_columns"] == 0
        assert result["total_byte_size"] == 0
        assert result["columns"] == []

    def test_single_column(self):
        """Test with a single column."""
        cols = [
            {
                "path_in_schema": "col1",
                "type": "INT32",
                "total_compressed_size": 1024,
                "total_uncompressed_size": 2048,
                "compression": "ZSTD",
            }
        ]
        result = _build_row_group_json(1, cols, {})

        assert result["id"] == 1
        assert result["num_columns"] == 1
        assert result["total_byte_size"] == 1024
        assert len(result["columns"]) == 1
        assert result["columns"][0]["path_in_schema"] == "col1"

    def test_geo_column(self):
        """Test with a geometry column."""
        cols = [
            {
                "path_in_schema": "geometry",
                "type": "BYTE_ARRAY",
                "total_compressed_size": 4096,
                "total_uncompressed_size": 8192,
                "compression": "ZSTD",
            }
        ]
        geo_columns = {"geometry": "WKB"}
        result = _build_row_group_json(0, cols, geo_columns)

        assert result["columns"][0]["is_geo"] is True
        assert result["columns"][0]["geo_type"] == "WKB"

    def test_with_statistics(self):
        """Test columns with min/max statistics."""
        cols = [
            {
                "path_in_schema": "id",
                "type": "INT64",
                "total_compressed_size": 100,
                "total_uncompressed_size": 200,
                "compression": "SNAPPY",
                "stats_min": 1,
                "stats_max": 100,
            }
        ]
        result = _build_row_group_json(0, cols, {})

        assert "statistics" in result["columns"][0]
        assert result["columns"][0]["statistics"]["min"] == "1"
        assert result["columns"][0]["statistics"]["max"] == "100"

    def test_multiple_columns_total_size(self):
        """Test that total size is sum of all columns."""
        cols = [
            {"path_in_schema": "a", "total_compressed_size": 100},
            {"path_in_schema": "b", "total_compressed_size": 200},
            {"path_in_schema": "c", "total_compressed_size": 300},
        ]
        result = _build_row_group_json(0, cols, {})

        assert result["total_byte_size"] == 600

    def test_none_sizes_treated_as_zero(self):
        """Test that None sizes are treated as zero."""
        cols = [
            {"path_in_schema": "a", "total_compressed_size": None},
            {"path_in_schema": "b", "total_compressed_size": 200},
        ]
        result = _build_row_group_json(0, cols, {})

        assert result["total_byte_size"] == 200
