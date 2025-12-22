"""Tests for core/check_parquet_structure.py module.

Fixtures used in this module:
    - places_test_file: Provided by conftest.py
    - buildings_test_file: Provided by conftest.py
"""

from geoparquet_io.core.check_parquet_structure import (
    assess_row_count,
    assess_row_group_size,
    check_compression,
    check_metadata_and_bbox,
    check_row_groups,
    get_compression_info,
    get_row_group_stats,
)


class TestAssessRowGroupSize:
    """Tests for assess_row_group_size function."""

    def test_optimal_for_small_file(self):
        """Test optimal status for small files."""
        # Small file under 64 MB
        total_size = 50 * 1024 * 1024  # 50 MB
        avg_group_size = 50 * 1024 * 1024  # 50 MB
        status, message, color = assess_row_group_size(avg_group_size, total_size)
        assert status == "optimal"
        assert color == "green"

    def test_optimal_for_64_to_256_mb(self):
        """Test optimal status for 64-256 MB row groups."""
        total_size = 500 * 1024 * 1024  # 500 MB total
        avg_group_size = 128 * 1024 * 1024  # 128 MB
        status, message, color = assess_row_group_size(avg_group_size, total_size)
        assert status == "optimal"
        assert "64-256 MB" in message
        assert color == "green"

    def test_suboptimal_for_32_to_64_mb(self):
        """Test suboptimal status for 32-64 MB row groups."""
        total_size = 500 * 1024 * 1024  # 500 MB total
        avg_group_size = 40 * 1024 * 1024  # 40 MB
        status, message, color = assess_row_group_size(avg_group_size, total_size)
        assert status == "suboptimal"
        assert color == "yellow"

    def test_suboptimal_for_256_to_512_mb(self):
        """Test suboptimal status for 256-512 MB row groups."""
        total_size = 1000 * 1024 * 1024  # 1 GB total
        avg_group_size = 300 * 1024 * 1024  # 300 MB
        status, message, color = assess_row_group_size(avg_group_size, total_size)
        assert status == "suboptimal"
        assert color == "yellow"

    def test_poor_for_very_small_groups(self):
        """Test poor status for very small row groups."""
        total_size = 500 * 1024 * 1024  # 500 MB total
        avg_group_size = 10 * 1024 * 1024  # 10 MB
        status, message, color = assess_row_group_size(avg_group_size, total_size)
        assert status == "poor"
        assert color == "red"

    def test_poor_for_very_large_groups(self):
        """Test poor status for very large row groups."""
        total_size = 2000 * 1024 * 1024  # 2 GB total
        avg_group_size = 600 * 1024 * 1024  # 600 MB
        status, message, color = assess_row_group_size(avg_group_size, total_size)
        assert status == "poor"
        assert color == "red"


class TestAssessRowCount:
    """Tests for assess_row_count function."""

    def test_optimal_for_10k_to_200k(self):
        """Test optimal status for 10,000-200,000 rows."""
        status, message, color = assess_row_count(100000)
        assert status == "optimal"
        assert color == "green"

    def test_poor_for_very_low_rows(self):
        """Test poor status for very low row count."""
        status, message, color = assess_row_count(500)
        assert status == "poor"
        assert "very low" in message
        assert color == "red"

    def test_poor_for_very_high_rows(self):
        """Test poor status for very high row count."""
        status, message, color = assess_row_count(2000000)
        assert status == "poor"
        assert "very high" in message
        assert color == "red"

    def test_suboptimal_for_between_ranges(self):
        """Test suboptimal status for row counts between optimal and poor."""
        # 2000 < rows < 10000 (below optimal range)
        status, message, color = assess_row_count(5000)
        assert status == "suboptimal"
        assert color == "yellow"

        # 200000 < rows < 1000000 (above optimal range)
        status, message, color = assess_row_count(500000)
        assert status == "suboptimal"
        assert color == "yellow"

    def test_optimal_for_small_file_single_group(self):
        """Test small file leniency: any row count is optimal for small single-group files."""
        # Small file under 64MB with single row group
        total_size = 50 * 1024 * 1024  # 50 MB
        low_row_count = 100  # Normally would be "poor"
        status, message, color = assess_row_count(
            low_row_count, total_size_bytes=total_size, num_groups=1
        )
        assert status == "optimal"
        assert color == "green"
        assert "appropriate for small file" in message


class TestGetRowGroupStats:
    """Tests for get_row_group_stats function."""

    def test_returns_stats_dict(self, places_test_file):
        """Test that get_row_group_stats returns expected dict with valid values."""
        import numbers

        stats = get_row_group_stats(places_test_file)
        assert isinstance(stats, dict)
        assert "num_groups" in stats
        assert "total_rows" in stats
        assert "avg_rows_per_group" in stats

        # Validate types and ranges
        assert isinstance(stats["num_groups"], int)
        assert stats["num_groups"] >= 1

        assert isinstance(stats["total_rows"], int)
        assert stats["total_rows"] >= 0

        assert isinstance(stats["avg_rows_per_group"], numbers.Number)
        assert stats["avg_rows_per_group"] >= 0

        # Sanity checks
        assert stats["total_rows"] >= stats["num_groups"]
        if stats["num_groups"] > 0:
            expected_avg = stats["total_rows"] / stats["num_groups"]
            assert abs(stats["avg_rows_per_group"] - expected_avg) < 0.01


class TestGetCompressionInfo:
    """Tests for get_compression_info function."""

    # Known Parquet compression codecs
    VALID_CODECS = {"ZSTD", "SNAPPY", "GZIP", "LZ4", "BROTLI", "UNCOMPRESSED", "LZO"}

    def test_returns_compression_dict(self, places_test_file):
        """Test that get_compression_info returns expected dict with valid codecs."""
        info = get_compression_info(places_test_file)
        assert isinstance(info, dict)
        # Should contain at least one column
        assert len(info) > 0

        # Validate all compression values are strings and known codecs
        for col_name, codec in info.items():
            assert isinstance(codec, str), f"Codec for {col_name} should be a string"
            assert codec in self.VALID_CODECS, f"Unknown codec '{codec}' for column {col_name}"

    def test_with_specific_column(self, places_test_file):
        """Test get_compression_info with specific column returns valid codec."""
        info = get_compression_info(places_test_file, "geometry")
        assert isinstance(info, dict)
        assert "geometry" in info

        # Validate geometry column codec
        geometry_codec = info["geometry"]
        assert isinstance(geometry_codec, str)
        assert geometry_codec in self.VALID_CODECS


class TestCheckCompression:
    """Tests for check_compression function."""

    # Known Parquet compression codecs
    VALID_CODECS = {"ZSTD", "SNAPPY", "GZIP", "LZ4", "BROTLI", "UNCOMPRESSED", "LZO"}

    def test_returns_results(self, places_test_file):
        """Test check_compression with return_results=True returns valid structure."""
        result = check_compression(places_test_file, verbose=False, return_results=True)
        assert isinstance(result, dict)
        assert "current_compression" in result
        assert "passed" in result

        # Validate value types
        assert isinstance(result["passed"], bool)
        assert isinstance(result["current_compression"], str)
        assert len(result["current_compression"]) > 0

        # Check if geometry_column is present and not None (if available)
        if "geometry_column" in result:
            assert result["geometry_column"] is not None

    def test_with_verbose(self, places_test_file, caplog):
        """Test check_compression with verbose flag produces output."""
        import logging

        with caplog.at_level(logging.DEBUG):
            check_compression(places_test_file, verbose=True, return_results=False)

        # Verify log output was captured
        assert len(caplog.records) > 0, "Expected log output when verbose=True"
        combined = " ".join(record.message.lower() for record in caplog.records)
        # Check for compression-related text
        assert any(
            phrase in combined
            for phrase in ["compression", "zstd", "snappy", "gzip", "uncompressed"]
        )


class TestCheckRowGroups:
    """Tests for check_row_groups function."""

    def test_returns_results(self, places_test_file):
        """Test check_row_groups with return_results=True returns valid structure."""
        result = check_row_groups(places_test_file, verbose=False, return_results=True)
        assert isinstance(result, dict)
        assert "passed" in result

        # Validate passed is a bool
        assert isinstance(result["passed"], bool)

        # Check for expected keys if present
        if "num_row_groups" in result:
            assert isinstance(result["num_row_groups"], int)
            assert result["num_row_groups"] >= 1
        if "avg_rows_per_group" in result:
            assert result["avg_rows_per_group"] >= 0

    def test_with_verbose(self, places_test_file, caplog):
        """Test check_row_groups with verbose flag produces output."""
        import logging

        with caplog.at_level(logging.DEBUG):
            check_row_groups(places_test_file, verbose=True, return_results=False)

        # Verify log output was captured
        assert len(caplog.records) > 0, "Expected log output when verbose=True"
        combined = " ".join(record.message.lower() for record in caplog.records)
        # Check for row-group-related text
        assert any(phrase in combined for phrase in ["row", "group", "rows", "size"])


class TestCheckMetadataAndBbox:
    """Tests for check_metadata_and_bbox function."""

    def test_returns_results(self, places_test_file):
        """Test check_metadata_and_bbox with return_results=True."""
        result = check_metadata_and_bbox(places_test_file, verbose=False, return_results=True)
        assert isinstance(result, dict)
        assert "has_bbox_column" in result
        assert "passed" in result

    def test_with_file_without_bbox(self, buildings_test_file):
        """Test check_metadata_and_bbox with file without bbox."""
        result = check_metadata_and_bbox(buildings_test_file, verbose=False, return_results=True)
        assert isinstance(result, dict)
        assert result["has_bbox_column"] is False

    def test_with_verbose(self, places_test_file):
        """Test check_metadata_and_bbox with verbose flag."""
        # Should not raise
        check_metadata_and_bbox(places_test_file, verbose=True, return_results=False)
