"""Tests for partition_by_h3 helper functions."""

from geoparquet_io.core.partition_common import calculate_partition_stats


class TestCalculatePartitionStats:
    """Tests for calculate_partition_stats function."""

    def test_empty_folder(self, tmp_path):
        """Test with empty folder."""
        total_mb, avg_mb = calculate_partition_stats(str(tmp_path), 0)
        assert total_mb == 0.0
        assert avg_mb == 0.0

    def test_zero_partitions(self, tmp_path):
        """Test with zero partitions (avoid division by zero)."""
        total_mb, avg_mb = calculate_partition_stats(str(tmp_path), 0)
        assert avg_mb == 0.0

    def test_with_files(self, tmp_path):
        """Test with parquet files."""
        # Create mock parquet files
        for i in range(5):
            f = tmp_path / f"partition_{i}.parquet"
            f.write_bytes(b"x" * 2048)  # 2KB each

        total_mb, avg_mb = calculate_partition_stats(str(tmp_path), 5)
        assert total_mb > 0
        assert avg_mb > 0
        assert abs(total_mb / 5 - avg_mb) < 0.001  # avg should be total/5

    def test_nested_folders(self, tmp_path):
        """Test with nested folder structure."""
        # Create nested structure like Hive partitioning
        subdir = tmp_path / "h3_cell=abc123"
        subdir.mkdir()
        f = subdir / "data.parquet"
        f.write_bytes(b"x" * 1024)

        total_mb, avg_mb = calculate_partition_stats(str(tmp_path), 1)
        assert total_mb > 0

    def test_non_parquet_files_ignored(self, tmp_path):
        """Test that non-parquet files are ignored."""
        # Create a parquet and a non-parquet file
        parquet = tmp_path / "data.parquet"
        parquet.write_bytes(b"x" * 1024)
        txt = tmp_path / "readme.txt"
        txt.write_text("not a parquet file")

        total_mb, avg_mb = calculate_partition_stats(str(tmp_path), 1)
        # Should only count the parquet file
        expected_mb = 1024 / (1024 * 1024)
        assert abs(total_mb - expected_mb) < 0.001
