"""
Tests for reading and processing partitioned GeoParquet data.

These tests verify that gpio commands properly handle partition input:
- Directory paths (flat partitions)
- Glob patterns
- Partition info detection
- Aggregation across partitions
"""

import os

import pyarrow.parquet as pq
from click.testing import CliRunner

from geoparquet_io.cli.main import check, extract, inspect
from geoparquet_io.core.partition_reader import (
    build_read_parquet_expr,
    get_files_to_check,
    get_partition_info,
)


class TestPartitionInfo:
    """Tests for partition detection and info extraction."""

    def test_directory_detected_as_partition(self, country_partition_dir):
        """Test that a directory with parquet files is detected as partition."""
        info = get_partition_info(country_partition_dir)
        assert info["is_partition"] is True
        assert info["partition_type"] == "flat"
        assert info["file_count"] == 4

    def test_partition_all_files_listed(self, country_partition_dir):
        """Test that all partition files are listed."""
        info = get_partition_info(country_partition_dir)
        all_files = info["all_files"]
        assert len(all_files) == 4
        filenames = [os.path.basename(f) for f in all_files]
        assert "El_Salvador.parquet" in filenames
        assert "Guatemala.parquet" in filenames
        assert "Honduras.parquet" in filenames
        assert "Nicaragua.parquet" in filenames

    def test_partition_first_file_valid(self, country_partition_dir):
        """Test that first file is a valid parquet file."""
        info = get_partition_info(country_partition_dir)
        first_file = info["first_file"]
        assert first_file is not None
        assert os.path.exists(first_file)
        assert first_file.endswith(".parquet")

    def test_single_file_not_partition(self, buildings_test_file):
        """Test that a single file is not detected as partition."""
        info = get_partition_info(buildings_test_file)
        assert info["is_partition"] is False
        assert info["partition_type"] == "single"
        assert info["file_count"] == 1

    def test_glob_pattern_detected(self, country_partition_dir):
        """Test that glob patterns are detected as partitions."""
        glob_path = os.path.join(country_partition_dir, "*.parquet")
        info = get_partition_info(glob_path)
        assert info["is_partition"] is True
        assert info["partition_type"] == "glob"
        assert info["file_count"] == 4


class TestGetFilesToCheck:
    """Tests for get_files_to_check function."""

    def test_default_returns_first_file(self, country_partition_dir):
        """Test that default mode returns only the first file."""
        files, notice = get_files_to_check(country_partition_dir)
        assert len(files) == 1
        assert "first file" in notice.lower()
        assert "4 total" in notice

    def test_check_all_returns_all_files(self, country_partition_dir):
        """Test that check_all returns all files."""
        files, notice = get_files_to_check(country_partition_dir, check_all=True)
        assert len(files) == 4
        assert "all 4 files" in notice.lower()

    def test_sample_returns_subset(self, country_partition_dir):
        """Test that sample returns requested number of files."""
        files, notice = get_files_to_check(country_partition_dir, check_sample=2)
        assert len(files) == 2
        assert "sample of 2" in notice.lower()

    def test_sample_caps_at_total(self, country_partition_dir):
        """Test that sample is capped at total file count."""
        files, notice = get_files_to_check(country_partition_dir, check_sample=100)
        assert len(files) == 4
        assert "sample of 4" in notice.lower()

    def test_single_file_returns_file(self, buildings_test_file):
        """Test that single file input returns that file."""
        files, notice = get_files_to_check(buildings_test_file)
        assert len(files) == 1
        assert files[0] == buildings_test_file
        assert notice == ""


class TestBuildReadParquetExpr:
    """Tests for build_read_parquet_expr function."""

    def test_single_file_expr(self, buildings_test_file):
        """Test expression for single file."""
        expr = build_read_parquet_expr(buildings_test_file)
        assert "read_parquet(" in expr
        assert buildings_test_file in expr or "buildings_test.parquet" in expr

    def test_directory_expr_uses_glob(self, country_partition_dir):
        """Test expression for directory includes glob pattern."""
        expr = build_read_parquet_expr(country_partition_dir)
        assert "read_parquet(" in expr
        assert "*.parquet" in expr

    def test_schema_diff_option(self, country_partition_dir):
        """Test that allow_schema_diff adds union_by_name option."""
        expr = build_read_parquet_expr(country_partition_dir, allow_schema_diff=True)
        assert "union_by_name=true" in expr

    def test_hive_input_option(self, country_partition_dir):
        """Test that hive_input adds hive_partitioning option."""
        expr = build_read_parquet_expr(country_partition_dir, hive_input=True)
        assert "hive_partitioning=true" in expr


class TestInspectPartition:
    """Tests for gpio inspect on partitioned data."""

    def test_inspect_partition_shows_file_count(self, country_partition_dir):
        """Test that inspect shows partition file count."""
        runner = CliRunner()
        result = runner.invoke(inspect, [country_partition_dir])
        assert result.exit_code == 0
        # Should indicate this is a partition with multiple files
        assert "of 4 total" in result.output or "4 files" in result.output

    def test_inspect_partition_check_all(self, country_partition_dir):
        """Test inspect --check-all aggregates partition info."""
        runner = CliRunner()
        result = runner.invoke(inspect, [country_partition_dir, "--check-all"])
        assert result.exit_code == 0
        # Should show aggregated row count (sum of all files)
        assert "Total Rows" in result.output or "rows" in result.output.lower()

    def test_inspect_partition_json_output(self, country_partition_dir):
        """Test inspect partition with JSON output."""
        runner = CliRunner()
        result = runner.invoke(inspect, [country_partition_dir, "--check-all", "--json"])
        assert result.exit_code == 0
        # JSON output should be parseable
        import json

        data = json.loads(result.output)
        assert "total_rows" in data or "files" in data

    def test_inspect_glob_pattern(self, country_partition_dir):
        """Test inspect with glob pattern."""
        runner = CliRunner()
        glob_path = os.path.join(country_partition_dir, "*.parquet")
        result = runner.invoke(inspect, [glob_path])
        assert result.exit_code == 0


class TestExtractPartition:
    """Tests for gpio extract on partitioned data."""

    def test_extract_from_partition_directory(self, country_partition_dir, temp_output_file):
        """Test extracting data from partition directory."""
        runner = CliRunner()
        result = runner.invoke(extract, [country_partition_dir, temp_output_file])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify output has data from all files
        pf = pq.ParquetFile(temp_output_file)
        # Total rows should be sum of all partition files (~5020 rows)
        assert pf.metadata.num_rows > 4000

    def test_extract_from_glob_pattern(self, country_partition_dir, temp_output_file):
        """Test extracting data from glob pattern."""
        runner = CliRunner()
        glob_path = os.path.join(country_partition_dir, "*.parquet")
        result = runner.invoke(extract, [glob_path, temp_output_file])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        pf = pq.ParquetFile(temp_output_file)
        assert pf.metadata.num_rows > 4000

    def test_extract_partition_with_limit(self, country_partition_dir, temp_output_file):
        """Test extract with row limit from partition."""
        runner = CliRunner()
        result = runner.invoke(extract, [country_partition_dir, temp_output_file, "--limit", "100"])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        pf = pq.ParquetFile(temp_output_file)
        assert pf.metadata.num_rows == 100

    def test_extract_partition_preserves_geo_metadata(
        self, country_partition_dir, temp_output_file
    ):
        """Test that extract preserves GeoParquet metadata."""
        runner = CliRunner()
        result = runner.invoke(extract, [country_partition_dir, temp_output_file])
        assert result.exit_code == 0

        pf = pq.ParquetFile(temp_output_file)
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata

    def test_extract_partition_with_columns(self, country_partition_dir, temp_output_file):
        """Test extract specific columns from partition."""
        runner = CliRunner()
        result = runner.invoke(
            extract,
            [country_partition_dir, temp_output_file, "--include-cols", "id"],
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        pf = pq.ParquetFile(temp_output_file)
        columns = pf.schema_arrow.names
        assert "id" in columns
        assert "geometry" in columns  # geometry is always included
        # Should have id, geometry, and bbox (geometry/bbox always included)
        assert len(columns) == 3


class TestCheckPartition:
    """Tests for gpio check commands on partitioned data."""

    def test_check_all_partition_default(self, country_partition_dir):
        """Test check all on partition (default: first file)."""
        runner = CliRunner()
        result = runner.invoke(check, ["all", country_partition_dir])
        assert result.exit_code == 0
        # Should show version info and indicate partition
        assert "1.1" in result.output or "Version" in result.output
        assert "first file" in result.output.lower()

    def test_check_all_partition_all_files(self, country_partition_dir):
        """Test check all on all files in partition."""
        runner = CliRunner()
        result = runner.invoke(check, ["all", country_partition_dir, "--all-files"])
        assert result.exit_code == 0
        # Should check all 4 files
        assert "all 4 files" in result.output.lower()

    def test_check_all_partition_sample(self, country_partition_dir):
        """Test check all on sample of partition files."""
        runner = CliRunner()
        result = runner.invoke(check, ["all", country_partition_dir, "--sample-files", "2"])
        assert result.exit_code == 0
        assert "sample" in result.output.lower()

    def test_check_compression_partition(self, country_partition_dir):
        """Test check compression on partition."""
        runner = CliRunner()
        result = runner.invoke(check, ["compression", country_partition_dir])
        assert result.exit_code == 0
        assert "ZSTD" in result.output or "compression" in result.output.lower()

    def test_check_bbox_partition(self, country_partition_dir):
        """Test check bbox on partition."""
        runner = CliRunner()
        result = runner.invoke(check, ["bbox", country_partition_dir])
        assert result.exit_code == 0
        # Files have proper bbox metadata
        assert "bbox" in result.output.lower()


class TestPartitionDataIntegrity:
    """Tests for data integrity when working with partitions."""

    def test_partition_row_counts(self, country_partition_dir):
        """Test that partition file row counts are correct."""
        info = get_partition_info(country_partition_dir)
        total_rows = 0
        for f in info["all_files"]:
            pf = pq.ParquetFile(f)
            total_rows += pf.metadata.num_rows

        # Expected: 1445 + 844 + 1350 + 1381 = 5020 rows
        assert total_rows == 5020

    def test_partition_schema_consistent(self, country_partition_dir):
        """Test that all partition files have consistent schema."""
        info = get_partition_info(country_partition_dir)
        schemas = []
        for f in info["all_files"]:
            pf = pq.ParquetFile(f)
            schemas.append(pf.schema_arrow.names)

        # All files should have the same columns
        first_schema = schemas[0]
        for schema in schemas[1:]:
            assert schema == first_schema

    def test_partition_geo_metadata_consistent(self, country_partition_dir):
        """Test that all partition files have consistent geo metadata."""
        import json

        info = get_partition_info(country_partition_dir)
        versions = []
        for f in info["all_files"]:
            pf = pq.ParquetFile(f)
            metadata = pf.schema_arrow.metadata
            if b"geo" in metadata:
                geo = json.loads(metadata[b"geo"].decode())
                versions.append(geo.get("version"))

        # All files should have the same GeoParquet version
        assert len(set(versions)) == 1
        assert versions[0] == "1.1.0"
