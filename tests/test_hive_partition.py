"""
Tests for Hive-style partitioning in GeoParquet tools.
These tests verify that Hive partitioning works correctly and produces valid output.
"""

import json
import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli


@pytest.fixture
def sample_parquet():
    """Create a sample GeoParquet file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_name = tmp.name

    # Create valid WKB for POINT(0 0)
    wkb_point = bytes.fromhex("0101000000000000000000000000000000000000000000000000000000")

    # Create test data with multiple categories
    table = pa.table(
        {
            "id": ["1", "2", "3", "4", "5", "6"],
            "category": ["A", "A", "B", "B", "C", "C"],
            "region": ["north", "north", "south", "south", "east", "east"],
            "geometry": [wkb_point] * 6,
        }
    )

    # Add GeoParquet metadata
    metadata = {
        b"geo": json.dumps(
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
            }
        ).encode("utf-8")
    }

    table = table.replace_schema_metadata(metadata)
    pq.write_table(table, tmp_name)

    yield tmp_name

    # Cleanup
    try:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
    except PermissionError:
        # On Windows, file might still be locked - try again after a short delay
        import gc
        import time

        gc.collect()  # Force garbage collection to close file handles
        time.sleep(0.1)
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for output."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


class TestHivePartitioning:
    """Test Hive-style partitioning functionality."""

    def test_string_partition_hive(self, sample_parquet, temp_dir):
        """Test string partitioning with Hive-style output."""
        runner = CliRunner()

        # Run partition command with Hive style
        result = runner.invoke(
            cli,
            [
                "partition",
                "string",
                sample_parquet,
                temp_dir,
                "--column",
                "category",
                "--hive",
                "--skip-analysis",
            ],
        )

        assert result.exit_code == 0, f"Command failed: {result.output}"

        # Check that Hive-style directories were created
        expected_dirs = ["category=A", "category=B", "category=C"]
        for dir_name in expected_dirs:
            dir_path = os.path.join(temp_dir, dir_name)
            assert os.path.isdir(dir_path), f"Expected directory {dir_path} not found"

            # Check that parquet files exist in each directory
            files = os.listdir(dir_path)
            parquet_files = [f for f in files if f.endswith(".parquet")]
            assert len(parquet_files) > 0, f"No parquet files in {dir_path}"

    def test_hive_partition_readable_as_dataset(self, sample_parquet, temp_dir):
        """Test that Hive-partitioned output can be read as a PyArrow dataset."""
        runner = CliRunner()

        # Run partition command with Hive style
        result = runner.invoke(
            cli,
            [
                "partition",
                "string",
                sample_parquet,
                temp_dir,
                "--column",
                "category",
                "--hive",
                "--skip-analysis",
            ],
        )

        assert result.exit_code == 0, f"Command failed: {result.output}"

        # Try to read the entire directory as a dataset
        try:
            dataset = ds.dataset(temp_dir, format="parquet", partitioning="hive")
            table = dataset.to_table()

            # Verify we got all the data
            assert len(table) == 6, f"Expected 6 rows, got {len(table)}"

            # Verify the partition column is present and correct
            categories = table.column("category").to_pylist()
            assert set(categories) == {"A", "B", "C"}, f"Unexpected categories: {categories}"

        except Exception as e:
            pytest.fail(f"Failed to read Hive-partitioned dataset: {e}")

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows doesn't allow colons in directory names",
    )
    def test_hive_partition_with_special_column(self, temp_dir):
        """Test Hive partitioning with special column names like 'admin:country_code'."""
        # Create test data with special column name
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            wkb_point = bytes.fromhex("0101000000000000000000000000000000000000000000000000000000")

            table = pa.table(
                {
                    "id": ["1", "2", "3", "4"],
                    "admin:country_code": ["US", "US", "CA", "CA"],
                    "geometry": [wkb_point] * 4,
                }
            )

            # Add GeoParquet metadata
            metadata = {
                b"geo": json.dumps(
                    {
                        "version": "1.1.0",
                        "primary_column": "geometry",
                        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
                    }
                ).encode("utf-8")
            }

            table = table.replace_schema_metadata(metadata)
            pq.write_table(table, tmp_name)

            runner = CliRunner()

            # Run partition command with Hive style
            # Note: partition admin now does spatial join, so use partition string for existing columns
            result = runner.invoke(
                cli,
                [
                    "partition",
                    "string",
                    tmp_name,
                    temp_dir,
                    "--column",
                    "admin:country_code",
                    "--hive",
                    "--skip-analysis",
                ],
            )

            assert result.exit_code == 0, f"Command failed: {result.output}"

            # Check that Hive-style directories were created
            expected_dirs = ["admin:country_code=US", "admin:country_code=CA"]
            for dir_name in expected_dirs:
                dir_path = os.path.join(temp_dir, dir_name)
                assert os.path.isdir(dir_path), f"Expected directory {dir_path} not found"

        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def test_non_hive_partition_still_works(self, sample_parquet, temp_dir):
        """Test that non-Hive partitioning still works correctly."""
        runner = CliRunner()

        # Run partition command WITHOUT Hive style
        result = runner.invoke(
            cli,
            [
                "partition",
                "string",
                sample_parquet,
                temp_dir,
                "--column",
                "category",
                "--skip-analysis",
            ],
        )

        assert result.exit_code == 0, f"Command failed: {result.output}"

        # Check that flat files were created (not in subdirectories)
        expected_files = ["A.parquet", "B.parquet", "C.parquet"]
        for file_name in expected_files:
            file_path = os.path.join(temp_dir, file_name)
            assert os.path.isfile(file_path), f"Expected file {file_path} not found"

            # Verify each file is valid GeoParquet
            table = pq.read_table(file_path)
            assert len(table) == 2, f"Expected 2 rows in {file_name}, got {len(table)}"

    def test_hive_partition_preserves_geoparquet_metadata(self, sample_parquet, temp_dir):
        """Test that Hive partitioning preserves GeoParquet metadata."""
        runner = CliRunner()

        # Run partition command with Hive style
        result = runner.invoke(
            cli,
            [
                "partition",
                "string",
                sample_parquet,
                temp_dir,
                "--column",
                "category",
                "--hive",
                "--skip-analysis",
            ],
        )

        assert result.exit_code == 0, f"Command failed: {result.output}"

        # Check metadata in one of the partition files
        partition_file = os.path.join(temp_dir, "category=A", "A.parquet")
        assert os.path.exists(partition_file), f"Partition file not found: {partition_file}"

        # Read metadata
        with open(partition_file, "rb") as f:
            pf = pq.ParquetFile(f)
            metadata = pf.schema_arrow.metadata

            assert b"geo" in metadata, "GeoParquet metadata not found"

            geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
            assert geo_meta["version"] == "1.1.0", (
                f"Wrong GeoParquet version: {geo_meta['version']}"
            )
            assert geo_meta["primary_column"] == "geometry", "Wrong primary column"

            # Check compression (should be ZSTD)
            if pf.num_row_groups > 0:
                row_group = pf.metadata.row_group(0)
                # Find geometry column
                for i in range(row_group.num_columns):
                    if "geometry" in pf.schema_arrow.field(i).name:
                        col_meta = row_group.column(i)
                        compression = str(col_meta.compression)
                        assert "ZSTD" in compression.upper(), (
                            f"Expected ZSTD compression, got {compression}"
                        )
                        break


class TestPartitionStringWithChars:
    """Test partitioning by character prefix."""

    def test_partition_by_prefix_hive(self, temp_dir):
        """Test partitioning by character prefix with Hive style."""
        # Create test data with longer strings
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            wkb_point = bytes.fromhex("0101000000000000000000000000000000000000000000000000000000")

            table = pa.table(
                {
                    "id": ["1", "2", "3", "4", "5", "6"],
                    "code": ["ABC123", "ABC456", "DEF789", "DEF012", "GHI345", "GHI678"],
                    "geometry": [wkb_point] * 6,
                }
            )

            # Add GeoParquet metadata
            metadata = {
                b"geo": json.dumps(
                    {
                        "version": "1.1.0",
                        "primary_column": "geometry",
                        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
                    }
                ).encode("utf-8")
            }

            table = table.replace_schema_metadata(metadata)
            pq.write_table(table, tmp_name)

            runner = CliRunner()

            # Partition by first 3 characters
            result = runner.invoke(
                cli,
                [
                    "partition",
                    "string",
                    tmp_name,
                    temp_dir,
                    "--column",
                    "code",
                    "--chars",
                    "3",
                    "--hive",
                    "--skip-analysis",
                ],
            )

            assert result.exit_code == 0, f"Command failed: {result.output}"

            # Check that correct directories were created
            expected_dirs = ["code_prefix=ABC", "code_prefix=DEF", "code_prefix=GHI"]
            for dir_name in expected_dirs:
                dir_path = os.path.join(temp_dir, dir_name)
                assert os.path.isdir(dir_path), f"Expected directory {dir_path} not found"

                # Each partition should have 2 records
                files = [f for f in os.listdir(dir_path) if f.endswith(".parquet")]
                assert len(files) > 0, f"No parquet files in {dir_path}"

                table = pq.read_table(os.path.join(dir_path, files[0]))
                assert len(table) == 2, f"Expected 2 rows in partition, got {len(table)}"

        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)


class TestPartitionFormatCompliance:
    """Test that all partition commands produce compliant GeoParquet 1.1.0 format."""

    def test_string_partition_format_compliance(self, sample_parquet, temp_dir):
        """Test that string partition produces GeoParquet 1.1.0 with ZSTD compression."""
        runner = CliRunner()

        # Run partition command
        result = runner.invoke(
            cli,
            [
                "partition",
                "string",
                sample_parquet,
                temp_dir,
                "--column",
                "category",
                "--hive",
                "--skip-analysis",
            ],
        )

        assert result.exit_code == 0, f"Command failed: {result.output}"

        # Check each partition file
        for dir_name in os.listdir(temp_dir):
            if os.path.isdir(os.path.join(temp_dir, dir_name)):
                for file_name in os.listdir(os.path.join(temp_dir, dir_name)):
                    if file_name.endswith(".parquet"):
                        file_path = os.path.join(temp_dir, dir_name, file_name)
                        self._verify_geoparquet_format(file_path)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows doesn't allow colons in directory names",
    )
    def test_admin_partition_format_compliance(self, temp_dir):
        """Test that admin partition produces GeoParquet 1.1.0 with ZSTD compression."""
        # Create test data with admin:country_code column
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            wkb_point = bytes.fromhex("0101000000000000000000000000000000000000000000000000000000")

            table = pa.table(
                {
                    "id": ["1", "2", "3", "4"],
                    "admin:country_code": ["US", "US", "CA", "CA"],
                    "geometry": [wkb_point] * 4,
                }
            )

            # Add GeoParquet metadata
            metadata = {
                b"geo": json.dumps(
                    {
                        "version": "1.1.0",
                        "primary_column": "geometry",
                        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
                    }
                ).encode("utf-8")
            }

            table = table.replace_schema_metadata(metadata)
            pq.write_table(table, tmp_name)

            runner = CliRunner()

            # Run admin partition command (now use partition string for existing column)
            result = runner.invoke(
                cli,
                [
                    "partition",
                    "string",
                    tmp_name,
                    temp_dir,
                    "--column",
                    "admin:country_code",
                    "--hive",
                    "--skip-analysis",
                ],
            )

            assert result.exit_code == 0, f"Command failed: {result.output}"

            # Check each partition file
            for dir_name in os.listdir(temp_dir):
                if os.path.isdir(os.path.join(temp_dir, dir_name)):
                    for file_name in os.listdir(os.path.join(temp_dir, dir_name)):
                        if file_name.endswith(".parquet"):
                            file_path = os.path.join(temp_dir, dir_name, file_name)
                            self._verify_geoparquet_format(file_path)

        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def test_non_hive_partition_format_compliance(self, sample_parquet, temp_dir):
        """Test that non-Hive partition also produces GeoParquet 1.1.0 with ZSTD compression."""
        runner = CliRunner()

        # Run partition command without Hive style
        result = runner.invoke(
            cli,
            [
                "partition",
                "string",
                sample_parquet,
                temp_dir,
                "--column",
                "category",
                "--skip-analysis",
            ],
        )

        assert result.exit_code == 0, f"Command failed: {result.output}"

        # Check each partition file (should be flat files in temp_dir)
        for file_name in os.listdir(temp_dir):
            if file_name.endswith(".parquet"):
                file_path = os.path.join(temp_dir, file_name)
                self._verify_geoparquet_format(file_path)

    def _verify_geoparquet_format(self, file_path):
        """Verify that a file conforms to GeoParquet 1.1.0 format requirements."""
        with open(file_path, "rb") as f:
            pf = pq.ParquetFile(f)
            metadata = pf.schema_arrow.metadata

            # Check for GeoParquet metadata
            assert b"geo" in metadata, f"GeoParquet metadata not found in {file_path}"

            geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))

            # Check version is 1.1.0
            assert geo_meta["version"] == "1.1.0", (
                f"Wrong GeoParquet version in {file_path}: {geo_meta['version']}"
            )

            # Check primary column
            assert "primary_column" in geo_meta, f"Missing primary_column in {file_path}"
            assert geo_meta["primary_column"] == "geometry", f"Wrong primary column in {file_path}"

            # Check geometry column metadata
            assert "columns" in geo_meta, f"Missing columns metadata in {file_path}"
            assert "geometry" in geo_meta["columns"], (
                f"Missing geometry column metadata in {file_path}"
            )

            geom_meta = geo_meta["columns"]["geometry"]
            assert geom_meta["encoding"] == "WKB", f"Wrong geometry encoding in {file_path}"

            # Check compression (should be ZSTD)
            if pf.num_row_groups > 0:
                row_group = pf.metadata.row_group(0)
                # Find geometry column
                for i in range(row_group.num_columns):
                    col_meta = row_group.column(i)
                    compression = str(col_meta.compression)
                    # All columns should use ZSTD compression
                    assert "ZSTD" in compression.upper(), (
                        f"Expected ZSTD compression in {file_path}, got {compression}"
                    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
