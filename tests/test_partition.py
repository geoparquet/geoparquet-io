"""
Tests for partition commands.
"""

import os

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import partition


class TestPartitionCommands:
    """Test suite for partition commands."""

    def test_partition_string_preview(self, places_test_file):
        """Test partition string command with preview mode."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            ["string", places_test_file, "--column", "fsq_place_id", "--chars", "1", "--preview"],
        )
        assert result.exit_code == 0
        # Preview should show partition information
        assert "partition" in result.output.lower() or "preview" in result.output.lower()

    def test_partition_string_by_column(self, places_test_file, temp_output_dir):
        """Test partition string command by first character."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "string",
                places_test_file,
                temp_output_dir,
                "--column",
                "fsq_place_id",
                "--chars",
                "1",
            ],
        )
        assert result.exit_code == 0
        # Should have created partition files
        output_files = os.listdir(temp_output_dir)
        assert len(output_files) > 0
        # All files should be .parquet
        assert all(f.endswith(".parquet") for f in output_files)

    def test_partition_string_with_hive(self, places_test_file, temp_output_dir):
        """Test partition string command with Hive-style partitioning."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "string",
                places_test_file,
                temp_output_dir,
                "--column",
                "fsq_place_id",
                "--chars",
                "1",
                "--hive",
            ],
        )
        assert result.exit_code == 0
        # Should have created partition directories
        items = os.listdir(temp_output_dir)
        assert len(items) > 0

    def test_partition_string_with_verbose(self, places_test_file, temp_output_dir):
        """Test partition string command with verbose flag."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "string",
                places_test_file,
                temp_output_dir,
                "--column",
                "fsq_place_id",
                "--chars",
                "1",
                "--verbose",
            ],
        )
        assert result.exit_code == 0

    def test_partition_string_preview_with_limit(self, places_test_file):
        """Test partition string preview with custom limit."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "string",
                places_test_file,
                "--column",
                "fsq_place_id",
                "--chars",
                "2",
                "--preview",
                "--preview-limit",
                "5",
            ],
        )
        assert result.exit_code == 0

    def test_partition_string_no_output_folder(self, places_test_file):
        """Test partition string without output folder (should fail unless preview)."""
        runner = CliRunner()
        result = runner.invoke(partition, ["string", places_test_file, "--column", "fsq_place_id"])
        # Should fail because output folder is required without --preview
        assert result.exit_code != 0

    def test_partition_string_nonexistent_column(self, places_test_file, temp_output_dir):
        """Test partition string with nonexistent column."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            ["string", places_test_file, temp_output_dir, "--column", "nonexistent_column"],
        )
        # Should fail with non-zero exit code
        assert result.exit_code != 0

    # Admin partition tests - skip because test files don't have admin:country_code column
    @pytest.mark.skip(reason="Test files don't have admin:country_code column")
    def test_partition_admin_preview(self, places_test_file):
        """Test partition admin command with preview mode."""
        runner = CliRunner()
        runner.invoke(partition, ["admin", places_test_file, "--preview"])
        # Will fail because column doesn't exist, but testing command structure
        pass

    def test_partition_admin_no_output_folder(self, places_test_file):
        """Test partition admin without output folder (should fail unless preview)."""
        runner = CliRunner()
        result = runner.invoke(partition, ["admin", places_test_file])
        # Should fail because output folder is required without --preview
        assert result.exit_code != 0

    # H3 partition tests
    def test_partition_h3_preview(self, buildings_test_file):
        """Test partition h3 command with preview mode."""
        runner = CliRunner()
        result = runner.invoke(
            partition, ["h3", buildings_test_file, "--resolution", "9", "--preview"]
        )
        assert result.exit_code == 0
        # Preview should show partition information
        assert "Partition Preview" in result.output
        assert "Total partitions:" in result.output
        assert "Total records:" in result.output

    def test_partition_h3_basic(self, buildings_test_file, temp_output_dir):
        """Test partition h3 command with auto-add H3 column."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            ["h3", buildings_test_file, temp_output_dir, "--resolution", "9", "--skip-analysis"],
        )
        assert result.exit_code == 0
        # Should have created partition files
        output_files = os.listdir(temp_output_dir)
        assert len(output_files) > 0
        # All files should be .parquet
        assert all(f.endswith(".parquet") for f in output_files)
        # H3 cell IDs are always 15 characters regardless of resolution
        assert all(len(f.replace(".parquet", "")) == 15 for f in output_files)

    def test_partition_h3_custom_resolution(self, buildings_test_file, temp_output_dir):
        """Test partition h3 with custom resolution."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            ["h3", buildings_test_file, temp_output_dir, "--resolution", "7", "--skip-analysis"],
        )
        assert result.exit_code == 0
        # Should have created partition files
        output_files = os.listdir(temp_output_dir)
        assert len(output_files) > 0
        # H3 cell IDs are always 15 characters regardless of resolution
        assert all(len(f.replace(".parquet", "")) == 15 for f in output_files)

    def test_partition_h3_with_hive(self, buildings_test_file, temp_output_dir):
        """Test partition h3 command with Hive-style partitioning."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "h3",
                buildings_test_file,
                temp_output_dir,
                "--resolution",
                "9",
                "--hive",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0
        # Should have created partition directories
        items = os.listdir(temp_output_dir)
        assert len(items) > 0

    def test_partition_h3_with_verbose(self, buildings_test_file, temp_output_dir):
        """Test partition h3 command with verbose flag."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "h3",
                buildings_test_file,
                temp_output_dir,
                "--resolution",
                "9",
                "--verbose",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0
        assert "H3 column" in result.output
        assert "Adding it now" in result.output or "Using existing" in result.output

    def test_partition_h3_preview_with_limit(self, buildings_test_file):
        """Test partition h3 preview with custom limit."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "h3",
                buildings_test_file,
                "--resolution",
                "9",
                "--preview",
                "--preview-limit",
                "2",
            ],
        )
        assert result.exit_code == 0
        assert "Partition Preview" in result.output

    def test_partition_h3_no_output_folder(self, buildings_test_file):
        """Test partition h3 without output folder (should fail unless preview)."""
        runner = CliRunner()
        result = runner.invoke(partition, ["h3", buildings_test_file, "--resolution", "9"])
        # Should fail because output folder is required without --preview
        assert result.exit_code != 0

    def test_partition_h3_custom_column_name(self, buildings_test_file, temp_output_dir):
        """Test partition h3 with custom H3 column name."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "h3",
                buildings_test_file,
                temp_output_dir,
                "--h3-name",
                "custom_h3",
                "--resolution",
                "9",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0
        output_files = os.listdir(temp_output_dir)
        assert len(output_files) > 0

    def test_partition_h3_invalid_resolution(self, buildings_test_file, temp_output_dir):
        """Test partition h3 with invalid resolution."""
        runner = CliRunner()
        result = runner.invoke(
            partition, ["h3", buildings_test_file, temp_output_dir, "--resolution", "16"]
        )
        # Should fail with invalid resolution
        assert result.exit_code != 0

    def test_partition_h3_excludes_column_by_default(self, buildings_test_file, temp_output_dir):
        """Test that H3 column is excluded from output by default (non-Hive)."""
        import pyarrow.parquet as pq

        runner = CliRunner()
        result = runner.invoke(
            partition,
            ["h3", buildings_test_file, temp_output_dir, "--resolution", "9", "--skip-analysis"],
        )
        assert result.exit_code == 0

        # Check that output files exist
        output_files = [f for f in os.listdir(temp_output_dir) if f.endswith(".parquet")]
        assert len(output_files) > 0

        # Check that H3 column is NOT in the output files
        sample_file = os.path.join(temp_output_dir, output_files[0])
        table = pq.read_table(sample_file)
        column_names = table.schema.names
        assert "h3_cell" not in column_names, "H3 column should be excluded by default"

    def test_partition_h3_keeps_column_with_flag(self, buildings_test_file, temp_output_dir):
        """Test that H3 column is kept when --keep-h3-column flag is used."""
        import pyarrow.parquet as pq

        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "h3",
                buildings_test_file,
                temp_output_dir,
                "--resolution",
                "9",
                "--keep-h3-column",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0

        # Check that output files exist
        output_files = [f for f in os.listdir(temp_output_dir) if f.endswith(".parquet")]
        assert len(output_files) > 0

        # Check that H3 column IS in the output files
        sample_file = os.path.join(temp_output_dir, output_files[0])
        table = pq.read_table(sample_file)
        column_names = table.schema.names
        assert "h3_cell" in column_names, "H3 column should be kept with --keep-h3-column flag"

    def test_partition_h3_hive_keeps_column_by_default(self, buildings_test_file, temp_output_dir):
        """Test that H3 column is kept by default when using Hive partitioning."""
        import pyarrow.parquet as pq

        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "h3",
                buildings_test_file,
                temp_output_dir,
                "--resolution",
                "9",
                "--hive",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0

        # Find a parquet file in the Hive-style directory structure
        hive_dirs = [
            d
            for d in os.listdir(temp_output_dir)
            if os.path.isdir(os.path.join(temp_output_dir, d))
        ]
        assert len(hive_dirs) > 0

        # Find a parquet file in one of the partition directories
        sample_dir = os.path.join(temp_output_dir, hive_dirs[0])
        parquet_files = [f for f in os.listdir(sample_dir) if f.endswith(".parquet")]
        assert len(parquet_files) > 0

        # Check that H3 column IS in the output files (default for Hive)
        sample_file = os.path.join(sample_dir, parquet_files[0])
        # Use ParquetFile to read single file without auto-detecting Hive partitioning
        pf = pq.ParquetFile(sample_file)
        column_names = pf.schema_arrow.names
        assert "h3_cell" in column_names, (
            "H3 column should be kept by default for Hive partitioning"
        )

    # Prefix tests
    def test_partition_string_with_prefix(self, places_test_file, temp_output_dir):
        """Test partition string command with custom filename prefix."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "string",
                places_test_file,
                temp_output_dir,
                "--column",
                "fsq_place_id",
                "--chars",
                "1",
                "--prefix",
                "places",
            ],
        )
        assert result.exit_code == 0
        # Should have created partition files with prefix
        output_files = os.listdir(temp_output_dir)
        assert len(output_files) > 0
        # All files should start with "places_" and end with ".parquet"
        assert all(f.startswith("places_") and f.endswith(".parquet") for f in output_files)

    def test_partition_h3_with_prefix(self, buildings_test_file, temp_output_dir):
        """Test partition h3 command with custom filename prefix."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "h3",
                buildings_test_file,
                temp_output_dir,
                "--resolution",
                "9",
                "--prefix",
                "buildings",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0
        # Should have created partition files with prefix
        output_files = os.listdir(temp_output_dir)
        assert len(output_files) > 0
        # All files should start with "buildings_" and end with ".parquet"
        assert all(f.startswith("buildings_") and f.endswith(".parquet") for f in output_files)
        # Pattern should be: buildings_<h3_cell>.parquet
        for f in output_files:
            # Remove prefix and .parquet extension to get H3 cell
            h3_cell = f.replace("buildings_", "").replace(".parquet", "")
            # H3 cell IDs are always 15 characters regardless of resolution
            assert len(h3_cell) == 15, f"Expected 15-char H3 cell, got {h3_cell}"

    def test_partition_string_with_prefix_and_hive(self, places_test_file, temp_output_dir):
        """Test partition string with prefix and Hive-style partitioning."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "string",
                places_test_file,
                temp_output_dir,
                "--column",
                "fsq_place_id",
                "--chars",
                "1",
                "--prefix",
                "places",
                "--hive",
            ],
        )
        assert result.exit_code == 0

        # Find Hive partition directory and check files have prefix
        items = os.listdir(temp_output_dir)
        hive_dirs = [d for d in items if os.path.isdir(os.path.join(temp_output_dir, d))]
        sample_dir = os.path.join(temp_output_dir, hive_dirs[0])
        parquet_files = [f for f in os.listdir(sample_dir) if f.endswith(".parquet")]
        assert all(f.startswith("places_") for f in parquet_files)
