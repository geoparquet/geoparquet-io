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
