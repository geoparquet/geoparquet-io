"""Tests for --overwrite behavior in add commands."""

from pathlib import Path

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli

# Sample test files
TEST_PARQUET = Path(__file__).parent / "data" / "fields_pgo_crs84_zstd.parquet"


class TestAddCommandsOverwrite:
    """Test --overwrite behavior for add commands.

    These tests verify that:
    1. Commands fail with helpful error when output exists (without --overwrite)
    2. The --overwrite flag is accepted by the CLI

    Note: We only test that --overwrite is accepted, not that the full operation
    completes, to keep tests fast. Full operation tests are in other test files.
    """

    def test_add_admin_divisions_fails_without_overwrite(self, tmp_path):
        """Test that add admin-divisions fails by default if output exists."""
        output_file = tmp_path / "output.parquet"
        output_file.write_text("existing content")

        runner = CliRunner()
        result = runner.invoke(cli, ["add", "admin-divisions", str(TEST_PARQUET), str(output_file)])

        assert result.exit_code != 0
        assert "already exists" in result.output or "Use --overwrite" in result.output

    @pytest.mark.slow
    def test_add_admin_divisions_with_overwrite(self, tmp_path):
        """Test that add admin-divisions works with --overwrite."""
        output_file = tmp_path / "output.parquet"
        output_file.write_text("existing content")

        runner = CliRunner()
        result = runner.invoke(
            cli, ["add", "admin-divisions", str(TEST_PARQUET), str(output_file), "--overwrite"]
        )

        # Should succeed with --overwrite
        assert result.exit_code == 0, f"Expected success with --overwrite, got: {result.output}"

    def test_add_bbox_fails_without_overwrite(self, tmp_path):
        """Test that add bbox fails by default if output exists."""
        output_file = tmp_path / "output.parquet"
        output_file.write_text("existing content")

        runner = CliRunner()
        result = runner.invoke(cli, ["add", "bbox", str(TEST_PARQUET), str(output_file)])

        assert result.exit_code != 0
        assert "already exists" in result.output or "Use --overwrite" in result.output

    @pytest.mark.slow
    def test_add_bbox_with_overwrite(self, tmp_path):
        """Test that add bbox works with --overwrite."""
        output_file = tmp_path / "output.parquet"
        output_file.write_text("existing content")

        runner = CliRunner()
        result = runner.invoke(
            cli, ["add", "bbox", str(TEST_PARQUET), str(output_file), "--overwrite"]
        )

        # Should succeed with --overwrite
        assert result.exit_code == 0, f"Expected success with --overwrite, got: {result.output}"

    def test_add_h3_fails_without_overwrite(self, tmp_path):
        """Test that add h3 fails by default if output exists."""
        output_file = tmp_path / "output.parquet"
        output_file.write_text("existing content")

        runner = CliRunner()
        result = runner.invoke(cli, ["add", "h3", str(TEST_PARQUET), str(output_file)])

        assert result.exit_code != 0
        assert "already exists" in result.output or "Use --overwrite" in result.output

    def test_add_kdtree_fails_without_overwrite(self, tmp_path):
        """Test that add kdtree fails by default if output exists."""
        output_file = tmp_path / "output.parquet"
        output_file.write_text("existing content")

        runner = CliRunner()
        result = runner.invoke(cli, ["add", "kdtree", str(TEST_PARQUET), str(output_file)])

        assert result.exit_code != 0
        assert "already exists" in result.output or "Use --overwrite" in result.output

    def test_add_quadkey_fails_without_overwrite(self, tmp_path):
        """Test that add quadkey fails by default if output exists."""
        output_file = tmp_path / "output.parquet"
        output_file.write_text("existing content")

        runner = CliRunner()
        result = runner.invoke(cli, ["add", "quadkey", str(TEST_PARQUET), str(output_file)])

        assert result.exit_code != 0
        assert "already exists" in result.output or "Use --overwrite" in result.output
