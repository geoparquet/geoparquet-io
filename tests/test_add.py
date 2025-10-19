"""
Tests for add commands.
"""

import os

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_tools.cli.main import add


class TestAddCommands:
    """Test suite for add commands."""

    def test_add_bbox_to_buildings(self, buildings_test_file, temp_output_file):
        """Test adding bbox column to buildings file (which doesn't have bbox)."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", buildings_test_file, temp_output_file])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify bbox column was added
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "bbox" in column_names

        # Verify row count matches
        input_count = conn.execute(f'SELECT COUNT(*) FROM "{buildings_test_file}"').fetchone()[0]
        output_count = conn.execute(f'SELECT COUNT(*) FROM "{temp_output_file}"').fetchone()[0]
        assert input_count == output_count

        # Verify bbox structure
        bbox_col = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        bbox_info = [col for col in bbox_col if col[0] == "bbox"][0]
        assert "STRUCT" in bbox_info[1]

    def test_add_bbox_to_places(self, places_test_file, temp_output_file):
        """Test adding bbox column to places file (which already has bbox)."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", places_test_file, temp_output_file])
        # Should fail because bbox column already exists
        assert result.exit_code != 0
        assert "already exists" in result.output

    def test_add_bbox_with_custom_name(self, buildings_test_file, temp_output_file):
        """Test adding bbox column with custom name."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["bbox", buildings_test_file, temp_output_file, "--bbox-name", "bounds"]
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify custom bbox column name was used
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "bounds" in column_names

    def test_add_bbox_with_verbose(self, buildings_test_file, temp_output_file):
        """Test adding bbox column with verbose flag."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", buildings_test_file, temp_output_file, "--verbose"])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

    def test_add_bbox_preserves_columns(self, buildings_test_file, temp_output_file):
        """Test that add bbox preserves all original columns."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", buildings_test_file, temp_output_file])
        assert result.exit_code == 0

        # Verify columns are preserved
        conn = duckdb.connect()
        input_columns = conn.execute(f'DESCRIBE SELECT * FROM "{buildings_test_file}"').fetchall()
        output_columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()

        input_col_names = {col[0] for col in input_columns}
        output_col_names = {col[0] for col in output_columns}

        # All input columns should be in output
        assert input_col_names.issubset(output_col_names)
        # Output should have bbox column added
        assert "bbox" in output_col_names

    def test_add_bbox_nonexistent_file(self, temp_output_file):
        """Test add bbox on nonexistent file."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", "nonexistent.parquet", temp_output_file])
        # Should fail with non-zero exit code
        assert result.exit_code != 0

    # Note: add admin-divisions tests are skipped because they require a countries file
    # and network access. These should be tested separately with appropriate test data.
    @pytest.mark.skip(reason="Requires countries file and network access")
    def test_add_admin_divisions(self, places_test_file, temp_output_file):
        """Test adding admin divisions (skipped - requires countries file)."""
        pass
