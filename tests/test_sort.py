"""
Tests for sort commands.
"""

import os

import duckdb
from click.testing import CliRunner

from geoparquet_io.cli.main import sort


class TestSortCommands:
    """Test suite for sort commands."""

    def test_hilbert_sort_places(self, places_test_file, temp_output_file):
        """Test Hilbert sort on places file."""
        runner = CliRunner()
        result = runner.invoke(sort, ["hilbert", places_test_file, temp_output_file])
        assert result.exit_code == 0
        # Verify output file was created
        assert os.path.exists(temp_output_file)

        # Verify row count matches
        conn = duckdb.connect()
        input_count = conn.execute(f'SELECT COUNT(*) FROM "{places_test_file}"').fetchone()[0]
        output_count = conn.execute(f'SELECT COUNT(*) FROM "{temp_output_file}"').fetchone()[0]
        assert input_count == output_count

    def test_hilbert_sort_buildings(self, buildings_test_file, temp_output_file):
        """Test Hilbert sort on buildings file."""
        runner = CliRunner()
        result = runner.invoke(sort, ["hilbert", buildings_test_file, temp_output_file])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify row count matches
        conn = duckdb.connect()
        input_count = conn.execute(f'SELECT COUNT(*) FROM "{buildings_test_file}"').fetchone()[0]
        output_count = conn.execute(f'SELECT COUNT(*) FROM "{temp_output_file}"').fetchone()[0]
        assert input_count == output_count

    def test_hilbert_sort_with_verbose(self, places_test_file, temp_output_file):
        """Test Hilbert sort with verbose flag."""
        runner = CliRunner()
        result = runner.invoke(sort, ["hilbert", places_test_file, temp_output_file, "--verbose"])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

    def test_hilbert_sort_with_custom_geometry_column(self, places_test_file, temp_output_file):
        """Test Hilbert sort with custom geometry column name."""
        runner = CliRunner()
        result = runner.invoke(
            sort, ["hilbert", places_test_file, temp_output_file, "--geometry-column", "geometry"]
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

    def test_hilbert_sort_with_add_bbox(self, buildings_test_file, temp_output_file):
        """Test Hilbert sort with add-bbox flag."""
        runner = CliRunner()
        result = runner.invoke(
            sort, ["hilbert", buildings_test_file, temp_output_file, "--add-bbox"]
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify bbox column was added
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "bbox" in column_names

    def test_hilbert_sort_preserves_columns_places(self, places_test_file, temp_output_file):
        """Test that Hilbert sort preserves all columns from places file."""
        runner = CliRunner()
        result = runner.invoke(sort, ["hilbert", places_test_file, temp_output_file])
        assert result.exit_code == 0

        # Verify columns are preserved
        conn = duckdb.connect()
        input_columns = conn.execute(f'DESCRIBE SELECT * FROM "{places_test_file}"').fetchall()
        output_columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()

        input_col_names = {col[0] for col in input_columns}
        output_col_names = {col[0] for col in output_columns}

        # All input columns should be in output
        assert input_col_names.issubset(output_col_names)

    def test_hilbert_sort_preserves_columns_buildings(self, buildings_test_file, temp_output_file):
        """Test that Hilbert sort preserves all columns from buildings file."""
        runner = CliRunner()
        result = runner.invoke(sort, ["hilbert", buildings_test_file, temp_output_file])
        assert result.exit_code == 0

        # Verify columns are preserved
        conn = duckdb.connect()
        input_columns = conn.execute(f'DESCRIBE SELECT * FROM "{buildings_test_file}"').fetchall()
        output_columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()

        input_col_names = {col[0] for col in input_columns}
        output_col_names = {col[0] for col in output_columns}

        # All input columns should be in output
        assert input_col_names.issubset(output_col_names)

    def test_hilbert_sort_nonexistent_file(self, temp_output_file):
        """Test Hilbert sort on nonexistent file."""
        runner = CliRunner()
        result = runner.invoke(sort, ["hilbert", "nonexistent.parquet", temp_output_file])
        # Should fail with non-zero exit code
        assert result.exit_code != 0
