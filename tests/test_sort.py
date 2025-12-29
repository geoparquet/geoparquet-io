"""
Tests for sort commands.
"""

import io
import os
import sys
import tempfile
import uuid
from pathlib import Path
from unittest import mock

import duckdb
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import sort
from geoparquet_io.core.sort_by_column import sort_by_column, sort_by_column_table
from tests.conftest import safe_unlink


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


class TestSortColumnCommands:
    """Test suite for column sort commands."""

    def test_column_sort_single(self, places_test_file, temp_output_file):
        """Test sorting by a single column."""
        runner = CliRunner()
        # Get a column name from the file first
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{places_test_file}"').fetchall()
        # Find a non-geometry column
        test_column = None
        for col in columns:
            if col[0] != "geometry":
                test_column = col[0]
                break
        conn.close()

        assert test_column is not None, "No non-geometry columns found"

        result = runner.invoke(sort, ["column", places_test_file, temp_output_file, test_column])
        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert os.path.exists(temp_output_file)

        # Verify row count matches
        conn = duckdb.connect()
        input_count = conn.execute(f'SELECT COUNT(*) FROM "{places_test_file}"').fetchone()[0]
        output_count = conn.execute(f'SELECT COUNT(*) FROM "{temp_output_file}"').fetchone()[0]
        assert input_count == output_count
        conn.close()

    def test_column_sort_descending(self, places_test_file, temp_output_file):
        """Test sorting in descending order."""
        runner = CliRunner()
        # Get a column name from the file first
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{places_test_file}"').fetchall()
        # Find a non-geometry column
        test_column = None
        for col in columns:
            if col[0] != "geometry":
                test_column = col[0]
                break
        conn.close()

        assert test_column is not None, "No non-geometry columns found"

        result = runner.invoke(
            sort, ["column", places_test_file, temp_output_file, test_column, "--descending"]
        )
        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert os.path.exists(temp_output_file)

    def test_column_sort_invalid_column(self, places_test_file, temp_output_file):
        """Test sorting by a column that doesn't exist."""
        runner = CliRunner()
        result = runner.invoke(
            sort, ["column", places_test_file, temp_output_file, "nonexistent_column"]
        )
        # Should fail because column doesn't exist
        assert result.exit_code != 0
        assert "nonexistent_column" in result.output

    def test_column_sort_preserves_columns(self, places_test_file, temp_output_file):
        """Test that column sort preserves all columns."""
        runner = CliRunner()
        # Get a column name from the file first
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{places_test_file}"').fetchall()
        # Find a non-geometry column
        test_column = None
        for col in columns:
            if col[0] != "geometry":
                test_column = col[0]
                break
        conn.close()

        assert test_column is not None, "No non-geometry columns found"

        result = runner.invoke(sort, ["column", places_test_file, temp_output_file, test_column])
        assert result.exit_code == 0, f"Failed with: {result.output}"

        # Verify columns are preserved
        conn = duckdb.connect()
        input_columns = conn.execute(f'DESCRIBE SELECT * FROM "{places_test_file}"').fetchall()
        output_columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()

        input_col_names = {col[0] for col in input_columns}
        output_col_names = {col[0] for col in output_columns}

        assert input_col_names == output_col_names
        conn.close()

    def test_column_sort_with_verbose(self, places_test_file, temp_output_file):
        """Test column sort with verbose flag."""
        runner = CliRunner()
        # Get a column name from the file first
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{places_test_file}"').fetchall()
        # Find a non-geometry column
        test_column = None
        for col in columns:
            if col[0] != "geometry":
                test_column = col[0]
                break
        conn.close()

        assert test_column is not None, "No non-geometry columns found"

        result = runner.invoke(
            sort, ["column", places_test_file, temp_output_file, test_column, "--verbose"]
        )
        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert os.path.exists(temp_output_file)


class TestSortByColumnTable:
    """Tests for sort_by_column_table function."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def sample_table(self, places_file):
        """Create a sample table from places test data."""
        return pq.read_table(places_file)

    def test_sort_single_column(self, sample_table):
        """Test sorting by single column."""
        result = sort_by_column_table(sample_table, columns="name")
        assert result.num_rows == sample_table.num_rows

    def test_sort_multiple_columns(self, sample_table):
        """Test sorting by multiple columns."""
        result = sort_by_column_table(sample_table, columns=["name", "address"])
        assert result.num_rows == sample_table.num_rows

    def test_sort_descending(self, sample_table):
        """Test sorting in descending order."""
        result = sort_by_column_table(sample_table, columns="name", descending=True)
        assert result.num_rows == sample_table.num_rows

    def test_sort_invalid_column(self, sample_table):
        """Test error with invalid column name."""
        with pytest.raises(ValueError, match="not found in table"):
            sort_by_column_table(sample_table, columns="nonexistent_column")

    def test_sort_empty_columns(self, sample_table):
        """Test error with empty columns."""
        with pytest.raises(ValueError, match="not found in table"):
            sort_by_column_table(sample_table, columns="")

    def test_sort_metadata_preserved(self, sample_table):
        """Test that GeoParquet metadata is preserved."""
        result = sort_by_column_table(sample_table, columns="name")
        if sample_table.schema.metadata and b"geo" in sample_table.schema.metadata:
            assert b"geo" in result.schema.metadata


class TestSortByColumnStreaming:
    """Tests for streaming sort_by_column."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def sample_geo_table(self, places_file):
        """Create a geo table from test data."""
        return pq.read_table(places_file)

    @pytest.fixture
    def output_file(self):
        """Create a temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_sort_column_stream_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_stdin_to_file(self, sample_geo_table, output_file, monkeypatch):
        """Test reading from mocked stdin."""
        # Create IPC buffer
        ipc_buffer = io.BytesIO()
        writer = ipc.RecordBatchStreamWriter(ipc_buffer, sample_geo_table.schema)
        writer.write_table(sample_geo_table)
        writer.close()
        ipc_buffer.seek(0)

        # Create a mock stdin with buffer attribute
        mock_stdin = mock.MagicMock()
        mock_stdin.isatty.return_value = False
        mock_stdin.buffer = ipc_buffer

        monkeypatch.setattr(sys, "stdin", mock_stdin)

        # Call function with "-" input
        sort_by_column("-", output_file, columns="name")

        # Verify output
        assert Path(output_file).exists()
        result = pq.read_table(output_file)
        assert result.num_rows == sample_geo_table.num_rows

    def test_file_to_stdout(self, places_file, monkeypatch):
        """Test writing to mocked stdout."""
        output_buffer = io.BytesIO()
        mock_stdout = mock.MagicMock()
        mock_stdout.buffer = output_buffer
        mock_stdout.isatty.return_value = False
        monkeypatch.setattr(sys, "stdout", mock_stdout)

        # Call function with "-" output
        sort_by_column(places_file, "-", columns="name")

        # Verify stream
        output_buffer.seek(0)
        reader = ipc.RecordBatchStreamReader(output_buffer)
        result = reader.read_all()
        assert result.num_rows > 0
