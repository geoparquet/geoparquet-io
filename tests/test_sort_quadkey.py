"""Tests for sort_quadkey module."""

import io
import sys
import tempfile
import uuid
from pathlib import Path
from unittest import mock

import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.core.sort_quadkey import sort_by_quadkey, sort_by_quadkey_table
from tests.conftest import safe_unlink


class TestSortQuadkeyCommand:
    """Tests for the sort quadkey CLI command."""

    @pytest.fixture
    def sample_file(self):
        """Return path to the sample file."""
        return str(Path(__file__).parent / "data" / "sample.parquet")

    @pytest.fixture
    def output_file(self):
        """Create a temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_sort_quadkey_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_sort_quadkey_help(self):
        """Test that quadkey sort command has help."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["sort", "quadkey", "--help"])
        assert result.exit_code == 0
        assert "quadkey" in result.output.lower()

    def test_sort_quadkey_missing_column_custom_name(self, sample_file, output_file):
        """Test error when custom quadkey column name doesn't exist."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["sort", "quadkey", sample_file, output_file, "--quadkey-name", "nonexistent_column"],
        )
        assert result.exit_code != 0
        assert "not found" in result.output.lower() or "error" in result.output.lower()

    def test_sort_quadkey_invalid_resolution(self, sample_file, output_file):
        """Test with invalid resolution parameter."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(
            cli, ["sort", "quadkey", sample_file, output_file, "--resolution", "30"]
        )
        # Should fail validation
        assert result.exit_code != 0


class TestSortByQuadkeyTable:
    """Tests for sort_by_quadkey_table function."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def sample_table(self, places_file):
        """Create a sample table from places test data."""
        return pq.read_table(places_file)

    def test_sort_auto_add_quadkey_column(self, sample_table):
        """Test sorting when quadkey column doesn't exist (auto-added)."""
        result = sort_by_quadkey_table(sample_table, resolution=10)
        assert "quadkey" in result.column_names
        assert result.num_rows == sample_table.num_rows

    def test_sort_with_custom_resolution(self, sample_table):
        """Test with custom resolution parameter."""
        result = sort_by_quadkey_table(sample_table, resolution=8)
        assert "quadkey" in result.column_names
        assert result.num_rows == sample_table.num_rows

    def test_sort_remove_quadkey_column(self, sample_table):
        """Test removing quadkey column after sorting."""
        result = sort_by_quadkey_table(sample_table, resolution=10, remove_quadkey_column=True)
        assert "quadkey" not in result.column_names
        assert result.num_rows == sample_table.num_rows

    def test_sort_with_existing_quadkey(self, sample_table):
        """Test sorting when quadkey column already exists."""
        # First add a quadkey column
        table_with_qk = sort_by_quadkey_table(sample_table, resolution=10)
        # Then sort again using existing column
        result = sort_by_quadkey_table(table_with_qk, quadkey_column_name="quadkey")
        assert "quadkey" in result.column_names
        assert result.num_rows == sample_table.num_rows

    def test_metadata_preserved(self, sample_table):
        """Test that GeoParquet metadata is preserved."""
        result = sort_by_quadkey_table(sample_table, resolution=10)
        # Check that geo metadata is preserved
        if sample_table.schema.metadata and b"geo" in sample_table.schema.metadata:
            assert b"geo" in result.schema.metadata

    def test_sort_with_use_centroid(self, sample_table):
        """Test sorting with use_centroid=True."""
        result = sort_by_quadkey_table(sample_table, resolution=10, use_centroid=True)
        assert "quadkey" in result.column_names
        assert result.num_rows == sample_table.num_rows


class TestSortByQuadkeyFile:
    """Tests for file-based sort_by_quadkey function."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def output_file(self):
        """Create a temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_sort_quadkey_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_sort_basic(self, places_file, output_file):
        """Test basic file sorting."""
        sort_by_quadkey(places_file, output_file, resolution=10)
        assert Path(output_file).exists()
        result = pq.read_table(output_file)
        assert "quadkey" in result.column_names
        assert result.num_rows == 766

    def test_sort_remove_quadkey(self, places_file, output_file):
        """Test removing quadkey after sort."""
        sort_by_quadkey(places_file, output_file, resolution=10, remove_quadkey_column=True)
        assert Path(output_file).exists()
        result = pq.read_table(output_file)
        assert "quadkey" not in result.column_names
        assert result.num_rows == 766


class TestSortByQuadkeyStreaming:
    """Tests for streaming mode."""

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
        tmp_path = Path(tempfile.gettempdir()) / f"test_sort_quadkey_stream_{uuid.uuid4()}.parquet"
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
        sort_by_quadkey("-", output_file, resolution=10)

        # Verify output
        assert Path(output_file).exists()
        result = pq.read_table(output_file)
        assert "quadkey" in result.column_names
        assert result.num_rows == sample_geo_table.num_rows

    def test_file_to_stdout(self, places_file, monkeypatch):
        """Test writing to mocked stdout."""
        output_buffer = io.BytesIO()
        mock_stdout = mock.MagicMock()
        mock_stdout.buffer = output_buffer
        mock_stdout.isatty.return_value = False
        monkeypatch.setattr(sys, "stdout", mock_stdout)

        # Call function with "-" output
        sort_by_quadkey(places_file, "-", resolution=10)

        # Verify stream
        output_buffer.seek(0)
        reader = ipc.RecordBatchStreamReader(output_buffer)
        result = reader.read_all()
        assert result.num_rows > 0
        assert "quadkey" in result.column_names
