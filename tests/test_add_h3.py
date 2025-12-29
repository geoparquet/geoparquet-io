"""Tests for add_h3_column module."""

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

from geoparquet_io.core.add_h3_column import add_h3_column, add_h3_table
from tests.conftest import safe_unlink


class TestAddH3Table:
    """Tests for add_h3_table function."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def sample_table(self, places_file):
        """Create a sample table from places test data."""
        return pq.read_table(places_file)

    def test_add_h3_basic(self, sample_table):
        """Test basic H3 column addition."""
        result = add_h3_table(sample_table, resolution=9)
        assert "h3_cell" in result.column_names
        assert result.num_rows == sample_table.num_rows

    def test_add_h3_custom_column_name(self, sample_table):
        """Test with custom column name."""
        result = add_h3_table(sample_table, h3_column_name="my_h3", resolution=9)
        assert "my_h3" in result.column_names
        assert result.num_rows == sample_table.num_rows

    def test_add_h3_different_resolutions(self, sample_table):
        """Test different resolution levels."""
        for resolution in [5, 9, 12]:
            result = add_h3_table(sample_table, resolution=resolution)
            assert "h3_cell" in result.column_names
            assert result.num_rows == sample_table.num_rows

    def test_add_h3_invalid_resolution_low(self, sample_table):
        """Test error with resolution too low."""
        with pytest.raises(ValueError, match="resolution must be between"):
            add_h3_table(sample_table, resolution=-1)

    def test_add_h3_invalid_resolution_high(self, sample_table):
        """Test error with resolution too high."""
        with pytest.raises(ValueError, match="resolution must be between"):
            add_h3_table(sample_table, resolution=16)

    def test_add_h3_metadata_preserved(self, sample_table):
        """Test that GeoParquet metadata is preserved."""
        result = add_h3_table(sample_table, resolution=9)
        # Check that geo metadata is preserved
        if sample_table.schema.metadata and b"geo" in sample_table.schema.metadata:
            assert b"geo" in result.schema.metadata


class TestAddH3File:
    """Tests for file-based add_h3_column function."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def output_file(self):
        """Create a temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_add_h3_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_add_h3_file_basic(self, places_file, output_file):
        """Test basic file-to-file H3 addition."""
        add_h3_column(places_file, output_file, h3_resolution=9)
        assert Path(output_file).exists()
        result = pq.read_table(output_file)
        assert "h3_cell" in result.column_names
        assert result.num_rows == 766

    def test_add_h3_file_custom_name(self, places_file, output_file):
        """Test with custom column name."""
        add_h3_column(places_file, output_file, h3_column_name="custom_h3", h3_resolution=9)
        assert Path(output_file).exists()
        result = pq.read_table(output_file)
        assert "custom_h3" in result.column_names


class TestAddH3Streaming:
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
        tmp_path = Path(tempfile.gettempdir()) / f"test_add_h3_stream_{uuid.uuid4()}.parquet"
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
        add_h3_column("-", output_file, h3_resolution=9)

        # Verify output
        assert Path(output_file).exists()
        result = pq.read_table(output_file)
        assert "h3_cell" in result.column_names
        assert result.num_rows == sample_geo_table.num_rows

    def test_file_to_stdout(self, places_file, monkeypatch):
        """Test writing to mocked stdout."""
        output_buffer = io.BytesIO()
        mock_stdout = mock.MagicMock()
        mock_stdout.buffer = output_buffer
        mock_stdout.isatty.return_value = False
        monkeypatch.setattr(sys, "stdout", mock_stdout)

        # Call function with "-" output
        add_h3_column(places_file, "-", h3_resolution=9)

        # Verify stream
        output_buffer.seek(0)
        reader = ipc.RecordBatchStreamReader(output_buffer)
        result = reader.read_all()
        assert result.num_rows > 0
        assert "h3_cell" in result.column_names


class TestAddH3CLI:
    """Tests for add h3 CLI command."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def output_file(self):
        """Create a temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_add_h3_cli_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_add_h3_cli_help(self):
        """Test that add h3 command has help."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["add", "h3", "--help"])
        assert result.exit_code == 0
        assert "h3" in result.output.lower()

    def test_add_h3_cli_basic(self, places_file, output_file):
        """Test basic CLI invocation."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["add", "h3", places_file, output_file, "--resolution", "9"])
        assert result.exit_code == 0
        assert Path(output_file).exists()
        loaded = pq.read_table(output_file)
        assert "h3_cell" in loaded.column_names
