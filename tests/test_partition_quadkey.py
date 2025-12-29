"""Tests for partition_by_quadkey module."""

import io
import sys
import tempfile
import uuid
from pathlib import Path
from unittest import mock

import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest
from click import UsageError
from click.testing import CliRunner

from geoparquet_io.core.partition_by_quadkey import _validate_resolutions, partition_by_quadkey
from geoparquet_io.core.partition_common import calculate_partition_stats
from tests.conftest import safe_rmtree


class TestValidateResolutions:
    """Tests for _validate_resolutions function."""

    def test_valid_resolutions(self):
        """Test with valid resolutions."""
        # Should not raise
        _validate_resolutions(13, 9)
        _validate_resolutions(23, 23)
        _validate_resolutions(0, 0)

    def test_resolution_out_of_range(self):
        """Test with resolution out of range."""
        with pytest.raises(UsageError):
            _validate_resolutions(25, 9)

    def test_partition_resolution_out_of_range(self):
        """Test with partition resolution out of range."""
        with pytest.raises(UsageError):
            _validate_resolutions(13, 25)

    def test_partition_resolution_exceeds_resolution(self):
        """Test with partition resolution exceeding column resolution."""
        with pytest.raises(UsageError):
            _validate_resolutions(5, 10)


class TestCalculatePartitionStats:
    """Tests for calculate_partition_stats function."""

    def test_empty_folder(self, tmp_path):
        """Test with empty folder."""
        total_mb, avg_mb = calculate_partition_stats(str(tmp_path), 0)
        assert total_mb == 0
        assert avg_mb == 0

    def test_with_parquet_files(self, tmp_path):
        """Test with parquet files in folder."""
        # Create some dummy parquet files
        for i in range(3):
            f = tmp_path / f"file_{i}.parquet"
            f.write_bytes(b"x" * 1024)  # 1KB each

        total_mb, avg_mb = calculate_partition_stats(str(tmp_path), 3)
        assert total_mb > 0
        assert avg_mb > 0


class TestPartitionQuadkeyCommand:
    """Tests for the partition quadkey CLI command."""

    @pytest.fixture
    def sample_file(self):
        """Return path to the sample file."""
        return str(Path(__file__).parent / "data" / "sample.parquet")

    @pytest.fixture
    def output_folder(self):
        """Create a temp output folder path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_partition_quadkey_{uuid.uuid4()}"
        yield str(tmp_path)
        safe_rmtree(tmp_path)

    def test_partition_quadkey_help(self):
        """Test that quadkey partition command has help."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["partition", "quadkey", "--help"])
        assert result.exit_code == 0
        assert "quadkey" in result.output.lower()

    def test_partition_quadkey_invalid_resolution(self, sample_file, output_folder):
        """Test with invalid resolution."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(
            cli, ["partition", "quadkey", sample_file, output_folder, "--resolution", "30"]
        )
        assert result.exit_code != 0


class TestPartitionByQuadkeyFunction:
    """Tests for partition_by_quadkey function."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def output_folder(self):
        """Create a temp output folder path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_partition_func_{uuid.uuid4()}"
        yield str(tmp_path)
        safe_rmtree(tmp_path)

    def test_partition_basic(self, places_file, output_folder):
        """Test basic partitioning."""
        partition_by_quadkey(
            places_file,
            output_folder,
            resolution=10,
            partition_resolution=5,
            skip_analysis=True,
        )
        # Check partitions were created
        output_path = Path(output_folder)
        assert output_path.exists()
        parquet_files = list(output_path.glob("*.parquet"))
        assert len(parquet_files) > 0

    def test_partition_hive_style(self, places_file, output_folder):
        """Test Hive-style partitioning."""
        partition_by_quadkey(
            places_file,
            output_folder,
            resolution=10,
            partition_resolution=3,
            hive=True,
            skip_analysis=True,
        )
        # Check partitions were created in subdirectories
        output_path = Path(output_folder)
        assert output_path.exists()
        # Hive style creates directories like quadkey=abc/
        subdirs = [d for d in output_path.iterdir() if d.is_dir()]
        assert len(subdirs) > 0


class TestPartitionByQuadkeyStreaming:
    """Tests for streaming input to partition_by_quadkey."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def sample_geo_table(self, places_file):
        """Create a geo table from test data."""
        return pq.read_table(places_file)

    @pytest.fixture
    def output_folder(self):
        """Create a temp output folder path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_partition_stream_{uuid.uuid4()}"
        yield str(tmp_path)
        safe_rmtree(tmp_path)

    def test_stdin_to_partition(self, sample_geo_table, output_folder, monkeypatch):
        """Test partitioning from stdin."""
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
        partition_by_quadkey(
            "-",
            output_folder,
            resolution=10,
            partition_resolution=5,
            skip_analysis=True,
        )

        # Verify partitions were created
        output_path = Path(output_folder)
        assert output_path.exists()
        parquet_files = list(output_path.glob("*.parquet"))
        assert len(parquet_files) > 0
