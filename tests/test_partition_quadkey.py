"""Tests for partition_by_quadkey module."""

import tempfile
import uuid
from pathlib import Path

import pytest
from click import UsageError
from click.testing import CliRunner

from geoparquet_io.core.partition_by_quadkey import (
    _calculate_partition_stats,
    _validate_resolutions,
)


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
    """Tests for _calculate_partition_stats function."""

    def test_empty_folder(self, tmp_path):
        """Test with empty folder."""
        total_mb, avg_mb = _calculate_partition_stats(str(tmp_path), 0)
        assert total_mb == 0
        assert avg_mb == 0

    def test_with_parquet_files(self, tmp_path):
        """Test with parquet files in folder."""
        # Create some dummy parquet files
        for i in range(3):
            f = tmp_path / f"file_{i}.parquet"
            f.write_bytes(b"x" * 1024)  # 1KB each

        total_mb, avg_mb = _calculate_partition_stats(str(tmp_path), 3)
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
        # Cleanup
        import shutil

        if tmp_path.exists():
            shutil.rmtree(tmp_path)

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
