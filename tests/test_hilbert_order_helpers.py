"""Tests for hilbert_order helper functions."""

from pathlib import Path

import pytest

from geoparquet_io.core.hilbert_order import _cleanup_temp_file


class TestCleanupTempFile:
    """Tests for _cleanup_temp_file function."""

    def test_cleanup_nonexistent_file(self, tmp_path):
        """Test cleanup with non-existent file."""
        nonexistent = str(tmp_path / "nonexistent.parquet")
        # Should not raise
        _cleanup_temp_file(nonexistent, verbose=False)

    def test_cleanup_none_file(self):
        """Test cleanup with None file."""
        # Should not raise
        _cleanup_temp_file(None, verbose=False)

    def test_cleanup_existing_file(self, tmp_path):
        """Test cleanup with existing file."""
        temp_file = tmp_path / "temp.parquet"
        temp_file.write_text("test content")
        assert temp_file.exists()

        _cleanup_temp_file(str(temp_file), verbose=False)

        assert not temp_file.exists()

    def test_cleanup_with_verbose(self, tmp_path, capsys):
        """Test cleanup with verbose output."""
        temp_file = tmp_path / "temp.parquet"
        temp_file.write_text("test content")

        _cleanup_temp_file(str(temp_file), verbose=True)

        assert not temp_file.exists()


class TestHilbertOrderIntegration:
    """Integration tests for hilbert_order."""

    @pytest.fixture
    def sample_file(self):
        """Return path to the sample file."""
        return str(Path(__file__).parent / "data" / "sample.parquet")

    def test_hilbert_order_help(self):
        """Test that hilbert sort command has help."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["sort", "hilbert", "--help"])
        assert result.exit_code == 0
        assert "hilbert" in result.output.lower()
