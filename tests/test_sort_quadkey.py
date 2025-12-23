"""Tests for sort_quadkey module."""

import tempfile
import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner


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
        if tmp_path.exists():
            tmp_path.unlink()

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
