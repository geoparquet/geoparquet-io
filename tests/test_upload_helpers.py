"""Tests for upload helper functions."""

from click.testing import CliRunner


class TestUploadHelp:
    """Test upload command."""

    def test_upload_help(self):
        """Test that upload command has help."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["upload", "--help"])
        assert result.exit_code == 0
        assert "upload" in result.output.lower()


class TestSortByColumnHelp:
    """Test sort column command."""

    def test_sort_column_help(self):
        """Test that sort column command has help."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["sort", "column", "--help"])
        assert result.exit_code == 0
        assert "column" in result.output.lower()
