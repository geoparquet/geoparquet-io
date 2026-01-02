"""Tests for helpful error messages when shell expands glob patterns."""

from click.testing import CliRunner

from geoparquet_io.cli.main import cli


class TestGlobAwareCommand:
    """Test GlobAwareCommand provides helpful errors for shell-expanded globs."""

    def test_extract_shell_expanded_glob_error(self):
        """Test extract shows helpful error when glob is shell-expanded."""
        runner = CliRunner()
        # Simulate shell expansion: multiple parquet files as args
        result = runner.invoke(
            cli,
            [
                "extract",
                "file1.parquet",
                "file2.parquet",
                "file3.parquet",
                "output.parquet",
            ],
        )
        assert result.exit_code != 0
        assert "Received 4 parquet files" in result.output
        assert "shell expanded a glob pattern" in result.output
        assert 'gpio extract "path/*.parquet"' in result.output

    def test_inspect_shell_expanded_glob_error(self):
        """Test inspect shows helpful error when glob is shell-expanded."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "inspect",
                "file1.parquet",
                "file2.parquet",
                "file3.parquet",
            ],
        )
        assert result.exit_code != 0
        assert "Received 3 parquet files" in result.output
        assert "shell expanded a glob pattern" in result.output
        assert 'gpio inspect "path/*.parquet"' in result.output

    def test_check_all_shell_expanded_glob_error(self):
        """Test check all shows helpful error when glob is shell-expanded."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "check",
                "all",
                "file1.parquet",
                "file2.parquet",
                "file3.parquet",
            ],
        )
        assert result.exit_code != 0
        assert "Received 3 parquet files" in result.output
        assert "shell expanded a glob pattern" in result.output
        assert 'gpio check all "path/*.parquet"' in result.output


class TestSingleFileCommand:
    """Test SingleFileCommand provides helpful errors suggesting gpio extract."""

    def test_convert_shell_expanded_glob_error(self):
        """Test convert shows helpful error suggesting gpio extract."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                "geoparquet",
                "file1.parquet",
                "file2.parquet",
                "file3.parquet",
                "output.parquet",
            ],
        )
        assert result.exit_code != 0
        assert "Received 4 parquet files" in result.output
        assert "'convert geoparquet' command requires a single file" in result.output
        assert 'gpio extract "path/*.parquet"' in result.output
        assert "gpio convert geoparquet consolidated.parquet" in result.output

    def test_sort_hilbert_shell_expanded_glob_error(self):
        """Test sort hilbert shows helpful error suggesting gpio extract."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "sort",
                "hilbert",
                "file1.parquet",
                "file2.parquet",
                "file3.parquet",
                "output.parquet",
            ],
        )
        assert result.exit_code != 0
        assert "Received 4 parquet files" in result.output
        assert "'sort hilbert' command requires a single file" in result.output
        assert 'gpio extract "path/*.parquet"' in result.output
        assert "gpio sort hilbert consolidated.parquet" in result.output

    def test_add_bbox_shell_expanded_glob_error(self):
        """Test add bbox shows helpful error suggesting gpio extract."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "bbox",
                "file1.parquet",
                "file2.parquet",
                "file3.parquet",
            ],
        )
        assert result.exit_code != 0
        assert "Received 3 parquet files" in result.output
        assert "'add bbox' command requires a single file" in result.output
        assert 'gpio extract "path/*.parquet"' in result.output
        assert "gpio add bbox consolidated.parquet" in result.output

    def test_partition_admin_shell_expanded_glob_error(self):
        """Test partition admin shows helpful error suggesting gpio extract."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "partition",
                "admin",
                "file1.parquet",
                "file2.parquet",
                "file3.parquet",
                "output_folder",
            ],
        )
        assert result.exit_code != 0
        assert "Received 3 parquet files" in result.output
        assert "'partition admin' command requires a single file" in result.output
        assert 'gpio extract "path/*.parquet"' in result.output
        assert "gpio partition admin consolidated.parquet" in result.output


class TestTwoFilesNotTriggered:
    """Test that 2 parquet files (input + output) don't trigger the error."""

    def test_extract_two_files_ok(self):
        """Two parquet files should not trigger glob expansion error."""
        runner = CliRunner()
        # Two files is normal: input + output
        result = runner.invoke(cli, ["extract", "input.parquet", "output.parquet"])
        # Should fail for different reason (file not found), not glob expansion
        if result.exit_code != 0:
            assert "shell expanded a glob pattern" not in result.output

    def test_convert_two_files_ok(self):
        """Two parquet files should not trigger glob expansion error."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", "geoparquet", "input.parquet", "output.parquet"])
        # Should fail for different reason (file not found), not glob expansion
        if result.exit_code != 0:
            assert "requires a single file" not in result.output
