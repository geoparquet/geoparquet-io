"""
Tests for check commands.
"""

from click.testing import CliRunner

from geoparquet_io.cli.main import check


class TestCheckCommands:
    """Test suite for check commands."""

    def test_check_all_places(self, places_test_file):
        """Test check all command on places file."""
        runner = CliRunner()
        result = runner.invoke(check, ["all", places_test_file])
        assert result.exit_code == 0
        assert "GeoParquet Metadata" in result.output or "metadata" in result.output.lower()

    def test_check_all_buildings(self, buildings_test_file):
        """Test check all command on buildings file."""
        runner = CliRunner()
        result = runner.invoke(check, ["all", buildings_test_file])
        assert result.exit_code == 0
        assert "GeoParquet Metadata" in result.output or "metadata" in result.output.lower()

    def test_check_all_verbose(self, places_test_file):
        """Test check all command with verbose flag."""
        runner = CliRunner()
        result = runner.invoke(check, ["all", places_test_file, "--verbose"])
        assert result.exit_code == 0

    def test_check_spatial_places(self, places_test_file):
        """Test check spatial command on places file."""
        runner = CliRunner()
        result = runner.invoke(check, ["spatial", places_test_file])
        assert result.exit_code == 0
        # Should show spatial ordering result
        assert "spatially ordered" in result.output.lower() or "spatial" in result.output.lower()

    def test_check_spatial_buildings(self, buildings_test_file):
        """Test check spatial command on buildings file."""
        runner = CliRunner()
        result = runner.invoke(check, ["spatial", buildings_test_file])
        assert result.exit_code == 0

    def test_check_spatial_with_options(self, places_test_file):
        """Test check spatial command with custom options."""
        runner = CliRunner()
        result = runner.invoke(
            check,
            [
                "spatial",
                places_test_file,
                "--random-sample-size",
                "50",
                "--limit-rows",
                "1000",
                "--verbose",
            ],
        )
        assert result.exit_code == 0

    def test_check_compression_places(self, places_test_file):
        """Test check compression command on places file."""
        runner = CliRunner()
        result = runner.invoke(check, ["compression", places_test_file])
        assert result.exit_code == 0
        # Should mention compression
        assert "compression" in result.output.lower() or "codec" in result.output.lower()

    def test_check_compression_buildings(self, buildings_test_file):
        """Test check compression command on buildings file."""
        runner = CliRunner()
        result = runner.invoke(check, ["compression", buildings_test_file])
        assert result.exit_code == 0

    def test_check_bbox_places(self, places_test_file):
        """Test check bbox command on places file."""
        runner = CliRunner()
        result = runner.invoke(check, ["bbox", places_test_file])
        assert result.exit_code == 0
        # Should mention bbox or metadata
        assert "bbox" in result.output.lower() or "metadata" in result.output.lower()

    def test_check_bbox_buildings(self, buildings_test_file):
        """Test check bbox command on buildings file."""
        runner = CliRunner()
        result = runner.invoke(check, ["bbox", buildings_test_file])
        assert result.exit_code == 0

    def test_check_row_group_places(self, places_test_file):
        """Test check row-group command on places file."""
        runner = CliRunner()
        result = runner.invoke(check, ["row-group", places_test_file])
        assert result.exit_code == 0
        # Should mention row group
        assert "row group" in result.output.lower() or "rows" in result.output.lower()

    def test_check_row_group_buildings(self, buildings_test_file):
        """Test check row-group command on buildings file."""
        runner = CliRunner()
        result = runner.invoke(check, ["row-group", buildings_test_file])
        assert result.exit_code == 0

    def test_check_nonexistent_file(self):
        """Test check command on nonexistent file."""
        runner = CliRunner()
        result = runner.invoke(check, ["all", "nonexistent.parquet"])
        # Should fail with non-zero exit code
        assert result.exit_code != 0
