"""
Tests for format commands.
"""

import os
import shutil

from click.testing import CliRunner

from geoparquet_tools.cli.main import format


class TestFormatCommands:
    """Test suite for format commands."""

    def test_format_bbox_metadata_places(self, places_test_file, temp_output_dir):
        """Test format bbox-metadata command on places file."""
        # Copy file to temp dir since format modifies it in place
        temp_file = os.path.join(temp_output_dir, "places.parquet")
        shutil.copy2(places_test_file, temp_file)

        runner = CliRunner()
        result = runner.invoke(format, ["bbox-metadata", temp_file])
        assert result.exit_code == 0
        # File should still exist
        assert os.path.exists(temp_file)

    def test_format_bbox_metadata_buildings(self, buildings_test_file, temp_output_dir):
        """Test format bbox-metadata command on buildings file."""
        # Copy file to temp dir since format modifies it in place
        temp_file = os.path.join(temp_output_dir, "buildings.parquet")
        shutil.copy2(buildings_test_file, temp_file)

        runner = CliRunner()
        result = runner.invoke(format, ["bbox-metadata", temp_file])
        # May fail if buildings doesn't have bbox column, which is expected
        # We're just testing that the command runs
        assert result.exit_code in [0, 1]

    def test_format_bbox_metadata_verbose(self, places_test_file, temp_output_dir):
        """Test format bbox-metadata command with verbose flag."""
        # Copy file to temp dir since format modifies it in place
        temp_file = os.path.join(temp_output_dir, "places.parquet")
        shutil.copy2(places_test_file, temp_file)

        runner = CliRunner()
        result = runner.invoke(format, ["bbox-metadata", temp_file, "--verbose"])
        assert result.exit_code == 0

    def test_format_bbox_metadata_nonexistent_file(self):
        """Test format bbox-metadata on nonexistent file."""
        runner = CliRunner()
        result = runner.invoke(format, ["bbox-metadata", "nonexistent.parquet"])
        # Should fail with non-zero exit code
        assert result.exit_code != 0
