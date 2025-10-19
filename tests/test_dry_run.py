"""
Tests for dry-run functionality in add commands.
"""

from click.testing import CliRunner

from geoparquet_io.cli.main import add


class TestDryRunCommands:
    """Test suite for dry-run functionality."""

    def test_add_bbox_dry_run(self, buildings_test_file):
        """Test dry-run mode for add bbox command."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", buildings_test_file, "output.parquet", "--dry-run"])

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        assert "COPY (" in result.output
        assert "STRUCT_PACK(" in result.output
        assert "ST_XMin" in result.output
        assert "ST_YMin" in result.output
        assert "ST_XMax" in result.output
        assert "ST_YMax" in result.output
        assert "FORMAT PARQUET" in result.output
        # Should show geometry column name
        assert "-- Geometry column:" in result.output
        # Should not actually create the file
        assert "Successfully added" not in result.output

    def test_add_bbox_dry_run_with_custom_name(self, buildings_test_file):
        """Test dry-run mode with custom bbox column name."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["bbox", buildings_test_file, "output.parquet", "--bbox-name", "bounds", "--dry-run"],
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        assert "AS bounds" in result.output
        assert "-- Bbox column name: bounds" in result.output

    def test_add_admin_divisions_dry_run(self, buildings_test_file):
        """Test dry-run mode for add admin-divisions command."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["admin-divisions", buildings_test_file, "output.parquet", "--dry-run"]
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        # Should show bounds calculation SQL
        assert "-- Step 1: Calculate bounding box" in result.output
        assert "MIN(ST_XMin" in result.output or "MIN(bbox.xmin" in result.output
        # Should show the filtered countries query
        assert "-- Step 2: Create temporary table" in result.output
        assert "ST_Intersects" in result.output
        assert "CREATE TEMP TABLE filtered_countries" in result.output
        # Should show main spatial join
        assert "-- Step 3: Main spatial join query" in result.output
        assert 'b."country" as "admin:country_code"' in result.output
        # Should calculate actual bounds
        assert "-- Bounds calculated:" in result.output

    def test_add_admin_divisions_dry_run_with_countries_file(
        self, buildings_test_file, places_test_file
    ):
        """Test dry-run mode with custom countries file."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            [
                "admin-divisions",
                buildings_test_file,
                "output.parquet",
                "--countries-file",
                places_test_file,
                "--dry-run",
            ],
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        # Should not have steps for filtering
        assert "-- Step 1: Main spatial join query" in result.output
        # Should not calculate bounds for non-default countries
        assert "Calculate bounding box" not in result.output
        assert 'b."admin:country_code"' in result.output

    def test_add_bbox_dry_run_verbose(self, buildings_test_file):
        """Test dry-run mode with verbose flag."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["bbox", buildings_test_file, "output.parquet", "--dry-run", "--verbose"]
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        # Verbose should not affect dry-run output significantly
        assert "COPY (" in result.output

    def test_dry_run_does_not_create_files(self, buildings_test_file, temp_output_file):
        """Ensure dry-run doesn't create output files."""
        import os

        # Make sure output doesn't exist
        if os.path.exists(temp_output_file):
            os.remove(temp_output_file)

        runner = CliRunner()

        # Test bbox dry-run
        result = runner.invoke(add, ["bbox", buildings_test_file, temp_output_file, "--dry-run"])
        assert result.exit_code == 0
        assert not os.path.exists(temp_output_file)

        # Test admin-divisions dry-run
        result = runner.invoke(
            add, ["admin-divisions", buildings_test_file, temp_output_file, "--dry-run"]
        )
        assert result.exit_code == 0
        assert not os.path.exists(temp_output_file)

    def test_dry_run_with_bbox_column_present(self, places_test_file):
        """Test dry-run when input has bbox column (for admin-divisions)."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["admin-divisions", places_test_file, "output.parquet", "--dry-run"]
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        # Should use bbox column for bounds calculation
        assert "MIN(bbox.xmin)" in result.output
        # Should still show bounds
        assert "-- Bounds calculated:" in result.output
