"""
Tests for check commands.

These assert the specific verdicts gpio reaches on fixtures with pinned,
known properties, not just exit codes:

- ``places_test.parquet``: 766 rows, 1 row group, SNAPPY-compressed geometry,
  GeoParquet 1.0.0, has a ``bbox`` column (but no covering — 1.0 cannot carry
  one).
- ``buildings_test.parquet``: 42 rows, 1 row group, ZSTD-compressed geometry,
  GeoParquet 1.0.0, no bbox column.
- ``tests/data/canonical/places.parquet`` (``places_with_covering_file``):
  GeoParquet 1.1.0 with a proper bbox covering — the known-good contrast.

If a fixture is regenerated with different properties, update these constants
alongside it (see tests/data/canonical/README.md).
"""

import logging

from click.testing import CliRunner

from geoparquet_io.cli.main import check


class TestCheckCommands:
    """Test suite for check commands."""

    def test_check_all_places(self, places_test_file):
        """check all reaches the right verdict on every section for places."""
        runner = CliRunner()
        result = runner.invoke(check, ["all", places_test_file])
        assert result.exit_code == 0
        # Row groups: one group of all 766 rows.
        assert "Number of row groups: 1" in result.output
        assert "Average rows per group: 766" in result.output
        # Metadata: 1.0.0 flagged as outdated; bbox column found but the
        # covering key needs 1.1+.
        assert "Version 1.0.0 (upgrade to 1.1.0+ recommended)" in result.output
        assert "Found bbox column 'bbox'" in result.output
        # Compression: SNAPPY geometry draws the ZSTD recommendation.
        assert (
            "SNAPPY compression on geometry column 'geometry' (ZSTD recommended)" in result.output
        )
        # Spatial order: the fixture is hilbert-sorted.
        assert "Data appears to be spatially ordered" in result.output
        # Spec validation passes.
        assert "checks passed" in result.output

    def test_check_all_buildings(self, buildings_test_file):
        """check all reaches the right verdict on every section for buildings."""
        runner = CliRunner()
        result = runner.invoke(check, ["all", buildings_test_file])
        assert result.exit_code == 0
        assert "Number of row groups: 1" in result.output
        assert "Average rows per group: 42" in result.output
        assert "Version 1.0.0 (upgrade to 1.1.0+ recommended)" in result.output
        # No bbox column in this fixture.
        assert "No bbox column found" in result.output
        # Geometry is already ZSTD, so no recommendation is drawn.
        assert "ZSTD compression on geometry column 'geometry'" in result.output
        assert "ZSTD recommended" not in result.output
        assert "Data appears to be spatially ordered" in result.output

    def test_check_all_known_good(self, places_with_covering_file):
        """check all on the canonical 1.1 file draws no warnings at all."""
        runner = CliRunner()
        result = runner.invoke(check, ["all", places_with_covering_file])
        assert result.exit_code == 0
        assert "Version 1.1.0" in result.output
        assert "Found bbox column 'bbox' with proper metadata covering" in result.output
        assert "ZSTD compression on geometry column 'geometry'" in result.output
        assert "Data appears to be spatially ordered" in result.output
        assert "WARNING" not in result.output

    def test_check_all_verbose(self, places_test_file, caplog):
        """--verbose adds the schema and file-type detail to check all."""
        runner = CliRunner()
        # Depending on which handler bootstrapped first in the process, the
        # debug lines land on Click's stdout or on the root logger pytest
        # captures — assert on the union so the test is order-independent.
        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            result = runner.invoke(check, ["all", places_test_file, "--verbose"])
        assert result.exit_code == 0
        combined = result.output + caplog.text
        # Verbose-only output: file type detection (possibly served from the
        # process-wide cache when another test already inspected this file)
        # and the schema listing.
        assert "File type detection" in combined
        assert "Schema fields:" in combined
        assert "fsq_place_id: string" in combined

    def test_check_spatial_places(self, places_test_file):
        """check spatial says the hilbert-sorted places file is ordered."""
        runner = CliRunner()
        result = runner.invoke(check, ["spatial", places_test_file])
        assert result.exit_code == 0
        assert "Data appears to be spatially ordered" in result.output

    def test_check_spatial_buildings(self, buildings_test_file):
        """Without a bbox column check spatial samples, and still finds order."""
        runner = CliRunner()
        result = runner.invoke(check, ["spatial", buildings_test_file])
        assert result.exit_code == 0
        # No bbox column forces the sampling method...
        assert "using slower sampling method" in result.output
        # ...which still detects the clustered fixture as ordered.
        assert "Data appears to be spatially ordered" in result.output
        # Pushdown readiness flags the missing bbox column.
        assert "No geo_bbox column found" in result.output

    def test_check_spatial_with_options(self, places_test_file, caplog):
        """check spatial options are accepted and the bbox-stats path is used."""
        runner = CliRunner()
        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
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
        # The places fixture has a bbox column, so verbose shows the
        # bbox-stats method rather than sampling.
        combined = result.output + caplog.text
        assert "Using bbox-stats method (bbox column: bbox)" in combined
        assert "Data appears to be spatially ordered" in result.output

    def test_check_compression_places(self, places_test_file):
        """check compression flags the SNAPPY geometry column."""
        runner = CliRunner()
        result = runner.invoke(check, ["compression", places_test_file])
        assert result.exit_code == 0
        assert (
            "SNAPPY compression on geometry column 'geometry' (ZSTD recommended)" in result.output
        )

    def test_check_compression_buildings(self, buildings_test_file):
        """check compression approves the ZSTD geometry column."""
        runner = CliRunner()
        result = runner.invoke(check, ["compression", buildings_test_file])
        assert result.exit_code == 0
        assert "ZSTD compression on geometry column 'geometry'" in result.output
        # A compliant file draws no recommendation.
        assert "ZSTD recommended" not in result.output

    def test_check_bbox_places(self, places_test_file):
        """check bbox finds the bbox column but notes 1.0 cannot advertise it."""
        runner = CliRunner()
        result = runner.invoke(check, ["bbox", places_test_file])
        assert result.exit_code == 0
        assert "Found bbox column 'bbox'" in result.output
        # The covering key needs 1.1+, and this file declares 1.0.0.
        assert "needs GeoParquet 1.1+" in result.output

    def test_check_bbox_buildings(self, buildings_test_file):
        """check bbox reports the missing bbox column on buildings."""
        runner = CliRunner()
        result = runner.invoke(check, ["bbox", buildings_test_file])
        assert result.exit_code == 0
        assert "No bbox column found" in result.output

    def test_check_row_group_places(self, places_test_file):
        """check row-group reports the pinned single-group layout for places."""
        runner = CliRunner()
        result = runner.invoke(check, ["row-group", places_test_file])
        assert result.exit_code == 0
        assert "Number of row groups: 1" in result.output
        assert "Average rows per group: 766" in result.output
        assert "appropriate for small file" in result.output

    def test_check_row_group_buildings(self, buildings_test_file):
        """check row-group reports the pinned single-group layout for buildings."""
        runner = CliRunner()
        result = runner.invoke(check, ["row-group", buildings_test_file])
        assert result.exit_code == 0
        assert "Number of row groups: 1" in result.output
        assert "Average rows per group: 42" in result.output

    def test_check_nonexistent_file(self):
        """check all on a missing file fails with a clear error."""
        runner = CliRunner()
        result = runner.invoke(check, ["all", "nonexistent.parquet"])
        assert result.exit_code != 0
        assert "File not found" in result.output
