"""Tests for the meta command and inspect --meta."""

import json
import os

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from tests.conftest import _extract_json_from_output


@pytest.fixture
def runner():
    """Provide a Click CLI runner."""
    return CliRunner()


@pytest.fixture
def test_file():
    """Provide path to test GeoParquet file."""
    return os.path.join(os.path.dirname(__file__), "data", "places_test.parquet")


def test_meta_default(runner, test_file):
    """Test default meta output (all sections)."""
    result = runner.invoke(cli, ["meta", test_file])

    assert result.exit_code == 0
    # Should contain all three sections
    assert "Parquet File Metadata" in result.output
    assert "Parquet Geo Metadata" in result.output
    assert "GeoParquet Metadata" in result.output


def test_meta_parquet_only(runner, test_file):
    """Test meta with --parquet flag."""
    result = runner.invoke(cli, ["meta", test_file, "--parquet"])

    assert result.exit_code == 0
    assert "Parquet File Metadata" in result.output
    assert "Total Rows:" in result.output
    assert "Row Groups:" in result.output
    assert "Schema:" in result.output


def test_meta_geoparquet_only(runner, test_file):
    """Test meta with --geoparquet flag."""
    result = runner.invoke(cli, ["meta", test_file, "--geoparquet"])

    assert result.exit_code == 0
    # Output should contain either GeoParquet metadata or message that none found
    assert "GeoParquet Metadata" in result.output or "No GeoParquet metadata" in result.output


def test_meta_parquet_geo_only(runner, test_file):
    """Test meta with --parquet-geo flag."""
    result = runner.invoke(cli, ["meta", test_file, "--parquet-geo"])

    assert result.exit_code == 0
    assert "Parquet Geo Metadata" in result.output


def test_meta_multiple_flags(runner, test_file):
    """Test meta with multiple specific flags."""
    result = runner.invoke(cli, ["meta", test_file, "--parquet", "--geoparquet"])

    assert result.exit_code == 0
    assert "Parquet File Metadata" in result.output
    assert "GeoParquet Metadata" in result.output


def test_meta_row_groups_limit(runner, test_file):
    """Test meta with --row-groups flag."""
    result = runner.invoke(cli, ["meta", test_file, "--row-groups", "2"])

    assert result.exit_code == 0
    # Should work without error
    assert "Parquet File Metadata" in result.output


def test_meta_json_output(runner, test_file):
    """Test meta with --json flag."""
    result = runner.invoke(cli, ["meta", test_file, "--geoparquet", "--json"])

    assert result.exit_code == 0

    # Should be valid JSON (filter out deprecation warning)
    try:
        json_output = _extract_json_from_output(result.output)
        data = json.loads(json_output)
        # Data should be either None (no geo metadata) or a dict with geo metadata
        assert data is None or isinstance(data, dict)
    except json.JSONDecodeError:
        pytest.fail("Output is not valid JSON")


def test_meta_parquet_json(runner, test_file):
    """Test meta with --parquet and --json flags."""
    result = runner.invoke(cli, ["meta", test_file, "--parquet", "--json"])

    assert result.exit_code == 0

    # Parse JSON output (filter out deprecation warning)
    json_output = _extract_json_from_output(result.output)
    data = json.loads(json_output)

    # Verify structure
    assert "num_rows" in data
    assert "num_row_groups" in data
    assert "num_columns" in data
    assert "serialized_size" in data
    assert "schema" in data
    assert "row_groups" in data


def test_meta_nonexistent_file(runner):
    """Test meta with nonexistent file."""
    result = runner.invoke(cli, ["meta", "nonexistent.parquet"])

    assert result.exit_code != 0


def test_meta_help(runner):
    """Test meta command help."""
    result = runner.invoke(cli, ["meta", "--help"])

    assert result.exit_code == 0
    # Deprecated command help shows deprecation notice
    assert "DEPRECATED" in result.output or "deprecated" in result.output
    assert "--parquet" in result.output
    assert "--geoparquet" in result.output
    assert "--parquet-geo" in result.output
    assert "--row-groups" in result.output
    assert "--json" in result.output


def test_meta_with_buildings_file(runner):
    """Test meta with buildings test file."""
    buildings_file = os.path.join(os.path.dirname(__file__), "data", "buildings_test.parquet")

    if not os.path.exists(buildings_file):
        pytest.skip("buildings_test.parquet not available")

    result = runner.invoke(cli, ["meta", buildings_file])
    assert result.exit_code == 0
    assert "Parquet File Metadata" in result.output


def test_meta_shows_deprecation_warning(runner, test_file):
    """Test that meta command shows deprecation warning."""
    result = runner.invoke(cli, ["meta", test_file])

    assert result.exit_code == 0
    assert "deprecated" in result.output.lower()
    assert "gpio inspect --meta" in result.output


# Tests for new inspect --meta functionality


def test_inspect_meta_default(runner, test_file):
    """Test inspect --meta shows all metadata sections."""
    result = runner.invoke(cli, ["inspect", test_file, "--meta"])

    assert result.exit_code == 0
    # Should contain all three sections (same as gpio meta)
    assert "Parquet File Metadata" in result.output
    assert "Parquet Geo Metadata" in result.output
    assert "GeoParquet Metadata" in result.output


def test_inspect_meta_parquet_only(runner, test_file):
    """Test inspect --meta --parquet shows only Parquet metadata."""
    result = runner.invoke(cli, ["inspect", test_file, "--meta", "--parquet"])

    assert result.exit_code == 0
    assert "Parquet File Metadata" in result.output
    assert "Total Rows:" in result.output


def test_inspect_meta_geoparquet_only(runner, test_file):
    """Test inspect --meta --geoparquet shows only GeoParquet metadata."""
    result = runner.invoke(cli, ["inspect", test_file, "--meta", "--geoparquet"])

    assert result.exit_code == 0
    assert "GeoParquet Metadata" in result.output or "No GeoParquet metadata" in result.output


def test_inspect_meta_json_output(runner, test_file):
    """Test inspect --meta --json produces valid JSON."""
    result = runner.invoke(cli, ["inspect", test_file, "--meta", "--geoparquet", "--json"])

    assert result.exit_code == 0

    # Should be valid JSON (no deprecation warning with inspect)
    try:
        data = json.loads(result.output)
        assert data is None or isinstance(data, dict)
    except json.JSONDecodeError:
        pytest.fail("Output is not valid JSON")


def test_inspect_meta_mutually_exclusive_with_head(runner, test_file):
    """Test --meta cannot be used with --head."""
    result = runner.invoke(cli, ["inspect", test_file, "--meta", "--head"])

    assert result.exit_code != 0
    assert "cannot be used with" in result.output


def test_inspect_meta_mutually_exclusive_with_stats(runner, test_file):
    """Test --meta cannot be used with --stats."""
    result = runner.invoke(cli, ["inspect", test_file, "--meta", "--stats"])

    assert result.exit_code != 0
    assert "cannot be used with" in result.output


def test_inspect_meta_options_require_meta_flag(runner, test_file):
    """Test --parquet and other meta options require --meta flag."""
    result = runner.invoke(cli, ["inspect", test_file, "--parquet"])

    assert result.exit_code != 0
    assert "require --meta flag" in result.output
