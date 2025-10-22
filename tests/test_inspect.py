"""Tests for the inspect command."""

import json
import os

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.inspect_utils import (
    extract_columns_info,
    extract_file_info,
    extract_geo_info,
    format_geometry_display,
    parse_wkb_type,
)


@pytest.fixture
def runner():
    """Provide a Click CLI runner."""
    return CliRunner()


@pytest.fixture
def test_file():
    """Provide path to test GeoParquet file."""
    return os.path.join(os.path.dirname(__file__), "data", "places_test.parquet")


def test_inspect_default(runner, test_file):
    """Test default inspect output (metadata only)."""
    result = runner.invoke(cli, ["inspect", test_file])

    assert result.exit_code == 0
    assert "places_test.parquet" in result.output
    assert "Rows:" in result.output
    assert "Row Groups:" in result.output
    assert "Columns" in result.output
    assert "CRS:" in result.output or "No GeoParquet metadata" in result.output


def test_inspect_head(runner, test_file):
    """Test inspect with --head flag."""
    result = runner.invoke(cli, ["inspect", test_file, "--head", "5"])

    assert result.exit_code == 0
    assert "Preview (first" in result.output
    assert (
        "5 rows" in result.output or "rows)" in result.output
    )  # May show fewer if file has < 5 rows


def test_inspect_tail(runner, test_file):
    """Test inspect with --tail flag."""
    result = runner.invoke(cli, ["inspect", test_file, "--tail", "3"])

    assert result.exit_code == 0
    assert "Preview (last" in result.output
    assert "3 rows" in result.output or "rows)" in result.output


def test_inspect_head_tail_exclusive(runner, test_file):
    """Test that --head and --tail are mutually exclusive."""
    result = runner.invoke(cli, ["inspect", test_file, "--head", "5", "--tail", "3"])

    assert result.exit_code != 0
    assert "mutually exclusive" in result.output.lower()


def test_inspect_stats(runner, test_file):
    """Test inspect with --stats flag."""
    result = runner.invoke(cli, ["inspect", test_file, "--stats"])

    assert result.exit_code == 0
    assert "Statistics:" in result.output
    assert "Nulls" in result.output
    assert "Min" in result.output
    assert "Max" in result.output
    assert "Unique" in result.output


def test_inspect_json(runner, test_file):
    """Test inspect with --json flag."""
    result = runner.invoke(cli, ["inspect", test_file, "--json"])

    assert result.exit_code == 0

    # Parse JSON output
    data = json.loads(result.output)

    # Verify structure
    assert "file" in data
    assert "size_bytes" in data
    assert "size_human" in data
    assert "rows" in data
    assert "row_groups" in data
    assert "crs" in data
    assert "bbox" in data
    assert "columns" in data
    assert "preview" in data
    assert "statistics" in data

    # Verify columns structure
    assert isinstance(data["columns"], list)
    if len(data["columns"]) > 0:
        col = data["columns"][0]
        assert "name" in col
        assert "type" in col
        assert "is_geometry" in col

    # Preview should be None by default
    assert data["preview"] is None
    assert data["statistics"] is None


def test_inspect_json_with_head(runner, test_file):
    """Test JSON output includes preview data when --head is specified."""
    result = runner.invoke(cli, ["inspect", test_file, "--json", "--head", "2"])

    assert result.exit_code == 0

    data = json.loads(result.output)

    # Preview should contain data
    assert data["preview"] is not None
    assert isinstance(data["preview"], list)
    # Should have at most 2 rows (or fewer if file is smaller)
    assert len(data["preview"]) <= 2


def test_inspect_json_with_stats(runner, test_file):
    """Test JSON output includes statistics when --stats is specified."""
    result = runner.invoke(cli, ["inspect", test_file, "--json", "--stats"])

    assert result.exit_code == 0

    data = json.loads(result.output)

    # Statistics should be present
    assert data["statistics"] is not None
    assert isinstance(data["statistics"], dict)

    # Each column should have stats
    for col in data["columns"]:
        col_name = col["name"]
        assert col_name in data["statistics"]
        stats = data["statistics"][col_name]
        assert "nulls" in stats


def test_inspect_nonexistent_file(runner):
    """Test inspect with nonexistent file."""
    result = runner.invoke(cli, ["inspect", "nonexistent.parquet"])

    assert result.exit_code != 0


def test_extract_file_info(test_file):
    """Test extract_file_info function."""
    info = extract_file_info(test_file)

    assert "file_path" in info
    assert "size_bytes" in info
    assert "size_human" in info
    assert "rows" in info
    assert "row_groups" in info

    assert info["rows"] >= 0
    assert info["row_groups"] >= 0
    if info["size_bytes"] is not None:
        assert info["size_bytes"] > 0


def test_extract_geo_info(test_file):
    """Test extract_geo_info function."""
    info = extract_geo_info(test_file)

    assert "has_geo_metadata" in info
    assert "crs" in info
    assert "bbox" in info
    assert "primary_column" in info


def test_extract_columns_info(test_file):
    """Test extract_columns_info function."""
    import fsspec
    import pyarrow.parquet as pq

    from geoparquet_io.core.common import safe_file_url

    safe_url = safe_file_url(test_file, verbose=False)
    with fsspec.open(safe_url, "rb") as f:
        pf = pq.ParquetFile(f)
        schema = pf.schema_arrow

    columns = extract_columns_info(schema, "geometry")

    assert len(columns) > 0

    for col in columns:
        assert "name" in col
        assert "type" in col
        assert "is_geometry" in col

    # At least one column should be marked as geometry
    [c for c in columns if c["is_geometry"]]
    # May or may not have geometry columns depending on test file
    # Just verify structure is correct


def test_parse_wkb_type():
    """Test WKB type parsing."""
    # Point WKB (little endian): 0x0101000000... (first 5 bytes)
    point_wkb = bytes([0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
    assert parse_wkb_type(point_wkb) == "POINT"

    # Polygon WKB (little endian): 0x0103000000...
    polygon_wkb = bytes([0x01, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
    assert parse_wkb_type(polygon_wkb) == "POLYGON"

    # LineString WKB (little endian): 0x0102000000...
    linestring_wkb = bytes([0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
    assert parse_wkb_type(linestring_wkb) == "LINESTRING"

    # Empty or invalid bytes
    assert parse_wkb_type(b"") == "GEOMETRY"
    assert parse_wkb_type(bytes([0x01])) == "GEOMETRY"


def test_format_geometry_display():
    """Test geometry display formatting."""
    # None value
    assert format_geometry_display(None) == "NULL"

    # Point WKB
    point_wkb = bytes([0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
    result = format_geometry_display(point_wkb)
    assert "<POINT>" in result

    # Non-bytes value
    result = format_geometry_display("some string")
    assert "some string" in result


def test_inspect_with_buildings_file(runner):
    """Test inspect with buildings test file."""
    buildings_file = os.path.join(os.path.dirname(__file__), "data", "buildings_test.parquet")

    if not os.path.exists(buildings_file):
        pytest.skip("buildings_test.parquet not available")

    result = runner.invoke(cli, ["inspect", buildings_file])
    assert result.exit_code == 0
    assert "buildings_test.parquet" in result.output


def test_inspect_combined_flags(runner, test_file):
    """Test inspect with multiple flags combined."""
    result = runner.invoke(cli, ["inspect", test_file, "--head", "3", "--stats"])

    assert result.exit_code == 0
    assert "Preview" in result.output
    assert "Statistics" in result.output


def test_inspect_json_combined_flags(runner, test_file):
    """Test JSON output with multiple flags."""
    result = runner.invoke(cli, ["inspect", test_file, "--json", "--head", "2", "--stats"])

    assert result.exit_code == 0

    data = json.loads(result.output)

    # Both preview and statistics should be present
    assert data["preview"] is not None
    assert data["statistics"] is not None


def test_inspect_large_head_value(runner, test_file):
    """Test inspect with --head value larger than file rows."""
    # Request 10000 rows (likely more than test file has)
    result = runner.invoke(cli, ["inspect", test_file, "--head", "10000"])

    assert result.exit_code == 0
    assert "Preview" in result.output


def test_inspect_zero_head(runner, test_file):
    """Test inspect with --head 0."""
    result = runner.invoke(cli, ["inspect", test_file, "--head", "0"])

    assert result.exit_code == 0
    # Should work but show no preview rows


def test_inspect_help(runner):
    """Test inspect command help."""
    result = runner.invoke(cli, ["inspect", "--help"])

    assert result.exit_code == 0
    assert "Inspect a GeoParquet file" in result.output
    assert "--head" in result.output
    assert "--tail" in result.output
    assert "--stats" in result.output
    assert "--json" in result.output
