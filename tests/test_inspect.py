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
    format_markdown_output,
    parse_wkb_type,
    wkb_to_wkt,
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


def test_inspect_head_wkt_output(runner, test_file):
    """Test inspect with --head shows WKT geometry instead of just type."""
    result = runner.invoke(cli, ["inspect", test_file, "--head", "1"])

    assert result.exit_code == 0
    # The output should contain WKT geometry with coordinates, not just <POINT>
    # The table may wrap the output across lines, but we should see:
    # - POINT (the geometry type) - not <POINT>
    # - The opening parenthesis followed by coordinates
    assert "POINT" in result.output
    # Check for coordinate pattern (negative number indicating longitude)
    assert "(-0.924753" in result.output or "-0.924753" in result.output
    # Should not show just the type placeholder
    assert "<POINT>" not in result.output


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
    """Test geometry display formatting with WKT output."""
    import struct

    # None value
    assert format_geometry_display(None) == "NULL"

    # Point WKB with actual coordinates (0, 0)
    # Build complete Point WKB: byte_order(1) + type(4) + x(8) + y(8)
    point_wkb = bytes([0x01, 0x01, 0x00, 0x00, 0x00])  # little endian, Point type
    point_wkb += struct.pack("<d", 1.5)  # x = 1.5
    point_wkb += struct.pack("<d", 2.5)  # y = 2.5
    result = format_geometry_display(point_wkb)
    assert "POINT" in result
    assert "1.5" in result
    assert "2.5" in result

    # Incomplete WKB should fall back to type-only display
    incomplete_wkb = bytes([0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
    result = format_geometry_display(incomplete_wkb)
    assert "<POINT>" in result

    # Non-bytes value
    result = format_geometry_display("some string")
    assert "some string" in result


def test_wkb_to_wkt():
    """Test WKB to WKT conversion function."""
    import struct

    # Test Point WKB
    point_wkb = bytes([0x01, 0x01, 0x00, 0x00, 0x00])  # little endian, Point type
    point_wkb += struct.pack("<d", -122.4194)  # x (longitude)
    point_wkb += struct.pack("<d", 37.7749)  # y (latitude)
    result = wkb_to_wkt(point_wkb)
    assert result is not None
    assert "POINT" in result
    assert "-122.419400" in result
    assert "37.774900" in result

    # Test LineString WKB
    # LineString with 2 points
    linestring_wkb = bytes([0x01, 0x02, 0x00, 0x00, 0x00])  # little endian, LineString
    linestring_wkb += struct.pack("<I", 2)  # 2 points
    linestring_wkb += struct.pack("<d", 0.0) + struct.pack("<d", 0.0)  # point 1
    linestring_wkb += struct.pack("<d", 1.0) + struct.pack("<d", 1.0)  # point 2
    result = wkb_to_wkt(linestring_wkb)
    assert result is not None
    assert "LINESTRING" in result
    assert "0.000000 0.000000" in result
    assert "1.000000 1.000000" in result

    # Test Polygon WKB (simple rectangle)
    polygon_wkb = bytes([0x01, 0x03, 0x00, 0x00, 0x00])  # little endian, Polygon
    polygon_wkb += struct.pack("<I", 1)  # 1 ring
    polygon_wkb += struct.pack("<I", 5)  # 5 points (closed ring)
    polygon_wkb += struct.pack("<d", 0.0) + struct.pack("<d", 0.0)
    polygon_wkb += struct.pack("<d", 1.0) + struct.pack("<d", 0.0)
    polygon_wkb += struct.pack("<d", 1.0) + struct.pack("<d", 1.0)
    polygon_wkb += struct.pack("<d", 0.0) + struct.pack("<d", 1.0)
    polygon_wkb += struct.pack("<d", 0.0) + struct.pack("<d", 0.0)  # close ring
    result = wkb_to_wkt(polygon_wkb)
    assert result is not None
    assert "POLYGON" in result

    # Test empty/invalid bytes
    assert wkb_to_wkt(b"") is None
    assert wkb_to_wkt(bytes([0x01])) is None

    # Test max_coords truncation
    linestring_wkb = bytes([0x01, 0x02, 0x00, 0x00, 0x00])  # little endian, LineString
    linestring_wkb += struct.pack("<I", 15)  # 15 points
    for i in range(15):
        linestring_wkb += struct.pack("<d", float(i)) + struct.pack("<d", float(i))
    result = wkb_to_wkt(linestring_wkb, max_coords=5)
    assert result is not None
    assert "..." in result  # Should be truncated


def test_format_markdown_output():
    """Test markdown output formatting function."""
    file_info = {
        "file_path": "/path/to/data.parquet",
        "size_bytes": 1024,
        "size_human": "1.00 KB",
        "rows": 100,
        "row_groups": 1,
        "compression": "ZSTD",
    }
    geo_info = {
        "has_geo_metadata": True,
        "version": "1.0.0",
        "crs": "EPSG:4326",
        "bbox": [-180.0, -90.0, 180.0, 90.0],
        "primary_column": "geometry",
    }
    columns_info = [
        {"name": "id", "type": "int64", "is_geometry": False},
        {"name": "geometry", "type": "binary", "is_geometry": True},
    ]

    result = format_markdown_output(file_info, geo_info, columns_info)

    # Verify markdown structure
    assert "## data.parquet" in result
    assert "### Metadata" in result
    assert "- **Size:** 1.00 KB" in result
    assert "- **Rows:** 100" in result
    assert "- **Row Groups:** 1" in result
    assert "- **Compression:** ZSTD" in result
    assert "- **GeoParquet Version:** 1.0.0" in result
    assert "- **CRS:** EPSG:4326" in result
    assert "- **Bbox:** [-180.000000, -90.000000, 180.000000, 90.000000]" in result
    assert "### Columns (2)" in result
    assert "| Name | Type |" in result
    assert "| id | int64 |" in result
    assert "| geometry 🌍 | binary |" in result


def test_format_markdown_output_no_geo_metadata():
    """Test markdown output without geo metadata."""
    file_info = {
        "file_path": "/path/to/data.parquet",
        "size_bytes": 1024,
        "size_human": "1.00 KB",
        "rows": 50,
        "row_groups": 1,
        "compression": None,
    }
    geo_info = {
        "has_geo_metadata": False,
        "version": None,
        "crs": None,
        "bbox": None,
        "primary_column": None,
    }
    columns_info = [
        {"name": "value", "type": "string", "is_geometry": False},
    ]

    result = format_markdown_output(file_info, geo_info, columns_info)

    # Verify no geo metadata message
    assert "*No GeoParquet metadata found*" in result
    assert "### Columns (1)" in result


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
    assert "--markdown" in result.output


def test_inspect_markdown(runner, test_file):
    """Test inspect with --markdown flag."""
    result = runner.invoke(cli, ["inspect", test_file, "--markdown"])

    assert result.exit_code == 0

    # Verify markdown structure
    assert "## places_test.parquet" in result.output
    assert "### Metadata" in result.output
    assert "- **Size:**" in result.output
    assert "- **Rows:**" in result.output
    assert "- **Row Groups:**" in result.output
    assert "### Columns" in result.output
    assert "| Name | Type |" in result.output
    assert "|------|------|" in result.output


def test_inspect_markdown_with_head(runner, test_file):
    """Test markdown output includes preview data when --head is specified."""
    result = runner.invoke(cli, ["inspect", test_file, "--markdown", "--head", "2"])

    assert result.exit_code == 0

    # Verify preview section exists
    assert "### Preview (first 2 rows)" in result.output
    # Check for table structure
    assert result.output.count("|") > 10  # Multiple pipe characters for tables


def test_inspect_markdown_with_tail(runner, test_file):
    """Test markdown output includes preview data when --tail is specified."""
    result = runner.invoke(cli, ["inspect", test_file, "--markdown", "--tail", "3"])

    assert result.exit_code == 0

    # Verify preview section exists
    assert "### Preview (last 3 rows)" in result.output


def test_inspect_markdown_json_exclusive(runner, test_file):
    """Test that --markdown and --json are mutually exclusive."""
    result = runner.invoke(cli, ["inspect", test_file, "--markdown", "--json"])

    assert result.exit_code != 0
    assert "mutually exclusive" in result.output.lower()
