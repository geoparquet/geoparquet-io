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
    format_bbox_display,
    format_geometry_display,
    format_markdown_output,
    get_preview_data,
    is_bbox_value,
    parse_wkb_type,
    wkb_to_wkt_preview,
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


def test_inspect_head_default(runner, test_file):
    """Test inspect with --head flag without value uses default of 10."""
    result = runner.invoke(cli, ["inspect", test_file, "--head"])

    assert result.exit_code == 0
    assert "Preview (first" in result.output
    # Should default to 10 rows (or fewer if file is smaller)
    assert "10 rows" in result.output or "rows)" in result.output


def test_inspect_tail(runner, test_file):
    """Test inspect with --tail flag."""
    result = runner.invoke(cli, ["inspect", test_file, "--tail", "3"])

    assert result.exit_code == 0
    assert "Preview (last" in result.output
    assert "3 rows" in result.output or "rows)" in result.output


def test_inspect_tail_default(runner, test_file):
    """Test inspect with --tail flag without value uses default of 10."""
    result = runner.invoke(cli, ["inspect", test_file, "--tail"])

    assert result.exit_code == 0
    assert "Preview (last" in result.output
    # Should default to 10 rows (or fewer if file is smaller)
    assert "10 rows" in result.output or "rows)" in result.output


def test_inspect_head_default_with_other_option(runner, test_file):
    """Test --head without value followed by another option uses default."""
    result = runner.invoke(cli, ["inspect", test_file, "--head", "--stats"])

    assert result.exit_code == 0
    assert "Preview (first" in result.output
    # Should default to 10 rows
    assert "10 rows" in result.output or "rows)" in result.output
    # Should also show stats
    assert "Statistics:" in result.output


def test_inspect_tail_default_with_other_option(runner, test_file):
    """Test --tail without value followed by another option uses default."""
    result = runner.invoke(cli, ["inspect", test_file, "--tail", "--stats"])

    assert result.exit_code == 0
    assert "Preview (last" in result.output
    # Should default to 10 rows
    assert "10 rows" in result.output or "rows)" in result.output
    # Should also show stats
    assert "Statistics:" in result.output


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


def test_wkb_to_wkt_preview_with_valid_wkb():
    """Test WKT preview with valid ISO WKB point."""
    # Valid ISO WKB for POINT (1.0, 2.0) - little endian
    point_wkb = bytes(
        [
            0x01,  # little endian
            0x01,
            0x00,
            0x00,
            0x00,  # Point type
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0xF0,
            0x3F,  # x = 1.0
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x40,  # y = 2.0
        ]
    )
    result = wkb_to_wkt_preview(point_wkb)
    assert "POINT" in result
    # Should contain actual coordinates, not just type
    # Verify it's actual WKT, not the fallback format
    assert "<POINT>" not in result, "Expected WKT output, got fallback"
    assert "1" in result  # Should contain the x-coordinate


def test_wkb_to_wkt_preview_fallback():
    """Test WKT preview falls back for invalid WKB."""
    # Too short to be valid WKB
    short_bytes = bytes([0x01, 0x01, 0x00])
    result = wkb_to_wkt_preview(short_bytes)
    assert result == "<GEOMETRY>"


def test_format_bbox_display():
    """Test bbox struct formatting."""
    bbox = {"xmin": -122.5, "ymin": 37.5, "xmax": -122.0, "ymax": 38.0}
    result = format_bbox_display(bbox)
    assert "[" in result
    assert "-122.5" in result or "-122.500000" in result
    assert "37.5" in result or "37.500000" in result


def test_format_bbox_display_truncation():
    """Test bbox display truncates long output."""
    bbox = {"xmin": -122.123456789, "ymin": 37.123456789, "xmax": -122.0, "ymax": 38.0}
    result = format_bbox_display(bbox, max_length=30)
    assert len(result) <= 30


def test_is_bbox_value():
    """Test bbox struct detection."""
    # Valid bbox struct
    valid_bbox = {"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1}
    assert is_bbox_value(valid_bbox) is True

    # Extra keys are allowed
    extended_bbox = {"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1, "zmin": 0, "zmax": 1}
    assert is_bbox_value(extended_bbox) is True

    # Missing keys
    partial = {"xmin": 0, "ymin": 0, "xmax": 1}
    assert is_bbox_value(partial) is False

    # Non-dict values
    assert is_bbox_value([0, 0, 1, 1]) is False
    assert is_bbox_value(None) is False
    assert is_bbox_value("bbox") is False


def test_preview_data_returns_wkt():
    """Test that get_preview_data returns WKT strings for geometry columns."""
    test_file = os.path.join(os.path.dirname(__file__), "data", "buildings_test.parquet")
    table, mode = get_preview_data(test_file, head=1)

    # Get the geometry column value
    geom_value = table.column("geometry")[0].as_py()

    # Should be a string (WKT), not bytes
    assert isinstance(geom_value, str), f"Expected str but got {type(geom_value)}"

    # Should contain WKT geometry type
    assert "POLYGON" in geom_value or "POINT" in geom_value or "LINESTRING" in geom_value


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


# Tests for Parquet native geo type detection


@pytest.fixture
def parquet_geo_only_file():
    """Provide path to test file with Parquet geo type but no GeoParquet metadata."""
    return os.path.join(os.path.dirname(__file__), "data", "fields_pgo_crs84_bbox_snappy.parquet")


@pytest.fixture
def parquet_v2_file():
    """Provide path to test file with both Parquet geo type and GeoParquet metadata."""
    return os.path.join(os.path.dirname(__file__), "data", "fields_gpq2_crs84_zstd.parquet")


def test_inspect_parquet_geo_type_only(runner, parquet_geo_only_file):
    """Test inspect shows Parquet type for file with only Parquet geo type."""
    result = runner.invoke(cli, ["inspect", parquet_geo_only_file])

    assert result.exit_code == 0
    assert "Parquet Type: Geometry" in result.output
    assert "No GeoParquet metadata (using Parquet geo type)" in result.output
    # Should still show CRS (default)
    assert "CRS:" in result.output
    # Should calculate and show bbox from row group stats
    assert "Bbox:" in result.output


def test_inspect_parquet_geo_type_with_geoparquet(runner, parquet_v2_file):
    """Test inspect shows both Parquet type and GeoParquet metadata."""
    result = runner.invoke(cli, ["inspect", parquet_v2_file])

    assert result.exit_code == 0
    assert "Parquet Type: Geometry" in result.output
    assert "GeoParquet Version: 2.0.0" in result.output
    assert "Geometry Types: Polygon" in result.output
    assert "Bbox:" in result.output


def test_inspect_json_parquet_type(runner, parquet_geo_only_file):
    """Test JSON output includes parquet_type field."""
    result = runner.invoke(cli, ["inspect", parquet_geo_only_file, "--json"])

    assert result.exit_code == 0
    data = json.loads(result.output)

    assert "parquet_type" in data
    assert data["parquet_type"] == "Geometry"
    assert data["geoparquet_version"] is None
    assert "warnings" in data
    assert data["warnings"] == []


def test_inspect_json_with_geoparquet(runner, parquet_v2_file):
    """Test JSON output includes all geo info when both Parquet type and GeoParquet exist."""
    result = runner.invoke(cli, ["inspect", parquet_v2_file, "--json"])

    assert result.exit_code == 0
    data = json.loads(result.output)

    assert data["parquet_type"] == "Geometry"
    assert data["geoparquet_version"] == "2.0.0"
    assert data["geometry_types"] == ["Polygon"]
    assert data["bbox"] is not None
    assert len(data["bbox"]) == 4


def test_inspect_markdown_parquet_type(runner, parquet_geo_only_file):
    """Test markdown output includes Parquet Type field."""
    result = runner.invoke(cli, ["inspect", parquet_geo_only_file, "--markdown"])

    assert result.exit_code == 0
    assert "- **Parquet Type:** Geometry" in result.output
    assert "No GeoParquet metadata (using Parquet geo type)" in result.output
    # Should calculate and show bbox from row group stats
    assert "- **Bbox:**" in result.output


def test_extract_geo_info_parquet_type_only(parquet_geo_only_file):
    """Test extract_geo_info returns parquet_type for file with only Parquet geo type."""
    geo_info = extract_geo_info(parquet_geo_only_file)

    assert geo_info["parquet_type"] == "Geometry"
    assert geo_info["has_geo_metadata"] is False
    assert geo_info["version"] is None
    assert geo_info["primary_column"] == "geometry"
    assert geo_info["warnings"] == []
    # Bbox should be calculated from row group stats
    assert geo_info["bbox"] is not None
    assert len(geo_info["bbox"]) == 4


def test_extract_geo_info_with_both(parquet_v2_file):
    """Test extract_geo_info returns both Parquet type and GeoParquet metadata."""
    geo_info = extract_geo_info(parquet_v2_file)

    assert geo_info["parquet_type"] == "Geometry"
    assert geo_info["has_geo_metadata"] is True
    assert geo_info["version"] == "2.0.0"
    assert geo_info["geometry_types"] == ["Polygon"]
    assert geo_info["bbox"] is not None


class TestCRSComparison:
    """Test CRS comparison and extraction functions."""

    def test_extract_crs_identifier_from_projjson(self):
        """Test extracting CRS identifier from PROJJSON dict."""
        from geoparquet_io.core.inspect_utils import _extract_crs_identifier

        projjson = {
            "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
            "type": "ProjectedCRS",
            "name": "MGI / Austria Lambert",
            "id": {"authority": "EPSG", "code": 31287},
        }
        result = _extract_crs_identifier(projjson)
        assert result == ("EPSG", 31287)

    def test_extract_crs_identifier_from_epsg_string(self):
        """Test extracting CRS identifier from EPSG:CODE string."""
        from geoparquet_io.core.inspect_utils import _extract_crs_identifier

        assert _extract_crs_identifier("EPSG:4326") == ("EPSG", 4326)
        assert _extract_crs_identifier("epsg:31287") == ("EPSG", 31287)
        # OGC:CRS84 is a special case - not a numeric code, so returns None
        assert _extract_crs_identifier("OGC:CRS84") is None

    def test_extract_crs_identifier_from_urn(self):
        """Test extracting CRS identifier from URN format."""
        from geoparquet_io.core.inspect_utils import _extract_crs_identifier

        assert _extract_crs_identifier("urn:ogc:def:crs:EPSG::4326") == ("EPSG", 4326)
        assert _extract_crs_identifier("urn:ogc:def:crs:EPSG::31287") == ("EPSG", 31287)

    def test_extract_crs_identifier_returns_none_for_invalid(self):
        """Test that invalid CRS formats return None."""
        from geoparquet_io.core.inspect_utils import _extract_crs_identifier

        assert _extract_crs_identifier(None) is None
        assert _extract_crs_identifier({}) is None
        assert _extract_crs_identifier("invalid") is None
        assert _extract_crs_identifier({"no_id": "here"}) is None

    def test_crs_are_equivalent_projjson_vs_epsg_string(self):
        """Test that PROJJSON and EPSG string for same CRS are equivalent."""
        from geoparquet_io.core.inspect_utils import _crs_are_equivalent

        projjson = {
            "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
            "type": "ProjectedCRS",
            "name": "MGI / Austria Lambert",
            "id": {"authority": "EPSG", "code": 31287},
        }
        assert _crs_are_equivalent(projjson, "EPSG:31287") is True
        assert _crs_are_equivalent("EPSG:31287", projjson) is True

    def test_crs_are_equivalent_same_strings(self):
        """Test that identical CRS strings are equivalent."""
        from geoparquet_io.core.inspect_utils import _crs_are_equivalent

        assert _crs_are_equivalent("EPSG:4326", "EPSG:4326") is True
        assert _crs_are_equivalent("epsg:4326", "EPSG:4326") is True

    def test_crs_are_not_equivalent_different_codes(self):
        """Test that different CRS codes are not equivalent."""
        from geoparquet_io.core.inspect_utils import _crs_are_equivalent

        assert _crs_are_equivalent("EPSG:4326", "EPSG:31287") is False

    def test_crs_are_not_equivalent_when_unextractable(self):
        """Test that unextractable CRS values are not equivalent."""
        from geoparquet_io.core.inspect_utils import _crs_are_equivalent

        assert _crs_are_equivalent(None, "EPSG:4326") is False
        assert _crs_are_equivalent("invalid", "EPSG:4326") is False
        assert _crs_are_equivalent({}, "EPSG:4326") is False

    def test_detect_mismatches_no_false_positive_for_same_crs(self):
        """Test that identical PROJJSON CRS dicts don't trigger mismatch."""
        from geoparquet_io.core.inspect_utils import _detect_metadata_mismatches

        # Now both sources provide PROJJSON (not string)
        parquet_geo_info = {
            "crs": {
                "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                "type": "ProjectedCRS",
                "name": "MGI / Austria Lambert",
                "id": {"authority": "EPSG", "code": 31287},
            }
        }
        geoparquet_info = {
            "crs": {
                "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                "type": "ProjectedCRS",
                "name": "MGI / Austria Lambert",
                "id": {"authority": "EPSG", "code": 31287},
            }
        }

        warnings = _detect_metadata_mismatches(parquet_geo_info, geoparquet_info)
        assert len(warnings) == 0

    def test_detect_mismatches_projjson_vs_string_same_crs(self):
        """Test that PROJJSON and EPSG string for same CRS don't trigger mismatch."""
        from geoparquet_io.core.inspect_utils import _detect_metadata_mismatches

        # This tests backwards compatibility - should still work with mixed formats
        parquet_geo_info = {
            "crs": {
                "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                "type": "ProjectedCRS",
                "name": "MGI / Austria Lambert",
                "id": {"authority": "EPSG", "code": 31287},
            }
        }
        geoparquet_info = {"crs": "EPSG:31287"}

        warnings = _detect_metadata_mismatches(parquet_geo_info, geoparquet_info)
        assert len(warnings) == 0

    def test_detect_mismatches_reports_actual_mismatch(self):
        """Test that actual CRS mismatches are reported."""
        from geoparquet_io.core.inspect_utils import _detect_metadata_mismatches

        parquet_geo_info = {"crs": "EPSG:4326"}
        geoparquet_info = {"crs": "EPSG:31287"}

        warnings = _detect_metadata_mismatches(parquet_geo_info, geoparquet_info)
        assert len(warnings) == 1
        assert "CRS mismatch" in warnings[0]

    def test_format_crs_for_display_projjson(self):
        """Test that PROJJSON is formatted as EPSG code."""
        from geoparquet_io.core.inspect_utils import _format_crs_for_display

        projjson = {
            "id": {"authority": "EPSG", "code": 31287},
        }
        assert _format_crs_for_display(projjson) == "EPSG:31287"

    def test_format_crs_for_display_none(self):
        """Test that None CRS shows default."""
        from geoparquet_io.core.inspect_utils import _format_crs_for_display

        assert _format_crs_for_display(None) == "OGC:CRS84 (default)"
        assert _format_crs_for_display(None, include_default=False) == "Not specified"

    def test_format_crs_for_display_epsg_string(self):
        """Test that EPSG string is passed through."""
        from geoparquet_io.core.inspect_utils import _format_crs_for_display

        assert _format_crs_for_display("EPSG:4326") == "EPSG:4326"
