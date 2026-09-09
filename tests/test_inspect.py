"""Tests for the inspect command."""

import json
import os

import pyarrow as pa
import pytest
from click.testing import CliRunner
from rich.console import Console

from geoparquet_io.cli.main import cli
from geoparquet_io.core.inspect_utils import (
    _columns_that_fit,
    _create_preview_table,
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
    """Test inspect head subcommand."""
    result = runner.invoke(cli, ["inspect", "head", test_file, "5"])

    assert result.exit_code == 0
    assert "Preview (first" in result.output
    assert (
        "5 rows" in result.output or "rows)" in result.output
    )  # May show fewer if file has < 5 rows


def test_inspect_head_default(runner, test_file):
    """Test inspect head subcommand without count uses default of 10."""
    result = runner.invoke(cli, ["inspect", "head", test_file])

    assert result.exit_code == 0
    assert "Preview (first" in result.output
    # Should default to 10 rows (or fewer if file is smaller)
    assert "10 rows" in result.output or "rows)" in result.output


def test_inspect_tail(runner, test_file):
    """Test inspect tail subcommand."""
    result = runner.invoke(cli, ["inspect", "tail", test_file, "3"])

    assert result.exit_code == 0
    assert "Preview (last" in result.output
    assert "3 rows" in result.output or "rows)" in result.output


def test_inspect_tail_default(runner, test_file):
    """Test inspect tail subcommand without count uses default of 10."""
    result = runner.invoke(cli, ["inspect", "tail", test_file])

    assert result.exit_code == 0
    assert "Preview (last" in result.output
    # Should default to 10 rows (or fewer if file is smaller)
    assert "10 rows" in result.output or "rows)" in result.output


def test_inspect_stats(runner, test_file):
    """Test inspect stats subcommand."""
    result = runner.invoke(cli, ["inspect", "stats", test_file])

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
    """Test JSON output includes preview data when head subcommand is used."""
    result = runner.invoke(cli, ["inspect", "head", test_file, "2", "--json"])

    assert result.exit_code == 0

    data = json.loads(result.output)

    # Preview should contain data
    assert data["preview"] is not None
    assert isinstance(data["preview"], list)
    # Should have at most 2 rows (or fewer if file is smaller)
    assert len(data["preview"]) <= 2


def test_inspect_json_with_stats(runner, test_file):
    """Test JSON output includes statistics when stats subcommand is used."""
    result = runner.invoke(cli, ["inspect", "stats", test_file, "--json"])

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


def test_inspect_non_parquet_file(runner, tmp_path):
    """Test inspect with non-parquet file gives friendly error."""
    # Create a CSV file
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("id,name\n1,test\n")

    result = runner.invoke(cli, ["inspect", str(csv_file)])

    assert result.exit_code != 0
    assert "only works with Parquet files" in result.output
    assert ".csv" in result.output


def test_inspect_head_non_parquet_file(runner, tmp_path):
    """Test inspect head with non-parquet file gives friendly error."""
    csv_file = tmp_path / "data.geojson"
    csv_file.write_text('{"type": "FeatureCollection", "features": []}')

    result = runner.invoke(cli, ["inspect", "head", str(csv_file)])

    assert result.exit_code != 0
    assert "only works with Parquet files" in result.output
    assert ".geojson" in result.output


def test_inspect_stats_non_parquet_file(runner, tmp_path):
    """Test inspect stats with non-parquet file gives friendly error."""
    txt_file = tmp_path / "notes.txt"
    txt_file.write_text("Some notes")

    result = runner.invoke(cli, ["inspect", "stats", str(txt_file)])

    assert result.exit_code != 0
    assert "only works with Parquet files" in result.output
    assert ".txt" in result.output


def test_inspect_misnamed_parquet_file(runner, tmp_path):
    """Test inspect with CSV file misnamed as .parquet gives friendly error."""
    # Create a CSV file with .parquet extension
    fake_parquet = tmp_path / "data.parquet"
    fake_parquet.write_text("id,name\n1,test\n2,example\n")

    result = runner.invoke(cli, ["inspect", str(fake_parquet)])

    assert result.exit_code != 0
    assert "has a .parquet extension but is not a valid Parquet file" in result.output
    assert "data.parquet" in result.output


def test_validate_parquet_input_presigned_url():
    """Test that presigned URLs with query strings are handled correctly."""
    from geoparquet_io.cli.commands.inspect import _validate_parquet_input

    # Presigned URL with .parquet extension should pass validation
    presigned_url = (
        "https://bucket.s3.amazonaws.com/data.parquet?X-Amz-Algorithm=AWS4&X-Amz-Signature=abc123"
    )
    # Should not raise (parquet extension is correctly detected)
    _validate_parquet_input(presigned_url)

    # Presigned URL without .parquet should fail
    presigned_csv = "https://bucket.s3.amazonaws.com/data.csv?X-Amz-Algorithm=AWS4"
    with pytest.raises(Exception) as exc_info:
        _validate_parquet_input(presigned_csv)
    assert "only works with Parquet files" in str(exc_info.value)


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

    from geoparquet_io.core.file_utils import safe_file_url

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


def test_preview_data_returns_wkt_for_native_geoarrow():
    """get_preview_data must convert native GeoArrow geometry to WKT (not crash).

    Native GeoArrow geometry is exposed by DuckDB as a raw STRUCT/LIST, which
    ST_AsText cannot handle. head/tail must detect this and convert via geoarrow.
    """
    test_file = os.path.join(
        os.path.dirname(__file__), "data", "data-polygon-encoding_native.parquet"
    )
    table, mode = get_preview_data(test_file, head=1)

    geom_value = table.column("geometry")[0].as_py()
    assert isinstance(geom_value, str), f"Expected WKT str but got {type(geom_value)}"
    assert "POLYGON" in geom_value


@pytest.mark.parametrize(
    "geom_type,wkt_prefix",
    [
        ("point", "POINT"),
        ("linestring", "LINESTRING"),
        ("polygon", "POLYGON"),
        ("multipoint", "MULTIPOINT"),
        ("multilinestring", "MULTILINESTRING"),
        ("multipolygon", "MULTIPOLYGON"),
    ],
)
def test_preview_native_geoarrow_all_types(geom_type, wkt_prefix):
    """All native GeoArrow geometry types render to WKT in head, including NULLs."""
    test_file = os.path.join(
        os.path.dirname(__file__), "data", f"data-{geom_type}-encoding_native.parquet"
    )
    table, _ = get_preview_data(test_file, head=10)
    values = table.column("geometry").to_pylist()
    # Every non-null value is a WKT string of the expected type; NULLs pass through.
    non_null = [v for v in values if v is not None]
    assert non_null, "expected at least one non-null geometry"
    assert all(isinstance(v, str) for v in non_null)
    assert all(v.startswith(wkt_prefix) for v in non_null), (
        f"every non-null value must render as {wkt_prefix} WKT, got {non_null}"
    )


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


def test_inspect_help(runner):
    """Test inspect command group help shows available subcommands."""
    result = runner.invoke(cli, ["inspect", "--help"])

    assert result.exit_code == 0
    assert "Inspect GeoParquet files" in result.output
    # Check for subcommands
    assert "head" in result.output
    assert "tail" in result.output
    assert "stats" in result.output
    assert "meta" in result.output
    assert "summary" in result.output
    # Check for Commands section
    assert "Commands:" in result.output


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
        # OGC:CRS84 has non-numeric code, returned as string
        assert _extract_crs_identifier("OGC:CRS84") == ("OGC", "CRS84")

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

    def test_no_warning_when_parquet_has_default_crs_and_geoparquet_omits(self):
        """Missing CRS in GeoParquet metadata means OGC:CRS84 — no warning if Parquet agrees."""
        from geoparquet_io.core.inspect_utils import _detect_metadata_mismatches

        parquet_geo_info = {
            "crs": {
                "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                "type": "GeographicCRS",
                "name": "WGS 84 (CRS84)",
                "id": {"authority": "OGC", "code": "CRS84"},
            }
        }
        geoparquet_info = {}

        warnings = _detect_metadata_mismatches(parquet_geo_info, geoparquet_info)
        assert len(warnings) == 0

    def test_no_warning_when_parquet_has_epsg4326_and_geoparquet_omits(self):
        """EPSG:4326 is also a default CRS — no warning if GeoParquet metadata omits CRS."""
        from geoparquet_io.core.inspect_utils import _detect_metadata_mismatches

        parquet_geo_info = {
            "crs": {
                "type": "GeographicCRS",
                "name": "WGS 84",
                "id": {"authority": "EPSG", "code": 4326},
            }
        }
        geoparquet_info = {}

        warnings = _detect_metadata_mismatches(parquet_geo_info, geoparquet_info)
        assert len(warnings) == 0

    def test_warning_when_parquet_has_non_default_crs_and_geoparquet_omits(self):
        """Non-default CRS in Parquet but missing in GeoParquet should still warn."""
        from geoparquet_io.core.inspect_utils import _detect_metadata_mismatches

        parquet_geo_info = {
            "crs": {
                "type": "ProjectedCRS",
                "name": "NAD83 / Conus Albers",
                "id": {"authority": "EPSG", "code": 5070},
            }
        }
        geoparquet_info = {}

        warnings = _detect_metadata_mismatches(parquet_geo_info, geoparquet_info)
        assert len(warnings) == 1
        assert "missing in GeoParquet metadata" in warnings[0]

    def test_crs_are_equivalent_projjson_without_id(self):
        """PROJJSON dicts without id fields should use pyproj semantic comparison."""
        from geoparquet_io.core.inspect_utils import _crs_are_equivalent

        crs1 = {
            "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
            "type": "GeographicCRS",
            "name": "WGS 84",
            "datum": {
                "type": "GeodeticReferenceFrame",
                "name": "World Geodetic System 1984",
                "ellipsoid": {
                    "name": "WGS 84",
                    "semi_major_axis": 6378137,
                    "inverse_flattening": 298.257223563,
                },
            },
            "coordinate_system": {
                "subtype": "ellipsoidal",
                "axis": [
                    {
                        "name": "Geodetic latitude",
                        "abbreviation": "Lat",
                        "direction": "north",
                        "unit": "degree",
                    },
                    {
                        "name": "Geodetic longitude",
                        "abbreviation": "Lon",
                        "direction": "east",
                        "unit": "degree",
                    },
                ],
            },
        }
        crs2 = dict(crs1)

        assert _crs_are_equivalent(crs1, crs2) is True

    def test_crs_are_equivalent_projjson_without_id_different(self):
        """Different PROJJSON dicts without id fields should not be equivalent."""
        from geoparquet_io.core.inspect_utils import _crs_are_equivalent

        crs1 = {
            "type": "GeographicCRS",
            "name": "WGS 84",
            "datum": {
                "type": "GeodeticReferenceFrame",
                "name": "World Geodetic System 1984",
                "ellipsoid": {
                    "name": "WGS 84",
                    "semi_major_axis": 6378137,
                    "inverse_flattening": 298.257223563,
                },
            },
            "coordinate_system": {
                "subtype": "ellipsoidal",
                "axis": [
                    {
                        "name": "Geodetic latitude",
                        "abbreviation": "Lat",
                        "direction": "north",
                        "unit": "degree",
                    },
                    {
                        "name": "Geodetic longitude",
                        "abbreviation": "Lon",
                        "direction": "east",
                        "unit": "degree",
                    },
                ],
            },
        }
        crs2 = {
            "type": "ProjectedCRS",
            "name": "NAD83 / Conus Albers",
            "datum": {
                "type": "GeodeticReferenceFrame",
                "name": "North American Datum 1983",
                "ellipsoid": {
                    "name": "GRS 1980",
                    "semi_major_axis": 6378137,
                    "inverse_flattening": 298.257222101,
                },
            },
        }

        assert _crs_are_equivalent(crs1, crs2) is False


@pytest.fixture(scope="module")
def v2_multi_rg_file(tmp_path_factory):
    """Create a GeoParquet 2.0 file with multiple row groups and native geo_bbox."""
    import duckdb

    output = str(tmp_path_factory.mktemp("v2") / "v2_multi_rg.parquet")
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    con.execute(
        """
        COPY (
            SELECT
                ST_Point(i % 360 - 180, (i % 180) - 90) AS geometry,
                i AS id
            FROM range(500000) t(i)
        )
        TO '{}'
        (FORMAT PARQUET, COMPRESSION ZSTD, GEOPARQUET_VERSION 'V2', ROW_GROUP_SIZE 100000)
    """.format(output.replace("'", "''"))
    )
    con.close()
    return output


@pytest.fixture(scope="module")
def v11_multi_rg_file(tmp_path_factory):
    """Create a GeoParquet 1.1 file with multiple row groups and a bbox column."""
    import duckdb

    from geoparquet_io.core.convert import convert_to_geoparquet

    tmpdir = tmp_path_factory.mktemp("v11")
    v2_path = str(tmpdir / "v2_source.parquet")
    output = str(tmpdir / "v11_multi_rg.parquet")
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    con.execute(
        """
        COPY (
            SELECT
                ST_Point(i % 360 - 180, (i % 180) - 90) AS geometry,
                i AS id
            FROM range(500000) t(i)
        )
        TO '{}'
        (FORMAT PARQUET, COMPRESSION ZSTD, GEOPARQUET_VERSION 'V1', ROW_GROUP_SIZE 100000)
    """.format(v2_path.replace("'", "''"))
    )
    con.close()

    convert_to_geoparquet(
        v2_path,
        output,
        skip_hilbert=True,
        verbose=False,
        geoparquet_version="1.1",
    )
    return output


@pytest.fixture(scope="module")
def v2_multi_geom_file(tmp_path_factory):
    """Create a GeoParquet 2.0 file with two geometry columns."""
    import duckdb

    output = str(tmp_path_factory.mktemp("v2mg") / "v2_multi_geom.parquet")
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    con.execute(
        """
        COPY (
            SELECT
                ST_Point(i % 360 - 180, (i % 180) - 90) AS geometry,
                ST_Point((i + 100) % 360 - 180, ((i + 50) % 180) - 90) AS geometry2,
                i AS id
            FROM range(500000) t(i)
        )
        TO '{}'
        (FORMAT PARQUET, COMPRESSION ZSTD, GEOPARQUET_VERSION 'V2', ROW_GROUP_SIZE 100000)
    """.format(output.replace("'", "''"))
    )
    con.close()
    return output


# Tests for new subcommand structure
class TestInspectSubcommands:
    """Tests for the new inspect subcommand structure."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def test_file(self):
        return os.path.join(os.path.dirname(__file__), "data", "places_test.parquet")

    def test_inspect_summary_subcommand(self, runner, test_file):
        """Test explicit summary subcommand."""
        result = runner.invoke(cli, ["inspect", "summary", test_file])

        assert result.exit_code == 0
        assert "Rows:" in result.output
        assert "Columns" in result.output

    def test_inspect_head_subcommand_with_count(self, runner, test_file):
        """Test head subcommand with explicit count."""
        result = runner.invoke(cli, ["inspect", "head", test_file, "3"])

        assert result.exit_code == 0
        assert "Preview (first" in result.output
        assert "3 rows" in result.output or "rows)" in result.output

    def test_inspect_tail_subcommand_json(self, runner, test_file):
        """Test tail subcommand with JSON output."""
        result = runner.invoke(cli, ["inspect", "tail", test_file, "--json"])

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["preview"] is not None

    def test_inspect_meta_subcommand(self, runner, test_file):
        """Test meta subcommand."""
        result = runner.invoke(cli, ["inspect", "meta", test_file])

        assert result.exit_code == 0
        # Should show metadata output
        assert "Parquet" in result.output or "GeoParquet" in result.output

    def test_inspect_meta_subcommand_geo_flag(self, runner, test_file):
        """Test meta subcommand with --geo flag."""
        result = runner.invoke(cli, ["inspect", "meta", test_file, "--geo"])

        assert result.exit_code == 0

    def test_inspect_meta_geo_stats_flag(self, runner, test_file):
        """Test meta subcommand with --geo-stats flag shows per-RG bbox table."""
        result = runner.invoke(cli, ["inspect", "meta", test_file, "--geo-stats"])

        assert result.exit_code == 0
        assert "Row Group" in result.output
        assert "xmin" in result.output
        assert "ymin" in result.output
        assert "xmax" in result.output
        assert "ymax" in result.output

    def test_inspect_meta_geo_stats_json(self, runner, test_file):
        """Test meta subcommand with --geo-stats and --json outputs valid JSON."""
        result = runner.invoke(cli, ["inspect", "meta", test_file, "--geo-stats", "--json"])

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "row_group_geo_stats" in data
        assert isinstance(data["row_group_geo_stats"], list)
        if len(data["row_group_geo_stats"]) > 0:
            first = data["row_group_geo_stats"][0]
            assert "row_group_id" in first
            assert "xmin" in first
            assert "ymin" in first
            assert "xmax" in first
            assert "ymax" in first

    def test_inspect_meta_geo_stats_no_bbox(self, runner):
        """Test --geo-stats on file without bbox/native stats shows informative message."""
        no_bbox_file = os.path.join(os.path.dirname(__file__), "data", "buildings_test.parquet")
        result = runner.invoke(cli, ["inspect", "meta", no_bbox_file, "--geo-stats"])

        assert result.exit_code == 0
        # Should indicate no geo stats available (native or bbox column)
        assert "No geo statistics found" in result.output or "no geo" in result.output.lower()

    def test_inspect_meta_without_geo_stats_unchanged(self, runner, test_file):
        """Test that without --geo-stats the output does not include the geo stats table."""
        result = runner.invoke(cli, ["inspect", "meta", test_file])

        assert result.exit_code == 0
        # The dedicated geo stats table header should not appear without the flag
        assert "Per-Row-Group geo_bbox Statistics" not in result.output

    def test_inspect_meta_geo_stats_python_api(self, test_file):
        """Test Python API for getting per-row-group geo_bbox stats."""
        from geoparquet_io.api.ops import get_row_group_geo_stats

        stats = get_row_group_geo_stats(test_file)
        assert isinstance(stats, list)
        assert len(stats) > 0
        first = stats[0]
        assert "row_group_id" in first
        assert "num_rows" in first
        assert "xmin" in first
        assert "ymin" in first
        assert "xmax" in first
        assert "ymax" in first
        assert first["num_rows"] > 0

    def test_inspect_meta_geo_stats_python_api_no_bbox(self):
        """Test Python API returns empty list for files without bbox."""
        from geoparquet_io.api.ops import get_row_group_geo_stats

        no_bbox_file = os.path.join(os.path.dirname(__file__), "data", "buildings_test.parquet")
        stats = get_row_group_geo_stats(no_bbox_file)
        assert stats == []

    # --- V2 multi-row-group geo_bbox display tests ---

    @pytest.mark.slow
    @pytest.mark.integration
    def test_geo_stats_defaults_to_one_row_group_with_hint(self, runner, v2_multi_rg_file):
        """--geo-stats should show 1 row group by default with '... and N more' hint."""
        result = runner.invoke(cli, ["inspect", "meta", v2_multi_rg_file, "--geo-stats"])

        assert result.exit_code == 0
        assert "4 more row group(s)" in result.output
        assert "--row-groups 5" in result.output

    @pytest.mark.slow
    @pytest.mark.integration
    def test_geo_stats_shows_all_with_row_groups_flag(self, runner, v2_multi_rg_file):
        """--geo-stats with --row-groups 5 should show all 5 row groups."""
        result = runner.invoke(
            cli, ["inspect", "meta", v2_multi_rg_file, "--geo-stats", "--row-groups", "5"]
        )

        assert result.exit_code == 0
        assert "more row group(s)" not in result.output

    @pytest.mark.slow
    @pytest.mark.integration
    def test_geo_stats_respects_row_groups_limit(self, runner, v2_multi_rg_file):
        """--geo-stats with --row-groups N should show only N row groups."""
        result = runner.invoke(
            cli, ["inspect", "meta", v2_multi_rg_file, "--geo-stats", "--row-groups", "2"]
        )

        assert result.exit_code == 0
        assert "3 more row group(s)" in result.output
        assert "--row-groups 5" in result.output

    @pytest.mark.slow
    @pytest.mark.integration
    def test_inspect_meta_geometry_shows_geo_bbox_minmax(self, runner, v2_multi_rg_file):
        """V2 geometry column should show geo_bbox coords as MinValue/MaxValue, not '-'."""
        result = runner.invoke(cli, ["inspect", "meta", v2_multi_rg_file, "--row-groups", "1"])

        assert result.exit_code == 0
        lines = result.output.split("\n")
        geo_line = next(
            (line for line in lines if "BYTE_ARRAY" in line and "Geometry" in line),
            None,
        )
        assert geo_line is not None, "Could not find geometry column in row group output"
        assert "(" in geo_line, (
            f"Geometry MinValue/MaxValue should show geo_bbox coordinates, got: {geo_line}"
        )

    @pytest.mark.slow
    @pytest.mark.integration
    def test_parquet_geo_section_shows_native_row_group_stats(self, runner, v2_multi_rg_file):
        """Parquet Geo section should show per-RG bbox stats from native geo_bbox for V2."""
        result = runner.invoke(
            cli, ["inspect", "meta", v2_multi_rg_file, "--parquet-geo", "--row-groups", "2"]
        )

        assert result.exit_code == 0
        assert "Row Group" in result.output
        assert "Bbox:" in result.output or "xmin" in result.output.lower()

    # --- V1.1 regression tests ---

    @pytest.mark.slow
    @pytest.mark.integration
    def test_v11_geo_stats_defaults_to_one_with_hint(self, runner, v11_multi_rg_file):
        """--geo-stats should show 1 row group by default with hint for V1.1 files."""
        result = runner.invoke(cli, ["inspect", "meta", v11_multi_rg_file, "--geo-stats"])

        assert result.exit_code == 0
        assert "more row group(s)" in result.output
        assert "--row-groups" in result.output

    @pytest.mark.slow
    @pytest.mark.integration
    def test_v11_geo_stats_respects_row_groups_limit(self, runner, v11_multi_rg_file):
        """--geo-stats with --row-groups N should work for V1.1 bbox column files."""
        result = runner.invoke(
            cli, ["inspect", "meta", v11_multi_rg_file, "--geo-stats", "--row-groups", "2"]
        )

        assert result.exit_code == 0
        assert "more row group(s)" in result.output

    def test_v11_inspect_meta_unchanged(self, runner):
        """V1.1 file inspect meta should continue working (no regression)."""
        v11_file = os.path.join(os.path.dirname(__file__), "data", "places_test.parquet")
        result = runner.invoke(cli, ["inspect", "meta", v11_file])

        assert result.exit_code == 0
        assert "GeoParquet Metadata" in result.output
        assert "Parquet File Metadata" in result.output

    def test_v11_inspect_meta_row_groups_default_still_1(self, runner):
        """Default row groups display (without --geo-stats) should still be 1."""
        v11_file = os.path.join(os.path.dirname(__file__), "data", "places_test.parquet")
        result = runner.invoke(cli, ["inspect", "meta", v11_file])

        assert result.exit_code == 0
        # Should show "showing 1 of N" for the parquet section
        assert "Row Group 0:" in result.output

    # --- Multi-geometry tests ---

    @pytest.mark.slow
    @pytest.mark.integration
    def test_multi_geom_parquet_geo_shows_per_column_stats(self, runner, v2_multi_geom_file):
        """Each geometry column should get its own native geo_bbox stats."""
        result = runner.invoke(
            cli,
            ["inspect", "meta", v2_multi_geom_file, "--parquet-geo", "--row-groups", "5"],
        )

        assert result.exit_code == 0
        assert "geometry" in result.output
        assert "geometry2" in result.output
        output_lower = result.output.lower()
        assert output_lower.count("xmin") >= 2 or result.output.count("Bbox:") >= 2

    def test_inspect_stats_subcommand_markdown(self, runner, test_file):
        """Test stats subcommand with markdown output."""
        result = runner.invoke(cli, ["inspect", "stats", test_file, "--markdown"])

        assert result.exit_code == 0
        # Should contain markdown formatting
        assert "#" in result.output or "Statistics" in result.output

    def test_inspect_head_subcommand_help(self, runner):
        """Test head subcommand help."""
        result = runner.invoke(cli, ["inspect", "head", "--help"])

        assert result.exit_code == 0
        assert "Show first N rows" in result.output


def test_columns_that_fit_wide_terminal_shows_more():
    # width 200, 50 columns -> several columns fit, capped at total
    n = _columns_that_fit(50, 200)
    assert 1 < n < 50


def test_columns_that_fit_never_exceeds_total():
    assert _columns_that_fit(3, 500) == 3


def test_columns_that_fit_always_at_least_one():
    assert _columns_that_fit(40, 10) == 1


def test_columns_that_fit_honors_max_columns():
    assert _columns_that_fit(40, 500, max_columns=5) == 5


def test_columns_that_fit_max_columns_clamped_to_total():
    assert _columns_that_fit(3, 500, max_columns=10) == 3


def test_columns_that_fit_max_columns_floor_one():
    # max_columns below 1 should never drop below 1 (Click also guards this)
    assert _columns_that_fit(40, 500, max_columns=1) == 1
    assert _columns_that_fit(40, 500, max_columns=0) == 1
    assert _columns_that_fit(40, 500, max_columns=-5) == 1


# ---------------------------------------------------------------------------
# Task 2: width-aware _create_preview_table tests
# ---------------------------------------------------------------------------


def _wide_preview(num_cols=20, num_rows=2):
    cols = {f"c{i}": [f"val_{i}_{r}" for r in range(num_rows)] for i in range(num_cols)}
    table = pa.table(cols)
    columns_info = [
        {"name": f"c{i}", "type": "string", "is_geometry": False} for i in range(num_cols)
    ]
    return table, columns_info


def test_create_preview_table_hides_overflow_columns():
    table, columns_info = _wide_preview(num_cols=20)
    console = Console(width=80, force_terminal=True)
    rich_table, hidden = _create_preview_table(
        columns_info=columns_info, preview_table=table, console=console
    )
    assert hidden > 0
    assert len(rich_table.columns) == 20 - hidden
    # cells are one line (no fold): ellipsis overflow on the visible columns
    assert all(c.overflow == "ellipsis" for c in rich_table.columns)


def test_create_preview_table_non_tty_shows_all_columns():
    # Redirected/piped output (is_terminal False) must not silently drop columns.
    table, columns_info = _wide_preview(num_cols=20)
    console = Console(width=80)
    assert console.is_terminal is False
    rich_table, hidden = _create_preview_table(
        columns_info=columns_info, preview_table=table, console=console
    )
    assert hidden == 0
    assert len(rich_table.columns) == 20
    assert all(c.overflow == "fold" for c in rich_table.columns)


def test_create_preview_table_non_tty_honors_explicit_max_columns():
    # An explicit --max-columns is still respected when output is piped.
    table, columns_info = _wide_preview(num_cols=20)
    console = Console(width=80)
    assert console.is_terminal is False
    rich_table, hidden = _create_preview_table(
        columns_info=columns_info, preview_table=table, console=console, max_columns=4
    )
    assert len(rich_table.columns) == 4
    assert hidden == 16


def test_create_preview_table_max_columns():
    table, columns_info = _wide_preview(num_cols=20)
    console = Console(width=500)
    rich_table, hidden = _create_preview_table(
        columns_info=columns_info, preview_table=table, console=console, max_columns=4
    )
    assert len(rich_table.columns) == 4
    assert hidden == 16


def test_create_preview_table_no_truncate_shows_all():
    table, columns_info = _wide_preview(num_cols=20)
    console = Console(width=80)
    rich_table, hidden = _create_preview_table(
        columns_info=columns_info, preview_table=table, console=console, no_truncate=True
    )
    assert len(rich_table.columns) == 20
    assert hidden == 0
    assert all(c.overflow == "fold" for c in rich_table.columns)


def test_create_preview_table_few_columns_no_hidden():
    table, columns_info = _wide_preview(num_cols=2)
    console = Console(width=200)
    rich_table, hidden = _create_preview_table(
        columns_info=columns_info, preview_table=table, console=console
    )
    assert len(rich_table.columns) == 2
    assert hidden == 0


# ---------------------------------------------------------------------------
# Task 3: CLI --max-columns / --no-truncate for inspect head/tail
# ---------------------------------------------------------------------------


def test_inspect_head_max_columns(runner):
    test_file = os.path.join(os.path.dirname(__file__), "data", "buildings_test.parquet")
    result = runner.invoke(cli, ["inspect", "head", test_file, "2", "--max-columns", "1"])
    assert result.exit_code == 0
    # only one data column shown -> the other columns are reported as hidden
    assert "more columns" in result.output


def test_inspect_head_no_truncate(runner):
    test_file = os.path.join(os.path.dirname(__file__), "data", "buildings_test.parquet")
    result = runner.invoke(cli, ["inspect", "head", test_file, "1", "--no-truncate"])
    assert result.exit_code == 0
    assert "more columns" not in result.output
    assert "id" in result.output
    assert "geometry" in result.output


def test_inspect_head_no_truncate_overrides_max_columns(runner):
    """When both flags are given, --no-truncate wins and warns that --max-columns is ignored."""
    test_file = os.path.join(os.path.dirname(__file__), "data", "buildings_test.parquet")
    result = runner.invoke(
        cli, ["inspect", "head", test_file, "--max-columns", "1", "--no-truncate"]
    )
    assert result.exit_code == 0
    assert "more columns" not in result.output
    assert "id" in result.output and "geometry" in result.output
    assert "Note: --no-truncate overrides --max-columns" in result.output


def test_inspect_head_max_columns_rejects_zero(runner):
    test_file = os.path.join(os.path.dirname(__file__), "data", "buildings_test.parquet")
    result = runner.invoke(cli, ["inspect", "head", test_file, "--max-columns", "0"])
    assert result.exit_code != 0


def test_inspect_tail_no_truncate(runner):
    test_file = os.path.join(os.path.dirname(__file__), "data", "buildings_test.parquet")
    result = runner.invoke(cli, ["inspect", "tail", test_file, "1", "--no-truncate"])
    assert result.exit_code == 0
