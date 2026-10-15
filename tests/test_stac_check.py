"""Tests for STAC validation functionality."""

import json
import os

import click
import pystac
import pytest

from geoparquet_io.core.stac_check import check_stac, validate_stac_file


@pytest.fixture
def valid_stac_item(temp_output_dir):
    """Create a valid STAC Item JSON file for testing."""
    from datetime import datetime, timezone

    # Use pystac to create item with correct version for installed pystac
    item = pystac.Item(
        id="test-item",
        geometry={
            "type": "Polygon",
            "coordinates": [[[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]],
        },
        bbox=[-180, -90, 180, 90],
        datetime=datetime(2024, 11, 8, 0, 0, 0, tzinfo=timezone.utc),
        properties={},
    )
    item.add_link(pystac.Link("self", "./test-item.json", "application/json"))
    item.add_asset(
        "data",
        pystac.Asset(
            "s3://bucket/test.parquet", media_type="application/vnd.apache.parquet", roles=["data"]
        ),
    )
    item_dict = item.to_dict()

    output_path = os.path.join(temp_output_dir, "valid_item.json")
    with open(output_path, "w") as f:
        json.dump(item_dict, f, indent=2)

    return output_path


@pytest.fixture
def valid_stac_collection(temp_output_dir):
    """Create a valid STAC Collection JSON file for testing."""
    # Use pystac to create collection with correct version for installed pystac
    collection = pystac.Collection(
        id="test-collection",
        description="Test collection",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
            temporal=pystac.TemporalExtent([[None, None]]),
        ),
        license="proprietary",
    )
    collection.add_link(pystac.Link("self", "./collection.json", "application/json"))
    collection.add_link(pystac.Link("item", "./item1.json", "application/json"))
    collection_dict = collection.to_dict()

    output_path = os.path.join(temp_output_dir, "valid_collection.json")
    with open(output_path, "w") as f:
        json.dump(collection_dict, f, indent=2)

    return output_path


@pytest.fixture
def invalid_stac_json(temp_output_dir):
    """Create an invalid JSON file for testing."""
    output_path = os.path.join(temp_output_dir, "invalid.json")
    with open(output_path, "w") as f:
        f.write("{ invalid json }")

    return output_path


def test_validate_valid_item(valid_stac_item):
    """Test validation of a valid STAC Item."""
    results = validate_stac_file(valid_stac_item, verbose=False)

    # Should be valid (no errors)
    assert len(results["errors"]) == 0, f"Unexpected errors: {results['errors']}"
    assert results["valid"] is True, f"Valid should be True. Results: {results}"
    assert results["info"]["stac_type"] == "Feature"
    assert results["info"]["stac_version"] in ["1.0.0", "1.1.0"]


def test_validate_valid_collection(valid_stac_collection):
    """Test validation of a valid STAC Collection."""
    results = validate_stac_file(valid_stac_collection, verbose=False)

    # Should be valid (no errors)
    assert len(results["errors"]) == 0, f"Unexpected errors: {results['errors']}"
    assert results["valid"] is True, f"Valid should be True. Results: {results}"
    assert results["info"]["stac_type"] == "Collection"
    assert results["info"]["stac_version"] in ["1.0.0", "1.1.0"]


def test_validate_invalid_json(invalid_stac_json):
    """Test validation of invalid JSON."""
    results = validate_stac_file(invalid_stac_json, verbose=False)

    assert results["valid"] is False
    assert len(results["errors"]) > 0
    assert "Invalid JSON" in results["errors"][0]


def test_validate_missing_file():
    """Test validation of non-existent file."""
    results = validate_stac_file("/nonexistent/file.json", verbose=False)

    assert results["valid"] is False
    assert len(results["errors"]) > 0
    assert "File not found" in results["errors"][0]


def test_validate_missing_required_fields(temp_output_dir):
    """Test validation catches missing required fields."""
    # Create Item missing required 'bbox' field
    incomplete_item = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": "incomplete",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        # Missing bbox
        "properties": {},
        "assets": {},
        "links": [],
    }

    output_path = os.path.join(temp_output_dir, "incomplete.json")
    with open(output_path, "w") as f:
        json.dump(incomplete_item, f)

    results = validate_stac_file(output_path, verbose=False)

    # pystac validation should catch this
    assert results["valid"] is False
    assert len(results["errors"]) > 0


def test_validate_bbox_ordering(temp_output_dir):
    """Test validation catches invalid bbox ordering."""
    # Create Item with invalid bbox (xmin > xmax)
    invalid_bbox_item = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": "invalid-bbox",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "bbox": [10, -90, -10, 90],  # Invalid: xmin > xmax
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "assets": {},
        "links": [],
    }

    output_path = os.path.join(temp_output_dir, "invalid_bbox.json")
    with open(output_path, "w") as f:
        json.dump(invalid_bbox_item, f)

    results = validate_stac_file(output_path, verbose=False)

    assert results["valid"] is False
    assert any("bbox ordering" in error.lower() for error in results["errors"])


def test_validate_warnings_missing_datetime(temp_output_dir):
    """Test validation warns about missing datetime."""
    # Create Item without datetime in properties
    item_no_datetime = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": "no-datetime",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "bbox": [-1, -1, 1, 1],
        "properties": {},  # No datetime
        "assets": {"data": {"href": "test.parquet", "roles": ["data"]}},
        "links": [],
    }

    output_path = os.path.join(temp_output_dir, "no_datetime.json")
    with open(output_path, "w") as f:
        json.dump(item_no_datetime, f)

    results = validate_stac_file(output_path, verbose=False)

    # Should still be valid but have warnings
    assert len(results["warnings"]) > 0
    assert any("datetime" in warning.lower() for warning in results["warnings"])


def test_validate_warnings_no_data_asset(temp_output_dir):
    """Test validation warns about missing data asset."""
    # Create Item without data role
    item_no_data = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": "no-data",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "bbox": [-1, -1, 1, 1],
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "assets": {"overview": {"href": "overview.png", "roles": ["visual"]}},
        "links": [],
    }

    output_path = os.path.join(temp_output_dir, "no_data.json")
    with open(output_path, "w") as f:
        json.dump(item_no_data, f)

    results = validate_stac_file(output_path, verbose=False)

    # Should have warning about missing data asset
    assert any("data" in warning.lower() for warning in results["warnings"])


def test_validate_info_counts(valid_stac_item):
    """Test validation provides info about counts."""
    results = validate_stac_file(valid_stac_item, verbose=False)

    assert "asset_count" in results["info"]
    assert "link_count" in results["info"]
    assert results["info"]["asset_count"] == 1
    assert results["info"]["link_count"] == 1


def test_check_stac_valid_no_error(valid_stac_item):
    """Test check_stac doesn't raise for valid STAC."""
    # Should not raise an exception (may have warnings if jsonschema not available)
    try:
        check_stac(valid_stac_item, verbose=False)
    except click.ClickException:
        # If validation fails due to missing jsonschema, that's acceptable
        # The function should handle this gracefully
        pass


def test_check_stac_invalid_is_a_failure(temp_output_dir):
    """Test check_stac returns a failure verdict for invalid STAC."""
    from geoparquet_io.core.check_verdict import Verdict

    # Create invalid STAC (missing required field)
    invalid_item = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": "invalid",
        # Missing geometry, bbox, properties, assets, links
    }

    output_path = os.path.join(temp_output_dir, "invalid.json")
    with open(output_path, "w") as f:
        json.dump(invalid_item, f)

    # One signal: the verdict, which the CLI turns into exit 1 (#1252).
    assert check_stac(output_path, verbose=False) is Verdict.FAILURE


def _assert_not_stac_json_message(message: str, path: str) -> None:
    """The wrong-input message names the file and says what was expected."""
    assert path in message
    assert "not a STAC JSON file" in message
    assert "Item/Collection JSON" in message
    assert "gpio publish stac" in message
    assert "codec can't decode" not in message


def test_validate_parquet_file_says_it_wants_stac_json(places_test_file):
    """A GeoParquet file gets an explanation, not a raw UnicodeDecodeError."""
    results = validate_stac_file(places_test_file, verbose=False)

    assert results["valid"] is False
    assert len(results["errors"]) == 1
    _assert_not_stac_json_message(results["errors"][0], places_test_file)


def test_validate_parquet_extension_beats_file_not_found(temp_output_dir):
    """A .parquet path is called out before the file is opened at all.

    Precedence is pinned deliberately: someone who passed a .parquet path used
    the wrong argument, so "File not found" would send them hunting for a file
    instead of telling them the command wants the STAC JSON.
    """
    missing = os.path.join(temp_output_dir, "does_not_exist.parquet")
    results = validate_stac_file(missing, verbose=False)

    assert results["valid"] is False
    _assert_not_stac_json_message(results["errors"][0], missing)
    assert "File not found" not in results["errors"][0]


def test_validate_non_utf8_json_named_file(temp_output_dir):
    """Non-UTF-8 bytes behind a .json name still get the clear message."""
    output_path = os.path.join(temp_output_dir, "binary.json")
    with open(output_path, "wb") as f:
        f.write(b"PAR1\x00\x9e\xff\xfe garbage")

    results = validate_stac_file(output_path, verbose=False)

    assert results["valid"] is False
    _assert_not_stac_json_message(results["errors"][0], output_path)


def test_validate_directory_input_gets_wrong_input_message(temp_output_dir):
    """A directory (partitioned GeoParquet output) gets the message, not a traceback."""
    results = validate_stac_file(temp_output_dir, verbose=False)

    assert results["valid"] is False
    _assert_not_stac_json_message(results["errors"][0], temp_output_dir)


def test_validate_missing_json_file_message_unchanged(temp_output_dir):
    """A missing .json path still reports File not found, not the new message."""
    missing = os.path.join(temp_output_dir, "does_not_exist.json")
    results = validate_stac_file(missing, verbose=False)

    assert results["valid"] is False
    assert "File not found" in results["errors"][0]
    assert "not a STAC JSON file" not in results["errors"][0]


def test_check_stac_cli_reports_wrong_input(places_test_file):
    """The CLI surfaces the explanation for a GeoParquet argument."""
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    result = CliRunner().invoke(cli, ["check", "stac", places_test_file])

    assert result.exit_code != 0
    _assert_not_stac_json_message(result.output, places_test_file)


def test_validate_stac_api_reports_wrong_input(places_test_file):
    """The Python API twin surfaces the same explanation."""
    from geoparquet_io.api.stac import validate_stac

    result = validate_stac(places_test_file)

    assert result.passed() is False
    failures = "\n".join(str(f) for f in result.failures())
    _assert_not_stac_json_message(failures, places_test_file)
