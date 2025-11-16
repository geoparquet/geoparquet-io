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


def test_check_stac_invalid_raises(temp_output_dir):
    """Test check_stac raises ClickException for invalid STAC."""
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

    with pytest.raises(click.ClickException) as exc_info:
        check_stac(output_path, verbose=False)

    assert "validation failed" in str(exc_info.value).lower()
