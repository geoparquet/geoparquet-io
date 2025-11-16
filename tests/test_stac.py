"""Tests for STAC generation functionality."""

import json
import os
from pathlib import Path

import pytest

from geoparquet_io.core.stac import (
    construct_asset_href,
    detect_pmtiles,
    detect_stac,
    generate_item_id,
    generate_stac_collection,
    generate_stac_geometry,
    generate_stac_item,
    write_stac_json,
)


def _assert_stac_item_structure(item_dict: dict) -> None:
    """Assert basic STAC Item structure."""
    assert item_dict["type"] == "Feature"
    assert "stac_version" in item_dict
    assert item_dict["stac_version"] in ["1.0.0", "1.1.0"]
    assert "id" in item_dict
    _assert_item_geometry(item_dict)
    _assert_item_properties(item_dict)


def _assert_item_geometry(item_dict: dict) -> None:
    """Assert STAC Item geometry structure."""
    assert "bbox" in item_dict
    assert len(item_dict["bbox"]) == 4
    assert "geometry" in item_dict
    assert item_dict["geometry"]["type"] == "Polygon"


def _assert_item_properties(item_dict: dict) -> None:
    """Assert STAC Item properties."""
    assert "properties" in item_dict
    assert "datetime" in item_dict["properties"]


def _assert_stac_item_assets(item_dict: dict, bucket_prefix: str) -> None:
    """Assert STAC Item assets."""
    assert "assets" in item_dict
    assert "data" in item_dict["assets"]
    data_asset = item_dict["assets"]["data"]
    assert data_asset["type"] == "application/vnd.apache.parquet"
    assert "data" in data_asset["roles"]
    assert bucket_prefix in data_asset["href"]


def _assert_stac_item_links(item_dict: dict) -> None:
    """Assert STAC Item links."""
    assert "links" in item_dict
    assert any(link["rel"] == "self" for link in item_dict["links"])


def _assert_stac_collection_structure(collection_dict: dict) -> None:
    """Assert basic STAC Collection structure."""
    assert collection_dict["type"] == "Collection"
    assert "stac_version" in collection_dict
    assert collection_dict["stac_version"] in ["1.0.0", "1.1.0"]
    assert "id" in collection_dict
    assert "description" in collection_dict
    _assert_collection_extent(collection_dict)


def _assert_collection_extent(collection_dict: dict) -> None:
    """Assert STAC Collection extent structure."""
    assert "extent" in collection_dict
    assert "spatial" in collection_dict["extent"]
    assert "temporal" in collection_dict["extent"]
    assert len(collection_dict["extent"]["spatial"]["bbox"]) == 1
    assert len(collection_dict["extent"]["spatial"]["bbox"][0]) == 4


def _assert_collection_items(
    collection_dict: dict, item_dicts: list[dict], expected_count: int
) -> None:
    """Assert collection items structure."""
    item_links = [link for link in collection_dict["links"] if link["rel"] == "item"]
    assert len(item_links) == expected_count

    assert len(item_dicts) == expected_count
    assert all(item["type"] == "Feature" for item in item_dicts)
    assert all(
        item["stac_version"] in ["1.0.0", "1.1.0"] for item in item_dicts
    )  # pystac version-dependent


def _assert_collection_bounds(collection_dict: dict, item_dicts: list[dict]) -> None:
    """Assert collection bounds encompass all item bounds."""
    collection_bbox = collection_dict["extent"]["spatial"]["bbox"][0]
    item_bboxes = [item["bbox"] for item in item_dicts]

    xmin, ymin, xmax, ymax = collection_bbox
    for item_bbox in item_bboxes:
        ixmin, iymin, ixmax, iymax = item_bbox
        assert xmin <= ixmin
        assert ymin <= iymin
        assert xmax >= ixmax
        assert ymax >= iymax


def test_generate_stac_geometry(places_test_file):
    """Test GeoJSON geometry generation from bounds."""
    geometry = generate_stac_geometry(places_test_file)

    assert geometry["type"] == "Polygon"
    assert "coordinates" in geometry
    assert len(geometry["coordinates"]) == 1  # Single polygon
    assert len(geometry["coordinates"][0]) == 5  # Closed ring
    # First and last coordinates should be the same (closed polygon)
    assert geometry["coordinates"][0][0] == geometry["coordinates"][0][-1]


def test_generate_item_id_from_file():
    """Test Item ID generation from filename."""
    item_id = generate_item_id("/path/to/roads.parquet")
    assert item_id == "roads"


def test_generate_item_id_from_partition_key():
    """Test Item ID generation from partition key."""
    item_id = generate_item_id("/path/to/usa.parquet", partition_key="usa")
    assert item_id == "usa"


def test_construct_asset_href_basic():
    """Test basic asset href construction."""
    href = construct_asset_href("test.parquet", "s3://my-bucket/data/")
    assert href == "s3://my-bucket/data/test.parquet"


def test_construct_asset_href_with_public_url():
    """Test asset href construction with public URL mapping."""
    href = construct_asset_href(
        "test.parquet", "s3://my-bucket/data/", public_url="https://data.example.com/files/"
    )
    assert href == "https://data.example.com/files/test.parquet"


def test_detect_pmtiles_none(temp_output_dir):
    """Test PMTiles detection when none present."""
    result = detect_pmtiles(temp_output_dir)
    assert result is None


def test_detect_pmtiles_single(temp_output_dir):
    """Test PMTiles detection with exactly one file."""
    pmtiles_path = Path(temp_output_dir) / "overview.pmtiles"
    pmtiles_path.touch()

    result = detect_pmtiles(temp_output_dir)
    assert result is not None
    assert result.endswith(".pmtiles")
    assert "overview.pmtiles" in result


def test_detect_pmtiles_multiple(temp_output_dir):
    """Test PMTiles detection error with multiple files."""
    import click

    # Create multiple PMTiles files
    (Path(temp_output_dir) / "overview.pmtiles").touch()
    (Path(temp_output_dir) / "old.pmtiles").touch()

    with pytest.raises(click.ClickException) as exc_info:
        detect_pmtiles(temp_output_dir)

    assert "Multiple PMTiles files found" in str(exc_info.value)


def test_generate_stac_item_basic(places_test_file, temp_output_dir):
    """Test STAC Item generation for single file."""
    item_dict = generate_stac_item(
        places_test_file, bucket_prefix="s3://test-bucket/places/", verbose=False
    )

    _assert_stac_item_structure(item_dict)
    _assert_stac_item_assets(item_dict, "s3://test-bucket/places/")
    _assert_stac_item_links(item_dict)


def test_generate_stac_item_with_pmtiles(places_test_file, temp_output_dir):
    """Test STAC Item generation with PMTiles overview."""
    # Copy test file to temp dir
    import shutil

    temp_parquet = Path(temp_output_dir) / "test.parquet"
    shutil.copy(places_test_file, temp_parquet)

    # Create PMTiles file in same directory
    pmtiles_path = Path(temp_output_dir) / "overview.pmtiles"
    pmtiles_path.touch()

    # Generate STAC item
    item_dict = generate_stac_item(
        str(temp_parquet), bucket_prefix="s3://test-bucket/data/", verbose=False
    )

    # Check PMTiles asset was added
    assert "overview" in item_dict["assets"]
    overview_asset = item_dict["assets"]["overview"]
    assert overview_asset["type"] == "application/vnd.pmtiles"
    assert "overview" in overview_asset["roles"]
    assert "visual" in overview_asset["roles"]


def test_generate_stac_item_custom_id(places_test_file):
    """Test STAC Item generation with custom ID."""
    item_dict = generate_stac_item(
        places_test_file, bucket_prefix="s3://test-bucket/", item_id="custom-id", verbose=False
    )

    assert item_dict["id"] == "custom-id"


def test_write_stac_json(temp_output_dir):
    """Test writing STAC JSON to file."""
    test_dict = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "test-item",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "bbox": [-1, -1, 1, 1],
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "assets": {},
        "links": [],
    }

    output_path = os.path.join(temp_output_dir, "test.json")
    write_stac_json(test_dict, output_path)

    # Verify file was written
    assert os.path.exists(output_path)

    # Verify contents
    with open(output_path) as f:
        loaded_dict = json.load(f)
    assert loaded_dict == test_dict


def test_stac_item_bbox_ordering(places_test_file):
    """Test that bbox has correct ordering (xmin, ymin, xmax, ymax)."""
    item_dict = generate_stac_item(
        places_test_file, bucket_prefix="s3://test-bucket/", verbose=False
    )

    bbox = item_dict["bbox"]
    xmin, ymin, xmax, ymax = bbox

    # Verify correct ordering
    assert xmin < xmax, f"Invalid bbox: xmin ({xmin}) should be < xmax ({xmax})"
    assert ymin < ymax, f"Invalid bbox: ymin ({ymin}) should be < ymax ({ymax})"


def test_stac_item_projection_info(places_test_file):
    """Test that projection info is included if available."""
    item_dict = generate_stac_item(
        places_test_file, bucket_prefix="s3://test-bucket/", verbose=False
    )

    properties = item_dict["properties"]

    # Check if CRS info is present (may not be in all test files)
    # This is optional, so we just check structure if present
    if "proj:epsg" in properties:
        assert isinstance(properties["proj:epsg"], int)
    if "proj:projjson" in properties:
        assert isinstance(properties["proj:projjson"], dict)
    if "proj:wkt2" in properties:
        assert isinstance(properties["proj:wkt2"], str)


def test_generate_stac_collection(places_test_file, temp_output_dir):
    """Test STAC Collection generation for partitioned dataset."""
    import shutil

    # Create a partitioned directory structure
    partition_dir = Path(temp_output_dir) / "partitions"
    partition_dir.mkdir()

    # Create multiple partition files
    partition1 = partition_dir / "usa.parquet"
    partition2 = partition_dir / "can.parquet"
    shutil.copy(places_test_file, partition1)
    shutil.copy(places_test_file, partition2)

    # Generate collection
    collection_dict, item_dicts = generate_stac_collection(
        str(partition_dir), bucket_prefix="s3://test-bucket/partitions/", verbose=False
    )

    _assert_stac_collection_structure(collection_dict)
    _assert_collection_items(collection_dict, item_dicts, expected_count=2)

    # Check item IDs match partition filenames
    item_ids = {item["id"] for item in item_dicts}
    assert "usa" in item_ids
    assert "can" in item_ids

    _assert_collection_bounds(collection_dict, item_dicts)


def test_generate_stac_collection_with_pmtiles(places_test_file, temp_output_dir):
    """Test STAC Collection generation with PMTiles overview."""
    import shutil

    # Create a partitioned directory structure
    partition_dir = Path(temp_output_dir) / "partitions"
    partition_dir.mkdir()

    # Create partition files
    partition1 = partition_dir / "usa.parquet"
    shutil.copy(places_test_file, partition1)

    # Create PMTiles overview in partition directory
    pmtiles_path = partition_dir / "overview.pmtiles"
    pmtiles_path.touch()

    # Generate collection
    collection_dict, item_dicts = generate_stac_collection(
        str(partition_dir), bucket_prefix="s3://test-bucket/partitions/", verbose=False
    )

    # Check PMTiles asset was added to collection
    assert "assets" in collection_dict
    assert "overview" in collection_dict["assets"]
    overview_asset = collection_dict["assets"]["overview"]
    assert overview_asset["type"] == "application/vnd.pmtiles"
    assert "overview" in overview_asset["roles"]
    assert "visual" in overview_asset["roles"]


def test_generate_stac_collection_custom_id(places_test_file, temp_output_dir):
    """Test STAC Collection generation with custom collection ID."""
    import shutil

    # Create a partitioned directory structure
    partition_dir = Path(temp_output_dir) / "partitions"
    partition_dir.mkdir()

    # Create partition file
    partition1 = partition_dir / "usa.parquet"
    shutil.copy(places_test_file, partition1)

    # Generate collection with custom ID
    collection_dict, _ = generate_stac_collection(
        str(partition_dir),
        bucket_prefix="s3://test-bucket/partitions/",
        collection_id="custom-collection",
        verbose=False,
    )

    assert collection_dict["id"] == "custom-collection"


def test_generate_stac_collection_no_files(temp_output_dir):
    """Test STAC Collection generation error when no parquet files found."""
    import click

    # Create empty directory
    empty_dir = Path(temp_output_dir) / "empty"
    empty_dir.mkdir()

    with pytest.raises(click.ClickException) as exc_info:
        generate_stac_collection(str(empty_dir), bucket_prefix="s3://test-bucket/", verbose=False)

    assert "No parquet files found" in str(exc_info.value)


def test_detect_stac_item_json(temp_output_dir):
    """Test detect_stac for STAC Item JSON file."""
    item_file = Path(temp_output_dir) / "item.json"
    item_data = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": "test-item",
        "bbox": [-180, -90, 180, 90],
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]],
        },
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
    }
    with open(item_file, "w") as f:
        json.dump(item_data, f)

    assert detect_stac(str(item_file)) == "Item"


def test_detect_stac_collection_json(temp_output_dir):
    """Test detect_stac for STAC Collection JSON file."""
    collection_file = Path(temp_output_dir) / "collection.json"
    collection_data = {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": "test-collection",
        "description": "Test collection",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        },
    }
    with open(collection_file, "w") as f:
        json.dump(collection_data, f)

    assert detect_stac(str(collection_file)) == "Collection"


def test_detect_stac_pure_collection_dir(temp_output_dir):
    """Test detect_stac for directory with only collection.json (no parquet files)."""
    collection_file = Path(temp_output_dir) / "collection.json"
    collection_data = {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": "test-collection",
        "description": "Test collection",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        },
    }
    with open(collection_file, "w") as f:
        json.dump(collection_data, f)

    assert detect_stac(str(temp_output_dir)) == "Collection"


def test_detect_stac_mixed_dir(places_test_file, temp_output_dir):
    """Test detect_stac for directory with both collection.json and parquet files (mixed)."""
    # Create collection.json
    collection_file = Path(temp_output_dir) / "collection.json"
    collection_data = {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": "test-collection",
        "description": "Test collection",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        },
    }
    with open(collection_file, "w") as f:
        json.dump(collection_data, f)

    # Copy parquet file to directory
    import shutil

    shutil.copy(places_test_file, Path(temp_output_dir) / "places.parquet")

    # Mixed directory should return None (not detected as pure STAC)
    assert detect_stac(str(temp_output_dir)) is None


def test_detect_stac_non_json_file(temp_output_dir):
    """Test detect_stac for non-JSON file."""
    parquet_file = Path(temp_output_dir) / "data.parquet"
    parquet_file.touch()
    assert detect_stac(str(parquet_file)) is None


def test_detect_stac_invalid_json(temp_output_dir):
    """Test detect_stac for invalid JSON file."""
    json_file = Path(temp_output_dir) / "invalid.json"
    with open(json_file, "w") as f:
        f.write("{ invalid json }")
    assert detect_stac(str(json_file)) is None


def test_detect_stac_non_stac_json(temp_output_dir):
    """Test detect_stac for JSON file that's not STAC."""
    json_file = Path(temp_output_dir) / "data.json"
    with open(json_file, "w") as f:
        json.dump({"type": "NotSTAC", "data": "test"}, f)
    assert detect_stac(str(json_file)) is None


def test_stac_cli_input_is_stac_item(places_test_file, temp_output_dir):
    """Test CLI errors when input is already a STAC Item."""
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    # Create STAC Item JSON
    item_file = Path(temp_output_dir) / "input.json"
    item_data = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": "test-item",
        "bbox": [-180, -90, 180, 90],
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]],
        },
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
    }
    with open(item_file, "w") as f:
        json.dump(item_data, f)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "stac",
            str(item_file),
            str(Path(temp_output_dir) / "output.json"),
            "--bucket",
            "s3://bucket/",
        ],
    )
    assert result.exit_code != 0
    assert "already a STAC Item" in result.output


def test_stac_cli_input_is_pure_stac_collection(temp_output_dir):
    """Test CLI errors when input is a pure STAC Collection (no parquet files)."""
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    # Create collection.json
    collection_file = Path(temp_output_dir) / "collection.json"
    collection_data = {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": "test-collection",
        "description": "Test collection",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        },
    }
    with open(collection_file, "w") as f:
        json.dump(collection_data, f)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "stac",
            str(temp_output_dir),
            str(Path(temp_output_dir) / "output"),
            "--bucket",
            "s3://bucket/",
        ],
    )
    assert result.exit_code != 0
    assert "already a STAC Collection" in result.output


def test_stac_cli_output_exists_no_overwrite(places_test_file, temp_output_dir):
    """Test CLI errors when output exists and is STAC, without --overwrite."""
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    # Create output STAC Item
    output_file = Path(temp_output_dir) / "output.json"
    item_data = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": "existing-item",
        "bbox": [-180, -90, 180, 90],
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]],
        },
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
    }
    with open(output_file, "w") as f:
        json.dump(item_data, f)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "stac",
            places_test_file,
            str(output_file),
            "--bucket",
            "s3://bucket/",
        ],
    )
    assert result.exit_code != 0
    assert "already exists and is a STAC Item" in result.output
    assert "--overwrite" in result.output


def test_stac_cli_output_exists_with_overwrite(places_test_file, temp_output_dir):
    """Test CLI overwrites when output exists and is STAC, with --overwrite."""
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    # Create output STAC Item
    output_file = Path(temp_output_dir) / "output.json"
    item_data = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": "existing-item",
        "bbox": [-180, -90, 180, 90],
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]],
        },
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
    }
    with open(output_file, "w") as f:
        json.dump(item_data, f)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "stac",
            places_test_file,
            str(output_file),
            "--bucket",
            "s3://bucket/",
            "--overwrite",
        ],
    )
    assert result.exit_code == 0
    assert "Overwriting existing STAC Item" in result.output
    assert "Created STAC Item" in result.output

    # Verify file was overwritten
    with open(output_file) as f:
        new_data = json.load(f)
    assert new_data["id"] != "existing-item"  # Should have new ID from parquet file


def test_stac_cli_mixed_dir_allowed(places_test_file, temp_output_dir):
    """Test CLI allows input directory with both STAC and parquet files (mixed)."""
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    # Create collection.json
    collection_file = Path(temp_output_dir) / "collection.json"
    collection_data = {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": "test-collection",
        "description": "Test collection",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        },
    }
    with open(collection_file, "w") as f:
        json.dump(collection_data, f)

    # Copy parquet file to directory
    import shutil

    shutil.copy(places_test_file, Path(temp_output_dir) / "places.parquet")

    # Should work (mixed directory allowed)
    output_dir = Path(temp_output_dir) / "output"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "stac",
            str(temp_output_dir),
            str(output_dir),
            "--bucket",
            "s3://bucket/",
        ],
    )
    assert result.exit_code == 0
    assert "Created STAC Collection" in result.output


def test_stac_collection_items_colocated(places_test_file, temp_output_dir):
    """Test STAC Collection generates Items alongside parquet files."""
    import shutil

    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    # Create partitioned directory
    partition_dir = Path(temp_output_dir) / "partitions"
    partition_dir.mkdir()
    shutil.copy(places_test_file, partition_dir / "usa.parquet")
    shutil.copy(places_test_file, partition_dir / "can.parquet")

    # Generate STAC collection
    output_dir = Path(temp_output_dir) / "output"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "stac",
            str(partition_dir),
            str(output_dir),
            "--bucket",
            "s3://bucket/partitions/",
        ],
    )
    assert result.exit_code == 0

    # Check collection.json in output directory
    assert (output_dir / "collection.json").exists()

    # Check items are next to parquet files in input directory
    assert (partition_dir / "usa.json").exists()
    assert (partition_dir / "can.json").exists()

    # Verify item has collection link
    with open(partition_dir / "usa.json") as f:
        usa_item = json.load(f)
    links = usa_item["links"]
    collection_links = [link for link in links if link["rel"] == "collection"]
    assert len(collection_links) == 1
    assert "collection.json" in collection_links[0]["href"]


def test_stac_item_has_collection_link(places_test_file, temp_output_dir):
    """Test that STAC Items in collections have proper collection links."""
    import shutil

    # Create partitioned directory
    partition_dir = Path(temp_output_dir) / "partitions"
    partition_dir.mkdir()
    shutil.copy(places_test_file, partition_dir / "usa.parquet")

    # Generate collection
    collection_dict, item_dicts = generate_stac_collection(
        str(partition_dir), bucket_prefix="s3://test-bucket/partitions/", verbose=False
    )

    # Check item has collection link
    assert len(item_dicts) == 1
    usa_item = item_dicts[0]
    links = usa_item["links"]
    collection_links = [link for link in links if link["rel"] == "collection"]
    assert len(collection_links) == 1
    assert collection_links[0]["href"] == "s3://test-bucket/partitions/collection.json"
