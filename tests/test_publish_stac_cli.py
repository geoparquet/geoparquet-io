"""
CLI tests for ``gpio publish stac`` (cli/commands/publish.py).

Everything here is offline: STAC generation refuses remote inputs, so the
whole command surface runs against the small committed fixtures. The upload
credential-failure test mocks ``check_credentials`` where the publish module
resolves it (patch.object, not a dotted string -- see test_upload.py).
"""

import importlib
import json
import shutil
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

# Use importlib to get the actual module (avoids namespace collision with cli group)
main_module = importlib.import_module("geoparquet_io.cli.main")
cli = main_module.cli
publish_module = importlib.import_module("geoparquet_io.cli.commands.publish")

BUCKET = "s3://bucket/data/"


def _write_stac_item_json(path: Path) -> None:
    path.write_text(json.dumps({"type": "Feature", "id": "existing"}))


class TestPublishStacItem:
    """Single-file input -> STAC Item JSON."""

    def test_creates_item_json(self, places_test_file, tmp_path):
        out = tmp_path / "item.json"
        result = CliRunner().invoke(
            cli, ["publish", "stac", places_test_file, str(out), "--bucket", BUCKET]
        )
        assert result.exit_code == 0, result.output
        assert "✓ Created STAC Item" in result.output
        item = json.loads(out.read_text())
        assert item["type"] == "Feature"
        assert item["id"] == "places_test"
        assert item["assets"]["data"]["href"] == f"{BUCKET}places_test.parquet"
        xmin, ymin, xmax, ymax = item["bbox"]
        assert xmin < xmax and ymin < ymax

    def test_verbose_and_custom_item_id(self, places_test_file, tmp_path):
        out = tmp_path / "item.json"
        result = CliRunner().invoke(
            cli,
            [
                "publish",
                "stac",
                places_test_file,
                str(out),
                "--bucket",
                BUCKET,
                "--item-id",
                "ghana",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert f"Generating STAC Item for {places_test_file}" in result.output
        assert json.loads(out.read_text())["id"] == "ghana"

    def test_existing_item_refused_without_overwrite(self, places_test_file, tmp_path):
        out = tmp_path / "item.json"
        _write_stac_item_json(out)
        result = CliRunner().invoke(
            cli, ["publish", "stac", places_test_file, str(out), "--bucket", BUCKET]
        )
        assert result.exit_code != 0
        assert "already exists and is a STAC Item" in result.output
        assert "--overwrite" in result.output
        # The refused run must not have clobbered the existing file.
        assert json.loads(out.read_text())["id"] == "existing"

    def test_existing_item_overwritten_with_flag(self, places_test_file, tmp_path):
        out = tmp_path / "item.json"
        _write_stac_item_json(out)
        result = CliRunner().invoke(
            cli,
            ["publish", "stac", places_test_file, str(out), "--bucket", BUCKET, "--overwrite"],
        )
        assert result.exit_code == 0, result.output
        assert "Overwriting existing STAC Item" in result.output
        assert json.loads(out.read_text())["id"] == "places_test"

    def test_existing_non_stac_output_is_replaced(self, places_test_file, tmp_path):
        """A pre-existing file that is not a STAC Item does not block the write."""
        out = tmp_path / "item.json"
        out.write_text("not json at all")
        result = CliRunner().invoke(
            cli, ["publish", "stac", places_test_file, str(out), "--bucket", BUCKET]
        )
        assert result.exit_code == 0, result.output
        assert json.loads(out.read_text())["id"] == "places_test"


class TestPublishStacInputValidation:
    def test_stac_input_refused(self, tmp_path):
        item = tmp_path / "item.json"
        _write_stac_item_json(item)
        result = CliRunner().invoke(
            cli, ["publish", "stac", str(item), str(tmp_path / "out.json"), "--bucket", BUCKET]
        )
        assert result.exit_code != 0
        assert "Input is already a STAC Item" in result.output
        assert "gpio check stac" in result.output

    def test_missing_input_refused(self, tmp_path):
        result = CliRunner().invoke(
            cli,
            [
                "publish",
                "stac",
                str(tmp_path / "nope.parquet"),
                str(tmp_path / "out.json"),
                "--bucket",
                BUCKET,
            ],
        )
        assert result.exit_code != 0
        assert "Input must be file or directory" in result.output


class TestPublishStacCollection:
    """Directory input -> collection.json + Items next to the parquet files."""

    @staticmethod
    def _partition_dir(tmp_path, places_test_file, buildings_test_file):
        d = tmp_path / "parts"
        d.mkdir()
        shutil.copy(places_test_file, d / "places.parquet")
        shutil.copy(buildings_test_file, d / "buildings.parquet")
        return d

    def test_creates_collection_and_items(self, places_test_file, buildings_test_file, tmp_path):
        parts = self._partition_dir(tmp_path, places_test_file, buildings_test_file)
        out = tmp_path / "out"
        result = CliRunner().invoke(
            cli, ["publish", "stac", str(parts), str(out), "--bucket", BUCKET]
        )
        assert result.exit_code == 0, result.output
        assert "✓ Created STAC Collection" in result.output
        assert "✓ Created 2 STAC Items" in result.output
        collection = json.loads((out / "collection.json").read_text())
        assert collection["type"] == "Collection"
        assert collection["id"] == "parts"
        # Items are co-located with their parquet files in the input dir.
        for name in ("places", "buildings"):
            item = json.loads((parts / f"{name}.json").read_text())
            assert item["type"] == "Feature"
            assert item["id"] == name

    def test_verbose_and_custom_collection_id(
        self, places_test_file, buildings_test_file, tmp_path
    ):
        parts = self._partition_dir(tmp_path, places_test_file, buildings_test_file)
        out = tmp_path / "out"
        result = CliRunner().invoke(
            cli,
            [
                "publish",
                "stac",
                str(parts),
                str(out),
                "--bucket",
                BUCKET,
                "--collection-id",
                "my-collection",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert f"Generating STAC Collection for {parts}" in result.output
        assert json.loads((out / "collection.json").read_text())["id"] == "my-collection"

    def test_hive_partitioned_items_written_next_to_parquet(self, places_test_file, tmp_path):
        parts = tmp_path / "hive"
        (parts / "key=a").mkdir(parents=True)
        shutil.copy(places_test_file, parts / "key=a" / "places.parquet")
        out = tmp_path / "out"
        result = CliRunner().invoke(
            cli, ["publish", "stac", str(parts), str(out), "--bucket", BUCKET]
        )
        assert result.exit_code == 0, result.output
        item = json.loads((parts / "key=a" / "places.json").read_text())
        assert item["id"] == "places"

    def test_existing_collection_refused_without_overwrite(
        self, places_test_file, buildings_test_file, tmp_path
    ):
        parts = self._partition_dir(tmp_path, places_test_file, buildings_test_file)
        out = tmp_path / "out"
        out.mkdir()
        (out / "collection.json").write_text(json.dumps({"type": "Collection", "id": "old"}))
        result = CliRunner().invoke(
            cli, ["publish", "stac", str(parts), str(out), "--bucket", BUCKET]
        )
        assert result.exit_code != 0
        assert "already contains a STAC Collection" in result.output
        assert json.loads((out / "collection.json").read_text())["id"] == "old"

    def test_existing_collection_overwritten_with_flag(
        self, places_test_file, buildings_test_file, tmp_path
    ):
        parts = self._partition_dir(tmp_path, places_test_file, buildings_test_file)
        out = tmp_path / "out"
        out.mkdir()
        (out / "collection.json").write_text(json.dumps({"type": "Collection", "id": "old"}))
        result = CliRunner().invoke(
            cli,
            ["publish", "stac", str(parts), str(out), "--bucket", BUCKET, "--overwrite"],
        )
        assert result.exit_code == 0, result.output
        assert "Overwriting existing STAC Collection" in result.output
        assert json.loads((out / "collection.json").read_text())["id"] == "parts"

    def test_existing_item_next_to_parquet_refused_without_overwrite(
        self, places_test_file, buildings_test_file, tmp_path
    ):
        parts = self._partition_dir(tmp_path, places_test_file, buildings_test_file)
        _write_stac_item_json(parts / "places.json")
        out = tmp_path / "out"
        result = CliRunner().invoke(
            cli, ["publish", "stac", str(parts), str(out), "--bucket", BUCKET]
        )
        assert result.exit_code != 0
        assert "STAC Item already exists" in result.output
        assert json.loads((parts / "places.json").read_text())["id"] == "existing"

    def test_in_place_collection_when_output_is_none(
        self, places_test_file, buildings_test_file, tmp_path
    ):
        """The in-place branch (no output dir) is only reachable through the
        helper, since the CLI requires OUTPUT -- cover it directly."""
        parts = self._partition_dir(tmp_path, places_test_file, buildings_test_file)
        publish_module._handle_stac_collection(
            parts,
            output=None,
            bucket=BUCKET,
            public_url=None,
            collection_id=None,
            overwrite=False,
            verbose=False,
        )
        assert json.loads((parts / "collection.json").read_text())["type"] == "Collection"


class TestPublishUploadCredentialFailure:
    def test_upload_fails_with_hint_when_credentials_missing(self, places_test_file):
        with patch.object(
            publish_module, "check_credentials", return_value=(False, "run aws configure")
        ):
            result = CliRunner().invoke(
                cli,
                ["publish", "upload", places_test_file, "s3://bucket/data.parquet", "--dry-run"],
            )
        assert result.exit_code != 0
        assert "Authentication failed" in result.output
        assert "run aws configure" in result.output
