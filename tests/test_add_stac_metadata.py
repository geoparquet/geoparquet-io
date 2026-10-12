"""`gpio add stac-metadata`: the stac-geoparquet footer key.

The stac-geoparquet specification asks a Parquet file holding STAC items to
carry the collections those items belong to in its own footer, under the
`stac-geoparquet` key. Nothing in gpio wrote that key before this command:
`publish stac` generates STAC JSON *from* GeoParquet and `check stac`
validates STAC JSON, but neither puts anything in the footer.

The shape under test is the 1.1.0 one: `collections` is a **map keyed by
collection id**, and the singular `collection` key the earlier draft allowed is
deprecated and must not be written.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import add

COLLECTION = {
    "type": "Collection",
    "id": "PSScene",
    "stac_version": "1.1.0",
    "description": "x",
    "license": "proprietary",
    "extent": {
        "spatial": {"bbox": [[-180, -90, 180, 90]]},
        "temporal": {"interval": [[None, None]]},
    },
    "links": [],
}


@pytest.fixture
def parquet(tmp_path):
    path = tmp_path / "items.parquet"
    table = pa.table({"id": ["a"]}).replace_schema_metadata(
        {b"geo": b'{"version":"1.1.0","primary_column":"geometry"}'}
    )
    pq.write_table(table, path)
    return path


@pytest.fixture
def collection_file(tmp_path):
    p = tmp_path / "collection.json"
    p.write_text(json.dumps(COLLECTION))
    return p


def _meta(path):
    return json.loads(pq.ParquetFile(str(path)).schema_arrow.metadata[b"stac-geoparquet"])


def test_it_writes_the_stac_geoparquet_key(parquet, collection_file):
    r = CliRunner().invoke(
        add, ["stac-metadata", str(parquet), "--collection", str(collection_file)]
    )
    assert r.exit_code == 0, r.output
    assert _meta(parquet)["version"] == "1.1.0"


def test_collections_is_a_map_keyed_by_id(parquet, collection_file):
    CliRunner().invoke(add, ["stac-metadata", str(parquet), "--collection", str(collection_file)])
    assert set(_meta(parquet)["collections"]) == {"PSScene"}


def test_the_deprecated_singular_collection_key_is_not_written(parquet, collection_file):
    CliRunner().invoke(add, ["stac-metadata", str(parquet), "--collection", str(collection_file)])
    assert "collection" not in _meta(parquet)


def test_the_geo_key_survives(parquet, collection_file):
    CliRunner().invoke(add, ["stac-metadata", str(parquet), "--collection", str(collection_file)])
    meta = pq.ParquetFile(str(parquet)).schema_arrow.metadata
    assert json.loads(meta[b"geo"])["primary_column"] == "geometry"


def test_the_row_data_is_byte_identical(parquet, collection_file):
    before = pq.read_table(parquet).to_pydict()
    CliRunner().invoke(add, ["stac-metadata", str(parquet), "--collection", str(collection_file)])
    assert pq.read_table(parquet).to_pydict() == before


def test_a_collection_without_an_id_is_refused(parquet, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"type": "Collection"}))
    r = CliRunner().invoke(add, ["stac-metadata", str(parquet), "--collection", str(bad)])
    assert r.exit_code != 0
    assert "id" in r.output


def test_an_unsupported_spec_version_is_refused(parquet, collection_file):
    # The spec allows only "1.0.0" and "1.1.0".
    r = CliRunner().invoke(
        add,
        [
            "stac-metadata",
            str(parquet),
            "--collection",
            str(collection_file),
            "--stac-geoparquet-version",
            "2.0.0",
        ],
    )
    assert r.exit_code != 0
    assert "1.1.0" in r.output


def test_running_it_twice_replaces_rather_than_duplicates(parquet, collection_file):
    for _ in range(2):
        CliRunner().invoke(
            add, ["stac-metadata", str(parquet), "--collection", str(collection_file)]
        )
    assert _meta(parquet)["collections"]["PSScene"]["id"] == "PSScene"


def test_the_python_api_matches_the_cli(parquet):
    from geoparquet_io.api import ops

    ops.add_stac_metadata(str(parquet), COLLECTION)
    assert _meta(parquet)["version"] == "1.1.0"


# --- the rest of the surface ------------------------------------------------


def test_the_1_0_0_version_is_also_allowed(parquet, collection_file):
    r = CliRunner().invoke(
        add,
        [
            "stac-metadata",
            str(parquet),
            "--collection",
            str(collection_file),
            "--stac-geoparquet-version",
            "1.0.0",
        ],
    )
    assert r.exit_code == 0, r.output
    assert _meta(parquet)["version"] == "1.0.0"


def test_a_named_output_leaves_the_input_alone(parquet, collection_file, tmp_path):
    out = tmp_path / "out.parquet"
    r = CliRunner().invoke(
        add, ["stac-metadata", str(parquet), str(out), "--collection", str(collection_file)]
    )
    assert r.exit_code == 0, r.output
    assert _meta(out)["collections"]["PSScene"]["id"] == "PSScene"
    assert b"stac-geoparquet" not in (pq.ParquetFile(str(parquet)).schema_arrow.metadata or {})


def test_an_existing_output_is_refused_without_overwrite(parquet, collection_file, tmp_path):
    out = tmp_path / "out.parquet"
    out.write_bytes(b"not parquet")
    r = CliRunner().invoke(
        add, ["stac-metadata", str(parquet), str(out), "--collection", str(collection_file)]
    )
    assert r.exit_code != 0
    assert "--overwrite" in r.output
    assert out.read_bytes() == b"not parquet"


def test_overwrite_replaces_an_existing_output(parquet, collection_file, tmp_path):
    out = tmp_path / "out.parquet"
    out.write_bytes(b"not parquet")
    r = CliRunner().invoke(
        add,
        [
            "stac-metadata",
            str(parquet),
            str(out),
            "--collection",
            str(collection_file),
            "--overwrite",
        ],
    )
    assert r.exit_code == 0, r.output
    assert _meta(out)["version"] == "1.1.0"


def test_verbose_reports_the_collection(parquet, collection_file):
    r = CliRunner().invoke(
        add,
        ["stac-metadata", str(parquet), "--collection", str(collection_file), "--verbose"],
    )
    assert r.exit_code == 0, r.output
    assert "PSScene" in r.output


def test_a_remote_input_is_refused(collection_file):
    r = CliRunner().invoke(
        add,
        ["stac-metadata", "s3://bucket/items.parquet", "--collection", str(collection_file)],
    )
    assert r.exit_code != 0
    assert "s3://bucket/items.parquet" in r.output


def test_a_remote_output_is_refused(parquet, collection_file):
    r = CliRunner().invoke(
        add,
        [
            "stac-metadata",
            str(parquet),
            "s3://bucket/out.parquet",
            "--collection",
            str(collection_file),
        ],
    )
    assert r.exit_code != 0
    assert "s3://bucket/out.parquet" in r.output


class TestTheCoreModule:
    """`core/add/stac_metadata.py` holds every refusal, so the API shares them."""

    def test_a_collection_file_that_is_not_json_is_refused(self, parquet, tmp_path):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata
        from geoparquet_io.core.exceptions import GeoParquetError

        bad = tmp_path / "bad.json"
        bad.write_text("{not json")
        with pytest.raises(GeoParquetError, match="valid JSON"):
            add_stac_metadata(str(parquet), str(bad))

    def test_a_collection_file_that_is_not_an_object_is_refused(self, parquet, tmp_path):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata
        from geoparquet_io.core.exceptions import GeoParquetError

        bad = tmp_path / "bad.json"
        bad.write_text("[]")
        with pytest.raises(GeoParquetError, match="JSON object"):
            add_stac_metadata(str(parquet), str(bad))

    def test_a_missing_collection_file_is_refused(self, parquet, tmp_path):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata
        from geoparquet_io.core.exceptions import GeoParquetError

        with pytest.raises(GeoParquetError, match="Cannot read"):
            add_stac_metadata(str(parquet), str(tmp_path / "nope.json"))

    def test_an_id_that_is_not_a_string_is_refused(self, parquet):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata
        from geoparquet_io.core.exceptions import GeoParquetError

        with pytest.raises(GeoParquetError, match="'id'"):
            add_stac_metadata(str(parquet), {"type": "Collection", "id": 7})

    def test_an_empty_id_is_refused(self, parquet):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata
        from geoparquet_io.core.exceptions import GeoParquetError

        with pytest.raises(GeoParquetError, match="'id'"):
            add_stac_metadata(str(parquet), {"type": "Collection", "id": ""})

    def test_the_version_gate_is_in_core_not_only_in_click(self, parquet):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata
        from geoparquet_io.core.exceptions import GeoParquetError

        with pytest.raises(GeoParquetError, match="1.1.0"):
            add_stac_metadata(str(parquet), COLLECTION, version="0.9.0")

    def test_nothing_is_written_when_the_collection_is_refused(self, parquet):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata
        from geoparquet_io.core.exceptions import GeoParquetError

        before = parquet.read_bytes()
        with pytest.raises(GeoParquetError):
            add_stac_metadata(str(parquet), {"type": "Collection"})
        assert parquet.read_bytes() == before

    def test_a_footer_it_cannot_patch_is_reported_not_silently_rewritten(
        self, parquet, monkeypatch
    ):
        from geoparquet_io.core.add import stac_metadata as module
        from geoparquet_io.core.exceptions import GeoParquetError
        from geoparquet_io.core.parquet_footer import FooterPatchUnsupported

        def refuse(*args, **kwargs):
            raise FooterPatchUnsupported("encrypted footer")

        monkeypatch.setattr(module, "patch_footer_kv", refuse)
        with pytest.raises(GeoParquetError, match="encrypted footer"):
            module.add_stac_metadata(str(parquet), COLLECTION)

    def test_the_payload_builder_keys_by_id_and_names_the_version(self):
        from geoparquet_io.core.add.stac_metadata import stac_geoparquet_metadata

        payload = stac_geoparquet_metadata(COLLECTION)
        assert payload == {"version": "1.1.0", "collections": {"PSScene": COLLECTION}}


class TestThePythonAPI:
    def test_ops_takes_a_collection_path(self, parquet, collection_file):
        from geoparquet_io.api import ops

        ops.add_stac_metadata(str(parquet), str(collection_file), version="1.0.0")
        assert _meta(parquet)["version"] == "1.0.0"

    def test_ops_writes_to_a_named_output(self, parquet, collection_file, tmp_path):
        from geoparquet_io.api import ops

        out = tmp_path / "out.parquet"
        ops.add_stac_metadata(str(parquet), str(collection_file), output_file=str(out))
        assert _meta(out)["collections"]["PSScene"]["id"] == "PSScene"

    def test_the_table_method_sets_the_schema_metadata(self, parquet):
        import geoparquet_io as gpio

        table = gpio.read(str(parquet)).add_stac_metadata(COLLECTION)
        payload = json.loads(table.table.schema.metadata[b"stac-geoparquet"])
        assert payload["collections"]["PSScene"]["id"] == "PSScene"

    def test_the_table_method_keeps_the_geo_key(self, parquet):
        import geoparquet_io as gpio

        table = gpio.read(str(parquet)).add_stac_metadata(COLLECTION)
        assert b"geo" in table.table.schema.metadata

    def test_the_table_method_refuses_a_bad_version(self, parquet):
        import geoparquet_io as gpio
        from geoparquet_io.core.exceptions import GeoParquetError

        with pytest.raises(GeoParquetError, match="1.1.0"):
            gpio.read(str(parquet)).add_stac_metadata(COLLECTION, version="2.0.0")

    def test_the_table_op_works_on_a_table_with_no_metadata_at_all(self):
        from geoparquet_io.api import ops

        bare = pa.table({"id": ["a"]})
        out = ops.add_stac_metadata_table(bare, COLLECTION)
        assert json.loads(out.schema.metadata[b"stac-geoparquet"])["version"] == "1.1.0"
