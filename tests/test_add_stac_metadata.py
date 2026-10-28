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
import os

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

# A second, distinct Collection -- used to prove a second call adds to the
# `collections` map rather than replacing it.
COLLECTION_B = {
    "type": "Collection",
    "id": "SkySatCollect",
    "stac_version": "1.1.0",
    "description": "y",
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


@pytest.fixture
def collection_b_file(tmp_path):
    p = tmp_path / "collection-b.json"
    p.write_text(json.dumps(COLLECTION_B))
    return p


def _carry_raw(path, raw):
    """Rewrite `path` so its footer carries `raw` as the stac-geoparquet value."""
    table = pq.read_table(path)
    meta = dict(table.schema.metadata or {})
    meta[b"stac-geoparquet"] = raw
    pq.write_table(table.replace_schema_metadata(meta), path)


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


def test_a_second_different_collection_is_added_not_replacing_the_first(
    parquet, collection_file, collection_b_file
):
    # `collections` is a map precisely so one stac-geoparquet file can index
    # items from several collections; a second call naming a different id
    # must not throw the first one away.
    CliRunner().invoke(add, ["stac-metadata", str(parquet), "--collection", str(collection_file)])
    r = CliRunner().invoke(
        add, ["stac-metadata", str(parquet), "--collection", str(collection_b_file)]
    )
    assert r.exit_code == 0, r.output
    collections = _meta(parquet)["collections"]
    assert set(collections) == {"PSScene", "SkySatCollect"}
    assert collections["PSScene"]["id"] == "PSScene"
    assert collections["SkySatCollect"]["id"] == "SkySatCollect"


def test_a_later_calls_declared_version_wins_over_an_earlier_ones(
    parquet, collection_file, collection_b_file
):
    # `version` describes how to interpret the write being made right now, not
    # a property of any one collection, so the most recent call's choice wins
    # -- and the merge still happens even though the version changed.
    CliRunner().invoke(
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
    r = CliRunner().invoke(
        add,
        [
            "stac-metadata",
            str(parquet),
            "--collection",
            str(collection_b_file),
            "--stac-geoparquet-version",
            "1.1.0",
        ],
    )
    assert r.exit_code == 0, r.output
    meta = _meta(parquet)
    assert meta["version"] == "1.1.0"
    assert set(meta["collections"]) == {"PSScene", "SkySatCollect"}


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

    def test_a_malformed_existing_stac_geoparquet_key_is_refused_not_overwritten(self, parquet):
        from geoparquet_io.core.add import stac_metadata as module
        from geoparquet_io.core.exceptions import GeoParquetError

        _carry_raw(parquet, b"{not json")
        before = parquet.read_bytes()
        with pytest.raises(GeoParquetError, match="not valid JSON"):
            module.add_stac_metadata(str(parquet), COLLECTION)
        assert parquet.read_bytes() == before

    def test_an_existing_value_that_is_not_a_json_object_is_refused(self, parquet):
        # A bare JSON scalar has no `collections` to merge into -- and must
        # not reach `.get()` and raise an uncaught AttributeError instead.
        from geoparquet_io.core.add import stac_metadata as module
        from geoparquet_io.core.exceptions import GeoParquetError

        _carry_raw(parquet, b"7")
        before = parquet.read_bytes()
        with pytest.raises(GeoParquetError, match="JSON object"):
            module.add_stac_metadata(str(parquet), COLLECTION)
        assert parquet.read_bytes() == before

    def test_an_existing_collections_entry_that_is_not_an_object_is_refused(self, parquet):
        # A `collections` entry that isn't a map (here, a JSON array) must be
        # refused, not silently discarded and merged over as if it were `{}`.
        from geoparquet_io.core.add import stac_metadata as module
        from geoparquet_io.core.exceptions import GeoParquetError

        _carry_raw(parquet, b'{"version": "1.1.0", "collections": ["not", "a", "map"]}')
        before = parquet.read_bytes()
        with pytest.raises(GeoParquetError, match="collections"):
            module.add_stac_metadata(str(parquet), COLLECTION)
        assert parquet.read_bytes() == before

    def test_the_table_path_refuses_an_existing_value_that_is_not_a_json_object(self):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata_table
        from geoparquet_io.core.exceptions import GeoParquetError

        bad = pa.table({"id": ["a"]}).replace_schema_metadata({b"stac-geoparquet": b"7"})
        with pytest.raises(GeoParquetError, match="JSON object"):
            add_stac_metadata_table(bad, COLLECTION)

    def test_the_table_path_refuses_an_existing_collections_entry_that_is_not_an_object(self):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata_table
        from geoparquet_io.core.exceptions import GeoParquetError

        bad = pa.table({"id": ["a"]}).replace_schema_metadata(
            {b"stac-geoparquet": b'{"version": "1.1.0", "collections": ["bad"]}'}
        )
        with pytest.raises(GeoParquetError, match="collections"):
            add_stac_metadata_table(bad, COLLECTION)


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

    def test_chaining_the_table_method_merges_rather_than_replaces(self, parquet):
        import geoparquet_io as gpio

        table = (
            gpio.read(str(parquet)).add_stac_metadata(COLLECTION).add_stac_metadata(COLLECTION_B)
        )
        payload = json.loads(table.table.schema.metadata[b"stac-geoparquet"])
        assert set(payload["collections"]) == {"PSScene", "SkySatCollect"}


class TestReviewFixes:
    """Defects found reviewing #1231: each test failed before its fix."""

    def _with_existing(self, tmp_path, value):
        path = tmp_path / "carried.parquet"
        table = pa.table({"id": ["a"]}).replace_schema_metadata(
            {b"stac-geoparquet": json.dumps(value).encode("utf-8")}
        )
        pq.write_table(table, path)
        return path

    def test_an_existing_singular_1_0_0_collection_is_kept(self, tmp_path, collection_file):
        # 1.0.0 files carry their Collection under the singular key; merging must
        # fold it into the map rather than silently dropping it.
        old = {**COLLECTION_B}
        path = self._with_existing(tmp_path, {"version": "1.0.0", "collection": old})
        r = CliRunner().invoke(
            add, ["stac-metadata", str(path), "--collection", str(collection_file)]
        )
        assert r.exit_code == 0, r.output
        meta = _meta(path)
        assert set(meta["collections"]) == {"PSScene", "SkySatCollect"}
        assert meta["collections"]["SkySatCollect"] == old
        assert "collection" not in meta

    def test_an_existing_singular_collection_without_an_id_is_refused(self, tmp_path):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata_table
        from geoparquet_io.core.exceptions import GeoParquetError

        table = pa.table({"id": ["a"]}).replace_schema_metadata(
            {b"stac-geoparquet": json.dumps({"collection": {"type": "Collection"}}).encode()}
        )
        with pytest.raises(GeoParquetError, match="singular 'collection'"):
            add_stac_metadata_table(table, COLLECTION)

    def test_an_existing_singular_collection_that_is_not_an_object_is_refused(self):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata_table
        from geoparquet_io.core.exceptions import GeoParquetError

        table = pa.table({"id": ["a"]}).replace_schema_metadata(
            {b"stac-geoparquet": json.dumps({"collection": "PSScene"}).encode()}
        )
        with pytest.raises(GeoParquetError, match="singular 'collection'"):
            add_stac_metadata_table(table, COLLECTION)

    def test_a_collection_dict_that_is_not_json_is_refused_not_a_traceback(self):
        import datetime

        from geoparquet_io.core.add.stac_metadata import add_stac_metadata_table
        from geoparquet_io.core.exceptions import GeoParquetError

        bad = {**COLLECTION, "created": datetime.date(2024, 1, 1)}
        with pytest.raises(GeoParquetError, match="JSON"):
            add_stac_metadata_table(pa.table({"id": ["a"]}), bad)

    def test_a_nan_in_the_collection_is_refused(self, tmp_path, parquet):
        from geoparquet_io.core.exceptions import GeoParquetError

        bad = tmp_path / "nan.json"
        bad.write_text('{"id": "x", "type": "Collection", "extent": NaN}')
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata

        with pytest.raises(GeoParquetError, match="JSON"):
            add_stac_metadata(str(parquet), str(bad))

    def test_a_deeply_nested_collection_file_is_refused_not_a_traceback(self, tmp_path, parquet):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata
        from geoparquet_io.core.exceptions import GeoParquetError

        deep = tmp_path / "deep.json"
        deep.write_text("[" * 200_000)
        with pytest.raises(GeoParquetError, match="JSON"):
            add_stac_metadata(str(parquet), str(deep))

    def test_an_aliased_output_is_treated_as_in_place(self, parquet, collection_file):
        # `<dir>/sub/../items.parquet` and `<dir>/items.parquet` are one file:
        # it is an in-place patch, not an "output already exists" refusal.
        # Built as a string -- pathlib would collapse the alias away.
        (parquet.parent / "sub").mkdir()
        alias = f"{parquet.parent}{os.sep}sub{os.sep}..{os.sep}{parquet.name}"
        r = CliRunner().invoke(
            add, ["stac-metadata", str(parquet), alias, "--collection", str(collection_file)]
        )
        assert r.exit_code == 0, r.output
        assert set(_meta(parquet)["collections"]) == {"PSScene"}

    def test_a_missing_input_is_named_as_missing(self, tmp_path):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata
        from geoparquet_io.core.exceptions import GeoParquetError

        with pytest.raises(GeoParquetError, match="Input file not found"):
            add_stac_metadata(str(tmp_path / "missing.parquet"), COLLECTION)

    def test_version_1_0_0_also_writes_the_singular_collection(self, parquet):
        # 1.0.0 defines only the singular `collection`; a strict 1.0.0 reader
        # looks there. The map is kept beside it, as the reference library does.
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata

        add_stac_metadata(str(parquet), COLLECTION, version="1.0.0")
        meta = _meta(parquet)
        assert meta["collection"] == COLLECTION
        assert meta["collections"] == {"PSScene": COLLECTION}

    def test_version_1_0_0_refuses_a_second_collection(self, parquet):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata
        from geoparquet_io.core.exceptions import GeoParquetError

        add_stac_metadata(str(parquet), COLLECTION)
        before = parquet.read_bytes()
        with pytest.raises(GeoParquetError, match="1.0.0 holds a single"):
            add_stac_metadata(str(parquet), COLLECTION_B, version="1.0.0")
        assert parquet.read_bytes() == before

    def test_an_unknown_top_level_field_is_carried_through(self, tmp_path):
        path = self._with_existing(
            tmp_path, {"version": "1.1.0", "collections": {}, "x-custom": [1, 2]}
        )
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata

        add_stac_metadata(str(path), COLLECTION)
        assert _meta(path)["x-custom"] == [1, 2]

    def test_a_value_only_in_the_plain_footer_entry_is_merged_not_lost(self, tmp_path):
        # A Parquet-level patch can leave the plain entry and pyarrow's
        # ARROW:schema copy disagreeing; both copies' Collections survive.
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata
        from geoparquet_io.core.parquet_footer import patch_footer_kv

        path = tmp_path / "split.parquet"
        arrow_side = {"version": "1.1.0", "collections": {"A": {"id": "A"}}}
        table = pa.table({"id": ["a"]}).replace_schema_metadata(
            {b"stac-geoparquet": json.dumps(arrow_side).encode()}
        )
        pq.write_table(table, path)
        blob = pq.read_metadata(path).metadata[b"ARROW:schema"]
        plain_side = {"version": "1.1.0", "collections": {"P": {"id": "P"}}}
        # Naming ARROW:schema keeps the blob as-is, so only the plain entry moves.
        patch_footer_kv(
            str(path), {"stac-geoparquet": json.dumps(plain_side), "ARROW:schema": blob}
        )

        add_stac_metadata(str(path), {"id": "B"})
        plain = json.loads(pq.read_metadata(path).metadata[b"stac-geoparquet"])
        assert set(plain["collections"]) == {"A", "P", "B"}
        assert set(_meta(path)["collections"]) == {"A", "P", "B"}

    def test_a_non_collection_stac_object_is_warned_about(self, caplog):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata_table

        with caplog.at_level("WARNING"):
            add_stac_metadata_table(pa.table({"id": ["a"]}), {"type": "Feature", "id": "i1"})
        assert "not 'Collection'" in caplog.text

    def test_rows_missing_item_columns_are_warned_about(self, parquet, caplog):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata

        with caplog.at_level("WARNING"):
            add_stac_metadata(str(parquet), COLLECTION)
        assert "requires of a STAC Item row" in caplog.text
        assert "links" in caplog.text

    def test_an_item_shaped_table_is_not_warned_about(self, caplog):
        from geoparquet_io.core.add.stac_metadata import (
            REQUIRED_ITEM_COLUMNS,
            add_stac_metadata_table,
        )

        table = pa.table({name: ["x"] for name in REQUIRED_ITEM_COLUMNS})
        with caplog.at_level("WARNING"):
            add_stac_metadata_table(table, COLLECTION)
        assert "STAC Item row" not in caplog.text

    def test_the_success_line_names_the_collection_this_call_added(
        self, parquet, collection_file, collection_b_file, caplog
    ):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata

        add_stac_metadata(str(parquet), str(collection_file))
        caplog.clear()
        with caplog.at_level("INFO"):
            add_stac_metadata(str(parquet), str(collection_b_file))
        text = caplog.text
        assert "'SkySatCollect'" in text
        assert "'PSScene, SkySatCollect'" not in text

    def test_the_table_method_default_follows_the_core_constant(self):
        import inspect

        from geoparquet_io.api.table import Table
        from geoparquet_io.core.add.stac_metadata import DEFAULT_STAC_GEOPARQUET_VERSION

        default = inspect.signature(Table.add_stac_metadata).parameters["version"].default
        assert default is DEFAULT_STAC_GEOPARQUET_VERSION

    def test_a_value_only_the_plain_entry_carries_is_kept(self, tmp_path):
        # The ARROW:schema copy has no stac-geoparquet key at all.
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata
        from geoparquet_io.core.parquet_footer import patch_footer_kv

        path = tmp_path / "plain-only.parquet"
        pq.write_table(pa.table({"id": ["a"]}), path)
        blob = pq.read_metadata(path).metadata[b"ARROW:schema"]
        plain_side = {"version": "1.1.0", "collections": {"P": {"id": "P"}}}
        patch_footer_kv(
            str(path), {"stac-geoparquet": json.dumps(plain_side), "ARROW:schema": blob}
        )

        add_stac_metadata(str(path), {"id": "B"})
        assert set(_meta(path)["collections"]) == {"P", "B"}

    def test_an_input_that_is_not_parquet_is_refused_by_name(self, tmp_path):
        from geoparquet_io.core.add.stac_metadata import add_stac_metadata
        from geoparquet_io.core.exceptions import GeoParquetError

        junk = tmp_path / "junk.parquet"
        junk.write_bytes(b"not a parquet file at all")
        with pytest.raises(GeoParquetError, match="Cannot read"):
            add_stac_metadata(str(junk), COLLECTION)
        assert junk.read_bytes() == b"not a parquet file at all"
