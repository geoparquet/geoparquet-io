"""`patch_footer_kv` edits the footer and copies the data bytes verbatim (#1141).

The oracle throughout is the file itself: everything below the footer must come
out byte for byte identical, and patching a file with no changes at all must
reproduce it exactly. A rewrite cannot pass either of those, which is the point
-- `add bbox-metadata` used to re-encode every page to add one key, at DuckDB's
default compression level, and inflated a zstd-15 file by 11.6%.
"""

import io
import json
import struct
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core import parquet_footer
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.parquet_footer import (
    _LIST,
    FooterPatchUnsupported,
    _copy_below_footer,
    _decode_kv_list,
    _encode_kv_list,
    _field_header,
    _patch_footer,
    _read_metadata_or_refuse,
    _read_varint,
    _rewrite_kv_field,
    _struct_fields,
    _verify_data_untouched,
    patch_footer_kv,
)

GEO = json.dumps(
    {
        "version": "2.0.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
    }
)


def _data_section(path) -> bytes:
    """Every byte of the file below its footer."""
    raw = Path(path).read_bytes()
    (footer_length,) = struct.unpack("<I", raw[-8:-4])
    return raw[: len(raw) - 8 - footer_length]


def _footer(path) -> bytes:
    raw = Path(path).read_bytes()
    (footer_length,) = struct.unpack("<I", raw[-8:-4])
    return raw[len(raw) - 8 - footer_length : len(raw) - 8]


def _entries(path) -> list[tuple[bytes, bytes | None]]:
    """The footer's key-value entries in file order, duplicates included.

    pyarrow hands them back as a mapping, which is exactly what must not be
    used here: a file may carry the same key twice.
    """
    footer = _footer(path)
    fields, _ = _struct_fields(footer)
    kv = [field for field in fields if field.field_id == 5]
    return _decode_kv_list(footer, kv[0].value_start) if kv else []


def _keys(path) -> list[bytes]:
    return [key for key, _ in _entries(path)]


def _write_geoparquet(path, *, geo=GEO, level=15, duplicate_geo=False, rows=4000, label="n"):
    """A zstd-15 file with bloom filters, native GEOMETRY and a bbox column.

    `duplicate_geo` asks DuckDB to write its own `geo` block as well as the one
    passed through `KV_METADATA`, which leaves the file carrying the key twice.
    `rows` and `label` are there to build a second file that differs from the
    first in a chosen way.
    """
    version = "V2" if duplicate_geo else "NONE"
    conn = get_duckdb_connection(load_spatial=True)
    try:
        conn.execute(f"""
            COPY (
              SELECT
                id,
                '{label}' || id AS name,
                {{'xmin': ST_XMin(geom), 'ymin': ST_YMin(geom),
                  'xmax': ST_XMax(geom), 'ymax': ST_YMax(geom)}} AS bbox,
                geom AS geometry
              FROM (
                SELECT i AS id, ST_Point(-71.5 + (i % 97) / 100.0, 41.3 + (i % 61) / 100.0) AS geom
                FROM range({rows}) t(i)
              )
            ) TO '{path.as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL {level},
             ROW_GROUP_SIZE 1000, WRITE_BLOOM_FILTER true,
             GEOPARQUET_VERSION '{version}', KV_METADATA {{geo: '{geo}'}})
        """)
    finally:
        conn.close()
    return path


@pytest.fixture
def geo_file(tmp_path):
    """One `geo` key, zstd 15, four row groups, bloom filters."""
    return _write_geoparquet(tmp_path / "geo.parquet")


@pytest.fixture
def keyless_file(tmp_path):
    """No `key_value_metadata` field in the footer at all."""
    path = tmp_path / "keyless.parquet"
    conn = get_duckdb_connection(load_spatial=False)
    try:
        conn.execute(
            f"COPY (SELECT i AS id, 'row' || i AS name FROM range(500) t(i)) "
            f"TO '{path.as_posix()}' (FORMAT PARQUET)"
        )
    finally:
        conn.close()
    assert pq.read_metadata(str(path)).metadata is None, "fixture must start with no footer keys"
    return path


@pytest.fixture
def arrow_file(tmp_path):
    """A pyarrow-written file, which carries an `ARROW:schema` key."""
    path = tmp_path / "arrow.parquet"
    pq.write_table(pa.table({"a": [1, 2, 3]}), str(path))
    assert b"ARROW:schema" in pq.read_metadata(str(path)).metadata
    return path


class TestTheDataIsNeverTouched:
    """The whole promise: no page is decoded, so no page can change."""

    def test_bytes_below_the_footer_are_identical(self, geo_file, tmp_path):
        before = _data_section(geo_file)
        patch_footer_kv(str(geo_file), {"geo": GEO, "extra": "x"})

        assert _data_section(geo_file) == before

    def test_the_compression_level_survives(self, geo_file):
        """The regression: a rewrite came back at DuckDB's default level."""
        before = geo_file.stat().st_size
        added = json.dumps({"covering": {"bbox": {"xmin": ["bbox", "xmin"]}}})
        patch_footer_kv(str(geo_file), {"covering-ish": added})

        grew_by = geo_file.stat().st_size - before
        assert grew_by < 2 * len(added), (
            f"the file grew by {grew_by} bytes for a {len(added)}-byte key, so the data was rewritten"
        )

    def test_row_groups_and_bloom_filters_are_identical(self, geo_file):
        conn = get_duckdb_connection(load_spatial=False)
        try:
            query = (
                "SELECT row_group_id, path_in_schema, compression, encodings, "
                "total_compressed_size, data_page_offset, bloom_filter_offset "
                f"FROM parquet_metadata('{geo_file.as_posix()}') ORDER BY 1, 2"
            )
            before = conn.execute(query).fetchall()
            patch_footer_kv(str(geo_file), {"geo": GEO, "note": "x"})
            assert conn.execute(query).fetchall() == before
        finally:
            conn.close()

    def test_the_rows_still_read_back(self, geo_file):
        conn = get_duckdb_connection(load_spatial=False)
        try:
            query = f"SELECT count(*), sum(id) FROM '{geo_file.as_posix()}'"
            before = conn.execute(query).fetchone()
            patch_footer_kv(str(geo_file), {"geo": GEO, "note": "x"})
            assert conn.execute(query).fetchone() == before
        finally:
            conn.close()

    @pytest.mark.parametrize("fixture", ["geo_file", "keyless_file", "arrow_file"])
    def test_no_updates_reproduces_the_file_byte_for_byte(self, fixture, request):
        """The strongest oracle for the thrift surgery: only field 5 is re-encoded."""
        path = request.getfixturevalue(fixture)
        before = path.read_bytes()
        patch_footer_kv(str(path), {})

        assert path.read_bytes() == before


class TestTheKeys:
    def test_adds_a_key_to_a_file_that_has_none(self, keyless_file):
        before = _data_section(keyless_file)

        patch_footer_kv(str(keyless_file), {"geo": GEO})

        assert pq.read_metadata(str(keyless_file)).metadata[b"geo"] == GEO.encode()
        assert _data_section(keyless_file) == before

    def test_the_insert_keeps_the_fields_that_follow(self, keyless_file):
        """`key_value_metadata` is field 5, and field 7 usually follows it.

        Inserting ahead of a field means re-encoding that field's delta, so the
        fixture is only a regression test if it really has a later field.
        """
        fields, _ = _struct_fields(_footer(keyless_file))
        ids_before = [field.field_id for field in fields]
        assert 5 not in ids_before and max(ids_before) > 5, "fixture does not exercise the insert"

        patch_footer_kv(str(keyless_file), {"geo": GEO})

        ids_after = [field.field_id for field in _struct_fields(_footer(keyless_file))[0]]
        assert ids_after == sorted(ids_before + [5])

    def test_keeps_every_other_key(self, arrow_file):
        patch_footer_kv(str(arrow_file), {"stac:collection": "places"})

        metadata = pq.read_metadata(str(arrow_file)).metadata
        assert metadata[b"stac:collection"] == b"places"
        assert b"ARROW:schema" in metadata, "a colon in a key used to break the rewrite (#756)"

    def test_none_removes_a_key(self, geo_file):
        patch_footer_kv(str(geo_file), {"gone": "soon"})
        assert b"gone" in _keys(geo_file)

        patch_footer_kv(str(geo_file), {"gone": None})
        assert b"gone" not in _keys(geo_file)
        assert b"geo" in _keys(geo_file)

    def test_removing_a_key_the_file_never_had_changes_nothing(self, keyless_file):
        before = keyless_file.read_bytes()
        inode = keyless_file.stat().st_ino

        patch_footer_kv(str(keyless_file), {"absent": None})

        assert keyless_file.read_bytes() == before
        assert keyless_file.stat().st_ino == inode, (
            "an in-place call that changes nothing should not copy the file"
        )

    def test_a_key_the_file_carries_twice_collapses_when_it_is_set(self, tmp_path):
        """DuckDB writes its own `geo` beside a passed-in one, so this happens."""
        path = _write_geoparquet(tmp_path / "twice.parquet", duplicate_geo=True)
        assert _keys(path).count(b"geo") == 2, "fixture must start with the key twice"

        patch_footer_kv(str(path), {"geo": GEO})

        assert _keys(path) == [b"geo"]
        assert pq.read_metadata(str(path)).metadata[b"geo"] == GEO.encode()

    def test_a_key_the_file_carries_twice_is_left_alone_otherwise(self, tmp_path):
        path = _write_geoparquet(tmp_path / "twice.parquet", duplicate_geo=True)
        before = _entries(path)

        patch_footer_kv(str(path), {"unrelated": "x"})

        assert _entries(path) == before + [(b"unrelated", b"x")]

    def test_more_than_fifteen_keys(self, arrow_file):
        """A list of 15 or more needs the long header and a varint count."""
        patch_footer_kv(str(arrow_file), {f"k{i}": f"v{i}" for i in range(20)})

        metadata = pq.read_metadata(str(arrow_file)).metadata
        assert metadata[b"k19"] == b"v19"
        assert len(metadata) == 21, "the original ARROW:schema key should still be there"


@pytest.mark.skipif(sys.platform == "win32", reason="chmod permissions not supported on Windows")
class TestTheFileOnDisk:
    def test_the_mode_is_kept(self, geo_file):
        """Staging goes through mkstemp, which creates 0600."""
        geo_file.chmod(0o644)

        patch_footer_kv(str(geo_file), {"geo": GEO, "note": "x"})

        assert geo_file.stat().st_mode & 0o777 == 0o644

    def test_an_existing_destination_keeps_its_own_mode(self, geo_file, tmp_path):
        destination = tmp_path / "out.parquet"
        destination.write_bytes(b"placeholder")
        destination.chmod(0o640)

        patch_footer_kv(str(geo_file), {"geo": GEO}, output_file=str(destination))

        assert destination.stat().st_mode & 0o777 == 0o640


class TestAnInputThatChangesUnderTheCall:
    """`os.replace` is atomic; it is not a compare-and-swap.

    Another writer landing between the footer being read and the pages being
    copied would leave a file pairing one version's footer with another
    version's data. gpio takes no locks, so the call checks and refuses.
    """

    def test_a_file_replaced_mid_copy_is_refused(self, geo_file, tmp_path, monkeypatch):
        other = _write_geoparquet(tmp_path / "other.parquet", rows=2500, label="other")
        real_copy = parquet_footer._copy_below_footer

        def copy_then_let_another_writer_land(source, sink, footer_start):
            real_copy(source, sink, footer_start)
            geo_file.write_bytes(other.read_bytes())

        monkeypatch.setattr(parquet_footer, "_copy_below_footer", copy_then_let_another_writer_land)

        with pytest.raises(FooterPatchUnsupported, match="changed while it was being copied"):
            patch_footer_kv(str(geo_file), {"note": "x"})

        assert geo_file.read_bytes() == other.read_bytes(), "the other writer's file was clobbered"


class TestTheGuards:
    """Every refusal reached directly.

    These are the promise that the module publishes nothing it cannot vouch
    for, so each one is worth a case of its own rather than being left to a
    file that happens to trigger it.
    """

    def test_a_runaway_varint(self):
        with pytest.raises(FooterPatchUnsupported, match="past 64 bits"):
            _read_varint(b"\xff" * 12, 0)

    def test_an_empty_map_is_skipped(self):
        """A map is one of the types a Parquet footer does not have to carry."""
        fields, stop = _struct_fields(b"\x1b\x00\x00")

        assert [field.field_id for field in fields] == [1]
        assert stop == 2

    def test_a_field_id_too_far_for_the_delta_nibble(self):
        """Field 30 after field 5 needs the long form: type nibble, then the id."""
        header = _field_header(30, 5, _LIST)

        assert header == b"\x09\x3c"
        fields, _ = _struct_fields(header + b"\x00\x00")
        assert [(field.field_id, field.type_id) for field in fields] == [(30, _LIST)]

    def test_a_key_with_no_value_keeps_having_none(self):
        """`KeyValue.value` is optional, so a file may carry a key without one."""
        encoded = b"\x1c\x18\x01k\x00"

        entries = _decode_kv_list(encoded, 0)

        assert entries == [(b"k", None)]
        assert _encode_kv_list(entries) == encoded

    def test_a_key_value_entry_that_is_not_a_string(self):
        with pytest.raises(FooterPatchUnsupported, match="where a string was expected"):
            _decode_kv_list(b"\x1c\x15\x02\x00", 0)

    def test_a_key_value_list_that_is_not_structs(self):
        with pytest.raises(FooterPatchUnsupported, match="where structs were expected"):
            _decode_kv_list(b"\x15\x02", 0)

    def test_the_field_is_appended_when_nothing_follows_it(self):
        """A footer whose last field id is below 5 has no neighbour to move."""
        patched = _rewrite_kv_field(b"\x15\x02\x00", {"k": "v"})

        fields, _ = _struct_fields(patched)
        assert [field.field_id for field in fields] == [1, 5]
        assert _decode_kv_list(patched, fields[1].value_start) == [(b"k", b"v")]

    def test_a_footer_that_runs_out_mid_structure(self):
        with pytest.raises(FooterPatchUnsupported, match="thrift structure could not be read"):
            _patch_footer(b"\x19\x3c", {"k": "v"})

    def test_a_file_too_short_to_be_parquet(self, tmp_path):
        path = tmp_path / "tiny.parquet"
        path.write_bytes(b"PAR1")

        with pytest.raises(FooterPatchUnsupported, match="too short"):
            patch_footer_kv(str(path), {"geo": GEO})

    def test_a_source_that_ends_before_its_footer(self):
        with pytest.raises(FooterPatchUnsupported, match="ended before its footer"):
            _copy_below_footer(io.BytesIO(b"short"), io.BytesIO(), 4096)

    def test_metadata_that_pyarrow_cannot_read(self, tmp_path):
        path = tmp_path / "prose.txt"
        path.write_text("not a parquet file")

        with pytest.raises(FooterPatchUnsupported, match="footer could not be read"):
            _read_metadata_or_refuse(str(path))

    def test_a_patched_file_pyarrow_cannot_read(self, geo_file, tmp_path):
        prose = tmp_path / "prose.txt"
        prose.write_text("not a parquet file")

        with pytest.raises(FooterPatchUnsupported, match="could not be read back"):
            _verify_data_untouched(pq.read_metadata(str(geo_file)), str(prose))

    def test_a_patched_file_describing_a_different_shape(self, geo_file, tmp_path):
        fewer = _write_geoparquet(tmp_path / "fewer.parquet", rows=1000)

        with pytest.raises(FooterPatchUnsupported, match="describes"):
            _verify_data_untouched(pq.read_metadata(str(geo_file)), str(fewer))

    def test_a_patched_file_whose_row_groups_are_not_the_same(self, geo_file, tmp_path):
        wider = _write_geoparquet(tmp_path / "wider.parquet", label="a-much-longer-label-")

        with pytest.raises(FooterPatchUnsupported, match="row group 0"):
            _verify_data_untouched(pq.read_metadata(str(geo_file)), str(wider))

    def test_a_patched_file_whose_pages_moved(self, geo_file, tmp_path):
        """Same rows, same columns, same uncompressed size, different offsets."""
        looser = _write_geoparquet(tmp_path / "looser.parquet", level=1)

        with pytest.raises(FooterPatchUnsupported, match="row group"):
            _verify_data_untouched(pq.read_metadata(str(geo_file)), str(looser))


class TestRefusals:
    def test_an_encrypted_footer(self, geo_file):
        encrypted = geo_file.read_bytes()[:-4] + b"PARE"
        geo_file.write_bytes(encrypted)

        with pytest.raises(FooterPatchUnsupported, match="encrypted"):
            patch_footer_kv(str(geo_file), {"geo": GEO})

    def test_a_file_that_is_not_parquet(self, tmp_path):
        path = tmp_path / "not.parquet"
        path.write_bytes(b"PAR1" + b"\x00" * 64)

        with pytest.raises(FooterPatchUnsupported):
            patch_footer_kv(str(path), {"geo": GEO})

    def test_a_refusal_leaves_the_destination_alone(self, geo_file, tmp_path):
        destination = tmp_path / "out.parquet"
        destination.write_bytes(b"untouched")
        geo_file.write_bytes(geo_file.read_bytes()[:-4] + b"PARE")

        with pytest.raises(FooterPatchUnsupported):
            patch_footer_kv(str(geo_file), {"geo": GEO}, output_file=str(destination))

        assert destination.read_bytes() == b"untouched"

    def test_a_truncated_footer(self, geo_file):
        raw = bytearray(geo_file.read_bytes())
        struct.pack_into("<I", raw, len(raw) - 8, len(raw) * 2)
        geo_file.write_bytes(bytes(raw))

        with pytest.raises(FooterPatchUnsupported, match="longer than the file"):
            patch_footer_kv(str(geo_file), {"geo": GEO})


class TestOutputFile:
    def test_writes_beside_the_input_without_touching_it(self, geo_file, tmp_path):
        destination = tmp_path / "out.parquet"
        before = geo_file.read_bytes()

        patch_footer_kv(str(geo_file), {"geo": GEO, "note": "x"}, output_file=str(destination))

        assert geo_file.read_bytes() == before
        assert pq.read_metadata(str(destination)).metadata[b"note"] == b"x"
        assert _data_section(destination) == _data_section(geo_file)


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if not value:
            return bytes(out + bytes([byte]))
        out.append(byte | 0x80)


class TestTheThriftSkip:
    """The skip table, on types a Parquet footer does not necessarily carry.

    A footer that used one of these and was skipped wrongly would take the
    rewrite of `key_value_metadata` to the wrong offset, so each is exercised
    directly rather than through a file that happens not to have one.
    """

    def test_every_compact_type_is_skipped_to_the_right_offset(self):
        struct_bytes = b"".join(
            [
                b"\x11",  # 1: bool true, value in the type nibble, no payload
                b"\x13\x07",  # 2: byte
                b"\x17" + b"\x00" * 8,  # 3: double
                b"\x1d" + b"\x00" * 16,  # 4: uuid
                b"\x19\x21\x01\x02",  # 5: list<bool>, two elements, a byte each
                b"\x19" + b"\xf5" + _varint(16) + b"\x04" * 16,  # 6: list<i32>, long header
                b"\x1b" + _varint(1) + b"\x85" + b"\x01a" + b"\x04",  # 7: map<binary, i32>
                b"\x06" + _varint(60) + _varint(9),  # 30: i64, long form (delta > 15)
                b"\x18\x03abc",  # 31: binary
                b"\x1c\x15\x02\x00",  # 32: struct { 1: i32 }
                b"\x00",  # STOP
            ]
        )

        fields, stop = _struct_fields(struct_bytes)

        assert [field.field_id for field in fields] == [1, 2, 3, 4, 5, 6, 7, 30, 31, 32]
        assert stop == len(struct_bytes) - 1

    def test_an_unknown_type_is_refused(self):
        with pytest.raises(FooterPatchUnsupported, match="unknown thrift type"):
            _struct_fields(b"\x1f\x00")
