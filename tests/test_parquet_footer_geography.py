"""`patch_footer(geography=...)` retypes a GEOMETRY column GEOGRAPHY in the footer (#999).

DuckDB writes a planar GEOMETRY whatever it read, and a GEOGRAPHY's data pages
are the same WKB bytes, so the retype is a footer edit: the column's logical
type, and the bounding box in each of its chunks' geospatial statistics. The
oracle is the file: everything below the footer must stay byte for byte the
same, and only the named top-level GEOMETRY leaf may change.
"""

import base64
import json
import struct
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
from geoparquet_io.core.parquet_footer import (
    _BINARY,
    _DOUBLE,
    _LIST,
    _STRUCT,
    FooterPatchUnsupported,
    _apply_splices,
    _bbox_splice,
    _field_header,
    _patch_footer,
    _patch_geography,
    _struct_fields,
    _with_geography_fields,
    patch_footer,
    read_footer_kv,
)


def _data_section(path) -> bytes:
    raw = Path(path).read_bytes()
    (footer_length,) = struct.unpack("<I", raw[-8:-4])
    return raw[: len(raw) - 8 - footer_length]


def _footer(path) -> bytes:
    raw = Path(path).read_bytes()
    (footer_length,) = struct.unpack("<I", raw[-8:-4])
    return raw[len(raw) - 8 - footer_length : len(raw) - 8]


def _logical(path) -> dict[str, dict]:
    """``{dotted path: logical type as JSON}`` for every leaf."""
    with pq.ParquetFile(str(path)) as reader:
        schema = reader.schema
    return {
        schema.column(i).path: json.loads(schema.column(i).logical_type.to_json())
        for i in range(len(schema))
    }


def _logical_text(path, column: str) -> str:
    """One leaf's logical type as pyarrow prints it, which names the algorithm."""
    schema = pq.read_metadata(str(path)).schema
    index = [schema.column(i).path for i in range(len(schema))].index(column)
    return str(schema.column(index).logical_type)


def _geo_stats(path, column: str) -> list:
    """Each row group's geospatial statistics for one leaf, as (bbox, types)."""
    metadata = pq.read_metadata(str(path))
    index = [metadata.schema.column(i).path for i in range(metadata.num_columns)].index(column)
    found = []
    for group in range(metadata.num_row_groups):
        stats = metadata.row_group(group).column(index).geo_statistics
        found.append(None if stats is None else (stats.xmin, list(stats.geospatial_types)))
    return found


@pytest.fixture
def duckdb_file(tmp_path):
    """Three row groups; a struct with a nested ``geometry`` ahead of the real one."""
    path = tmp_path / "duckdb.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            f"""
            COPY (
                SELECT i AS id, {{'geometry': ST_Point(i, 0), 'n': i}} AS s,
                       ST_Point(i, i / 100.0) AS geometry, ST_Point(0, i) AS other
                FROM range(5000) t(i)
            ) TO {sql_path(str(path))}
            (FORMAT PARQUET, GEOPARQUET_VERSION 'V2', ROW_GROUP_SIZE 2048)
            """
        )
    finally:
        con.close()
    return path


def test_the_fixture_is_planar_with_stats(duckdb_file):
    """Non-vacuity: three row groups, every geometry leaf a GEOMETRY with a bbox."""
    logical = _logical(duckdb_file)
    assert {logical[c]["Type"] for c in ("geometry", "s.geometry", "other")} == {"Geometry"}
    assert len(_geo_stats(duckdb_file, "geometry")) == 3
    assert all(stats[0] is not None for stats in _geo_stats(duckdb_file, "geometry"))


def test_the_named_column_becomes_a_geography(duckdb_file, tmp_path):
    out = tmp_path / "out.parquet"
    before = _logical(duckdb_file)

    retyped = patch_footer(
        str(duckdb_file), geography={"geometry": "spherical"}, output_file=str(out)
    )

    assert retyped == ["geometry"]
    after = _logical(out)
    assert after["geometry"]["Type"] == "Geography"
    assert after["geometry"].get("crs") == before["geometry"].get("crs")
    assert "algorithm=spherical" in _logical_text(out, "geometry")
    # Neither the nested leaf that shares the name nor the other column moves.
    assert after["s.geometry"] == before["s.geometry"]
    assert after["other"] == before["other"]
    assert _data_section(out) == _data_section(duckdb_file)


def test_each_chunk_loses_its_bbox_and_keeps_its_types(duckdb_file, tmp_path):
    out = tmp_path / "out.parquet"
    patch_footer(str(duckdb_file), geography={"geometry": "spherical"}, output_file=str(out))

    assert _geo_stats(out, "geometry") == [(None, [1])] * 3
    assert _geo_stats(out, "other") == _geo_stats(duckdb_file, "other")
    assert _geo_stats(out, "s.geometry") == _geo_stats(duckdb_file, "s.geometry")


def test_the_rows_read_back_unchanged(duckdb_file, tmp_path):
    out = tmp_path / "out.parquet"
    patch_footer(str(duckdb_file), geography={"geometry": "karney"}, output_file=str(out))

    con = get_duckdb_connection(load_spatial=True)
    try:
        query = "SELECT id, ST_AsText(geometry) FROM {} ORDER BY id"
        expected = con.execute(query.format(sql_path(str(duckdb_file)))).fetchall()
        assert con.execute(query.format(sql_path(str(out)))).fetchall() == expected
    finally:
        con.close()
    assert "algorithm=karney" in _logical_text(out, "geometry")


def test_kv_updates_and_the_retype_land_in_one_patch(duckdb_file, tmp_path):
    out = tmp_path / "out.parquet"
    patch_footer(
        str(duckdb_file),
        {"note": "hello"},
        geography={"geometry": "spherical"},
        output_file=str(out),
    )

    assert pq.read_metadata(str(out)).metadata[b"note"] == b"hello"
    assert _logical(out)["geometry"]["Type"] == "Geography"


def test_an_already_geography_column_writes_nothing(duckdb_file):
    patch_footer(str(duckdb_file), geography={"geometry": "spherical"})
    before = Path(duckdb_file).read_bytes()

    assert patch_footer(str(duckdb_file), geography={"geometry": "spherical"}) == []
    assert Path(duckdb_file).read_bytes() == before


def test_a_column_with_no_geo_logical_type_is_left_alone(tmp_path):
    """A 1.x WKB column, and a STRING one, have nothing to retype."""
    path = tmp_path / "wkb.parquet"
    pq.write_table(
        pa.table({"geometry": pa.array([b"\x01"], pa.binary()), "name": ["x"], "id": [1]}),
        str(path),
    )
    before = Path(path).read_bytes()

    assert patch_footer(str(path), geography={"geometry": "spherical", "name": "spherical"}) == []
    assert Path(path).read_bytes() == before


def test_an_unknown_algorithm_is_refused_before_anything_is_written(duckdb_file):
    before = Path(duckdb_file).read_bytes()

    with pytest.raises(ValueError, match="not a GEOGRAPHY edge algorithm: geodesic"):
        patch_footer(str(duckdb_file), geography={"geometry": "geodesic"})
    assert Path(duckdb_file).read_bytes() == before


def test_a_truncated_footer_is_unsupported(duckdb_file):
    with pytest.raises(FooterPatchUnsupported, match="thrift structure"):
        _patch_geography(_footer(duckdb_file)[:200], {"geometry": "spherical"})


def test_no_geography_requested_returns_the_footer_untouched(duckdb_file):
    footer = _footer(duckdb_file)
    assert _patch_geography(footer, {}) == (footer, [])


class TestTheEmbeddedArrowSchema:
    """pyarrow's ``ARROW:schema`` copy must say the new edges too."""

    def test_a_pyarrow_written_column_reads_back_spherical(self, tmp_path):
        import geoarrow.pyarrow as ga

        path = tmp_path / "arrow.parquet"
        storage = pa.array([bytes.fromhex("0101000000000000000000f03f0000000000000040")])
        pq.write_table(
            pa.table({"geometry": pa.ExtensionArray.from_storage(ga.wkb(), storage)}), str(path)
        )
        assert b"ARROW:schema" in pq.read_metadata(str(path)).metadata

        assert patch_footer(str(path), geography={"geometry": "spherical"}) == ["geometry"]

        field = pq.read_schema(str(path)).field("geometry")
        assert str(field.type.edge_type) == "EdgeType.SPHERICAL"
        # pyarrow happens to prefer the logical type, but the blob is what an
        # Arrow reader restores field metadata from: it must not stay planar.
        blob = pq.read_metadata(str(path)).metadata[b"ARROW:schema"]
        embedded = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(blob)))
        assert str(embedded.field("geometry").type.edge_type) == "EdgeType.SPHERICAL"
        assert _logical(path)["geometry"]["Type"] == "Geography"

    def test_an_unregistered_field_has_its_extension_metadata_edited(self):
        field = pa.field(
            "geometry",
            pa.binary(),
            metadata={
                b"ARROW:extension:name": b"geoarrow.wkb",
                b"ARROW:extension:metadata": b'{"crs": "OGC:CRS84"}',
            },
        )
        schema = _with_geography_fields(pa.schema([field]), {"geometry": "vincenty"})

        extension = json.loads(schema.field("geometry").metadata[b"ARROW:extension:metadata"])
        assert extension == {"crs": "OGC:CRS84", "edges": "vincenty"}

    def test_a_field_that_is_not_geoarrow_is_left_alone(self):
        schema = pa.schema([pa.field("geometry", pa.binary()), pa.field("id", pa.int64())])
        assert (
            _with_geography_fields(schema, {"geometry": "spherical", "gone": "spherical"}) is schema
        )

    def test_extension_metadata_that_is_not_json_is_unsupported(self):
        field = pa.field(
            "geometry",
            pa.binary(),
            metadata={b"ARROW:extension:name": b"geoarrow.wkb", b"ARROW:extension:metadata": b"{"},
        )
        with pytest.raises(FooterPatchUnsupported, match="not JSON"):
            _with_geography_fields(pa.schema([field]), {"geometry": "spherical"})


def _binary_list(*items: bytes) -> bytes:
    return bytes([(len(items) << 4) | _BINARY]) + b"".join(
        bytes([len(item)]) + item for item in items
    )


def _chunk_with_stats(stats_body: bytes, path=(b"g",)) -> bytes:
    """A ColumnChunk whose ColumnMetaData holds a path and geospatial stats."""
    meta = _field_header(3, 0, _LIST) + _binary_list(*path)
    meta += _field_header(17, 3, _STRUCT) + stats_body + b"\x00"
    return _field_header(3, 0, _STRUCT) + meta + b"\x00" + b"\x00"


BBOX_ONLY = _field_header(1, 0, _STRUCT) + _field_header(1, 0, _DOUBLE) + bytes(8) + b"\x00"


class TestTheStatsSplice:
    """The branches a DuckDB file does not reach, on hand-built thrift."""

    def test_stats_holding_only_a_bbox_go_as_a_whole(self):
        chunk = _chunk_with_stats(BBOX_ONLY)
        fields, _ = _struct_fields(chunk)

        start, end, value = _bbox_splice(chunk, fields, {"g"})
        patched = chunk[:start] + value + chunk[end:]

        meta, _ = _struct_fields(patched, _struct_fields(patched)[0][0].value_start)
        assert [field.field_id for field in meta] == [3]

    def test_stats_with_no_bbox_need_nothing(self):
        types_only = _field_header(2, 0, _LIST) + bytes([0x15]) + b"\x02"
        chunk = _chunk_with_stats(types_only)
        assert _bbox_splice(chunk, _struct_fields(chunk)[0], {"g"}) is None

    def test_a_nested_path_is_not_the_column(self):
        chunk = _chunk_with_stats(BBOX_ONLY, path=(b"s", b"g"))
        assert _bbox_splice(chunk, _struct_fields(chunk)[0], {"g"}) is None

    def test_a_chunk_without_inline_metadata_is_skipped(self):
        chunk = _field_header(2, 0, 6) + b"\x00" + b"\x00"  # file_offset only
        assert _bbox_splice(chunk, _struct_fields(chunk)[0], {"g"}) is None


class TestManySplices:
    """Splices are joined in one pass; each lands at its own original offset."""

    def test_splices_of_every_size_apply_at_their_original_offsets(self):
        footer = bytes(range(100))
        # Out of order, growing, shrinking, emptying and one at each end.
        splices = [(50, 60, b"X" * 25), (0, 3, b""), (97, 100, b"END"), (10, 11, b"ab")]

        expected = footer
        for start, end, value in sorted(splices, reverse=True):
            expected = expected[:start] + value + expected[end:]

        assert _apply_splices(footer, splices) == expected
        assert _apply_splices(footer, []) == footer

    def test_a_footer_with_many_row_groups_loses_every_bbox(self, tmp_path):
        import geoarrow.pyarrow as ga
        import shapely

        rows = 300
        source = tmp_path / "many.parquet"
        wkb = shapely.to_wkb(shapely.points(range(rows), range(rows)))
        table = pa.table(
            {
                "id": pa.array(range(rows)),
                "geometry": pa.ExtensionArray.from_storage(
                    ga.wkb(), pa.array(list(wkb), pa.binary())
                ),
            }
        )
        pq.write_table(table, str(source), row_group_size=1)
        assert all(stats[0] is not None for stats in _geo_stats(source, "geometry"))
        out = tmp_path / "out.parquet"

        patch_footer(str(source), geography={"geometry": "spherical"}, output_file=str(out))

        assert _geo_stats(out, "geometry") == [(None, [1])] * rows
        assert pq.read_table(str(out)).column("id").to_pylist() == list(range(rows))
        assert _data_section(out) == _data_section(source)


#: A struct whose first field is a struct whose first field is a struct...,
#: nested deeper than Python's recursion limit allows a reader to follow.
DEEPLY_NESTED = b"\x1c" * 5000 + b"\x00" * 5001


def _file_with_footer(path: Path, footer: bytes) -> Path:
    path.write_bytes(b"PAR1" + footer + struct.pack("<I", len(footer)) + b"PAR1")
    return path


class TestDeeplyNestedFooters:
    """A footer nested past the recursion limit is unreadable, not a crash."""

    def test_read_footer_kv_names_it_unsupported(self, tmp_path):
        crafted = _file_with_footer(tmp_path / "deep.parquet", DEEPLY_NESTED)
        with pytest.raises(FooterPatchUnsupported, match="thrift structure"):
            read_footer_kv(str(crafted))

    def test_the_geography_patch_names_it_unsupported(self):
        with pytest.raises(FooterPatchUnsupported, match="thrift structure"):
            _patch_geography(DEEPLY_NESTED, {"geometry": "spherical"})

    def test_the_kv_patch_names_it_unsupported(self):
        with pytest.raises(FooterPatchUnsupported, match="thrift structure"):
            _patch_footer(DEEPLY_NESTED, {"k": "v"})
