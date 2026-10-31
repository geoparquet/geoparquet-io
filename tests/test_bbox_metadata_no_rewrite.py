"""`add bbox-metadata` adds a footer key without re-encoding the file (#1141).

The command is documented as metadata-only and implemented it as a full DuckDB
COPY of every page. A Parquet file does not record the compression *level* it
was written at, so the rewrite could not carry it: a zstd-15 fixture came back
at DuckDB's default of 3, 11.6% bigger, and a real 11.73 MB file came back at
15.93 MB. Everything else the rewrite defended -- the geometry column's
physical type (#712), the quoting of preserved keys (#700, #756), the row-group
size -- it defended because it was re-encoding. Patching the footer removes the
question.

`tests/test_bbox_metadata_integrity.py` covers what the command refuses and
what metadata it writes; this file covers what the file does *not* do.
"""

import json
import struct
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.add.bbox_metadata import add_bbox_metadata
from geoparquet_io.core.duckdb_utils import fetch_one, get_duckdb_connection
from geoparquet_io.core.parquet_footer import FooterPatchUnsupported

GEO_NO_COVERING = json.dumps(
    {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "bbox": [-71.5, 41.3, -70.5, 42.0],
            }
        },
    }
)


def _data_section(path) -> bytes:
    """Every byte of the file below its footer."""
    raw = Path(path).read_bytes()
    (footer_length,) = struct.unpack("<I", raw[-8:-4])
    return raw[: len(raw) - 8 - footer_length]


def _column_stats(path) -> list[tuple]:
    conn = get_duckdb_connection(load_spatial=False)
    try:
        return conn.execute(
            "SELECT row_group_id, path_in_schema, compression, encodings, "
            "total_compressed_size, data_page_offset, bloom_filter_offset "
            f"FROM parquet_metadata('{Path(path).as_posix()}') ORDER BY 1, 2"
        ).fetchall()
    finally:
        conn.close()


@pytest.fixture
def zstd15_file(tmp_path):
    """The shape that showed the defect: zstd 15, bloom filters, WKB, a bbox column.

    Written at 1.1 with plain WKB rather than a native GEOMETRY column, so the
    fixture is one the rewrite path had to be taught to keep intact (#712) and
    the patch keeps without being taught.
    """
    path = tmp_path / "level15.parquet"
    conn = get_duckdb_connection(load_spatial=True)
    try:
        conn.execute(f"""
            COPY (
              SELECT
                id,
                'name-' || id AS name,
                {{'xmin': ST_XMin(geom), 'ymin': ST_YMin(geom),
                  'xmax': ST_XMax(geom), 'ymax': ST_YMax(geom)}} AS bbox,
                ST_AsWKB(geom)::BLOB AS geometry
              FROM (
                SELECT i AS id, ST_Point(-71.5 + (i % 97) / 100.0, 41.3 + (i % 61) / 100.0) AS geom
                FROM range(20000) t(i)
              )
            ) TO '{path.as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15,
             ROW_GROUP_SIZE 5000, WRITE_BLOOM_FILTER true,
             GEOPARQUET_VERSION 'NONE', KV_METADATA {{geo: '{GEO_NO_COVERING}'}})
        """)
    finally:
        conn.close()
    return path


def test_the_file_is_not_re_encoded(zstd15_file):
    before = _data_section(zstd15_file)

    add_bbox_metadata(str(zstd15_file))

    assert _data_section(zstd15_file) == before, "the data pages were rewritten"


def test_the_file_does_not_grow_by_more_than_the_key(zstd15_file):
    """The reported symptom: +11.6% on this shape, +36% on a real file."""
    before = zstd15_file.stat().st_size

    add_bbox_metadata(str(zstd15_file))

    grew_by = zstd15_file.stat().st_size - before
    assert grew_by < 512, f"the file grew {grew_by} bytes, so it was recompressed"


def test_row_groups_and_bloom_filters_are_untouched(zstd15_file):
    before = _column_stats(zstd15_file)

    add_bbox_metadata(str(zstd15_file))

    assert _column_stats(zstd15_file) == before


def test_the_covering_is_there(zstd15_file):
    """The command's own job, unchanged by how it is now written."""
    add_bbox_metadata(str(zstd15_file))

    geo = json.loads(pq.read_metadata(str(zstd15_file)).metadata[b"geo"])
    assert geo["columns"]["geometry"]["covering"] == {
        "bbox": {
            "xmin": ["bbox", "xmin"],
            "ymin": ["bbox", "ymin"],
            "xmax": ["bbox", "xmax"],
            "ymax": ["bbox", "ymax"],
        }
    }


def test_an_output_file_leaves_the_input_alone(zstd15_file, tmp_path):
    destination = tmp_path / "out.parquet"
    before = zstd15_file.read_bytes()

    add_bbox_metadata(str(zstd15_file), output_file=str(destination))

    assert zstd15_file.read_bytes() == before
    assert _data_section(destination) == _data_section(zstd15_file)


def test_a_footer_it_cannot_patch_falls_back_to_the_rewrite(zstd15_file, monkeypatch):
    """The safety net: an unreadable footer costs what the command cost before.

    The rewrite is still the only way to reach an encrypted footer or a thrift
    structure the skip does not recognise, so it stays wired up. This plants
    the refusal rather than crafting such a file, because what is under test is
    the hand-off, not the refusal.
    """

    def refuse(*args, **kwargs):
        raise FooterPatchUnsupported("planted")

    monkeypatch.setattr("geoparquet_io.core.parquet_footer._patch_footer_file", refuse)
    before = _data_section(zstd15_file)

    add_bbox_metadata(str(zstd15_file))

    geo = json.loads(pq.read_metadata(str(zstd15_file)).metadata[b"geo"])
    assert "covering" in geo["columns"]["geometry"], "the fallback did not write the key"
    assert _data_section(zstd15_file) != before, "the fallback is the rewrite: pages re-encoded"
    # The rewrite writes the facade's row groups, not the input's four of 5,000
    # (approved change 3; it used the input's average rows per group).
    md = pq.read_metadata(str(zstd15_file))
    assert [md.row_group(i).num_rows for i in range(md.num_row_groups)] == [20_000]


def test_the_fallback_on_a_native_file_writes_one_geo_entry(tmp_path, monkeypatch):
    """#1225: the rewrite used 'V2' beside a KV `geo` block, and DuckDB appended its own."""
    path = tmp_path / "native.parquet"
    conn = get_duckdb_connection(load_spatial=True)
    try:
        conn.execute(f"""
            COPY (
              SELECT i AS id,
                {{'xmin': ST_XMin(g), 'ymin': ST_YMin(g),
                  'xmax': ST_XMax(g), 'ymax': ST_YMax(g)}} AS bbox,
                g AS geometry
              FROM (SELECT i, ST_SetCRS(ST_Point(i, i), 'EPSG:3857') AS g
                    FROM range(100) t(i))
            ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
        """)
    finally:
        conn.close()

    def refuse(*args, **kwargs):
        raise FooterPatchUnsupported("planted")

    monkeypatch.setattr("geoparquet_io.core.parquet_footer._patch_footer_file", refuse)

    add_bbox_metadata(str(path))

    conn = get_duckdb_connection(load_spatial=False)
    try:
        entries = conn.execute(
            "SELECT decode(value) FROM parquet_kv_metadata(?) WHERE decode(key) = 'geo'",
            [path.as_posix()],
        ).fetchall()
        logical = fetch_one(
            conn.execute(
                "SELECT logical_type FROM parquet_schema(?) WHERE name = 'geometry'",
                [path.as_posix()],
            )
        )[0]
    finally:
        conn.close()
    assert len(entries) == 1, entries
    col = json.loads(entries[0][0])["columns"]["geometry"]
    assert "covering" in col
    assert col["crs"]["id"] == {"authority": "EPSG", "code": 3857}
    assert logical.startswith("GeometryType"), "the native GEOMETRY type was lost"


def test_a_failed_rewrite_leaves_the_input_and_no_staging_debris(zstd15_file, monkeypatch):
    """#1244's other half: a failed fallback leaves the input as it was.

    The rewrite writes into ``atomic_write``'s staging directory beside the
    file and only a finished one is swapped in, so a write that dies part-way
    leaves nothing next to the input. (#1244's leaked connection has no
    successor: the fallback, ``write_funnels.rewrite_footer_kv``, streams with
    pyarrow and opens none.)
    """

    def refuse(*args, **kwargs):
        raise FooterPatchUnsupported("planted")

    def boom(*args, **kwargs):
        raise RuntimeError("planted failure")

    monkeypatch.setattr("geoparquet_io.core.parquet_footer._patch_footer_file", refuse)
    monkeypatch.setattr("geoparquet_io.core.write_funnels._copy_row_groups", boom)
    before = zstd15_file.read_bytes()

    # The funnel's own error, unwrapped: the fallback is the funnel's rewrite
    # (`write_funnels.rewrite_footer_kv`), not this command's (#1367 T2d).
    with pytest.raises(RuntimeError, match="planted failure"):
        add_bbox_metadata(str(zstd15_file))

    assert zstd15_file.read_bytes() == before
    assert sorted(p.name for p in zstd15_file.parent.iterdir()) == [zstd15_file.name]


def test_a_verbose_fallback_reports_the_keys_it_writes(zstd15_file, monkeypatch, caplog):
    """The verbose rewrite logs the footer keys it writes."""

    def refuse(*args, **kwargs):
        raise FooterPatchUnsupported("planted")

    monkeypatch.setattr("geoparquet_io.core.parquet_footer._patch_footer_file", refuse)

    with caplog.at_level("DEBUG", logger="geoparquet_io"):
        add_bbox_metadata(str(zstd15_file), verbose=True)

    assert "Rewriting with footer keys: ['geo']" in caplog.text, caplog.text
    geo = json.loads(pq.read_metadata(str(zstd15_file)).metadata[b"geo"])
    assert "covering" in geo["columns"]["geometry"]


def test_the_fallback_keeps_sidecar_keys_but_not_the_inputs_arrow_and_pandas_blocks(
    tmp_path, monkeypatch
):
    """The fallback is a write like any other: its footer keys are the facade's.

    It used to carry every input key verbatim, the input's ``pandas`` block and
    ``ARROW:schema`` included (pyarrow writes the output's own ``ARROW:schema``), which describe the input's rows and are dropped
    by every other gpio write (``output_schema_metadata``, #710, #1268). A
    sidecar key such as ``stac:collection`` still survives (#690, #756), and a
    1.1 file's geometry stays plain WKB rather than turning native (#712).
    """
    import pandas as pd
    import pyarrow as pa
    import shapely

    path = tmp_path / "pandas.parquet"
    frame = pd.DataFrame(
        {
            "id": range(50),
            "bbox": [
                {"xmin": float(i), "ymin": float(i), "xmax": float(i), "ymax": float(i)}
                for i in range(50)
            ],
            "geometry": [shapely.to_wkb(shapely.Point(i, i)) for i in range(50)],
        }
    )
    table = pa.Table.from_pandas(frame, preserve_index=False)
    geo = json.loads(GEO_NO_COVERING)
    geo["columns"]["geometry"]["bbox"] = [0, 0, 49, 49]
    table = table.replace_schema_metadata(
        {
            **table.schema.metadata,
            b"geo": json.dumps(geo).encode(),
            b"stac:collection": b'{"id": "c"}',
        }
    )
    pq.write_table(table, path)
    assert b"pandas" in pq.read_metadata(path).metadata
    input_arrow_schema = pq.read_metadata(path).metadata[b"ARROW:schema"]

    def refuse(*args, **kwargs):
        raise FooterPatchUnsupported("planted")

    monkeypatch.setattr("geoparquet_io.core.parquet_footer._patch_footer_file", refuse)

    add_bbox_metadata(str(path))

    keys = pq.read_metadata(path).metadata
    assert b"pandas" not in keys, "the input's pandas block was carried verbatim"
    # pyarrow writes the output's own ARROW:schema; the input's is not carried.
    assert keys.get(b"ARROW:schema") != input_arrow_schema, "the input's ARROW:schema was carried"
    assert json.loads(keys[b"stac:collection"]) == {"id": "c"}
    written = json.loads(keys[b"geo"])
    assert written["columns"]["geometry"]["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]
    assert written["version"] == "1.1.0"
    assert pq.read_schema(path).field("geometry").type == pa.binary()
