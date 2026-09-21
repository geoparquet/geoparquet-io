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

from geoparquet_io.core.add import bbox_metadata
from geoparquet_io.core.add.bbox_metadata import add_bbox_metadata
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
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

    monkeypatch.setattr(bbox_metadata, "patch_footer_kv", refuse)
    before = zstd15_file.stat().st_size

    add_bbox_metadata(str(zstd15_file))

    geo = json.loads(pq.read_metadata(str(zstd15_file)).metadata[b"geo"])
    assert "covering" in geo["columns"]["geometry"], "the fallback did not write the key"
    assert zstd15_file.stat().st_size > before, (
        "the fallback is the rewrite, so this file is expected to come back inflated"
    )
