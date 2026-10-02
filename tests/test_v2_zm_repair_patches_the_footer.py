"""The XYM/XYZM 2.0 geo repair must patch the footer, not re-encode the file.

DuckDB 1.5.4's V2 writer omits the ``geo`` key for geometries with an M
dimension, so gpio rebuilds it from the file's own geospatial statistics and
attaches it (#589). Attaching it used to mean a full pyarrow rewrite of a file
the COPY had just finished writing -- every page decoded and re-encoded with a
different writer's defaults to add one footer key (#1177, item 2).

``core/parquet_footer.patch_footer_kv`` copies every byte below the footer
verbatim, so the codec, the compression level, the encodings, the row-group
boundaries and the row order are kept by construction rather than by
re-derivation. These tests hold that: the data bytes are untouched, the layout
matches the same COPY with no repair at all, and the result is still a file
pyarrow and ``gpio check spec`` accept.
"""

import json
import struct

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.duckdb_utils import get_duckdb_connection

_ROWS = 400


def _source(path, wkt_expr: str):
    """A native 2.0 COPY of ``_ROWS`` rows, whose geo key DuckDB may omit."""
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT i AS id, md5(i::VARCHAR) AS payload, {wkt_expr} AS geometry
          FROM range({_ROWS}) t(i)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
    """)
    con.close()
    return path


@pytest.fixture
def xym_source(tmp_path):
    return _source(
        tmp_path / "xym.parquet",
        "ST_GeomFromText('POINT M (' || (i % 180) || ' ' || (i % 80) || ' ' || i || ')')",
    )


@pytest.fixture
def xy_source(tmp_path):
    return _source(tmp_path / "xy.parquet", "ST_Point(i % 180, i % 80)")


def _data_bytes(path) -> bytes:
    """Every byte of the file below its footer -- the pages a rewrite re-encodes."""
    raw = path.read_bytes()
    footer_length = struct.unpack("<I", raw[-8:-4])[0]
    return raw[: len(raw) - 8 - footer_length]


def _layout(path) -> list[tuple[int, int, str]]:
    """(rows, compressed bytes, codec) per row group -- what a rewrite re-chunks."""
    with pq.ParquetFile(str(path)) as pf:
        return [
            (
                pf.metadata.row_group(i).num_rows,
                pf.metadata.row_group(i).total_byte_size,
                pf.metadata.row_group(i).column(0).compression,
            )
            for i in range(pf.metadata.num_row_groups)
        ]


def test_the_repair_leaves_every_data_byte_alone(xym_source, tmp_path):
    """The repair adds a footer key; the pages below the footer must not move."""
    from geoparquet_io.core.write_funnels import _ensure_v2_geo_metadata

    # The precondition the repair exists for: DuckDB's V2 writer wrote no geo key.
    assert (pq.ParquetFile(str(xym_source)).metadata.metadata or {}).get(b"geo") is None
    before = _data_bytes(xym_source)
    layout = _layout(xym_source)

    _ensure_v2_geo_metadata(str(xym_source), primary_column="geometry")

    assert json.loads(pq.ParquetFile(str(xym_source)).metadata.metadata[b"geo"])["version"] == (
        "2.0.0"
    )
    assert _data_bytes(xym_source) == before, "the repair re-encoded the data pages"
    assert _layout(xym_source) == layout, "the repair re-chunked the row groups"


def test_the_repaired_output_matches_the_copy_it_repairs(xym_source, tmp_path):
    """An XYM 2.0 convert must cost no more bytes than the COPY plus a footer key.

    ``parquet-geo-only`` is the same COPY of the same rows with no repair at
    all, so it is the reference the repaired output has to match.
    """
    repaired = tmp_path / "repaired.parquet"
    reference = tmp_path / "reference.parquet"
    convert_to_geoparquet(
        str(xym_source), str(repaired), skip_hilbert=True, geoparquet_version="2.0"
    )
    convert_to_geoparquet(
        str(xym_source), str(reference), skip_hilbert=True, geoparquet_version="parquet-geo-only"
    )

    assert _layout(repaired) == _layout(reference), (
        "the repaired output's row groups differ from the COPY's"
    )
    # The whole geo block is a few hundred bytes; a re-encode moved this by
    # double digits of a percent.
    overhead = repaired.stat().st_size - reference.stat().st_size
    assert 0 < overhead < 4096, f"repair added {overhead} bytes over the plain COPY"


def test_an_xym_output_has_the_same_layout_as_an_xy_one(xym_source, xy_source, tmp_path):
    """Same command, same row count, same row-group boundaries -- M or not (#1177)."""
    xym_out = tmp_path / "xym_out.parquet"
    xy_out = tmp_path / "xy_out.parquet"
    for src, out in ((xym_source, xym_out), (xy_source, xy_out)):
        convert_to_geoparquet(
            str(src), str(out), skip_hilbert=True, geoparquet_version="2.0", row_group_rows=137
        )

    assert [rows for rows, _, _ in _layout(xym_out)] == [rows for rows, _, _ in _layout(xy_out)]


def test_the_repaired_file_is_readable_and_valid(xym_source, tmp_path):
    """pyarrow must see the patched key on both schema views, and check spec pass."""
    from geoparquet_io.core.validate import CheckStatus, validate_geoparquet

    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(xym_source), str(out), skip_hilbert=True, geoparquet_version="2.0")

    table = pq.read_table(str(out))
    assert table.num_rows == _ROWS
    with pq.ParquetFile(str(out)) as pf:
        assert b"geo" in pf.metadata.metadata, "footer key missing"
        assert b"geo" in (pf.schema_arrow.metadata or {}), "geo key invisible on the Arrow schema"

    failed = [
        f"{c.name}: {c.message}"
        for c in validate_geoparquet(str(out)).checks
        if c.status is CheckStatus.FAILED
    ]
    assert not failed, failed


def test_the_repair_keeps_the_compression_level_the_copy_used(xym_source, tmp_path):
    """A rewrite could not carry the level; a footer patch cannot lose it.

    Parquet records the codec but not the *level*, which is why the old rewrite
    came back bigger. Measured against the same COPY at the same level with no
    repair.
    """
    repaired = tmp_path / "repaired.parquet"
    reference = tmp_path / "reference.parquet"
    for out, version in ((repaired, "2.0"), (reference, "parquet-geo-only")):
        convert_to_geoparquet(
            str(xym_source),
            str(out),
            skip_hilbert=True,
            geoparquet_version=version,
            compression="ZSTD",
            compression_level=22,
        )

    assert _data_bytes(repaired) == _data_bytes(reference)


def test_an_unpatchable_footer_still_gets_its_geo_metadata(xym_source, monkeypatch):
    """The rewrite stays as the fallback for a footer that cannot be patched."""
    from geoparquet_io.core import write_funnels
    from geoparquet_io.core.parquet_footer import FooterPatchUnsupported

    def refuse(*args, **kwargs):
        raise FooterPatchUnsupported("pretend this footer cannot be read")

    monkeypatch.setattr(write_funnels, "patch_footer_kv", refuse)

    write_funnels._ensure_v2_geo_metadata(str(xym_source), primary_column="geometry")

    geo = json.loads(pq.ParquetFile(str(xym_source)).metadata.metadata[b"geo"])
    assert geo["primary_column"] == "geometry"


def test_a_file_that_already_has_geo_metadata_is_left_alone(xy_source):
    """XY data gets its geo key from DuckDB; the repair must not touch the file."""
    from geoparquet_io.core.write_funnels import _ensure_v2_geo_metadata

    assert (pq.ParquetFile(str(xy_source)).metadata.metadata or {}).get(b"geo") is not None
    before = xy_source.read_bytes()

    _ensure_v2_geo_metadata(str(xy_source), primary_column="geometry")

    assert xy_source.read_bytes() == before
