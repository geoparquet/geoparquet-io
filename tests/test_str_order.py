"""Tests for Sort-Tile-Recursive spatial ordering."""

from __future__ import annotations

import io
import json
import logging
import struct
import sys
from unittest import mock

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import write_geoparquet_table
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.parquet_writer import align_to_writer_vector
from geoparquet_io.core.str_order import _str_layout, str_order, str_order_table


def _point_wkb(x: float, y: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, x, y)


def _empty_collection_wkb() -> bytes:
    return struct.pack("<BII", 1, 7, 0)


def _point_table(coords: list[tuple[int, float, float]]) -> pa.Table:
    """Build a metadata-bearing WKB point table from ``(id, x, y)`` rows."""
    metadata = {
        b"geo": json.dumps(
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
            }
        ).encode()
    }
    return pa.table(
        {
            "id": pa.array([row[0] for row in coords]),
            "geometry": pa.array(
                [_point_wkb(row[1], row[2]) for row in coords],
                type=pa.binary(),
            ),
        }
    ).replace_schema_metadata(metadata)


def _points(table: pa.Table) -> list[tuple[float, float]]:
    return [struct.unpack("<BIdd", value)[2:] for value in table["geometry"].to_pylist()]


def _tile_boxes(table: pa.Table, tile_size: int) -> list[tuple[float, float, float, float]]:
    """Return ``(xmin, ymin, xmax, ymax)`` per consecutive ``tile_size`` rows."""
    points = _points(table)
    boxes = []
    for start in range(0, len(points), tile_size):
        chunk = points[start : start + tile_size]
        xs = [point[0] for point in chunk]
        ys = [point[1] for point in chunk]
        boxes.append((min(xs), min(ys), max(xs), max(ys)))
    return boxes


def _mean_candidate_tiles(
    boxes: list[tuple[float, float, float, float]],
    window: float,
    extent: float,
) -> float:
    """Mean tiles a square query window of side ``window`` has to open.

    This is what spatial filter pushdown actually pays for: candidate row
    groups. Total tile *area* is close to invariant under any ordering that
    partitions the data (a plain ``ORDER BY x, y`` reaches the same area as
    STR), so area alone cannot tell a good spatial order from a bad one.
    """
    total = 0
    windows = 0
    step = 1.0
    y = 0.0
    while y < extent:
        x = 0.0
        while x < extent:
            total += sum(
                1
                for (xmin, ymin, xmax, ymax) in boxes
                if not (xmax < x or xmin > x + window or ymax < y or ymin > y + window)
            )
            windows += 1
            x += step
        y += step
    return total / windows


# 400 points spread over a 20x20 extent by two irrational strides: deterministic,
# no grid structure, and every x is distinct so a lexicographic order cannot win
# by producing zero-width tiles.
_SPREAD_EXTENT = 20.0
_SPREAD_POINTS = [
    (
        index,
        ((index * 0.6180339887498949) % 1.0) * _SPREAD_EXTENT,
        ((index * 0.41421356237309515) % 1.0) * _SPREAD_EXTENT,
    )
    for index in range(400)
]


def test_str_order_packs_square_grid_into_snaking_tiles():
    """STR makes X strips and alternates Y direction between adjacent strips."""
    shuffled = [
        (15, 3, 3),
        (0, 0, 0),
        (10, 2, 2),
        (5, 1, 1),
        (12, 3, 0),
        (3, 0, 3),
        (9, 2, 1),
        (6, 1, 2),
        (13, 3, 1),
        (2, 0, 2),
        (8, 2, 0),
        (7, 1, 3),
        (14, 3, 2),
        (1, 0, 1),
        (11, 2, 3),
        (4, 1, 0),
    ]

    result = str_order_table(_point_table(shuffled), tile_size=4)

    assert result["id"].to_pylist() == [0, 4, 1, 5, 2, 6, 3, 7, 11, 15, 10, 14, 9, 13, 8, 12]
    assert result.schema.metadata == _point_table(shuffled).schema.metadata


def test_str_order_beats_lexicographic_order_on_candidate_tiles():
    """STR opens measurably fewer tiles per query window than ``ORDER BY x, y``.

    The lexicographic baseline is the one that matters: it is what you get for
    free, it produces tall thin x-slabs, and on total tile *area* it ties with
    (or narrowly beats) STR - so a test that only measures area passes without
    proving STR does anything spatial.
    """
    tile_size = 20
    scattered = _point_table(_SPREAD_POINTS)
    lexicographic = _point_table(sorted(_SPREAD_POINTS, key=lambda row: (row[1], row[2])))

    result = str_order_table(scattered, tile_size=tile_size)

    str_boxes = _tile_boxes(result, tile_size)
    lex_boxes = _tile_boxes(lexicographic, tile_size)
    scattered_boxes = _tile_boxes(scattered, tile_size)

    str_candidates = _mean_candidate_tiles(str_boxes, 2.0, _SPREAD_EXTENT)
    lex_candidates = _mean_candidate_tiles(lex_boxes, 2.0, _SPREAD_EXTENT)
    scattered_candidates = _mean_candidate_tiles(scattered_boxes, 2.0, _SPREAD_EXTENT)

    # Measured: STR 1.58, lexicographic 2.27, unsorted 19.65 tiles per window.
    assert str_candidates < lex_candidates * 0.8
    assert str_candidates < scattered_candidates * 0.25

    # And the point of the new metric: area does not separate the two orders.
    str_area = sum((b[2] - b[0]) * (b[3] - b[1]) for b in str_boxes)
    lex_area = sum((b[2] - b[0]) * (b[3] - b[1]) for b in lex_boxes)
    assert str_area > lex_area * 0.9


def test_str_order_places_empty_and_null_geometries_last():
    table = pa.table(
        {
            "id": ["null", "high", "empty", "low"],
            "geometry": pa.array(
                [
                    None,
                    _point_wkb(10, 10),
                    _empty_collection_wkb(),
                    _point_wkb(0, 0),
                ],
                type=pa.binary(),
            ),
        }
    )

    result = str_order_table(table, tile_size=1)

    assert result["id"].to_pylist() == ["low", "high", "null", "empty"]


def test_str_order_all_empty_returns_input_unchanged():
    table = pa.table(
        {
            "id": [1, 2],
            "geometry": pa.array([None, _empty_collection_wkb()], type=pa.binary()),
        }
    )

    assert str_order_table(table, tile_size=2).equals(table)


def test_str_order_preserves_columns_that_resemble_internal_names():
    """A user column sharing the real internal prefix survives the EXCLUDE list.

    The internal columns are ``__gpio_str_<role>_<uuid4 hex>``; the uuid suffix
    is what keeps them from colliding, so the collision has to be tested with a
    name carrying the real prefix.
    """
    collider = "__gpio_str_input_order_deadbeef"
    table = _point_table([(1, 1, 1), (2, 0, 0)]).append_column(
        collider, pa.array(["first", "second"])
    )

    result = str_order_table(table, tile_size=1)

    assert result.column_names == table.column_names
    assert result[collider].to_pylist() == ["second", "first"]


@pytest.mark.parametrize("tile_size", [0, -1])
def test_str_order_rejects_non_positive_tile_size(tile_size):
    with pytest.raises(InvalidParameterError):
        str_order_table(_point_table([(1, 0, 0)]), tile_size=tile_size)


def test_str_layout_rounds_strip_size_to_whole_tiles():
    """``strip_size`` is a multiple of ``tile_size``, so tiles never straddle strips."""
    assert _str_layout(row_count=16, tile_size=4) == (4, 2, 8)
    # 5 tiles over 3 strips: 2 tiles per strip (8 rows), not ceil(17 / 3) == 6,
    # which would leave the third tile spanning the strip-0/strip-1 boundary.
    assert _str_layout(row_count=17, tile_size=4) == (5, 3, 8)
    for row_count, tile_size in ((100_000, 10_000), (2_000_000, 122_880), (17, 4)):
        _, _, strip_size = _str_layout(row_count, tile_size)
        assert strip_size % tile_size == 0


def test_str_order_streams_stdin_to_file(tmp_path, monkeypatch):
    table = _point_table([(index, index % 4, index // 4) for index in range(16)])
    stream = io.BytesIO()
    with ipc.RecordBatchStreamWriter(stream, table.schema) as writer:
        writer.write_table(table)
    stream.seek(0)
    mock_stdin = mock.MagicMock()
    mock_stdin.isatty.return_value = False
    mock_stdin.buffer = stream
    monkeypatch.setattr(sys, "stdin", mock_stdin)
    output = tmp_path / "stdin-str.parquet"

    str_order("-", str(output), row_group_rows=4, geoparquet_version="1.1")

    assert pq.read_table(output).num_rows == 16


def test_str_order_streams_file_to_stdout(tmp_path, monkeypatch):
    table = _point_table([(index, index % 4, index // 4) for index in range(16)])
    source = tmp_path / "source.parquet"
    write_geoparquet_table(table, str(source), geometry_column="geometry")
    output = io.BytesIO()
    mock_stdout = mock.MagicMock()
    mock_stdout.isatty.return_value = False
    mock_stdout.buffer = output
    monkeypatch.setattr(sys, "stdout", mock_stdout)

    str_order(str(source), "-", row_group_rows=4)

    output.seek(0)
    assert ipc.RecordBatchStreamReader(output).read_all().num_rows == 16


def test_str_order_file_adds_bbox_and_handles_byte_sized_groups(tmp_path):
    source = tmp_path / "source.parquet"
    output = tmp_path / "str.parquet"
    write_geoparquet_table(
        _point_table([(index, index % 4, index // 4) for index in range(16)]),
        str(source),
        geometry_column="geometry",
    )

    str_order(
        str(source),
        str(output),
        add_bbox_flag=True,
        row_group_size_mb=1,
        geoparquet_version="1.1",
    )

    result = pq.read_table(output)
    assert result.num_rows == 16
    assert "bbox" in result.column_names


def _all_empty_table() -> pa.Table:
    return pa.table(
        {
            "id": [1, 2],
            "geometry": pa.array([None, _empty_collection_wkb()], type=pa.binary()),
        }
    )


def test_str_order_file_passes_through_all_empty_geometries(tmp_path, caplog):
    """The passthrough branch must not claim it reordered anything."""
    source = tmp_path / "empty-source.parquet"
    output = tmp_path / "empty-str.parquet"
    write_geoparquet_table(_all_empty_table(), str(source), geometry_column="geometry")

    with caplog.at_level(logging.INFO, logger="geoparquet_io"):
        str_order(str(source), str(output), geoparquet_version="1.1")

    assert pq.read_table(output).num_rows == 2
    messages = [record.message for record in caplog.records]
    assert any("Wrote data without STR ordering to:" in message for message in messages)
    assert not any("Successfully reordered data using STR" in message for message in messages)


def test_str_order_streaming_passthrough_does_not_claim_it_sorted(tmp_path, monkeypatch, caplog):
    """Same honesty requirement on the streaming dispatch."""
    table = _all_empty_table()
    stream = io.BytesIO()
    with ipc.RecordBatchStreamWriter(stream, table.schema) as writer:
        writer.write_table(table)
    stream.seek(0)
    mock_stdin = mock.MagicMock()
    mock_stdin.isatty.return_value = False
    mock_stdin.buffer = stream
    monkeypatch.setattr(sys, "stdin", mock_stdin)
    output = tmp_path / "empty-stream.parquet"

    with caplog.at_level(logging.INFO, logger="geoparquet_io"):
        str_order("-", str(output), geoparquet_version="1.1")

    assert pq.read_table(output).num_rows == 2
    messages = [record.message for record in caplog.records]
    assert any("Wrote data without STR ordering to:" in message for message in messages)
    assert not any("Successfully reordered data using STR" in message for message in messages)


def test_str_order_verbose_emits_the_layout_it_chose(tmp_path, caplog):
    """``--verbose`` has to actually raise the logger, or its only new line is dead."""
    source = tmp_path / "verbose-source.parquet"
    output = tmp_path / "verbose-str.parquet"
    write_geoparquet_table(
        _point_table([(index, index % 4, index // 4) for index in range(16)]),
        str(source),
        geometry_column="geometry",
    )

    with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
        str_order(
            str(source), str(output), row_group_rows=4, verbose=True, geoparquet_version="1.1"
        )

    assert any("STR layout" in record.message for record in caplog.records)


def test_str_order_streaming_verbose_emits_the_layout_it_chose(tmp_path, monkeypatch, caplog):
    """The streaming dispatch has its own layout line; it must emit too."""
    table = _point_table([(index, index % 4, index // 4) for index in range(16)])
    stream = io.BytesIO()
    with ipc.RecordBatchStreamWriter(stream, table.schema) as writer:
        writer.write_table(table)
    stream.seek(0)
    mock_stdin = mock.MagicMock()
    mock_stdin.isatty.return_value = False
    mock_stdin.buffer = stream
    monkeypatch.setattr(sys, "stdin", mock_stdin)
    output = tmp_path / "verbose-stream.parquet"

    with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
        str_order("-", str(output), row_group_rows=4, verbose=True, geoparquet_version="1.1")

    assert any("STR layout" in record.message for record in caplog.records)


def test_str_order_rejects_zero_row_group_size_by_the_name_the_user_typed(tmp_path):
    """The error names --row-group-size, not the tile_size it is derived into."""
    source = tmp_path / "reject-source.parquet"
    write_geoparquet_table(_point_table([(1, 0, 0)]), str(source), geometry_column="geometry")

    with pytest.raises(InvalidParameterError) as excinfo:
        str_order(str(source), str(tmp_path / "out.parquet"), row_group_rows=0)

    assert excinfo.value.param_name == "--row-group-size"
    assert "tile_size" not in str(excinfo.value)


def test_str_order_streaming_warns_that_add_bbox_is_ignored(tmp_path, monkeypatch, caplog):
    """Streaming has no bbox step (same gap as `sort hilbert`), so say so."""
    table = _point_table([(index, index % 4, index // 4) for index in range(16)])
    stream = io.BytesIO()
    with ipc.RecordBatchStreamWriter(stream, table.schema) as writer:
        writer.write_table(table)
    stream.seek(0)
    mock_stdin = mock.MagicMock()
    mock_stdin.isatty.return_value = False
    mock_stdin.buffer = stream
    monkeypatch.setattr(sys, "stdin", mock_stdin)
    output = tmp_path / "stream-bbox.parquet"

    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        str_order("-", str(output), add_bbox_flag=True, geoparquet_version="1.1")

    assert any("--add-bbox is ignored in streaming mode" in r.message for r in caplog.records)
    assert "bbox" not in pq.read_table(output).column_names


def test_str_order_streaming_uses_the_streams_own_primary_column(tmp_path, monkeypatch):
    """A non-standard primary column works in stream mode, as it does in file mode."""
    metadata = {
        b"geo": json.dumps(
            {
                "version": "1.1.0",
                "primary_column": "footprint",
                "columns": {"footprint": {"encoding": "WKB", "geometry_types": ["Point"]}},
            }
        ).encode()
    }
    table = pa.table(
        {
            "id": pa.array(list(range(16))),
            "footprint": pa.array(
                [_point_wkb(index % 4, index // 4) for index in range(16)],
                type=pa.binary(),
            ),
        }
    ).replace_schema_metadata(metadata)
    stream = io.BytesIO()
    with ipc.RecordBatchStreamWriter(stream, table.schema) as writer:
        writer.write_table(table)
    stream.seek(0)
    mock_stdin = mock.MagicMock()
    mock_stdin.isatty.return_value = False
    mock_stdin.buffer = stream
    monkeypatch.setattr(sys, "stdin", mock_stdin)
    output = tmp_path / "footprint-str.parquet"

    str_order("-", str(output), row_group_rows=4, geoparquet_version="1.1")

    result = pq.read_table(output)
    assert result.num_rows == 16
    assert result.column_names == ["id", "footprint"]


def test_str_order_file_write_preserves_the_in_memory_ordering(tmp_path):
    """The written row order is str_order_table's order - the write does not reshuffle.

    The tile size the file path uses is ``--row-group-size`` snapped to a whole
    2,048-row writer vector (#961), because STR's tile size is meant to be "the
    rows you intend to put in a row group" and that is now the number a row
    group will actually hold. So the in-memory comparison has to be made at the
    same snapped size; comparing against the raw request would be asserting that
    the file path ignores the alignment.
    """
    source = tmp_path / "order-source.parquet"
    output = tmp_path / "order-str.parquet"
    table = _point_table(_SPREAD_POINTS)
    write_geoparquet_table(table, str(source), geometry_column="geometry")

    str_order(str(source), str(output), row_group_rows=20, geoparquet_version="1.1")

    written = pq.read_table(output)
    expected = str_order_table(table, tile_size=align_to_writer_vector(20))
    assert written["id"].to_pylist() == expected["id"].to_pylist()
    assert written["geometry"].to_pylist() == expected["geometry"].to_pylist()
