"""The streaming writer re-chunks DuckDB's batches to the requested row count.

Each batch the streaming writer receives becomes one output row group. DuckDB
1.5's ``fetch_record_batch(n)`` re-chunked a result to ``n`` rows; 2.0's
``to_arrow_reader(batch_size=n)`` only caps a batch at ``n``, so a source
written in small row groups streams back in those small batches and the output
would keep them (``tests/test_write_contract.py::test_a2_row_group_coarsen_contract``).
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from geoparquet_io.core.write_strategies.arrow_streaming import _rebatch


def _batches(*sizes: int) -> list[pa.RecordBatch]:
    start, out = 0, []
    for size in sizes:
        out.append(pa.record_batch({"i": pa.array(range(start, start + size), pa.int64())}))
        start += size
    return out


@pytest.mark.parametrize(
    ("sizes", "rows", "expected"),
    [
        ((10, 10, 10, 10), 40, [40]),
        ((10, 10, 10, 10), 25, [25, 15]),
        ((100,), 40, [40, 40, 20]),
        ((7, 30, 3), 20, [20, 20]),
        ((5,), 40, [5]),
    ],
    ids=["coarsen", "uneven", "split", "straddle", "short"],
)
def test_batches_come_out_at_the_requested_size(sizes, rows, expected):
    out = list(_rebatch(iter(_batches(*sizes)), rows))
    assert [b.num_rows for b in out] == expected


def test_rows_keep_their_order_and_values():
    out = list(_rebatch(iter(_batches(7, 30, 3)), 20))
    assert [v for b in out for v in b.column(0).to_pylist()] == list(range(40))


def test_an_empty_stream_yields_nothing():
    assert list(_rebatch(iter([]), 10)) == []
