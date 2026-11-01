"""One regrouper (``parquet_writer.regroup``) cuts every streamed write into row groups.

The streaming writer, the disk-rewrite strategy and streaming simplify all cut
their rows into groups of exactly ``rows``, the last taking the remainder.

Each batch the streaming writer receives becomes one output row group. DuckDB
1.5's ``fetch_record_batch(n)`` re-chunked a result to ``n`` rows; 2.0's
``to_arrow_reader(batch_size=n)`` only caps a batch at ``n``, so a source
written in small row groups streams back in those small batches and the output
would keep them (``tests/test_write_contract.py::test_a2_row_group_coarsen_contract``).
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from geoparquet_io.core.parquet_writer import regroup


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
    out = list(regroup(iter(_batches(*sizes)), rows))
    assert [b.num_rows for b in out] == expected


def test_rows_keep_their_order_and_values():
    out = list(regroup(iter(_batches(7, 30, 3)), 20))
    assert [v for b in out for v in b.column(0).to_pylist()] == list(range(40))


def test_an_empty_stream_yields_nothing():
    assert list(regroup(iter([]), 10)) == []


def test_tables_regroup_like_batches():
    tables = [pa.Table.from_batches([b]) for b in _batches(7, 30, 3)]
    out = list(regroup(iter(tables), 20))
    assert all(isinstance(t, pa.Table) for t in out)
    assert [t.num_rows for t in out] == [20, 20]
    assert [v for t in out for v in t.column(0).to_pylist()] == list(range(40))


def test_the_three_callers_cut_the_same_groups(tmp_path):
    """Streaming write, disk-rewrite and streaming simplify: same groups for one count."""
    import pyarrow.parquet as pq
    from click.testing import CliRunner

    shapely = pytest.importorskip("shapely")
    from geoparquet_io.cli.main import cli
    from geoparquet_io.core.write_funnels import write_geoparquet_table

    n = 5000
    pts = [shapely.Point(i % 360 - 180, (i * 7) % 140 - 70).buffer(0.1) for i in range(n)]
    src = tmp_path / "src.parquet"
    write_geoparquet_table(
        pa.table({"i": list(range(n)), "geometry": shapely.to_wkb(pts).tolist()}),
        str(src),
        row_group_rows=2048,
    )
    # Irregular source groups, so every caller has to regroup.
    irregular = tmp_path / "irregular.parquet"
    pq.write_table(pq.read_table(src), irregular, row_group_size=700)
    runs = {
        "streaming": ["extract", str(irregular), "{o}", "--write-strategy", "streaming"],
        "disk-rewrite": ["extract", str(irregular), "{o}", "--write-strategy", "disk-rewrite"],
        "simplify": ["process", "simplify", str(irregular), "{o}", "--tolerance", "0.001"],
    }
    sizes = {}
    for name, args in runs.items():
        out = tmp_path / f"{name}.parquet"
        argv = [a.format(o=out) for a in args] + ["--row-group-size", "2048"]
        result = CliRunner().invoke(cli, argv)
        assert result.exit_code == 0, (name, result.output)
        meta = pq.ParquetFile(out).metadata
        sizes[name] = [meta.row_group(i).num_rows for i in range(meta.num_row_groups)]
    assert sizes["streaming"] == sizes["disk-rewrite"] == sizes["simplify"] == [2048, 2048, 904]
