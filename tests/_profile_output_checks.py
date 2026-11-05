"""Only demonstrated nondeterministic fields may disappear from comparisons."""

import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.benchmarks.output_comparison import comparable_bytes


def check_dbf_only_date_bytes(tmp_path):
    path = tmp_path / "result.dbf"
    path.write_bytes(b"\x03\x7e\x0b\x04payload")
    first = comparable_bytes(path, "convert-shapefile")
    path.write_bytes(b"\x03\x7e\x0b\x05payload")
    assert comparable_bytes(path, "convert-shapefile") == first
    path.write_bytes(b"\x03\x7e\x0b\x05changed")
    assert comparable_bytes(path, "convert-shapefile") != first


def check_gpkg_only_last_change(tmp_path):
    path = tmp_path / "result.gpkg"
    with sqlite3.connect(path) as con:
        con.execute("CREATE TABLE gpkg_contents (last_change TEXT, identifier TEXT)")
        con.execute("INSERT INTO gpkg_contents VALUES ('2026-11-04T01:02:03.000Z', 'original')")
    first = comparable_bytes(path, "convert-geopackage")
    raw = path.read_bytes()
    path.write_bytes(raw.replace(b"2026-11-04T01:02:03.000Z", b"2026-11-05T01:02:03.000Z"))
    assert comparable_bytes(path, "convert-geopackage") == first
    path.write_bytes(path.read_bytes().replace(b"original", b"modified"))
    assert comparable_bytes(path, "convert-geopackage") != first


def check_kdtree_only_sampled_cell(tmp_path):
    path = tmp_path / "result.parquet"

    def write(cell, value=1, metadata=b"original", compression="snappy"):
        table = pa.table({"id": [1], "value": [value], "kdtree_cell": [cell]})
        pq.write_table(
            table.replace_schema_metadata({b"geo": metadata}), path, compression=compression
        )

    write("0000")
    first = comparable_bytes(path, "api-kdtree")
    write("0111")
    assert comparable_bytes(path, "api-kdtree") == first
    write("0111", value=2)
    assert comparable_bytes(path, "api-kdtree") != first
    write("0111", metadata=b"changed")
    assert comparable_bytes(path, "api-kdtree") != first
    write("0111", compression="zstd")
    assert comparable_bytes(path, "api-kdtree") != first


def check_stdout_only_ratio_and_tied_order(tmp_path):
    path = tmp_path / "stdout.txt"
    path.write_bytes(b"warning Poor spatial ordering (ratio: 1.04)\n")
    first = comparable_bytes(path, "check-all")
    path.write_bytes(b"warning Poor spatial ordering (ratio: 1.05)\n")
    assert comparable_bytes(path, "check-all") == first
    path.write_bytes(b"PASS Poor spatial ordering (ratio: 1.05)\n")
    assert comparable_bytes(path, "check-all") != first
    header = "Compression Ratios:\nheader\n"
    a, b = "│ a │ SNAPPY │ 10 │ 20 │ 2.0 │\n", "│ b │ SNAPPY │ 10 │ 20 │ 2.0 │\n"
    path.write_text(header + a + b + "end\n")
    first = comparable_bytes(path, "inspect-stats")
    path.write_text(header + b + a + "end\n")
    assert comparable_bytes(path, "inspect-stats") == first
    path.write_text(header + b.replace("20", "30") + a + "end\n")
    assert comparable_bytes(path, "inspect-stats") != first
