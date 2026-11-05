"""Comparison-only normalization of fields observed varying on unchanged base.

Raw outputs remain untouched. This policy applies only to the seeded matrix;
all other commands and files retain their literal byte comparison.
"""

import json
import re
import sqlite3
from collections import Counter
from itertools import groupby


def comparable_bytes(path, command):
    """Keep every byte except the documented base nondeterminism below."""
    data = path.read_bytes()
    if command == "benchmark-explain" and path.name == "report.json":
        report = json.loads(data)
        del report["total_time"]
        for operator in report["operators"]:
            del operator["timing"]
        return json.dumps(report, sort_keys=True).encode()
    if command == "convert-shapefile" and path.suffix == ".dbf":
        # DBF header offsets 1..3 are the year/month/day of the write.
        return data[:1] + b"\0\0\0" + data[4:]
    if command == "convert-geopackage" and path.suffix == ".gpkg":
        # Preserve SQLite layout; replace only the last_change cell payloads.
        with sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True) as con:
            stamps = Counter(row[0] for row in con.execute("SELECT last_change FROM gpkg_contents"))
        for stamp, count in stamps.items():
            value = stamp.encode()
            assert re.fullmatch(rb"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z", value)
            assert data.count(value) == count, "ambiguous timestamp outside gpkg_contents"
            data = data.replace(value, b"2000-01-01T00:00:00.000Z")
        return data
    if command == "api-kdtree" and path.name == "result.parquet":
        import pyarrow.parquet as pq

        # Approximate KD-tree uses unseeded reservoir sampling on base too.
        # Keep literal encoded chunks for every other column. Only this column's
        # payload/sizes/statistics and the consequent file offsets may vary.
        parquet = pq.ParquetFile(path)
        cells = parquet.read(columns=["kdtree_cell"])["kdtree_cell"]
        assert cells.null_count == 0
        assert all(re.fullmatch(r"0[01]{3}", cell) for cell in cells.to_pylist())
        metadata = parquet.metadata.to_dict()
        metadata.pop("serialized_size")  # Footer size depends on encoded offsets.
        chunks = []
        for group in metadata["row_groups"]:
            for column in group["columns"]:
                start = column["dictionary_page_offset"] or column["data_page_offset"]
                size = column["total_compressed_size"]
                if column["path_in_schema"] == "kdtree_cell":
                    group["total_byte_size"] -= column["total_uncompressed_size"]
                    column["total_compressed_size"] = 0
                    column["total_uncompressed_size"] = 0
                    column["statistics"] = None
                else:
                    chunks.append(data[start : start + size])
                bloom = column.pop("bloom_filter_offset", None)
                if bloom is not None:
                    chunks.append(data[bloom : bloom + column["bloom_filter_length"]])
                for offset in ["file_offset", "dictionary_page_offset", "data_page_offset"]:
                    column.pop(offset)
        # Preserve exact key/value metadata too, including the encoded Arrow schema.
        description = repr((metadata, sorted((parquet.metadata.metadata or {}).items()))).encode()
        return description + b"".join(chunks)
    if command == "check-all" and path.name == "stdout.txt":
        # Only the printed random-pair estimate; keep the warning and verdict.
        return re.sub(rb"(?<=Poor spatial ordering \(ratio: )\d+\.\d+(?=\))", b"<sampled>", data)
    if command == "inspect-stats" and path.name == "stdout.txt":
        # DuckDB's ratio sort has no tie breaker. Sort only adjacent tied rows
        # of this table, keeping row multiplicity, all cells and other tables.
        prefix, marker, suffix = data.partition(b"Compression Ratios:\n")
        lines = suffix.splitlines(keepends=True)
        row = "│".encode()

        def key(line):
            return line.split(row)[-2].strip() if line.startswith(row) else None

        normalized = []
        for ratio, rows in groupby(lines, key=key):
            values = list(rows)
            normalized.extend(sorted(values) if ratio is not None else values)
        return prefix + marker + b"".join(normalized)
    return data
