"""Bound the Arrow side of stdout independently of the selection's row count."""

import json
import sys
import weakref
from unittest.mock import MagicMock

import duckdb
import pyarrow as pa

from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.stream_io import write_output
from scripts.profile_command import CallSpies


class _BufferOwner:
    def __init__(self, buffer):
        self.buffer = buffer


class _ArrowLiveBytes:
    """Observe buffer release through C++ aliases, including DuckDB-owned data."""

    def __init__(self):
        self.live = self.peak = 0
        self.seen = set()

    def buffer(self, buffer):
        if buffer is None or not buffer.size:
            return buffer
        key = (buffer.address, buffer.size)
        if key in self.seen:
            return buffer
        self.seen.add(key)
        self.live += buffer.size
        self.peak = max(self.peak, self.live)
        owner = _BufferOwner(buffer)
        weakref.finalize(owner, self.release, key, buffer.size)
        return pa.foreign_buffer(buffer.address, buffer.size, base=owner)

    def release(self, key, size):
        self.live -= size
        self.seen.remove(key)

    def array(self, array):
        if isinstance(array, pa.ExtensionArray):
            return pa.ExtensionArray.from_storage(array.type, self.array(array.storage))
        # This fixture exports only primitive values and WKB, with no children.
        assert array.type.num_fields == 0
        return pa.Array.from_buffers(
            array.type,
            len(array),
            [self.buffer(b) for b in array.buffers()],
            offset=array.offset,
            null_count=array.null_count,
        )

    def batch(self, batch):
        return pa.RecordBatch.from_arrays(
            [self.array(array) for array in batch.columns], schema=batch.schema
        )

    def table(self, table):
        return pa.Table.from_batches(
            [self.batch(b) for b in table.to_batches()], schema=table.schema
        )


class _ObservedReader:
    def __init__(self, reader, tracker):
        self.reader, self.tracker = reader, tracker

    def __iter__(self):
        for batch in self.reader:
            yield self.tracker.batch(batch)

    def __getattr__(self, name):
        return getattr(self.reader, name)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.reader.close()


def assert_stdout_arrow_memory_is_bounded(tmp_path, monkeypatch):
    import geoparquet_io.core.stream_io as stream_io

    metadata = {
        b"geo": json.dumps(
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {"geometry": {"encoding": "WKB"}},
            }
        ).encode()
    }
    peaks = []
    input_peaks = []
    for count in (20_000, 200_000):
        live = []
        stream = (tmp_path / f"{count}.arrow").open("wb")

        class Sink:
            closed = False

            def __init__(self, stream, live):
                self.stream = stream
                self.live = live

            def write(self, data):
                self.live.append(pa.total_allocated_bytes())
                return self.stream.write(data)

            def writable(self):
                return True

        monkeypatch.setattr(
            sys, "stdout", MagicMock(buffer=Sink(stream, live), isatty=lambda: False)
        )
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            query = f"SELECT i, ST_Buffer(ST_Point(i % 100, i % 90), 0.1) geometry FROM range({count}) t(i)"
            start = pa.total_allocated_bytes()
            tracker = _ArrowLiveBytes()
            original_reader = duckdb.DuckDBPyRelation.to_arrow_reader
            original_convert = stream_io.apply_geoarrow_extension_type

            def observed_reader(
                relation, *args, tracker=tracker, original=original_reader, **kwargs
            ):
                return _ObservedReader(original(relation, *args, **kwargs), tracker)

            def converted(table, *args, tracker=tracker, original=original_convert, **kwargs):
                return tracker.table(original(table, *args, **kwargs))

            with monkeypatch.context() as patches, CallSpies() as spies:
                patches.setattr(duckdb.DuckDBPyRelation, "to_arrow_reader", observed_reader)
                patches.setattr(stream_io, "apply_geoarrow_extension_type", converted)
                write_output(con, query, "-", original_metadata=metadata)
            assert spies.counts["arrow_read_all"] == 0
            peaks.append(tracker.peak + max(live) - start)
            stream.close()
            from geoparquet_io.core.file_utils import unlink_released_file
            from geoparquet_io.core.write_funnels import read_stdin_to_temp_file

            with (tmp_path / f"{count}.arrow").open("rb") as source:
                monkeypatch.setattr(sys, "stdin", MagicMock(buffer=source, isatty=lambda: False))
                import pyarrow.parquet as pq

                from geoparquet_io.core import streaming

                input_live = []
                input_tracker = _ArrowLiveBytes()
                original_stream = streaming.read_arrow_stream
                original_write = pq.ParquetWriter.write_table
                input_start = pa.total_allocated_bytes()

                def measured_write(
                    writer,
                    table,
                    *args,
                    input_live=input_live,
                    input_start=input_start,
                    original_write=original_write,
                    **kwargs,
                ):
                    input_live.append(pa.total_allocated_bytes() - input_start)
                    return original_write(writer, table, *args, **kwargs)

                def observed_input(original=original_stream, tracker=input_tracker):
                    return _ObservedReader(original(), tracker)

                with monkeypatch.context() as patches, CallSpies() as input_spies:
                    patches.setattr(streaming, "read_arrow_stream", observed_input)
                    patches.setattr(pq.ParquetWriter, "write_table", measured_write)
                    scratch = read_stdin_to_temp_file()
                input_peaks.append(input_tracker.peak + max(input_live))
                unlink_released_file(scratch)
                assert input_spies.counts["arrow_read_all"] == 0
        finally:
            con.close()
    # 4096 polygons take <5 MB; allow a few simultaneously live batches.
    assert max(peaks) < 16_000_000, peaks
    assert peaks[1] <= peaks[0] * 2 + 1_000_000, peaks
    assert max(input_peaks) < 16_000_000, input_peaks
    assert input_peaks[1] <= input_peaks[0] * 2 + 1_000_000, input_peaks
    return peaks, input_peaks


def assert_stream_geo_matches_head_for_changed_selections(monkeypatch):
    """Raw geo bytes captured from a6d86cb8, before batching (#1126)."""
    from pathlib import Path

    import geoparquet_io.core.stream_io as stream_io

    cases = json.loads((Path(__file__).parent / "data" / "stream_geo_head.json").read_text())
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        for name, case in cases.items():
            captured = []
            monkeypatch.setattr(
                stream_io,
                "write_arrow_stream",
                lambda reader, captured=captured: captured.append(reader.read_all()),
            )
            stream_io._write_stream_output(
                con, case["query"], {b"geo": case["metadata"].encode()}, None
            )
            assert captured[0].schema.metadata[b"geo"] == case["geo"].encode(), name
        # Replaying this query would measure different coordinates and can
        # choose a different subset. The snapshot must be the single witness.
        query = "SELECT i, ST_Point(random()*10, i) geometry FROM range(1000) t(i) USING SAMPLE 20% (bernoulli, 42) LIMIT 50"
        captured = []
        monkeypatch.setattr(
            stream_io, "write_arrow_stream", lambda reader: captured.append(reader.read_all())
        )
        stream_io._write_stream_output(
            con, query, {b"geo": cases["filtered"]["metadata"].encode()}, None
        )
        from geoparquet_io.core.geo_metadata import geometry_stats_of

        table = captured[0]
        actual = geometry_stats_of(table, "geometry")
        assert (
            json.loads(table.schema.metadata[b"geo"])["columns"]["geometry"]["bbox"] == actual.bbox
        )
        from unittest.mock import patch

        import pytest

        from geoparquet_io.core import geo_metadata

        # Complete carried stats need neither a snapshot nor a measurement.
        statements = []
        execute = duckdb.DuckDBPyConnection.execute

        def observed_execute(connection, query, *args, **kwargs):
            statements.append(query)
            return execute(connection, query, *args, **kwargs)

        with (
            patch.object(duckdb.DuckDBPyConnection, "execute", observed_execute),
            patch.object(
                geo_metadata, "geometry_stats_of", side_effect=AssertionError("unexpected scan")
            ),
        ):
            stream_io._write_stream_output(
                con, cases["native"]["query"], {b"geo": cases["native"]["geo"].encode()}, None
            )
        assert not any(q.startswith("CREATE TEMP TABLE") for q in statements)

        def failed_writer(reader):
            raise OSError("sink closed")

        with monkeypatch.context() as patches:
            patches.setattr(stream_io, "write_arrow_stream", failed_writer)
            with pytest.raises(OSError, match="sink closed"):
                stream_io._write_stream_output(
                    con,
                    "SELECT ST_Point(i,i) geometry FROM range(20000) t(i)",
                    {b"geo": cases["native"]["metadata"].encode()},
                    None,
                )
        assert con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name LIKE '__gpio_stream_%'"
        ).fetchall() == [(0,)]
    finally:
        con.close()
