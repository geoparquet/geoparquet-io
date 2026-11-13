"""
Tests for Arrow IPC streaming utilities.

Tests the low-level streaming primitives in core/streaming.py
and the high-level abstractions in core/stream_io.py.
"""

from __future__ import annotations

import io
import json
import logging
import sys
from unittest import mock

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from geoparquet_io.core.crs_utils import crs_of
from geoparquet_io.core.duckdb_utils import fetch_one
from geoparquet_io.core.geo_metadata import read_raw_geo, with_geo_block
from geoparquet_io.core.geometry_detection import primary_geometry_column
from geoparquet_io.core.streaming import (
    STREAM_MARKER,
    StreamingError,
    _rebatch_wkb_under_byte_limit,
    _split_array_under_byte_limit,
    _wkb_chunk_data_nbytes,
    apply_geoarrow_extension_type,
    apply_metadata_to_table,
    is_stdin,
    read_arrow_stream,
    should_stream_output,
    validate_output,
    validate_stdin,
    write_arrow_stream,
)


class TestStreamMarker:
    """Tests for stream marker detection."""

    def test_is_stdin_with_marker(self):
        assert is_stdin("-") is True

    def test_is_stdin_with_file(self):
        assert is_stdin("/path/to/file.parquet") is False

    def test_is_stdin_with_none(self):
        assert is_stdin(None) is False

    def test_stream_marker_value(self):
        assert STREAM_MARKER == "-"


class TestShouldStreamOutput:
    """Tests for output stream detection."""

    def test_explicit_stdout_marker(self):
        assert should_stream_output("-") is True

    def test_file_path_returns_false(self):
        assert should_stream_output("/path/to/file.parquet") is False

    def test_none_with_tty_returns_false(self):
        with mock.patch.object(sys.stdout, "isatty", return_value=True):
            assert should_stream_output(None) is False

    def test_none_with_pipe_returns_true(self):
        with mock.patch.object(sys.stdout, "isatty", return_value=False):
            assert should_stream_output(None) is True


class TestValidation:
    """Tests for stdin/stdout validation."""

    def test_validate_stdin_raises_when_terminal(self):
        with mock.patch.object(sys.stdin, "isatty", return_value=True):
            with pytest.raises(StreamingError, match="No data on stdin"):
                validate_stdin()

    def test_validate_stdin_passes_when_piped(self):
        with mock.patch.object(sys.stdin, "isatty", return_value=False):
            # Should not raise
            validate_stdin()

    def test_validate_output_raises_when_no_output_and_terminal(self):
        with mock.patch.object(sys.stdout, "isatty", return_value=True):
            with pytest.raises(StreamingError, match="Missing output"):
                validate_output(None)

    def test_validate_output_passes_when_file_provided(self):
        with mock.patch.object(sys.stdout, "isatty", return_value=True):
            # Should not raise
            validate_output("/path/to/output.parquet")

    def test_validate_output_passes_when_stdout_piped(self):
        with mock.patch.object(sys.stdout, "isatty", return_value=False):
            # Should not raise
            validate_output(None)


class TestArrowStreamIO:
    """Tests for Arrow IPC read/write operations."""

    @pytest.fixture
    def sample_table(self):
        """Create a simple test table."""
        return pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    @pytest.fixture
    def geo_table(self):
        """Create a table with geometry column (as WKB)."""
        # Create simple WKB point geometry (POINT(0 0))
        # WKB format: 1 byte order + 4 bytes type + 8 bytes X + 8 bytes Y = 21 bytes
        wkb_point = bytes.fromhex("010100000000000000000000000000000000000000")
        return pa.table({"id": [1, 2], "geometry": [wkb_point, wkb_point], "name": ["a", "b"]})

    def test_write_and_read_roundtrip(self, sample_table):
        """Test that data survives write/read roundtrip."""
        # Write to buffer
        buffer = io.BytesIO()
        writer = ipc.RecordBatchStreamWriter(buffer, sample_table.schema)
        writer.write_table(sample_table)
        writer.close()

        # Read back
        buffer.seek(0)
        reader = ipc.RecordBatchStreamReader(buffer)
        result = reader.read_all()

        assert result.equals(sample_table)

    def test_read_arrow_stream_from_stdin(self, sample_table, monkeypatch):
        """Test reading Arrow IPC from mocked stdin."""
        # Create IPC buffer
        ipc_buffer = io.BytesIO()
        writer = ipc.RecordBatchStreamWriter(ipc_buffer, sample_table.schema)
        writer.write_table(sample_table)
        writer.close()
        ipc_buffer.seek(0)

        # Create a mock stdin with buffer attribute
        mock_stdin = mock.MagicMock()
        mock_stdin.isatty.return_value = False
        mock_stdin.buffer = ipc_buffer

        monkeypatch.setattr(sys, "stdin", mock_stdin)
        result = read_arrow_stream()

        assert result.read_all().equals(sample_table)

    def test_write_arrow_stream_to_stdout(self, sample_table, monkeypatch):
        """Test writing Arrow IPC to mocked stdout."""
        output_buffer = io.BytesIO()

        # Create a mock stdout with buffer attribute
        mock_stdout = mock.MagicMock()
        mock_stdout.buffer = output_buffer

        monkeypatch.setattr(sys, "stdout", mock_stdout)
        write_arrow_stream(sample_table)

        # Read back and verify
        output_buffer.seek(0)
        reader = ipc.RecordBatchStreamReader(output_buffer)
        result = reader.read_all()

        assert result.equals(sample_table)

    def test_read_arrow_stream_raises_on_invalid_data(self, monkeypatch, tmp_path, sample_table):
        """Test that invalid data raises StreamingError."""
        invalid_buffer = io.BytesIO(b"not arrow ipc data")

        mock_stdin = mock.MagicMock()
        mock_stdin.isatty.return_value = False
        mock_stdin.buffer = invalid_buffer

        monkeypatch.setattr(sys, "stdin", mock_stdin)
        with pytest.raises(StreamingError, match="Invalid Arrow IPC stream"):
            read_arrow_stream()

        # A valid schema followed by a truncated batch fails during draining.
        data = io.BytesIO()
        with ipc.new_stream(data, sample_table.schema) as writer:
            writer.write_table(sample_table)
        mock_stdin.buffer = io.BytesIO(data.getvalue()[:-16])
        from geoparquet_io.core.write_funnels import read_stdin_to_temp_file

        scratch = tmp_path / "broken.parquet"
        monkeypatch.setattr(
            "geoparquet_io.core.file_utils.scratch_path", lambda **kwargs: str(scratch)
        )
        with pytest.raises(StreamingError, match="Invalid Arrow IPC stream"):
            read_stdin_to_temp_file()
        assert not scratch.exists()


class TestMetadataHandling:
    """Tests for GeoParquet metadata preservation."""

    @pytest.fixture
    def geo_metadata(self):
        """Sample GeoParquet metadata."""
        return {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                }
            },
        }

    @pytest.fixture
    def table_with_geo_metadata(self, geo_metadata):
        """Create a table with geo metadata."""
        table = pa.table({"id": [1, 2], "geometry": [b"wkb1", b"wkb2"]})
        metadata = {b"geo": json.dumps(geo_metadata).encode("utf-8")}
        return table.replace_schema_metadata(metadata)

    def test_read_raw_geo_from_table(self, table_with_geo_metadata, geo_metadata):
        """Test extracting geo metadata from table."""
        result = read_raw_geo(table_with_geo_metadata.schema.metadata).value
        assert result == geo_metadata

    def test_read_raw_geo_returns_none_when_missing(self):
        """Test that missing metadata returns None."""
        table = pa.table({"id": [1, 2]})
        assert read_raw_geo(table.schema.metadata).value is None

    def test_with_geo_block(self, geo_metadata):
        """The one block attacher (``geo_metadata.with_geo_block``) keeps the block as given."""
        table = pa.table({"id": [1, 2]})
        result = with_geo_block(table, geo_metadata)

        # Verify metadata was applied
        assert b"geo" in result.schema.metadata
        stored = json.loads(result.schema.metadata[b"geo"].decode("utf-8"))
        assert stored == geo_metadata

    def test_apply_metadata_to_table(self):
        """Test applying raw metadata dict."""
        table = pa.table({"id": [1, 2]})
        metadata = {b"key1": b"value1", b"key2": b"value2"}

        result = apply_metadata_to_table(table, metadata)

        assert result.schema.metadata[b"key1"] == b"value1"
        assert result.schema.metadata[b"key2"] == b"value2"

    def test_apply_metadata_to_table_with_none(self):
        """Test that None metadata returns unchanged table."""
        table = pa.table({"id": [1, 2]})
        result = apply_metadata_to_table(table, None)
        assert result is table

    def test_metadata_survives_roundtrip(self, geo_metadata):
        """Test that geo metadata survives Arrow IPC roundtrip."""
        # Create table with metadata
        table = pa.table({"id": [1, 2], "geometry": [b"wkb1", b"wkb2"]})
        table = with_geo_block(table, geo_metadata)

        # Write to buffer
        buffer = io.BytesIO()
        writer = ipc.RecordBatchStreamWriter(buffer, table.schema)
        writer.write_table(table)
        writer.close()

        # Read back
        buffer.seek(0)
        reader = ipc.RecordBatchStreamReader(buffer)
        result = reader.read_all()

        # Verify metadata survived
        restored = read_raw_geo(result.schema.metadata).value
        assert restored == geo_metadata


class TestGeometryColumnDetection:
    """Tests for geometry column detection."""

    def test_primary_geometry_column_of_a_table_prefers_a_present_primary(self):
        """A declared primary the table lacks does not win over the schema."""
        table = pa.table({"id": [1], "geom": [b"wkb"]}).replace_schema_metadata(
            {b"geo": json.dumps({"primary_column": "gone"}).encode("utf-8")}
        )
        assert primary_geometry_column(table) == "geom"

    def test_primary_geometry_column_of_a_table_with_metadata(self):
        """Test finding geometry from table with metadata."""
        geo_meta = {"primary_column": "geom"}
        table = pa.table({"id": [1], "geom": [b"wkb"]})
        metadata = {b"geo": json.dumps(geo_meta).encode("utf-8")}
        table = table.replace_schema_metadata(metadata)

        assert primary_geometry_column(table) == "geom"

    def test_primary_geometry_column_of_a_table_common_names(self):
        """Test finding geometry from common column names."""
        # Test 'geometry'
        table = pa.table({"id": [1], "geometry": [b"wkb"]})
        assert primary_geometry_column(table) == "geometry"

        # Test 'geom'
        table = pa.table({"id": [1], "geom": [b"wkb"]})
        assert primary_geometry_column(table) == "geom"

        # Test 'the_geom'
        table = pa.table({"id": [1], "the_geom": [b"wkb"]})
        assert primary_geometry_column(table) == "the_geom"

    def test_primary_geometry_column_of_a_table_no_match(self):
        """Test None when no geometry column found."""
        table = pa.table({"id": [1], "name": ["test"]})
        assert primary_geometry_column(table) is None


@pytest.mark.slow
class TestStreamIO:
    """Tests for stream_io.py high-level abstractions."""

    @pytest.fixture
    def sample_geo_table(self):
        """Create a table with WKB geometry."""
        # Simple WKB POINT(1 2)
        wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
        return pa.table({"id": [1, 2], "geometry": [wkb, wkb], "name": ["a", "b"]})

    @pytest.fixture
    def geo_metadata(self):
        """Sample GeoParquet metadata."""
        return {
            b"geo": json.dumps(
                {
                    "version": "1.1.0",
                    "primary_column": "geometry",
                    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
                }
            ).encode("utf-8")
        }

    def test_wrap_query_with_wkb_conversion(self):
        """Test WKB wrapping of queries."""
        from geoparquet_io.core.duckdb_utils import _wrap_query_with_wkb_conversion

        query = "SELECT * FROM data"
        result = _wrap_query_with_wkb_conversion(query, "geometry")
        # The geometry column is an identifier and is quoted at every raw SQL
        # interpolation (see CLAUDE.md's DuckDB patterns; enforced since #662).
        assert 'ST_AsWKB("geometry")' in result
        assert "REPLACE" in result

    def test_wrap_query_with_wkb_conversion_no_geom(self):
        """Test that None geometry skips wrapping."""
        from geoparquet_io.core.duckdb_utils import _wrap_query_with_wkb_conversion

        query = "SELECT * FROM data"
        result = _wrap_query_with_wkb_conversion(query, None)
        assert result == query

    def test_open_input_with_file(self, tmp_path, sample_geo_table, geo_metadata):
        """Test open_input with a file path."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.memory_limits import (
            _MATERIALIZED_BYTES_PER_THREAD,
            open_bounded_connection,
        )
        from geoparquet_io.core.stream_io import open_input

        # Write test file
        test_file = tmp_path / "test.parquet"
        table_with_meta = sample_geo_table.replace_schema_metadata(geo_metadata)
        pq.write_table(table_with_meta, str(test_file))

        with (
            mock.patch(
                "geoparquet_io.core.memory_limits.open_bounded_connection",
                wraps=open_bounded_connection,
            ) as bounded,
            open_input(str(test_file), stream_output=True) as (source, metadata, is_stream, con),
        ):
            assert is_stream is False
            assert "read_parquet" in source
            assert metadata is not None
            assert con is not None
            bounded.assert_called_once_with(
                load_spatial=True,
                load_httpfs=False,
                preserve_insertion_order=True,
                per_thread=_MATERIALIZED_BYTES_PER_THREAD,
            )
            assert con.execute("SELECT current_setting('preserve_insertion_order')").fetchall() == [
                (True,)
            ]

    def test_open_input_with_stdin(self, sample_geo_table, geo_metadata, monkeypatch):
        """Test open_input with stdin."""
        from geoparquet_io.core.stream_io import open_input

        # Create Arrow IPC buffer
        table_with_meta = sample_geo_table.replace_schema_metadata(geo_metadata)
        ipc_buffer = io.BytesIO()
        writer = ipc.RecordBatchStreamWriter(ipc_buffer, table_with_meta.schema)
        writer.write_table(table_with_meta)
        writer.close()
        ipc_buffer.seek(0)

        # Mock stdin
        mock_stdin = mock.MagicMock()
        mock_stdin.isatty.return_value = False
        mock_stdin.buffer = ipc_buffer

        monkeypatch.setattr(sys, "stdin", mock_stdin)

        from geoparquet_io.core.memory_limits import (
            _MATERIALIZED_BYTES_PER_THREAD,
            open_bounded_connection,
        )

        with (
            mock.patch(
                "geoparquet_io.core.memory_limits.open_bounded_connection",
                wraps=open_bounded_connection,
            ) as bounded,
            open_input("-") as (source, metadata, is_stream, con),
        ):
            bounded.assert_called_once_with(
                load_spatial=True,
                load_httpfs=False,
                preserve_insertion_order=True,
                per_thread=_MATERIALIZED_BYTES_PER_THREAD,
            )
            assert is_stream is True
            # Source should be a view name for stream input
            assert con.sql(f"SELECT count(*) FROM {source}").fetchall() == [(2,)]
            assert con.sql(f"SELECT ST_X(geometry) FROM {source}").fetchall() == [(1.0,), (1.0,)]
            assert con is not None

    def test_write_output_to_file(self, tmp_path, sample_geo_table, geo_metadata):
        """Test write_output to a file."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.stream_io import write_output

        # Write input file first (so DuckDB sees geometry as proper type)
        input_file = tmp_path / "input.parquet"
        table_with_meta = sample_geo_table.replace_schema_metadata(geo_metadata)
        pq.write_table(table_with_meta, str(input_file))

        # Create connection and query from file
        con = get_duckdb_connection(load_spatial=True)

        output_file = tmp_path / "output.parquet"

        result = write_output(
            con,
            f"SELECT * FROM read_parquet('{input_file}')",
            str(output_file),
            original_metadata=geo_metadata,
            verbose=False,
        )

        # File output returns None
        assert result is None
        assert output_file.exists()

        con.close()

    def test_write_output_to_stream(self, tmp_path, sample_geo_table, geo_metadata, monkeypatch):
        """Test write_output to stdout stream."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.stream_io import write_output

        # Write input file first (so DuckDB sees geometry as proper type)
        input_file = tmp_path / "input.parquet"
        table_with_meta = sample_geo_table.replace_schema_metadata(geo_metadata)
        pq.write_table(table_with_meta, str(input_file))

        # Create connection
        con = get_duckdb_connection(load_spatial=True)

        # Mock stdout
        output_buffer = io.BytesIO()
        mock_stdout = mock.MagicMock()
        mock_stdout.buffer = output_buffer
        mock_stdout.isatty.return_value = False

        monkeypatch.setattr(sys, "stdout", mock_stdout)

        result = write_output(
            con,
            f"SELECT * FROM read_parquet('{input_file}')",
            "-",  # Explicit stdout marker
            original_metadata=geo_metadata,
            verbose=False,
        )

        # Stream output returns the table
        assert result is not None
        assert isinstance(result, pa.Table)

        # Verify output is valid Arrow IPC
        output_buffer.seek(0)
        reader = ipc.RecordBatchStreamReader(output_buffer)
        read_table = reader.read_all()
        assert read_table.num_rows == 2

        con.close()
        from tests.stream_assertions import assert_stdout_arrow_memory_is_bounded

        assert_stdout_arrow_memory_is_bounded(tmp_path, monkeypatch)

    def test_execute_transform_file_to_file(
        self, tmp_path, sample_geo_table, geo_metadata, monkeypatch
    ):
        """Test execute_transform with file input and file output."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.stream_io import execute_transform

        # Write input file
        input_file = tmp_path / "input.parquet"
        table_with_meta = sample_geo_table.replace_schema_metadata(geo_metadata)
        pq.write_table(table_with_meta, str(input_file))

        output_file = tmp_path / "output.parquet"

        def transform_fn(source, con):
            return f"SELECT id, name FROM {source}"

        result = execute_transform(
            str(input_file),
            str(output_file),
            transform_fn,
            verbose=False,
        )

        assert result is None  # File output
        assert output_file.exists()

        # Verify output content
        out_table = pq.read_table(str(output_file))
        assert "id" in out_table.column_names
        assert "name" in out_table.column_names

        # A geometry-free projection must not recreate geo from custom metadata.
        output_buffer = io.BytesIO()
        mock_stdout = mock.MagicMock()
        mock_stdout.buffer = output_buffer
        monkeypatch.setattr(sys, "stdout", mock_stdout)
        execute_transform(
            str(input_file),
            "-",
            transform_fn,
            custom_metadata={"covering": {"bbox": {"xmin": ["bbox", "xmin"]}}},
        )
        output_buffer.seek(0)
        streamed = ipc.open_stream(output_buffer).read_all()
        assert streamed.column_names == ["id", "name"]
        assert streamed.num_rows == sample_geo_table.num_rows
        assert b"geo" not in (streamed.schema.metadata or {})

    def test_projected_secondary_custom_covering_matches_file(self, tmp_path, monkeypatch, caplog):
        import pyarrow.parquet as pq

        from geoparquet_io.core.geo_metadata import build_bbox_covering
        from geoparquet_io.core.stream_io import execute_transform

        wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
        table = pa.table({"id": [1], "geometry": [wkb], "secondary": [wkb]})
        entry = {"encoding": "WKB", "geometry_types": ["Point"], "bbox": [1, 2, 1, 2]}
        table = with_geo_block(
            table,
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {"geometry": entry, "secondary": entry},
            },
        )
        source = tmp_path / "source.parquet"
        pq.write_table(table, source)
        custom = {"covering": {"bbox": build_bbox_covering("computed")}}
        for selected in ("*", "id, secondary", "id"):

            def transform(ref, con, selected=selected):
                return (
                    f"SELECT {selected}, {{'xmin': 1.0::DOUBLE, 'ymin': 2.0::DOUBLE, "
                    f"'xmax': 1.0::DOUBLE, 'ymax': 2.0::DOUBLE}} AS computed FROM {ref}"
                )

            dest = tmp_path / "output.parquet"
            execute_transform(str(source), str(dest), transform, custom_metadata=custom)
            captured = []
            monkeypatch.setattr(
                "geoparquet_io.core.stream_io.write_arrow_stream",
                lambda reader, captured=captured: captured.append(reader.read_all()),
            )
            execute_transform(str(source), "-", transform, custom_metadata=custom)
            expected = read_raw_geo(pq.read_metadata(dest).metadata).value
            actual = read_raw_geo(captured[0].schema.metadata).value
            assert actual == expected, selected
            if selected == "id, secondary":
                assert actual["primary_column"] == "secondary"
                assert (
                    actual["columns"]["secondary"]["covering"]["bbox"] == custom["covering"]["bbox"]
                )
            elif selected == "id":
                assert actual is None

        from geoparquet_io.core.add.bbox import add_bbox_column
        from geoparquet_io.core.extract import extract
        from geoparquet_io.core.hilbert_order import hilbert_order
        from geoparquet_io.core.sort_by_column import sort_by_column
        from geoparquet_io.core.sort_quadkey import sort_by_quadkey
        from geoparquet_io.core.str_order import str_order

        transforms = [
            (hilbert_order, {}),
            (str_order, {}),
            (sort_by_column, {"columns": "id"}),
            (sort_by_quadkey, {}),
            (hilbert_order, {"add_bbox_flag": True}),
            (str_order, {"add_bbox_flag": True}),
            (extract, {}),
            (extract, {"exclude_cols": "geometry"}),
            (extract, {"exclude_cols": "geometry,secondary"}),
            (add_bbox_column, {"bbox_column_name": "computed"}),
        ]
        for transform, options in transforms:
            captured.clear()
            transform(str(source), str(dest), overwrite=True, **options)
            transform(str(source), "-", **options)
            assert (
                read_raw_geo(captured[0].schema.metadata).value
                == read_raw_geo(pq.read_metadata(dest).metadata).value
            ), (transform.__name__, options)

        from geoparquet_io.core.add.bbox import add_bbox_table

        caplog.set_level("INFO", logger="geoparquet_io")
        pq.write_table(add_bbox_table(table, bbox_column_name="existing_bounds"), source)
        for transform, options in transforms[:6]:
            if not options.get("add_bbox_flag"):
                continue
            captured.clear()
            caplog.clear()
            transform(str(source), str(dest), overwrite=True, **options)
            transform(str(source), "-", **options)
            assert "Use --force" not in caplog.text
            assert (
                read_raw_geo(captured[0].schema.metadata).value
                == read_raw_geo(pq.read_metadata(dest).metadata).value
            ), (transform.__name__, "existing covering")
        collision = table.append_column("bbox", pa.array(["keep"]))
        pq.write_table(collision, source)
        for transform, options in transforms[:6]:
            if not options.get("add_bbox_flag"):
                continue
            captured.clear()
            transform(str(source), str(dest), overwrite=True, **options)
            transform(str(source), "-", **options)
            assert "bbox_1" in pq.read_schema(dest).names
            assert captured[0]["bbox"].to_pylist() == ["keep"]
            assert (
                read_raw_geo(captured[0].schema.metadata).value
                == read_raw_geo(pq.read_metadata(dest).metadata).value
            )
        pq.write_table(table, source)

        bare = collision.rename_columns(["id", "footprint", "secondary", "bbox"])
        bare = bare.drop(["secondary"]).replace_schema_metadata(None)
        for transform in (hilbert_order, str_order):
            for explicit in (None, "footprint"):
                stream_table = bare if explicit else bare.rename_columns(["id", "geometry", "bbox"])
                monkeypatch.setattr(
                    "geoparquet_io.core.streaming.read_arrow_stream",
                    lambda stream_table=stream_table: stream_table.to_reader(),
                )
                captured.clear()
                kwargs = {"geometry_column": explicit, "add_bbox_flag": True}
                transform("-", str(dest), overwrite=True, **kwargs)
                transform("-", "-", **kwargs)
                expected = read_raw_geo(pq.read_metadata(dest).metadata).value
                actual = read_raw_geo(captured[0].schema.metadata).value
                assert expected is not None, (transform.__name__, explicit)
                assert actual == expected, (transform.__name__, explicit)
                geom = explicit or "geometry"
                assert actual["columns"][geom]["covering"]["bbox"]["xmin"] == ["bbox_1", "xmin"]

    def test_bare_wkb_sorts_preserve_bytes(self, tmp_path, monkeypatch):
        from contextlib import nullcontext
        from itertools import product

        import pyarrow.parquet as pq
        from geoarrow.c.lib import GeoArrowCException

        from geoparquet_io.core.hilbert_order import hilbert_order
        from geoparquet_io.core.str_order import str_order

        source = tmp_path / "bare.parquet"
        dest = tmp_path / "sorted.parquet"
        wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
        captured = []
        monkeypatch.setattr(
            "geoparquet_io.core.stream_io.write_arrow_stream",
            lambda reader, captured=captured: captured.append(reader.read_all()),
        )
        versions = (None, "1.0", "1.1", "2.0", "parquet-geo-only", "1.1-geoarrow")
        for malformed, transform, stdin, stdout, version in product(
            (False, True), (hilbert_order, str_order), (False, True), (False, True), versions
        ):
            values = [wkb, b"malformed", b""] if malformed else [wkb, wkb, wkb]
            table = pa.table({"id": [1, 2, 3], "geometry": values})
            pq.write_table(table, source)
            monkeypatch.setattr(
                "geoparquet_io.core.streaming.read_arrow_stream",
                lambda table=table: table.to_reader(),
            )
            captured.clear()
            rejects_invalid = malformed and not stdout and version == "1.1-geoarrow"
            context = (
                pytest.raises(GeoArrowCException, match="Expected uint32")
                if rejects_invalid
                else nullcontext()
            )
            with context:
                transform(
                    "-" if stdin else str(source),
                    "-" if stdout else str(dest),
                    overwrite=True,
                    geoparquet_version=version,
                )
            if rejects_invalid:
                continue
            result = captured[0] if stdout else pq.read_table(dest)
            native = not stdout and version in ("2.0", "parquet-geo-only")
            if native:
                field = pq.ParquetFile(dest).schema.column(1)
                assert "Geometry" in str(field.logical_type), (transform, stdin, version)
                assert field.name == "geometry"
                column = pq.ParquetFile(dest).metadata.row_group(0).column(1)
                assert column.is_geo_stats_set
                stats = column.geo_statistics
                assert (stats.xmin, stats.ymin, stats.xmax, stats.ymax) == (1, 2, 1, 2)
            if stdout or version != "1.1-geoarrow":
                expected = [wkb, None, None] if native and malformed else values
                assert dict(
                    zip(result["id"].to_pylist(), result["geometry"].to_pylist(), strict=True)
                ) == dict(zip([1, 2, 3], expected, strict=True))
            assert result["id"].to_pylist()[0] == 1

    def test_stdin_extract_projection_resolves_output_geometry(self, tmp_path, monkeypatch):
        from itertools import product

        import pyarrow.parquet as pq
        from pyproj import CRS

        from geoparquet_io.core.extract import extract

        wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
        entry = {"encoding": "WKB", "geometry_types": ["Point"]}
        declarations = (
            {},
            {"crs": CRS.from_epsg(3857).to_json_dict()},
            {"crs": None},
            {"edges": "spherical"},
        )
        for extra, secondary, excluded in product(
            declarations, (False, True), ("geometry", "geometry,secondary")
        ):
            if not secondary and excluded != "geometry":
                continue
            table = pa.table({"id": [1], "geometry": [wkb]})
            columns = {"geometry": entry}
            if secondary:
                table = table.append_column("secondary", table["geometry"])
                columns["secondary"] = {**entry, **extra}
            table = with_geo_block(
                table, {"version": "1.1.0", "primary_column": "geometry", "columns": columns}
            )
            source = tmp_path / "source.parquet"
            pq.write_table(table, source)
            monkeypatch.setattr(
                "geoparquet_io.core.streaming.read_arrow_stream",
                lambda table=table: table.to_reader(),
            )
            captured = []
            monkeypatch.setattr(
                "geoparquet_io.core.stream_io.write_arrow_stream",
                lambda reader, captured=captured: captured.append(reader.read_all()),
            )
            expected_geo = {}
            versions = (None, "2.0") if extra == {"crs": None} else (None,)
            for version, stdin, stdout in product(versions, (False, True), (False, True)):
                dest = tmp_path / "projected.parquet"
                extract(
                    "-" if stdin else str(source),
                    "-" if stdout else str(dest),
                    exclude_cols=excluded,
                    overwrite=True,
                    geoparquet_version=version,
                )
                result = captured[-1] if stdout else pq.read_table(dest)
                geo = read_raw_geo(result.schema.metadata).value
                if secondary and excluded == "geometry":
                    assert geo["primary_column"] == "secondary"
                    assert set(geo["columns"]) == {"secondary"}
                    for key, value in extra.items():
                        assert key in geo["columns"]["secondary"]
                        assert geo["columns"]["secondary"][key] == value
                else:
                    assert geo is None
                if not stdin:
                    expected_geo[version, stdout] = geo

                assert geo == expected_geo[version, stdout]
                if version is None:
                    assert geo == expected_geo[version, False]
                if version == "2.0" and not stdout and secondary and excluded == "geometry":
                    logical = str(pq.ParquetFile(dest).schema.column(1).logical_type)
                    assert "Geometry" in logical
                    assert "srid:0" in logical

    def test_projected_survivors_native_types_all_strategies(self, tmp_path):
        from itertools import product

        import pyarrow.parquet as pq
        from pyproj import CRS

        wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
        entry = {"encoding": "WKB", "geometry_types": ["Point"]}
        source = tmp_path / "source.parquet"
        dest = tmp_path / "projected.parquet"
        # Two declarations survive: every strategy must type both, with their own CRS.
        from geoparquet_io.core.crs_utils import crs_equal
        from geoparquet_io.core.duckdb_metadata import parse_geometry_logical_type
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
        from geoparquet_io.core.write_funnels import write_parquet_with_metadata

        table = pa.table({"id": [1], "geometry": [wkb], "secondary": [wkb], "tertiary": [wkb]})
        columns = {
            "geometry": entry,
            "secondary": {**entry, "crs": CRS.from_epsg(3857).to_json_dict()},
            "tertiary": {**entry, "crs": None},
        }
        table = with_geo_block(
            table, {"version": "1.1.0", "primary_column": "geometry", "columns": columns}
        )
        pq.write_table(table, source)
        projected = table.drop(["geometry"])
        metadata = with_geo_block(
            projected,
            {
                "version": "1.1.0",
                "primary_column": "secondary",
                "columns": {name: columns[name] for name in ("secondary", "tertiary")},
            },
        ).schema.metadata
        for strategy, version, stdin in product(
            ("duckdb-kv", "in-memory", "streaming", "disk-rewrite"),
            ("2.0", "parquet-geo-only"),
            (False, True),
        ):
            with get_duckdb_connection(load_httpfs=False) as con:
                con.register("projected", projected)
                query = (
                    "SELECT * FROM projected"
                    if stdin
                    else (
                        f"SELECT id, secondary, tertiary FROM read_parquet({sql_path(str(source))})"
                    )
                )
                write_parquet_with_metadata(
                    con,
                    query,
                    str(dest),
                    original_metadata=metadata,
                    input_file=None if stdin else str(source),
                    geometry_info={"primary": "secondary"},
                    geoparquet_version=version,
                    write_strategy=strategy,
                )
            pf = pq.ParquetFile(dest)
            for index, name in enumerate(("secondary", "tertiary"), start=1):
                logical = parse_geometry_logical_type(str(pf.schema.column(index).logical_type))
                assert logical is not None, (strategy, version, stdin, name)
                if name == "secondary":
                    assert crs_equal(logical["crs"], columns[name]["crs"])
                else:
                    assert logical["crs"] == "srid:0"
            geo = read_raw_geo(pf.schema_arrow.metadata).value
            if version == "parquet-geo-only":
                assert geo is None
            else:
                assert set(geo["columns"]) == {"secondary", "tertiary"}
                assert crs_equal(geo["columns"]["secondary"]["crs"], columns["secondary"]["crs"])
                assert geo["columns"]["tertiary"]["crs"] is None

    def test_execute_transform_dry_run(self, tmp_path, sample_geo_table, geo_metadata):
        """Test execute_transform with dry_run=True."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.stream_io import execute_transform

        # Write input file
        input_file = tmp_path / "input.parquet"
        table_with_meta = sample_geo_table.replace_schema_metadata(geo_metadata)
        pq.write_table(table_with_meta, str(input_file))

        output_file = tmp_path / "output.parquet"

        def transform_fn(source, con):
            return f"SELECT * FROM {source}"

        result = execute_transform(
            str(input_file),
            str(output_file),
            transform_fn,
            dry_run=True,
            verbose=False,
        )

        assert result is None
        # Output file should NOT be created in dry_run
        assert not output_file.exists()

    def test_str_order_streaming_adds_bbox(self, tmp_path, monkeypatch, caplog):
        """A requested bbox survives stdin dispatch (#776)."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.str_order import str_order
        from tests.test_str_order import _point_table

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

        result = pq.read_table(output)
        assert "bbox" in result.column_names
        geo = json.loads(result.schema.metadata[b"geo"])
        assert geo["columns"]["geometry"]["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]

    def test_sorts_stdout_ignores_local_dash(self, tmp_path):
        import subprocess

        import pyarrow.parquet as pq

        from tests.test_str_order import _point_table

        source = tmp_path / "input.parquet"
        pq.write_table(_point_table([(0, 1, 2)]), source)
        (tmp_path / "-").write_text("keep me", encoding="utf-8")
        for module, function, options in [
            ("sort_by_column", "sort_by_column", ", columns='id'"),
            ("sort_quadkey", "sort_by_quadkey", ""),
        ]:
            script = (
                f"from geoparquet_io.core.{module} import {function}; "
                f"{function}({str(source)!r}, '-' {options})"
            )
            result = subprocess.run(
                [sys.executable, "-c", script], cwd=tmp_path, capture_output=True, check=True
            )
            assert ipc.open_stream(result.stdout).read_all().num_rows == 1
        assert (tmp_path / "-").read_text(encoding="utf-8") == "keep me"

    def test_streaming_bbox_native_multipolygon(self, tmp_path, monkeypatch):
        import geoarrow.pyarrow as ga
        import pyarrow.parquet as pq

        from geoparquet_io.core.str_order import str_order

        table = ga.as_geoarrow(
            pa.array(["MULTIPOLYGON (((0 0, 4 0, 4 3, 0 0)))"]), type=ga.multipolygon()
        )
        table = pa.table({"geometry": table})
        stream = io.BytesIO()
        with ipc.new_stream(stream, table.schema) as writer:
            writer.write_table(table)
        stream.seek(0)
        monkeypatch.setattr(sys, "stdin", mock.Mock(buffer=stream, isatty=lambda: False))
        output = tmp_path / "native.parquet"
        str_order("-", str(output), add_bbox_flag=True, geoparquet_version="1.1-geoarrow")
        assert pq.read_table(output)["bbox"].to_pylist() == [
            {"xmin": 0.0, "ymin": 0.0, "xmax": 4.0, "ymax": 3.0}
        ]

    def test_streaming_bbox_explicit_geometry(self, tmp_path, monkeypatch):
        import pyarrow.parquet as pq

        from geoparquet_io.core.str_order import str_order
        from tests.test_str_order import _point_table

        table = (
            _point_table([(0, 1, 2)])
            .rename_columns(["id", "footprint"])
            .replace_schema_metadata(None)
        )
        stream = io.BytesIO()
        with ipc.new_stream(stream, table.schema) as writer:
            writer.write_table(table)
        stream.seek(0)
        monkeypatch.setattr(sys, "stdin", mock.Mock(buffer=stream, isatty=lambda: False))
        output = tmp_path / "explicit.parquet"
        str_order("-", str(output), geometry_column="footprint", add_bbox_flag=True)
        assert pq.read_table(output)["bbox"].to_pylist() == [
            {"xmin": 1.0, "ymin": 2.0, "xmax": 1.0, "ymax": 2.0}
        ]

    def test_sort_secondary_key_bbox_covers_primary(self, tmp_path, monkeypatch):
        import pyarrow.parquet as pq

        from geoparquet_io.core.str_order import str_order
        from tests.test_str_order import _point_table, _point_wkb

        table = _point_table([(0, 1, 2)]).append_column(
            "footprint", pa.array([_point_wkb(100, 200)])
        )
        geo = json.loads(table.schema.metadata[b"geo"])
        geo["columns"]["footprint"] = {"encoding": "WKB", "geometry_types": ["Point"]}
        table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
        source = io.BytesIO()
        with ipc.new_stream(source, table.schema) as writer:
            writer.write_table(table)
        source.seek(0)
        monkeypatch.setattr(sys, "stdin", mock.Mock(buffer=source, isatty=lambda: False))
        output = tmp_path / "secondary.parquet"
        str_order(
            "-",
            str(output),
            geometry_column="footprint",
            add_bbox_flag=True,
            geoparquet_version="1.1",
        )
        result = pq.read_table(output)
        geo = json.loads(result.schema.metadata[b"geo"])
        assert geo["primary_column"] == "geometry"
        assert geo["columns"]["geometry"]["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]
        assert "covering" not in geo["columns"]["footprint"]
        assert result["bbox"].to_pylist() == [{"xmin": 1.0, "ymin": 2.0, "xmax": 1.0, "ymax": 2.0}]

    def test_stdout_bbox_collision(self, monkeypatch):
        from geoparquet_io.core.str_order import str_order
        from tests.test_str_order import _point_table

        table = _point_table([(0, 1, 2)]).append_column("bbox", pa.array(["keep"]))
        table = table.replace_schema_metadata({**table.schema.metadata, b"source": b"survey"})
        source = io.BytesIO()
        with ipc.new_stream(source, table.schema) as writer:
            writer.write_table(table)
        source.seek(0)
        output = io.BytesIO()
        monkeypatch.setattr(sys, "stdin", mock.Mock(buffer=source, isatty=lambda: False))
        monkeypatch.setattr(sys, "stdout", mock.Mock(buffer=output, isatty=lambda: False))
        str_order("-", "-", add_bbox_flag=True)
        result = ipc.open_stream(output.getvalue()).read_all()
        assert result.schema.metadata[b"source"] == b"survey"
        assert "bbox_1" in result.column_names
        geo = json.loads(result.schema.metadata[b"geo"])
        assert geo["columns"]["geometry"]["covering"]["bbox"]["xmin"] == ["bbox_1", "xmin"]

    def test_hilbert_file_to_stdout_keeps_native_crs_and_edges(self, tmp_path, monkeypatch):
        import geoarrow.pyarrow as ga

        from geoparquet_io.core.crs_utils import crs_of
        from geoparquet_io.core.edges import geoarrow_type_edges
        from geoparquet_io.core.hilbert_order import hilbert_order
        from tests.native_geo_probes import write_native_geo_only
        from tests.test_str_order import _point_wkb

        source = tmp_path / "geography.parquet"

        native_type = ga.wkb().with_crs("EPSG:4269").with_edge_type(ga.EdgeType.SPHERICAL)
        write_native_geo_only(
            source, [(0, "point", _point_wkb(1, 2))], {"geometry": (2, native_type)}
        )
        output = io.BytesIO()
        monkeypatch.setattr(sys, "stdout", mock.Mock(buffer=output, isatty=lambda: False))
        hilbert_order(str(source), "-")
        result = ipc.open_stream(output.getvalue()).read_all()
        assert geoarrow_type_edges(result.schema.field("geometry").type) == "spherical"
        assert "4269" in str(crs_of(result))

        import pyarrow.parquet as pq

        from geoparquet_io.core.sort_by_column import sort_by_column
        from geoparquet_io.core.sort_quadkey import sort_by_quadkey

        for sort, kwargs in (
            (sort_by_column, {"columns": "id"}),
            (sort_by_quadkey, {"quadkey_column_name": "grp"}),
        ):
            output.seek(0)
            output.truncate()
            sort(str(source), "-", **kwargs)
            result = ipc.open_stream(output.getvalue()).read_all()
            assert geoarrow_type_edges(result.schema.field("geometry").type) == "spherical"
            assert "4269" in str(crs_of(result))

        table = pq.read_table(source)
        monkeypatch.setattr(
            "geoparquet_io.core.streaming.read_arrow_stream", lambda: table.to_reader()
        )
        for sort, kwargs in (
            (sort_by_column, {"columns": "id"}),
            (sort_by_quadkey, {"quadkey_column_name": "grp"}),
        ):
            outputs = []
            for stdin in (False, True):
                dest = tmp_path / f"{sort.__name__}-{stdin}.parquet"
                sort("-" if stdin else str(source), str(dest), **kwargs)
                outputs.append(pq.read_table(dest))
            assert outputs[0].equals(outputs[1])
            blocks = [json.loads(output.schema.metadata[b"geo"]) for output in outputs]
            from geoparquet_io.core.crs_utils import crs_equal

            assert crs_equal(crs_of(outputs[0]), crs_of(outputs[1]))
            for block in blocks:
                block["columns"]["geometry"].pop("crs")
            assert blocks[0] == blocks[1]
            assert geoarrow_type_edges(outputs[1].schema.field("geometry").type) == "spherical"
            assert "4269" in str(crs_of(outputs[1]))


class TestVersionExtraction:
    """The stream path's version witnesses, asked of the owner (#1367 concept 1)."""

    @pytest.mark.parametrize(
        ("declared", "expected"), [("1.1.0", "1.1"), ("1.0.0", "1.1"), ("2.0.0", "2.0")]
    )
    def test_carried_version_normalizes(self, declared, expected):
        """1.x reads as 1.1 (backwards compatible), 2.x as 2.0."""
        from geoparquet_io.core.parquet_writer import carried_geoparquet_version

        metadata = {b"geo": json.dumps({"version": declared}).encode("utf-8")}
        assert carried_geoparquet_version(metadata) == expected

    @pytest.mark.parametrize(
        "metadata",
        [
            None,
            {},
            {b"other": b"data"},
            {b"geo": json.dumps({"primary_column": "geometry"}).encode("utf-8")},
        ],
    )
    def test_carried_version_absent(self, metadata):
        from geoparquet_io.core.parquet_writer import carried_geoparquet_version

        assert carried_geoparquet_version(metadata) is None

    def test_a_geoarrow_table_with_no_geo_key_writes_2_0(self, tmp_path, monkeypatch):
        """No metadata, but geoarrow types: native data, so 2.0 (approved change 10)."""
        import geoarrow.pyarrow as ga

        from geoparquet_io.core.parquet_writer import resolve_output_geoparquet_version

        wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
        table = pa.table({"id": [1, 2], "geometry": ga.as_wkb([wkb, wkb])})

        assert resolve_output_geoparquet_version(None, table=table) == "2.0"
        assert resolve_output_geoparquet_version(None, original_metadata={}, table=table) == "2.0"

        import pyarrow.parquet as pq

        from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
        from geoparquet_io.core.stream_io import write_output

        captured = []
        monkeypatch.setattr(
            "geoparquet_io.core.stream_io.write_arrow_stream",
            lambda reader, captured=captured: captured.append(reader.read_all()),
        )
        dest = tmp_path / "native.parquet"
        con = get_duckdb_connection(load_spatial=True)
        try:
            source = tmp_path / "source.parquet"
            pq.write_table(table, source)
            for output in (str(dest), "-"):
                write_output(
                    con,
                    f"SELECT * FROM read_parquet({sql_path(str(source))})",
                    output,
                    original_metadata={},
                    input_file=str(source),
                )
        finally:
            con.close()
        file_table = pq.read_table(dest)
        assert json.loads(file_table.schema.metadata[b"geo"])["version"] == "2.0.0"
        assert json.loads(captured[0].schema.metadata[b"geo"])["version"] == "2.0.0"
        assert file_table.to_pydict() == captured[0].to_pydict()

    def test_the_carried_version_takes_precedence_over_geoarrow_types(self):
        import geoarrow.pyarrow as ga

        from geoparquet_io.core.parquet_writer import resolve_output_geoparquet_version

        wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
        metadata = {b"geo": json.dumps({"version": "1.1.0"}).encode("utf-8")}
        table = pa.table({"geometry": ga.as_wkb([wkb])}).replace_schema_metadata(metadata)

        assert resolve_output_geoparquet_version(None, table=table) == "1.1"

    def test_no_witness_is_the_default(self):
        from geoparquet_io.core.parquet_writer import resolve_output_geoparquet_version

        table = pa.table({"id": [1, 2], "name": ["a", "b"]})
        assert resolve_output_geoparquet_version(None, table=table) == "1.1"
        assert resolve_output_geoparquet_version(None, original_metadata={}) == "1.1"
        assert resolve_output_geoparquet_version(None) == "1.1"


def _wkb_polygon(npts: int) -> bytes:
    """Build a little-endian WKB Polygon with one ring of ``npts`` vertices.

    Each vertex is 16 bytes, so the WKB size scales ~linearly with ``npts`` —
    used to manufacture large geometry payloads in tests (issue #511).
    """
    import struct

    hdr = struct.pack("<BII", 1, 3, 1) + struct.pack("<I", npts)
    coords = bytearray()
    for i in range(npts - 1):
        coords += struct.pack("<dd", (i % 1000) * 1e-4, (i % 997) * 1e-4)
    coords += struct.pack("<dd", 0.0, 0.0)  # close the ring
    return hdr + bytes(coords)


@pytest.mark.parametrize("carrier", ["text", "large_binary"])
def test_stream_geometry_conversion_and_byte_budget(carrier, monkeypatch):
    import geoarrow.pyarrow as ga

    from geoparquet_io.core import stream_io, streaming
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    output = []
    monkeypatch.setattr(stream_io, "write_arrow_stream", lambda reader: output.extend(reader))
    monkeypatch.setattr(streaming, "_MAX_WKB_CHUNK_BYTES", 42)
    with get_duckdb_connection(load_spatial=True) as con:
        if carrier == "text":
            query = "SELECT 'POINT (1 2)' AS geometry"
        else:
            con.register(
                "input_wkb",
                pa.table(
                    {
                        "id": range(6),
                        "geometry": pa.array(
                            [ga.as_wkb(pa.array(["POINT (1 2)"])).storage[0].as_py()] * 6,
                            type=pa.large_binary(),
                        ),
                    }
                ),
            )
            query = "SELECT * FROM input_wkb"
        from geoparquet_io.core.geojson_stream import _build_feature_query, _feature_rows
        from geoparquet_io.core.geometry_detection import create_geometry_view

        # GeoJSON serializes SQL GEOMETRY to JSON before Arrow sees the batch.
        feature_source = (
            f"(SELECT ST_GeomFromText(geometry) AS geometry FROM ({query}))"
            if carrier == "text"
            else f"({query})"
        )
        source = create_geometry_view(con, feature_source, "geometry", "feature_source")
        features = list(_feature_rows(con, _build_feature_query(source, "geometry", [])))
        assert [json.loads(value)["geometry"]["coordinates"] for value in features] == (
            [[1.0, 2.0]] * (1 if carrier == "text" else 6)
        )
        stream_io.write_output(con, query, "-")
    assert output
    for batch in output:
        geometry = batch.column("geometry")
        assert geometry.type.extension_name == "geoarrow.wkb"
        assert ga.as_wkt(geometry).storage.to_pylist() == ["POINT (1 2)"] * len(batch)
        assert _wkb_chunk_data_nbytes(geometry.storage) <= 42
    if carrier == "large_binary":
        assert [row for batch in output for row in batch.column("id").to_pylist()] == list(range(6))
        assert len(output) >= 3


class TestWkbByteRebatching:
    """Tests for the 32-bit WKB offset-overflow guard (issue #511)."""

    def test_data_nbytes_sums_value_lengths(self):
        """_wkb_chunk_data_nbytes counts value bytes, not offset/validity bytes."""
        blobs = [b"abc", b"de", b""]
        arr = pa.array(blobs, type=pa.large_binary())
        assert _wkb_chunk_data_nbytes(arr) == 5

        arr = pa.array([], type=pa.large_binary())
        assert _wkb_chunk_data_nbytes(arr) == 0

    def test_split_respects_byte_budget(self):
        """Each emitted slice stays within the byte budget and rows are preserved."""
        blobs = [b"x" * 1000 for _ in range(10)]
        arr = pa.array(blobs, type=pa.large_binary())

        slices = _split_array_under_byte_limit(arr, max_bytes=2500)

        # 2500-byte budget fits two 1000-byte values per slice.
        assert all(_wkb_chunk_data_nbytes(s) <= 2500 for s in slices)
        assert sum(len(s) for s in slices) == 10
        # Reassembling yields the original values in order.
        rejoined = pa.concat_arrays(slices)
        assert rejoined.to_pylist() == arr.to_pylist()

        # A single oversized value still occupies its own nonempty slice.
        arr = pa.array([b"y" * 100, b"z" * 5000, b"y" * 100], type=pa.large_binary())

        slices = _split_array_under_byte_limit(arr, max_bytes=1000)

        assert all(len(s) > 0 for s in slices)
        assert sum(len(s) for s in slices) == 3
        # The oversized value lands in a slice on its own.
        assert any(len(s) == 1 and _wkb_chunk_data_nbytes(s) == 5000 for s in slices)

    def test_rebatch_noop_when_under_limit(self):
        """Arrays already under the limit are returned unchanged (identity)."""
        arr = pa.chunked_array([pa.array([b"abc", b"def"], type=pa.large_binary())])
        result = _rebatch_wkb_under_byte_limit(arr, max_bytes=1_000_000)
        assert result is arr

        # Oversized chunks split without changing values or row order.
        blobs = [b"q" * 1000 for _ in range(10)]
        arr = pa.chunked_array([pa.array(blobs, type=pa.large_binary())])

        result = _rebatch_wkb_under_byte_limit(arr, max_bytes=2500)

        assert isinstance(result, pa.ChunkedArray)
        assert result.num_chunks > 1
        assert all(_wkb_chunk_data_nbytes(c) <= 2500 for c in result.chunks)
        assert result.combine_chunks().to_pylist() == arr.combine_chunks().to_pylist()


class TestApplyGeoArrowExtensionType:
    """Tests for apply_geoarrow_extension_type, including the #511 overflow fix."""

    def _wkb_table(self, n: int = 6, npts: int = 64):
        """Build a table whose geometry column is large_binary WKB polygons."""
        blob = _wkb_polygon(npts)
        return pa.table(
            {
                "id": list(range(n)),
                "geometry": pa.array([blob] * n, type=pa.large_binary()),
            }
        )

    def test_converts_to_geoarrow_wkb(self):
        """A normal WKB column becomes a geoarrow.wkb extension type."""
        table = self._wkb_table()
        result = apply_geoarrow_extension_type(table, "geometry")

        geom_type = result.column("geometry").type
        assert getattr(geom_type, "extension_name", "") == "geoarrow.wkb"
        assert result.num_rows == table.num_rows

    def test_missing_column_returns_table_unchanged(self):
        """Absent geometry column is a no-op."""
        table = pa.table({"id": [1, 2]})
        assert apply_geoarrow_extension_type(table, "geometry") is table

    def _zero_chunk_table(self, geom_type=pa.large_binary()):
        """A zero-row table whose geometry column holds zero chunks.

        This is exactly what ``con.execute(...).arrow().read_all()`` returns for
        a DuckDB result with no rows -- not one empty chunk, but none at all.
        """
        table = pa.table(
            {
                "id": pa.chunked_array([], type=pa.int64()),
                "geometry": pa.chunked_array([], type=geom_type),
            }
        )
        assert table.column("geometry").num_chunks == 0
        return table

    def test_zero_chunk_column_converts_without_aborting(self):
        """A zero-row (zero-chunk) geometry column yields a typed empty column.

        Regression guard for #804: geoarrow rebuilt a ChunkedArray from an empty
        chunk list with no type, which aborted the interpreter (SIGABRT) instead
        of raising. Streaming an empty spatial filter must be ordinary.
        """
        table = self._zero_chunk_table()

        for preserve_wkb in (False, True):
            result = apply_geoarrow_extension_type(table, "geometry", preserve_wkb=preserve_wkb)

            geom = result.column("geometry")
            assert getattr(geom.type, "extension_name", "") == "geoarrow.wkb"
            assert result.num_rows == 0
            assert geom.num_chunks == 1
            assert result.column_names == ["id", "geometry"]

    def test_zero_chunk_column_keeps_crs(self):
        """The CRS branch also has to survive the zero-chunk case."""
        table = self._zero_chunk_table()

        result = apply_geoarrow_extension_type(table, "geometry", crs="EPSG:4326")

        geom_type = result.column("geometry").type
        assert getattr(geom_type, "extension_name", "") == "geoarrow.wkb"
        assert geom_type.crs is not None
        assert result.num_rows == 0

    def test_zero_chunk_binary_column_converts(self):
        """32-bit ``binary`` WKB (a re-read stream) hits the same path."""
        table = self._zero_chunk_table(geom_type=pa.binary())

        result = apply_geoarrow_extension_type(table, "geometry")

        assert getattr(result.column("geometry").type, "extension_name", "") == "geoarrow.wkb"
        assert result.num_rows == 0

    def test_rebatches_when_chunk_exceeds_limit(self, monkeypatch):
        """A chunk above the byte budget is split before conversion and survives.

        Drives the #511 overflow path with a tiny budget so it runs without the
        ~2.2 GB of RAM the real-size reproduction requires.
        """
        import geoparquet_io.core.streaming as streaming

        table = self._wkb_table(n=8, npts=64)
        blob_size = len(_wkb_polygon(64))
        # Budget that holds ~2 polygons per chunk, forcing multiple sub-chunks.
        monkeypatch.setattr(streaming, "_MAX_WKB_CHUNK_BYTES", blob_size * 2)

        result = apply_geoarrow_extension_type(table, "geometry")

        geom = result.column("geometry")
        assert getattr(geom.type, "extension_name", "") == "geoarrow.wkb"
        # The single oversized chunk was split into several before conversion.
        assert geom.num_chunks > 1
        assert result.num_rows == table.num_rows
        # WKB values survive the split + re-encode unchanged.
        storage = pa.chunked_array([c.storage for c in geom.chunks])
        assert storage.to_pylist() == table.column("geometry").to_pylist()

    def test_preserves_crs(self):
        """A provided CRS is attached to the resulting geoarrow type."""
        table = self._wkb_table()
        result = apply_geoarrow_extension_type(table, "geometry", crs="EPSG:4326")

        geom_type = result.column("geometry").type
        assert getattr(geom_type, "extension_name", "") == "geoarrow.wkb"
        assert geom_type.crs is not None

    def test_conversion_failure_raises_clear_error(self, monkeypatch):
        """A geoarrow failure surfaces as StreamingError, not a silent passthrough.

        Regression guard for the masked "No data received on stdin" bug: a
        forced conversion failure must raise with the true cause rather than
        returning un-converted geometry (issue #511).
        """
        import geoarrow.pyarrow as ga

        def _boom(_arr):
            # Mimic geoarrow's GeoArrowCException (a RuntimeError subclass).
            raise RuntimeError("push_batch() failed (75)")

        monkeypatch.setattr(ga, "as_wkb", _boom)

        table = self._wkb_table()
        with pytest.raises(StreamingError, match="GeoArrow"):
            apply_geoarrow_extension_type(table, "geometry")

    @pytest.mark.slow
    def test_real_overflow_succeeds(self):
        """End-to-end: a >2 GB large_binary WKB batch converts without overflow.

        Reproduces issue #511 at real scale: ga.as_wkb alone raises
        GeoArrowCException on this input, but apply_geoarrow_extension_type
        re-chunks it first and succeeds. Needs ~2.2 GB RAM, hence ``slow``.
        """
        blob = _wkb_polygon(65536)  # ~1 MB each
        int32_max = 2_147_483_647
        n = int32_max // len(blob) + 50  # push the batch just over 2 GB
        table = pa.table({"geometry": pa.array([blob] * n, type=pa.large_binary())})

        result = apply_geoarrow_extension_type(table, "geometry")

        geom = result.column("geometry")
        assert getattr(geom.type, "extension_name", "") == "geoarrow.wkb"
        assert result.num_rows == n


class TestReadStdinToTempFile:
    """The stdin -> temp Parquet bridge used by the partition commands (#722).

    DuckDB refuses to read a Parquet file whose ``geo`` metadata declares a
    geometry column without ``geometry_types`` (required by GeoParquet 1.1).
    gpio's own producers now carry it, but a stream can come from anywhere, so
    the bridge fills the gap in the temp file it owns rather than writing a file
    nothing downstream can open.
    """

    def _point_table(self, geo: dict) -> pa.Table:
        wkb_point = bytes.fromhex("0101000000000000000000f03f000000000000f03f")  # POINT (1 1)
        table = pa.table({"id": [1], "geometry": [wkb_point]})
        return table.replace_schema_metadata({b"geo": json.dumps(geo).encode("utf-8")})

    def _run(self, monkeypatch, table):
        from geoparquet_io.core import streaming as streaming_mod

        monkeypatch.setattr(streaming_mod, "read_arrow_stream", lambda: table.to_reader())
        from geoparquet_io.core.write_funnels import read_stdin_to_temp_file

        return read_stdin_to_temp_file()

    def test_missing_geometry_types_is_filled_in(self, monkeypatch):
        import os

        import duckdb
        import pyarrow.parquet as pq

        table = self._point_table(
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {"geometry": {"encoding": "WKB"}},
            }
        )
        path = self._run(monkeypatch, table)
        try:
            geo = json.loads(pq.read_schema(path).metadata[b"geo"].decode("utf-8"))
            assert geo["columns"]["geometry"]["geometry_types"] == ["Point"]
            # The whole point: DuckDB can now open the file.
            con = duckdb.connect()
            try:
                assert fetch_one(con.execute(f"SELECT COUNT(*) FROM '{path}'"))[0] == 1
            finally:
                con.close()
        finally:
            os.remove(path)

    def test_declared_geometry_types_are_preserved(self, monkeypatch):
        import os

        import pyarrow.parquet as pq

        table = self._point_table(
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {"encoding": "WKB", "geometry_types": ["Point", "MultiPoint"]}
                },
            }
        )
        path = self._run(monkeypatch, table)
        try:
            geo = json.loads(pq.read_schema(path).metadata[b"geo"].decode("utf-8"))
            assert geo["columns"]["geometry"]["geometry_types"] == ["Point", "MultiPoint"]
        finally:
            os.remove(path)

    def test_table_without_geo_metadata_is_unchanged(self, monkeypatch):
        import os

        import pyarrow.parquet as pq

        table = pa.table({"id": [1, 2]})
        path = self._run(monkeypatch, table)
        try:
            assert pq.read_table(path).num_rows == 2
        finally:
            os.remove(path)


OGC_CRS84_PROJJSON = {
    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
    "type": "GeographicCRS",
    "name": "WGS 84 (CRS84)",
    "datum": {
        "type": "GeodeticReferenceFrame",
        "name": "World Geodetic System 1984",
        "ellipsoid": {
            "name": "WGS 84",
            "semi_major_axis": 6378137,
            "inverse_flattening": 298.257223563,
        },
    },
    "coordinate_system": {
        "subtype": "ellipsoidal",
        "axis": [
            {
                "name": "Geodetic longitude",
                "abbreviation": "Lon",
                "direction": "east",
                "unit": "degree",
            },
            {
                "name": "Geodetic latitude",
                "abbreviation": "Lat",
                "direction": "north",
                "unit": "degree",
            },
        ],
    },
    "id": {"authority": "OGC", "code": "CRS84"},
}


class _OpaqueCrs:
    """A CRS-ish object that is neither PROJJSON nor a geoarrow ``Crs``."""

    def __repr__(self) -> str:
        return "Opaque(EPSG:9999)"


class _ExplodingCrs:
    """A geoarrow-shaped ``Crs`` whose PROJJSON accessor fails.

    Real ``StringCrs.to_json_dict()`` calls pyproj, which raises for an
    identifier PROJ does not know.
    """

    def to_json_dict(self):
        raise ValueError("crs not found: EPSG:999999")


# PyArrow may rebuild an extension type from its serialized form while a table
# is assembled, so the payload cannot live on the instance. Keyed by the token
# the type serializes, it survives any number of round-trips.
_CRS_PAYLOADS: dict[bytes, object] = {}


class _CrsCarrierType(pa.ExtensionType):
    """An Arrow extension type exposing an arbitrary value as ``.crs``.

    Lets a test hand ``crs_of`` exactly one CRS carrier shape
    with no dependence on import order or on what geoarrow chooses to wrap the
    value in -- the process state that made issue #816 intermittent.
    """

    def __init__(self, token: bytes):
        self._token = token
        super().__init__(pa.binary(), "gpio.test.crs_carrier")

    def __arrow_ext_serialize__(self) -> bytes:
        return self._token

    @classmethod
    def __arrow_ext_deserialize__(cls, _storage_type, serialized):
        return cls(serialized)

    @property
    def crs(self):
        return _CRS_PAYLOADS[self._token]


def _table_with_crs_carrier(crs) -> pa.Table:
    """A one-row table whose geometry field's type reports ``crs``."""
    token = f"crs-{len(_CRS_PAYLOADS)}".encode()
    _CRS_PAYLOADS[token] = crs
    storage = pa.array([_wkb_polygon(4)], type=pa.binary())
    geom = pa.ExtensionArray.from_storage(_CrsCarrierType(token), storage)
    return pa.table({"id": [1], "geometry": geom})


def _geoarrow_typed_table(crs, nrows: int = 1) -> pa.Table:
    """A table whose geometry column carries a *resolved* geoarrow type.

    Importing ``geoarrow.pyarrow`` registers the extension types, which is the
    process state issue #816 depends on.
    """
    import geoarrow.pyarrow  # noqa: F401  -- registers the extension types

    table = pa.table(
        {
            "id": list(range(nrows)),
            "geometry": pa.array([_wkb_polygon(4)] * nrows, type=pa.large_binary()),
        }
    )
    return apply_geoarrow_extension_type(table, "geometry", crs=crs)


class TestExtractCrsFromGeoArrowType:
    """`crs_of` must read a geoarrow CRS object, not stringify it.

    Regression guard for issue #816. Once ``geoarrow.pyarrow`` is imported
    anywhere in the process, PyArrow resolves ``geoarrow.wkb`` fields into real
    extension types whose ``.crs`` is a ``geoarrow.types.crs.Crs`` object. The
    old ``if hasattr(crs, "__str__"): return str(crs)`` catch-all matched every
    Python object, so the CRS came back as the repr ``"ProjJsonCrs(OGC:CRS84)"``
    and the writer split it into authority ``PROJJSONCRS(OGC`` / code ``CRS84)``.
    """

    def test_projjson_crs_object_returns_projjson_dict(self):
        """A ProjJsonCrs object comes back as PROJJSON, not as its repr."""
        table = _geoarrow_typed_table(OGC_CRS84_PROJJSON)

        crs = crs_of(table, geometry_column="geometry")

        assert not isinstance(crs, str), f"stringified the CRS object: {crs!r}"
        assert isinstance(crs, dict)
        assert crs["id"] == {"authority": "OGC", "code": "CRS84"}

    def test_string_crs_object_resolves_to_projjson(self):
        """A StringCrs object ("EPSG:3857") resolves through its PROJJSON accessor."""
        table = _geoarrow_typed_table("EPSG:3857")

        crs = crs_of(table, geometry_column="geometry")

        assert crs is not None
        parsed = crs if isinstance(crs, dict) else json.loads(crs)
        assert parsed["id"] == {"authority": "EPSG", "code": 3857}

    def test_result_is_parseable_by_pyproj(self):
        """Whatever comes back must be a CRS pyproj can round-trip."""
        import pyproj

        table = _geoarrow_typed_table(OGC_CRS84_PROJJSON)

        crs = crs_of(table, geometry_column="geometry")

        assert pyproj.CRS(crs).to_authority() == ("OGC", "CRS84")

    def test_unregistered_extension_shape_falls_back_to_geo_metadata(self):
        """The other shape -- ``ARROW:extension:name`` in field metadata with no
        resolved extension type -- must keep reading the CRS out of ``geo``.

        This is what PyArrow produces for a geoarrow field when the extension
        type is not registered, and it is the path most sessions take.
        """
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": [],
                    "crs": OGC_CRS84_PROJJSON,
                }
            },
        }
        field = pa.field(
            "geometry",
            pa.binary(),
            metadata={
                b"ARROW:extension:name": b"geoarrow.wkb",
                b"ARROW:extension:metadata": b'{"crs": {"id": {"authority": "OGC"}}}',
            },
        )
        schema = pa.schema(
            [pa.field("id", pa.int64()), field],
            metadata={b"geo": json.dumps(geo).encode("utf-8")},
        )
        table = pa.table(
            [pa.array([1]), pa.array([_wkb_polygon(4)], type=pa.binary())], schema=schema
        )

        crs = crs_of(table, geometry_column="geometry")

        assert crs == OGC_CRS84_PROJJSON

    def test_unknown_crs_object_is_rejected_not_stringified(self, caplog):
        """An unrecognised CRS object yields None, never its repr.

        The old catch-all would have returned ``"Opaque(EPSG:9999)"``, which the
        writer then split on ``:`` into a fabricated authority/code pair.
        """
        table = _table_with_crs_carrier(_OpaqueCrs())

        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            crs = crs_of(table, geometry_column="geometry")

        assert crs is None
        assert "_OpaqueCrs" in caplog.text
        assert "Opaque(EPSG:9999)" not in caplog.text


class TestCrsCarrierShapes:
    """Every shape ``geom_type.crs`` can take, driven directly (#816).

    These call ``crs_of`` with one carrier shape each, with no
    dependence on whether ``geoarrow.pyarrow`` happened to be imported first --
    so each branch is exercised deterministically on every run.
    """

    def test_real_projjson_crs_object(self):
        """A ``geoarrow.types.crs.ProjJsonCrs`` resolves to its PROJJSON."""
        from geoarrow.types.crs import ProjJsonCrs

        table = _table_with_crs_carrier(ProjJsonCrs(OGC_CRS84_PROJJSON))

        crs = crs_of(table, geometry_column="geometry")

        assert isinstance(crs, dict)
        assert crs["id"] == {"authority": "OGC", "code": "CRS84"}

    def test_real_string_crs_object(self):
        """A ``geoarrow.types.crs.StringCrs`` resolves through pyproj to PROJJSON."""
        from geoarrow.types.crs import StringCrs

        table = _table_with_crs_carrier(StringCrs("EPSG:3857"))

        crs = crs_of(table, geometry_column="geometry")

        assert isinstance(crs, dict)
        assert crs["id"] == {"authority": "EPSG", "code": 3857}

    def test_pyproj_crs_object(self):
        """``pyproj.CRS`` satisfies the same protocol and must work too."""
        import pyproj

        table = _table_with_crs_carrier(pyproj.CRS("EPSG:4326"))

        crs = crs_of(table, geometry_column="geometry")

        assert isinstance(crs, dict)
        assert crs["id"] == {"authority": "EPSG", "code": 4326}

    def test_plain_projjson_dict_passes_through(self):
        """A type that already holds PROJJSON is returned unchanged."""
        table = _table_with_crs_carrier(OGC_CRS84_PROJJSON)

        assert crs_of(table, geometry_column="geometry") == OGC_CRS84_PROJJSON

    def test_plain_string_identifier_passes_through(self):
        """A bare identifier string stays a string for the caller to normalise."""
        table = _table_with_crs_carrier("EPSG:3857")

        assert crs_of(table, geometry_column="geometry") == "EPSG:3857"

    def test_utf8_bytes_are_decoded(self):
        """Bytes are decoded to text rather than stringified as ``b'...'``."""
        table = _table_with_crs_carrier(b"EPSG:3857")

        crs = crs_of(table, geometry_column="geometry")

        assert crs == "EPSG:3857"
        assert not str(crs).startswith("b'")

    def test_undecodable_bytes_are_rejected(self):
        """Bytes that are not UTF-8 are not a CRS; return None, never a repr."""
        table = _table_with_crs_carrier(b"\xff\xfe not utf-8")

        assert crs_of(table, geometry_column="geometry") is None

    def test_failing_projjson_accessor_warns_and_returns_none(self, caplog):
        """An accessor that raises is reported, and yields None -- not a repr."""
        table = _table_with_crs_carrier(_ExplodingCrs())

        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            crs = crs_of(table, geometry_column="geometry")

        assert crs is None
        assert "_ExplodingCrs" in caplog.text
        assert "crs not found: EPSG:999999" in caplog.text

    def test_carrier_failure_falls_back_to_geo_metadata(self, caplog):
        """A rejected CRS object must not shadow the truth in the `geo` metadata."""
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {"encoding": "WKB", "geometry_types": [], "crs": OGC_CRS84_PROJJSON}
            },
        }
        table = _table_with_crs_carrier(_OpaqueCrs())
        table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode("utf-8")})

        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            crs = crs_of(table, geometry_column="geometry")

        assert crs == OGC_CRS84_PROJJSON


class TestGeoArrowCrsSurvivesWrite:
    """End-to-end: a geoarrow-typed table writes a CRS the validator accepts (#816)."""

    def _write_and_validate(self, tmp_path, crs):
        import pyarrow.parquet as pq

        import geoparquet_io as gpio
        from geoparquet_io.core.validate import validate_geoparquet

        table = _geoarrow_typed_table(crs, nrows=2)
        out = tmp_path / "out.parquet"
        gpio.Table(table, geometry_column="geometry").write(str(out))

        geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"].decode("utf-8"))
        written = geo["columns"][geo["primary_column"]].get("crs")
        failed = sorted(
            {c.name for c in validate_geoparquet(str(out)).checks if c.status.value == "failed"}
        )
        return written, failed

    def test_crs84_geoarrow_table_passes_crs_valid_geometry(self, tmp_path):
        written, failed = self._write_and_validate(tmp_path, OGC_CRS84_PROJJSON)

        assert "crs_valid_geometry" not in failed
        assert written is None or written["id"] == {"authority": "OGC", "code": "CRS84"}

    def test_projected_geoarrow_table_keeps_its_authority_code(self, tmp_path):
        written, failed = self._write_and_validate(tmp_path, "EPSG:3857")

        assert "crs_valid_geometry" not in failed
        assert written is not None
        assert written["id"] == {"authority": "EPSG", "code": 3857}
