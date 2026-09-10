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

from geoparquet_io.core.streaming import (
    STREAM_MARKER,
    StreamingError,
    _rebatch_wkb_under_byte_limit,
    _split_array_under_byte_limit,
    _wkb_chunk_data_nbytes,
    apply_geo_metadata,
    apply_geoarrow_extension_type,
    apply_metadata_to_table,
    extract_crs_from_table,
    extract_geo_metadata,
    find_geometry_column_from_metadata,
    find_geometry_column_from_table,
    is_stdin,
    is_stdout,
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

    def test_is_stdout_with_marker(self):
        assert is_stdout("-") is True

    def test_is_stdout_with_file(self):
        assert is_stdout("/path/to/file.parquet") is False

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

        assert result.equals(sample_table)

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

    def test_read_arrow_stream_raises_on_invalid_data(self, monkeypatch):
        """Test that invalid data raises StreamingError."""
        invalid_buffer = io.BytesIO(b"not arrow ipc data")

        mock_stdin = mock.MagicMock()
        mock_stdin.isatty.return_value = False
        mock_stdin.buffer = invalid_buffer

        monkeypatch.setattr(sys, "stdin", mock_stdin)
        with pytest.raises(StreamingError, match="Invalid Arrow IPC stream"):
            read_arrow_stream()


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

    def test_extract_geo_metadata(self, table_with_geo_metadata, geo_metadata):
        """Test extracting geo metadata from table."""
        result = extract_geo_metadata(table_with_geo_metadata)
        assert result == geo_metadata

    def test_extract_geo_metadata_returns_none_when_missing(self):
        """Test that missing metadata returns None."""
        table = pa.table({"id": [1, 2]})
        assert extract_geo_metadata(table) is None

    def test_apply_geo_metadata(self, geo_metadata):
        """Test applying geo metadata to table."""
        table = pa.table({"id": [1, 2]})
        result = apply_geo_metadata(table, geo_metadata)

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
        table = apply_geo_metadata(table, geo_metadata)

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
        restored = extract_geo_metadata(result)
        assert restored == geo_metadata


class TestGeometryColumnDetection:
    """Tests for geometry column detection."""

    def test_find_geometry_column_from_metadata(self):
        """Test finding geometry column from metadata."""
        metadata = {b"geo": json.dumps({"primary_column": "geom"}).encode("utf-8")}
        assert find_geometry_column_from_metadata(metadata) == "geom"

    def test_find_geometry_column_from_metadata_names_nothing_it_cannot_read(self):
        """A block that names no usable primary column names none (#968).

        This used to answer with the literal ``"geometry"``, which is a guess:
        on a file whose column is ``geom`` it names a column that does not
        exist, and #945's review traced silently-wrong coordinates to exactly
        that default. Every caller asks the file's schema when this reader says
        nothing -- see ``test_find_geometry_column_from_table_common_names``.
        """
        assert find_geometry_column_from_metadata({b"geo": json.dumps({}).encode("utf-8")}) is None
        malformed = {b"geo": json.dumps({"primary_column": 123}).encode("utf-8")}
        assert find_geometry_column_from_metadata(malformed) is None

    def test_find_geometry_column_from_metadata_missing(self):
        """Test None when no geo metadata."""
        assert find_geometry_column_from_metadata(None) is None
        assert find_geometry_column_from_metadata({}) is None

    def test_find_geometry_column_from_table_with_metadata(self):
        """Test finding geometry from table with metadata."""
        geo_meta = {"primary_column": "geom"}
        table = pa.table({"id": [1], "geom": [b"wkb"]})
        metadata = {b"geo": json.dumps(geo_meta).encode("utf-8")}
        table = table.replace_schema_metadata(metadata)

        assert find_geometry_column_from_table(table) == "geom"

    def test_find_geometry_column_from_table_common_names(self):
        """Test finding geometry from common column names."""
        # Test 'geometry'
        table = pa.table({"id": [1], "geometry": [b"wkb"]})
        assert find_geometry_column_from_table(table) == "geometry"

        # Test 'geom'
        table = pa.table({"id": [1], "geom": [b"wkb"]})
        assert find_geometry_column_from_table(table) == "geom"

        # Test 'the_geom'
        table = pa.table({"id": [1], "the_geom": [b"wkb"]})
        assert find_geometry_column_from_table(table) == "the_geom"

    def test_find_geometry_column_from_table_no_match(self):
        """Test None when no geometry column found."""
        table = pa.table({"id": [1], "name": ["test"]})
        assert find_geometry_column_from_table(table) is None


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
        from geoparquet_io.core.stream_io import _wrap_query_with_wkb_conversion

        query = "SELECT * FROM data"
        result = _wrap_query_with_wkb_conversion(query, "geometry")
        # The geometry column is an identifier and is quoted at every raw SQL
        # interpolation (see CLAUDE.md's DuckDB patterns; enforced since #662).
        assert 'ST_AsWKB("geometry")' in result
        assert "__stream_source" in result

    def test_wrap_query_with_wkb_conversion_no_geom(self):
        """Test that None geometry skips wrapping."""
        from geoparquet_io.core.stream_io import _wrap_query_with_wkb_conversion

        query = "SELECT * FROM data"
        result = _wrap_query_with_wkb_conversion(query, None)
        assert result == query

    def test_open_input_with_file(self, tmp_path, sample_geo_table, geo_metadata):
        """Test open_input with a file path."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.stream_io import open_input

        # Write test file
        test_file = tmp_path / "test.parquet"
        table_with_meta = sample_geo_table.replace_schema_metadata(geo_metadata)
        pq.write_table(table_with_meta, str(test_file))

        with open_input(str(test_file)) as (source, metadata, is_stream, con):
            assert is_stream is False
            assert "read_parquet" in source
            assert metadata is not None
            assert con is not None

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

        with open_input("-") as (source, metadata, is_stream, con):
            assert is_stream is True
            # Source should be a view name for stream input
            assert "input_stream" in source
            assert metadata is not None
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

    def test_execute_transform_file_to_file(self, tmp_path, sample_geo_table, geo_metadata):
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


class TestVersionExtraction:
    """Tests for GeoParquet version extraction and auto-detection."""

    def test_extract_version_from_metadata_v11(self):
        """Test extracting version 1.1 from metadata."""
        from geoparquet_io.core.streaming import extract_version_from_metadata

        metadata = {b"geo": json.dumps({"version": "1.1.0"}).encode("utf-8")}
        assert extract_version_from_metadata(metadata) == "1.1"

    def test_extract_version_from_metadata_v10_upgrades_to_v11(self):
        """Test that version 1.0 is upgraded to 1.1."""
        from geoparquet_io.core.streaming import extract_version_from_metadata

        metadata = {b"geo": json.dumps({"version": "1.0.0"}).encode("utf-8")}
        # 1.0 is upgraded to 1.1 since 1.1 is backwards compatible
        assert extract_version_from_metadata(metadata) == "1.1"

    def test_extract_version_from_metadata_v20(self):
        """Test extracting version 2.0 from metadata."""
        from geoparquet_io.core.streaming import extract_version_from_metadata

        metadata = {b"geo": json.dumps({"version": "2.0.0"}).encode("utf-8")}
        assert extract_version_from_metadata(metadata) == "2.0"

    def test_extract_version_from_metadata_none(self):
        """Test None when no metadata."""
        from geoparquet_io.core.streaming import extract_version_from_metadata

        assert extract_version_from_metadata(None) is None
        assert extract_version_from_metadata({}) is None

    def test_extract_version_from_metadata_no_geo(self):
        """Test None when no geo key in metadata."""
        from geoparquet_io.core.streaming import extract_version_from_metadata

        metadata = {b"other": b"data"}
        assert extract_version_from_metadata(metadata) is None

    def test_extract_version_from_metadata_no_version(self):
        """Test None when geo metadata lacks version field."""
        from geoparquet_io.core.streaming import extract_version_from_metadata

        metadata = {b"geo": json.dumps({"primary_column": "geometry"}).encode("utf-8")}
        assert extract_version_from_metadata(metadata) is None

    def test_has_geoarrow_extension_in_table_true(self):
        """Test detection of geoarrow extension type in table."""
        import geoarrow.pyarrow as ga

        from geoparquet_io.core.streaming import has_geoarrow_extension_in_table

        # Create WKB geometry with geoarrow type
        wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
        geom_arr = ga.as_wkb([wkb, wkb])
        table = pa.table({"id": [1, 2], "geometry": geom_arr})

        assert has_geoarrow_extension_in_table(table) is True

    def test_has_geoarrow_extension_in_table_false(self):
        """Test no geoarrow extension type in regular table."""
        from geoparquet_io.core.streaming import has_geoarrow_extension_in_table

        table = pa.table({"id": [1, 2], "geometry": [b"wkb1", b"wkb2"]})
        assert has_geoarrow_extension_in_table(table) is False

    def test_detect_version_for_output_from_metadata(self):
        """Test version detection from metadata."""
        from geoparquet_io.core.streaming import detect_version_for_output

        metadata = {b"geo": json.dumps({"version": "2.0.0"}).encode("utf-8")}
        assert detect_version_for_output(metadata) == "2.0"

    def test_detect_version_for_output_from_geoarrow_table(self):
        """Test version detection from geoarrow extension types."""
        import geoarrow.pyarrow as ga

        from geoparquet_io.core.streaming import detect_version_for_output

        # Create table with geoarrow extension type but no geo metadata
        wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
        geom_arr = ga.as_wkb([wkb, wkb])
        table = pa.table({"id": [1, 2], "geometry": geom_arr})

        # No metadata, but has geoarrow types -> should return "2.0"
        assert detect_version_for_output(None, table) == "2.0"
        assert detect_version_for_output({}, table) == "2.0"

    def test_detect_version_for_output_prefers_metadata(self):
        """Test that metadata version takes precedence over geoarrow type."""
        import geoarrow.pyarrow as ga

        from geoparquet_io.core.streaming import detect_version_for_output

        # Create table with geoarrow extension type AND v1.1 metadata
        wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
        geom_arr = ga.as_wkb([wkb, wkb])
        table = pa.table({"id": [1, 2], "geometry": geom_arr})
        metadata = {b"geo": json.dumps({"version": "1.1.0"}).encode("utf-8")}

        # Metadata version should take precedence
        assert detect_version_for_output(metadata, table) == "1.1"

    def test_detect_version_for_output_returns_none(self):
        """Test None returned when no version info available."""
        from geoparquet_io.core.streaming import detect_version_for_output

        # No metadata, no geoarrow types
        table = pa.table({"id": [1, 2], "name": ["a", "b"]})
        assert detect_version_for_output(None, table) is None
        assert detect_version_for_output({}, table) is None
        assert detect_version_for_output(None, None) is None


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


class TestWkbByteRebatching:
    """Tests for the 32-bit WKB offset-overflow guard (issue #511)."""

    def test_data_nbytes_sums_value_lengths(self):
        """_wkb_chunk_data_nbytes counts value bytes, not offset/validity bytes."""
        blobs = [b"abc", b"de", b""]
        arr = pa.array(blobs, type=pa.large_binary())
        assert _wkb_chunk_data_nbytes(arr) == 5

    def test_data_nbytes_empty_array(self):
        """An empty array has zero value bytes."""
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

    def test_split_oversized_single_value(self):
        """A single value above the budget is emitted alone (no empty slices)."""
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

    def test_rebatch_splits_oversized_chunk(self):
        """Oversized chunks are split while preserving row order and values."""
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

        result = apply_geoarrow_extension_type(table, "geometry")

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

        monkeypatch.setattr(streaming_mod, "read_arrow_stream", lambda: table)
        return streaming_mod.read_stdin_to_temp_file()

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
                assert con.execute(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0] == 1
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

    Lets a test hand ``extract_crs_from_table`` exactly one CRS carrier shape
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
    """`extract_crs_from_table` must read a geoarrow CRS object, not stringify it.

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

        crs = extract_crs_from_table(table, "geometry")

        assert not isinstance(crs, str), f"stringified the CRS object: {crs!r}"
        assert isinstance(crs, dict)
        assert crs["id"] == {"authority": "OGC", "code": "CRS84"}

    def test_string_crs_object_resolves_to_projjson(self):
        """A StringCrs object ("EPSG:3857") resolves through its PROJJSON accessor."""
        table = _geoarrow_typed_table("EPSG:3857")

        crs = extract_crs_from_table(table, "geometry")

        assert crs is not None
        parsed = crs if isinstance(crs, dict) else json.loads(crs)
        assert parsed["id"] == {"authority": "EPSG", "code": 3857}

    def test_result_is_parseable_by_pyproj(self):
        """Whatever comes back must be a CRS pyproj can round-trip."""
        import pyproj

        table = _geoarrow_typed_table(OGC_CRS84_PROJJSON)

        crs = extract_crs_from_table(table, "geometry")

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

        crs = extract_crs_from_table(table, "geometry")

        assert crs == OGC_CRS84_PROJJSON

    def test_unknown_crs_object_is_rejected_not_stringified(self, caplog):
        """An unrecognised CRS object yields None, never its repr.

        The old catch-all would have returned ``"Opaque(EPSG:9999)"``, which the
        writer then split on ``:`` into a fabricated authority/code pair.
        """
        table = _table_with_crs_carrier(_OpaqueCrs())

        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            crs = extract_crs_from_table(table, "geometry")

        assert crs is None
        assert "_OpaqueCrs" in caplog.text
        assert "Opaque(EPSG:9999)" not in caplog.text


class TestCrsCarrierShapes:
    """Every shape ``geom_type.crs`` can take, driven directly (#816).

    These call ``extract_crs_from_table`` with one carrier shape each, with no
    dependence on whether ``geoarrow.pyarrow`` happened to be imported first --
    so each branch is exercised deterministically on every run.
    """

    def test_real_projjson_crs_object(self):
        """A ``geoarrow.types.crs.ProjJsonCrs`` resolves to its PROJJSON."""
        from geoarrow.types.crs import ProjJsonCrs

        table = _table_with_crs_carrier(ProjJsonCrs(OGC_CRS84_PROJJSON))

        crs = extract_crs_from_table(table, "geometry")

        assert isinstance(crs, dict)
        assert crs["id"] == {"authority": "OGC", "code": "CRS84"}

    def test_real_string_crs_object(self):
        """A ``geoarrow.types.crs.StringCrs`` resolves through pyproj to PROJJSON."""
        from geoarrow.types.crs import StringCrs

        table = _table_with_crs_carrier(StringCrs("EPSG:3857"))

        crs = extract_crs_from_table(table, "geometry")

        assert isinstance(crs, dict)
        assert crs["id"] == {"authority": "EPSG", "code": 3857}

    def test_pyproj_crs_object(self):
        """``pyproj.CRS`` satisfies the same protocol and must work too."""
        import pyproj

        table = _table_with_crs_carrier(pyproj.CRS("EPSG:4326"))

        crs = extract_crs_from_table(table, "geometry")

        assert isinstance(crs, dict)
        assert crs["id"] == {"authority": "EPSG", "code": 4326}

    def test_plain_projjson_dict_passes_through(self):
        """A type that already holds PROJJSON is returned unchanged."""
        table = _table_with_crs_carrier(OGC_CRS84_PROJJSON)

        assert extract_crs_from_table(table, "geometry") == OGC_CRS84_PROJJSON

    def test_plain_string_identifier_passes_through(self):
        """A bare identifier string stays a string for the caller to normalise."""
        table = _table_with_crs_carrier("EPSG:3857")

        assert extract_crs_from_table(table, "geometry") == "EPSG:3857"

    def test_utf8_bytes_are_decoded(self):
        """Bytes are decoded to text rather than stringified as ``b'...'``."""
        table = _table_with_crs_carrier(b"EPSG:3857")

        crs = extract_crs_from_table(table, "geometry")

        assert crs == "EPSG:3857"
        assert not str(crs).startswith("b'")

    def test_undecodable_bytes_are_rejected(self):
        """Bytes that are not UTF-8 are not a CRS; return None, never a repr."""
        table = _table_with_crs_carrier(b"\xff\xfe not utf-8")

        assert extract_crs_from_table(table, "geometry") is None

    def test_failing_projjson_accessor_warns_and_returns_none(self, caplog):
        """An accessor that raises is reported, and yields None -- not a repr."""
        table = _table_with_crs_carrier(_ExplodingCrs())

        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            crs = extract_crs_from_table(table, "geometry")

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
            crs = extract_crs_from_table(table, "geometry")

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
