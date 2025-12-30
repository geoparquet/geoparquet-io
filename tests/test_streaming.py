"""
Tests for Arrow IPC streaming utilities.

Tests the low-level streaming primitives in core/streaming.py
and the high-level abstractions in core/stream_io.py.
"""

from __future__ import annotations

import io
import json
import sys
from unittest import mock

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from geoparquet_io.core.streaming import (
    STREAM_MARKER,
    StreamingError,
    apply_geo_metadata,
    apply_metadata_to_table,
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

    def test_find_geometry_column_from_metadata_default(self):
        """Test default geometry column when not specified."""
        metadata = {b"geo": json.dumps({}).encode("utf-8")}
        assert find_geometry_column_from_metadata(metadata) == "geometry"

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
        assert "ST_AsWKB(geometry)" in result
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

        from geoparquet_io.core.common import get_duckdb_connection
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

        from geoparquet_io.core.common import get_duckdb_connection
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
