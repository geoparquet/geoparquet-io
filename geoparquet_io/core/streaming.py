#!/usr/bin/env python3
"""
Arrow IPC streaming utilities for Unix-style piping between gpio commands.

This module provides low-level streaming primitives for reading/writing
Arrow IPC format to stdin/stdout, enabling pipelines like:

    gpio add bbox input.parquet | gpio sort hilbert - output.parquet

Arrow IPC is used because:
- Zero-copy data exchange between processes
- Preserves schema metadata (including GeoParquet geo metadata)
- Native support in PyArrow and DuckDB
- Efficient columnar format for geospatial data
"""

from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.ipc as ipc

if TYPE_CHECKING:
    pass


# Marker for stdin/stdout in CLI arguments
STREAM_MARKER = "-"


def is_stdin(path: str | None) -> bool:
    """Check if path indicates stdin streaming."""
    return path == STREAM_MARKER


def is_stdout(path: str | None) -> bool:
    """Check if path indicates explicit stdout streaming."""
    return path == STREAM_MARKER


def should_stream_output(output_path: str | None) -> bool:
    """
    Determine if output should go to stdout.

    Returns True if:
    - output_path is "-" (explicit stdout)
    - output_path is None and stdout is a pipe (auto-detect)

    Returns False if:
    - output_path is a file path
    - output_path is None and stdout is a terminal
    """
    if output_path == STREAM_MARKER:
        return True
    if output_path is None:
        # Auto-detect: stream if stdout is piped, not a terminal
        return not sys.stdout.isatty()
    return False


def validate_stdin() -> None:
    """
    Validate stdin is available for streaming.

    Raises:
        StreamingError: If stdin is a terminal (no data piped)
    """
    if sys.stdin.isatty():
        raise StreamingError(
            "No data on stdin. Pipe from another command or use a file path.\n\n"
            "Examples:\n"
            "  gpio add bbox input.parquet | gpio sort hilbert - output.parquet\n"
            "  gpio sort hilbert input.parquet output.parquet"
        )


def validate_output(output_path: str | None) -> None:
    """
    Validate output configuration and raise/warn appropriately.

    Raises:
        StreamingError: If no output and stdout is a terminal

    Warns:
        If explicit "-" and stdout is a terminal (binary to terminal)
    """
    if output_path is None and sys.stdout.isatty():
        raise StreamingError(
            "Missing output. Pipe to another command or specify an output file.\n\n"
            "Examples:\n"
            "  gpio add bbox input.parquet output.parquet\n"
            "  gpio add bbox input.parquet | gpio sort hilbert - output.parquet"
        )
    if output_path == STREAM_MARKER and sys.stdout.isatty():
        from geoparquet_io.core.logging_config import warn

        warn("Writing binary Arrow IPC data to terminal...")


def read_arrow_stream() -> pa.Table:
    """
    Read an Arrow IPC stream from stdin.

    Returns:
        PyArrow Table with all data from the stream

    Raises:
        StreamingError: If stdin is a terminal or stream is invalid
    """
    validate_stdin()
    try:
        reader = ipc.RecordBatchStreamReader(sys.stdin.buffer)
        return reader.read_all()
    except pa.ArrowInvalid as e:
        error_msg = str(e)
        if "null or length 0" in error_msg:
            raise StreamingError(
                "No data received on stdin. This usually means the upstream command failed.\n\n"
                "Common causes:\n"
                "  - Upstream command encountered an error (check messages above)\n"
                "  - Input file doesn't exist or is invalid\n\n"
                "Example of correct piping syntax:\n"
                "  gpio extract input.parquet | gpio add bbox - | gpio sort hilbert - out.parquet\n"
                "                             ^               ^\n"
                "              (auto-streams when piped)  (read from stdin)"
            ) from e
        raise StreamingError(
            f"Invalid Arrow IPC stream on stdin. Ensure input is from a gpio command.\n\nError: {e}"
        ) from e


def write_arrow_stream(table: pa.Table) -> None:
    """
    Write a PyArrow Table as Arrow IPC stream to stdout.

    Args:
        table: PyArrow Table to write
    """
    writer = ipc.RecordBatchStreamWriter(sys.stdout.buffer, table.schema)
    writer.write_table(table)
    writer.close()


def extract_geo_metadata(table: pa.Table) -> dict | None:
    """
    Extract GeoParquet metadata from Arrow table schema.

    Args:
        table: PyArrow Table with potential geo metadata

    Returns:
        Parsed geo metadata dict, or None if not present
    """
    if table.schema.metadata and b"geo" in table.schema.metadata:
        try:
            return json.loads(table.schema.metadata[b"geo"].decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
    return None


def apply_geo_metadata(table: pa.Table, geo_meta: dict) -> pa.Table:
    """
    Apply geo metadata to Arrow table schema.

    Args:
        table: PyArrow Table to update
        geo_meta: GeoParquet metadata dict to apply

    Returns:
        New table with updated schema metadata
    """
    metadata = dict(table.schema.metadata) if table.schema.metadata else {}
    metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
    return table.replace_schema_metadata(metadata)


def apply_metadata_to_table(table: pa.Table, metadata: dict | None) -> pa.Table:
    """
    Apply raw metadata dict to Arrow table schema.

    Args:
        table: PyArrow Table to update
        metadata: Metadata dict (with bytes keys) to apply

    Returns:
        New table with updated schema metadata
    """
    if not metadata:
        return table
    return table.replace_schema_metadata(metadata)


def find_geometry_column_from_metadata(metadata: dict | None) -> str | None:
    """
    Find the primary geometry column name from metadata.

    Args:
        metadata: Schema metadata dict (with bytes keys)

    Returns:
        Geometry column name or None if not found
    """
    if not metadata or b"geo" not in metadata:
        return None
    try:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        if isinstance(geo_meta, dict):
            return geo_meta.get("primary_column", "geometry")
    except (json.JSONDecodeError, UnicodeDecodeError):
        pass
    return None


def find_geometry_column_from_table(table: pa.Table) -> str | None:
    """
    Find the geometry column name from table metadata or common names.

    Args:
        table: PyArrow Table to inspect

    Returns:
        Geometry column name or None if not found
    """
    metadata = dict(table.schema.metadata) if table.schema.metadata else {}

    # Try to find from geo metadata
    geom_col = find_geometry_column_from_metadata(metadata)
    if geom_col and geom_col in table.column_names:
        return geom_col

    # Fall back to common names
    for name in ["geometry", "geom", "the_geom", "wkb_geometry"]:
        if name in table.column_names:
            return name

    return None


class StreamingError(Exception):
    """Error raised during Arrow IPC streaming operations."""

    pass
