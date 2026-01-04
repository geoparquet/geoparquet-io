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


def get_crs_from_arrow_table(table: pa.Table, geometry_column: str) -> str | None:
    """
    Get CRS from Arrow table's GeoParquet metadata.

    Args:
        table: PyArrow Table to inspect
        geometry_column: Name of the geometry column

    Returns:
        CRS string (e.g., "EPSG:4326") or None if not found
    """
    import json

    metadata = dict(table.schema.metadata) if table.schema.metadata else {}

    # Check for GeoParquet geo metadata
    geo_bytes = metadata.get(b"geo")
    if not geo_bytes:
        return None

    try:
        geo_meta = json.loads(geo_bytes.decode("utf-8"))
        columns = geo_meta.get("columns", {})
        col_meta = columns.get(geometry_column, {})

        crs = col_meta.get("crs")
        if crs:
            # Extract EPSG code from CRS object
            if isinstance(crs, dict):
                auth = crs.get("id", {})
                if auth.get("authority") == "EPSG":
                    return f"EPSG:{auth.get('code')}"
            return str(crs) if not isinstance(crs, dict) else None

        return None
    except Exception:
        return None


def read_stdin_to_temp_file(verbose: bool = False) -> str:
    """
    Read Arrow IPC stream from stdin and write to a temporary parquet file.

    This is a shared utility for commands that need file-based processing
    but want to support stdin input. The caller is responsible for cleanup.

    Args:
        verbose: Whether to print verbose output

    Returns:
        Path to the temporary parquet file. Caller must delete after use.
    """
    import os
    import tempfile
    import uuid

    import pyarrow.parquet as pq

    from geoparquet_io.core.logging_config import debug

    if verbose:
        debug("Reading Arrow IPC stream from stdin...")

    table = read_arrow_stream()

    # Write to temp file with UUID for uniqueness
    temp_dir = tempfile.gettempdir()
    temp_path = os.path.join(temp_dir, f"gpio_stdin_{uuid.uuid4()}.parquet")

    pq.write_table(table, temp_path)

    if verbose:
        debug(f"Wrote {table.num_rows} rows to temporary file: {temp_path}")

    return temp_path


def apply_geoarrow_extension_type(
    table: pa.Table,
    geometry_column: str,
    crs: dict | str | None = None,
) -> pa.Table:
    """
    Convert geometry column to geoarrow extension type.

    This enables native geometry performance in downstream operations.
    Arrow IPC preserves extension types, so geoarrow types survive piping.

    Args:
        table: PyArrow Table with geometry column
        geometry_column: Name of the geometry column
        crs: CRS as PROJJSON dict, string identifier, or None

    Returns:
        Table with geometry column converted to geoarrow extension type
    """
    import geoarrow.pyarrow as ga

    if geometry_column not in table.column_names:
        return table

    try:
        geom_col = table.column(geometry_column)

        # Convert to geoarrow WKB extension type
        wkb_arr = ga.as_wkb(geom_col)

        # Apply CRS if provided
        if crs:
            new_type = wkb_arr.type.with_crs(crs)
            # Use from_storage to preserve CRS (cast() resets it)
            new_chunks = []
            for chunk in wkb_arr.chunks:
                new_chunk = pa.ExtensionArray.from_storage(new_type, chunk.storage)
                new_chunks.append(new_chunk)
            wkb_arr = pa.chunked_array(new_chunks, type=new_type)

        # Replace geometry column in table
        col_index = table.schema.get_field_index(geometry_column)
        return table.set_column(col_index, geometry_column, wkb_arr)

    except (TypeError, ValueError, AttributeError):
        # If conversion fails, return original table
        return table


def extract_crs_from_table(
    table: pa.Table,
    geometry_column: str | None = None,
) -> dict | str | None:
    """
    Extract CRS from Arrow table.

    Checks in order:
    1. Geoarrow extension type CRS
    2. GeoParquet geo metadata

    Args:
        table: PyArrow Table to inspect
        geometry_column: Name of geometry column (auto-detect if None)

    Returns:
        CRS as PROJJSON dict, string, or None if not found
    """
    # Find geometry column if not specified
    if geometry_column is None:
        geometry_column = find_geometry_column_from_table(table)

    if geometry_column and geometry_column in table.column_names:
        geom_type = table.column(geometry_column).type

        # Check for geoarrow extension type with CRS
        if hasattr(geom_type, "crs") and geom_type.crs is not None:
            crs = geom_type.crs
            # Convert to string if it's a CRS object
            if hasattr(crs, "__str__"):
                return str(crs)
            return crs

    # Fall back to geo metadata
    if table.schema.metadata and b"geo" in table.schema.metadata:
        try:
            geo_meta = json.loads(table.schema.metadata[b"geo"].decode("utf-8"))
            if isinstance(geo_meta, dict):
                columns = geo_meta.get("columns", {})
                geom_col_name = geometry_column or geo_meta.get("primary_column", "geometry")
                if geom_col_name in columns:
                    return columns[geom_col_name].get("crs")
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    return None


def strip_geoarrow_extension_type(
    table: pa.Table,
    geometry_column: str,
) -> pa.Table:
    """
    Convert geoarrow extension type back to plain binary WKB.

    Used when writing GeoParquet 1.x output which uses plain binary
    geometry with CRS only in metadata.

    Args:
        table: PyArrow Table with geoarrow geometry column
        geometry_column: Name of the geometry column

    Returns:
        Table with geometry column as plain binary
    """
    if geometry_column not in table.column_names:
        return table

    geom_col = table.column(geometry_column)
    geom_type = geom_col.type

    # Check if it's a geoarrow extension type
    if not hasattr(geom_type, "extension_name"):
        return table  # Already plain binary

    if not geom_type.extension_name.startswith("geoarrow"):
        return table  # Not a geoarrow type

    try:
        # Extract storage (plain binary) from extension type
        new_chunks = []
        for chunk in geom_col.chunks:
            if hasattr(chunk, "storage"):
                new_chunks.append(chunk.storage)
            else:
                new_chunks.append(chunk)

        # Create new binary column
        plain_col = pa.chunked_array(new_chunks, type=pa.binary())

        # Replace in table
        col_index = table.schema.get_field_index(geometry_column)
        return table.set_column(col_index, geometry_column, plain_col)

    except (TypeError, ValueError, AttributeError):
        return table


def is_geoarrow_type(arrow_type) -> bool:
    """Check if an Arrow type is a geoarrow extension type."""
    if hasattr(arrow_type, "extension_name"):
        return arrow_type.extension_name.startswith("geoarrow")
    return False


def extract_version_from_metadata(metadata: dict | None) -> str | None:
    """
    Extract GeoParquet version string from schema metadata.

    Upgrades 1.0 to 1.1 since 1.1 is backwards compatible and preferred.

    Args:
        metadata: Schema metadata dict (with bytes keys)

    Returns:
        Version string suitable for --geoparquet-version (e.g., "1.1", "2.0")
        or None if no version detected
    """
    if not metadata or b"geo" not in metadata:
        return None
    try:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        if isinstance(geo_meta, dict):
            version = geo_meta.get("version")
            if version:
                parts = version.split(".")
                if len(parts) >= 2:
                    major = parts[0]
                    # Upgrade all 1.x versions to 1.1 (backwards compatible)
                    if major == "1":
                        return "1.1"
                    return f"{major}.{parts[1]}"
        return None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def has_geoarrow_extension_in_table(table: pa.Table) -> bool:
    """
    Check if table has geoarrow extension types (indicating native geo types).

    Args:
        table: PyArrow Table to inspect

    Returns:
        True if table has geoarrow extension type columns
    """
    for field in table.schema:
        if is_geoarrow_type(field.type):
            return True
    return False


def detect_version_for_output(
    original_metadata: dict | None,
    table: pa.Table | None = None,
) -> str | None:
    """
    Detect the appropriate GeoParquet version for output.

    Logic:
    - If geo metadata has version 1.x -> return "1.1" (upgrade 1.0 to 1.1)
    - If geo metadata has version 2.x -> return "2.0"
    - If no geo metadata but has geoarrow types -> return "2.0" (upgrade)
    - Otherwise -> return None (will use default 1.1)

    Args:
        original_metadata: Schema metadata from input
        table: Arrow table (optional, for detecting geoarrow types)

    Returns:
        Version string or None
    """
    # First check geo metadata for explicit version
    version = extract_version_from_metadata(original_metadata)
    if version:
        return version

    # Check for parquet-geo-only (geoarrow types without geo metadata)
    if table is not None and has_geoarrow_extension_in_table(table):
        return "2.0"  # Upgrade to 2.0 with proper metadata

    # No version info - will use default (1.1)
    return None


class StreamingError(Exception):
    """Error raised during Arrow IPC streaming operations."""

    pass
