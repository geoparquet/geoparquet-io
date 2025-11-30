#!/usr/bin/env python3
"""
Arrow IPC streaming utilities for Unix-style piping between gpio commands.

This module provides low-level streaming primitives for reading/writing
Arrow IPC format to stdin/stdout, enabling pipelines like:

    gpio add bbox input.parquet | gpio sort hilbert - output.parquet
"""

import sys
from typing import Optional

import click
import pyarrow as pa
import pyarrow.ipc as ipc


# Marker for stdin/stdout
STREAM_MARKER = "-"


def is_stdin(path: Optional[str]) -> bool:
    """Check if path indicates stdin streaming."""
    return path == STREAM_MARKER


def is_stdout(path: Optional[str]) -> bool:
    """Check if path indicates explicit stdout streaming."""
    return path == STREAM_MARKER


def should_stream_output(output_path: Optional[str]) -> bool:
    """
    Determine if we should stream to stdout.

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


def validate_output(output_path: Optional[str]) -> None:
    """
    Validate output configuration and raise/warn appropriately.

    Raises:
        click.UsageError: If no output and stdout is a terminal

    Warns:
        If explicit "-" and stdout is a terminal (binary to terminal)
    """
    if output_path is None and sys.stdout.isatty():
        raise click.UsageError(
            "Missing output. Pipe to another command or specify an output file.\n\n"
            "Examples:\n"
            "  gpio add bbox input.parquet output.parquet\n"
            "  gpio add bbox input.parquet | gpio sort hilbert - output.parquet"
        )
    if output_path == STREAM_MARKER and sys.stdout.isatty():
        click.echo(
            click.style(
                "Warning: Writing binary Arrow IPC data to terminal...", fg="yellow"
            ),
            err=True,
        )


def validate_stdin() -> None:
    """
    Validate stdin is available for streaming.

    Raises:
        click.UsageError: If stdin is a terminal (no data piped)
    """
    if sys.stdin.isatty():
        raise click.UsageError(
            "No data on stdin. Pipe from another command or use a file path.\n\n"
            "Examples:\n"
            "  gpio add bbox input.parquet | gpio sort hilbert - output.parquet\n"
            "  gpio sort hilbert input.parquet output.parquet"
        )


def read_arrow_stream() -> pa.Table:
    """
    Read an Arrow IPC stream from stdin.

    Returns:
        PyArrow Table with all data from the stream

    Raises:
        click.UsageError: If stdin is a terminal
        pa.ArrowInvalid: If stream is not valid Arrow IPC
    """
    validate_stdin()
    try:
        reader = ipc.RecordBatchStreamReader(sys.stdin.buffer)
        return reader.read_all()
    except pa.ArrowInvalid as e:
        raise click.ClickException(
            f"Invalid Arrow IPC stream on stdin. "
            f"Ensure input is from a gpio command.\n\nError: {e}"
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


def extract_geo_metadata(table: pa.Table) -> Optional[dict]:
    """
    Extract GeoParquet metadata from Arrow table schema.

    Args:
        table: PyArrow Table with potential geo metadata

    Returns:
        Parsed geo metadata dict, or None if not present
    """
    import json

    if table.schema.metadata and b"geo" in table.schema.metadata:
        try:
            return json.loads(table.schema.metadata[b"geo"].decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
    return None


def apply_metadata_to_table(
    table: pa.Table, metadata: Optional[dict]
) -> pa.Table:
    """
    Apply metadata dict to Arrow table schema.

    Args:
        table: PyArrow Table to update
        metadata: Metadata dict (with bytes keys) to apply

    Returns:
        New table with updated schema metadata
    """
    if not metadata:
        return table
    return table.replace_schema_metadata(metadata)
