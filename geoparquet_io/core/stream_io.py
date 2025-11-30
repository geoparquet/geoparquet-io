#!/usr/bin/env python3
"""
Unified I/O abstraction for file and stream modes.

This module provides high-level abstractions that work seamlessly with both
file-based and streaming I/O, allowing commands to handle both modes with
minimal code changes.
"""

from contextlib import contextmanager
from typing import Optional, Tuple

import duckdb
import pyarrow as pa

from geoparquet_io.core.common import (
    get_duckdb_connection,
    get_parquet_metadata,
    needs_httpfs,
    safe_file_url,
    write_parquet_with_metadata,
)
from geoparquet_io.core.streaming import (
    apply_metadata_to_table,
    is_stdin,
    read_arrow_stream,
    should_stream_output,
    validate_output,
    write_arrow_stream,
)


def _find_geometry_column(table: pa.Table, metadata: Optional[dict]) -> Optional[str]:
    """Find the geometry column name from metadata or schema."""
    import json

    # Try to find from geo metadata
    if metadata and b"geo" in metadata:
        try:
            geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
            if isinstance(geo_meta, dict):
                return geo_meta.get("primary_column", "geometry")
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    # Fall back to common names
    column_names = table.column_names
    for name in ["geometry", "geom", "the_geom", "wkb_geometry"]:
        if name in column_names:
            return name

    return None


def _create_view_with_geometry(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    geometry_column: Optional[str],
) -> str:
    """
    Create a view that converts WKB BLOB to GEOMETRY type for DuckDB.

    When Arrow IPC is registered, WKB geometry is seen as BLOB.
    This creates a view that converts it to proper GEOMETRY type.
    """
    if not geometry_column:
        # No geometry column found, just use the table as-is
        return table_name

    # Get column info
    columns = con.execute(f"DESCRIBE {table_name}").fetchall()
    column_defs = []

    for col_name, col_type, *_ in columns:
        if col_name == geometry_column and "BLOB" in col_type.upper():
            # Convert BLOB to GEOMETRY using ST_GeomFromWKB
            column_defs.append(f"ST_GeomFromWKB({col_name}) AS {col_name}")
        else:
            column_defs.append(col_name)

    # Create view with proper geometry type
    view_name = f"{table_name}_geom"
    select_cols = ", ".join(column_defs)
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {select_cols} FROM {table_name}")

    return view_name


@contextmanager
def open_input(
    path: str,
    con: Optional[duckdb.DuckDBPyConnection] = None,
    verbose: bool = False,
):
    """
    Open input source (file or stream) and prepare for DuckDB processing.

    For files: Returns the file path for direct DuckDB query
    For streams: Reads Arrow IPC, registers in DuckDB, returns table name

    Args:
        path: Input path (file path or "-" for stdin)
        con: Optional existing DuckDB connection (one will be created if None)
        verbose: Whether to print verbose output

    Yields:
        Tuple of (source_reference, original_metadata, is_streaming, connection)
        - source_reference: String to use in SQL queries (table name or file path)
        - original_metadata: Schema metadata dict from input
        - is_streaming: True if reading from stream
        - connection: DuckDB connection (created or passed in)

    Example:
        with open_input("input.parquet", con) as (source, metadata, is_stream, con):
            result = con.execute(f"SELECT * FROM {source}")
    """
    if is_stdin(path):
        # Streaming input: read Arrow IPC and register in DuckDB
        table = read_arrow_stream()
        metadata = dict(table.schema.metadata) if table.schema.metadata else {}

        if con is None:
            con = get_duckdb_connection(load_spatial=True, load_httpfs=False)

        # Register the Arrow table for SQL queries
        con.register("input_stream", table)

        # Find geometry column and create view with proper geometry type
        geom_col = _find_geometry_column(table, metadata)
        source_ref = _create_view_with_geometry(con, "input_stream", geom_col)

        yield source_ref, metadata, True, con
    else:
        # File input: use file path directly
        safe_url = safe_file_url(path, verbose=verbose)
        file_metadata, _ = get_parquet_metadata(path, verbose=verbose)

        if con is None:
            con = get_duckdb_connection(
                load_spatial=True, load_httpfs=needs_httpfs(path)
            )

        yield f"'{safe_url}'", file_metadata, False, con


def _get_geometry_column_from_metadata(metadata: Optional[dict]) -> Optional[str]:
    """Get geometry column name from metadata."""
    import json

    if not metadata or b"geo" not in metadata:
        return None
    try:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        if isinstance(geo_meta, dict):
            return geo_meta.get("primary_column", "geometry")
    except (json.JSONDecodeError, UnicodeDecodeError):
        pass
    return None


def _wrap_query_with_wkb_conversion(
    con: duckdb.DuckDBPyConnection,
    query: str,
    geometry_column: Optional[str],
) -> str:
    """
    Wrap query to convert DuckDB geometry back to WKB for Arrow export.

    DuckDB's fetch_arrow_table() exports geometry in DuckDB's native format,
    not WKB. This wraps the query to convert geometry back to WKB using ST_AsWKB.
    """
    if not geometry_column:
        return query

    # Create a CTE and wrap with WKB conversion
    # We need to check if the geometry column exists and is GEOMETRY type
    return f"""
        WITH __stream_source AS ({query})
        SELECT * REPLACE (ST_AsWKB({geometry_column}) AS {geometry_column})
        FROM __stream_source
    """


def write_output(
    con: duckdb.DuckDBPyConnection,
    query: str,
    output_path: Optional[str],
    original_metadata: Optional[dict] = None,
    compression: str = "ZSTD",
    compression_level: int = 15,
    row_group_size_mb: Optional[float] = None,
    row_group_rows: Optional[int] = None,
    verbose: bool = False,
    profile: Optional[str] = None,
) -> None:
    """
    Execute query and write result to file or stream.

    Uses auto-detect for output:
    - If output_path is None and stdout is piped → streams to stdout
    - If output_path is "-" → streams to stdout (explicit)
    - If output_path is a file path → writes Parquet with full optimization

    Args:
        con: DuckDB connection
        query: SQL query to execute
        output_path: Output path (None for auto-detect, "-" for stdout, or file path)
        original_metadata: Metadata to preserve in output
        compression: Compression for file output
        compression_level: Compression level for file output
        row_group_size_mb: Row group size for file output
        row_group_rows: Row group rows for file output
        verbose: Whether to print verbose output
        profile: AWS profile for S3 output

    Raises:
        click.UsageError: If no output and stdout is a terminal
    """
    # Validate output configuration
    validate_output(output_path)

    if should_stream_output(output_path):
        # Streaming output: execute query and write Arrow IPC
        # Need to convert geometry back to WKB for portable Arrow export
        geom_col = _get_geometry_column_from_metadata(original_metadata)
        stream_query = _wrap_query_with_wkb_conversion(con, query, geom_col)

        result = con.execute(stream_query)
        table = result.fetch_arrow_table()

        # Apply metadata to output table
        if original_metadata:
            table = apply_metadata_to_table(table, original_metadata)

        write_arrow_stream(table)
    else:
        # File output: use existing optimized writer
        write_parquet_with_metadata(
            con,
            query,
            output_path,
            original_metadata=original_metadata,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
        )


def execute_transform(
    input_path: str,
    output_path: Optional[str],
    transform_query_fn,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: int = 15,
    row_group_size_mb: Optional[float] = None,
    row_group_rows: Optional[int] = None,
    profile: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """
    Execute a transformation with unified streaming/file I/O.

    This is a high-level helper that handles the full input→transform→output
    pipeline for both file and streaming modes.

    Args:
        input_path: Input path (file or "-" for stdin)
        output_path: Output path (file, "-" for stdout, or None for auto-detect)
        transform_query_fn: Callable(source_ref, con) -> SQL query string
        verbose: Whether to print verbose output
        compression: Compression for file output
        compression_level: Compression level for file output
        row_group_size_mb: Row group size for file output
        row_group_rows: Row group rows for file output
        profile: AWS profile for remote I/O
        dry_run: If True, print query without executing

    Example:
        def make_query(source, con):
            return f"SELECT *, bbox_col FROM {source}"

        execute_transform("input.parquet", None, make_query, verbose=True)
    """
    import click

    with open_input(input_path, verbose=verbose) as (source, metadata, is_stream, con):
        # Generate the transform query
        query = transform_query_fn(source, con)

        if dry_run:
            click.echo(
                click.style(
                    "\n=== DRY RUN MODE - SQL that would be executed ===\n",
                    fg="yellow",
                    bold=True,
                )
            )
            click.echo(query)
            return

        # Execute and write output
        write_output(
            con,
            query,
            output_path,
            original_metadata=metadata,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose if not should_stream_output(output_path) else False,
            profile=profile,
        )
