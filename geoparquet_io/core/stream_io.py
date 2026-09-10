#!/usr/bin/env python3
"""
Unified I/O abstraction for file and stream modes.

This module provides high-level abstractions that work seamlessly with both
file-based and streaming I/O, allowing commands to handle both modes with
minimal code changes.

Key abstractions:
- open_input(): Context manager that handles file or stdin input
- write_output(): Writes to file or stdout based on configuration
- execute_transform(): Full input→transform→output pipeline helper
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa

from geoparquet_io.core.common import get_parquet_metadata, write_parquet_with_metadata
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier, sql_path
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geo_metadata import (
    backfill_derived_stats,
    prune_geo_metadata_to_columns,
    sanitized_carried_geo,
)
from geoparquet_io.core.logging_config import warn
from geoparquet_io.core.remote import needs_httpfs
from geoparquet_io.core.streaming import (
    apply_geoarrow_extension_type,
    apply_metadata_to_table,
    detect_version_for_output,
    find_geometry_column_from_metadata,
    find_geometry_column_from_table,
    is_stdin,
    read_arrow_stream,
    should_stream_output,
    validate_output,
    write_arrow_stream,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


def _create_view_with_geometry(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    geometry_column: str | None,
) -> str:
    """
    Create a view that converts WKB BLOB to GEOMETRY type for DuckDB.

    When Arrow IPC is registered, WKB geometry is seen as BLOB.
    This creates a view that converts it to proper GEOMETRY type.

    Args:
        con: DuckDB connection
        table_name: Name of registered table
        geometry_column: Name of geometry column, or None if unknown

    Returns:
        Name of view to use in queries (may be same as table_name)
    """
    if not geometry_column:
        return table_name

    quoted_table = quote_identifier(table_name)

    # Get column info to check types
    columns = con.execute(f"DESCRIBE {quoted_table}").fetchall()
    column_defs = []

    for col_name, col_type, *_ in columns:
        quoted_col = quote_identifier(col_name)
        if col_name == geometry_column and "BLOB" in col_type.upper():
            # Convert BLOB to GEOMETRY using ST_GeomFromWKB
            column_defs.append(f"ST_GeomFromWKB({quoted_col}) AS {quoted_col}")
        else:
            column_defs.append(quoted_col)

    # Create view with proper geometry type
    view_name = f"{table_name}_geom"
    quoted_view = quote_identifier(view_name)
    select_cols = ", ".join(column_defs)
    con.execute(f"CREATE OR REPLACE VIEW {quoted_view} AS SELECT {select_cols} FROM {quoted_table}")

    return view_name


@contextmanager
def open_input(
    path: str,
    con: duckdb.DuckDBPyConnection | None = None,
    verbose: bool = False,
) -> Iterator[tuple[str, dict | None, bool, duckdb.DuckDBPyConnection]]:
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
        - source_reference: String to use in SQL queries (table name or read_parquet())
        - original_metadata: Schema metadata dict from input
        - is_streaming: True if reading from stream
        - connection: DuckDB connection (created or passed in)

    Example:
        with open_input("input.parquet") as (source, metadata, is_stream, con):
            result = con.execute(f"SELECT * FROM {source}")
    """
    if is_stdin(path):
        yield from _open_stdin_input(con, verbose)
    else:
        yield from _open_file_input(path, con, verbose)


def _open_stdin_input(
    con: duckdb.DuckDBPyConnection | None,
    verbose: bool,
) -> Iterator[tuple[str, dict | None, bool, duckdb.DuckDBPyConnection]]:
    """Handle streaming input from stdin."""
    table = read_arrow_stream()
    metadata = dict(table.schema.metadata) if table.schema.metadata else {}

    created_connection = con is None
    if created_connection:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)

    try:
        # Register the Arrow table for SQL queries
        con.register("input_stream", table)

        # Find geometry column and create view with proper geometry type
        geom_col = find_geometry_column_from_table(table)
        source_ref = _create_view_with_geometry(con, "input_stream", geom_col)

        yield source_ref, metadata, True, con
    finally:
        if created_connection:
            con.close()


def _open_file_input(
    path: str,
    con: duckdb.DuckDBPyConnection | None,
    verbose: bool,
) -> Iterator[tuple[str, dict | None, bool, duckdb.DuckDBPyConnection]]:
    """Handle file-based input."""
    raw_url = resolve_file_url(path, verbose=verbose)
    file_metadata, _ = get_parquet_metadata(path, verbose=verbose)

    created_connection = con is None
    if created_connection:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(path))

    try:
        yield f"read_parquet({sql_path(raw_url)})", file_metadata, False, con
    finally:
        if created_connection:
            con.close()


def _wrap_query_with_wkb_conversion(
    query: str,
    geometry_column: str | None,
) -> str:
    """
    Wrap query to convert DuckDB geometry back to WKB for Arrow export.

    DuckDB's Arrow export returns geometry in DuckDB's native format,
    not WKB. This wraps the query to convert geometry back to WKB using ST_AsWKB.

    Args:
        query: Original SQL query
        geometry_column: Name of geometry column, or None to skip conversion

    Returns:
        Query wrapped with WKB conversion if needed
    """
    if not geometry_column:
        return query

    quoted_geom = quote_identifier(geometry_column)
    return f"""
        WITH __stream_source AS ({query})
        SELECT * REPLACE (ST_AsWKB({quoted_geom}) AS {quoted_geom})
        FROM __stream_source
    """


def write_output(
    con: duckdb.DuckDBPyConnection,
    query: str,
    output_path: str | None,
    original_metadata: dict | None = None,
    geometry_column: str | None = None,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    verbose: bool = False,
    profile: str | None = None,
    custom_metadata: dict | None = None,
    geoparquet_version: str | None = None,
    extra_kv_metadata: dict[str, str] | None = None,
    memory_limit: str | None = None,
) -> pa.Table | None:
    """
    Execute query and write result to file or stream.

    Uses auto-detect for output:
    - If output_path is None and stdout is piped -> streams to stdout
    - If output_path is "-" -> streams to stdout (explicit)
    - If output_path is a file path -> writes Parquet with full optimization

    Args:
        con: DuckDB connection
        query: SQL query to execute
        output_path: Output path (None for auto-detect, "-" for stdout, or file path)
        original_metadata: Metadata to preserve in output
        geometry_column: Geometry column name for WKB conversion (auto-detect if None)
        compression: Compression for file output
        compression_level: Compression level for file output
        row_group_size_mb: Row group size for file output
        row_group_rows: Row group rows for file output
        verbose: Whether to print verbose output
        profile: AWS profile for S3 output
        custom_metadata: Optional dict with custom metadata
        geoparquet_version: GeoParquet version to write
        memory_limit: DuckDB memory limit for file writes (e.g., '2GB', '512MB');
            ignored for stream output

    Returns:
        Table if streaming output, None if file output

    Raises:
        StreamingError: If no output and stdout is a terminal
    """
    validate_output(output_path)

    if should_stream_output(output_path):
        # Arrow IPC to stdout has no DuckDB write engine to configure, so a
        # memory limit cannot be applied. Say so rather than dropping it silently.
        if memory_limit is not None:
            warn(
                "--write-memory is ignored when streaming to stdout "
                "(it only applies to file outputs)."
            )
        return _write_stream_output(con, query, original_metadata, geometry_column)
    else:
        _write_file_output(
            con,
            query,
            output_path,
            original_metadata,
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            verbose,
            profile,
            custom_metadata,
            geoparquet_version,
            extra_kv_metadata,
            memory_limit,
        )
        return None


def _extract_crs_from_metadata(
    metadata: dict | None, geometry_column: str | None = None
) -> dict | str | None:
    """Extract CRS from GeoParquet metadata.

    A write-path reader: the answer becomes the geoarrow extension type's CRS on
    the Arrow stream this process writes to stdout, so a malformed carried block
    goes through the shared shape check (#947). It used to be indexed raw, and
    the ``except`` beside it caught only the JSON errors -- ``columns: null``
    escaped as ``argument of type 'NoneType' is not iterable``, and a list- or
    string-shaped ``columns`` as a ``TypeError`` from the line after it.

    ``geometry_column`` names the column the CRS is about to be attached to.
    The caller knows it, and reading some other column's CRS onto it is the
    silent half of #887: a block whose ``primary_column`` sanitizing had to
    drop would otherwise fall back to the literal ``"geometry"`` and stream a
    ``geom`` column as if its CRS were absent.
    """
    geo_meta = sanitized_carried_geo(metadata)
    columns = geo_meta.get("columns")
    if not isinstance(columns, dict):
        return None
    col = geometry_column or geo_meta.get("primary_column", "geometry")
    col_meta = columns.get(col)
    return col_meta.get("crs") if isinstance(col_meta, dict) else None


def _write_stream_output(
    con: duckdb.DuckDBPyConnection,
    query: str,
    original_metadata: dict | None,
    geometry_column: str | None,
) -> pa.Table:
    """Write output as Arrow IPC stream to stdout.

    Uses geoarrow extension types for streaming, which enables:
    - Native geometry performance in downstream commands
    - CRS preservation through the pipeline
    - Automatic native Parquet geometry when written to file
    """
    # Auto-detect geometry column if not provided
    if geometry_column is None:
        geometry_column = find_geometry_column_from_metadata(original_metadata)

    # Convert geometry to WKB (DuckDB's Arrow export uses native format)
    stream_query = _wrap_query_with_wkb_conversion(query, geometry_column)

    result = con.execute(stream_query)
    table = result.arrow().read_all()

    # Convert WKB binary to geoarrow extension type for streaming
    # This enables native geometry performance in downstream operations
    if geometry_column:
        crs = _extract_crs_from_metadata(original_metadata, geometry_column)
        table = apply_geoarrow_extension_type(table, geometry_column, crs)

    # Apply metadata to output table, the way the file path does (#722):
    # first drop entries for columns a projection removed -- a dropped secondary
    # geometry column would otherwise leave an orphan `geo.columns` entry -- then
    # recompute the derived stats a filtering caller stripped, from the rows
    # actually being written. Without the backfill the stream omits
    # `geometry_types`, which GeoParquet 1.1 requires and which DuckDB refuses to
    # read a file without; without the prune the orphan entry has none either.
    if original_metadata:
        original_metadata = prune_geo_metadata_to_columns(original_metadata, table.column_names)
        original_metadata = backfill_derived_stats(original_metadata, table)
        table = apply_metadata_to_table(table, original_metadata)

    write_arrow_stream(table)
    return table


def _write_file_output(
    con: duckdb.DuckDBPyConnection,
    query: str,
    output_path: str,
    original_metadata: dict | None,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    verbose: bool,
    profile: str | None,
    custom_metadata: dict | None,
    geoparquet_version: str | None,
    extra_kv_metadata: dict[str, str] | None = None,
    memory_limit: str | None = None,
) -> None:
    """Write output to Parquet file."""
    # Auto-detect version from input metadata if not explicitly provided
    if geoparquet_version is None:
        geoparquet_version = detect_version_for_output(original_metadata)

    write_parquet_with_metadata(
        con,
        query,
        output_path,
        original_metadata=original_metadata,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        custom_metadata=custom_metadata,
        verbose=verbose,
        profile=profile,
        geoparquet_version=geoparquet_version,
        extra_kv_metadata=extra_kv_metadata,
        memory_limit=memory_limit,
    )


def _detect_transform_source_crs(
    input_path: str, metadata: dict | None, is_stream: bool
) -> str | None:
    """Detect the input's CRS as an ``ST_Transform`` source string, or ``None``.

    Streamed input carries its CRS in the Arrow ``geo`` schema metadata; file
    input is read directly. Best-effort: detection runs for every caller (incl.
    ones whose query fn ignores CRS), so a failure must never break the transform.
    ``None`` means CRS84/CRS-less input (no reprojection needed).
    """
    from geoparquet_io.core.crs_utils import (
        crs_string_from_geo_meta,
        parse_geo_metadata_from_schema,
        source_crs_string,
    )

    try:
        if is_stream:
            return crs_string_from_geo_meta(parse_geo_metadata_from_schema(metadata), None)
        return source_crs_string(input_path)
    except Exception:
        return None


def _call_transform_query_fn(transform_query_fn, source, con, source_crs):
    """Call the query builder, passing ``source_crs`` only if it accepts it.

    Keeps the ``(source, con)`` contract for existing callers (sort, bbox, …)
    while letting CRS-aware builders opt in with a third ``source_crs`` parameter.
    """
    import inspect

    try:
        params = inspect.signature(transform_query_fn).parameters
    except (TypeError, ValueError):
        return transform_query_fn(source, con)
    if len(params) >= 3:
        return transform_query_fn(source, con, source_crs)
    return transform_query_fn(source, con)


def execute_transform(
    input_path: str,
    output_path: str | None,
    transform_query_fn: Callable[..., str],
    verbose: bool = False,
    dry_run: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    custom_metadata: dict | None = None,
    geoparquet_version: str | None = None,
    extra_kv_metadata: dict[str, str] | None = None,
    memory_limit: str | None = None,
) -> pa.Table | None:
    """
    Execute a transformation with unified streaming/file I/O.

    This is a high-level helper that handles the full input->transform->output
    pipeline for both file and streaming modes.

    Args:
        input_path: Input path (file or "-" for stdin)
        output_path: Output path (file, "-" for stdout, or None for auto-detect)
        transform_query_fn: Callable(source_ref, con) -> SQL query string
        verbose: Whether to print verbose output
        dry_run: If True, print query without executing
        compression: Compression for file output
        compression_level: Compression level for file output
        row_group_size_mb: Row group size for file output
        row_group_rows: Row group rows for file output
        profile: AWS profile for remote I/O
        custom_metadata: Optional dict with custom metadata
        geoparquet_version: GeoParquet version to write
        memory_limit: DuckDB memory limit for file writes (e.g., '2GB', '512MB');
            ignored for stream output

    Returns:
        Table if streaming output, None if file output or dry_run

    Example:
        def make_query(source, con):
            return f"SELECT *, bbox_col FROM {source}"

        execute_transform("input.parquet", None, make_query, verbose=True)
    """
    from geoparquet_io.core.logging_config import progress

    # Suppress verbose when streaming to stdout (would corrupt the stream)
    if should_stream_output(output_path):
        verbose = False

    with open_input(input_path, verbose=verbose) as (source, metadata, is_stream, con):
        # Detect the input's CRS once so a transform-aware query fn can reproject
        # non-CRS84 input (e.g. lon/lat grid keying, #525). Streamed input is an
        # Arrow IPC stream, so its CRS comes from the schema metadata, not a file.
        source_crs = _detect_transform_source_crs(input_path, metadata, is_stream)
        query = _call_transform_query_fn(transform_query_fn, source, con, source_crs)

        if dry_run:
            warn("\n=== DRY RUN MODE - SQL that would be executed ===\n")
            progress(query)
            return None

        # Execute and write output
        return write_output(
            con,
            query,
            output_path,
            original_metadata=metadata,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
            custom_metadata=custom_metadata,
            geoparquet_version=geoparquet_version,
            extra_kv_metadata=extra_kv_metadata,
            memory_limit=memory_limit,
        )
