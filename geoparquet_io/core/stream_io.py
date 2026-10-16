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

import json
from collections.abc import Callable
from contextlib import contextmanager
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa

from geoparquet_io.core.arrow_geo_metadata import table_geometry_info
from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.crs_utils import (
    crs_string_from_geo_meta,
    entry_crs_is_unknown,
    parse_geo_metadata_from_schema,
    source_crs_string,
)
from geoparquet_io.core.duckdb_utils import (
    get_duckdb_connection,
    quote_identifier,
    register_arrow,
    sql_path,
)
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geo_metadata import (
    backfill_derived_stats,
    carried_geometry_column,
    gate_carried_covering,
    prune_geo_metadata_to_columns,
    sanitized_carried_geo,
)
from geoparquet_io.core.geometry_detection import native_geometry_types_from_query
from geoparquet_io.core.logging_config import progress, warn
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
from geoparquet_io.core.write_funnels import (
    collect_nonplanar_edges,
    table_nonplanar_edges,
    write_parquet_with_metadata,
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
    *,
    stream_output: bool = False,
) -> Iterator[tuple[str, dict | None, bool, duckdb.DuckDBPyConnection]]:
    """
    Open input source (file or stream) and prepare for DuckDB processing.

    For files: Returns the file path for direct DuckDB query
    For streams: Reads Arrow IPC, registers in DuckDB, returns table name

    Args:
        path: Input path (file path or "-" for stdin)
        con: Optional existing DuckDB connection (one will be created if None)
        verbose: Whether to print verbose output
        stream_output: Carry native field descriptions for an IPC producer

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
        yield from _open_file_input(path, con, verbose, stream_output=stream_output)


def _open_stdin_input(
    con: duckdb.DuckDBPyConnection | None,
    verbose: bool,
) -> Iterator[tuple[str, dict | None, bool, duckdb.DuckDBPyConnection]]:
    """Handle streaming input from stdin."""
    import geoarrow.pyarrow  # noqa: F401 - register IPC field types before reading

    table = read_arrow_stream()
    metadata = _stream_input_metadata(table)

    created_connection = con is None
    if created_connection:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)

    try:
        # Register the Arrow table for SQL queries
        register_arrow(con, "input_stream", table)

        # Find geometry column and create view with proper geometry type
        geom_col = find_geometry_column_from_metadata(metadata) or find_geometry_column_from_table(
            table
        )
        source_ref = _create_view_with_geometry(con, "input_stream", geom_col)

        yield source_ref, metadata, True, con
    finally:
        if created_connection:
            con.close()


def _open_file_input(
    path: str,
    con: duckdb.DuckDBPyConnection | None,
    verbose: bool,
    *,
    stream_output: bool = False,
) -> Iterator[tuple[str, dict | None, bool, duckdb.DuckDBPyConnection]]:
    """Handle file-based input."""
    import geoarrow.pyarrow  # noqa: F401 - retain native geo field descriptions

    raw_url = resolve_file_url(path, verbose=verbose)
    file_metadata, schema = get_parquet_metadata(path, verbose=verbose)

    created_connection = con is None
    if created_connection:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(path))

    try:
        source = f"read_parquet({sql_path(raw_url)})"
        if stream_output:
            file_metadata = _file_stream_metadata(con, source, path, schema, file_metadata)
        yield source, file_metadata, False, con
    finally:
        if created_connection:
            con.close()


def _file_stream_metadata(con, source, path, schema, metadata) -> dict:
    """Expose a file's field descriptions only when it becomes an IPC stream."""
    if isinstance(schema, pa.Schema):
        described = pa.Table.from_batches([], schema=schema)
    else:
        # The remote schema adapter has no Arrow field types. This zero-row
        # result carries each CRS, and the file reader supplies the edges
        # DuckDB does not understand. No source geometry rows are materialized.
        described = con.execute(f"SELECT * FROM {source} LIMIT 0").arrow().read_all()
        for name, edges in collect_nonplanar_edges(path).items():
            described = apply_geoarrow_extension_type(described, name, edges=edges)
    return _stream_input_metadata(described.replace_schema_metadata(metadata))


def _stream_input_metadata(table: pa.Table) -> dict:
    """Carry every input geometry's CRS and resolved edges before registration.

    The I/O interface passes KV metadata alongside a DuckDB source, whose Arrow
    field types are gone. Fill the description from those fields once, using
    the same block-over-type edge authority as the table write and repair paths.
    """
    metadata = dict(table.schema.metadata or {})
    geo = sanitized_carried_geo(metadata)
    geometry_info = table_geometry_info(table, None, geo)
    if geometry_info is None:
        return metadata
    inferred_columns = geometry_info["metadata"]
    primary = carried_geometry_column(geo, inferred_columns)
    if primary not in inferred_columns:
        primary = next(iter(inferred_columns))
    edges_by_col = table_nonplanar_edges(table, primary, None)
    columns = {}
    for name, inferred in inferred_columns.items():
        column = {"encoding": "WKB", **inferred, **geo.get("columns", {}).get(name, {})}
        # Normalize the declaration with the shared reader, so an unknown value
        # cannot become a non-planar model merely by crossing a pipe.
        column["edges"] = edges_by_col.get(name, "planar")
        columns[name] = column
    metadata[b"geo"] = json.dumps(
        {
            **geo,
            "version": geo.get("version", "2.0.0"),
            "primary_column": primary,
            "columns": columns,
        }
    ).encode("utf-8")
    return metadata


def _wrap_query_with_wkb_conversion(
    query: str,
    geometry_column: str | list[str] | None,
) -> str:
    """
    Wrap query to convert DuckDB geometry back to WKB for Arrow export.

    DuckDB's Arrow export returns geometry in DuckDB's native format,
    not WKB. This wraps the query to convert geometry back to WKB using ST_AsWKB.

    Args:
        query: Original SQL query
        geometry_column: Geometry column name or names, or None to skip conversion

    Returns:
        Query wrapped with WKB conversion if needed
    """
    if not geometry_column:
        return query

    columns = [geometry_column] if isinstance(geometry_column, str) else geometry_column
    replacements = ", ".join(
        f"ST_AsWKB({quote_identifier(name)}) AS {quote_identifier(name)}" for name in columns
    )
    return f"""
        WITH __stream_source AS ({query})
        SELECT * REPLACE ({replacements})
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
    native_columns = native_geometry_types_from_query(con, query)
    stream_query = _wrap_query_with_wkb_conversion(query, list(native_columns))

    result = con.execute(stream_query)
    table = result.arrow().read_all()

    # Convert WKB binary to geoarrow extension type for streaming
    # This enables native geometry performance in downstream operations
    columns = sanitized_carried_geo(original_metadata).get("columns", {})
    geometry_columns = dict.fromkeys([*native_columns, *columns, geometry_column])
    for name in geometry_columns:
        if name is None or name not in table.column_names:
            continue
        crs = _extract_crs_from_metadata(original_metadata, name)
        if entry_crs_is_unknown(columns.get(name)):
            crs = "srid:0"
        edges = columns.get(name, {}).get("edges", "planar")
        table = apply_geoarrow_extension_type(table, name, crs, edges=edges)

    # Apply metadata to output table, the way the file path does (#722):
    # first drop entries for columns a projection removed -- a dropped secondary
    # geometry column would otherwise leave an orphan `geo.columns` entry -- then
    # recompute the derived stats a filtering caller stripped, from the rows
    # actually being written. Without the backfill the stream omits
    # `geometry_types`, which GeoParquet 1.1 requires and which DuckDB refuses to
    # read a file without; without the prune the orphan entry has none either.
    # ... and then run the two covering gates every file write runs. A stream is
    # read and persisted exactly like a file gpio wrote, so a covering the
    # pruning has just emptied of its bbox member -- or one declared over a
    # struct the spec forbids -- is no more writable here than there (#1172).
    if original_metadata:
        original_metadata = prune_geo_metadata_to_columns(original_metadata, table.column_names)
        original_metadata = backfill_derived_stats(original_metadata, table)
        original_metadata = gate_carried_covering(original_metadata, table.schema)
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
    """Detect the input's CRS as an ``ST_Transform`` source, or ``None``.

    Streamed input carries its CRS in the Arrow ``geo`` schema metadata; file
    input is read directly. ``None`` means lon/lat or CRS-less input (no
    reprojection needed). A CRS that cannot be resolved raises
    :class:`crs_utils.UnresolvableCRSError` rather than being keyed as lon/lat
    (#1258), so this runs only for a query fn that transforms.
    """
    if is_stream:
        return crs_string_from_geo_meta(parse_geo_metadata_from_schema(metadata), None)
    return source_crs_string(input_path)


def _wants_source_crs(transform_query_fn) -> bool:
    """True when the query builder takes a third ``source_crs`` parameter.

    Keeps the ``(source, con)`` contract for existing callers (sort, bbox, ...)
    while letting CRS-aware builders opt in.
    """
    import inspect

    try:
        params = inspect.signature(transform_query_fn).parameters
    except (TypeError, ValueError):
        return False
    return len(params) >= 3


def _build_transform_query(transform_query_fn, source, con, input_path, metadata, is_stream):
    """Call the query builder, resolving the input CRS only when it takes one."""
    if not _wants_source_crs(transform_query_fn):
        return transform_query_fn(source, con)
    source_crs = _detect_transform_source_crs(input_path, metadata, is_stream)
    return transform_query_fn(source, con, source_crs)


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

    # Suppress verbose when streaming to stdout (would corrupt the stream)
    if should_stream_output(output_path):
        verbose = False

    with open_input(
        input_path, verbose=verbose, stream_output=should_stream_output(output_path)
    ) as (source, metadata, is_stream, con):
        # Detect the input's CRS once so a transform-aware query fn can reproject
        # non-CRS84 input (e.g. lon/lat grid keying, #525). Streamed input is an
        # Arrow IPC stream, so its CRS comes from the schema metadata, not a file.
        query = _build_transform_query(
            transform_query_fn, source, con, input_path, metadata, is_stream
        )

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
