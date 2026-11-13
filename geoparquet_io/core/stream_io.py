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
from uuid import uuid4

import pyarrow as pa

from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.crs_utils import (
    crs_of,
    entry_crs_is_unknown,
    transform_source_crs,
)
from geoparquet_io.core.duckdb_utils import (
    _wrap_query_with_wkb_conversion,
    describe_query,
    get_duckdb_connection,
    query_schema,
    quote_identifier,
    sql_path,
)
from geoparquet_io.core.edges import declared_edges
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geo_metadata import (
    QuerySource,
    backfill_derived_stats,
    compose_carried_geo,
    prune_geo_metadata_to_columns,
    sanitized_carried_geo,
    strip_derived_stats,
)
from geoparquet_io.core.geometry_detection import (
    _query_with_native_geometry,
    create_geometry_view,
    native_geometry_types_from_query,
    primary_geometry_column,
)
from geoparquet_io.core.logging_config import progress, warn
from geoparquet_io.core.parquet_writer import (
    WriteOptions,
    resolve_output_geoparquet_version,
)
from geoparquet_io.core.remote import needs_httpfs
from geoparquet_io.core.streaming import (
    STREAM_BATCH_ROWS,
    apply_geoarrow_extension_type,
    apply_metadata_to_table,
    is_stdin,
    should_stream_output,
    validate_output,
    write_arrow_stream,
)
from geoparquet_io.core.write_funnels import (
    _stream_input_metadata,
    collect_nonplanar_edges,
    declared_nonplanar_edges,
    read_stdin_to_temp_file,
    write_parquet_with_metadata,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    import duckdb


@contextmanager
def open_input(
    path: str,
    con: duckdb.DuckDBPyConnection | None = None,
    verbose: bool = False,
    *,
    stream_output: bool = False,
    preserve_wkb: bool = False,
) -> Iterator[tuple[str, dict | None, bool, duckdb.DuckDBPyConnection]]:
    """
    Open input source (file or stream) and prepare for DuckDB processing.

    For files: Returns the file path for direct DuckDB query
    For streams: Spools IPC batches to scratch Parquet for replayable DuckDB reads

    Args:
        path: Input path (file path or "-" for stdin)
        con: Optional existing DuckDB connection (one will be created if None)
        verbose: Whether to print verbose output
        stream_output: Carry native field descriptions for an IPC producer
        preserve_wkb: Keep stream WKB as stored bytes for operations such as sorting

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
        yield from _open_stdin_input(con, verbose, preserve_wkb=preserve_wkb)
    else:
        yield from _open_file_input(path, con, verbose, stream_output=stream_output)


def _open_stdin_input(
    con: duckdb.DuckDBPyConnection | None,
    verbose: bool,
    *,
    preserve_wkb: bool = False,
) -> Iterator[tuple[str, dict | None, bool, duckdb.DuckDBPyConnection]]:
    """Handle streaming input from stdin."""
    from geoparquet_io.core.file_utils import unlink_released_file

    path = read_stdin_to_temp_file(verbose)
    try:
        for source, metadata, _, connection in _open_file_input(
            path, con, verbose, stream_output=True
        ):
            geom_col = primary_geometry_column(
                describe_query(connection, f"SELECT * FROM {source}"),
                geo=sanitized_carried_geo(metadata),
            )
            if geom_col and not preserve_wkb:
                source = create_geometry_view(connection, source, geom_col)
            yield source, metadata, True, connection
    finally:
        unlink_released_file(path)


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
        if stream_output:
            from geoparquet_io.core.memory_limits import open_bounded_connection

            con = open_bounded_connection(
                load_spatial=True, load_httpfs=needs_httpfs(path), preserve_insertion_order=True
            )
        else:
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
        described = pa.Table.from_batches([], schema=query_schema(con, f"SELECT * FROM {source}"))
        for name, edges in collect_nonplanar_edges(path).items():
            described = apply_geoarrow_extension_type(described, name, edges=edges)
    return _stream_input_metadata(described.replace_schema_metadata(metadata))


def write_output(
    con: duckdb.DuckDBPyConnection,
    query: str,
    output_path: str | None,
    original_metadata: dict | None = None,
    geometry_column: str | None = None,
    compression: str | None = None,
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    verbose: bool = False,
    profile: str | None = None,
    custom_metadata: dict | None = None,
    geoparquet_version: str | None = None,
    extra_kv_metadata: dict[str, str] | None = None,
    memory_limit: str | None = None,
    *,
    preserve_wkb: bool = False,
    input_file: str | None = None,
    invalidate_derived_stats: bool = False,
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
        Zero-row output-schema table after streaming, None if file output

    Raises:
        StreamingError: If no output and stdout is a terminal
    """
    validate_output(output_path)

    output_schema = describe_query(con, query)
    original_metadata = prune_geo_metadata_to_columns(
        original_metadata, [name for name, _ in output_schema], repoint_primary=True
    )
    carried = sanitized_carried_geo(original_metadata)
    geometry_column = primary_geometry_column(
        output_schema,
        geo=carried,
        preferred=geometry_column,
    )

    if should_stream_output(output_path):
        if invalidate_derived_stats:
            original_metadata = strip_derived_stats(original_metadata)
        # --write-memory governs final Parquet output; internal stats snapshots
        # follow the connection's existing memory and spill policy.
        if memory_limit is not None:
            warn(
                "--write-memory is ignored when streaming to stdout "
                "(it only applies to file outputs)."
            )
        return _write_stream_output(
            con,
            query,
            original_metadata,
            geometry_column,
            custom_metadata,
            preserve_wkb=preserve_wkb,
        )
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
            geometry_column=geometry_column,
            input_file=input_file,
            invalidate_derived_stats=invalidate_derived_stats,
        )
        return None


def _write_stream_output(
    con: duckdb.DuckDBPyConnection,
    query: str,
    original_metadata: dict | None,
    geometry_column: str | None,
    custom_metadata: dict | None = None,
    *,
    preserve_wkb: bool = False,
) -> pa.Table:
    """Write output as Arrow IPC stream to stdout.

    Uses geoarrow extension types for streaming, which enables:
    - Native geometry performance in downstream commands
    - CRS preservation through the pipeline
    - Automatic native Parquet geometry when written to file
    """
    carried = sanitized_carried_geo(original_metadata)

    # Convert geometry to WKB (DuckDB's Arrow export uses native format)
    native_columns = native_geometry_types_from_query(con, query)
    stream_query = _wrap_query_with_wkb_conversion(query, list(native_columns))

    # Resolve field metadata without holding data. Stats and rows must describe
    # the same selection even for random(), sampling and unordered LIMIT.
    schema = query_schema(con, stream_query)
    if not carried:
        description = schema.empty_table().replace_schema_metadata(original_metadata)
        for name in dict.fromkeys([*native_columns, geometry_column]):
            if name is not None and name in schema.names:
                description = apply_geoarrow_extension_type(description, name, None)
        original_metadata = _stream_input_metadata(
            description,
            version=resolve_output_geoparquet_version(
                None,
                original_metadata=original_metadata,
                table=description.select(list(native_columns)),
            ),
        )
        carried = sanitized_carried_geo(original_metadata)
    needs_stats = any(
        name in schema.names and (not entry.get("geometry_types") or "bbox" not in entry)
        for name, entry in carried.get("columns", {}).items()
        if isinstance(entry, dict)
    )
    with _stream_selection(con, query, needs_stats) as query:
        table = pa.Table.from_batches([], schema=schema)

        # Convert WKB binary to geoarrow extension type for streaming
        # This enables native geometry performance in downstream operations
        columns = carried.get("columns", {})
        geometry_columns = dict.fromkeys([*native_columns, *columns, geometry_column])
        column_models = {}
        for name in geometry_columns:
            if name is None or name not in table.column_names:
                continue
            crs = crs_of(table.replace_schema_metadata(original_metadata), geometry_column=name)
            if entry_crs_is_unknown(columns.get(name)):
                crs = "srid:0"
            entry = columns.get(name)
            edges = declared_edges(entry)
            column_models[name] = (crs, edges)
            table = apply_geoarrow_extension_type(
                table, name, crs, edges=edges, preserve_wkb=preserve_wkb
            )

        # Apply metadata to output table, the way the file path does (#722):
        # first drop entries for columns a projection removed -- a dropped secondary
        # geometry column would otherwise leave an orphan `geo.columns` entry -- then
        # recompute the derived stats a filtering caller stripped, from the rows
        # actually being written. Without the backfill the stream omits
        # `geometry_types`, which GeoParquet 1.1 requires and which DuckDB refuses to
        # read a file without; without the prune the orphan entry has none either.
        # ... and then compose the block as every file write does. A stream is
        # read and persisted exactly like a file gpio wrote, so a covering the
        # pruning has just emptied of its bbox member -- or one declared over a
        # struct the spec forbids -- is no more writable here than there (#1172),
        # and a 1.0 input upgrades and declares its bbox column as a file does.
        if original_metadata:
            original_metadata = backfill_derived_stats(
                original_metadata,
                QuerySource(
                    con, _query_with_native_geometry(con, query, columns, native_output=True)
                ),
            )
            # A stream takes no version option: auto mode, the facade's answer.
            version = resolve_output_geoparquet_version(None, original_metadata=original_metadata)
            original_metadata = compose_carried_geo(
                original_metadata, table.schema, version, custom_metadata=custom_metadata
            )
            table = apply_metadata_to_table(table, original_metadata)

        output_schema = table.schema
        stream_query = _wrap_query_with_wkb_conversion(query, list(native_columns))
        reader = con.sql(stream_query).to_arrow_reader(batch_size=STREAM_BATCH_ROWS)
        try:

            def batches():
                for batch in reader:
                    chunk = pa.Table.from_batches([batch])
                    for name, (crs, edges) in column_models.items():
                        chunk = apply_geoarrow_extension_type(
                            chunk, name, crs, edges=edges, preserve_wkb=preserve_wkb
                        )
                    yield from chunk.cast(output_schema).to_batches()

            write_arrow_stream(pa.RecordBatchReader.from_batches(output_schema, batches()))
        finally:
            reader.close()
        return table


@contextmanager
def _stream_selection(con, query, needs_stats):
    """Freeze rows once when schema statistics need a replayable selection."""
    if not needs_stats:
        yield query
        return
    snapshot = quote_identifier(f"__gpio_stream_{uuid4().hex}")
    # Keep native geometry for the stats owner; only exported batches need WKB.
    con.execute(f"CREATE TEMP TABLE {snapshot} AS {query}").fetchall()
    try:
        yield f"SELECT * FROM {snapshot}"
    finally:
        con.execute(f"DROP TABLE {snapshot}").fetchall()


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
    *,
    geometry_column: str | None = None,
    input_file: str | None = None,
    invalidate_derived_stats: bool = False,
) -> None:
    """Write output to Parquet file.

    An auto-mode ``geoparquet_version`` (None) is decided by the funnel, from
    ``original_metadata``, through ``parquet_writer.resolve_output_geoparquet_version``.
    """
    write_parquet_with_metadata(
        con,
        query,
        output_path,
        original_metadata=original_metadata,
        geometry_info={"primary": geometry_column} if geometry_column else None,
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
        input_file=input_file,
        invalidate_derived_stats=invalidate_derived_stats,
    )


def _build_transform_query(
    transform_query_fn, source, con, input_path, metadata, is_stream, *, input_aware=False
):
    """Call the query builder under one of its two contracts.

    ``(source, con)`` for a builder that needs only the source (sort, ...).
    With ``input_aware`` the one input-aware contract,
    ``(source, con, source_crs, edges, metadata)``: the input's CRS as an
    ``ST_Transform`` source (``None`` for lon/lat or CRS-less input), its
    ``{column: edges}`` (:func:`input_nonplanar_edges`, so a cell key on stdin
    keys the way the file path does, #1293) and its schema metadata. The CRS is
    resolved only for an input-aware builder, since an unresolvable one raises.
    """
    if not input_aware:
        return transform_query_fn(source, con)
    input_source = (
        query_schema(con, f"SELECT * FROM {source}").with_metadata(metadata)
        if is_stream
        else input_path
    )
    source_crs = transform_source_crs(crs_of(input_source))
    edges = input_nonplanar_edges(input_path, metadata, is_stream)
    return transform_query_fn(source, con, source_crs, edges, metadata)


def input_nonplanar_edges(input_path: str, metadata: dict | None, is_stream: bool) -> dict:
    """``{column: edges}`` for an ``open_input`` input's non-planar columns.

    The one place a streamed cell-key builder learns its edges (#1293): the
    file when there is one, otherwise the stream's own block
    (``write_funnels.declared_nonplanar_edges``). ``_build_transform_query``
    hands it to every input-aware builder as its ``edges`` argument
    (``index_ops.add_index_streaming``, the one driver for h3, s2, a5 and
    quadkey, is the one that keys from it).
    """
    return declared_nonplanar_edges(None if is_stream else input_path, metadata)


def execute_transform(
    input_path: str,
    output_path: str | None,
    transform_query_fn: Callable[..., str],
    verbose: bool = False,
    dry_run: bool = False,
    compression: str | None = None,
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    custom_metadata: dict | None = None,
    geoparquet_version: str | None = None,
    extra_kv_metadata: dict[str, str] | None = None,
    memory_limit: str | None = None,
    input_aware: bool = False,
) -> pa.Table | None:
    """
    Execute a transformation with unified streaming/file I/O.

    This is a high-level helper that handles the full input->transform->output
    pipeline for both file and streaming modes.

    Args:
        input_path: Input path (file or "-" for stdin)
        output_path: Output path (file, "-" for stdout, or None for auto-detect)
        transform_query_fn: Callable(source_ref, con) -> SQL query string, or
            with ``input_aware`` Callable(source_ref, con, source_crs, edges,
            metadata) (see ``_build_transform_query``)
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
        input_aware: Call ``transform_query_fn`` with the input's CRS, edges
            and metadata as well

    Returns:
        Zero-row output-schema table after streaming, None if file output or dry_run

    Example:
        def make_query(source, con):
            return f"SELECT *, bbox_col FROM {source}"

        execute_transform("input.parquet", None, make_query, verbose=True)
    """

    # Suppress verbose when streaming to stdout (would corrupt the stream)
    if should_stream_output(output_path):
        verbose = False

    write = WriteOptions.pick(locals())
    with open_input(
        input_path, verbose=verbose, stream_output=should_stream_output(output_path)
    ) as (source, metadata, is_stream, con):
        # Detect the input's CRS once so a transform-aware query fn can reproject
        # non-CRS84 input (e.g. lon/lat grid keying, #525). Streamed input is an
        # Arrow IPC stream, so its CRS comes from the schema metadata, not a file.
        query = _build_transform_query(
            transform_query_fn,
            source,
            con,
            input_path,
            metadata,
            is_stream,
            input_aware=input_aware,
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
            verbose=verbose,
            custom_metadata=custom_metadata,
            extra_kv_metadata=extra_kv_metadata,
            **write.funnel_kwargs(),
        )
