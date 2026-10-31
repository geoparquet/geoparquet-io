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

import pyarrow as pa

from geoparquet_io.core.arrow_geo_metadata import table_geometry_info
from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.crs_utils import (
    crs_string_from_geo_meta,
    entry_crs_is_unknown,
    source_crs_string,
)
from geoparquet_io.core.duckdb_utils import (
    as_wkb_sql,
    describe_query,
    get_duckdb_connection,
    query_schema,
    quote_identifier,
    register_arrow,
    sql_path,
)
from geoparquet_io.core.edges import declared_edges
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geo_metadata import (
    KEEP_CRS,
    ArrowTableSource,
    GeoBlockOptions,
    backfill_derived_stats,
    compose_carried_geo,
    compose_geo_block,
    prune_geo_metadata_to_columns,
    sanitized_carried_geo,
    with_geo_block,
)
from geoparquet_io.core.geometry_detection import (
    create_geometry_view,
    native_geometry_types_from_query,
    primary_geometry_column,
)
from geoparquet_io.core.logging_config import progress, warn
from geoparquet_io.core.parquet_writer import (
    WriteOptions,
    resolve_output_geoparquet_version,
    version_to_describe,
)
from geoparquet_io.core.remote import needs_httpfs
from geoparquet_io.core.streaming import (
    apply_geoarrow_extension_type,
    apply_metadata_to_table,
    is_stdin,
    read_arrow_stream,
    should_stream_output,
    validate_output,
    write_arrow_stream,
)
from geoparquet_io.core.write_funnels import (
    collect_nonplanar_edges,
    declared_nonplanar_edges,
    table_nonplanar_edges,
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
        geom_col = primary_geometry_column(table, geo=sanitized_carried_geo(metadata))
        source_ref = (
            create_geometry_view(con, "input_stream", geom_col, "input_stream_geom")
            if geom_col
            else "input_stream"
        )

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
        described = pa.Table.from_batches([], schema=query_schema(con, f"SELECT * FROM {source}"))
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
    # The owner resolves over the geometry columns' own Arrow fields: a
    # non-geometry column that merely has a standard name cannot win, and every
    # inferred column is declared by the block or GeoArrow-typed, so one answers.
    geometry_fields = pa.schema([table.schema.field(name) for name in inferred_columns])
    primary = primary_geometry_column(geometry_fields, geo=geo)
    # What each column's field says (its CRS, the "not known" geometry types)
    # and the edges resolved block-over-type, as the composer's inputs: the
    # primary's through ``crs``/``stats``/``edges``, every other column's
    # through ``geometry_info``. Whatever the carried block already says wins.
    # Edges are recorded, not mapped, for a description: the write at the other
    # end of the pipe maps them to the output's version, so a 2.0 output keeps
    # an ellipsoidal algorithm.
    declared = inferred_columns[primary]
    carried = (geo.get("columns") or {}).get(primary) or {}
    block = compose_geo_block(
        ArrowTableSource(table),
        GeoBlockOptions(
            geometry_column=primary,
            version=version_to_describe(table.schema),
            describe=True,
            covering=None,
            column_edges=table_nonplanar_edges(table, primary, None),
            crs=declared["crs"] if "crs" in declared and "crs" not in carried else KEEP_CRS,
            stats={primary: {k: v for k, v in declared.items() if k == "geometry_types"}}
            if "geometry_types" not in carried
            else None,
            geometry_info={
                "secondary": [name for name in inferred_columns if name != primary],
                "metadata": inferred_columns,
            },
        ),
    )
    return dict(with_geo_block(table, block).schema.metadata)


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
        f"{as_wkb_sql(quote_identifier(name))} AS {quote_identifier(name)}" for name in columns
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
    col = geometry_column or primary_geometry_column(None, geo=geo_meta)
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
    carried = sanitized_carried_geo(original_metadata)
    if geometry_column is None:
        geometry_column = primary_geometry_column(describe_query(con, query), geo=carried)

    # Convert geometry to WKB (DuckDB's Arrow export uses native format)
    native_columns = native_geometry_types_from_query(con, query)
    stream_query = _wrap_query_with_wkb_conversion(query, list(native_columns))

    result = con.sql(stream_query)
    table = result.arrow().read_all()

    # Convert WKB binary to geoarrow extension type for streaming
    # This enables native geometry performance in downstream operations
    columns = carried.get("columns", {})
    geometry_columns = dict.fromkeys([*native_columns, *columns, geometry_column])
    for name in geometry_columns:
        if name is None or name not in table.column_names:
            continue
        crs = _extract_crs_from_metadata(original_metadata, name)
        if entry_crs_is_unknown(columns.get(name)):
            crs = "srid:0"
        entry = columns.get(name)
        edges = declared_edges(entry)
        table = apply_geoarrow_extension_type(table, name, crs, edges=edges)

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
        original_metadata = prune_geo_metadata_to_columns(original_metadata, table.column_names)
        original_metadata = backfill_derived_stats(original_metadata, table)
        # A stream takes no version option: auto mode, the facade's answer.
        version = resolve_output_geoparquet_version(None, original_metadata=original_metadata)
        original_metadata = compose_carried_geo(original_metadata, table.schema, version)
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
    """Write output to Parquet file.

    An auto-mode ``geoparquet_version`` (None) is decided by the funnel, from
    ``original_metadata``, through ``parquet_writer.resolve_output_geoparquet_version``.
    """
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
        return crs_string_from_geo_meta(sanitized_carried_geo(metadata), None)
    return source_crs_string(input_path)


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
    source_crs = _detect_transform_source_crs(input_path, metadata, is_stream)
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
        Table if streaming output, None if file output or dry_run

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
