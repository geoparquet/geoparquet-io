#!/usr/bin/env python3

from __future__ import annotations

import pyarrow as pa

from geoparquet_io.core.column_selection import resolve_columns_against_schema
from geoparquet_io.core.duckdb_utils import (
    describe_query,
    get_duckdb_connection,
    quote_identifier,
    register_arrow,
    uses_aws_profile,
)
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.logging_config import configure_verbose, progress
from geoparquet_io.core.parquet_writer import WriteOptions, resolve_row_group_rows
from geoparquet_io.core.sort_common import run_sort


def sort_by_column_table(
    table: pa.Table,
    columns: str | list[str],
    descending: bool = False,
) -> pa.Table:
    """
    Sort an Arrow Table by specified column(s).

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table
        columns: Column name or list of column names to sort by
        descending: Sort in descending order (default: ascending)

    Returns:
        New table with rows sorted by specified columns
    """
    # Parse columns
    if isinstance(columns, str):
        column_list = [c.strip() for c in columns.split(",")]
    else:
        column_list = list(columns)

    if not column_list:
        raise ValueError("At least one column name must be specified")

    # Validate columns exist
    column_list = resolve_columns_against_schema(column_list, table.column_names, "columns")

    # Register table and execute query
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        registered = register_arrow(con, "__input_table", table)

        # Build ORDER BY clause
        direction = " DESC" if descending else ""
        order_clause = ", ".join(f"{quote_identifier(col)}{direction}" for col in column_list)

        query = f"SELECT * FROM __input_table ORDER BY {order_clause}"
        result = con.sql(query).arrow().read_all()

        # Preserve metadata
        if table.schema.metadata:
            result = result.replace_schema_metadata(table.schema.metadata)

        result = registered.restore_edges(result)
        return result
    finally:
        con.close()


@uses_aws_profile
def sort_by_column(
    input_parquet: str,
    output_parquet: str | None = None,
    columns: str | list[str] = "",
    descending: bool = False,
    verbose: bool = False,
    compression: str | None = None,
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
    memory_limit: str | None = None,
    allow_schema_diff: bool = False,
) -> None:
    """
    Sort a GeoParquet file by specified column(s).

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    Reorders rows in the file based on one or more column values, which can
    improve query performance for filtering operations on those columns.

    Args:
        input_parquet: Path to input GeoParquet file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
        columns: Column name or comma-separated list of column names to sort by
        descending: Sort in descending order (default: ascending)
        verbose: Print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group. When neither this
            nor ``row_group_size_mb`` is given, the sort default
            (``DEFAULT_ROW_GROUP_ROWS``) applies.
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        memory_limit: DuckDB memory limit for the write (e.g., '2GB', '512MB')
        allow_schema_diff: For multi-file input, read the union of the files'
            columns (DuckDB ``union_by_name``) instead of the first file's
            schema, which silently drops any column the others add
    """
    configure_verbose(verbose)
    row_group_rows = resolve_row_group_rows(row_group_rows, row_group_size_mb)

    # Parse comma-separated columns into list
    if isinstance(columns, str):
        column_list = [c.strip() for c in columns.split(",")]
    else:
        column_list = list(columns)

    if not column_list:
        raise InvalidParameterError("columns", "at least one column name must be specified")

    def key(con, source, metadata, working):
        return resolve_columns_against_schema(
            column_list,
            [name for name, _ in describe_query(con, f"SELECT * FROM {source}")],
            "columns",
        )

    def query(con, source, resolved, verbose, working):
        direction = " DESC" if descending else ""
        order_clause = ", ".join(f"{quote_identifier(col)}{direction}" for col in resolved)
        progress(f"Sorting by {', '.join(resolved)}...")
        return f"SELECT * FROM {source} ORDER BY {order_clause}", True

    run_sort(
        input_parquet,
        output_parquet,
        command="sort column",
        label="column",
        effective_version=None,
        build_key=key,
        build_query=query,
        geometry_column=None,
        add_bbox_flag=False,
        verbose=verbose,
        write=WriteOptions.pick(locals()),
        overwrite=overwrite,
        allow_schema_diff=allow_schema_diff,
        success_message=f"Sorted by {', '.join(column_list)}",
    )
