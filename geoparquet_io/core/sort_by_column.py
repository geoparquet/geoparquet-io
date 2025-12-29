#!/usr/bin/env python3

from __future__ import annotations

import click
import duckdb
import pyarrow as pa

from geoparquet_io.core.common import (
    get_duckdb_connection,
    get_parquet_metadata,
    get_remote_error_hint,
    is_remote_url,
    needs_httpfs,
    safe_file_url,
    setup_aws_profile_if_needed,
    show_remote_read_message,
    validate_profile_for_urls,
    write_parquet_with_metadata,
)
from geoparquet_io.core.duckdb_metadata import get_usable_columns
from geoparquet_io.core.logging_config import configure_verbose, debug, success
from geoparquet_io.core.stream_io import execute_transform
from geoparquet_io.core.streaming import is_stdin, should_stream_output


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
    for col in column_list:
        if col not in table.column_names:
            raise ValueError(
                f"Column '{col}' not found in table. "
                f"Available columns: {', '.join(table.column_names)}"
            )

    # Register table and execute query
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.register("__input_table", table)

        # Build ORDER BY clause
        direction = " DESC" if descending else ""
        order_clause = ", ".join(f'"{col}"{direction}' for col in column_list)

        query = f"SELECT * FROM __input_table ORDER BY {order_clause}"
        result = con.execute(query).fetch_arrow_table()

        # Preserve metadata
        if table.schema.metadata:
            result = result.replace_schema_metadata(table.schema.metadata)

        return result
    finally:
        con.close()


def sort_by_column(
    input_parquet: str,
    output_parquet: str | None = None,
    columns: str | list[str] = "",
    descending: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
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
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
    """
    configure_verbose(verbose)

    # Parse comma-separated columns into list
    if isinstance(columns, str):
        column_list = [c.strip() for c in columns.split(",")]
    else:
        column_list = list(columns)

    if not column_list:
        raise click.ClickException("At least one column name must be specified")

    # Check for streaming mode (stdin input or stdout output)
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    if is_streaming:
        _sort_by_column_streaming(
            input_parquet,
            output_parquet,
            column_list,
            descending,
            verbose,
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            profile,
            geoparquet_version,
        )
        return

    # File-based mode
    # Validate profile is only used with S3
    validate_profile_for_urls(profile, input_parquet, output_parquet)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, input_parquet, output_parquet)

    # Show remote read message
    show_remote_read_message(input_parquet, verbose)

    safe_url = safe_file_url(input_parquet, verbose)

    # Get metadata from original file
    metadata, schema = get_parquet_metadata(input_parquet, verbose)

    # Validate that specified columns exist - use get_usable_columns for actual DuckDB column names
    usable_cols = get_usable_columns(safe_url)
    existing_columns = [c["name"] for c in usable_cols]
    for col in column_list:
        if col not in existing_columns:
            raise click.ClickException(
                f"Column '{col}' not found in input file. "
                f"Available columns: {', '.join(existing_columns)}"
            )

    if verbose:
        debug(f"Sorting by column(s): {', '.join(column_list)}")
        debug(f"Sort direction: {'descending' if descending else 'ascending'}")

    # Create DuckDB connection with httpfs if needed
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))

    # Build ORDER BY clause
    direction = " DESC" if descending else ""
    order_clause = ", ".join(f'"{col}"{direction}' for col in column_list)

    # Build SELECT query
    order_query = f"""
        SELECT *
        FROM '{safe_url}'
        ORDER BY {order_clause}
    """

    if verbose:
        debug(f"Sort query: {order_query}")

    try:
        # Use the common write function with metadata preservation
        write_parquet_with_metadata(
            con,
            order_query,
            output_parquet,
            original_metadata=metadata,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
        )

        success(f"Sorted by {', '.join(column_list)} to: {output_parquet}")

    except duckdb.IOException as e:
        if is_remote_url(input_parquet):
            hints = get_remote_error_hint(str(e), input_parquet)
            raise click.ClickException(
                f"Failed to read remote file.\n\n{hints}\n\nOriginal error: {str(e)}"
            ) from e
        raise
    finally:
        con.close()


def _sort_by_column_streaming(
    input_path: str,
    output_path: str | None,
    column_list: list[str],
    descending: bool,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
) -> None:
    """Handle streaming input/output for sort_by_column."""
    # Suppress verbose when streaming to stdout
    if should_stream_output(output_path):
        verbose = False

    def make_query(source: str, con) -> str:
        """Build the sort query for streaming source."""
        # Get column names from query result
        sample = con.execute(f"SELECT * FROM {source} LIMIT 0").description
        col_names = [col[0] for col in sample]

        # Validate columns exist
        for col in column_list:
            if col not in col_names:
                raise click.ClickException(
                    f"Column '{col}' not found in input. Available columns: {', '.join(col_names)}"
                )

        # Build ORDER BY clause
        direction = " DESC" if descending else ""
        order_clause = ", ".join(f'"{col}"{direction}' for col in column_list)

        if verbose:
            debug(f"Sorting by column(s): {', '.join(column_list)}")

        return f"SELECT * FROM {source} ORDER BY {order_clause}"

    execute_transform(
        input_path,
        output_path,
        make_query,
        verbose=verbose,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        profile=profile,
        geoparquet_version=geoparquet_version,
    )

    if not should_stream_output(output_path):
        success(f"Sorted by {', '.join(column_list)} to: {output_path}")


if __name__ == "__main__":
    sort_by_column()
