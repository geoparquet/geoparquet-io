#!/usr/bin/env python3

import click
import duckdb

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


def sort_by_column(
    input_parquet,
    output_parquet,
    columns,
    descending=False,
    verbose=False,
    compression="ZSTD",
    compression_level=None,
    row_group_size_mb=None,
    row_group_rows=None,
    profile=None,
    geoparquet_version=None,
):
    """
    Sort a GeoParquet file by specified column(s).

    Reorders rows in the file based on one or more column values, which can
    improve query performance for filtering operations on those columns.

    Args:
        input_parquet: Path to input GeoParquet file (local or remote URL)
        output_parquet: Path to output file (local or remote URL)
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
        con.close()
        if is_remote_url(input_parquet):
            hints = get_remote_error_hint(str(e), input_parquet)
            raise click.ClickException(
                f"Failed to read remote file.\n\n{hints}\n\nOriginal error: {str(e)}"
            ) from e
        raise


if __name__ == "__main__":
    sort_by_column()
