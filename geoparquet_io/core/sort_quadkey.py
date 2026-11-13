#!/usr/bin/env python3

from __future__ import annotations

import pyarrow as pa

from geoparquet_io.core.constants import DEFAULT_QUADKEY_COLUMN_NAME
from geoparquet_io.core.duckdb_utils import (
    get_duckdb_connection,
    quote_identifier,
    register_arrow,
    uses_aws_profile,
)
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.index_ops import ensure_index_column, get_index_spec
from geoparquet_io.core.logging_config import configure_verbose, progress
from geoparquet_io.core.parquet_writer import WriteOptions, resolve_row_group_rows
from geoparquet_io.core.partition.reader import resolve_read_path
from geoparquet_io.core.sort_common import run_sort


def sort_by_quadkey_table(
    table: pa.Table,
    quadkey_column_name: str = DEFAULT_QUADKEY_COLUMN_NAME,
    resolution: int = get_index_spec("quadkey").default_resolution,
    use_centroid: bool = False,
    remove_quadkey_column: bool = False,
) -> pa.Table:
    """
    Sort an Arrow Table by quadkey column.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table
        quadkey_column_name: Name of the quadkey column to sort by (default: 'quadkey')
        resolution: Resolution for auto-adding quadkey column (0-23). Default: 13
        use_centroid: Use geometry centroid when auto-adding quadkey column
        remove_quadkey_column: Exclude quadkey column from output after sorting

    Returns:
        New table sorted by quadkey
    """
    working_table, quadkey_column_name, _ = ensure_index_column(
        get_index_spec("quadkey"),
        table,
        quadkey_column_name,
        resolution,
        use_centroid=use_centroid,
    )

    # Sort by quadkey
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        registered = register_arrow(con, "__input_table", working_table)

        if remove_quadkey_column:
            select_cols = [
                quote_identifier(c) for c in working_table.column_names if c != quadkey_column_name
            ]
            select_clause = ", ".join(select_cols)
        else:
            select_clause = "*"

        query = f"SELECT {select_clause} FROM __input_table ORDER BY {quote_identifier(quadkey_column_name)}"
        result = registered.read_back(con, query, table.schema.metadata)
        return result
    finally:
        con.close()


@uses_aws_profile
def sort_by_quadkey(
    input_parquet: str,
    output_parquet: str | None = None,
    quadkey_column_name: str = DEFAULT_QUADKEY_COLUMN_NAME,
    resolution: int = get_index_spec("quadkey").default_resolution,
    use_centroid: bool = False,
    remove_quadkey_column: bool = False,
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
    Sort a GeoParquet file by quadkey column.

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    If the quadkey column doesn't exist and using the default column name, it will
    be auto-added at the specified resolution. If using a custom --quadkey-name and
    the column is missing, an error is raised.

    Args:
        input_parquet: Path to input GeoParquet file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
        quadkey_column_name: Name of the quadkey column to sort by (default: 'quadkey')
        resolution: Resolution for auto-adding quadkey column (0-23). Default: 13
        use_centroid: Use geometry centroid when auto-adding quadkey column
        remove_quadkey_column: Exclude quadkey column from output after sorting
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

    def prepare(path, verbose):
        nonlocal quadkey_column_name
        actual, options = resolve_read_path(path, verbose=verbose)
        missing_error = None
        if quadkey_column_name != DEFAULT_QUADKEY_COLUMN_NAME:
            missing_error = InvalidParameterError(
                "quadkey_column_name",
                f"column '{quadkey_column_name}' not found in input file. "
                f"Run 'gpio add quadkey --quadkey-name {quadkey_column_name}' to add it first.",
            )
        actual, quadkey_column_name, temp = ensure_index_column(
            get_index_spec("quadkey"),
            actual,
            quadkey_column_name,
            resolution,
            verbose,
            use_centroid,
            profile,
            missing_error=missing_error,
            allow_schema_diff=allow_schema_diff,
            hive_input=True if options.get("hive_partitioning") else None,
        )
        return actual, temp

    def key(con, source, metadata, working):
        return quadkey_column_name

    def query(con, source, column, verbose, working):
        select = f"* EXCLUDE ({quote_identifier(column)})" if remove_quadkey_column else "*"
        progress(f"Sorting by '{column}'")
        return f"SELECT {select} FROM {source} ORDER BY {quote_identifier(column)}", True

    run_sort(
        input_parquet,
        output_parquet,
        command="sort quadkey",
        label="quadkey",
        effective_version=None,
        prepare_input=prepare,
        build_key=key,
        build_query=query,
        geometry_column=None,
        add_bbox_flag=False,
        verbose=verbose,
        write=WriteOptions.pick(locals()),
        overwrite=overwrite,
        allow_schema_diff=allow_schema_diff,
        success_message="Sorted by quadkey"
        + (" and removed column" if remove_quadkey_column else ""),
    )
