#!/usr/bin/env python3

from __future__ import annotations

import os

import duckdb
import pyarrow as pa

from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.constants import DEFAULT_QUADKEY_COLUMN_NAME
from geoparquet_io.core.duckdb_metadata import get_usable_column_names
from geoparquet_io.core.duckdb_utils import (
    get_duckdb_connection,
    quote_identifier,
    register_arrow,
    sql_path,
    uses_aws_profile,
)
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import (
    handle_output_overwrite,
    is_partition_path,
    resolve_file_url,
)
from geoparquet_io.core.index_ops import ensure_index_column, get_index_spec
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success
from geoparquet_io.core.parquet_writer import WriteOptions, resolve_row_group_rows
from geoparquet_io.core.partition.reader import (
    build_read_parquet_expr,
    raise_for_schema_mismatch,
    resolve_read_path,
)
from geoparquet_io.core.remote import (
    needs_httpfs,
    validate_profile_for_urls,
)
from geoparquet_io.core.stream_io import write_output
from geoparquet_io.core.streaming import is_stdin, read_stdin_to_temp_file, should_stream_output
from geoparquet_io.core.write_funnels import write_parquet_with_metadata


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
        return registered.read_back(con, query, table.schema.metadata)
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

    # Check for streaming mode (stdin input or stdout output)
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    write = WriteOptions.pick(locals())
    if is_streaming:
        _sort_by_quadkey_streaming(
            input_parquet,
            output_parquet,
            quadkey_column_name,
            resolution,
            use_centroid,
            remove_quadkey_column,
            verbose,
            write,
        )
        return

    # File-based mode
    # Check if output file exists and handle overwrite (fixes issue #278)
    handle_output_overwrite(output_parquet, overwrite, input_parquet)

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, input_parquet, output_parquet)

    # A directory has to become the glob over its files before anything reads
    # it (#817). `input_parquet` itself stays raw: it is what messages quote and
    # what the multi-file stats guard below is asked about. The options travel
    # with the path: dropping them left hive keys to DuckDB's auto-detect,
    # which agrees only by coincidence (#867). Resolving first also means an
    # empty directory is named as one, rather than reaching the schema read
    # below and coming back as "Cannot read schema: <dir> ... is a directory".
    actual_input, read_options = resolve_read_path(input_parquet, verbose=verbose)
    hive_input: bool | None = True if read_options.get("hive_partitioning") else None

    missing_error = None
    if quadkey_column_name != DEFAULT_QUADKEY_COLUMN_NAME:
        missing_error = InvalidParameterError(
            "quadkey_column_name",
            f"column '{quadkey_column_name}' not found in input file. "
            f"Run 'gpio add quadkey --quadkey-name {quadkey_column_name}' to add it first.",
        )
    try:
        actual_input, quadkey_column_name, temp_file = ensure_index_column(
            get_index_spec("quadkey"),
            actual_input,
            quadkey_column_name,
            resolution,
            verbose,
            use_centroid,
            profile,
            missing_error=missing_error,
            allow_schema_diff=allow_schema_diff,
            hive_input=hive_input,
        )
    except Exception as exc:
        raise_for_schema_mismatch(exc, input_parquet)
        raise
    if temp_file:
        hive_input = None

    # Get metadata from input file (use actual_input in case we added quadkey)
    read_expr = build_read_parquet_expr(
        actual_input,
        allow_schema_diff=allow_schema_diff,
        hive_input=hive_input,
        verbose=verbose,
    )
    metadata, _ = get_parquet_metadata(actual_input, verbose)

    # Get the columns for building the SELECT clause. Under --allow-schema-diff
    # the read expression carries union_by_name, so the list must describe the
    # read itself rather than the first file -- or the remove-column branch
    # silently drops the very columns the flag was turned on to keep.
    existing_columns = get_usable_column_names(
        actual_input, read_expr=read_expr if allow_schema_diff else None
    )

    # Bound before the try: the `finally` below closes it, and a constructor
    # that throws would otherwise be masked by an UnboundLocalError (#867).
    con = None
    try:
        # Create DuckDB connection
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(actual_input))

        # Build SELECT clause - exclude quadkey if requested
        if remove_quadkey_column:
            select_cols = [
                quote_identifier(col) for col in existing_columns if col != quadkey_column_name
            ]
            select_clause = ", ".join(select_cols)
            progress(f"Sorting by '{quadkey_column_name}' (will be removed from output)")
        else:
            select_clause = "*"
            progress(f"Sorting by '{quadkey_column_name}'")

        # Build sort query
        query = f"""
            SELECT {select_clause}
            FROM {read_expr}
            ORDER BY {quote_identifier(quadkey_column_name)}
        """

        if verbose:
            debug(f"Sort query: {query}")

        # Write output with metadata preservation
        write_parquet_with_metadata(
            con,
            query,
            output_parquet,
            original_metadata=metadata,
            verbose=verbose,
            input_file=input_parquet,
            # `metadata` describes only the first file of a multi-file input
            # (and the auto-add step carried that footer forward), so its
            # bbox/geometry_types under-cover the merged output. Recompute
            # rather than carry (#793).
            invalidate_derived_stats=is_partition_path(input_parquet),
            **write.funnel_kwargs(),
        )

        if remove_quadkey_column:
            success(f"Sorted by quadkey and removed column to: {output_parquet}")
        else:
            success(f"Sorted by quadkey to: {output_parquet}")

    except duckdb.InvalidInputException as e:
        # When the quadkey column already exists there is no auto-add step,
        # so a schema mismatch across the glob first strikes the sort itself.
        raise_for_schema_mismatch(e, input_parquet)
        raise
    finally:
        if con:
            con.close()
        # Clean up temp file if we created one
        if temp_file and os.path.exists(temp_file):
            if verbose:
                debug("Cleaning up temporary quadkey-enriched file...")
            os.remove(temp_file)


def _sort_by_quadkey_streaming(
    input_path: str,
    output_path: str | None,
    quadkey_column_name: str,
    resolution: int,
    use_centroid: bool,
    remove_quadkey_column: bool,
    verbose: bool,
    write: WriteOptions,
) -> None:
    """Handle streaming input/output for sort_by_quadkey."""
    # Suppress verbose when streaming to stdout
    if should_stream_output(output_path):
        verbose = False

    temp_input_file = None
    temp_quadkey_file = None
    try:
        # If reading from stdin, write to temp file first. The shared bridge
        # reconciles the stream's geo metadata with its schema on the way, so
        # the temp file is one DuckDB will actually open (#722).
        if is_stdin(input_path):
            temp_input_file = read_stdin_to_temp_file(verbose)
            working_file = temp_input_file
        else:
            working_file = input_path

        missing_error = None
        if quadkey_column_name != DEFAULT_QUADKEY_COLUMN_NAME:
            missing_error = InvalidParameterError(
                "quadkey_column_name",
                f"column '{quadkey_column_name}' not found in input. "
                f"Use default name 'quadkey' for auto-addition.",
            )
        actual_input, quadkey_column_name, temp_quadkey_file = ensure_index_column(
            get_index_spec("quadkey"),
            working_file,
            quadkey_column_name,
            resolution,
            verbose,
            use_centroid,
            write.profile,
            missing_error=missing_error,
        )

        # Build and execute sort query
        actual_url = resolve_file_url(actual_input, verbose=False)
        metadata, _ = get_parquet_metadata(actual_input, verbose=False)

        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(actual_input))

        existing_columns = get_usable_column_names(actual_input)

        if remove_quadkey_column:
            select_cols = [
                quote_identifier(col) for col in existing_columns if col != quadkey_column_name
            ]
            select_clause = ", ".join(select_cols)
        else:
            select_clause = "*"

        query = f"""
            SELECT {select_clause}
            FROM {sql_path(actual_url)}
            ORDER BY {quote_identifier(quadkey_column_name)}
        """

        if verbose:
            debug(f"Sort query: {query}")

        # Write output
        write_output(
            con,
            query,
            output_path,
            original_metadata=metadata,
            verbose=verbose,
            **write.funnel_kwargs(),
        )

        con.close()

        if not should_stream_output(output_path):
            if remove_quadkey_column:
                success(f"Sorted by quadkey and removed column to: {output_path}")
            else:
                success(f"Sorted by quadkey to: {output_path}")

    finally:
        # Clean up temp files
        if temp_input_file and os.path.exists(temp_input_file):
            os.remove(temp_input_file)
        if temp_quadkey_file and os.path.exists(temp_quadkey_file):
            os.remove(temp_quadkey_file)


if __name__ == "__main__":
    sort_by_quadkey()
