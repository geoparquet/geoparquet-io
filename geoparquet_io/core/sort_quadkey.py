#!/usr/bin/env python3

from __future__ import annotations

import os
import tempfile
import uuid

import duckdb
import pyarrow as pa

from geoparquet_io.core.add.quadkey import add_quadkey_column, add_quadkey_table
from geoparquet_io.core.common import get_parquet_metadata, write_parquet_with_metadata
from geoparquet_io.core.constants import DEFAULT_QUADKEY_COLUMN_NAME, DEFAULT_QUADKEY_RESOLUTION
from geoparquet_io.core.duckdb_metadata import get_column_names, get_usable_columns
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier, sql_path
from geoparquet_io.core.exceptions import GeoParquetError, InvalidParameterError
from geoparquet_io.core.file_utils import (
    handle_output_overwrite,
    is_partition_path,
    resolve_file_url,
)
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success
from geoparquet_io.core.parquet_writer import resolve_row_group_rows
from geoparquet_io.core.partition.reader import (
    build_read_parquet_expr,
    raise_for_schema_mismatch,
    resolve_read_path,
)
from geoparquet_io.core.remote import (
    needs_httpfs,
    setup_aws_profile_if_needed,
    validate_profile_for_urls,
)
from geoparquet_io.core.sort_by_column import _sortable_columns
from geoparquet_io.core.stream_io import write_output
from geoparquet_io.core.streaming import is_stdin, read_stdin_to_temp_file, should_stream_output


def sort_by_quadkey_table(
    table: pa.Table,
    quadkey_column_name: str = DEFAULT_QUADKEY_COLUMN_NAME,
    resolution: int = DEFAULT_QUADKEY_RESOLUTION,
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
    # Check if quadkey column exists, add if needed
    working_table = table
    if quadkey_column_name not in table.column_names:
        working_table = add_quadkey_table(
            table,
            quadkey_column_name=quadkey_column_name,
            resolution=resolution,
            use_centroid=use_centroid,
        )

    # Sort by quadkey
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.register("__input_table", working_table)

        if remove_quadkey_column:
            select_cols = [
                quote_identifier(c) for c in working_table.column_names if c != quadkey_column_name
            ]
            select_clause = ", ".join(select_cols)
        else:
            select_clause = "*"

        query = f"SELECT {select_clause} FROM __input_table ORDER BY {quote_identifier(quadkey_column_name)}"
        result = con.execute(query).arrow().read_all()

        # Preserve metadata
        if table.schema.metadata:
            result = result.replace_schema_metadata(table.schema.metadata)

        return result
    finally:
        con.close()


def sort_by_quadkey(
    input_parquet: str,
    output_parquet: str | None = None,
    quadkey_column_name: str = DEFAULT_QUADKEY_COLUMN_NAME,
    resolution: int = DEFAULT_QUADKEY_RESOLUTION,
    use_centroid: bool = False,
    remove_quadkey_column: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
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

    if is_streaming:
        _sort_by_quadkey_streaming(
            input_parquet,
            output_parquet,
            quadkey_column_name,
            resolution,
            use_centroid,
            remove_quadkey_column,
            verbose,
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            profile,
            geoparquet_version,
            memory_limit=memory_limit,
        )
        return

    # File-based mode
    # Check if output file exists and handle overwrite (fixes issue #278)
    handle_output_overwrite(output_parquet, overwrite, input_parquet)

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, input_parquet, output_parquet)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, input_parquet, output_parquet)

    # A directory has to become the glob over its files before anything reads
    # it (#817). `input_parquet` itself stays raw: it is what messages quote and
    # what the multi-file stats guard below is asked about. The options travel
    # with the path: dropping them left hive keys to DuckDB's auto-detect,
    # which agrees only by coincidence (#867). Resolving first also means an
    # empty directory is named as one, rather than reaching the schema read
    # below and coming back as "Cannot read schema: <dir> ... is a directory".
    actual_input, read_options = resolve_read_path(input_parquet, verbose=verbose)
    hive_input: bool | None = True if read_options.get("hive_partitioning") else None

    # Check if quadkey column exists
    column_names = get_column_names(input_parquet)
    column_exists = quadkey_column_name in column_names
    using_default_name = quadkey_column_name == DEFAULT_QUADKEY_COLUMN_NAME

    # Track if we created a temporary file with quadkey
    temp_file = None

    if not column_exists:
        if not using_default_name:
            # Custom name specified but column doesn't exist - error
            raise InvalidParameterError(
                "quadkey_column_name",
                f"column '{quadkey_column_name}' not found in input file. "
                f"Run 'gpio add quadkey --quadkey-name {quadkey_column_name}' to add it first.",
            )

        # Auto-add quadkey column using default name
        if verbose:
            debug(
                f"Quadkey column '{quadkey_column_name}' not found. "
                f"Auto-adding at resolution {resolution}..."
            )

        # Create temporary file for quadkey-enriched data. The input's basename
        # is deliberately not part of the name: a directory input has none (or
        # a trailing separator), and a glob's `*` is not a legal filename
        # character on Windows.
        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(temp_dir, f"quadkey_enriched_{uuid.uuid4().hex}.parquet")

        try:
            add_quadkey_column(
                input_parquet=actual_input,
                output_parquet=temp_file,
                quadkey_column_name=quadkey_column_name,
                resolution=resolution,
                use_centroid=use_centroid,
                dry_run=False,
                verbose=verbose,
                compression="ZSTD",
                compression_level=15,
                row_group_size_mb=None,
                row_group_rows=None,
                profile=profile,
                allow_schema_diff=allow_schema_diff,
                hive_input=hive_input,
            )
            # The scratch file already holds the union (and the materialised
            # hive keys), so the sort below reads it as the plain single file
            # it is.
            actual_input = temp_file
            hive_input = None
            if verbose:
                debug(f"Quadkey column added successfully at resolution {resolution}")
        except Exception as e:
            if temp_file and os.path.exists(temp_file):
                os.remove(temp_file)
            # A multi-file input whose files disagree on their columns dies
            # here, in the first read over the glob. Report the mismatch,
            # not a failed quadkey add (#817's new surface).
            raise_for_schema_mismatch(e, input_parquet)
            raise GeoParquetError(f"Failed to add quadkey column: {str(e)}") from e

    elif verbose:
        debug(f"Using existing quadkey column '{quadkey_column_name}'")

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
    existing_columns = _sortable_columns(actual_input, read_expr, allow_schema_diff)

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
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
            input_file=input_parquet,
            memory_limit=memory_limit,
            # `metadata` describes only the first file of a multi-file input
            # (and the auto-add step carried that footer forward), so its
            # bbox/geometry_types under-cover the merged output. Recompute
            # rather than carry (#793).
            invalidate_derived_stats=is_partition_path(input_parquet),
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
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
    memory_limit: str | None,
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

        # Check if quadkey column exists
        column_names = get_column_names(working_file)
        column_exists = quadkey_column_name in column_names
        using_default_name = quadkey_column_name == DEFAULT_QUADKEY_COLUMN_NAME

        actual_input = working_file

        if not column_exists:
            if not using_default_name:
                raise InvalidParameterError(
                    "quadkey_column_name",
                    f"column '{quadkey_column_name}' not found in input. "
                    f"Use default name 'quadkey' for auto-addition.",
                )

            if verbose:
                debug(f"Auto-adding quadkey column at resolution {resolution}...")

            temp_fd, temp_quadkey_file = tempfile.mkstemp(suffix=".parquet")
            os.close(temp_fd)

            add_quadkey_column(
                input_parquet=working_file,
                output_parquet=temp_quadkey_file,
                quadkey_column_name=quadkey_column_name,
                resolution=resolution,
                use_centroid=use_centroid,
                dry_run=False,
                verbose=verbose,
                compression="ZSTD",
                compression_level=15,
                profile=profile,
                overwrite=True,  # Temp file created by mkstemp
            )
            actual_input = temp_quadkey_file

        # Build and execute sort query
        actual_url = resolve_file_url(actual_input, verbose=False)
        metadata, _ = get_parquet_metadata(actual_input, verbose=False)

        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(actual_input))

        usable_cols = get_usable_columns(actual_input)
        existing_columns = [c["name"] for c in usable_cols]

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
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
            memory_limit=memory_limit,
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
