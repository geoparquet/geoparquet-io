#!/usr/bin/env python3

from __future__ import annotations

from geoparquet_io.core.column_selection import find_column
from geoparquet_io.core.duckdb_metadata import get_schema_info, get_usable_column_names
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import unlink_released_file
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success, warn
from geoparquet_io.core.parquet_schema import root_schema_index
from geoparquet_io.core.parquet_writer import WriteOptions
from geoparquet_io.core.partition.common import (
    partition_by_column,
    preview_partition,
    raise_if_no_rows,
    readable_geoparquet,
)
from geoparquet_io.core.streaming import is_stdin, read_stdin_to_temp_file


def validate_column_exists(parquet_file: str, column_name: str, verbose: bool = False):
    """
    Validate that the specified column exists in the parquet file.

    Args:
        parquet_file: Path to the parquet file
        column_name: Name of the column to check
        verbose: Whether to print verbose output

    Raises:
        InvalidParameterError: If the column doesn't exist
    """

    # The reader's own view of the file, so a non-conformant one is refused the
    # way every other partition read refuses it (#722).
    with readable_geoparquet(parquet_file):
        column_names = get_usable_column_names(parquet_file)

    resolved = find_column(column_name, column_names, option_name="column")
    if resolved is None:
        available_columns = ", ".join(column_names)
        raise InvalidParameterError(
            "column",
            f"'{column_name}' not found in the Parquet file. Available columns: {available_columns}",
        )

    if verbose:
        schema_info = get_schema_info(parquet_file)
        index = root_schema_index(schema_info, resolved)
        if index is not None:
            column_type = schema_info[index].get("type", "unknown")
            debug(f"Found column '{resolved}' with type: {column_type}")


def partition_by_string(
    input_parquet: str,
    output_folder: str,
    column: str,
    chars: int | None = None,
    hive: bool = False,
    overwrite: bool = False,
    preview: bool = False,
    preview_limit: int = 15,
    verbose: bool = False,
    force: bool = False,
    skip_analysis: bool = False,
    filename_prefix: str | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    compression: str | None = None,
    compression_level: int | None = None,
    row_group_size_mb: int | None = None,
    row_group_rows: int | None = None,
    memory_limit: str | None = None,
):
    """
    Partition a GeoParquet file by string column values or prefixes.

    Supports Arrow IPC streaming for input:
    - Input "-" reads from stdin (output is always a directory)

    Args:
        input_parquet: Input GeoParquet file (local, remote URL, or "-" for stdin)
        output_folder: Output directory (always writes to directory, no stdout support)
        column: Column name to partition by (required)
        chars: Optional number of characters to use (prefix length)
        hive: Use Hive-style partitioning
        overwrite: Overwrite existing files
        preview: Show preview of partitions without creating files
        preview_limit: Maximum number of partitions to show in preview (default: 15)
        verbose: Verbose output
        force: Force partitioning even if analysis detects issues
        skip_analysis: Skip partition strategy analysis (for performance)
        filename_prefix: Optional prefix for partition filenames
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write
        compression: Compression codec (default: the compression owner's)
        compression_level: Compression level (default: 15)
        row_group_size_mb: Row group size in MB (mutually exclusive with row_group_rows)
        row_group_rows: Row group size in number of rows (mutually exclusive with row_group_size_mb)
        memory_limit: DuckDB memory limit for write operations (e.g., "2GB")
    """

    # Configure logging verbosity
    configure_verbose(verbose)

    # Handle stdin input - read stream to temp file first
    temp_file_path = None
    if is_stdin(input_parquet):
        temp_file_path = read_stdin_to_temp_file(verbose)
        input_parquet = temp_file_path

    write = WriteOptions.pick(locals())
    try:
        # Validate column exists
        if verbose:
            debug(f"Validating column '{column}'...")
        validate_column_exists(input_parquet, column, verbose)

        # Nothing to partition: stop before analysis or any write (#823).
        raise_if_no_rows(input_parquet)

        # Validate chars parameter if provided
        if chars is not None and chars < 1:
            raise InvalidParameterError("chars", "must be a positive integer")

        # If preview mode, show preview and analysis, then exit
        if preview:
            # Run analysis first to show recommendations
            try:
                from geoparquet_io.core.partition.common import (
                    PartitionAnalysisError,
                    analyze_partition_strategy,
                )

                analyze_partition_strategy(
                    input_parquet=input_parquet,
                    column_name=column,
                    column_prefix_length=chars,
                    verbose=True,
                )
            except PartitionAnalysisError:
                # Analysis already displayed the errors, just continue to preview
                pass
            except Exception as e:
                # If analysis fails unexpectedly, show error but continue to preview
                warn(f"\nAnalysis error: {e}")

            # Then show partition preview
            progress("\n" + "=" * 70)
            preview_partition(
                input_parquet=input_parquet,
                column_name=column,
                column_prefix_length=chars,
                limit=preview_limit,
                verbose=verbose,
            )
            return

        # Build description for user feedback
        if chars is not None:
            description = f"Partitioning by first {chars} character(s) of '{column}'"
        else:
            description = f"Partitioning by '{column}'"

        progress(description)

        # Use common partition function
        num_partitions = partition_by_column(
            input_parquet=input_parquet,
            output_folder=output_folder,
            column_name=column,
            column_prefix_length=chars,
            hive=hive,
            overwrite=overwrite,
            verbose=verbose,
            force=force,
            skip_analysis=skip_analysis,
            filename_prefix=filename_prefix,
            write=write,
        )

        success(f"Successfully created {num_partitions} partition file(s)")
    finally:
        # Clean up temporary file if created
        if temp_file_path and verbose:
            debug(f"Cleaning up temporary file: {temp_file_path}")
        unlink_released_file(temp_file_path)


if __name__ == "__main__":
    partition_by_string()
