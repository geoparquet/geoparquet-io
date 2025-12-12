#!/usr/bin/env python3


import click

from geoparquet_io.core.common import safe_file_url
from geoparquet_io.core.logging_config import debug, progress, success, warn
from geoparquet_io.core.partition_common import partition_by_column, preview_partition


def validate_column_exists(parquet_file: str, column_name: str, verbose: bool = False):
    """
    Validate that the specified column exists in the parquet file.

    Args:
        parquet_file: Path to the parquet file
        column_name: Name of the column to check
        verbose: Whether to print verbose output

    Raises:
        click.UsageError: If the column doesn't exist
    """
    from geoparquet_io.core.duckdb_metadata import get_column_names, get_schema_info

    safe_url = safe_file_url(parquet_file, verbose)
    column_names = get_column_names(safe_url)

    if column_name not in column_names:
        available_columns = ", ".join(column_names)
        raise click.UsageError(
            f"Column '{column_name}' not found in the Parquet file.\n"
            f"Available columns: {available_columns}"
        )

    if verbose:
        schema_info = get_schema_info(safe_url)
        for col in schema_info:
            if col.get("name") == column_name:
                column_type = col.get("type", "unknown")
                debug(f"Found column '{column_name}' with type: {column_type}")
                break


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
    filename_prefix: str = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
):
    """
    Partition a GeoParquet file by string column values or prefixes.

    Args:
        input_parquet: Input GeoParquet file
        output_folder: Output directory
        column: Column name to partition by (required)
        chars: Optional number of characters to use (prefix length)
        hive: Use Hive-style partitioning
        overwrite: Overwrite existing files
        preview: Show preview of partitions without creating files
        preview_limit: Maximum number of partitions to show in preview (default: 15)
        verbose: Verbose output
        force: Force partitioning even if analysis detects issues
        skip_analysis: Skip partition strategy analysis (for performance)
    """
    # Validate column exists
    if verbose:
        debug(f"Validating column '{column}'...")
    validate_column_exists(input_parquet, column, verbose)

    # Validate chars parameter if provided
    if chars is not None and chars < 1:
        raise click.UsageError("--chars must be a positive integer")

    # If preview mode, show preview and analysis, then exit
    if preview:
        # Run analysis first to show recommendations
        try:
            from geoparquet_io.core.partition_common import (
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
        profile=profile,
        geoparquet_version=geoparquet_version,
    )

    success(f"Successfully created {num_partitions} partition file(s)")


if __name__ == "__main__":
    partition_by_string()
