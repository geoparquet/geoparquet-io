#!/usr/bin/env python3

import os
import re
from typing import Optional

import click
import duckdb

from geoparquet_tools.core.common import (
    get_parquet_metadata,
    safe_file_url,
    write_parquet_with_metadata,
)


def sanitize_filename(value: str) -> str:
    """
    Sanitize a string value for use in a filename.

    Replaces special characters with underscores while preserving alphanumeric and common safe chars.

    Args:
        value: String value to sanitize

    Returns:
        Sanitized string safe for filenames
    """
    # Replace problematic characters with underscores
    # Keep alphanumeric, dash, underscore, and period
    sanitized = re.sub(r"[^a-zA-Z0-9._-]", "_", value)
    # Remove leading/trailing underscores and dots
    sanitized = sanitized.strip("_.")
    # Collapse multiple underscores
    sanitized = re.sub(r"_+", "_", sanitized)
    return sanitized


def preview_partition(
    input_parquet: str,
    column_name: str,
    column_prefix_length: Optional[int] = None,
    limit: int = 15,
    verbose: bool = False,
) -> dict:
    """
    Preview the partitions that would be created without actually creating them.

    Args:
        input_parquet: Input file path
        column_name: Column to partition by
        column_prefix_length: If set, use first N characters of column value
        limit: Maximum number of partitions to display (default: 15)
        verbose: Print detailed output

    Returns:
        Dictionary with partition statistics
    """
    input_url = safe_file_url(input_parquet, verbose)

    # Create DuckDB connection
    con = duckdb.connect()
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    # Build the column expression for partitioning
    if column_prefix_length is not None:
        column_expr = f'LEFT("{column_name}", {column_prefix_length})'
        partition_description = f"first {column_prefix_length} character(s) of '{column_name}'"
    else:
        column_expr = f'"{column_name}"'
        partition_description = f"'{column_name}'"

    # Get partition counts
    query = f"""
        SELECT
            {column_expr} as partition_value,
            COUNT(*) as record_count
        FROM '{input_url}'
        WHERE "{column_name}" IS NOT NULL
        GROUP BY partition_value
        ORDER BY record_count DESC
    """

    result = con.execute(query)
    all_partitions = result.fetchall()

    con.close()

    if len(all_partitions) == 0:
        raise click.ClickException(f"No non-NULL values found in column '{column_name}'")

    # Calculate total records
    total_records = sum(row[1] for row in all_partitions)

    # Display preview
    click.echo(f"\nPartition Preview for {partition_description}:")
    click.echo(f"Total partitions: {len(all_partitions)}")
    click.echo(f"Total records: {total_records:,}")
    click.echo("\nPartitions (sorted by record count):")
    click.echo(f"{'Partition Value':<30} {'Records':>15} {'Percentage':>12}")
    click.echo("-" * 60)

    # Show up to 'limit' partitions
    for i, (partition_value, count) in enumerate(all_partitions):
        if i >= limit:
            break
        percentage = (count / total_records) * 100
        click.echo(f"{str(partition_value):<30} {count:>15,} {percentage:>11.2f}%")

    # Show summary if there are more partitions
    if len(all_partitions) > limit:
        remaining_count = len(all_partitions) - limit
        remaining_records = sum(row[1] for row in all_partitions[limit:])
        remaining_pct = (remaining_records / total_records) * 100
        click.echo("-" * 60)
        click.echo(
            f"... and {remaining_count} more partition(s) with {remaining_records:,} records ({remaining_pct:.2f}%)"
        )
        click.echo("\nUse --preview-limit to show more partitions")

    return {
        "total_partitions": len(all_partitions),
        "total_records": total_records,
        "partitions": all_partitions,
    }


def partition_by_column(
    input_parquet: str,
    output_folder: str,
    column_name: str,
    column_prefix_length: Optional[int] = None,
    hive: bool = False,
    overwrite: bool = False,
    verbose: bool = False,
) -> int:
    """
    Common function to partition a GeoParquet file by column values.

    Args:
        input_parquet: Input file path
        output_folder: Output directory
        column_name: Column to partition by
        column_prefix_length: If set, use first N characters of column value
        hive: Use Hive-style partitioning
        overwrite: Overwrite existing files
        verbose: Print detailed output

    Returns:
        Number of partitions created
    """
    input_url = safe_file_url(input_parquet, verbose)

    # Get metadata before processing
    metadata, _ = get_parquet_metadata(input_parquet, verbose)

    # Create output directory
    os.makedirs(output_folder, exist_ok=True)
    if verbose:
        click.echo(f"Created output directory: {output_folder}")

    # Create DuckDB connection
    con = duckdb.connect()
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    # Build the column expression for partitioning
    if column_prefix_length is not None:
        # Use LEFT() function to get first N characters
        column_expr = f'LEFT("{column_name}", {column_prefix_length})'
        partition_description = f"first {column_prefix_length} characters of {column_name}"
    else:
        column_expr = f'"{column_name}"'
        partition_description = column_name

    # Get unique partition values
    if verbose:
        click.echo(f"Finding unique values for {partition_description}...")

    query = f"""
        SELECT DISTINCT {column_expr} as partition_value
        FROM '{input_url}'
        WHERE "{column_name}" IS NOT NULL
        ORDER BY partition_value
    """

    result = con.execute(query)
    partition_values = result.fetchall()

    if len(partition_values) == 0:
        raise click.ClickException(f"No non-NULL values found in column '{column_name}'")

    if verbose:
        click.echo(f"Found {len(partition_values)} unique partition values")

    # Process each partition value
    for row in partition_values:
        partition_value = row[0]

        # Sanitize the value for use in filenames
        safe_value = sanitize_filename(str(partition_value))

        # Determine output path
        if hive:
            # Hive-style: folder named "column=value"
            if column_prefix_length is not None:
                # For prefix partitioning, use a more descriptive folder name
                folder_name = f"{column_name}_prefix={safe_value}"
            else:
                folder_name = f"{column_name}={safe_value}"
            write_folder = os.path.join(output_folder, folder_name)
            os.makedirs(write_folder, exist_ok=True)
            output_filename = os.path.join(write_folder, f"{safe_value}.parquet")
        else:
            write_folder = output_folder
            output_filename = os.path.join(write_folder, f"{safe_value}.parquet")

        # Skip if file exists and not overwriting
        if os.path.exists(output_filename) and not overwrite:
            if verbose:
                click.echo(f"Output file for {partition_value} already exists, skipping...")
            continue

        if verbose:
            click.echo(f"Processing partition: {partition_value}...")

        # Build WHERE clause based on whether we're using prefix or full value
        if column_prefix_length is not None:
            # Match rows where the prefix matches
            where_clause = f"LEFT(\"{column_name}\", {column_prefix_length}) = '{partition_value}'"
        else:
            # Match rows where the full value matches
            where_clause = f"\"{column_name}\" = '{partition_value}'"

        # Build SELECT query for partition (without COPY wrapper)
        partition_query = f"""
            SELECT *
            FROM '{input_url}'
            WHERE {where_clause}
        """

        # Use common write function with metadata preservation
        write_parquet_with_metadata(
            con,
            partition_query,
            output_filename,
            original_metadata=metadata,
            compression="ZSTD",
            compression_level=15,
            verbose=False,
        )

        if verbose:
            click.echo(f"Wrote {output_filename}")

    con.close()

    return len(partition_values)
