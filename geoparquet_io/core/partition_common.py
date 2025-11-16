#!/usr/bin/env python3

import os
import re
from typing import Optional

import click
import duckdb

from geoparquet_io.core.common import (
    get_parquet_metadata,
    safe_file_url,
    write_parquet_with_metadata,
)


class PartitionAnalysisError(Exception):
    """Raised when partition analysis detects a problematic strategy."""

    pass


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


def analyze_partition_strategy(
    input_parquet: str,
    column_name: str,
    column_prefix_length: Optional[int] = None,
    max_partitions: int = 10000,
    min_rows_per_partition: int = 100,
    min_partition_size_mb: float = 0.001,  # 1KB
    max_imbalance_ratio: float = 1000.0,
    warn_imbalance_ratio: float = 100.0,
    warn_partition_count: int = 1000,
    warn_min_rows: int = 10000,
    verbose: bool = False,
) -> dict:
    """
    Analyze a partition strategy before execution to detect potential issues.

    Calculates comprehensive statistics about how data would be partitioned and
    checks for pathological cases like too many partitions, tiny partitions,
    extreme imbalance, etc.

    Args:
        input_parquet: Input file path
        column_name: Column to partition by
        column_prefix_length: If set, use first N characters of column value
        max_partitions: Maximum allowed partitions (raises error if exceeded)
        min_rows_per_partition: Minimum rows per partition (raises error if avg below this)
        min_partition_size_mb: Minimum partition size in MB (raises error if avg below this)
        max_imbalance_ratio: Maximum ratio of largest to median partition (raises error if exceeded)
        warn_imbalance_ratio: Warn if imbalance ratio exceeds this
        warn_partition_count: Warn if partition count exceeds this
        warn_min_rows: Warn if average rows per partition below this
        verbose: Print detailed analysis

    Returns:
        Dictionary with analysis results including:
        - partition_count: Number of partitions
        - total_rows: Total number of rows
        - row_stats: Dict with min, max, avg, median, stddev
        - size_stats: Dict with estimated sizes
        - imbalance_ratio: Ratio of max to median partition size
        - warnings: List of warning messages
        - errors: List of error messages (blocking)
        - largest_partition_pct: Percentage of data in largest partition

    Raises:
        PartitionAnalysisError: If blocking issues are detected
    """
    input_url = safe_file_url(input_parquet, verbose)

    # Create DuckDB connection
    con = duckdb.connect()
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    # Build the column expression for partitioning
    if column_prefix_length is not None:
        column_expr = f'LEFT("{column_name}", {column_prefix_length})'
    else:
        column_expr = f'"{column_name}"'

    if verbose:
        click.echo("\n📊 Analyzing partition strategy...")

    # Calculate comprehensive statistics in one query
    stats_query = f"""
        WITH partition_stats AS (
            SELECT
                {column_expr} as partition_value,
                COUNT(*) as row_count
            FROM '{input_url}'
            WHERE "{column_name}" IS NOT NULL
            GROUP BY partition_value
        )
        SELECT
            COUNT(*) as partition_count,
            SUM(row_count) as total_rows,
            MIN(row_count) as min_rows,
            MAX(row_count) as max_rows,
            AVG(row_count)::BIGINT as avg_rows,
            MEDIAN(row_count)::BIGINT as median_rows
        FROM partition_stats
    """

    result = con.execute(stats_query).fetchone()

    # Get approximate file size for estimation
    # Use file size as a rough proxy for row size
    try:
        import os

        file_size_bytes = os.path.getsize(input_parquet)
    except Exception:
        file_size_bytes = 0

    con.close()

    # Unpack results
    (
        partition_count,
        total_rows,
        min_rows,
        max_rows,
        avg_rows,
        median_rows,
    ) = result

    # Estimate size based on file size and row distribution
    if file_size_bytes > 0 and total_rows > 0:
        bytes_per_row = file_size_bytes / total_rows
        min_size_bytes = int(min_rows * bytes_per_row)
        max_size_bytes = int(max_rows * bytes_per_row)
        avg_size_bytes = int(avg_rows * bytes_per_row)
    else:
        # Fallback if we can't get file size
        min_size_bytes = min_rows * 1000  # Assume 1KB per row as rough estimate
        max_size_bytes = max_rows * 1000
        avg_size_bytes = avg_rows * 1000

    # Calculate derived metrics
    imbalance_ratio = max_rows / median_rows if median_rows > 0 else 0
    largest_partition_pct = (max_rows / total_rows * 100) if total_rows > 0 else 0
    avg_size_mb = avg_size_bytes / (1024 * 1024)
    max_size_mb = max_size_bytes / (1024 * 1024)

    # Collect warnings and errors
    warnings = []
    errors = []

    # Check for BREAKING conditions
    if partition_count > max_partitions:
        errors.append(
            f"Pathological partitioning: {partition_count:,} partitions exceeds maximum of {max_partitions:,}. "
            f"This will create too many small files with poor performance."
        )

    if avg_rows < min_rows_per_partition:
        errors.append(
            f"Tiny partitions: Average of {avg_rows:,} rows per partition is below minimum of {min_rows_per_partition:,}. "
            f"This will result in poor I/O performance and excessive file overhead."
        )

    if avg_size_mb < min_partition_size_mb:
        errors.append(
            f"Tiny partition files: Average size of {avg_size_mb:.3f} MB is below minimum of {min_partition_size_mb:.3f} MB. "
            f"These tiny files will have poor I/O performance."
        )

    if imbalance_ratio > max_imbalance_ratio:
        errors.append(
            f"Extreme imbalance: Largest partition ({max_rows:,} rows) is {imbalance_ratio:.1f}x the median ({median_rows:,} rows). "
            f"This indicates a data quality issue or wrong partition key choice."
        )

    # Check for WARNING conditions
    if warn_imbalance_ratio <= imbalance_ratio < max_imbalance_ratio:
        warnings.append(
            f"Moderate imbalance: Largest partition is {imbalance_ratio:.1f}x the median. "
            f"This might be expected (e.g., some regions are larger) but worth noting."
        )

    if warn_partition_count <= partition_count < max_partitions:
        warnings.append(
            f"Many partitions: Creating {partition_count:,} partitions. Not broken but getting unwieldy."
        )

    if min_rows_per_partition <= avg_rows < warn_min_rows:
        warnings.append(
            f"Small partitions: Average of {avg_rows:,} rows per partition is below recommended {warn_min_rows:,}. "
            f"Suboptimal but might be intentional for your use case."
        )

    if largest_partition_pct > 50:
        warnings.append(
            f"Single partition dominates: One partition contains {largest_partition_pct:.1f}% of all data. "
            f"This might defeat the purpose of partitioning."
        )

    # Generate recommendations
    recommendations = _generate_recommendations(
        partition_count=partition_count,
        total_rows=total_rows,
        avg_rows=avg_rows,
        max_rows=max_rows,
        avg_size_mb=avg_size_mb,
        imbalance_ratio=imbalance_ratio,
        column_prefix_length=column_prefix_length,
    )

    # Build result dictionary
    analysis = {
        "partition_count": partition_count,
        "total_rows": total_rows,
        "row_stats": {
            "min": min_rows,
            "max": max_rows,
            "avg": avg_rows,
        },
        "size_stats": {
            "min_mb": min_size_bytes / (1024 * 1024),
            "max_mb": max_size_mb,
            "avg_mb": avg_size_mb,
        },
        "imbalance_ratio": imbalance_ratio,
        "largest_partition_pct": largest_partition_pct,
        "warnings": warnings,
        "errors": errors,
        "recommendations": recommendations,
    }

    # Display analysis if verbose
    if verbose or warnings or errors or recommendations:
        _display_partition_analysis(analysis, column_name, column_prefix_length)

    # Raise error if blocking conditions found
    if errors:
        error_msg = "\n\n".join([f"❌ {err}" for err in errors])
        error_msg += "\n\nUse --force to override this check if you're certain about this partitioning strategy."
        raise PartitionAnalysisError(error_msg)

    return analysis


def _generate_recommendations(
    partition_count: int,
    total_rows: int,
    avg_rows: int,
    max_rows: int,
    avg_size_mb: float,
    imbalance_ratio: float,
    column_prefix_length: Optional[int] = None,
) -> list:
    """
    Generate actionable recommendations based on partition analysis.

    Args:
        partition_count: Number of partitions
        total_rows: Total number of rows
        avg_rows: Average rows per partition
        max_rows: Maximum rows in a partition
        avg_size_mb: Average partition size in MB
        imbalance_ratio: Ratio of max to median partition
        column_prefix_length: If partitioning by prefix (e.g., H3 resolution)

    Returns:
        List of recommendation strings
    """
    recommendations = []

    # Very small dataset - not worth partitioning
    if total_rows < 1000:
        recommendations.append(
            f"Dataset is very small ({total_rows:,} rows). "
            "Partitioning is not recommended - use as a single file for better performance."
        )
        return recommendations

    # Too many tiny files - suggest not partitioning or using fewer partitions
    if partition_count > 1000 and avg_rows < 10000:
        recommendations.append(
            "Consider NOT partitioning this dataset - you'll create many small files with poor performance. "
            "Alternative: Use the data as a single file with spatial indexing (bbox column)."
        )

    # Large partitions - suggest more granular partitioning
    if column_prefix_length is not None and avg_rows > 1000000:
        recommendations.append(
            f"Partitions are very large ({avg_rows:,} avg rows). "
            f"Consider using more granular partitioning (e.g., higher resolution/iterations) "
            f"for smaller, more manageable partitions."
        )

    # Very large max partition - suggest hierarchical partitioning
    if max_rows > 10000000 and imbalance_ratio > 5:
        recommendations.append(
            "Largest partition is very large and imbalanced. "
            "Consider hierarchical partitioning: partition by a coarser key first (e.g., country/region), "
            "then by spatial partitioning within each partition."
        )

    # Moderate number of reasonable-sized partitions - good strategy
    if 10 <= partition_count <= 500 and 10000 <= avg_rows <= 5000000:
        recommendations.append(
            "This looks like a reasonable partitioning strategy. "
            f"You'll create {partition_count} partitions with ~{avg_rows:,} rows each."
        )

    # Too few partitions - might not be worth it
    if partition_count < 5 and total_rows > 1000000:
        recommendations.append(
            f"Only {partition_count} partitions for {total_rows:,} rows. "
            "Consider using more granular partitioning (e.g., higher resolution/iterations or prefix partitioning)."
        )

    # Suggest hierarchical for specific cases
    if partition_count > 5000:
        recommendations.append(
            f"Too many partitions ({partition_count:,}). Consider hierarchical approach: "
            f"first partition by a coarser key (e.g., region/country), then by finer spatial partitioning within each."
        )

    return recommendations


def _display_partition_analysis(
    analysis: dict, column_name: str, column_prefix_length: Optional[int]
) -> None:
    """Display partition analysis results in a formatted way."""
    partition_desc = f"'{column_name}'"
    if column_prefix_length is not None:
        partition_desc = f"first {column_prefix_length} character(s) of {partition_desc}"

    click.echo(f"\nAnalyzing partition strategy for {partition_desc}...")
    click.echo(f"  Partitions: {analysis['partition_count']:,}")
    click.echo(f"  Total rows: {analysis['total_rows']:,}")

    # Row distribution
    stats = analysis["row_stats"]
    click.echo(
        f"  Rows per partition: min={stats['min']:,}, max={stats['max']:,}, avg={stats['avg']:,}"
    )

    # Size estimates
    size = analysis["size_stats"]
    if size["avg_mb"] >= 0.01:
        click.echo(
            f"  Estimated size per partition: min={size['min_mb']:.2f}MB, max={size['max_mb']:.2f}MB, avg={size['avg_mb']:.2f}MB"
        )

    # Balance metrics
    click.echo(
        f"  Imbalance: {analysis['imbalance_ratio']:.1f}x (largest/median), {analysis['largest_partition_pct']:.1f}% in largest"
    )

    # Recommendations
    if analysis.get("recommendations"):
        click.echo("\nRecommendations:")
        for rec in analysis["recommendations"]:
            click.echo(click.style(f"  💡 {rec}", fg="cyan"))

    # Warnings
    if analysis["warnings"]:
        click.echo("\nWarnings:")
        for warning in analysis["warnings"]:
            click.echo(click.style(f"  {warning}", fg="yellow"))

    # Errors
    if analysis["errors"]:
        click.echo("\nErrors:")
        for error in analysis["errors"]:
            click.echo(click.style(f"  {error}", fg="red"))


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
    keep_partition_column: bool = True,
    force: bool = False,
    skip_analysis: bool = False,
    filename_prefix: Optional[str] = None,
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
        keep_partition_column: Whether to keep the partition column in output files (default: True)
        force: Force partitioning even if analysis detects issues
        skip_analysis: Skip partition strategy analysis (for performance)
        filename_prefix: Optional prefix for partition filenames (e.g., 'fields' → fields_USA.parquet)

    Returns:
        Number of partitions created
    """
    input_url = safe_file_url(input_parquet, verbose)

    # Run partition analysis unless skipped
    if not skip_analysis:
        try:
            analyze_partition_strategy(
                input_parquet=input_parquet,
                column_name=column_name,
                column_prefix_length=column_prefix_length,
                verbose=verbose,
            )
        except PartitionAnalysisError as e:
            if not force:
                raise click.ClickException(str(e)) from e
            else:
                # User forced execution despite warnings
                if verbose:
                    click.echo(
                        click.style(
                            "\n⚠️  Forcing partition creation despite analysis warnings/errors...",
                            fg="yellow",
                            bold=True,
                        )
                    )

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
        # Build filename with optional prefix
        filename = (
            f"{filename_prefix}_{safe_value}.parquet"
            if filename_prefix
            else f"{safe_value}.parquet"
        )

        if hive:
            # Hive-style: folder named "column=value"
            if column_prefix_length is not None:
                # For prefix partitioning, use a more descriptive folder name
                folder_name = f"{column_name}_prefix={safe_value}"
            else:
                folder_name = f"{column_name}={safe_value}"
            write_folder = os.path.join(output_folder, folder_name)
            os.makedirs(write_folder, exist_ok=True)
            output_filename = os.path.join(write_folder, filename)
        else:
            write_folder = output_folder
            output_filename = os.path.join(write_folder, filename)

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

        # Build SELECT clause - exclude partition column if requested
        if keep_partition_column:
            select_clause = "*"
        else:
            # Get all column names from the input file
            schema_query = f"SELECT * FROM '{input_url}' LIMIT 0"
            schema_result = con.execute(schema_query)
            all_columns = [desc[0] for desc in schema_result.description]

            # Exclude the partition column
            columns_to_select = [f'"{col}"' for col in all_columns if col != column_name]
            select_clause = ", ".join(columns_to_select)

        # Build SELECT query for partition (without COPY wrapper)
        partition_query = f"""
            SELECT {select_clause}
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
