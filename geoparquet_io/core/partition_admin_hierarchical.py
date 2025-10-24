#!/usr/bin/env python3

"""
Hierarchical admin partition functionality.

This module provides partitioning by administrative boundaries through a two-step process:
1. Spatial join with remote admin boundaries dataset to add admin columns
2. Partition the enriched data by those admin columns
"""

import os
from typing import Optional

import click
import duckdb

from geoparquet_io.core.admin_datasets import AdminDatasetFactory
from geoparquet_io.core.common import (
    check_bbox_structure,
    find_primary_geometry_column,
    get_parquet_metadata,
    safe_file_url,
    write_parquet_with_metadata,
)
from geoparquet_io.core.partition_common import (
    sanitize_filename,
)


def partition_by_admin_hierarchical(
    input_parquet: str,
    output_folder: Optional[str],
    dataset_name: str,
    levels: list[str],
    hive: bool = False,
    overwrite: bool = False,
    preview: bool = False,
    preview_limit: int = 15,
    verbose: bool = False,
    force: bool = False,
    skip_analysis: bool = False,
) -> int:
    """
    Partition a GeoParquet file by administrative boundaries.

    This performs a two-step operation:
    1. Spatial join with remote admin boundaries to add admin columns
    2. Partition the enriched data by those admin columns

    Args:
        input_parquet: Input GeoParquet file path
        output_folder: Output directory for partitioned files
        dataset_name: Name of admin dataset ("gaul", "overture")
        levels: List of hierarchical levels to partition by
        hive: Use Hive-style partitioning
        overwrite: Overwrite existing partition files
        preview: Preview partitions without creating files
        preview_limit: Number of partitions to show in preview
        verbose: Enable verbose output
        force: Force partitioning even if analysis detects issues
        skip_analysis: Skip partition strategy analysis

    Returns:
        Number of partitions created
    """
    # Create dataset instance (no custom sources for now)
    dataset = AdminDatasetFactory.create(dataset_name, source_path=None, verbose=verbose)

    if verbose:
        click.echo(f"\nUsing admin dataset: {dataset.get_dataset_name()}")
        click.echo(f"Remote source: {dataset.get_source()}")
        click.echo(f"Hierarchical levels: {' → '.join(levels)}")

    # Validate levels
    dataset.validate_levels(levels)

    # Get column names for the requested levels in the boundaries dataset
    boundary_columns = dataset.get_partition_columns(levels)

    if verbose:
        click.echo(f"Boundary dataset columns: {', '.join(boundary_columns)}")

    # Get input file info
    input_url = safe_file_url(input_parquet, verbose)
    input_geom_col = find_primary_geometry_column(input_parquet, verbose)
    input_bbox_info = check_bbox_structure(input_parquet, verbose)
    input_bbox_col = input_bbox_info["bbox_column_name"]

    # Get admin dataset info
    admin_geom_col = dataset.get_geometry_column()
    admin_bbox_col = dataset.get_bbox_column()

    # Create DuckDB connection
    con = duckdb.connect()
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    # Configure for source.coop S3 access
    con.execute("SET s3_endpoint='data.source.coop';")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_use_ssl=true;")

    # STEP 1: Spatial join to create enriched data with admin columns
    click.echo("\n📍 Step 1/2: Performing spatial join with admin boundaries...")

    enriched_table = "_enriched_with_admin"

    # Prepare admin data source (downloads and caches if remote)
    admin_source = dataset.prepare_data_source(con)

    # Build SELECT clause for admin columns (only pull what we need)
    admin_select_parts = []
    output_column_names = []
    for level, col in zip(levels, boundary_columns):
        output_col = f"_admin_{level}"  # Temporary internal name
        output_column_names.append(output_col)
        admin_select_parts.append(f'b."{col}" as "{output_col}"')

    admin_select_clause = ", ".join(admin_select_parts)

    # Build efficient spatial join query
    if input_bbox_col and admin_bbox_col:
        if verbose:
            click.echo("  → Using bbox columns for optimized spatial join")

        # Only select necessary columns from boundaries dataset
        bbox_filter = f"""
            (a.{input_bbox_col}.xmin <= b.{admin_bbox_col}.xmax AND
             a.{input_bbox_col}.xmax >= b.{admin_bbox_col}.xmin AND
             a.{input_bbox_col}.ymin <= b.{admin_bbox_col}.ymax AND
             a.{input_bbox_col}.ymax >= b.{admin_bbox_col}.ymin)
        """

        enrichment_query = f"""
            CREATE TEMP TABLE {enriched_table} AS
            SELECT
                a.*,
                {admin_select_clause}
            FROM '{input_url}' a
            LEFT JOIN (
                SELECT "{admin_geom_col}", {admin_bbox_col}, {", ".join([f'"{col}"' for col in boundary_columns])}
                FROM '{admin_source}'
            ) b
            ON {bbox_filter}
                AND ST_Intersects(b."{admin_geom_col}", a."{input_geom_col}")
        """
    else:
        if verbose:
            click.echo("  → Using full geometry intersection (no bbox optimization)")

        enrichment_query = f"""
            CREATE TEMP TABLE {enriched_table} AS
            SELECT
                a.*,
                {admin_select_clause}
            FROM '{input_url}' a
            LEFT JOIN (
                SELECT "{admin_geom_col}", {", ".join([f'"{col}"' for col in boundary_columns])}
                FROM '{admin_source}'
            ) b
            ON ST_Intersects(b."{admin_geom_col}", a."{input_geom_col}")
        """

    con.execute(enrichment_query)

    # Get stats on enrichment
    stats_query = f"""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN {" OR ".join([f'"{col}" IS NOT NULL' for col in output_column_names])} THEN 1 END) as with_admin
        FROM {enriched_table}
    """
    stats = con.execute(stats_query).fetchone()
    total_count, with_admin_count = stats

    click.echo(f"  ✓ Matched {with_admin_count:,} of {total_count:,} features to admin boundaries")

    if with_admin_count == 0:
        con.close()
        raise click.ClickException(
            "No features matched to admin boundaries. Check that input data and boundaries "
            "are in compatible CRS and overlap geographically."
        )

    # STEP 2: Partition the enriched data
    click.echo(f"\n📁 Step 2/2: Partitioning by {' → '.join(levels)}...")

    # Preview mode
    if preview:
        _preview_hierarchical_partitions(
            con,
            enriched_table,
            output_column_names,
            levels,
            preview_limit,
            verbose,
        )
        con.close()
        return 0

    # Get metadata from input for preservation
    metadata, _ = get_parquet_metadata(input_parquet, verbose)

    # Create output directory
    os.makedirs(output_folder, exist_ok=True)

    # Get unique partition combinations
    group_by_cols = ", ".join([f'"{col}"' for col in output_column_names])
    combinations_query = f"""
        SELECT DISTINCT {group_by_cols}
        FROM {enriched_table}
        WHERE {" AND ".join([f'"{col}" IS NOT NULL' for col in output_column_names])}
        ORDER BY {group_by_cols}
    """

    result = con.execute(combinations_query)
    combinations = result.fetchall()

    if verbose:
        click.echo(f"  → Creating {len(combinations)} partition(s)...")

    # Create each partition
    partition_count = 0
    for combination in combinations:
        # Build nested folder path
        folder_parts = []
        for _i, (level, value) in enumerate(zip(levels, combination)):
            safe_value = sanitize_filename(str(value))
            if hive:
                folder_parts.append(f"{level}={safe_value}")
            else:
                folder_parts.append(safe_value)

        # Create partition folder
        partition_folder = os.path.join(output_folder, *folder_parts)
        os.makedirs(partition_folder, exist_ok=True)

        # Generate output filename
        filename = f"{sanitize_filename(str(combination[-1]))}.parquet"
        output_file = os.path.join(partition_folder, filename)

        # Skip if exists and not overwriting
        if os.path.exists(output_file) and not overwrite:
            if verbose:
                click.echo(f"  ⊘ Skipping existing: {'/'.join(folder_parts)}")
            continue

        if verbose:
            click.echo(f"  → Creating: {'/'.join(folder_parts)}")

        # Build WHERE clause
        where_conditions = [
            f"\"{col}\" = '{value}'" for col, value in zip(output_column_names, combination)
        ]
        where_clause = " AND ".join(where_conditions)

        # Build SELECT query (exclude temporary admin columns)
        original_columns_query = f"SELECT * FROM '{input_url}' LIMIT 0"
        original_schema = con.execute(original_columns_query)
        original_cols = [desc[0] for desc in original_schema.description]

        select_cols = ", ".join([f'"{col}"' for col in original_cols])

        partition_query = f"""
            SELECT {select_cols}
            FROM {enriched_table}
            WHERE {where_clause}
        """

        # Write partition
        write_parquet_with_metadata(
            con,
            partition_query,
            output_file,
            original_metadata=metadata,
            compression="ZSTD",
            compression_level=15,
            verbose=False,
        )

        partition_count += 1

    con.close()

    click.echo(f"\n✓ Created {partition_count} partition(s) in {output_folder}")

    return partition_count


def _preview_hierarchical_partitions(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    partition_columns: list[str],
    level_names: list[str],
    limit: int,
    verbose: bool,
) -> None:
    """Preview hierarchical partitions without creating files."""

    # Build query for partition stats
    group_by_cols = ", ".join([f'"{col}"' for col in partition_columns])
    select_cols = ", ".join(
        [f'"{col}" as {name}' for col, name in zip(partition_columns, level_names)]
    )

    query = f"""
        SELECT
            {select_cols},
            COUNT(*) as record_count
        FROM {table_name}
        WHERE {" AND ".join([f'"{col}" IS NOT NULL' for col in partition_columns])}
        GROUP BY {group_by_cols}
        ORDER BY record_count DESC
    """

    result = con.execute(query)
    all_partitions = result.fetchall()

    if len(all_partitions) == 0:
        click.echo("\n⚠️  No partitions would be created (no features with admin boundaries)")
        return

    # Calculate totals
    total_records = sum(row[-1] for row in all_partitions)

    # Display preview
    click.echo(f"\n📊 Partition Preview ({' → '.join(level_names)}):")
    click.echo(f"  Total partitions: {len(all_partitions)}")
    click.echo(f"  Total records: {total_records:,}")
    click.echo(f"\n  Top {min(limit, len(all_partitions))} partitions by size:")

    # Build header
    header_parts = [f"{name:<25}" for name in level_names]
    header_parts.append(f"{'Records':>15}")
    header_parts.append(f"{'%':>8}")
    header = "  ".join(header_parts)
    click.echo(f"\n  {header}")
    click.echo(f"  {'-' * len(header)}")

    # Show partitions
    for i, row in enumerate(all_partitions):
        if i >= limit:
            break

        values = row[:-1]
        count = row[-1]
        percentage = (count / total_records) * 100

        row_parts = [f"{str(val):<25}" for val in values]
        row_parts.append(f"{count:>15,}")
        row_parts.append(f"{percentage:>7.1f}%")
        click.echo(f"  {'  '.join(row_parts)}")

    # Show summary if more exist
    if len(all_partitions) > limit:
        remaining = len(all_partitions) - limit
        remaining_records = sum(row[-1] for row in all_partitions[limit:])
        remaining_pct = (remaining_records / total_records) * 100
        click.echo(f"  {'-' * len(header)}")
        click.echo(
            f"  ... and {remaining} more partition(s) with {remaining_records:,} records ({remaining_pct:.1f}%)"
        )
        click.echo("\n  Use --preview-limit to show more partitions")
