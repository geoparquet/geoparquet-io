#!/usr/bin/env python3

"""
Hierarchical admin partition functionality.

This module provides partitioning by administrative boundaries through a two-step process:
1. Spatial join with remote admin boundaries dataset to add admin columns
2. Partition the enriched data by those admin columns
"""

from __future__ import annotations

import os

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
from geoparquet_io.core.logging_config import debug, progress, success, warn
from geoparquet_io.core.partition_common import (
    sanitize_filename,
)
from geoparquet_io.core.streaming import is_stdin, read_stdin_to_temp_file


def _build_enrichment_query(
    input_url,
    admin_table_ref,
    admin_where_clause,
    admin_select_clause,
    admin_geom_col,
    admin_bbox_col,
    boundary_columns,
    input_geom_col,
    input_bbox_col,
    enriched_table,
):
    """Build enrichment query for spatial join."""
    # Build column list for subquery - handle struct access
    subquery_cols = []
    for i, col in enumerate(boundary_columns):
        if "[" in col or "(" in col:
            subquery_cols.append(f"{col} as _col_{i}")
        else:
            subquery_cols.append(f'"{col}"')
    subquery_cols_str = ", ".join(subquery_cols)

    if input_bbox_col and admin_bbox_col:
        bbox_filter = f"""
            (a.{input_bbox_col}.xmin <= b.{admin_bbox_col}.xmax AND
             a.{input_bbox_col}.xmax >= b.{admin_bbox_col}.xmin AND
             a.{input_bbox_col}.ymin <= b.{admin_bbox_col}.ymax AND
             a.{input_bbox_col}.ymax >= b.{admin_bbox_col}.ymin)
        """

        return f"""
            CREATE TEMP TABLE {enriched_table} AS
            SELECT
                a.*,
                {admin_select_clause}
            FROM '{input_url}' a
            LEFT JOIN (
                SELECT {admin_geom_col}, {admin_bbox_col}, {subquery_cols_str}
                FROM {admin_table_ref}
                {admin_where_clause}
            ) b
            ON {bbox_filter}
                AND ST_Intersects(b.{admin_geom_col}, a."{input_geom_col}")
        """
    else:
        return f"""
            CREATE TEMP TABLE {enriched_table} AS
            SELECT
                a.*,
                {admin_select_clause}
            FROM '{input_url}' a
            LEFT JOIN (
                SELECT {admin_geom_col}, {subquery_cols_str}
                FROM {admin_table_ref}
                {admin_where_clause}
            ) b
            ON ST_Intersects(b.{admin_geom_col}, a."{input_geom_col}")
        """


def _build_admin_where_clause(
    dataset, levels, con, input_url, input_bbox_col, input_geom_col, admin_bbox_col, verbose
):
    """Build WHERE clause for admin boundaries with filters."""
    admin_where_clauses = []

    # Add subtype filter if applicable
    subtype_filter = dataset.get_subtype_filter(levels)
    if subtype_filter:
        admin_where_clauses.append(subtype_filter)
        if verbose:
            debug(f"  → Filtering admin boundaries: {subtype_filter}")

    # Add bbox extent filter
    if admin_bbox_col:
        if input_bbox_col:
            extent_query = f"""
                SELECT
                    MIN({input_bbox_col}.xmin) as xmin,
                    MAX({input_bbox_col}.xmax) as xmax,
                    MIN({input_bbox_col}.ymin) as ymin,
                    MAX({input_bbox_col}.ymax) as ymax
                FROM '{input_url}'
            """
        else:
            extent_query = f"""
                SELECT
                    MIN(ST_XMin("{input_geom_col}")) as xmin,
                    MAX(ST_XMax("{input_geom_col}")) as xmax,
                    MIN(ST_YMin("{input_geom_col}")) as ymin,
                    MAX(ST_YMax("{input_geom_col}")) as ymax
                FROM '{input_url}'
            """

        extent = con.execute(extent_query).fetchone()
        if extent and all(v is not None for v in extent):
            xmin, xmax, ymin, ymax = extent
            extent_filter = f"""
                ({admin_bbox_col}.xmin <= {xmax} AND
                 {admin_bbox_col}.xmax >= {xmin} AND
                 {admin_bbox_col}.ymin <= {ymax} AND
                 {admin_bbox_col}.ymax >= {ymin})
            """
            admin_where_clauses.append(extent_filter)
            if verbose:
                debug(
                    f"  → Filtering admin boundaries to input extent: ({xmin:.2f}, {ymin:.2f}, {xmax:.2f}, {ymax:.2f})"
                )

    return "WHERE " + " AND ".join(admin_where_clauses) if admin_where_clauses else ""


def _build_partition_query(enriched_table, output_column_names, original_cols):
    """Build partition query for specific combination."""
    select_cols = ", ".join([f'"{col}"' for col in original_cols])
    return f"""
        SELECT {select_cols}
        FROM {enriched_table}
        WHERE {" AND ".join([f'"{col}" = ?' for col in output_column_names])}
    """


def _setup_admin_dataset(dataset_name, verbose, levels):
    """Setup and validate admin dataset."""
    dataset = AdminDatasetFactory.create(dataset_name, source_path=None, verbose=verbose)

    if verbose:
        debug(f"\nUsing admin dataset: {dataset.get_dataset_name()}")
        debug(f"Remote source: {dataset.get_source()}")
        debug(f"Hierarchical levels: {' → '.join(levels)}")

    dataset.validate_levels(levels)
    boundary_columns = dataset.get_partition_columns(levels)

    if verbose:
        debug(f"Boundary dataset columns: {', '.join(boundary_columns)}")

    return dataset, boundary_columns


def _get_input_file_info(input_parquet, verbose):
    """Get input file info (URL, geometry column, bbox column)."""
    input_url = safe_file_url(input_parquet, verbose)
    input_geom_col = find_primary_geometry_column(input_parquet, verbose)
    input_bbox_info = check_bbox_structure(input_parquet, verbose)
    input_bbox_col = input_bbox_info["bbox_column_name"]

    return input_url, input_geom_col, input_bbox_col


def _setup_duckdb_extensions(con):
    """Load required DuckDB extensions."""
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")


def _build_admin_select_for_partitioning(levels, boundary_columns):
    """Build admin SELECT clause for partitioning."""
    admin_select_parts = []
    output_column_names = []
    for i, (level, col) in enumerate(zip(levels, boundary_columns, strict=True)):
        output_col = f"_admin_{level}"  # Temporary internal name
        output_column_names.append(output_col)
        # Handle struct field access vs simple column names
        if "[" in col or "(" in col:
            admin_select_parts.append(f'b._col_{i} as "{output_col}"')
        else:
            admin_select_parts.append(f'b."{col}" as "{output_col}"')

    return ", ".join(admin_select_parts), output_column_names


def _build_admin_table_reference(dataset, admin_source):
    """Build admin table reference with read options if needed."""
    read_options = dataset.get_read_parquet_options()
    if read_options:
        options_str = ", ".join([f"{k}={v}" for k, v in read_options.items()])
        return f"read_parquet({admin_source}, {options_str})"
    return admin_source


def _perform_enrichment_join(
    con,
    enriched_table,
    input_url,
    admin_table_ref,
    admin_where_clause,
    admin_select_clause,
    admin_geom_col,
    admin_bbox_col,
    boundary_columns,
    input_geom_col,
    input_bbox_col,
):
    """Perform spatial join enrichment."""
    enrichment_query = _build_enrichment_query(
        input_url,
        admin_table_ref,
        admin_where_clause,
        admin_select_clause,
        admin_geom_col,
        admin_bbox_col,
        boundary_columns,
        input_geom_col,
        input_bbox_col,
        enriched_table,
    )
    con.execute(enrichment_query)


def _verify_enrichment_results(con, enriched_table, output_column_names):
    """Verify enrichment results and return stats."""
    stats_query = f"""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN {" OR ".join([f'"{col}" IS NOT NULL' for col in output_column_names])} THEN 1 END) as with_admin
        FROM {enriched_table}
    """
    stats = con.execute(stats_query).fetchone()
    total_count, with_admin_count = stats

    success(f"  ✓ Matched {with_admin_count:,} of {total_count:,} features to admin boundaries")

    if with_admin_count == 0:
        raise click.ClickException(
            "No features matched to admin boundaries. Check that input data and boundaries "
            "are in compatible CRS and overlap geographically."
        )

    return total_count, with_admin_count


def _get_partition_combinations(con, enriched_table, output_column_names):
    """Get unique partition combinations."""
    group_by_cols = ", ".join([f'"{col}"' for col in output_column_names])
    combinations_query = f"""
        SELECT DISTINCT {group_by_cols}
        FROM {enriched_table}
        WHERE {" AND ".join([f'"{col}" IS NOT NULL' for col in output_column_names])}
        ORDER BY {group_by_cols}
    """
    result = con.execute(combinations_query)
    return result.fetchall()


def _get_original_columns(con, input_url):
    """Get original column names from input file."""
    original_columns_query = f"SELECT * FROM '{input_url}' LIMIT 0"
    original_schema = con.execute(original_columns_query)
    return [desc[0] for desc in original_schema.description]


def _create_all_partitions(
    con,
    enriched_table,
    output_column_names,
    combinations,
    levels,
    output_folder,
    hive,
    filename_prefix,
    overwrite,
    metadata,
    verbose,
    profile,
    original_cols,
    geoparquet_version=None,
):
    """Create all partition files."""
    partition_count = 0
    for combination in combinations:
        if _create_partition_file(
            con,
            enriched_table,
            output_column_names,
            combination,
            levels,
            output_folder,
            hive,
            filename_prefix,
            overwrite,
            metadata,
            verbose,
            profile,
            original_cols,
            geoparquet_version,
        ):
            partition_count += 1
    return partition_count


def _create_partition_file(
    con,
    enriched_table,
    output_column_names,
    combination,
    levels,
    output_folder,
    hive,
    filename_prefix,
    overwrite,
    metadata,
    verbose,
    profile,
    original_cols,
    geoparquet_version=None,
):
    """Create a single partition file."""
    # Build nested folder path
    folder_parts = []
    for level, value in zip(levels, combination, strict=True):
        safe_value = sanitize_filename(str(value))
        if hive:
            folder_parts.append(f"{level}={safe_value}")
        else:
            folder_parts.append(safe_value)

    # Create partition folder
    partition_folder = os.path.join(output_folder, *folder_parts)
    os.makedirs(partition_folder, exist_ok=True)

    # Generate output filename
    safe_last_value = sanitize_filename(str(combination[-1]))
    filename = (
        f"{filename_prefix}_{safe_last_value}.parquet"
        if filename_prefix
        else f"{safe_last_value}.parquet"
    )
    output_file = os.path.join(partition_folder, filename)

    # Skip if exists and not overwriting
    if os.path.exists(output_file) and not overwrite:
        if verbose:
            debug(f"  ⊘ Skipping existing: {'/'.join(folder_parts)}")
        return False

    if verbose:
        debug(f"  → Creating: {'/'.join(folder_parts)}")

    # Build WHERE clause
    where_conditions = [
        f"\"{col}\" = '{value}'"
        for col, value in zip(output_column_names, combination, strict=True)
    ]
    where_clause = " AND ".join(where_conditions)

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
        profile=profile,
        geoparquet_version=geoparquet_version,
    )

    return True


def partition_by_admin_hierarchical(
    input_parquet: str,
    output_folder: str | None,
    dataset_name: str,
    levels: list[str],
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
) -> int:
    """
    Partition a GeoParquet file by administrative boundaries.

    Supports Arrow IPC streaming for input:
    - Input "-" reads from stdin (output is always a directory)

    This performs a two-step operation:
    1. Spatial join with remote admin boundaries to add admin columns
    2. Partition the enriched data by those admin columns

    Args:
        input_parquet: Input GeoParquet file (local, remote URL, or "-" for stdin)
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
    # Handle stdin input
    stdin_temp_file = None
    actual_input = input_parquet

    if is_stdin(input_parquet):
        stdin_temp_file = read_stdin_to_temp_file(verbose)
        actual_input = stdin_temp_file

    try:
        # Setup dataset and get input file info
        dataset, boundary_columns = _setup_admin_dataset(dataset_name, verbose, levels)
        input_url, input_geom_col, input_bbox_col = _get_input_file_info(actual_input, verbose)

        # Get admin dataset info
        admin_geom_col = dataset.get_geometry_column()
        admin_bbox_col = dataset.get_bbox_column()

        # Use context manager for DuckDB connection to ensure cleanup
        with duckdb.connect() as con:
            _setup_duckdb_extensions(con)

            # Configure S3 settings based on dataset requirements
            dataset.configure_s3(con)

            # STEP 1: Spatial join to create enriched data with admin columns
            progress("\n📍 Step 1/2: Performing spatial join with admin boundaries...")

            enriched_table = "_enriched_with_admin"
            admin_source = dataset.prepare_data_source(con)

            # Build SELECT clause for admin columns
            admin_select_clause, output_column_names = _build_admin_select_for_partitioning(
                levels, boundary_columns
            )

            # Build admin data source with read_parquet options if needed
            admin_table_ref = _build_admin_table_reference(dataset, admin_source)

            # Build WHERE clause for admin boundaries
            admin_where_clause = _build_admin_where_clause(
                dataset,
                levels,
                con,
                input_url,
                input_bbox_col,
                input_geom_col,
                admin_bbox_col,
                verbose,
            )

            # Build efficient spatial join query
            if input_bbox_col and admin_bbox_col and verbose:
                debug("  → Using bbox columns for optimized spatial join")
            elif not (input_bbox_col and admin_bbox_col) and verbose:
                debug("  → Using full geometry intersection (no bbox optimization)")

            # Perform enrichment join
            _perform_enrichment_join(
                con,
                enriched_table,
                input_url,
                admin_table_ref,
                admin_where_clause,
                admin_select_clause,
                admin_geom_col,
                admin_bbox_col,
                boundary_columns,
                input_geom_col,
                input_bbox_col,
            )

            # Verify enrichment results
            _verify_enrichment_results(con, enriched_table, output_column_names)

            # STEP 2: Partition the enriched data
            progress(f"\n📁 Step 2/2: Partitioning by {' → '.join(levels)}...")

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
                return 0

            # Get metadata from input for preservation
            metadata, _ = get_parquet_metadata(actual_input, verbose)

            # Create output directory
            os.makedirs(output_folder, exist_ok=True)

            # Get unique partition combinations
            combinations = _get_partition_combinations(con, enriched_table, output_column_names)

            if verbose:
                debug(f"  → Creating {len(combinations)} partition(s)...")

            # Get original columns (exclude temporary admin columns)
            original_cols = _get_original_columns(con, input_url)

            # Create each partition
            partition_count = _create_all_partitions(
                con,
                enriched_table,
                output_column_names,
                combinations,
                levels,
                output_folder,
                hive,
                filename_prefix,
                overwrite,
                metadata,
                verbose,
                profile,
                original_cols,
                geoparquet_version,
            )

        success(f"\n✓ Created {partition_count} partition(s) in {output_folder}")

        return partition_count
    finally:
        # Clean up stdin temp file
        if stdin_temp_file and os.path.exists(stdin_temp_file):
            os.remove(stdin_temp_file)


def _get_preview_partitions(con, table_name, partition_columns, level_names):
    """Query partition statistics for preview."""
    group_by_cols = ", ".join([f'"{col}"' for col in partition_columns])
    select_cols = ", ".join(
        [f'"{col}" as {name}' for col, name in zip(partition_columns, level_names, strict=True)]
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
    return result.fetchall()


def _display_preview_header(level_names, all_partitions, total_records, limit):
    """Display preview header and stats."""
    progress(f"\n📊 Partition Preview ({' → '.join(level_names)}):")
    progress(f"  Total partitions: {len(all_partitions)}")
    progress(f"  Total records: {total_records:,}")
    progress(f"\n  Top {min(limit, len(all_partitions))} partitions by size:")

    header_parts = [f"{name:<25}" for name in level_names]
    header_parts.append(f"{'Records':>15}")
    header_parts.append(f"{'%':>8}")
    header = "  ".join(header_parts)
    progress(f"\n  {header}")
    progress(f"  {'-' * len(header)}")
    return header


def _display_preview_rows(all_partitions, limit, total_records):
    """Display preview rows."""
    for i, row in enumerate(all_partitions):
        if i >= limit:
            break

        values = row[:-1]
        count = row[-1]
        percentage = (count / total_records) * 100

        row_parts = [f"{str(val):<25}" for val in values]
        row_parts.append(f"{count:>15,}")
        row_parts.append(f"{percentage:>7.1f}%")
        progress(f"  {'  '.join(row_parts)}")


def _display_preview_summary(all_partitions, limit, total_records, header):
    """Display preview summary if more exist."""
    if len(all_partitions) > limit:
        remaining = len(all_partitions) - limit
        remaining_records = sum(row[-1] for row in all_partitions[limit:])
        remaining_pct = (remaining_records / total_records) * 100
        progress(f"  {'-' * len(header)}")
        progress(
            f"  ... and {remaining} more partition(s) with {remaining_records:,} records ({remaining_pct:.1f}%)"
        )
        progress("\n  Use --preview-limit to show more partitions")


def _preview_hierarchical_partitions(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    partition_columns: list[str],
    level_names: list[str],
    limit: int,
    verbose: bool,
) -> None:
    """Preview hierarchical partitions without creating files."""
    all_partitions = _get_preview_partitions(con, table_name, partition_columns, level_names)

    if len(all_partitions) == 0:
        warn("\n⚠️  No partitions would be created (no features with admin boundaries)")
        return

    total_records = sum(row[-1] for row in all_partitions)
    header = _display_preview_header(level_names, all_partitions, total_records, limit)
    _display_preview_rows(all_partitions, limit, total_records)
    _display_preview_summary(all_partitions, limit, total_records, header)
