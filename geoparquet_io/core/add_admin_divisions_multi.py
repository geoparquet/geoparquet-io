#!/usr/bin/env python3

"""
Add admin division columns from multiple datasets.

This module extends the add_country_codes functionality to support
multiple admin datasets with hierarchical level support.
"""

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


def add_admin_divisions_multi(
    input_parquet: str,
    output_parquet: str,
    dataset_name: str,
    levels: list[str],
    dataset_source: Optional[str] = None,
    add_bbox_flag: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: Optional[int] = None,
    row_group_size_mb: Optional[float] = None,
    row_group_rows: Optional[int] = None,
):
    """
    Add admin division columns from a multi-level admin dataset.

    Args:
        input_parquet: Input GeoParquet file
        output_parquet: Output GeoParquet file
        dataset_name: Name of admin dataset ("current", "gaul", "overture")
        levels: List of hierarchical levels to add as columns
        dataset_source: Optional custom path/URL to admin dataset
        add_bbox_flag: Automatically add bbox column if missing
        dry_run: Show SQL without executing
        verbose: Enable verbose output
        compression: Compression type
        compression_level: Compression level
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
    """
    # Create dataset instance
    dataset = AdminDatasetFactory.create(dataset_name, dataset_source, verbose)

    if verbose:
        click.echo(f"\nUsing admin dataset: {dataset.get_dataset_name()}")
        click.echo(f"Data source: {dataset.get_source()}")
        click.echo(f"Adding admin levels: {', '.join(levels)}")

    # Validate levels
    dataset.validate_levels(levels)

    # Get column names for the requested levels
    partition_columns = dataset.get_partition_columns(levels)

    # Get safe URLs
    input_url = safe_file_url(input_parquet, verbose)
    admin_source = dataset.get_source()

    # Get geometry column names
    input_geom_col = find_primary_geometry_column(input_parquet, verbose)
    admin_geom_col = dataset.get_geometry_column()

    # Check bbox columns
    input_bbox_info = check_bbox_structure(input_parquet, verbose)
    input_bbox_col = input_bbox_info["bbox_column_name"]
    admin_bbox_col = dataset.get_bbox_column()

    # Start dry-run mode output if needed
    if dry_run:
        click.echo(
            click.style(
                "\n=== DRY RUN MODE - SQL Commands that would be executed ===\n",
                fg="yellow",
                bold=True,
            )
        )
        click.echo(click.style(f"-- Input file: {input_url}", fg="cyan"))
        click.echo(click.style(f"-- Admin dataset: {admin_source}", fg="cyan"))
        click.echo(click.style(f"-- Output file: {output_parquet}", fg="cyan"))
        click.echo(
            click.style(
                f"-- Geometry columns: {input_geom_col} (input), {admin_geom_col} (admin)",
                fg="cyan",
            )
        )
        click.echo(
            click.style(
                f"-- Bbox columns: {input_bbox_col or 'none'} (input), {admin_bbox_col or 'none'} (admin)\n",
                fg="cyan",
            )
        )

    # Get metadata before processing (skip in dry-run)
    metadata = None
    if not dry_run:
        metadata, _ = get_parquet_metadata(input_parquet, verbose)

        # Check bbox structure and provide warnings
        if input_bbox_info["status"] != "optimal":
            click.echo(
                click.style(
                    "\nWarning: Input file could benefit from bbox optimization:\n"
                    + input_bbox_info["message"],
                    fg="yellow",
                )
            )
            if add_bbox_flag:
                if not input_bbox_info["has_bbox_column"]:
                    click.echo("Adding bbox column to input file...")
                    from geoparquet_io.core.common import add_bbox

                    add_bbox(input_parquet, "bbox", verbose)
                    click.echo(
                        click.style("✓ Added bbox column and metadata to input file", fg="green")
                    )
                    # Re-check after adding bbox
                    input_bbox_info = check_bbox_structure(input_parquet, verbose)
                    input_bbox_col = input_bbox_info["bbox_column_name"]

        if verbose:
            click.echo(
                f"Using geometry columns: {input_geom_col} (input), {admin_geom_col} (admin)"
            )

    # Create DuckDB connection and load spatial extension
    con = duckdb.connect()
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    # Configure S3 access for source.coop
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("SET s3_endpoint='data.source.coop';")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_use_ssl=true;")

    # Get total input count (skip in dry-run)
    if not dry_run:
        total_count = con.execute(f"SELECT COUNT(*) FROM '{input_url}'").fetchone()[0]
        click.echo(f"Processing {total_count:,} input features...")

    # Prepare admin data source
    admin_table_ref = dataset.prepare_data_source(con)

    # Build SELECT clause for admin columns
    # Create column aliases based on level names
    admin_select_parts = []
    for level, col in zip(levels, partition_columns):
        # Use level name as the output column name with admin: prefix
        output_col_name = f"admin:{level}"
        admin_select_parts.append(f'b."{col}" as "{output_col_name}"')

    admin_select_clause = ", ".join(admin_select_parts)

    # Build spatial join query based on bbox availability
    if input_bbox_col and admin_bbox_col:
        if verbose and not dry_run:
            click.echo("Using bbox columns for initial filtering...")

        # Build bbox intersection condition
        # Handle different bbox column types (STRUCT vs separate columns)
        bbox_condition = f"""(a.{input_bbox_col}.xmin <= b.{admin_bbox_col}.xmax AND
        a.{input_bbox_col}.xmax >= b.{admin_bbox_col}.xmin AND
        a.{input_bbox_col}.ymin <= b.{admin_bbox_col}.ymax AND
        a.{input_bbox_col}.ymax >= b.{admin_bbox_col}.ymin)"""

        query = f"""
    SELECT
        a.*,
        {admin_select_clause}
    FROM '{input_url}' a
    LEFT JOIN {admin_table_ref} b
    ON {bbox_condition}  -- Fast bbox intersection test
        AND ST_Intersects(  -- More expensive precise check only on bbox matches
            b.{admin_geom_col},
            a.{input_geom_col}
        )
"""
    else:
        if not dry_run:
            click.echo("No bbox columns available, using full geometry intersection...")

        query = f"""
    SELECT
        a.*,
        {admin_select_clause}
    FROM '{input_url}' a
    LEFT JOIN {admin_table_ref} b
    ON ST_Intersects(b.{admin_geom_col}, a.{input_geom_col})
"""

    if dry_run:
        # In dry-run mode, just show the query
        click.echo(click.style("-- Main spatial join query", fg="cyan"))
        if input_bbox_col and admin_bbox_col:
            click.echo(click.style("-- Using bbox columns for optimized spatial join", fg="cyan"))
        else:
            click.echo(
                click.style("-- Using full geometry intersection (no bbox optimization)", fg="cyan")
            )

        # Show the query with COPY wrapper for display
        if compression in ["GZIP", "ZSTD", "BROTLI"]:
            compression_str = f"{compression}:{compression_level}"
        else:
            compression_str = compression

        duckdb_compression = (
            compression.lower() if compression != "UNCOMPRESSED" else "uncompressed"
        )
        display_query = f"""COPY ({query.strip()})
TO '{output_parquet}'
(FORMAT PARQUET, COMPRESSION '{duckdb_compression}');"""
        click.echo(display_query)

        click.echo(click.style(f"\n-- Note: Using {compression_str} compression", fg="cyan"))
        click.echo(
            click.style(
                "-- Original metadata would also be preserved in the output file", fg="cyan"
            )
        )
        con.close()
        return

    # Execute the query using the common write method
    if verbose:
        click.echo("Performing spatial join with admin boundaries...")

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
    )

    # Get statistics about the results
    # Build condition to check if ANY admin column is not null
    admin_cols_check = " OR ".join([f'"admin:{level}" IS NOT NULL' for level in levels])

    stats_query = f"""
    SELECT
        COUNT(*) as total_features,
        COUNT(CASE WHEN {admin_cols_check} THEN 1 END) as features_with_admin
    FROM '{output_parquet}';
    """

    stats = con.execute(stats_query).fetchone()
    total_features = stats[0]
    features_with_admin = stats[1]

    # Get count of unique values for each admin level
    unique_counts = []
    for level in levels:
        count_query = f"""
        SELECT COUNT(DISTINCT "admin:{level}") as unique_count
        FROM '{output_parquet}'
        WHERE "admin:{level}" IS NOT NULL;
        """
        result = con.execute(count_query).fetchone()
        unique_counts.append((level, result[0]))

    con.close()

    click.echo("\nResults:")
    click.echo(
        f"- Added admin division data to {features_with_admin:,} of {total_features:,} features"
    )
    for level, count in unique_counts:
        click.echo(f"- Found {count:,} unique {level} values")

    click.echo(f"\nSuccessfully wrote output to: {output_parquet}")
