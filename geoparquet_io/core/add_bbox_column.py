#!/usr/bin/env python3


import click
import duckdb
import fsspec
import pyarrow.parquet as pq

from geoparquet_io.core.common import (
    find_primary_geometry_column,
    get_parquet_metadata,
    safe_file_url,
    write_parquet_with_metadata,
)


def add_bbox_column(
    input_parquet,
    output_parquet,
    bbox_column_name="bbox",
    dry_run=False,
    verbose=False,
    compression="ZSTD",
    compression_level=None,
    row_group_size_mb=None,
    row_group_rows=None,
):
    """
    Add a bbox struct column to a GeoParquet file.

    Args:
        input_parquet: Path to the input parquet file
        output_parquet: Path to the output parquet file
        bbox_column_name: Name for the bbox column (default: 'bbox')
        dry_run: Whether to print SQL commands without executing them
        verbose: Whether to print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
    """
    # Get safe URL for input file
    input_url = safe_file_url(input_parquet, verbose)

    # Get geometry column
    geom_col = find_primary_geometry_column(input_parquet, verbose)

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
        click.echo(click.style(f"-- Output file: {output_parquet}", fg="cyan"))
        click.echo(click.style(f"-- Geometry column: {geom_col}", fg="cyan"))
        click.echo(click.style(f"-- Bbox column name: {bbox_column_name}\n", fg="cyan"))

    # Check if the requested column name already exists (skip in dry-run)
    if not dry_run:
        with fsspec.open(input_url, "rb") as f:
            pf = pq.ParquetFile(f)
            schema = pf.schema_arrow

        for field in schema:
            if field.name == bbox_column_name:
                raise click.ClickException(
                    f"Column '{bbox_column_name}' already exists in the file. Please choose a different name."
                )

        # Get metadata before processing
        metadata, _ = get_parquet_metadata(input_parquet, verbose)

        if verbose:
            click.echo(f"Adding bbox column '{bbox_column_name}' for geometry column: {geom_col}")

    # Create DuckDB connection and load spatial extension
    con = duckdb.connect()
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    # Get total count (skip in dry-run)
    if not dry_run:
        total_count = con.execute(f"SELECT COUNT(*) FROM '{input_url}'").fetchone()[0]
        click.echo(f"Processing {total_count:,} features...")

    # Build the query to add bbox column (without COPY wrapper for new method)
    query = f"""
        SELECT
            *,
            STRUCT_PACK(
                xmin := ST_XMin({geom_col}),
                ymin := ST_YMin({geom_col}),
                xmax := ST_XMax({geom_col}),
                ymax := ST_YMax({geom_col})
            ) AS {bbox_column_name}
        FROM '{input_url}'
    """

    if dry_run:
        # In dry-run mode, show the query with COPY wrapper
        if compression in ["GZIP", "ZSTD", "BROTLI"]:
            compression_str = f"{compression}:{compression_level}"
        else:
            compression_str = compression

        # Use lowercase for DuckDB format
        duckdb_compression = (
            compression.lower() if compression != "UNCOMPRESSED" else "uncompressed"
        )
        display_query = f"""COPY ({query.strip()})
TO '{output_parquet}'
(FORMAT PARQUET, COMPRESSION '{duckdb_compression}');"""

        click.echo(click.style("-- Main query to add bbox column:", fg="cyan"))
        click.echo(display_query)

        click.echo(click.style(f"\n-- Note: Using {compression_str} compression", fg="cyan"))
        click.echo(
            click.style(
                "-- This query creates a new parquet file with a bbox struct column added",
                fg="cyan",
            )
        )
        click.echo(
            click.style(
                "-- The bbox column contains (xmin, ymin, xmax, ymax) for each geometry", fg="cyan"
            )
        )
        click.echo(
            click.style(
                "-- Metadata would also be updated with bbox covering information (GeoParquet 1.1)",
                fg="cyan",
            )
        )
        return

    # Execute the query using the common write method
    if verbose:
        click.echo(f"Creating bbox column '{bbox_column_name}'...")

    # Get metadata before processing
    metadata = None
    if not dry_run:
        metadata, _ = get_parquet_metadata(input_parquet, verbose)

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

    click.echo(f"Successfully added bbox column '{bbox_column_name}' to: {output_parquet}")


if __name__ == "__main__":
    add_bbox_column()
