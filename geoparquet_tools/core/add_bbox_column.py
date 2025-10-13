#!/usr/bin/env python3

import click
import os
import duckdb
from geoparquet_tools.core.common import (
    safe_file_url, find_primary_geometry_column, get_parquet_metadata,
    update_metadata, check_bbox_structure
)
import pyarrow.parquet as pq
import fsspec

def add_bbox_column(input_parquet, output_parquet, bbox_column_name='bbox', dry_run=False, verbose=False):
    """
    Add a bbox struct column to a GeoParquet file.

    Args:
        input_parquet: Path to the input parquet file
        output_parquet: Path to the output parquet file
        bbox_column_name: Name for the bbox column (default: 'bbox')
        dry_run: Whether to print SQL commands without executing them
        verbose: Whether to print verbose output
    """
    # Get safe URL for input file
    input_url = safe_file_url(input_parquet, verbose)

    # Get geometry column
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # Start dry-run mode output if needed
    if dry_run:
        click.echo(click.style("\n=== DRY RUN MODE - SQL Commands that would be executed ===\n", fg="yellow", bold=True))
        click.echo(click.style(f"-- Input file: {input_url}", fg="cyan"))
        click.echo(click.style(f"-- Output file: {output_parquet}", fg="cyan"))
        click.echo(click.style(f"-- Geometry column: {geom_col}", fg="cyan"))
        click.echo(click.style(f"-- Bbox column name: {bbox_column_name}\n", fg="cyan"))

    # Check if the requested column name already exists (skip in dry-run)
    if not dry_run:
        with fsspec.open(input_url, 'rb') as f:
            pf = pq.ParquetFile(f)
            schema = pf.schema_arrow

        for field in schema:
            if field.name == bbox_column_name:
                raise click.ClickException(f"Column '{bbox_column_name}' already exists in the file. Please choose a different name.")

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

    # Build the query to add bbox column
    query = f"""
    COPY (
        SELECT
            *,
            STRUCT_PACK(
                xmin := ST_XMin({geom_col}),
                ymin := ST_YMin({geom_col}),
                xmax := ST_XMax({geom_col}),
                ymax := ST_YMax({geom_col})
            ) AS {bbox_column_name}
        FROM '{input_url}'
    )
    TO '{output_parquet}'
    (FORMAT PARQUET, COMPRESSION 'ZSTD', COMPRESSION_LEVEL 15);
    """

    if dry_run:
        # In dry-run mode, just show the query
        click.echo(click.style("-- Main query to add bbox column:", fg="cyan"))
        display_query = query.strip()
        click.echo(display_query)

        click.echo(click.style("\n-- Note: This query creates a new parquet file with a bbox struct column added", fg="cyan"))
        click.echo(click.style("-- The bbox column contains (xmin, ymin, xmax, ymax) for each geometry", fg="cyan"))
        click.echo(click.style("-- Metadata would also be updated with bbox covering information", fg="cyan"))
        return

    # Execute the query
    if verbose:
        click.echo(f"Creating bbox column '{bbox_column_name}'...")

    con.execute(query)

    # Update the output file with the original metadata and bbox covering
    if metadata:
        # Read the output file to update metadata
        table = pq.read_table(output_parquet)
        existing_metadata = table.schema.metadata or {}
        new_metadata = {
            k: v for k, v in existing_metadata.items()
            if not k.decode('utf-8').startswith('geo')  # Remove existing geo metadata
        }

        # Get or create geo metadata
        try:
            if b'geo' in metadata:
                import json
                geo_meta = json.loads(metadata[b'geo'].decode('utf-8'))
            else:
                geo_meta = {
                    "version": "1.1.0",
                    "primary_column": geom_col,
                    "columns": {}
                }
        except json.JSONDecodeError:
            import json
            geo_meta = {
                "version": "1.1.0",
                "primary_column": geom_col,
                "columns": {}
            }

        # Ensure proper structure
        if "columns" not in geo_meta:
            geo_meta["columns"] = {}
        if geom_col not in geo_meta["columns"]:
            geo_meta["columns"][geom_col] = {}

        # Add bbox covering metadata
        geo_meta["columns"][geom_col]["covering"] = {
            "bbox": {
                "xmin": [bbox_column_name, "xmin"],
                "ymin": [bbox_column_name, "ymin"],
                "xmax": [bbox_column_name, "xmax"],
                "ymax": [bbox_column_name, "ymax"]
            }
        }

        # Add updated geo metadata
        import json
        new_metadata[b'geo'] = json.dumps(geo_meta).encode('utf-8')

        # Update table schema with new metadata
        new_table = table.replace_schema_metadata(new_metadata)

        # Calculate optimal row groups
        file_size = os.path.getsize(output_parquet)
        from geoparquet_tools.core.common import calculate_row_group_count
        num_row_groups = calculate_row_group_count(new_table.num_rows, file_size)
        rows_per_group = new_table.num_rows // num_row_groups

        # Rewrite with updated metadata and optimized settings
        pq.write_table(
            new_table,
            output_parquet,
            row_group_size=rows_per_group,
            compression='ZSTD',
            compression_level=15,
            write_statistics=True,
            use_dictionary=True,
            version='2.6'
        )

        if verbose:
            click.echo("Updated output file with bbox covering metadata")

    click.echo(f"Successfully added bbox column '{bbox_column_name}' to: {output_parquet}")

if __name__ == "__main__":
    add_bbox_column()