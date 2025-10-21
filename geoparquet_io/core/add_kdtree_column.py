#!/usr/bin/env python3

import click
import fsspec
import pyarrow.parquet as pq

from geoparquet_io.core.common import (
    find_primary_geometry_column,
    safe_file_url,
)


def add_kdtree_column(
    input_parquet,
    output_parquet,
    kdtree_column_name="kdtree_cell",
    iterations=9,
    dry_run=False,
    verbose=False,
    compression="ZSTD",
    compression_level=None,
    row_group_size_mb=None,
    row_group_rows=None,
    force=False,
):
    """
    Add a KD-tree cell ID column to a GeoParquet file.

    Computes KD-tree partition IDs based on recursive spatial splits
    alternating between X and Y dimensions at medians. The partition
    ID is stored as a binary string (e.g., "01011001").

    Performance Note: Runtime scales with dataset size × iterations.
    For datasets > 50M rows, consider hierarchical partitioning
    (partition by country/region first, then apply KD-tree within
    each partition).

    Args:
        input_parquet: Path to the input parquet file
        output_parquet: Path to the output parquet file
        kdtree_column_name: Name for the KD-tree column (default: 'kdtree_cell')
        iterations: Number of recursive splits (1-20).
                   iterations=5: 32 partitions, iterations=9: 512 partitions,
                   iterations=12: 4,096 partitions.
                   Default: 9 (512 partitions)
        dry_run: Whether to print SQL commands without executing them
        verbose: Whether to print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        force: Force operation even on large datasets (not recommended)
    """
    # Validate iterations
    if not 1 <= iterations <= 20:
        raise click.BadParameter(f"Iterations must be between 1 and 20, got {iterations}")

    # Get geometry column for the SQL expression
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # Check dataset size - KD-tree computation is expensive on large datasets
    if not dry_run and not force:
        safe_url = safe_file_url(input_parquet, verbose)
        with fsspec.open(safe_url, "rb") as f:
            pf = pq.ParquetFile(f)
            total_rows = pf.metadata.num_rows

        if total_rows > 50_000_000:
            error_msg = click.style("\n⚠️  Large Dataset Warning\n", fg="yellow", bold=True)
            error_msg += click.style(
                f"\nThis dataset has {total_rows:,} rows. KD-tree computation runtime scales with "
                f"dataset size × iterations ({total_rows:,} × {iterations} = extremely slow).\n",
                fg="yellow",
            )
            error_msg += click.style("\nRecommended approach:", fg="cyan", bold=True)
            error_msg += click.style(
                "\n  1. Use hierarchical partitioning: partition by a coarser key first "
                "(country/region/state),\n     then apply KD-tree within each smaller partition.\n",
                fg="cyan",
            )
            error_msg += click.style(
                "  2. For example:\n"
                "     - First: gpio partition string <file> <output> --column state\n"
                "     - Then: For each state partition, gpio add kdtree <state_file> <output>\n",
                fg="cyan",
            )
            error_msg += click.style(
                "\nIf you still want to proceed (not recommended), use --force to override this check.\n",
                fg="yellow",
            )
            raise click.ClickException(error_msg)

        if verbose and total_rows > 10_000_000:
            click.echo(
                click.style(
                    f"⚠️  Processing {total_rows:,} rows - this may take several minutes...",
                    fg="yellow",
                )
            )

    # KD-tree requires a full table scan with recursive CTE - can't be done as a simple column expression
    # We need to use a different approach than add_computed_column
    # Build a query that selects all original columns plus the KD-tree partition ID

    input_url = safe_file_url(input_parquet, verbose)

    # Create DuckDB connection
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    if not dry_run:
        # Get total count
        total_count = con.execute(f"SELECT COUNT(*) FROM '{input_url}'").fetchone()[0]
        click.echo(f"Processing {total_count:,} features...")

    # Build the full KD-tree query following the tutorial pattern
    # https://duckdb.org/2024/09/09/spatial-extension.html
    query = f"""
        WITH RECURSIVE kdtree(iteration, x, y, partition_id, row_id) AS (
            SELECT
                0 AS iteration,
                ST_X(ST_Centroid({geom_col})) AS x,
                ST_Y(ST_Centroid({geom_col})) AS y,
                '0' AS partition_id,
                ROW_NUMBER() OVER () AS row_id
            FROM '{input_url}'

            UNION ALL

            SELECT
                iteration + 1 AS iteration,
                x,
                y,
                IF(
                    IF(MOD(iteration, 2) = 0, x, y) < APPROX_QUANTILE(
                        IF(MOD(iteration, 2) = 0, x, y),
                        0.5
                    ) OVER (
                        PARTITION BY partition_id
                    ),
                    partition_id || '0',
                    partition_id || '1'
                ) AS partition_id,
                row_id
            FROM kdtree
            WHERE
                iteration < {iterations}
        ),
        kdtree_final AS (
            SELECT row_id, partition_id
            FROM kdtree
            WHERE iteration = {iterations}
        ),
        original_with_rownum AS (
            SELECT *, ROW_NUMBER() OVER () AS row_id
            FROM '{input_url}'
        )
        SELECT original_with_rownum.* EXCLUDE (row_id), kdtree_final.partition_id AS {kdtree_column_name}
        FROM original_with_rownum
        JOIN kdtree_final ON original_with_rownum.row_id = kdtree_final.row_id
    """

    # Prepare KD-tree metadata for GeoParquet spec
    partition_count = 2**iterations
    kdtree_metadata = {
        "covering": {
            "kdtree": {
                "column": kdtree_column_name,
                "iterations": iterations,
                "partitions": partition_count,
            }
        }
    }

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
        click.echo(click.style(f"-- New column: {kdtree_column_name}", fg="cyan"))
        click.echo(
            click.style(f"-- Iterations: {iterations} (~{partition_count} partitions)", fg="cyan")
        )
        click.echo()
        click.echo(query)
        return

    # Get metadata before processing
    from geoparquet_io.core.common import get_parquet_metadata, write_parquet_with_metadata

    metadata, _ = get_parquet_metadata(input_parquet, verbose)

    # Execute the query and write output
    write_parquet_with_metadata(
        con,
        query,
        output_parquet,
        original_metadata=metadata,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        custom_metadata=kdtree_metadata,
        verbose=verbose,
    )

    con.close()

    if not dry_run:
        click.echo(
            f"Successfully added KD-tree column '{kdtree_column_name}' "
            f"({iterations} iterations, ~{partition_count} partitions) to: {output_parquet}"
        )


if __name__ == "__main__":
    add_kdtree_column()
