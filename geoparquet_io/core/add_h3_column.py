#!/usr/bin/env python3

import click

from geoparquet_io.core.common import add_computed_column, find_primary_geometry_column


def add_h3_column(
    input_parquet,
    output_parquet,
    h3_column_name="h3_cell",
    h3_resolution=9,
    dry_run=False,
    verbose=False,
    compression="ZSTD",
    compression_level=None,
    row_group_size_mb=None,
    row_group_rows=None,
    profile=None,
):
    """
    Add an H3 cell ID column to a GeoParquet file.

    Computes H3 cell IDs based on geometry centroids using the H3
    hierarchical hexagonal grid system. The cell ID is stored as a
    VARCHAR (string) for maximum portability.

    Args:
        input_parquet: Path to the input parquet file (local or remote URL)
        output_parquet: Path to the output parquet file (local or remote URL)
        h3_column_name: Name for the H3 column (default: 'h3_cell')
        h3_resolution: H3 resolution level (0-15)
                      Res 7: ~5 km², Res 9: ~0.1 km², Res 11: ~1,770 m²,
                      Res 13: ~44 m², Res 15: ~0.9 m²
                      Default: 9 (good balance for most use cases)
        dry_run: Whether to print SQL commands without executing them
        verbose: Whether to print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name (S3 only, optional)
    """
    # Validate resolution
    if not 0 <= h3_resolution <= 15:
        raise click.BadParameter(f"H3 resolution must be between 0 and 15, got {h3_resolution}")

    # Get geometry column for the SQL expression
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # Define the H3 SQL expression (using string format for portability)
    sql_expression = f"""h3_latlng_to_cell_string(
        ST_Y(ST_Centroid({geom_col})),
        ST_X(ST_Centroid({geom_col})),
        {h3_resolution}
    )"""

    # Prepare H3 metadata for GeoParquet spec
    h3_metadata = {"covering": {"h3": {"column": h3_column_name, "resolution": h3_resolution}}}

    # Use the generic helper
    add_computed_column(
        input_parquet=input_parquet,
        output_parquet=output_parquet,
        column_name=h3_column_name,
        sql_expression=sql_expression,
        extensions=["h3"],  # Load H3 extension from DuckDB community
        dry_run=dry_run,
        verbose=verbose,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        dry_run_description=f"H3 cell ID at resolution {h3_resolution} (~{_get_resolution_size(h3_resolution)})",
        custom_metadata=h3_metadata,
        profile=profile,
    )

    if not dry_run:
        click.echo(
            f"Successfully added H3 column '{h3_column_name}' "
            f"(resolution {h3_resolution}) to: {output_parquet}"
        )


def _get_resolution_size(resolution):
    """Get approximate cell size for a given H3 resolution."""
    sizes = {
        0: "4,357 km²",
        1: "609 km²",
        2: "87 km²",
        3: "12 km²",
        4: "1.8 km²",
        5: "0.26 km²",
        6: "36,000 m²",
        7: "5,200 m²",
        8: "730 m²",
        9: "105 m²",
        10: "15 m²",
        11: "2.2 m²",
        12: "0.31 m²",
        13: "0.04 m²",
        14: "0.006 m²",
        15: "0.0009 m²",
    }
    return sizes.get(resolution, f"resolution {resolution}")


if __name__ == "__main__":
    add_h3_column()
