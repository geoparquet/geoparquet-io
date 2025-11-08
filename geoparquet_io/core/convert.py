#!/usr/bin/env python3

import os
import time

import click
import duckdb

from geoparquet_io.core.common import (
    format_size,
    write_parquet_with_metadata,
)


def _validate_inputs(input_file, output_file):
    """Validate input file and output directory."""
    if not os.path.exists(input_file):
        raise click.ClickException(f"Input file not found: {input_file}")

    output_dir = os.path.dirname(output_file) or "."
    if not os.path.exists(output_dir):
        raise click.ClickException(f"Output directory not found: {output_dir}")
    if not os.access(output_dir, os.W_OK):
        raise click.ClickException(f"No write permission for: {output_dir}")


def _setup_duckdb():
    """Create and configure DuckDB connection."""
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        return con
    except Exception as e:
        con.close()
        raise click.ClickException(f"Failed to load DuckDB spatial extension: {str(e)}") from e


def _detect_geometry_column(con, input_file, verbose):
    """Detect geometry column name from input file."""
    if verbose:
        click.echo("Detecting geometry column from input...")

    detect_query = f"SELECT * FROM ST_Read('{input_file}') LIMIT 0"
    schema_result = con.execute(detect_query).description

    for col_info in schema_result:
        col_name = col_info[0].lower()
        if col_name in ["geom", "geometry", "wkb_geometry", "shape"]:
            if verbose:
                click.echo(f"Detected geometry column: {col_info[0]}")
            return col_info[0]

    raise click.ClickException(
        "Could not detect geometry column in input file. "
        "Expected column named 'geom', 'geometry', 'wkb_geometry', or 'shape'."
    )


def _calculate_bounds(con, input_file, geom_column, verbose):
    """Calculate dataset bounds from input file."""
    if verbose:
        click.echo("Calculating dataset bounds...")

    bounds_query = f"""
        SELECT
            MIN(ST_XMin({geom_column})) as xmin,
            MIN(ST_YMin({geom_column})) as ymin,
            MAX(ST_XMax({geom_column})) as xmax,
            MAX(ST_YMax({geom_column})) as ymax
        FROM ST_Read('{input_file}')
    """
    bounds_result = con.execute(bounds_query).fetchone()

    if not bounds_result or any(v is None for v in bounds_result):
        raise click.ClickException("Could not calculate dataset bounds")

    if verbose:
        xmin, ymin, xmax, ymax = bounds_result
        click.echo(f"Dataset bounds: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")

    return bounds_result


def _build_conversion_query(input_file, geom_column, skip_hilbert, bounds=None):
    """Build SQL query for conversion with optional Hilbert ordering."""
    base_select = f"""
        SELECT
            * EXCLUDE ({geom_column}),
            {geom_column} AS geometry,
            STRUCT_PACK(
                xmin := ST_XMin({geom_column}),
                ymin := ST_YMin({geom_column}),
                xmax := ST_XMax({geom_column}),
                ymax := ST_YMax({geom_column})
            ) AS bbox
        FROM ST_Read('{input_file}')
    """

    if skip_hilbert:
        return base_select

    xmin, ymin, xmax, ymax = bounds
    return f"""{base_select}
        ORDER BY ST_Hilbert(
            {geom_column},
            ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))
        )
    """


def convert_to_geoparquet(
    input_file,
    output_file,
    skip_hilbert=False,
    verbose=False,
    compression="ZSTD",
    compression_level=15,
    row_group_rows=100000,
):
    """
    Convert vector format to optimized GeoParquet.

    Applies best practices:
    - ZSTD compression
    - 100k row groups
    - Bbox column with metadata
    - Hilbert spatial ordering (unless --skip-hilbert)
    - GeoParquet 1.1.0 metadata

    Args:
        input_file: Path to input file (Shapefile, GeoJSON, GeoPackage, etc.)
        output_file: Path to output GeoParquet file
        skip_hilbert: Skip Hilbert ordering (faster, less optimal)
        verbose: Print detailed progress
        compression: Compression type (default: ZSTD)
        compression_level: Compression level (default: 15)
        row_group_rows: Rows per group (default: 100000)

    Raises:
        click.ClickException: If input file not found or conversion fails
    """
    start_time = time.time()

    _validate_inputs(input_file, output_file)
    click.echo(f"Converting {input_file}...")

    con = _setup_duckdb()

    try:
        geom_column = _detect_geometry_column(con, input_file, verbose)

        bounds = None
        if not skip_hilbert:
            bounds = _calculate_bounds(con, input_file, geom_column, verbose)

        if verbose:
            msg = "Reading input and adding bbox column..."
            if not skip_hilbert:
                msg = "Pass 1: Reading input, adding bbox, and applying Hilbert ordering..."
            click.echo(msg)

        query = _build_conversion_query(input_file, geom_column, skip_hilbert, bounds)

        write_parquet_with_metadata(
            con,
            query,
            output_file,
            original_metadata=None,
            compression=compression,
            compression_level=compression_level,
            row_group_rows=row_group_rows,
            verbose=verbose,
        )

        # Report results
        elapsed = time.time() - start_time
        file_size = os.path.getsize(output_file)

        click.echo(f"Done in {elapsed:.1f}s")
        click.echo(f"Output: {output_file} ({format_size(file_size)})")
        click.echo(click.style("✓ Output passes GeoParquet validation", fg="green"))

    except duckdb.IOException as e:
        con.close()
        raise click.ClickException(f"Failed to read input file: {str(e)}") from e

    except duckdb.BinderException as e:
        con.close()
        raise click.ClickException(f"Invalid geometry data: {str(e)}") from e

    except OSError as e:
        con.close()
        if e.errno == 28:  # ENOSPC
            raise click.ClickException("Not enough disk space for output file") from e
        else:
            raise click.ClickException(f"File system error: {str(e)}") from e

    except Exception as e:
        con.close()
        raise click.ClickException(f"Conversion failed: {str(e)}") from e

    finally:
        con.close()


if __name__ == "__main__":
    convert_to_geoparquet()
