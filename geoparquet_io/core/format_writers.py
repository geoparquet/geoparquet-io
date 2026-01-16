"""
Writers for converting GeoParquet to various output formats.

Supports writing GeoParquet files to:
- GeoPackage (.gpkg)
- FlatGeobuf (.fgb)
- CSV with WKT (.csv)
- Shapefile (.shp)

All writers use DuckDB's spatial extension and GDAL drivers for format conversion.
Writers handle local file output only; remote uploads are handled by the upload module.
"""

import tempfile
import uuid
from pathlib import Path

import click
import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.common import (
    get_duckdb_connection,
    is_remote_url,
    needs_httpfs,
    safe_file_url,
    setup_aws_profile_if_needed,
    validate_output_path,
    validate_profile_for_urls,
)
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success


def _table_to_temp_parquet(table: pa.Table, prefix: str = "gpio_write") -> Path:
    """
    Write PyArrow Table to temporary parquet file.

    Args:
        table: PyArrow Table to write
        prefix: Prefix for temp filename

    Returns:
        Path to temporary parquet file
    """
    temp_dir = Path(tempfile.gettempdir())
    temp_path = temp_dir / f"{prefix}_{uuid.uuid4()}.parquet"
    pq.write_table(table, str(temp_path))
    return temp_path


def write_geopackage(
    input_path: str,
    output_path: str,
    overwrite: bool = False,
    layer_name: str = "features",
    verbose: bool = False,
    profile: str | None = None,
) -> str:
    """
    Convert GeoParquet to GeoPackage format.

    Uses DuckDB's spatial extension with GDAL GeoPackage driver.
    Creates spatial index by default (GDAL standard behavior).

    Args:
        input_path: Path to input GeoParquet file or PyArrow Table
        output_path: Path to output GeoPackage file (must be local)
        overwrite: Overwrite existing file if True
        layer_name: Layer name in GeoPackage (default: 'features')
        verbose: Print verbose output
        profile: AWS profile for S3 input files

    Returns:
        Path to output file

    Raises:
        click.ClickException: If output exists and overwrite=False, or conversion fails
    """
    configure_verbose(verbose)

    # Validate inputs
    if is_remote_url(output_path):
        raise click.ClickException(
            "GeoPackage output path must be local. Use upload() for cloud destinations."
        )

    validate_profile_for_urls(profile, input_path)
    setup_aws_profile_if_needed(profile, input_path)

    output_file = Path(output_path)

    # Check if output exists
    if output_file.exists() and not overwrite:
        raise click.ClickException(
            f"Output file already exists: {output_path}\nUse --overwrite to replace existing file."
        )

    # Validate output directory exists
    validate_output_path(output_path, verbose)

    progress(f"Converting to GeoPackage: {output_path}")

    # Get DuckDB connection
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_path))

    try:
        # Build safe input URL
        input_url = safe_file_url(input_path, verbose)

        # Read table and write to GeoPackage using GDAL
        # GDAL will automatically create spatial index
        query = f"""
            COPY (SELECT * FROM read_parquet('{input_url}'))
            TO '{output_path}'
            WITH (FORMAT GDAL, DRIVER 'GPKG', LAYER_CREATION_OPTIONS 'LAYER_NAME={layer_name}')
        """

        debug(f"Executing: {query}")
        con.execute(query)

        success(f"Created GeoPackage: {output_path}")
        return output_path

    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower():
            raise click.ClickException(
                f"GeoPackage file already exists: {output_path}\nUse --overwrite to replace it."
            ) from e
        raise click.ClickException(f"Failed to create GeoPackage: {error_msg}") from e
    finally:
        con.close()


def write_flatgeobuf(
    input_path: str,
    output_path: str,
    verbose: bool = False,
    profile: str | None = None,
) -> str:
    """
    Convert GeoParquet to FlatGeobuf format.

    Uses DuckDB's spatial extension with GDAL FlatGeobuf driver.
    Creates spatial index by default (FlatGeobuf standard).

    Args:
        input_path: Path to input GeoParquet file
        output_path: Path to output FlatGeobuf file (must be local)
        verbose: Print verbose output
        profile: AWS profile for S3 input files

    Returns:
        Path to output file

    Raises:
        click.ClickException: If conversion fails
    """
    configure_verbose(verbose)

    # Validate inputs
    if is_remote_url(output_path):
        raise click.ClickException(
            "FlatGeobuf output path must be local. Use upload() for cloud destinations."
        )

    validate_profile_for_urls(profile, input_path)
    setup_aws_profile_if_needed(profile, input_path)

    # Validate output directory exists
    validate_output_path(output_path, verbose)

    progress(f"Converting to FlatGeobuf: {output_path}")

    # Get DuckDB connection
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_path))

    try:
        # Build safe input URL
        input_url = safe_file_url(input_path, verbose)

        # Write to FlatGeobuf using GDAL
        # Spatial index is created automatically by FlatGeobuf driver
        query = f"""
            COPY (SELECT * FROM read_parquet('{input_url}'))
            TO '{output_path}'
            WITH (FORMAT GDAL, DRIVER 'FlatGeobuf')
        """

        debug(f"Executing: {query}")
        con.execute(query)

        success(f"Created FlatGeobuf: {output_path}")
        return output_path

    except Exception as e:
        raise click.ClickException(f"Failed to create FlatGeobuf: {str(e)}") from e
    finally:
        con.close()


def write_csv(
    input_path: str,
    output_path: str,
    include_wkt: bool = True,
    include_bbox: bool = True,
    verbose: bool = False,
    profile: str | None = None,
) -> str:
    """
    Convert GeoParquet to CSV format with optional WKT geometry.

    Converts geometry column to WKT text representation.
    Complex types (STRUCT, LIST, MAP) are JSON-encoded.
    Bbox column is included if present in input.

    Args:
        input_path: Path to input GeoParquet file
        output_path: Path to output CSV file (must be local)
        include_wkt: Include geometry as WKT column (default: True)
        include_bbox: Include bbox column if present (default: True)
        verbose: Print verbose output
        profile: AWS profile for S3 input files

    Returns:
        Path to output file

    Raises:
        click.ClickException: If conversion fails
    """
    configure_verbose(verbose)

    # Validate inputs
    if is_remote_url(output_path):
        raise click.ClickException(
            "CSV output path must be local. Use upload() for cloud destinations."
        )

    validate_profile_for_urls(profile, input_path)
    setup_aws_profile_if_needed(profile, input_path)

    # Validate output directory exists
    validate_output_path(output_path, verbose)

    progress(f"Converting to CSV: {output_path}")

    # Get DuckDB connection
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_path))

    try:
        # Build safe input URL
        input_url = safe_file_url(input_path, verbose)

        # Read parquet to inspect schema
        pf = pq.ParquetFile(input_url)
        schema = pf.schema_arrow
        columns = [field.name for field in schema]

        # Find geometry column
        geom_col = None
        for col in ["geometry", "geom", "wkb_geometry"]:
            if col in columns:
                geom_col = col
                break

        if not geom_col:
            raise click.ClickException(
                "No geometry column found. Expected 'geometry', 'geom', or 'wkb_geometry'."
            )

        # Build column list
        select_cols = []
        for col in columns:
            if col == geom_col:
                if include_wkt:
                    # Convert geometry to WKT
                    select_cols.append(f'ST_AsText("{col}") as wkt')
            elif col == "bbox":
                if include_bbox:
                    # Convert bbox struct to JSON
                    select_cols.append(f'to_json("{col}") as bbox')
            else:
                # Check if column is complex type, JSON-encode if needed
                field = schema.field(col)
                if (
                    pa.types.is_struct(field.type)
                    or pa.types.is_list(field.type)
                    or pa.types.is_map(field.type)
                ):
                    select_cols.append(f'to_json("{col}") as "{col}"')
                else:
                    select_cols.append(f'"{col}"')

        if not select_cols:
            raise click.ClickException("No columns to export after filtering geometry.")

        select_expr = ", ".join(select_cols)

        # Write to CSV
        query = f"""
            COPY (
                SELECT {select_expr}
                FROM read_parquet('{input_url}')
            )
            TO '{output_path}'
            WITH (HEADER TRUE, DELIMITER ',')
        """

        debug(f"Executing: {query}")
        con.execute(query)

        success(f"Created CSV: {output_path}")
        return output_path

    except Exception as e:
        raise click.ClickException(f"Failed to create CSV: {str(e)}") from e
    finally:
        con.close()


def write_shapefile(
    input_path: str,
    output_path: str,
    overwrite: bool = False,
    encoding: str = "UTF-8",
    verbose: bool = False,
    profile: str | None = None,
) -> str:
    """
    Convert GeoParquet to Shapefile format.

    Uses DuckDB's spatial extension with GDAL Shapefile driver.

    Note: Shapefiles have limitations:
    - Column names truncated to 10 characters
    - File size limit of 2GB
    - Limited data type support
    - Multiple files created (.shp, .shx, .dbf, .prj)

    Args:
        input_path: Path to input GeoParquet file
        output_path: Path to output Shapefile (must be local)
        overwrite: Overwrite existing file if True
        encoding: Character encoding (default: UTF-8)
        verbose: Print verbose output
        profile: AWS profile for S3 input files

    Returns:
        Path to output file

    Raises:
        click.ClickException: If output exists and overwrite=False, or conversion fails
    """
    configure_verbose(verbose)

    # Validate inputs
    if is_remote_url(output_path):
        raise click.ClickException(
            "Shapefile output path must be local. Use upload() for cloud destinations."
        )

    validate_profile_for_urls(profile, input_path)
    setup_aws_profile_if_needed(profile, input_path)

    output_file = Path(output_path)

    # Check if output exists (check .shp file)
    if output_file.exists() and not overwrite:
        raise click.ClickException(
            f"Output file already exists: {output_path}\nUse --overwrite to replace existing file."
        )

    # Validate output directory exists
    validate_output_path(output_path, verbose)

    progress(f"Converting to Shapefile: {output_path}")

    # Get DuckDB connection
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_path))

    try:
        # Build safe input URL
        input_url = safe_file_url(input_path, verbose)

        # Write to Shapefile using GDAL
        query = f"""
            COPY (SELECT * FROM read_parquet('{input_url}'))
            TO '{output_path}'
            WITH (FORMAT GDAL, DRIVER 'ESRI Shapefile', LAYER_CREATION_OPTIONS 'ENCODING={encoding}')
        """

        debug(f"Executing: {query}")
        con.execute(query)

        success(f"Created Shapefile: {output_path}")
        return output_path

    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower():
            raise click.ClickException(
                f"Shapefile already exists: {output_path}\nUse --overwrite to replace it."
            ) from e
        raise click.ClickException(f"Failed to create Shapefile: {error_msg}") from e
    finally:
        con.close()


def write_format(
    input_path: str,
    output_path: str,
    format: str,
    verbose: bool = False,
    profile: str | None = None,
    **format_options,
) -> str:
    """
    Generic format writer that routes to appropriate format-specific function.

    Args:
        input_path: Path to input GeoParquet file
        output_path: Path to output file
        format: Output format ('geopackage', 'flatgeobuf', 'csv', 'shapefile')
        verbose: Print verbose output
        profile: AWS profile for S3 input files
        **format_options: Format-specific options passed to writer functions

    Returns:
        Path to output file

    Raises:
        click.ClickException: If format is unsupported or conversion fails
    """
    format_lower = format.lower()

    if format_lower == "geopackage":
        return write_geopackage(
            input_path,
            output_path,
            overwrite=format_options.get("overwrite", False),
            layer_name=format_options.get("layer_name", "features"),
            verbose=verbose,
            profile=profile,
        )
    elif format_lower == "flatgeobuf":
        return write_flatgeobuf(
            input_path,
            output_path,
            verbose=verbose,
            profile=profile,
        )
    elif format_lower == "csv":
        return write_csv(
            input_path,
            output_path,
            include_wkt=format_options.get("include_wkt", True),
            include_bbox=format_options.get("include_bbox", True),
            verbose=verbose,
            profile=profile,
        )
    elif format_lower == "shapefile":
        return write_shapefile(
            input_path,
            output_path,
            overwrite=format_options.get("overwrite", False),
            encoding=format_options.get("encoding", "UTF-8"),
            verbose=verbose,
            profile=profile,
        )
    else:
        raise click.ClickException(
            f"Unsupported format: {format}\n"
            "Supported formats: geopackage, flatgeobuf, csv, shapefile"
        )
