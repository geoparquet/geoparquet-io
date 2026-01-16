"""
Writers for converting GeoParquet to various output formats.

Provides a unified interface for writing GeoParquet to multiple formats:
- GeoPackage, FlatGeobuf, Shapefile (via GDAL drivers)
- CSV with WKT (via DuckDB SQL)
- GeoJSON (via existing geojson_stream module)

All writers use DuckDB's spatial extension for maximum compatibility.
Writers handle local file output only; remote uploads are handled by the upload module.
"""

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

# Format configuration for GDAL-based writers
GDAL_FORMATS = {
    "geopackage": {
        "driver": "GPKG",
        "description": "GeoPackage",
        "check_overwrite": True,
        "layer_option": "LAYER_NAME",
    },
    "flatgeobuf": {
        "driver": "FlatGeobuf",
        "description": "FlatGeobuf",
        "check_overwrite": False,
        "layer_option": None,
    },
    "shapefile": {
        "driver": "ESRI Shapefile",
        "description": "Shapefile",
        "check_overwrite": True,
        "layer_option": None,
        "encoding_option": "ENCODING",
    },
}


def write_gdal_format(
    input_path: str,
    output_path: str,
    format_name: str,
    overwrite: bool = False,
    layer_name: str = "features",
    encoding: str = "UTF-8",
    verbose: bool = False,
    profile: str | None = None,
) -> str:
    """
    Write GeoParquet to a GDAL-supported format via DuckDB.

    Unified implementation for GeoPackage, FlatGeobuf, and Shapefile formats.

    Args:
        input_path: Path to input GeoParquet file
        output_path: Path to output file (must be local)
        format_name: Format key: 'geopackage', 'flatgeobuf', or 'shapefile'
        overwrite: Overwrite existing file if True
        layer_name: Layer name (for formats that support it)
        encoding: Character encoding (for Shapefile)
        verbose: Print verbose output
        profile: AWS profile for S3 input files

    Returns:
        Path to output file

    Raises:
        click.ClickException: If validation or conversion fails
    """
    configure_verbose(verbose)

    # Get format configuration
    if format_name not in GDAL_FORMATS:
        raise click.ClickException(
            f"Unsupported GDAL format: {format_name}\nSupported: {', '.join(GDAL_FORMATS.keys())}"
        )

    config = GDAL_FORMATS[format_name]

    # Validate inputs
    if is_remote_url(output_path):
        raise click.ClickException(
            f"{config['description']} output path must be local. "
            "Use upload() for cloud destinations."
        )

    validate_profile_for_urls(profile, input_path)
    setup_aws_profile_if_needed(profile, input_path)

    # Check if output exists
    from pathlib import Path

    output_file = Path(output_path)
    if config["check_overwrite"] and output_file.exists() and not overwrite:
        raise click.ClickException(
            f"Output file already exists: {output_path}\nUse --overwrite to replace existing file."
        )

    validate_output_path(output_path, verbose)
    progress(f"Converting to {config['description']}: {output_path}")

    # Get DuckDB connection
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_path))

    try:
        input_url = safe_file_url(input_path, verbose)

        # Build layer creation options
        lco_parts = []
        if config.get("layer_option"):
            lco_parts.append(f"{config['layer_option']}={layer_name}")
        if config.get("encoding_option"):
            lco_parts.append(f"{config['encoding_option']}={encoding}")

        lco_clause = f", LAYER_CREATION_OPTIONS '{' '.join(lco_parts)}'" if lco_parts else ""

        # Execute write
        query = f"""
            COPY (SELECT * FROM read_parquet('{input_url}'))
            TO '{output_path}'
            WITH (FORMAT GDAL, DRIVER '{config["driver"]}'{lco_clause})
        """

        debug(f"Executing: {query}")
        con.execute(query)

        success(f"Created {config['description']}: {output_path}")
        return output_path

    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower():
            raise click.ClickException(
                f"{config['description']} file already exists: {output_path}\n"
                "Use --overwrite to replace it."
            ) from e
        raise click.ClickException(f"Failed to create {config['description']}: {error_msg}") from e
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

    if is_remote_url(output_path):
        raise click.ClickException(
            "CSV output path must be local. Use upload() for cloud destinations."
        )

    validate_profile_for_urls(profile, input_path)
    setup_aws_profile_if_needed(profile, input_path)
    validate_output_path(output_path, verbose)

    progress(f"Converting to CSV: {output_path}")

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_path))

    try:
        input_url = safe_file_url(input_path, verbose)

        # Read parquet to inspect schema
        pf = pq.ParquetFile(input_url)
        schema = pf.schema_arrow
        columns = [field.name for field in schema]

        # Find geometry column
        geom_col = next(
            (col for col in ["geometry", "geom", "wkb_geometry"] if col in columns),
            None,
        )

        if not geom_col:
            raise click.ClickException(
                "No geometry column found. Expected 'geometry', 'geom', or 'wkb_geometry'."
            )

        # Build column list
        select_cols = []
        for col in columns:
            if col == geom_col:
                if include_wkt:
                    select_cols.append(f'ST_AsText("{col}") as wkt')
            elif col == "bbox":
                if include_bbox:
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

        # Write to CSV
        query = f"""
            COPY (
                SELECT {", ".join(select_cols)}
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


def write_geojson(
    input_path: str,
    output_path: str,
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
    pretty: bool = False,
    keep_crs: bool = False,
    verbose: bool = False,
    profile: str | None = None,
) -> str:
    """
    Convert GeoParquet to GeoJSON format.

    Uses existing geojson_stream module for conversion.
    Automatically reprojects to WGS84 unless keep_crs is True.

    Args:
        input_path: Path to input GeoParquet file
        output_path: Path to output GeoJSON file (must be local)
        precision: Coordinate decimal precision (default: 7)
        write_bbox: Include bbox property for features (default: False)
        id_field: Field to use as feature 'id' member
        pretty: Pretty-print JSON output (default: False)
        keep_crs: Keep original CRS instead of reprojecting to WGS84 (default: False)
        verbose: Print verbose output
        profile: AWS profile for S3 input files

    Returns:
        Path to output file

    Raises:
        click.ClickException: If conversion fails
    """
    from geoparquet_io.core.geojson_stream import convert_to_geojson

    configure_verbose(verbose)

    if is_remote_url(output_path):
        raise click.ClickException(
            "GeoJSON output path must be local. Use upload() for cloud destinations."
        )

    validate_profile_for_urls(profile, input_path)

    progress(f"Converting to GeoJSON: {output_path}")

    try:
        convert_to_geojson(
            input_path=input_path,
            output_path=output_path,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
            pretty=pretty,
            keep_crs=keep_crs,
            verbose=verbose,
            profile=profile,
        )

        success(f"Created GeoJSON: {output_path}")
        return output_path

    except Exception as e:
        raise click.ClickException(f"Failed to create GeoJSON: {str(e)}") from e


# Convenience wrappers for specific formats
def write_geopackage(input_path: str, output_path: str, **kwargs) -> str:
    """Write GeoParquet to GeoPackage format."""
    return write_gdal_format(input_path, output_path, "geopackage", **kwargs)


def write_flatgeobuf(input_path: str, output_path: str, **kwargs) -> str:
    """Write GeoParquet to FlatGeobuf format."""
    return write_gdal_format(input_path, output_path, "flatgeobuf", **kwargs)


def write_shapefile(input_path: str, output_path: str, **kwargs) -> str:
    """Write GeoParquet to Shapefile format."""
    return write_gdal_format(input_path, output_path, "shapefile", **kwargs)


def write_format(
    input_path: str,
    output_path: str,
    format: str,
    verbose: bool = False,
    profile: str | None = None,
    **format_options,
) -> str:
    """
    Generic format writer that routes to appropriate writer function.

    Args:
        input_path: Path to input GeoParquet file
        output_path: Path to output file
        format: Output format ('geopackage', 'flatgeobuf', 'csv', 'shapefile', 'geojson')
        verbose: Print verbose output
        profile: AWS profile for S3 input files
        **format_options: Format-specific options passed to writer functions

    Returns:
        Path to output file

    Raises:
        click.ClickException: If format is unsupported or conversion fails
    """
    format_lower = format.lower()

    if format_lower in GDAL_FORMATS:
        return write_gdal_format(
            input_path,
            output_path,
            format_lower,
            verbose=verbose,
            profile=profile,
            **format_options,
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
    elif format_lower == "geojson":
        return write_geojson(
            input_path,
            output_path,
            precision=format_options.get("precision", 7),
            write_bbox=format_options.get("write_bbox", False),
            id_field=format_options.get("id_field"),
            pretty=format_options.get("pretty", False),
            keep_crs=format_options.get("keep_crs", False),
            verbose=verbose,
            profile=profile,
        )
    else:
        supported = list(GDAL_FORMATS.keys()) + ["csv", "geojson"]
        raise click.ClickException(
            f"Unsupported format: {format}\nSupported formats: {', '.join(supported)}"
        )
