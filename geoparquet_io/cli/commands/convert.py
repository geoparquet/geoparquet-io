"""``gpio convert`` - convert between formats and coordinate systems.

Registered on the root ``gpio`` group by ``cli/main.py`` via
``cli.add_command(convert)``. This module must not import ``cli.main``: the
dependency runs one way, ``main`` -> ``commands`` -> ``_shared``/``decorators``.
"""

from pathlib import Path

import click

from geoparquet_io.cli._shared import _activate_s3, prepare_output
from geoparquet_io.cli.decorators import (
    SingleFileCommand,
    any_extension_option,
    aws_profile_option,
    geoparquet_version_option,
    linearize_curves_options,
    output_format_options,
    overwrite_option,
    parse_row_group_options,
    repair_geometry_option,
    show_sql_option,
    verbose_option,
)
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.file_utils import validate_parquet_extension
from geoparquet_io.core.logging_config import configure_verbose, setup_cli_logging
from geoparquet_io.core.reproject import reproject as reproject_core


class ConvertDefaultGroup(click.Group):
    """
    Custom Group for convert command with format auto-detection.

    Auto-detects output format from file extension when no subcommand is specified.
    Falls back to 'geoparquet' if extension is not recognized.

    Supported auto-detection:
    - .parquet -> geoparquet
    - .gpkg -> geopackage
    - .fgb -> flatgeobuf
    - .csv -> csv
    - .shp -> shapefile
    - .geojson, .json -> geojson

    Examples:
    - gpio convert input.parquet output.gpkg -> auto-detects 'geopackage'
    - gpio convert input.parquet output.fgb -> auto-detects 'flatgeobuf'
    - gpio convert geopackage input.parquet output.gpkg -> explicit subcommand
    """

    # Extension to subcommand mapping
    EXTENSION_TO_SUBCOMMAND = {
        ".parquet": "geoparquet",
        ".gpkg": "geopackage",
        ".fgb": "flatgeobuf",
        ".csv": "csv",
        ".shp": "shapefile",
        ".geojson": "geojson",
        ".json": "geojson",
    }

    def parse_args(self, ctx, args):
        """Parse args with format auto-detection from output file extension."""
        # Handle --help for group
        if "--help" in args and (not args or args[0] not in self.commands):
            return super().parse_args(ctx, [a for a in args if a != "--help"] + ["--help"])

        # If first arg is a known subcommand, use it directly
        if args and not args[0].startswith("-") and args[0] in self.commands:
            return super().parse_args(ctx, args)

        # Auto-detect format from output file extension
        subcommand = self._detect_format_from_args(args)
        return super().parse_args(ctx, [subcommand] + args)

    def _detect_format_from_args(self, args):
        """Extract output file from args and detect format from extension.

        Scans backwards through args to find the output file (last positional argument).
        This approach correctly handles options with values interspersed with positional args.
        """
        # Scan backwards to find first argument with a recognized extension
        # Skip tokens starting with "-" to avoid treating option values as file paths
        for arg in reversed(args):
            if arg.startswith("-"):
                continue

            ext = Path(arg).suffix.lower()
            if ext in self.EXTENSION_TO_SUBCOMMAND:
                return self.EXTENSION_TO_SUBCOMMAND[ext]

        return "geoparquet"  # Default fallback


# Convert commands group
@click.group(cls=ConvertDefaultGroup)
@click.pass_context
def convert(ctx):
    """Convert between formats and coordinate systems.

    Auto-detects output format from file extension. Supports GeoParquet, GeoPackage,
    FlatGeobuf, CSV, Shapefile, and GeoJSON.

    \b
    Auto-detection examples:
        gpio convert input.shp output.parquet                    # → GeoParquet
        gpio convert data.parquet output.gpkg                    # → GeoPackage
        gpio convert data.parquet output.fgb                     # → FlatGeobuf
        gpio convert data.parquet output.csv                     # → CSV with WKT
        gpio convert data.parquet output.geojson                 # → GeoJSON

    \b
    Explicit subcommands:
        gpio convert geoparquet input.shp output.parquet         # Force GeoParquet
        gpio convert geopackage data.parquet output.gpkg         # Force GeoPackage
        gpio convert reproject input.parquet out.parquet -d EPSG:32610
        gpio convert geojson data.parquet | tippecanoe -P -o tiles.pmtiles
    """
    ctx.ensure_object(dict)
    timestamps = ctx.obj.get("timestamps", False)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


@convert.command(name="geoparquet", cls=SingleFileCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@click.option(
    "--skip-hilbert",
    is_flag=True,
    help="Skip Hilbert spatial ordering (faster but less optimal for spatial queries)",
)
@click.option(
    "--wkt-column",
    help="CSV/TSV: Column name containing WKT geometry (auto-detected if not specified)",
)
@click.option(
    "--lat-column",
    help="CSV/TSV: Column name containing latitude values (requires --lon-column)",
)
@click.option(
    "--lon-column",
    help="CSV/TSV: Column name containing longitude values (requires --lat-column)",
)
@click.option(
    "--delimiter",
    help="CSV/TSV: Delimiter character (auto-detected if not specified). Common: ',' (comma), '\\t' (tab), ';' (semicolon), '|' (pipe)",
)
@click.option(
    "--layer",
    help="GeoPackage/FileGDB: Layer name to read (reads first layer if not specified)",
)
@click.option(
    "--crs",
    default="EPSG:4326",
    show_default=True,
    help="CSV/TSV: CRS for geometry data (WGS84 assumed for lat/lon)",
)
@click.option(
    "--skip-invalid",
    is_flag=True,
    help="CSV/TSV: Skip rows whose geometry cannot be parsed instead of failing "
    "(rows with no geometry at all are kept, with NULL geometry)",
)
@click.option(
    "--csv-max-line-size",
    type=int,
    default=None,
    help="CSV/TSV: Maximum line size in bytes (default: 50MB). Increase for very large WKT geometries.",
)
@click.option(
    "--allow-no-geometry",
    is_flag=True,
    help="Allow conversion to plain Parquet when no geometry column is detected (default: error)",
)
@repair_geometry_option
@linearize_curves_options
@geoparquet_version_option
@verbose_option
@output_format_options
@aws_profile_option
@any_extension_option
@show_sql_option
@click.pass_context
def convert_to_geoparquet_cmd(
    ctx,
    input_file,
    output_file,
    skip_hilbert,
    wkt_column,
    lat_column,
    lon_column,
    delimiter,
    layer,
    crs,
    skip_invalid,
    csv_max_line_size,
    allow_no_geometry,
    repair_geometry,
    linearize_curves,
    max_angle_deg,
    geoparquet_version,
    verbose,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    aws_profile,
    any_extension,
    show_sql,
):
    """
    Convert vector formats to optimized GeoParquet.

    Supports Shapefile, GeoJSON, GeoPackage, GDB, CSV/TSV with WKT or lat/lon columns.
    Applies ZSTD compression, bbox metadata, and Hilbert ordering by default.
    Auto-streams Arrow IPC to stdout when piped (or use "-" as output).

    \b
    Examples:
      # Standard conversion
      gpio convert input.gpkg output.parquet

      \b
      # Pipe to another command (auto-streams when piped)
      gpio convert input.gpkg | gpio add bbox - | gpio upload - s3://bucket/data.parquet
    """
    from geoparquet_io.core.convert import set_csv_max_line_size
    from geoparquet_io.core.streaming import should_stream_output

    # Set CSV max line size if specified
    if csv_max_line_size is not None:
        set_csv_max_line_size(csv_max_line_size)

    row_group_mb = prepare_output(output_file, any_extension, row_group_size, row_group_size_mb)

    # Check for streaming output
    with _activate_s3(ctx, aws_profile=aws_profile):
        if should_stream_output(output_file):
            # Suppress verbose for streaming
            verbose = False
            _convert_streaming(
                input_file,
                skip_hilbert=skip_hilbert,
                wkt_column=wkt_column,
                lat_column=lat_column,
                lon_column=lon_column,
                delimiter=delimiter,
                layer=layer,
                crs=crs,
                skip_invalid=skip_invalid,
                allow_no_geometry=allow_no_geometry,
                profile=aws_profile,
                geoparquet_version=geoparquet_version,
                compression=compression,
                compression_level=compression_level,
                row_group_rows=row_group_size,
                row_group_size_mb=row_group_mb,
                repair_geometry=repair_geometry,
                linearize_curves=linearize_curves,
                max_angle_deg=max_angle_deg,
                memory_limit=write_memory,
            )
        else:
            convert_to_geoparquet(
                input_file,
                output_file,
                skip_hilbert=skip_hilbert,
                verbose=verbose,
                compression=compression,
                compression_level=compression_level,
                row_group_rows=row_group_size,
                row_group_size_mb=row_group_mb,
                wkt_column=wkt_column,
                lat_column=lat_column,
                lon_column=lon_column,
                delimiter=delimiter,
                layer=layer,
                crs=crs,
                skip_invalid=skip_invalid,
                allow_no_geometry=allow_no_geometry,
                profile=aws_profile,
                geoparquet_version=geoparquet_version,
                repair_geometry=repair_geometry,
                linearize_curves=linearize_curves,
                max_angle_deg=max_angle_deg,
                memory_limit=write_memory,
            )


def _convert_streaming(
    input_file,
    skip_hilbert,
    wkt_column,
    lat_column,
    lon_column,
    delimiter,
    layer,
    crs,
    skip_invalid,
    allow_no_geometry,
    profile,
    geoparquet_version,
    compression="ZSTD",
    compression_level=15,
    row_group_rows=None,
    row_group_size_mb=None,
    repair_geometry=True,
    linearize_curves=True,
    max_angle_deg=None,
    memory_limit=None,
):
    """Handle streaming output for convert command."""
    import tempfile
    import uuid

    import pyarrow.parquet as pq

    from geoparquet_io.core.streaming import write_arrow_stream

    # Convert to temp file first, then stream
    temp_path = Path(tempfile.gettempdir()) / f"gpio_convert_{uuid.uuid4()}.parquet"

    try:
        convert_to_geoparquet(
            input_file,
            str(temp_path),
            skip_hilbert=skip_hilbert,
            verbose=False,
            allow_no_geometry=allow_no_geometry,
            compression=compression,
            compression_level=compression_level,
            row_group_rows=row_group_rows,
            row_group_size_mb=row_group_size_mb,
            wkt_column=wkt_column,
            lat_column=lat_column,
            lon_column=lon_column,
            delimiter=delimiter,
            layer=layer,
            crs=crs,
            skip_invalid=skip_invalid,
            profile=profile,
            geoparquet_version=geoparquet_version,
            repair_geometry=repair_geometry,
            linearize_curves=linearize_curves,
            max_angle_deg=max_angle_deg,
            memory_limit=memory_limit,
        )

        # Read and stream to stdout
        table = pq.read_table(temp_path)
        write_arrow_stream(table)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _reproject_impl_cli(
    input_file,
    output_file,
    dst_crs,
    src_crs,
    overwrite,
    verbose,
    aws_profile,
    compression,
    compression_level,
    geoparquet_version,
    row_group_size_mb=None,
    row_group_rows=None,
    memory_limit=None,
    assume_crs84=False,
):
    """Shared reproject CLI implementation."""
    from geoparquet_io.core.remote import validate_profile_for_urls

    # Configure verbose logging
    configure_verbose(verbose)

    # Validate profile is only used with S3
    validate_profile_for_urls(None, input_file, output_file)

    try:
        result = reproject_core(
            input_parquet=input_file,
            output_parquet=output_file,
            target_crs=dst_crs,
            source_crs=src_crs,
            overwrite=overwrite,
            compression=compression,
            compression_level=compression_level,
            verbose=verbose,
            geoparquet_version=geoparquet_version,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            memory_limit=memory_limit,
            assume_crs84=assume_crs84,
        )
    except ValueError as e:
        raise click.ClickException(str(e)) from None

    # result is None for streaming mode (stdout)
    if result:
        click.echo(f"\nReprojected {result.feature_count:,} features")
        click.echo(f"  Source CRS: {result.source_crs}")
        click.echo(f"  Destination CRS: {result.target_crs}")
        click.echo(f"  Output: {result.output_path}")


@convert.command(name="reproject", cls=SingleFileCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@click.option(
    "--dst-crs",
    "-d",
    default="EPSG:4326",
    show_default=True,
    help=(
        "Destination CRS (e.g., 'EPSG:4326', 'EPSG:32610'). The default "
        "(EPSG:4326 / OGC:CRS84) is written by omitting the crs key per the "
        "GeoParquet spec, so the output has no explicit crs."
    ),
)
@click.option(
    "--src-crs",
    "-s",
    default=None,
    help="Override source CRS (e.g., 'EPSG:4326'). If not provided, detected from file metadata.",
)
@click.option(
    "--assume-crs84",
    is_flag=True,
    help=(
        "Treat an unknown (explicit null) input CRS as OGC:CRS84 and rewrite the "
        "file so the crs key is omitted (the default). Use when data is really "
        "lon/lat WGS84 but the file declares crs:null."
    ),
)
@overwrite_option
@verbose_option
@aws_profile_option
@output_format_options
@geoparquet_version_option
@any_extension_option
@show_sql_option
@click.pass_context
def convert_reproject(
    ctx,
    input_file,
    output_file,
    dst_crs,
    src_crs,
    assume_crs84,
    overwrite,
    verbose,
    aws_profile,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
    any_extension,
    show_sql,
):
    """
    Reproject a GeoParquet file to a different CRS.

    Uses DuckDB's ST_Transform for fast, streaming reprojection.
    Automatically detects source CRS from GeoParquet metadata unless --src-crs is provided.

    If OUTPUT_FILE is not provided, creates <input>_<crs>.parquet.
    Use --overwrite to modify the input file in place.

    \b
    Examples:
        gpio convert reproject input.parquet output.parquet
        gpio convert reproject input.parquet -d EPSG:32610
        gpio convert reproject input.parquet --overwrite -d EPSG:4326
        gpio convert reproject input.parquet output.parquet --dst-crs EPSG:3857
    """
    # Validate .parquet extension
    validate_parquet_extension(output_file, any_extension)

    # Validate mutual exclusivity of row group options and get MB value
    row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

    with _activate_s3(ctx, aws_profile=aws_profile):
        _reproject_impl_cli(
            input_file,
            output_file,
            dst_crs,
            src_crs,
            overwrite,
            verbose,
            aws_profile,
            compression,
            compression_level,
            geoparquet_version,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            memory_limit=write_memory,
            assume_crs84=assume_crs84,
        )


@convert.command(name="geojson", cls=SingleFileCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@overwrite_option
@click.option(
    "--no-rs",
    is_flag=True,
    help="Disable RFC 8142 record separators (enabled by default for tippecanoe -P)",
)
@click.option(
    "--precision",
    type=int,
    default=7,
    help="Coordinate decimal precision for geometry and bbox (default: 7 per RFC 7946).",
)
@click.option(
    "--write-bbox",
    is_flag=True,
    help="Include bbox property for each feature",
)
@click.option(
    "--id-field",
    type=str,
    default=None,
    help="Source field to use as feature 'id' member",
)
@click.option(
    "--description",
    type=str,
    default=None,
    help="Description to add to the FeatureCollection",
)
@click.option(
    "--feature-collection",
    "no_seq",
    is_flag=True,
    help="Output a FeatureCollection instead of newline-delimited GeoJSONSeq (streaming only)",
)
@click.option(
    "--pretty",
    is_flag=True,
    help="Pretty-print the JSON output with indentation",
)
@click.option(
    "--keep-crs",
    is_flag=True,
    help="Keep original CRS instead of reprojecting to WGS84 (EPSG:4326)",
)
@repair_geometry_option
@verbose_option
@aws_profile_option
@show_sql_option
@click.pass_context
def convert_geojson(
    ctx,
    input_file,
    output_file,
    overwrite,
    no_rs,
    precision,
    write_bbox,
    id_field,
    description,
    no_seq,
    pretty,
    keep_crs,
    repair_geometry,
    verbose,
    aws_profile,
    show_sql,
):
    """
    Convert GeoParquet to GeoJSON format.

    Supports two modes based on whether OUTPUT_FILE is provided:

    \b
    STREAMING MODE (no output file):
      Streams newline-delimited GeoJSON (GeoJSONSeq) to stdout with RFC 8142
      record separators. Designed for piping to tippecanoe for PMTiles/MBTiles.
      Use --feature-collection to output a FeatureCollection instead.

    \b
    FILE MODE (with output file):
      Writes a standard GeoJSON FeatureCollection to the specified file.

    \b
    Either mode reads from stdin with "-" (Arrow IPC), for pipeline use.

    \b
    Examples:
      # Stream to tippecanoe for PMTiles generation
      gpio convert geojson buildings.parquet | tippecanoe -P -o buildings.pmtiles

      # Pipeline with filtering
      gpio extract data.parquet --bbox "-122.5,37.5,-122,38" | gpio convert geojson - | tippecanoe -P -o sf.pmtiles

      # Pipeline ending in a GeoJSON file
      gpio extract data.parquet --bbox "-122.5,37.5,-122,38" | gpio convert geojson - sf.geojson

      # Write to GeoJSON file
      gpio convert geojson data.parquet output.geojson

      # Pretty-print with description
      gpio convert geojson data.parquet output.geojson --pretty --description "My dataset"

    \b
    Note: GeoParquet input is automatically reprojected to WGS84 (EPSG:4326)
    for RFC 7946 compliance. Use --keep-crs to preserve the original CRS.
    """
    from geoparquet_io.core.format_writers import write_geojson
    from geoparquet_io.core.geojson_stream import convert_to_geojson
    from geoparquet_io.core.remote import validate_profile_for_urls

    configure_verbose(verbose)

    # Validate aws_profile is only used with S3
    validate_profile_for_urls(aws_profile, input_file, output_file)

    with _activate_s3(ctx, aws_profile=aws_profile):
        if output_file:
            # File mode - use write_geojson which handles no-geometry case
            write_geojson(
                input_path=input_file,
                output_path=output_file,
                precision=precision,
                write_bbox=write_bbox,
                id_field=id_field,
                description=description,
                pretty=pretty,
                keep_crs=keep_crs,
                overwrite=overwrite,
                verbose=verbose,
                profile=aws_profile,
                repair_geometry=repair_geometry,
            )
        else:
            # Streaming mode - use convert_to_geojson directly
            convert_to_geojson(
                input_path=input_file,
                output_path=output_file,
                rs=not no_rs,
                precision=precision,
                write_bbox=write_bbox,
                id_field=id_field,
                description=description,
                seq=not no_seq,
                pretty=pretty,
                verbose=verbose,
                profile=aws_profile,
                keep_crs=keep_crs,
                repair_geometry=repair_geometry,
            )


@convert.command(name="geopackage", cls=SingleFileCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@overwrite_option
@click.option(
    "--layer-name",
    default="features",
    show_default=True,
    help="Layer name in GeoPackage",
)
@verbose_option
@aws_profile_option
@show_sql_option
@click.pass_context
def convert_geopackage(
    ctx,
    input_file,
    output_file,
    overwrite,
    layer_name,
    verbose,
    aws_profile,
    show_sql,
):
    """
    Convert GeoParquet to GeoPackage format.

    GeoPackage is an OGC standard based on SQLite, supporting spatial indexing
    and multiple layers. Output includes a spatial index by default.

    \b
    Examples:
      # Convert to GeoPackage
      gpio convert geopackage data.parquet output.gpkg

      # With custom layer name
      gpio convert geopackage data.parquet output.gpkg --layer-name buildings

      # Overwrite existing file
      gpio convert geopackage data.parquet output.gpkg --overwrite

      # Auto-detection (no subcommand needed)
      gpio convert data.parquet output.gpkg
    """
    from geoparquet_io.core.format_writers import write_geopackage

    configure_verbose(verbose)

    if output_file is None:
        # Generate output filename
        output_file = Path(input_file).stem + ".gpkg"

    with _activate_s3(ctx, aws_profile=aws_profile):
        write_geopackage(
            input_path=input_file,
            output_path=output_file,
            overwrite=overwrite,
            layer_name=layer_name,
            verbose=verbose,
            profile=aws_profile,
        )


@convert.command(name="flatgeobuf", cls=SingleFileCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@overwrite_option
@verbose_option
@aws_profile_option
@show_sql_option
@click.pass_context
def convert_flatgeobuf(
    ctx,
    input_file,
    output_file,
    overwrite,
    verbose,
    aws_profile,
    show_sql,
):
    """
    Convert GeoParquet to FlatGeobuf format.

    FlatGeobuf is a cloud-native format with built-in spatial indexing, designed
    for efficient streaming and HTTP range requests. Spatial index is created
    automatically.

    \b
    Examples:
      # Convert to FlatGeobuf
      gpio convert flatgeobuf data.parquet output.fgb

      # Auto-detection (no subcommand needed)
      gpio convert data.parquet output.fgb
    """
    from geoparquet_io.core.format_writers import write_flatgeobuf

    configure_verbose(verbose)

    if output_file is None:
        # Generate output filename
        output_file = Path(input_file).stem + ".fgb"

    with _activate_s3(ctx, aws_profile=aws_profile):
        write_flatgeobuf(
            input_path=input_file,
            output_path=output_file,
            overwrite=overwrite,
            verbose=verbose,
            profile=aws_profile,
        )


@convert.command(name="csv", cls=SingleFileCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@overwrite_option
@click.option(
    "--no-wkt",
    is_flag=True,
    help="Exclude WKT geometry column (only non-spatial attributes)",
)
@click.option(
    "--no-bbox",
    is_flag=True,
    help="Exclude bbox column if present in input",
)
@verbose_option
@aws_profile_option
@show_sql_option
@click.pass_context
def convert_csv(
    ctx,
    input_file,
    output_file,
    overwrite,
    no_wkt,
    no_bbox,
    verbose,
    aws_profile,
    show_sql,
):
    """
    Convert GeoParquet to CSV format with optional WKT geometry.

    By default, includes geometry as WKT (Well-Known Text) and bbox column if present.
    Complex types (STRUCT, LIST, MAP) are JSON-encoded.

    \b
    Examples:
      # Convert to CSV with WKT geometry
      gpio convert csv data.parquet output.csv

      # Export only attributes (no geometry)
      gpio convert csv data.parquet output.csv --no-wkt

      # Exclude bbox column
      gpio convert csv data.parquet output.csv --no-bbox

      # Auto-detection (no subcommand needed)
      gpio convert data.parquet output.csv
    """
    from geoparquet_io.core.format_writers import write_csv

    configure_verbose(verbose)

    if output_file is None:
        # Generate output filename
        output_file = Path(input_file).stem + ".csv"

    with _activate_s3(ctx, aws_profile=aws_profile):
        write_csv(
            input_path=input_file,
            output_path=output_file,
            include_wkt=not no_wkt,
            include_bbox=not no_bbox,
            overwrite=overwrite,
            verbose=verbose,
            profile=aws_profile,
        )


@convert.command(name="shapefile", cls=SingleFileCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@overwrite_option
@click.option(
    "--encoding",
    default="UTF-8",
    show_default=True,
    help="Character encoding for attribute data",
)
@verbose_option
@aws_profile_option
@show_sql_option
@click.pass_context
def convert_shapefile(
    ctx,
    input_file,
    output_file,
    overwrite,
    encoding,
    verbose,
    aws_profile,
    show_sql,
):
    """
    Convert GeoParquet to Shapefile format.

    Note: Shapefiles have significant limitations:
    - Column names truncated to 10 characters
    - File size limit of 2GB
    - Limited data type support
    - Creates multiple files (.shp, .shx, .dbf, .prj)

    Consider using GeoPackage or FlatGeobuf instead for modern workflows.

    \b
    Examples:
      # Convert to Shapefile
      gpio convert shapefile data.parquet output.shp

      # With custom encoding
      gpio convert shapefile data.parquet output.shp --encoding Latin1

      # Overwrite existing file
      gpio convert shapefile data.parquet output.shp --overwrite

      # Auto-detection (no subcommand needed)
      gpio convert data.parquet output.shp
    """
    from geoparquet_io.core.format_writers import write_shapefile

    configure_verbose(verbose)

    if output_file is None:
        # Generate output filename
        output_file = Path(input_file).stem + ".shp"

    with _activate_s3(ctx, aws_profile=aws_profile):
        write_shapefile(
            input_path=input_file,
            output_path=output_file,
            overwrite=overwrite,
            encoding=encoding,
            verbose=verbose,
            profile=aws_profile,
        )
