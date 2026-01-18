"""CLI commands for gpio-pmtiles plugin."""

import click

from gpio_pmtiles.core import create_pmtiles_from_geoparquet


@click.group()
def pmtiles():
    """PMTiles generation commands.

    Generate PMTiles from GeoParquet files using tippecanoe.
    Requires tippecanoe to be installed and available in PATH.

    Install tippecanoe:
      macOS:  brew install tippecanoe
      Ubuntu: sudo apt install tippecanoe
      Source: https://github.com/felt/tippecanoe
    """
    pass


@pmtiles.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path())
@click.option(
    "--layer",
    "-l",
    help="Layer name in the PMTiles file. Defaults to filename without extension.",
)
@click.option(
    "--min-zoom",
    type=int,
    help="Minimum zoom level. Use with --max-zoom or tippecanoe will auto-detect.",
)
@click.option(
    "--max-zoom",
    type=int,
    help="Maximum zoom level. If not specified, tippecanoe will auto-detect.",
)
@click.option(
    "--bbox",
    help="Bounding box filter: minx,miny,maxx,maxy. Example: '-122.5,37.5,-122,38'",
)
@click.option(
    "--where",
    help="SQL WHERE clause for filtering rows. Example: 'population > 10000'",
)
@click.option(
    "--include-cols",
    help="Comma-separated list of columns to include. Example: 'name,type,height'",
)
@click.option(
    "--precision",
    type=int,
    default=6,
    help="Coordinate decimal precision (default: 6 for ~10cm accuracy)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output showing progress and commands",
)
@click.option(
    "--profile",
    help="AWS profile name for S3 files",
)
@click.option(
    "--src-crs",
    help="Source CRS if metadata is incorrect. Will reproject from this CRS to WGS84. Example: 'EPSG:32719'",
)
def create(
    input_file,
    output_file,
    layer,
    min_zoom,
    max_zoom,
    bbox,
    where,
    include_cols,
    precision,
    verbose,
    profile,
    src_crs,
):
    """Create PMTiles from GeoParquet file.

    This command converts a GeoParquet file to PMTiles format using tippecanoe.
    It streams GeoJSON features directly to tippecanoe for efficient processing.

    Examples:

      \b
      # Basic conversion
      gpio pmtiles create buildings.parquet buildings.pmtiles

      \b
      # With layer name and zoom levels
      gpio pmtiles create roads.parquet roads.pmtiles -l roads --max-zoom 14

      \b
      # With spatial filtering
      gpio pmtiles create data.parquet tiles.pmtiles \\
        --bbox "-122.5,37.5,-122.0,38.0"

      \b
      # With attribute filtering and column selection
      gpio pmtiles create data.parquet tiles.pmtiles \\
        --where "population > 10000" \\
        --include-cols name,population,area

      \b
      # From S3 with AWS profile
      gpio pmtiles create s3://bucket/data.parquet tiles.pmtiles \\
        --profile my-aws-profile
    """
    try:
        create_pmtiles_from_geoparquet(
            input_path=input_file,
            output_path=output_file,
            layer=layer,
            min_zoom=min_zoom,
            max_zoom=max_zoom,
            bbox=bbox,
            where=where,
            include_cols=include_cols,
            precision=precision,
            verbose=verbose,
            profile=profile,
            src_crs=src_crs,
        )
    except Exception as e:
        raise click.ClickException(str(e))
