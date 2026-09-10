"""``gpio pmtiles`` - PMTiles generation commands (requires tippecanoe).

Registered on the root ``gpio`` group by ``cli/main.py`` via
``cli.add_command(pmtiles)``. This module must not import ``cli.main``: the
dependency runs one way, ``main`` -> ``commands`` -> ``_shared``/``decorators``.
"""

import click

from geoparquet_io.cli.decorators import (
    SingleFileCommand,
    aws_profile_option,
    column_list_option,
    repair_geometry_option,
    verbose_option,
)

# =============================================================================
# PMTiles Commands (requires tippecanoe)
# =============================================================================


@click.group()
@click.pass_context
def pmtiles(ctx):
    """PMTiles generation commands.

    Generate PMTiles from GeoParquet files using tippecanoe.
    Requires tippecanoe to be installed and available in PATH.

    Install tippecanoe:
      macOS:  brew install tippecanoe
      Ubuntu: sudo apt install tippecanoe
    """
    pass


@pmtiles.command(name="create", cls=SingleFileCommand)
@click.argument("input_file", type=click.Path())
@click.argument("output_file", type=click.Path())
@click.option("--layer", "-l", help="Layer name in output (defaults to output filename)")
@click.option("--min-zoom", type=int, help="Minimum zoom level")
@click.option("--max-zoom", type=int, help="Maximum zoom level (auto-detected if not set)")
@click.option("--bbox", help="Bounding box filter: minx,miny,maxx,maxy")
@click.option("--where", help="SQL WHERE clause for filtering")
@column_list_option("--include-cols", help="Comma-separated list of columns to include")
@click.option(
    "--precision",
    type=int,
    default=6,
    show_default=True,
    help="Coordinate decimal precision",
)
@click.option("--src-crs", help="Source CRS for reprojection to WGS84")
@click.option("--attribution", help="Custom attribution HTML for tiles")
@click.option(
    "--layer-by-column",
    type=str,
    default=None,
    help="In the generated PMTiles, split tiles into layers based on the value of this column",
)
@click.option(
    "--simplify-only-low-zooms/--no-simplify-only-low-zooms",
    default=True,
    show_default=True,
    help="Pass tippecanoe --simplify-only-low-zooms",
)
@click.option(
    "--no-simplification-of-shared-nodes/--simplification-of-shared-nodes",
    default=True,
    show_default=True,
    help="Pass tippecanoe --no-simplification-of-shared-nodes",
)
@click.option(
    "--no-tile-size-limit/--tile-size-limit",
    default=True,
    show_default=True,
    help=(
        "Remove the tile size cap (--no-tile-size-limit). Use --tile-size-limit "
        "to keep tippecanoe's limit so --drop-densest-as-needed actually drops "
        "features on dense data."
    ),
)
@click.option(
    "--drop-densest-as-needed/--no-drop-densest-as-needed",
    default=True,
    show_default=True,
    help="Pass tippecanoe --drop-densest-as-needed (no effect while size limit is off)",
)
@click.option(
    "--maximum-tile-bytes",
    type=int,
    default=None,
    help="Explicit per-tile byte cap (--maximum-tile-bytes); takes precedence over --no-tile-size-limit",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Overwrite the output file if it already exists (tippecanoe --force)",
)
@repair_geometry_option
@verbose_option
@aws_profile_option
def pmtiles_create(
    input_file,
    output_file,
    layer,
    min_zoom,
    max_zoom,
    bbox,
    where,
    include_cols,
    precision,
    src_crs,
    attribution,
    verbose,
    aws_profile,
    layer_by_column,
    simplify_only_low_zooms,
    no_simplification_of_shared_nodes,
    no_tile_size_limit,
    drop_densest_as_needed,
    maximum_tile_bytes,
    force,
    repair_geometry,
):
    """Create PMTiles from a GeoParquet file.

    Streams GeoParquet through gpio and tippecanoe to generate PMTiles.
    All processing is done via subprocess pipelines - no intermediate files.

    Examples:

        gpio pmtiles create buildings.parquet buildings.pmtiles

        gpio pmtiles create roads.parquet roads.pmtiles -l roads --max-zoom 14

        gpio pmtiles create data.parquet tiles.pmtiles --bbox "-122.5,37.5,-122.0,38.0"

        gpio pmtiles create data.parquet tiles.pmtiles --where "population > 10000"

        gpio pmtiles create data.parquet tiles.pmtiles --layer-by-column owner

        gpio pmtiles create dense.parquet tiles.pmtiles --tile-size-limit --max-zoom 14

        gpio pmtiles create data.parquet tiles.pmtiles --force
    """
    from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

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
            profile=aws_profile,
            src_crs=src_crs,
            attribution=attribution,
            layer_by_column=layer_by_column,
            simplify_only_low_zooms=simplify_only_low_zooms,
            no_simplification_of_shared_nodes=no_simplification_of_shared_nodes,
            no_tile_size_limit=no_tile_size_limit,
            drop_densest_as_needed=drop_densest_as_needed,
            maximum_tile_bytes=maximum_tile_bytes,
            force=force,
            repair_geometry=repair_geometry,
        )
        click.echo(click.style(f"✓ Created {output_file}", fg="green"))
    except Exception as e:
        raise click.ClickException(str(e)) from e


@pmtiles.command(name="pyramid")
@click.argument("input_parquet", type=click.Path())
@click.argument("output_pmtiles", type=click.Path())
@click.option(
    "--levels",
    default=None,
    help=(
        "Comma-separated overview levels (grid resolutions like '5'; admin: "
        "'country'). Default: auto-select against --max-tile-kb."
    ),
)
@click.option(
    "--max-tile-kb",
    type=int,
    default=500,
    show_default=True,
    help="Tile-size budget in KB driving zoom-band selection.",
)
@click.option(
    "--bytes-per-cell",
    type=float,
    default=None,
    help="Override the estimated compressed bytes per cell used in band selection.",
)
@click.option(
    "--layer-mode",
    type=click.Choice(["single", "grouped", "per-level"]),
    default="grouped",
    show_default=True,
    help=(
        "Layer naming: 'single' puts everything in one layer, 'grouped' uses "
        "'aggregate' + 'features', 'per-level' uses r5/r10 (or country/region)."
    ),
)
@click.option(
    "--include-features",
    is_flag=True,
    help="Append the original features as the final zoom band.",
)
@click.option(
    "--features-source",
    type=click.Path(),
    default=None,
    help="GeoParquet source for the features band (required with --include-features).",
)
@click.option(
    "--features-min-zoom",
    type=int,
    default=None,
    help="First zoom of the features band (default: base band max zoom + 1).",
)
@click.option(
    "--max-zoom",
    type=int,
    default=None,
    help="Max zoom of the base aggregate band (auto-detected if not set).",
)
@click.option("--attribution", help="Custom attribution HTML for tiles")
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Overwrite the output archive if it already exists",
)
@verbose_option
def pmtiles_pyramid(
    input_parquet,
    output_pmtiles,
    levels,
    max_tile_kb,
    bytes_per_cell,
    layer_mode,
    include_features,
    features_source,
    features_min_zoom,
    max_zoom,
    attribution,
    force,
    verbose,
):
    """Create a multi-level PMTiles pyramid from an aggregate file.

    Detects the aggregate's scheme (a5/h3/admin) and base level, assigns each
    level a zoom band that fits the tile budget, runs tippecanoe once per band,
    and merges everything into one archive with tile-join. Existing overview
    siblings (from `gpio process overview`) are reused; missing levels are
    built automatically. Bands are recorded in the PMTiles metadata under
    `gpio:pyramid`.

    Requires tippecanoe and tile-join (ships with tippecanoe) in PATH.

    Examples:

        gpio pmtiles pyramid cells.parquet cells.pmtiles

        gpio pmtiles pyramid cells.parquet out.pmtiles --levels 5 --max-zoom 10

        gpio pmtiles pyramid cells.parquet out.pmtiles \\
            --include-features --features-source buildings.parquet --max-zoom 8

        gpio pmtiles pyramid by_region.parquet out.pmtiles --layer-mode per-level
    """
    from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

    try:
        create_pmtiles_pyramid(
            input_parquet,
            output_pmtiles,
            levels=levels,
            max_tile_kb=max_tile_kb,
            bytes_per_cell=bytes_per_cell,
            layer_mode=layer_mode,
            include_features=include_features,
            features_source=features_source,
            features_min_zoom=features_min_zoom,
            max_zoom=max_zoom,
            attribution=attribution,
            force=force,
            verbose=verbose,
        )
        click.echo(click.style(f"✓ Created {output_pmtiles}", fg="green"))
    except Exception as e:
        raise click.ClickException(str(e)) from e
