"""``gpio process`` - transform or reduce GeoParquet data.

Registered on the root ``gpio`` group by ``cli/main.py`` via
``cli.add_command(process)``. This module must not import ``cli.main``: the
dependency runs one way, ``main`` -> ``commands`` -> ``_shared``/``decorators``.
"""

import click
import duckdb

from geoparquet_io.cli._shared import _activate_s3
from geoparquet_io.cli.decorators import (
    breakdown_metric_option,
    bucket_point_options,
    compression_options,
    geoparquet_version_option,
    grid_aggregate_options,
    handle_geoparquet_errors,
    metric_nodata_option,
    row_group_options,
    show_sql_option,
    verbose_option,
    where_option,
)
from geoparquet_io.core.exceptions import InvalidParameterError, ValidationError
from geoparquet_io.core.process.simplify import simplify_file as simplify_file_impl
from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5 as aggregate_by_a5_impl
from geoparquet_io.core.process.aggregate.by_admin import (
    aggregate_by_admin as aggregate_by_admin_impl,
)
from geoparquet_io.core.process.aggregate.by_h3 import aggregate_by_h3 as aggregate_by_h3_impl
from geoparquet_io.core.process.overview import create_overviews as create_overviews_impl
from geoparquet_io.core.process.overview.run import create_overview_file

# =============================================================================
# Process Commands (aggregate, ...)
# =============================================================================


def _aggregate_error(exc: Exception, where: str | None) -> click.ClickException:
    """Turn an aggregation failure into a message a user can act on.

    A bad ``--where`` clause surfaces as a raw DuckDB binder/parser error that
    echoes the whole generated SQL, which buries the actual cause. Drop the SQL
    echo and name ``--where`` as the likely culprit (gpio #612). Non-DuckDB
    errors (validation, bad parameters) already read well and pass through.
    """
    if not isinstance(exc, duckdb.Error):
        return click.ClickException(str(exc))
    message = str(exc).split("\nLINE ")[0].strip()
    if where:
        message += (
            f'\n\nThis is most likely caused by --where "{where}" '
            "-- check the column names and SQL syntax."
        )
    return click.ClickException(message)


@click.group()
@click.pass_context
def process(ctx):
    """Transform or reduce GeoParquet data (aggregate, overview, ...)."""
    pass


@process.command(name="overview")
@click.argument("input_parquet")
@click.option(
    "--levels",
    default=None,
    help=(
        "Comma-separated coarser levels to build (grid resolutions like '4,7'; "
        "admin: 'country'). Default: auto-select against --max-tile-kb."
    ),
)
@click.option(
    "--max-tile-kb",
    type=int,
    default=500,
    show_default=True,
    help="Tile-size budget in KB driving auto level selection.",
)
@click.option(
    "--bytes-per-cell",
    type=float,
    default=None,
    help="Override the estimated compressed bytes per cell used in auto selection.",
)
@click.option(
    "--cell-column",
    default=None,
    help="Cell id column when auto-detection fails (default: a5_cell/h3_cell/admin_code).",
)
@click.option(
    "--scheme",
    type=click.Choice(["a5", "h3", "admin"]),
    default=None,
    help=(
        "Bucketing scheme of the cell column when inference is ambiguous "
        "(e.g. H3 ids stored as integers)."
    ),
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default=None,
    help="Directory for overview files (default: alongside the input).",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Overwrite existing overview output files.",
)
@click.option(
    "--overview-out",
    type=click.Path(),
    default=None,
    help="Also assemble the ladder into ONE levelled overview GeoParquet at this "
    "path (levels as rows, tagged by a `level` column). Tile it in one pass with "
    "`tylertoo export-pmtiles`.",
)
@click.option(
    "--cell-detail",
    type=float,
    default=None,
    help="Cell width in GSD units at the level serving it (default 4): each level's "
    "GSD is its measured cell width divided by this. Larger serves every level at a "
    "finer zoom. Only with --overview-out.",
)
@click.option(
    "--gsd",
    "explicit_gsd",
    default=None,
    help="Explicit GSDs in metres, coarse to fine, strictly decreasing, one per built "
    "level plus the base (e.g. 2000,800,300,120 for --levels 5,6,7). Overrides "
    "--cell-detail. Only with --overview-out.",
)
@compression_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def process_overview(
    ctx,
    input_parquet,
    levels,
    max_tile_kb,
    bytes_per_cell,
    cell_column,
    scheme,
    output_dir,
    force,
    compression,
    compression_level,
    verbose,
    geoparquet_version,
    show_sql,
    overview_out,
    cell_detail,
    explicit_gsd,
):
    """Build coarser overview levels from an aggregate output.

    Reads a `gpio process aggregate` output, detects its scheme (a5/h3/admin)
    and base level, and writes one GeoParquet sibling per coarser level
    (`cells.parquet` -> `cells_r4.parquet`; admin -> `by_region_country.parquet`).
    Counts, sums, mins, maxes, and breakdown counts roll up exactly; averages
    are count-weighted.

    Examples:

        gpio process overview cells.parquet

        gpio process overview cells.parquet --levels 4,7

        gpio process overview by_region.parquet --levels country

        gpio process overview cells.parquet --max-tile-kb 300
    """
    with _activate_s3(ctx):
        if (cell_detail is not None or explicit_gsd is not None) and not overview_out:
            raise click.ClickException(
                "--cell-detail and --gsd size the levels of a single overview "
                "file; pass --overview-out to write one."
            )
        kwargs = {
            "levels": levels,
            "max_tile_kb": max_tile_kb,
            "bytes_per_cell": bytes_per_cell,
            "cell_column": cell_column,
            "scheme": scheme,
            "output_dir": output_dir,
            "compression": compression.upper(),
            "compression_level": compression_level,
            "geoparquet_version": geoparquet_version,
            "force": force,
            "verbose": verbose,
            "show_sql": show_sql,
        }
        try:
            if overview_out:
                create_overview_file(
                    input_parquet,
                    overview_out,
                    cell_detail=cell_detail,
                    explicit_gsd=explicit_gsd,
                    **kwargs,
                )
            else:
                create_overviews_impl(input_parquet, **kwargs)
        except (InvalidParameterError, ValueError, duckdb.Error) as exc:
            raise click.ClickException(str(exc)) from exc


@process.group(name="aggregate")
@click.pass_context
def process_aggregate(ctx):
    """Aggregate features into spatial buckets with per-bucket statistics.

    Reduces large datasets into a small file of grid cells or admin regions,
    each carrying a count and optional metric/breakdown columns, for low-zoom
    visualization. Subcommands choose the bucketing scheme.
    """
    pass


@process_aggregate.command(name="a5")
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option(
    "--resolution",
    type=click.IntRange(0, 30),
    default=None,
    help="A5 resolution (0-30). Required unless --auto.",
)
@grid_aggregate_options
@compression_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def process_aggregate_a5(
    ctx,
    input_parquet,
    output_parquet,
    resolution,
    auto,
    target_per_cell,
    max_cells,
    metric,
    metric_nodata,
    breakdown,
    breakdown_limit,
    breakdown_metric,
    out_geometry,
    where,
    bucket_point,
    bbox_column,
    compression,
    compression_level,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Aggregate features into A5 grid cells.

    Examples:

        gpio process aggregate a5 fields.parquet cells.parquet --resolution 8
        gpio process aggregate a5 fields.parquet cells.parquet --auto \\
            --metric "sum:area_ha" --breakdown crop_type
        gpio process aggregate a5 fields.parquet cells.parquet --resolution 8 \\
            --where "\\"crop:name\\" = 'wheat'"
        gpio process aggregate a5 buildings.parquet cells.parquet --auto \\
            --metric "avg:height,max:height" --metric-nodata "-999"
        gpio process aggregate a5 buildings.parquet cells.parquet --auto \\
            --bucket-point bbox --metric "avg:height"
        gpio process aggregate a5 fields.parquet cells.csv-like.parquet \\
            --resolution 8 --out-geometry none
    """
    with _activate_s3(ctx):
        try:
            aggregate_by_a5_impl(
                input_parquet,
                output_parquet,
                resolution=resolution,
                auto=auto,
                target_per_cell=target_per_cell,
                max_cells=max_cells,
                metric=metric,
                breakdown=breakdown,
                breakdown_limit=breakdown_limit,
                breakdown_metric=breakdown_metric,
                out_geometry=out_geometry,
                compression=compression.upper(),
                compression_level=compression_level,
                geoparquet_version=geoparquet_version,
                verbose=verbose,
                show_sql=show_sql,
                where=where,
                metric_nodata=metric_nodata,
                bucket_point=bucket_point,
                bbox_column=bbox_column,
            )
        except (InvalidParameterError, ValidationError, ValueError, duckdb.Error) as exc:
            raise _aggregate_error(exc, where) from exc


@process_aggregate.command(name="h3")
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option(
    "--resolution",
    type=click.IntRange(0, 15),
    default=None,
    help="H3 resolution (0-15). Required unless --auto.",
)
@grid_aggregate_options
@compression_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def process_aggregate_h3(
    ctx,
    input_parquet,
    output_parquet,
    resolution,
    auto,
    target_per_cell,
    max_cells,
    metric,
    metric_nodata,
    breakdown,
    breakdown_limit,
    breakdown_metric,
    out_geometry,
    where,
    bucket_point,
    bbox_column,
    compression,
    compression_level,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Aggregate features into H3 grid cells.

    Examples:

        gpio process aggregate h3 fields.parquet cells.parquet --resolution 8
        gpio process aggregate h3 fields.parquet cells.parquet --auto \\
            --metric "sum:area_ha" --breakdown crop_type
        gpio process aggregate h3 fields.parquet cells.parquet --resolution 8 \\
            --where "confidence >= 50"
        gpio process aggregate h3 buildings.parquet cells.parquet --auto \\
            --metric "avg:height" --metric-nodata "-999"
        gpio process aggregate h3 buildings.parquet cells.parquet --auto \\
            --bucket-point bbox
        gpio process aggregate h3 fields.parquet cells.parquet \\
            --resolution 8 --out-geometry none
    """
    with _activate_s3(ctx):
        try:
            aggregate_by_h3_impl(
                input_parquet,
                output_parquet,
                resolution=resolution,
                auto=auto,
                target_per_cell=target_per_cell,
                max_cells=max_cells,
                metric=metric,
                breakdown=breakdown,
                breakdown_limit=breakdown_limit,
                breakdown_metric=breakdown_metric,
                out_geometry=out_geometry,
                compression=compression.upper(),
                compression_level=compression_level,
                geoparquet_version=geoparquet_version,
                verbose=verbose,
                show_sql=show_sql,
                where=where,
                metric_nodata=metric_nodata,
                bucket_point=bucket_point,
                bbox_column=bbox_column,
            )
        except (InvalidParameterError, ValidationError, ValueError, duckdb.Error) as exc:
            raise _aggregate_error(exc, where) from exc


@process_aggregate.command(name="admin")
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option(
    "--level",
    type=click.Choice(["country", "region"]),
    default="country",
    help="Administrative level to aggregate to (default: country).",
)
@click.option(
    "--metric",
    default=None,
    help='Numeric rollups, e.g. "sum:area_ha,avg:yield". Bare column = sum.',
)
@metric_nodata_option
@click.option(
    "--breakdown",
    default=None,
    help="Categorical column to pivot count by.",
)
@click.option(
    "--breakdown-limit",
    type=int,
    default=20,
    help="Max breakdown values before remainder rolls into the other bucket (default: 20).",
)
@breakdown_metric_option
@click.option(
    "--out-geometry",
    type=click.Choice(["polygon", "centroid", "both", "none"]),
    default="polygon",
    help="Output geometry per region (default: polygon).",
)
@where_option
@bucket_point_options
@compression_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def process_aggregate_admin(
    ctx,
    input_parquet,
    output_parquet,
    level,
    metric,
    metric_nodata,
    breakdown,
    breakdown_limit,
    breakdown_metric,
    out_geometry,
    where,
    bucket_point,
    bbox_column,
    compression,
    compression_level,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Aggregate features into administrative regions.

    Examples:

        gpio process aggregate admin fields.parquet by_country.parquet --level country
        gpio process aggregate admin fields.parquet by_region.parquet \\
            --level region --metric "sum:area_ha" --breakdown crop_type
        gpio process aggregate admin fields.parquet by_country.parquet \\
            --level country --where "confidence >= 50"
        gpio process aggregate admin buildings.parquet by_country.parquet \\
            --level country --metric "avg:height" --metric-nodata "-999"
        gpio process aggregate admin buildings.parquet by_country.parquet \\
            --level country --bucket-point bbox
    """
    with _activate_s3(ctx):
        try:
            aggregate_by_admin_impl(
                input_parquet,
                output_parquet,
                level=level,
                metric=metric,
                breakdown=breakdown,
                breakdown_limit=breakdown_limit,
                breakdown_metric=breakdown_metric,
                out_geometry=out_geometry,
                compression=compression.upper(),
                compression_level=compression_level,
                geoparquet_version=geoparquet_version,
                verbose=verbose,
                show_sql=show_sql,
                where=where,
                metric_nodata=metric_nodata,
                bucket_point=bucket_point,
                bbox_column=bbox_column,
            )
        except (InvalidParameterError, ValidationError, ValueError, duckdb.Error) as exc:
            raise _aggregate_error(exc, where) from exc


@process.command(name="simplify")
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option(
    "--tolerance",
    type=float,
    required=True,
    help="Simplification tolerance, in the geometry's CRS units.",
)
@click.option(
    "--coverage/--no-coverage",
    default=False,
    help=(
        "Treat the input as a polygonal coverage: shared edges stay shared "
        "and no gaps or overlaps are introduced (coarsen coverage_simplify)."
    ),
)
@click.option(
    "--preserve-topology/--no-preserve-topology",
    "preserve_topology",
    default=None,
    help="Keep geometries valid while simplifying (default: on; plain mode only).",
)
@click.option(
    "--simplify-boundary/--no-simplify-boundary",
    "simplify_boundary",
    default=None,
    help="Also simplify the coverage's outer boundary (default: on; --coverage only).",
)
@click.option(
    "--threads",
    type=int,
    default=None,
    help="Worker threads for coarsen (default: let the library decide).",
)
@click.option(
    "--geometry-column",
    default=None,
    help="Geometry column to simplify (default: the file's primary geometry column).",
)
@row_group_options
@compression_options
@geoparquet_version_option
@verbose_option
@handle_geoparquet_errors
@click.pass_context
def process_simplify(
    ctx,
    input_parquet,
    output_parquet,
    tolerance,
    coverage,
    preserve_topology,
    simplify_boundary,
    threads,
    geometry_column,
    row_group_size,
    row_group_size_mb,
    compression,
    compression_level,
    geoparquet_version,
    verbose,
):
    """Simplify geometries with coarsen (GEOS-identical, multithreaded Rust).

    Examples:

        gpio process simplify parcels.parquet simplified.parquet --tolerance 10

        gpio process simplify admin.parquet simplified.parquet \\
            --tolerance 0.001 --coverage
    """
    if coverage and preserve_topology is not None:
        raise click.UsageError(
            "--preserve-topology/--no-preserve-topology applies to plain mode "
            "and cannot be combined with --coverage (coverage simplification "
            "always preserves the coverage's topology)."
        )
    if not coverage and simplify_boundary is not None:
        raise click.UsageError(
            "--simplify-boundary/--no-simplify-boundary only applies with --coverage."
        )
    with _activate_s3(ctx):
        try:
            simplify_file_impl(
                input_parquet,
                output_parquet,
                tolerance,
                coverage=coverage,
                preserve_topology=True if preserve_topology is None else preserve_topology,
                simplify_boundary=True if simplify_boundary is None else simplify_boundary,
                threads=threads,
                geometry_column=geometry_column,
                compression=compression.upper(),
                compression_level=compression_level,
                row_group_size_mb=row_group_size_mb,
                row_group_rows=row_group_size,
                geoparquet_version=geoparquet_version,
                verbose=verbose,
            )
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc


def _parse_float_csv(value: str | None, option_name: str) -> list[float] | None:
    """Parse a comma-separated float option; None passes through."""
    if value is None:
        return None
    try:
        return [float(part) for part in value.split(",")]
    except ValueError as exc:
        raise click.BadParameter(
            f"expected comma-separated numbers, got '{value}'", param_hint=option_name
        ) from exc


@process.command(name="polygonize")
@click.argument("input_raster")
@click.argument("output_parquet")
@click.option("--band", type=int, default=1, show_default=True, help="Raster band to polygonize.")
@click.option(
    "--values",
    default=None,
    help="Comma-separated pixel values to keep (e.g. '1,3'). Default: every class.",
)
@click.option(
    "--value-column",
    default="value",
    show_default=True,
    help="Name of the class attribute column in the output.",
)
@click.option(
    "--nodata",
    type=float,
    default=None,
    help="Nodata value to exclude (default: the raster's own nodata tag).",
)
@click.option(
    "--no-mask",
    is_flag=True,
    help="Ignore the raster's mask/nodata and polygonize every pixel.",
)
@row_group_options
@compression_options
@geoparquet_version_option
@verbose_option
@handle_geoparquet_errors
@click.pass_context
def process_polygonize(
    ctx,
    input_raster,
    output_parquet,
    band,
    values,
    value_column,
    nodata,
    no_mask,
    row_group_size,
    row_group_size_mb,
    compression,
    compression_level,
    geoparquet_version,
    verbose,
):
    """Polygonize a categorical raster into GeoParquet (contourrs).

    Traces land-cover classes, segmentation masks and other categorical
    rasters into polygons, one feature per contiguous region.

    Examples:

        gpio process polygonize landcover.tif landcover.parquet

        gpio process polygonize mask.tif buildings.parquet --values 1
    """
    from geoparquet_io.core.process.raster.polygonize import polygonize_file

    value_list = _parse_float_csv(values, "--values")
    with _activate_s3(ctx):
        try:
            polygonize_file(
                input_raster,
                output_parquet,
                band=band,
                values=value_list,
                value_column=value_column,
                nodata=nodata,
                use_mask=not no_mask,
                compression=compression.upper(),
                compression_level=compression_level,
                row_group_size_mb=row_group_size_mb,
                row_group_rows=row_group_size,
                geoparquet_version=geoparquet_version,
                verbose=verbose,
            )
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc


@process.command(name="contour")
@click.argument("input_raster")
@click.argument("output_parquet")
@click.option(
    "--levels",
    default=None,
    help="Explicit comma-separated break values (e.g. '0,100,250,500').",
)
@click.option(
    "--interval",
    type=float,
    default=None,
    help="Generate breaks every N units from --base, spanning the band's range.",
)
@click.option(
    "--base",
    type=float,
    default=0.0,
    show_default=True,
    help="Offset for --interval breaks.",
)
@click.option("--band", type=int, default=1, show_default=True, help="Raster band to contour.")
@click.option(
    "--nodata",
    type=float,
    default=None,
    help="Nodata value to exclude (default: the raster's own nodata tag).",
)
@click.option(
    "--min-column",
    default="min",
    show_default=True,
    help="Name of the band's lower-break attribute column.",
)
@click.option(
    "--max-column",
    default="max",
    show_default=True,
    help="Name of the band's upper-break attribute column.",
)
@row_group_options
@compression_options
@geoparquet_version_option
@verbose_option
@handle_geoparquet_errors
@click.pass_context
def process_contour(
    ctx,
    input_raster,
    output_parquet,
    levels,
    interval,
    base,
    band,
    nodata,
    min_column,
    max_column,
    row_group_size,
    row_group_size_mb,
    compression,
    compression_level,
    geoparquet_version,
    verbose,
):
    """Extract filled contour bands from an elevation raster (contourrs).

    Each output feature is one band polygon attributed with its [min, max)
    break values.

    Examples:

        gpio process contour dem.tif contours.parquet --interval 100

        gpio process contour dem.tif contours.parquet --levels 0,250,500,1000
    """
    from geoparquet_io.core.process.raster.contour import contour_file

    if levels is not None and interval is not None:
        raise click.UsageError("--levels and --interval are mutually exclusive.")
    if levels is None and interval is None:
        raise click.UsageError("one of --levels or --interval is required.")
    level_list = _parse_float_csv(levels, "--levels")
    with _activate_s3(ctx):
        try:
            contour_file(
                input_raster,
                output_parquet,
                levels=level_list,
                interval=interval,
                base=base,
                band=band,
                nodata=nodata,
                min_column=min_column,
                max_column=max_column,
                compression=compression.upper(),
                compression_level=compression_level,
                row_group_size_mb=row_group_size_mb,
                row_group_rows=row_group_size,
                geoparquet_version=geoparquet_version,
                verbose=verbose,
            )
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc
