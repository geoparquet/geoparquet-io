"""``gpio process`` - transform or reduce GeoParquet data.

Registered on the root ``gpio`` group by ``cli/main.py`` via
``cli.add_command(process)``. This module must not import ``cli.main``: the
dependency runs one way, ``main`` -> ``commands`` -> ``_shared``/``decorators``.
"""

import click
import duckdb

from geoparquet_io.cli._shared import _activate_s3
from geoparquet_io.cli.decorators import (
    bucket_point_options,
    compression_options,
    geoparquet_version_option,
    grid_aggregate_options,
    metric_nodata_option,
    show_sql_option,
    verbose_option,
    where_option,
)
from geoparquet_io.core.exceptions import InvalidParameterError, ValidationError
from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5 as aggregate_by_a5_impl
from geoparquet_io.core.process.aggregate.by_admin import (
    aggregate_by_admin as aggregate_by_admin_impl,
)
from geoparquet_io.core.process.aggregate.by_h3 import aggregate_by_h3 as aggregate_by_h3_impl
from geoparquet_io.core.process.overview import create_overviews as create_overviews_impl

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
        try:
            create_overviews_impl(
                input_parquet,
                levels=levels,
                max_tile_kb=max_tile_kb,
                bytes_per_cell=bytes_per_cell,
                cell_column=cell_column,
                scheme=scheme,
                output_dir=output_dir,
                compression=compression.upper(),
                compression_level=compression_level,
                geoparquet_version=geoparquet_version,
                force=force,
                verbose=verbose,
                show_sql=show_sql,
            )
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
    help="Max breakdown values before remainder rolls into count_other (default: 20).",
)
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
