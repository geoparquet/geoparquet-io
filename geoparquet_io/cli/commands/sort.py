"""``gpio sort`` - commands for sorting GeoParquet files.

Registered on the root ``gpio`` group by ``cli/main.py`` via
``cli.add_command(sort)``. This module must not import ``cli.main``: the
dependency runs one way, ``main`` -> ``commands`` -> ``_shared``/``decorators``.
"""

import click

from geoparquet_io.cli._shared import activate_remote, init_group_context, prepare_output
from geoparquet_io.cli.decorators import (
    SingleFileCommand,
    allow_schema_diff_option,
    any_extension_option,
    bbox_option,
    column_name_option,
    geoparquet_version_option,
    output_format_options,
    overwrite_option,
    pack_write_options,
    show_sql_option,
    verbose_option,
)
from geoparquet_io.core.file_utils import validate_parquet_extension
from geoparquet_io.core.hilbert_order import hilbert_order as hilbert_impl
from geoparquet_io.core.index_ops import get_index_spec
from geoparquet_io.core.sort_by_column import sort_by_column as sort_by_column_impl
from geoparquet_io.core.sort_quadkey import sort_by_quadkey as sort_by_quadkey_impl
from geoparquet_io.core.str_order import str_order as str_impl


@click.group()
@click.pass_context
def sort(ctx):
    """Commands for sorting GeoParquet files."""
    init_group_context(ctx)


@sort.command(name="hilbert", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", type=click.Path(), required=False, default=None)
@column_name_option(
    "--geometry-column",
    "-g",
    default=None,
    help="Name of the geometry column (default: the file's primary geometry column)",
)
@bbox_option
@output_format_options
@geoparquet_version_option
@overwrite_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
@pack_write_options
def hilbert_order(
    ctx,
    input_parquet,
    output_parquet,
    geometry_column,
    add_bbox,
    write,
    overwrite,
    verbose,
    any_extension,
    show_sql,
):
    """
    Reorder a GeoParquet file using Hilbert curve ordering.

    Takes an input GeoParquet file and creates a new file with rows ordered
    by their position along a Hilbert space-filling curve.

    Applies optimal formatting (configurable compression, optimized row groups,
    bbox metadata). A non-default CRS is carried through unchanged; a CRS that
    spells out the GeoParquet default (OGC:CRS84 / EPSG:4326) is normalized to
    how the spec writes it, an omitted crs key. Use --verbose to see when that
    happens.

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.
    """
    with activate_remote(ctx):
        prepare_output(output_parquet, any_extension)

        hilbert_impl(
            input_parquet,
            output_parquet,
            geometry_column,
            add_bbox,
            verbose,
            overwrite=overwrite,
            **write.funnel_kwargs(),
        )


@sort.command(name="str", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", type=click.Path(), required=False, default=None)
@column_name_option(
    "--geometry-column",
    "-g",
    default=None,
    help="Name of the geometry column (default: the file's primary geometry column)",
)
@bbox_option
@output_format_options
@geoparquet_version_option
@overwrite_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
@pack_write_options
def str_order_command(
    ctx,
    input_parquet,
    output_parquet,
    geometry_column,
    add_bbox,
    write,
    overwrite,
    verbose,
    any_extension,
    show_sql,
):
    """Pack GeoParquet rows with Sort-Tile-Recursive ordering.

    STR sorts geometry bounding-box centers into X strips, sorts each strip on
    Y, and alternates the Y direction between strips. This can produce tighter
    row-group bounding boxes than a space-filling curve.

    --row-group-size does double duty: it is the writer's row-group target, and
    it selects how many X strips STR builds, as
    ceil(sqrt(num_rows / row-group-size)). That makes it a coarse control -
    nearby values often produce an identical ordering. Rows are not packed into
    row-group-sized tiles, but the value is snapped to a whole 2048-row writer
    vector before either use, so tiles and row groups line up whatever you
    pass.
    """
    with activate_remote(ctx):
        prepare_output(output_parquet, any_extension)
        str_impl(
            input_parquet,
            output_parquet,
            geometry_column=geometry_column,
            add_bbox_flag=add_bbox,
            verbose=verbose,
            overwrite=overwrite,
            **write.funnel_kwargs(),
        )


@sort.command(name="column", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", type=click.Path())
@click.argument("columns")
@click.option(
    "--descending",
    is_flag=True,
    help="Sort in descending order (default: ascending)",
)
@allow_schema_diff_option
@output_format_options
@geoparquet_version_option
@overwrite_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
@pack_write_options
def sort_column(
    ctx,
    input_parquet,
    output_parquet,
    columns,
    descending,
    allow_schema_diff,
    write,
    overwrite,
    verbose,
    any_extension,
    show_sql,
):
    """
    Sort a GeoParquet file by specified column(s).

    COLUMNS is a comma-separated list of column names to sort by.

    Examples:

        gpio sort column input.parquet output.parquet name

        gpio sort column input.parquet output.parquet name,date --descending

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.
    """
    with activate_remote(ctx):
        # Validate .parquet extension
        validate_parquet_extension(output_parquet, any_extension)

        sort_by_column_impl(
            input_parquet,
            output_parquet,
            columns=columns,
            descending=descending,
            verbose=verbose,
            overwrite=overwrite,
            allow_schema_diff=allow_schema_diff,
            **write.funnel_kwargs(),
        )


@sort.command(name="quadkey", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", type=click.Path())
@column_name_option(
    "--quadkey-name",
    default="quadkey",
    help="Name of the quadkey column to sort by (default: quadkey)",
)
@click.option(
    "--resolution",
    default=get_index_spec("quadkey").default_resolution,
    type=click.IntRange(
        get_index_spec("quadkey").min_resolution, get_index_spec("quadkey").max_resolution
    ),
    help=f"Resolution when auto-adding quadkey column ({get_index_spec('quadkey').bounds_text}). Default: {get_index_spec('quadkey').default_resolution}",
)
@click.option(
    "--use-centroid",
    is_flag=True,
    help="Use geometry centroid when auto-adding quadkey column",
)
@click.option(
    "--remove-quadkey-column",
    is_flag=True,
    help="Exclude quadkey column from output after sorting",
)
@allow_schema_diff_option
@output_format_options
@geoparquet_version_option
@overwrite_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
@pack_write_options
def sort_quadkey(
    ctx,
    input_parquet,
    output_parquet,
    quadkey_name,
    resolution,
    use_centroid,
    remove_quadkey_column,
    allow_schema_diff,
    write,
    overwrite,
    verbose,
    any_extension,
    show_sql,
):
    """
    Sort a GeoParquet file by quadkey spatial index.

    If the quadkey column doesn't exist and using the default column name,
    it will be auto-added at the specified resolution. If using --quadkey-name
    and the column is missing, an error is raised.

    Use --remove-quadkey-column to exclude the quadkey column from output
    after sorting (useful when you only want the sorted order).

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.
    """
    with activate_remote(ctx):
        # Validate .parquet extension
        validate_parquet_extension(output_parquet, any_extension)

        sort_by_quadkey_impl(
            input_parquet,
            output_parquet,
            quadkey_column_name=quadkey_name,
            resolution=resolution,
            use_centroid=use_centroid,
            remove_quadkey_column=remove_quadkey_column,
            verbose=verbose,
            overwrite=overwrite,
            allow_schema_diff=allow_schema_diff,
            **write.funnel_kwargs(),
        )
