"""``gpio sort`` - commands for sorting GeoParquet files.

Registered on the root ``gpio`` group by ``cli/main.py`` via
``cli.add_command(sort)``. This module must not import ``cli.main``: the
dependency runs one way, ``main`` -> ``commands`` -> ``_shared``/``decorators``.
"""

import click

from geoparquet_io.cli._shared import _activate_s3, prepare_output
from geoparquet_io.cli.decorators import (
    SingleFileCommand,
    allow_schema_diff_option,
    any_extension_option,
    geoparquet_version_option,
    output_format_options,
    overwrite_option,
    parse_row_group_options,
    show_sql_option,
    verbose_option,
)
from geoparquet_io.core.file_utils import validate_parquet_extension
from geoparquet_io.core.hilbert_order import hilbert_order as hilbert_impl
from geoparquet_io.core.logging_config import setup_cli_logging
from geoparquet_io.core.parquet_writer import DEFAULT_SORT_ROW_GROUP_ROWS
from geoparquet_io.core.sort_by_column import sort_by_column as sort_by_column_impl
from geoparquet_io.core.sort_quadkey import sort_by_quadkey as sort_by_quadkey_impl
from geoparquet_io.core.str_order import str_order as str_impl


@click.group()
@click.pass_context
def sort(ctx):
    """Commands for sorting GeoParquet files."""
    # Ensure logging is set up (in case this group is invoked directly in tests)
    ctx.ensure_object(dict)
    timestamps = ctx.obj.get("timestamps", False)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


@sort.command(name="hilbert", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", type=click.Path(), required=False, default=None)
@click.option(
    "--geometry-column",
    "-g",
    default="geometry",
    help="Name of the geometry column (default: geometry)",
)
@click.option(
    "--add-bbox", is_flag=True, help="Automatically add bbox column and metadata if missing."
)
@output_format_options(default_rows=DEFAULT_SORT_ROW_GROUP_ROWS)
@geoparquet_version_option
@overwrite_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
def hilbert_order(
    ctx,
    input_parquet,
    output_parquet,
    geometry_column,
    add_bbox,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
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
    with _activate_s3(ctx):
        row_group_mb = prepare_output(
            output_parquet, any_extension, row_group_size, row_group_size_mb
        )

        hilbert_impl(
            input_parquet,
            output_parquet,
            geometry_column,
            add_bbox,
            verbose,
            compression.upper(),
            compression_level,
            row_group_mb,
            row_group_size,
            None,
            geoparquet_version,
            overwrite,
            memory_limit=write_memory,
        )


@sort.command(name="str", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", type=click.Path(), required=False, default=None)
@click.option(
    "--geometry-column",
    "-g",
    default="geometry",
    help="Name of the geometry column (default: geometry)",
)
@click.option(
    "--add-bbox", is_flag=True, help="Automatically add bbox column and metadata if missing."
)
@output_format_options(default_rows=DEFAULT_SORT_ROW_GROUP_ROWS)
@geoparquet_version_option
@overwrite_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
def str_order_command(
    ctx,
    input_parquet,
    output_parquet,
    geometry_column,
    add_bbox,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
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
    row-group-sized tiles, and because the writer rounds row groups up to a
    multiple of 2048, tiles and row groups only line up when --row-group-size
    is itself a multiple of 2048.
    """
    with _activate_s3(ctx):
        row_group_mb = prepare_output(
            output_parquet, any_extension, row_group_size, row_group_size_mb
        )
        str_impl(
            input_parquet,
            output_parquet,
            geometry_column=geometry_column,
            add_bbox_flag=add_bbox,
            verbose=verbose,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            profile=None,
            geoparquet_version=geoparquet_version,
            overwrite=overwrite,
            memory_limit=write_memory,
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
@output_format_options(default_rows=DEFAULT_SORT_ROW_GROUP_ROWS)
@geoparquet_version_option
@overwrite_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
def sort_column(
    ctx,
    input_parquet,
    output_parquet,
    columns,
    descending,
    allow_schema_diff,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
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
    with _activate_s3(ctx):
        # Validate .parquet extension
        validate_parquet_extension(output_parquet, any_extension)

        # Parse row group options
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        sort_by_column_impl(
            input_parquet,
            output_parquet,
            columns=columns,
            descending=descending,
            verbose=verbose,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            geoparquet_version=geoparquet_version,
            overwrite=overwrite,
            memory_limit=write_memory,
            allow_schema_diff=allow_schema_diff,
        )


@sort.command(name="quadkey", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", type=click.Path())
@click.option(
    "--quadkey-name",
    default="quadkey",
    help="Name of the quadkey column to sort by (default: quadkey)",
)
@click.option(
    "--resolution",
    default=13,
    type=click.IntRange(0, 23),
    help="Resolution when auto-adding quadkey column (0-23). Default: 13",
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
@output_format_options(default_rows=DEFAULT_SORT_ROW_GROUP_ROWS)
@geoparquet_version_option
@overwrite_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
def sort_quadkey(
    ctx,
    input_parquet,
    output_parquet,
    quadkey_name,
    resolution,
    use_centroid,
    remove_quadkey_column,
    allow_schema_diff,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
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
    with _activate_s3(ctx):
        # Validate .parquet extension
        validate_parquet_extension(output_parquet, any_extension)

        # Parse row group options
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        sort_by_quadkey_impl(
            input_parquet,
            output_parquet,
            quadkey_column_name=quadkey_name,
            resolution=resolution,
            use_centroid=use_centroid,
            remove_quadkey_column=remove_quadkey_column,
            verbose=verbose,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            geoparquet_version=geoparquet_version,
            overwrite=overwrite,
            memory_limit=write_memory,
            allow_schema_diff=allow_schema_diff,
        )
