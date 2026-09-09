"""``gpio partition`` - split GeoParquet files into partitioned datasets.

Registered on the root ``gpio`` group by ``cli/main.py`` via
``cli.add_command(partition)``. This module must not import ``cli.main``: the
dependency runs one way, ``main`` -> ``commands`` -> ``_shared``/``decorators``.
"""

import click

from geoparquet_io.cli._shared import _activate_s3, init_group_context
from geoparquet_io.cli.decorators import (
    SingleFileCommand,
    geoparquet_version_option,
    handle_directory_sub_partition,
    output_format_options,
    parse_row_group_options,
    partition_options,
    partition_options_base,
    show_sql_option,
    verbose_option,
)
from geoparquet_io.core.partition.admin_hierarchical import (
    partition_by_admin_hierarchical as partition_admin_hierarchical_impl,
)
from geoparquet_io.core.partition.by_a5 import partition_by_a5 as partition_by_a5_impl
from geoparquet_io.core.partition.by_h3 import partition_by_h3 as partition_by_h3_impl
from geoparquet_io.core.partition.by_kdtree import partition_by_kdtree as partition_by_kdtree_impl
from geoparquet_io.core.partition.by_quadkey import (
    partition_by_quadkey as partition_by_quadkey_impl,
)
from geoparquet_io.core.partition.by_s2 import partition_by_s2 as partition_by_s2_impl
from geoparquet_io.core.partition.by_string import (
    partition_by_string as partition_by_string_impl,
)


# Partition commands group
@click.group()
@click.pass_context
def partition(ctx):
    """Commands for partitioning GeoParquet files."""
    init_group_context(ctx)


@partition.command(name="admin", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_folder", required=False)
@click.option(
    "--dataset",
    type=click.Choice(["gaul", "overture"], case_sensitive=False),
    default="gaul",
    help="Admin boundaries dataset: 'gaul' (GAUL L2) or 'overture' (Overture Maps)",
)
@click.option(
    "--levels",
    required=False,
    default=None,
    help="Comma-separated hierarchical levels to partition by. "
    "GAUL levels: continent,country,department. "
    "Overture levels: country,region. "
    "Not required when --vecorel is used.",
)
@click.option(
    "--vecorel",
    is_flag=True,
    help="Output Vecorel-compliant admin columns (admin:country_code, "
    "admin:subdivision_code) in each partition with schema metadata. "
    "Automatically uses the Overture dataset with country,region levels.",
)
@partition_options_base
@output_format_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def partition_admin(
    ctx,
    input_parquet,
    output_folder,
    dataset,
    levels,
    vecorel,
    hive,
    overwrite,
    preview,
    preview_limit,
    force,
    skip_analysis,
    prefix,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Partition by administrative boundaries via spatial join with remote datasets.

    This command performs a two-step operation:
    1. Spatially joins input data with remote admin boundaries (GAUL or Overture)
    2. Partitions the enriched data by specified admin levels

    \b
    **Datasets:**
    - gaul: GAUL L2 Admin Boundaries (levels: continent, country, department)
    - overture: Overture Maps Divisions (levels: country, region)

    \b
    **Examples:**

    \b
    # Preview GAUL partitions by continent
    gpio partition admin input.parquet --dataset gaul --levels continent --preview

    \b
    # Partition by continent and country
    gpio partition admin input.parquet output/ --dataset gaul --levels continent,country

    \b
    # All GAUL levels with Hive-style (continent=Africa/country=Kenya/...)
    gpio partition admin input.parquet output/ --dataset gaul \\
        --levels continent,country,department --hive

    \b
    # Overture Maps by country and region
    gpio partition admin input.parquet output/ --dataset overture --levels country,region

    \b
    # Vecorel-compliant partitions (forces Overture country,region)
    gpio partition admin input.parquet output/ --vecorel

    \b
    **Note:** This command fetches remote boundaries and performs spatial intersection.
    Requires internet connection. Input data must have valid geometries in WGS84 or
    compatible CRS.
    """
    with _activate_s3(ctx):
        # If preview mode, output_folder is not required
        if not preview and not output_folder:
            raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

        # Validate mutual exclusivity of row group options and get MB value
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        # Handle --vecorel: force Overture dataset with country,region levels
        if vecorel:
            from geoparquet_io.core.logging_config import warn as log_warn

            if dataset != "overture":
                log_warn(f"--vecorel overrides --dataset {dataset} to 'overture'")
            dataset = "overture"
            if levels:
                log_warn("--vecorel overrides --levels to 'country,region'")
            level_list = ["country", "region"]
        elif levels:
            level_list = [level.strip() for level in levels.split(",")]
        else:
            raise click.UsageError("Either --levels or --vecorel is required")

        # Use hierarchical partitioning (spatial join + partition)
        partition_admin_hierarchical_impl(
            input_parquet,
            output_folder,
            dataset_name=dataset,
            levels=level_list,
            vecorel=vecorel,
            hive=hive,
            overwrite=overwrite,
            preview=preview,
            preview_limit=preview_limit,
            verbose=verbose,
            force=force,
            skip_analysis=skip_analysis,
            filename_prefix=prefix,
            geoparquet_version=geoparquet_version,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            memory_limit=write_memory,
        )


@partition.command(name="string", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_folder", required=False)
@click.option("--column", required=True, help="Column name to partition by (required)")
@click.option("--chars", type=int, help="Number of characters to use as prefix for partitioning")
@partition_options_base
@output_format_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def partition_string(
    ctx,
    input_parquet,
    output_folder,
    column,
    chars,
    hive,
    overwrite,
    preview,
    preview_limit,
    force,
    skip_analysis,
    prefix,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Partition a GeoParquet file by string column values.

    Creates separate GeoParquet files based on distinct values in the specified column.
    When --chars is provided, partitions by the first N characters of the column values.

    Use --preview to see what partitions would be created without actually creating files.

    Examples:

        # Preview partitions by first character of MGRS codes
        gpio partition string input.parquet --column MGRS --chars 1 --preview

        # Partition by full column values
        gpio partition string input.parquet output/ --column category

        # Partition by first character of MGRS codes
        gpio partition string input.parquet output/ --column mgrs --chars 1

        # Use Hive-style partitioning
        gpio partition string input.parquet output/ --column region --hive
    """
    with _activate_s3(ctx):
        from geoparquet_io.core.streaming import StreamingError

        # If preview mode, output_folder is not required
        if not preview and not output_folder:
            raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

        # Validate mutual exclusivity of row group options and get MB value
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        try:
            partition_by_string_impl(
                input_parquet,
                output_folder,
                column,
                chars,
                hive,
                overwrite,
                preview,
                preview_limit,
                verbose,
                force,
                skip_analysis,
                prefix,
                None,
                geoparquet_version,
                compression=compression.upper(),
                compression_level=compression_level,
                row_group_size_mb=row_group_mb,
                row_group_rows=row_group_size,
                memory_limit=write_memory,
            )
        except StreamingError as e:
            raise click.ClickException(str(e)) from None


@partition.command(name="h3", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_folder", required=False)
@click.option(
    "--h3-name",
    default="h3_cell",
    help="Name of H3 column to partition by (default: h3_cell)",
)
@click.option(
    "--resolution",
    type=click.IntRange(0, 15),
    default=None,
    help="H3 resolution for partitioning (0-15). Required unless --auto is used.",
)
@click.option(
    "--auto",
    is_flag=True,
    help="Automatically calculate optimal resolution based on data size",
)
@click.option(
    "--target-rows",
    type=int,
    default=100000,
    help="Target rows per partition for auto mode (default: 100000)",
)
@click.option(
    "--max-partitions",
    type=int,
    default=10000,
    help="Maximum number of partitions for auto mode (default: 10000)",
)
@click.option(
    "--keep-h3-column",
    is_flag=True,
    help="Keep the H3 column in output files (default: excluded for non-Hive, included for Hive)",
)
@partition_options
@output_format_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def partition_h3(
    ctx,
    input_parquet,
    output_folder,
    h3_name,
    resolution,
    auto,
    target_rows,
    max_partitions,
    keep_h3_column,
    hive,
    overwrite,
    preview,
    preview_limit,
    force,
    skip_analysis,
    min_size,
    in_place,
    prefix,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Partition a GeoParquet file by H3 cells at specified resolution.

    Creates separate GeoParquet files based on H3 cell prefixes at the specified resolution.
    If the H3 column doesn't exist, it will be automatically added before partitioning.

    By default, the H3 column is excluded from output files (since it's redundant with the
    partition path) unless using Hive-style partitioning. Use --keep-h3-column to explicitly
    keep the column in all cases.

    Auto-resolution mode: Use --auto to automatically calculate the optimal H3 resolution
    based on your data size. Control partition sizing with --target-rows (default: 100K rows
    per partition) and --max-partitions (default: 10K partitions max).

    Use --preview to see what partitions would be created without actually creating files.

    Examples:

        # Auto-calculate optimal resolution for ~100K rows per partition
        gpio partition h3 input.parquet output/ --auto

        # Auto-calculate with custom target partition size
        gpio partition h3 input.parquet output/ --auto --target-rows 50000

        # Preview partitions at resolution 7 (~5km² cells)
        gpio partition h3 input.parquet --resolution 7 --preview

        # Partition by H3 cells at specific resolution 9
        gpio partition h3 input.parquet output/ --resolution 9

        # Partition with H3 column kept in output files
        gpio partition h3 input.parquet output/ --resolution 9 --keep-h3-column

        # Use Hive-style partitioning at resolution 8 (H3 column included by default)
        gpio partition h3 input.parquet output/ --resolution 8 --hive

        # Sub-partition all files over 100MB in a directory
        gpio partition h3 /data/partitions/ --min-size 100MB --resolution 4 --in-place
    """
    with _activate_s3(ctx):
        # Handle directory input with --min-size
        if handle_directory_sub_partition(
            input_parquet=input_parquet,
            partition_type="h3",
            min_size=min_size,
            resolution=resolution,
            preview=preview,
            column_name=h3_name,
            output_folder=output_folder,
            in_place=in_place,
            hive=hive,
            overwrite=overwrite,
            verbose=verbose,
            force=force,
            skip_analysis=skip_analysis,
            compression=compression,
            compression_level=compression_level,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
        ):
            return

        # Existing single-file logic continues below...

        # If preview mode, output_folder is not required
        if not preview and not output_folder:
            raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

        # Validate mutual exclusivity of row group options and get MB value
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        # Convert flag to None if not explicitly set, so implementation can determine default
        keep_h3_col = True if keep_h3_column else None

        partition_by_h3_impl(
            input_parquet,
            output_folder,
            h3_name,
            resolution,
            hive,
            overwrite,
            preview,
            preview_limit,
            verbose,
            keep_h3_col,
            force,
            skip_analysis,
            prefix,
            None,
            geoparquet_version,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            memory_limit=write_memory,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
        )


@partition.command(name="s2", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_folder", required=False)
@click.option(
    "--s2-name",
    default="s2_cell",
    help="Name of S2 column to partition by (default: s2_cell)",
)
@click.option(
    "--level",
    type=click.IntRange(0, 30),
    default=None,
    help="S2 level for partitioning (0-30). Required unless --auto is used.",
)
@click.option(
    "--auto",
    is_flag=True,
    help="Automatically calculate optimal level based on data size",
)
@click.option(
    "--target-rows",
    type=int,
    default=100000,
    help="Target rows per partition when using --auto (default: 100000)",
)
@click.option(
    "--max-partitions",
    type=int,
    default=10000,
    help="Maximum partitions when using --auto (default: 10000)",
)
@click.option(
    "--keep-s2-column",
    is_flag=True,
    help="Keep the S2 column in output files (default: excluded for non-Hive, included for Hive)",
)
@partition_options
@output_format_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def partition_s2(
    ctx,
    input_parquet,
    output_folder,
    s2_name,
    level,
    auto,
    target_rows,
    max_partitions,
    keep_s2_column,
    hive,
    overwrite,
    preview,
    preview_limit,
    force,
    skip_analysis,
    min_size,
    in_place,
    prefix,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Partition a GeoParquet file by S2 cells at specified level.

    Creates separate GeoParquet files based on S2 cell tokens. If the S2 column
    doesn't exist, it will be automatically added before partitioning.

    S2 (Google's Spherical Geometry library) uses a hierarchical quadtree structure
    that divides Earth's surface into cells. Level 0 has 6 base cells, and each
    subsequent level subdivides by 4.

    By default, the S2 column is excluded from output files (since it's redundant with the
    partition path) unless using Hive-style partitioning. Use --keep-s2-column to explicitly
    keep the column in all cases.

    Use --preview to see what partitions would be created without actually creating files.

    Auto-resolution mode: Use --auto to automatically calculate the optimal S2 level
    based on your target partition size. Specify --target-rows (default: 100K) to control
    partition granularity.

    Examples:

        # Auto-calculate optimal level for ~100K rows per partition
        gpio partition s2 input.parquet output/ --auto

        # Auto with custom target size (fewer, larger partitions)
        gpio partition s2 input.parquet output/ --auto --target-rows 500000

        # Preview partitions at level 10 (~78km² cells)
        gpio partition s2 input.parquet --level 10 --preview

        # Partition by S2 cells at level 13 (~1.2km² cells)
        gpio partition s2 input.parquet output/ --level 13

        # Partition with S2 column kept in output files
        gpio partition s2 input.parquet output/ --level 12 --keep-s2-column

        # Use Hive-style partitioning (S2 column included by default)
        gpio partition s2 input.parquet output/ --auto --hive

        # Sub-partition all files over 100MB in a directory
        gpio partition s2 /data/partitions/ --min-size 100MB --level 10 --in-place
    """
    with _activate_s3(ctx):
        # Handle directory input with --min-size
        if handle_directory_sub_partition(
            input_parquet=input_parquet,
            partition_type="s2",
            min_size=min_size,
            level=level,  # S2 uses "level" not "resolution"
            preview=preview,
            column_name=s2_name,
            output_folder=output_folder,
            in_place=in_place,
            hive=hive,
            overwrite=overwrite,
            verbose=verbose,
            force=force,
            skip_analysis=skip_analysis,
            compression=compression,
            compression_level=compression_level,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
        ):
            return

        # If preview mode, output_folder is not required
        if not preview and not output_folder:
            raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

        # Validate mutual exclusivity of row group options and get MB value
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        # Convert flag to None if not explicitly set, so implementation can determine default
        keep_s2_col = True if keep_s2_column else None

        partition_by_s2_impl(
            input_parquet,
            output_folder,
            s2_name,
            level,
            hive,
            overwrite,
            preview,
            preview_limit,
            verbose,
            keep_s2_col,
            force,
            skip_analysis,
            prefix,
            None,
            geoparquet_version,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            memory_limit=write_memory,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
        )


@partition.command(name="a5", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_folder", required=False)
@click.option(
    "--a5-name",
    default="a5_cell",
    help="Name of A5 column to partition by (default: a5_cell)",
)
@click.option(
    "--resolution",
    type=click.IntRange(0, 30),
    default=None,
    help="A5 resolution for partitioning (0-30). Required unless --auto is used.",
)
@click.option(
    "--auto",
    is_flag=True,
    help="Automatically calculate optimal resolution based on data size",
)
@click.option(
    "--target-rows",
    type=int,
    default=100000,
    help="Target rows per partition when using --auto (default: 100000)",
)
@click.option(
    "--max-partitions",
    type=int,
    default=10000,
    help="Maximum partitions when using --auto (default: 10000)",
)
@click.option(
    "--keep-a5-column",
    is_flag=True,
    help="Keep the A5 column in output files (default: excluded for non-Hive, included for Hive)",
)
@partition_options
@output_format_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def partition_a5(
    ctx,
    input_parquet,
    output_folder,
    a5_name,
    resolution,
    auto,
    target_rows,
    max_partitions,
    keep_a5_column,
    hive,
    overwrite,
    preview,
    preview_limit,
    force,
    skip_analysis,
    min_size,
    in_place,
    prefix,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Partition a GeoParquet file by A5 cells at specified resolution.

    Creates separate GeoParquet files based on A5 cell IDs at the specified resolution.
    If the A5 column doesn't exist, it will be automatically added before partitioning.

    By default, the A5 column is excluded from output files (since it's redundant with the
    partition path) unless using Hive-style partitioning. Use --keep-a5-column to explicitly
    keep the column in all cases.

    Use --preview to see what partitions would be created without actually creating files.

    Auto-resolution mode: Use --auto to automatically calculate the optimal A5 resolution
    based on your target partition size. Specify --target-rows (default: 100K) to control
    partition granularity.

    Examples:

        # Auto-calculate optimal resolution for ~100K rows per partition
        gpio partition a5 input.parquet output/ --auto

        # Auto with custom target size (fewer, larger partitions)
        gpio partition a5 input.parquet output/ --auto --target-rows 500000

        # Preview partitions at resolution 10 (~41km² cells)
        gpio partition a5 input.parquet --resolution 10 --preview

        # Partition by A5 cells at resolution 15
        gpio partition a5 input.parquet output/ --resolution 15

        # Partition with A5 column kept in output files
        gpio partition a5 input.parquet output/ --resolution 12 --keep-a5-column

        # Use Hive-style partitioning (A5 column included by default)
        gpio partition a5 input.parquet output/ --auto --hive

        # Sub-partition all files over 100MB in a directory
        gpio partition a5 /data/partitions/ --min-size 100MB --resolution 10 --in-place
    """
    with _activate_s3(ctx):
        # Handle directory input with --min-size
        if handle_directory_sub_partition(
            input_parquet=input_parquet,
            partition_type="a5",
            min_size=min_size,
            resolution=resolution,
            preview=preview,
            column_name=a5_name,
            output_folder=output_folder,
            in_place=in_place,
            hive=hive,
            overwrite=overwrite,
            verbose=verbose,
            force=force,
            skip_analysis=skip_analysis,
            compression=compression,
            compression_level=compression_level,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
        ):
            return

        # If preview mode, output_folder is not required
        if not preview and not output_folder:
            raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

        # Validate mutual exclusivity of row group options and get MB value
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        # Convert flag to None if not explicitly set, so implementation can determine default
        keep_a5_col = True if keep_a5_column else None

        partition_by_a5_impl(
            input_parquet,
            output_folder,
            a5_name,
            resolution,
            hive,
            overwrite,
            preview,
            preview_limit,
            verbose,
            keep_a5_col,
            force,
            skip_analysis,
            prefix,
            None,
            geoparquet_version,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            memory_limit=write_memory,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
        )


@partition.command(name="kdtree", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_folder", required=False)
@click.option(
    "--kdtree-name",
    default="kdtree_cell",
    help="Name of KD-tree column to partition by (default: kdtree_cell)",
)
@click.option(
    "--partitions",
    default=None,
    type=int,
    help="Explicit partition count (must be power of 2: 2, 4, 8, ...). Overrides default auto mode.",
)
@click.option(
    "--auto",
    default=None,
    type=int,
    help="Auto-select partitions targeting N rows/partition. Default: 120,000.",
)
@click.option(
    "--approx",
    default=100000,
    type=int,
    help="Use approximate computation by sampling N points (default: 100000). Mutually exclusive with --exact.",
)
@click.option(
    "--exact",
    is_flag=True,
    help="Use exact median computation on full dataset (slower but deterministic). Mutually exclusive with --approx.",
)
@click.option(
    "--keep-kdtree-column",
    is_flag=True,
    help="Keep the KD-tree column in output files (default: excluded for non-Hive, included for Hive)",
)
@partition_options_base
@output_format_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def partition_kdtree(
    ctx,
    input_parquet,
    output_folder,
    kdtree_name,
    partitions,
    auto,
    approx,
    exact,
    keep_kdtree_column,
    hive,
    overwrite,
    preview,
    preview_limit,
    force,
    skip_analysis,
    prefix,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Partition a GeoParquet file by KD-tree cells.

    Creates separate files based on KD-tree partition IDs. If the KD-tree column doesn't
    exist, it will be automatically added. Partition count must be a power of 2.

    By default, auto-selects partitions targeting ~120k rows each using approximate mode
    (O(n) with 100k sample). Use --partitions N for explicit control or --exact for
    deterministic computation.

    Performance Note: Approximate mode is O(n), exact mode is O(n × log2(partitions)).

    Use --verbose to track progress with iteration-by-iteration updates.

    Examples:

        # Preview with auto-selected partitions
        gpio partition kdtree input.parquet --preview

        # Partition with explicit partition count
        gpio partition kdtree input.parquet output/ --partitions 32

        # Partition with exact computation
        gpio partition kdtree input.parquet output/ --partitions 32 --exact

        # Partition with custom sample size
        gpio partition kdtree input.parquet output/ --approx 200000
    """
    with _activate_s3(ctx):
        # Validate mutually exclusive options
        import math

        if sum([partitions is not None, auto is not None]) > 1:
            raise click.UsageError("--partitions and --auto are mutually exclusive")

        # Set defaults
        if partitions is None and auto is None:
            auto = 120000  # Default: auto-select targeting 120k rows/partition

        # Validate partitions if specified
        if partitions is not None:
            if partitions < 2 or (partitions & (partitions - 1)) != 0:
                raise click.UsageError(
                    f"Partitions must be a power of 2 (2, 4, 8, ...), got {partitions}"
                )
            iterations = int(math.log2(partitions))
        else:
            iterations = None  # Will be computed in auto mode

        # Validate mutually exclusive options for approx/exact
        if exact and approx != 100000:
            raise click.UsageError("--approx and --exact are mutually exclusive")

        # Determine sample size
        sample_size = None if exact else approx

        # Prepare auto_target if in auto mode
        if auto is not None:
            target_rows = auto if auto > 0 else 120000
            auto_target = ("rows", target_rows)
        else:
            auto_target = None

        # If preview mode, output_folder is not required
        if not preview and not output_folder:
            raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

        # Validate mutual exclusivity of row group options and get MB value
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        # Convert flag to None if not explicitly set, so implementation can determine default
        keep_kdtree_col = True if keep_kdtree_column else None

        partition_by_kdtree_impl(
            input_parquet,
            output_folder,
            kdtree_name,
            iterations,
            hive,
            overwrite,
            preview,
            preview_limit,
            verbose,
            keep_kdtree_col,
            force,
            skip_analysis,
            sample_size,
            auto_target,
            prefix,
            None,
            geoparquet_version,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            memory_limit=write_memory,
        )


@partition.command(name="quadkey", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_folder", required=False)
@click.option(
    "--quadkey-column",
    default="quadkey",
    help="Name of quadkey column to partition by (default: quadkey)",
)
@click.option(
    "--resolution",
    type=click.IntRange(0, 23),
    default=None,
    help="Resolution for auto-adding quadkey column (0-23). Required unless --auto is used.",
)
@click.option(
    "--partition-resolution",
    type=click.IntRange(0, 23),
    default=None,
    help="Resolution for partitioning as prefix length (0-23). Required unless --auto is used.",
)
@click.option(
    "--auto",
    is_flag=True,
    help="Automatically calculate optimal resolution based on data size",
)
@click.option(
    "--target-rows",
    type=int,
    default=100000,
    help="Target rows per partition when using --auto (default: 100000)",
)
@click.option(
    "--max-partitions",
    type=int,
    default=10000,
    help="Maximum partitions when using --auto (default: 10000)",
)
@click.option(
    "--use-centroid",
    is_flag=True,
    help="Use geometry centroid when auto-adding quadkey column",
)
@click.option(
    "--keep-quadkey-column",
    is_flag=True,
    help="Keep the quadkey column in output files (default: excluded for non-Hive, included for Hive)",
)
@partition_options
@output_format_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def partition_quadkey(
    ctx,
    input_parquet,
    output_folder,
    quadkey_column,
    resolution,
    partition_resolution,
    auto,
    target_rows,
    max_partitions,
    use_centroid,
    keep_quadkey_column,
    hive,
    overwrite,
    preview,
    preview_limit,
    force,
    skip_analysis,
    min_size,
    in_place,
    prefix,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Partition a GeoParquet file by quadkey cells.

    Creates separate GeoParquet files based on quadkey prefixes at the specified
    partition resolution. If the quadkey column doesn't exist, it will be automatically
    added at the specified resolution before partitioning.

    The column is created at --resolution, but partitions are created using
    the first --partition-resolution characters of each quadkey. This allows
    for coarser partitioning while retaining full precision in the column.

    By default, the quadkey column is excluded from output files (since it's redundant
    with the partition path) unless using Hive-style partitioning. Use --keep-quadkey-column
    to explicitly keep the column in all cases.

    Use --preview to see what partitions would be created without actually creating files.

    Auto-resolution mode: Use --auto to automatically calculate the optimal quadkey zoom
    level based on your target partition size. Specify --target-rows (default: 100K) to
    control partition granularity.

    Examples:

        # Auto-calculate optimal resolution for ~100K rows per partition
        gpio partition quadkey input.parquet output/ --auto

        # Auto with custom target size (fewer, larger partitions)
        gpio partition quadkey input.parquet output/ --auto --target-rows 500000

        # Preview partitions with auto-resolution
        gpio partition quadkey input.parquet --auto --preview

        # Partition by quadkey cells at specific resolutions
        gpio partition quadkey input.parquet output/ --resolution 13 --partition-resolution 9

        # Partition with quadkey column kept in output files
        gpio partition quadkey input.parquet output/ --resolution 13 --partition-resolution 9 --keep-quadkey-column

        # Use Hive-style partitioning (quadkey column included by default)
        gpio partition quadkey input.parquet output/ --auto --hive

        # Sub-partition all files over 100MB in a directory
        gpio partition quadkey /data/partitions/ --min-size 100MB --auto --in-place
    """
    with _activate_s3(ctx):
        # Handle directory input with --min-size
        if handle_directory_sub_partition(
            input_parquet=input_parquet,
            partition_type="quadkey",
            min_size=min_size,
            resolution=resolution,
            partition_resolution=partition_resolution,
            use_centroid=use_centroid,
            preview=preview,
            column_name=quadkey_column,
            output_folder=output_folder,
            in_place=in_place,
            hive=hive,
            overwrite=overwrite,
            verbose=verbose,
            force=force,
            skip_analysis=skip_analysis,
            compression=compression,
            compression_level=compression_level,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
        ):
            return

        # If preview mode, output_folder is not required
        if not preview and not output_folder:
            raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

        # Validate mutual exclusivity of row group options and get MB value
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        # Convert flag to None if not explicitly set, so implementation can determine default
        keep_quadkey_col = True if keep_quadkey_column else None

        partition_by_quadkey_impl(
            input_parquet,
            output_folder,
            quadkey_column_name=quadkey_column,
            resolution=resolution,
            partition_resolution=partition_resolution,
            use_centroid=use_centroid,
            hive=hive,
            overwrite=overwrite,
            preview=preview,
            preview_limit=preview_limit,
            verbose=verbose,
            keep_quadkey_column=keep_quadkey_col,
            force=force,
            skip_analysis=skip_analysis,
            filename_prefix=prefix,
            geoparquet_version=geoparquet_version,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            memory_limit=write_memory,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
        )
