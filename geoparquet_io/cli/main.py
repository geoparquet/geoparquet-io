import click

from geoparquet_io.cli.decorators import (
    dry_run_option,
    output_format_options,
    partition_options,
    verbose_option,
)
from geoparquet_io.core.add_bbox_column import add_bbox_column as add_bbox_column_impl
from geoparquet_io.core.add_bbox_metadata import add_bbox_metadata as add_bbox_metadata_impl
from geoparquet_io.core.add_country_codes import add_country_codes as add_country_codes_impl
from geoparquet_io.core.add_h3_column import add_h3_column as add_h3_column_impl
from geoparquet_io.core.add_kdtree_column import add_kdtree_column as add_kdtree_column_impl
from geoparquet_io.core.check_parquet_structure import check_all as check_structure_impl
from geoparquet_io.core.check_spatial_order import check_spatial_order as check_spatial_impl
from geoparquet_io.core.hilbert_order import hilbert_order as hilbert_impl
from geoparquet_io.core.inspect_utils import (
    extract_columns_info,
    extract_file_info,
    extract_geo_info,
    format_json_output,
    format_terminal_output,
    get_column_statistics,
    get_preview_data,
)
from geoparquet_io.core.partition_by_h3 import partition_by_h3 as partition_by_h3_impl
from geoparquet_io.core.partition_by_kdtree import partition_by_kdtree as partition_by_kdtree_impl
from geoparquet_io.core.partition_by_string import (
    partition_by_string as partition_by_string_impl,
)
from geoparquet_io.core.split_by_country import split_by_country as split_country_impl

# Version info
__version__ = "0.2.0"


@click.group()
@click.version_option(version=__version__, prog_name="geoparquet-io")
def cli():
    """Fast I/O and transformation tools for GeoParquet files."""
    pass


# Check commands group
@cli.group()
def check():
    """Commands for checking GeoParquet files for best practices."""
    pass


@check.command(name="all")
@click.argument("parquet_file")
@click.option("--verbose", is_flag=True, help="Print full metadata and details")
@click.option(
    "--random-sample-size",
    default=100,
    show_default=True,
    help="Number of rows in each sample for spatial order check.",
)
@click.option(
    "--limit-rows",
    default=500000,
    show_default=True,
    help="Max number of rows to read for spatial order check.",
)
def check_all(parquet_file, verbose, random_sample_size, limit_rows):
    """Run all checks on a GeoParquet file."""
    check_structure_impl(parquet_file, verbose)
    click.echo("\nSpatial Order Analysis:")
    ratio = check_spatial_impl(parquet_file, random_sample_size, limit_rows, verbose)
    if ratio is not None:
        if ratio < 0.5:
            click.echo(click.style("✓ Data appears to be spatially ordered", fg="green"))
        else:
            click.echo(
                click.style(
                    "⚠️  Data may not be optimally spatially ordered\n"
                    "Consider running 'gpio sort hilbert' to improve spatial locality",
                    fg="yellow",
                )
            )


@check.command(name="spatial")
@click.argument("parquet_file")
@click.option(
    "--random-sample-size",
    default=100,
    show_default=True,
    help="Number of rows in each sample for spatial order check.",
)
@click.option(
    "--limit-rows",
    default=500000,
    show_default=True,
    help="Max number of rows to read for spatial order check.",
)
@click.option("--verbose", is_flag=True, help="Print additional information.")
def check_spatial(parquet_file, random_sample_size, limit_rows, verbose):
    """Check if a GeoParquet file is spatially ordered."""
    ratio = check_spatial_impl(parquet_file, random_sample_size, limit_rows, verbose)
    if ratio is not None:
        if ratio < 0.5:
            click.echo(click.style("✓ Data appears to be spatially ordered", fg="green"))
        else:
            click.echo(
                click.style(
                    "⚠️  Data may not be optimally spatially ordered\n"
                    "Consider running 'gpio sort hilbert' to improve spatial locality",
                    fg="yellow",
                )
            )


@check.command(name="compression")
@click.argument("parquet_file")
@click.option("--verbose", is_flag=True, help="Print additional information.")
def check_compression_cmd(parquet_file, verbose):
    """Check compression settings for geometry column."""
    from geoparquet_io.core.check_parquet_structure import check_compression

    check_compression(parquet_file, verbose)


@check.command(name="bbox")
@click.argument("parquet_file")
@click.option("--verbose", is_flag=True, help="Print additional information.")
def check_bbox_cmd(parquet_file, verbose):
    """Check GeoParquet metadata version and bbox structure."""
    from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

    check_metadata_and_bbox(parquet_file, verbose)


@check.command(name="row-group")
@click.argument("parquet_file")
@click.option("--verbose", is_flag=True, help="Print additional information.")
def check_row_group_cmd(parquet_file, verbose):
    """Check row group optimization."""
    from geoparquet_io.core.check_parquet_structure import check_row_groups

    check_row_groups(parquet_file, verbose)


# Inspect command
@cli.command()
@click.argument("parquet_file", type=click.Path(exists=True))
@click.option("--head", type=int, default=None, help="Show first N rows")
@click.option("--tail", type=int, default=None, help="Show last N rows")
@click.option(
    "--stats", is_flag=True, help="Show column statistics (nulls, min/max, unique counts)"
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON for scripting")
@click.option("--geo-metadata", is_flag=True, help="Show GeoParquet metadata from 'geo' key")
@click.option("--parquet-metadata", is_flag=True, help="Show Parquet file metadata")
@click.option("--parquet-geo-metadata", is_flag=True, help="Show geospatial metadata from Parquet footer")
def inspect(parquet_file, head, tail, stats, json_output, geo_metadata, parquet_metadata, parquet_geo_metadata):
    """
    Inspect a GeoParquet file and show metadata summary.

    Provides quick examination of GeoParquet files without launching external tools.
    Default behavior shows metadata only (instant). Use --head/--tail to preview data,
    or --stats to calculate column statistics.

    Examples:

        \b
        # Quick metadata inspection
        gpio inspect data.parquet

        \b
        # Preview first 10 rows
        gpio inspect data.parquet --head 10

        \b
        # Preview last 5 rows
        gpio inspect data.parquet --tail 5

        \b
        # Show statistics
        gpio inspect data.parquet --stats

        \b
        # JSON output for scripting
        gpio inspect data.parquet --json
    """
    import fsspec
    import pyarrow.parquet as pq

    from geoparquet_io.core.common import safe_file_url
    from geoparquet_io.core.inspect_utils import (
        format_geo_metadata_output,
        format_parquet_metadata_output,
        format_parquet_geo_metadata_output,
    )

    # Validate mutually exclusive options
    if head and tail:
        raise click.UsageError("--head and --tail are mutually exclusive")

    try:
        # Handle --geo-metadata flag
        if geo_metadata:
            format_geo_metadata_output(parquet_file, json_output)
            return

        # Handle --parquet-metadata flag
        if parquet_metadata:
            format_parquet_metadata_output(parquet_file, json_output)
            return

        # Handle --parquet-geo-metadata flag
        if parquet_geo_metadata:
            format_parquet_geo_metadata_output(parquet_file, json_output)
            return

        # Extract metadata
        file_info = extract_file_info(parquet_file)
        geo_info = extract_geo_info(parquet_file)

        # Get schema for column info
        safe_url = safe_file_url(parquet_file, verbose=False)
        with fsspec.open(safe_url, "rb") as f:
            pf = pq.ParquetFile(f)
            schema = pf.schema_arrow

        columns_info = extract_columns_info(schema, geo_info.get("primary_column"))

        # Get preview data if requested
        preview_table = None
        preview_mode = None
        if head or tail:
            preview_table, preview_mode = get_preview_data(parquet_file, head=head, tail=tail)

        # Get statistics if requested
        statistics = None
        if stats:
            statistics = get_column_statistics(parquet_file, columns_info)

        # Output
        if json_output:
            output = format_json_output(
                file_info, geo_info, columns_info, preview_table, statistics
            )
            click.echo(output)
        else:
            format_terminal_output(
                file_info, geo_info, columns_info, preview_table, preview_mode, statistics
            )

    except Exception as e:
        raise click.ClickException(str(e)) from e


# Format commands group
@cli.group()
def format():
    """Commands for formatting GeoParquet files."""
    pass


@format.command(name="bbox-metadata")
@click.argument("parquet_file")
@click.option("--verbose", is_flag=True, help="Print detailed information")
def format_bbox_metadata(parquet_file, verbose):
    """Add bbox covering metadata to a GeoParquet file."""
    add_bbox_metadata_impl(parquet_file, verbose)


# Sort commands group
@cli.group()
def sort():
    """Commands for sorting GeoParquet files."""
    pass


@sort.command(name="hilbert")
@click.argument("input_parquet", type=click.Path(exists=True))
@click.argument("output_parquet", type=click.Path())
@click.option(
    "--geometry-column",
    "-g",
    default="geometry",
    help="Name of the geometry column (default: geometry)",
)
@click.option(
    "--add-bbox", is_flag=True, help="Automatically add bbox column and metadata if missing."
)
@output_format_options
@verbose_option
def hilbert_order(
    input_parquet,
    output_parquet,
    geometry_column,
    add_bbox,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    verbose,
):
    """
    Reorder a GeoParquet file using Hilbert curve ordering.

    Takes an input GeoParquet file and creates a new file with rows ordered
    by their position along a Hilbert space-filling curve.

    Applies optimal formatting (configurable compression, optimized row groups,
    bbox metadata) while preserving the CRS. Output is written as GeoParquet 1.1.
    """
    # Validate mutually exclusive options
    if row_group_size and row_group_size_mb:
        raise click.UsageError("--row-group-size and --row-group-size-mb are mutually exclusive")

    # Parse size string if provided
    from geoparquet_io.core.common import parse_size_string

    row_group_mb = None
    if row_group_size_mb:
        try:
            size_bytes = parse_size_string(row_group_size_mb)
            row_group_mb = size_bytes / (1024 * 1024)
        except ValueError as e:
            raise click.UsageError(f"Invalid row group size: {e}") from e

    try:
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
        )
    except Exception as e:
        raise click.ClickException(str(e)) from e


# Add commands group
@cli.group()
def add():
    """Commands for enhancing GeoParquet files in various ways."""
    pass


@add.command(name="admin-divisions")
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option(
    "--countries-file",
    default=None,
    help="Path or URL to countries parquet file. If not provided, uses default from source.coop",
)
@click.option(
    "--add-bbox", is_flag=True, help="Automatically add bbox column and metadata if missing."
)
@output_format_options
@dry_run_option
@verbose_option
def add_country_codes(
    input_parquet,
    output_parquet,
    countries_file,
    add_bbox,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    dry_run,
    verbose,
):
    """Add country ISO codes to a GeoParquet file based on spatial intersection.

    If --countries-file is not provided, will use the default countries file from
    https://data.source.coop/cholmes/admin-boundaries/countries.parquet and filter
    to only the subset that overlaps with the input data (may take longer).

    Output is written as GeoParquet 1.1 with proper bbox covering metadata.
    """
    # Validate mutually exclusive options
    if row_group_size and row_group_size_mb:
        raise click.UsageError("--row-group-size and --row-group-size-mb are mutually exclusive")

    # Parse size string if provided
    from geoparquet_io.core.common import parse_size_string

    row_group_mb = None
    if row_group_size_mb:
        try:
            size_bytes = parse_size_string(row_group_size_mb)
            row_group_mb = size_bytes / (1024 * 1024)
        except ValueError as e:
            raise click.UsageError(f"Invalid row group size: {e}") from e

    add_country_codes_impl(
        input_parquet,
        countries_file,
        output_parquet,
        add_bbox,
        dry_run,
        verbose,
        compression.upper(),
        compression_level,
        row_group_mb,
        row_group_size,
    )


@add.command(name="bbox")
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option("--bbox-name", default="bbox", help="Name for the bbox column (default: bbox)")
@output_format_options
@dry_run_option
@verbose_option
def add_bbox(
    input_parquet,
    output_parquet,
    bbox_name,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    dry_run,
    verbose,
):
    """Add a bbox struct column to a GeoParquet file.

    Creates a new column with bounding box coordinates (xmin, ymin, xmax, ymax)
    for each geometry feature. The bbox column improves spatial query performance
    and adds proper bbox covering metadata to the GeoParquet file (GeoParquet 1.1).
    """
    # Validate mutually exclusive options
    if row_group_size and row_group_size_mb:
        raise click.UsageError("--row-group-size and --row-group-size-mb are mutually exclusive")

    # Parse size string if provided
    from geoparquet_io.core.common import parse_size_string

    row_group_mb = None
    if row_group_size_mb:
        try:
            size_bytes = parse_size_string(row_group_size_mb)
            row_group_mb = size_bytes / (1024 * 1024)
        except ValueError as e:
            raise click.UsageError(f"Invalid row group size: {e}") from e

    add_bbox_column_impl(
        input_parquet,
        output_parquet,
        bbox_name,
        dry_run,
        verbose,
        compression.upper(),
        compression_level,
        row_group_mb,
        row_group_size,
    )


@add.command(name="h3")
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option("--h3-name", default="h3_cell", help="Name for the H3 column (default: h3_cell)")
@click.option(
    "--resolution",
    default=9,
    type=click.IntRange(0, 15),
    help="H3 resolution level (0-15). Res 7: ~5km², Res 9: ~105m², Res 11: ~2m², Res 13: ~0.04m². Default: 9",
)
@output_format_options
@dry_run_option
@verbose_option
def add_h3(
    input_parquet,
    output_parquet,
    h3_name,
    resolution,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    dry_run,
    verbose,
):
    """Add an H3 cell ID column to a GeoParquet file.

    Computes H3 hexagonal cell IDs based on geometry centroids. H3 is a hierarchical
    hexagonal geospatial indexing system that provides consistent cell sizes and shapes
    across the globe.

    The cell ID is stored as a VARCHAR (string) for maximum portability across tools.
    Resolution determines cell size - higher values mean smaller cells with more precision.
    """
    # Validate mutually exclusive options
    if row_group_size and row_group_size_mb:
        raise click.UsageError("--row-group-size and --row-group-size-mb are mutually exclusive")

    # Parse size string if provided
    from geoparquet_io.core.common import parse_size_string

    row_group_mb = None
    if row_group_size_mb:
        try:
            size_bytes = parse_size_string(row_group_size_mb)
            row_group_mb = size_bytes / (1024 * 1024)
        except ValueError as e:
            raise click.UsageError(f"Invalid row group size: {e}") from e

    add_h3_column_impl(
        input_parquet,
        output_parquet,
        h3_name,
        resolution,
        dry_run,
        verbose,
        compression.upper(),
        compression_level,
        row_group_mb,
        row_group_size,
    )


@add.command(name="kdtree")
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option(
    "--kdtree-name",
    default="kdtree_cell",
    help="Name for the KD-tree column (default: kdtree_cell)",
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
    help="Auto-select partitions targeting N rows/partition. Default when neither --partitions nor --auto specified: 120,000.",
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
@output_format_options
@dry_run_option
@click.option(
    "--force",
    is_flag=True,
    help="Force operation on large datasets without confirmation",
)
@verbose_option
def add_kdtree(
    input_parquet,
    output_parquet,
    kdtree_name,
    partitions,
    auto,
    approx,
    exact,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    dry_run,
    force,
    verbose,
):
    """Add a KD-tree cell ID column to a GeoParquet file.

    Creates balanced spatial partitions using recursive splits alternating between
    X and Y dimensions at medians. Partition count must be a power of 2.

    By default, auto-selects partitions targeting ~120k rows each using approximate mode
    (O(n) with 100k sample). Use --partitions N for explicit control or --exact for
    deterministic computation.

    Performance Note: Approximate mode is O(n), exact mode is O(n × log2(partitions)).

    Use --verbose to track progress with iteration-by-iteration updates.
    """
    import math

    # Validate mutually exclusive options
    if sum([partitions is not None, auto is not None]) > 1:
        raise click.UsageError("--partitions and --auto are mutually exclusive")

    # Set defaults
    if partitions is None and auto is None:
        auto = 120000  # Default: auto-select targeting 120k rows/partition
        partitions = None
    elif auto is not None:
        # Auto mode: will compute partitions below
        partitions = None

    # Validate partitions if specified
    if partitions is not None and (partitions < 2 or (partitions & (partitions - 1)) != 0):
        raise click.UsageError(f"Partitions must be a power of 2 (2, 4, 8, ...), got {partitions}")

    # Validate mutually exclusive options for approx/exact
    if exact and approx != 100000:
        raise click.UsageError("--approx and --exact are mutually exclusive")

    # Determine sample size
    sample_size = None if exact else approx

    # If auto mode, compute optimal partitions
    if auto is not None:
        # Pass None for iterations, let implementation compute
        iterations = None
        target_rows = auto if auto > 0 else 120000
        auto_target = ("rows", target_rows)
    else:
        # Convert partitions to iterations
        iterations = int(math.log2(partitions))
        auto_target = None

    # Validate mutually exclusive options
    if row_group_size and row_group_size_mb:
        raise click.UsageError("--row-group-size and --row-group-size-mb are mutually exclusive")

    # Parse size string if provided
    from geoparquet_io.core.common import parse_size_string

    row_group_mb = None
    if row_group_size_mb:
        try:
            size_bytes = parse_size_string(row_group_size_mb)
            row_group_mb = size_bytes / (1024 * 1024)
        except ValueError as e:
            raise click.UsageError(f"Invalid row group size: {e}") from e

    add_kdtree_column_impl(
        input_parquet,
        output_parquet,
        kdtree_name,
        iterations,
        dry_run,
        verbose,
        compression.upper(),
        compression_level,
        row_group_mb,
        row_group_size,
        force,
        sample_size,
        auto_target,
    )


# Partition commands group
@cli.group()
def partition():
    """Commands for partitioning GeoParquet files."""
    pass


@partition.command(name="admin")
@click.argument("input_parquet")
@click.argument("output_folder", required=False)
@click.option(
    "--column",
    default="admin:country_code",
    help="Column name to partition by (default: admin:country_code)",
)
@partition_options
@verbose_option
def partition_admin(
    input_parquet,
    output_folder,
    column,
    hive,
    overwrite,
    preview,
    preview_limit,
    force,
    skip_analysis,
    verbose,
):
    """Split a GeoParquet file into separate files by country code.

    By default, partitions by the 'admin:country_code' column, but you can specify
    a different column using the --column option.

    Use --preview to see what partitions would be created without actually creating files.
    """
    # If preview mode, output_folder is not required
    if not preview and not output_folder:
        raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

    split_country_impl(
        input_parquet,
        output_folder,
        column,
        hive,
        verbose,
        overwrite,
        preview,
        preview_limit,
        force,
        skip_analysis,
    )


@partition.command(name="string")
@click.argument("input_parquet")
@click.argument("output_folder", required=False)
@click.option("--column", required=True, help="Column name to partition by (required)")
@click.option("--chars", type=int, help="Number of characters to use as prefix for partitioning")
@partition_options
@verbose_option
def partition_string(
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
    verbose,
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
    # If preview mode, output_folder is not required
    if not preview and not output_folder:
        raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

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
    )


@partition.command(name="h3")
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
    default=9,
    help="H3 resolution for partitioning (0-15, default: 9)",
)
@click.option(
    "--keep-h3-column",
    is_flag=True,
    help="Keep the H3 column in output files (default: excluded for non-Hive, included for Hive)",
)
@partition_options
@verbose_option
def partition_h3(
    input_parquet,
    output_folder,
    h3_name,
    resolution,
    keep_h3_column,
    hive,
    overwrite,
    preview,
    preview_limit,
    force,
    skip_analysis,
    verbose,
):
    """Partition a GeoParquet file by H3 cells at specified resolution.

    Creates separate GeoParquet files based on H3 cell prefixes at the specified resolution.
    If the H3 column doesn't exist, it will be automatically added before partitioning.

    By default, the H3 column is excluded from output files (since it's redundant with the
    partition path) unless using Hive-style partitioning. Use --keep-h3-column to explicitly
    keep the column in all cases.

    Use --preview to see what partitions would be created without actually creating files.

    Examples:

        # Preview partitions at resolution 7 (~5km² cells)
        gpio partition h3 input.parquet --resolution 7 --preview

        # Partition by H3 cells at default resolution 9 (H3 column excluded from output)
        gpio partition h3 input.parquet output/

        # Partition with H3 column kept in output files
        gpio partition h3 input.parquet output/ --keep-h3-column

        # Partition with custom H3 column name
        gpio partition h3 input.parquet output/ --h3-name my_h3

        # Use Hive-style partitioning at resolution 8 (H3 column included by default)
        gpio partition h3 input.parquet output/ --resolution 8 --hive
    """
    # If preview mode, output_folder is not required
    if not preview and not output_folder:
        raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

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
    )


@partition.command(name="kdtree")
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
@partition_options
@verbose_option
def partition_kdtree(
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
    verbose,
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
    )


if __name__ == "__main__":
    cli()
