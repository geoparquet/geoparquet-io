import click

from geoparquet_io.cli.decorators import (
    dry_run_option,
    output_format_options,
    partition_options,
    verbose_option,
)
from geoparquet_io.core.add_bbox_column import add_bbox_column as add_bbox_column_impl
from geoparquet_io.core.add_bbox_metadata import add_bbox_metadata as add_bbox_metadata_impl
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
from geoparquet_io.core.partition_admin_hierarchical import (
    partition_by_admin_hierarchical as partition_admin_hierarchical_impl,
)
from geoparquet_io.core.partition_by_h3 import partition_by_h3 as partition_by_h3_impl
from geoparquet_io.core.partition_by_kdtree import partition_by_kdtree as partition_by_kdtree_impl
from geoparquet_io.core.partition_by_string import (
    partition_by_string as partition_by_string_impl,
)

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
def inspect(parquet_file, head, tail, stats, json_output):
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

    # Validate mutually exclusive options
    if head and tail:
        raise click.UsageError("--head and --tail are mutually exclusive")

    try:
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
def cache():
    """Manage cache for remote admin boundary datasets."""
    pass


@cache.command(name="list")
@verbose_option
def cache_list(verbose):
    """List cached admin boundary datasets."""
    import time

    from geoparquet_io.core.admin_cache import list_cached_datasets

    cached = list_cached_datasets(verbose)

    if not cached:
        click.echo("No cached datasets found.")
        return

    click.echo(f"\nCached Admin Boundary Datasets ({len(cached)} total):\n")

    # Display table
    for item in cached:
        dataset_name = item.get("dataset_name", "unknown")
        size_mb = item["size_bytes"] / (1024 * 1024)
        cached_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(item["cached_at"]))

        click.echo(f"  {dataset_name}")
        click.echo(f"    Size: {size_mb:.1f} MB")
        click.echo(f"    Cached: {cached_at}")
        if verbose:
            click.echo(f"    Path: {item['file_path']}")
            if "url" in item:
                click.echo(f"    URL: {item['url']}")
        click.echo()


@cache.command(name="clear")
@click.option(
    "--dataset",
    help="Clear cache for specific dataset (gaul, overture). If not specified, clears all.",
)
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@verbose_option
def cache_clear(dataset, yes, verbose):
    """Clear cached admin boundary datasets."""
    from geoparquet_io.core.admin_cache import clear_cache, list_cached_datasets

    # Get list of what will be cleared
    cached = list_cached_datasets()

    if not cached:
        click.echo("No cached datasets to clear.")
        return

    if dataset:
        # Filter to specific dataset
        cached = [c for c in cached if dataset.lower() in c.get("dataset_name", "").lower()]
        if not cached:
            click.echo(f"No cached datasets found for: {dataset}")
            return

    # Calculate total size
    total_size_mb = sum(c["size_bytes"] for c in cached) / (1024 * 1024)

    # Confirm
    if not yes:
        if dataset:
            msg = f"Clear {len(cached)} cached file(s) for {dataset} ({total_size_mb:.1f} MB)?"
        else:
            msg = f"Clear all {len(cached)} cached file(s) ({total_size_mb:.1f} MB)?"

        if not click.confirm(msg):
            click.echo("Cancelled.")
            return

    # Clear cache
    removed = clear_cache(dataset, verbose)

    click.echo(f"✓ Removed {removed} file(s), freed {total_size_mb:.1f} MB")


@cache.command(name="info")
def cache_info():
    """Show cache directory information."""
    from geoparquet_io.core.admin_cache import get_cache_dir, list_cached_datasets

    cache_dir = get_cache_dir()

    click.echo(f"\nCache Directory: {cache_dir}")
    click.echo(f"Exists: {cache_dir.exists()}")

    cached = list_cached_datasets()
    if cached:
        total_size = sum(c["size_bytes"] for c in cached) / (1024 * 1024)
        click.echo(f"Total cached: {len(cached)} dataset(s), {total_size:.1f} MB")
    else:
        click.echo("Total cached: 0 datasets")

    click.echo("\nUse 'gpio cache list' to see cached datasets")
    click.echo("Use 'gpio cache clear' to remove cached datasets")


@cli.group()
def add():
    """Commands for enhancing GeoParquet files in various ways."""
    pass


@add.command(name="admin-divisions")
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option(
    "--dataset",
    type=click.Choice(["gaul", "overture"], case_sensitive=False),
    default="gaul",
    help="Admin boundaries dataset: 'gaul' (GAUL L2) or 'overture' (Overture Maps)",
)
@click.option(
    "--levels",
    help="Comma-separated hierarchical levels to add as columns (e.g., 'continent,country'). "
    "If not specified, adds all available levels for the dataset.",
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
    dataset,
    levels,
    add_bbox,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    dry_run,
    verbose,
):
    """Add admin division columns via spatial join with remote boundaries datasets.

    Performs spatial intersection to add administrative division columns to your data.

    \b
    **Datasets:**
    - gaul: GAUL L2 (levels: continent, country, department)
    - overture: Overture Maps (levels: country, region, locality)

    \b
    **Examples:**

    \b
    # Add all GAUL levels (continent, country, department)
    gpio add admin-divisions input.parquet output.parquet --dataset gaul

    \b
    # Add specific GAUL levels only
    gpio add admin-divisions input.parquet output.parquet --dataset gaul \\
        --levels continent,country

    \b
    # Preview SQL before execution
    gpio add admin-divisions input.parquet output.parquet --dataset gaul --dry-run

    \b
    **Note:** Requires internet connection to fetch remote boundaries datasets.
    Input data must have valid geometries in WGS84 or compatible CRS.
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

    # Use new multi-dataset implementation
    from geoparquet_io.core.add_admin_divisions_multi import add_admin_divisions_multi

    # Parse levels
    if levels:
        level_list = [level.strip() for level in levels.split(",")]
    else:
        # Use all available levels for the dataset
        from geoparquet_io.core.admin_datasets import AdminDatasetFactory

        temp_dataset = AdminDatasetFactory.create(dataset, None, verbose=False)
        level_list = temp_dataset.get_available_levels()

    add_admin_divisions_multi(
        input_parquet,
        output_parquet,
        dataset_name=dataset,
        levels=level_list,
        dataset_source=None,  # No custom sources for now
        add_bbox_flag=add_bbox,
        dry_run=dry_run,
        verbose=verbose,
        compression=compression.upper(),
        compression_level=compression_level,
        row_group_size_mb=row_group_mb,
        row_group_rows=row_group_size,
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
    "--dataset",
    type=click.Choice(["gaul", "overture"], case_sensitive=False),
    default="gaul",
    help="Admin boundaries dataset: 'gaul' (GAUL L2) or 'overture' (Overture Maps)",
)
@click.option(
    "--levels",
    required=True,
    help="Comma-separated hierarchical levels to partition by. "
    "GAUL levels: continent,country,department. "
    "Overture levels: country,region,locality.",
)
@partition_options
@verbose_option
def partition_admin(
    input_parquet,
    output_folder,
    dataset,
    levels,
    hive,
    overwrite,
    preview,
    preview_limit,
    force,
    skip_analysis,
    verbose,
):
    """Partition by administrative boundaries via spatial join with remote datasets.

    This command performs a two-step operation:
    1. Spatially joins input data with remote admin boundaries (GAUL or Overture)
    2. Partitions the enriched data by specified admin levels

    \b
    **Datasets:**
    - gaul: GAUL L2 Admin Boundaries (levels: continent, country, department)
    - overture: Overture Maps Divisions (levels: country, region, locality)

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
    **Note:** This command fetches remote boundaries and performs spatial intersection.
    Requires internet connection. Input data must have valid geometries in WGS84 or
    compatible CRS.
    """
    # If preview mode, output_folder is not required
    if not preview and not output_folder:
        raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

    # Parse levels
    level_list = [level.strip() for level in levels.split(",")]

    # Use hierarchical partitioning (spatial join + partition)
    partition_admin_hierarchical_impl(
        input_parquet,
        output_folder,
        dataset_name=dataset,
        levels=level_list,
        hive=hive,
        overwrite=overwrite,
        preview=preview,
        preview_limit=preview_limit,
        verbose=verbose,
        force=force,
        skip_analysis=skip_analysis,
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
