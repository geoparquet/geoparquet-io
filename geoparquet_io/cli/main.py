import click

from geoparquet_io.core.add_bbox_column import add_bbox_column as add_bbox_column_impl
from geoparquet_io.core.add_bbox_metadata import add_bbox_metadata as add_bbox_metadata_impl
from geoparquet_io.core.add_country_codes import add_country_codes as add_country_codes_impl
from geoparquet_io.core.add_h3_column import add_h3_column as add_h3_column_impl
from geoparquet_io.core.add_kdtree_column import add_kdtree_column as add_kdtree_column_impl
from geoparquet_io.core.check_parquet_structure import check_all as check_structure_impl
from geoparquet_io.core.check_spatial_order import check_spatial_order as check_spatial_impl
from geoparquet_io.core.hilbert_order import hilbert_order as hilbert_impl
from geoparquet_io.core.partition_by_h3 import partition_by_h3 as partition_by_h3_impl
from geoparquet_io.core.partition_by_kdtree import partition_by_kdtree as partition_by_kdtree_impl
from geoparquet_io.core.partition_by_string import (
    partition_by_string as partition_by_string_impl,
)
from geoparquet_io.core.split_by_country import split_by_country as split_country_impl

# Version info
__version__ = "0.1.0"


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
@click.option(
    "--compression",
    default="ZSTD",
    type=click.Choice(
        ["ZSTD", "GZIP", "BROTLI", "LZ4", "SNAPPY", "UNCOMPRESSED"], case_sensitive=False
    ),
    help="Compression type for output file (default: ZSTD)",
)
@click.option(
    "--compression-level",
    type=click.IntRange(1, 22),
    help="Compression level - GZIP: 1-9 (default: 6), ZSTD: 1-22 (default: 15), BROTLI: 1-11 (default: 6). Ignored for LZ4/SNAPPY.",
)
@click.option("--row-group-size", type=int, help="Exact number of rows per row group")
@click.option(
    "--row-group-size-mb", help="Target row group size (e.g. '256MB', '1GB', '128' assumes MB)"
)
@click.option("--verbose", "-v", is_flag=True, help="Print verbose output")
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
@click.option(
    "--compression",
    default="ZSTD",
    type=click.Choice(
        ["ZSTD", "GZIP", "BROTLI", "LZ4", "SNAPPY", "UNCOMPRESSED"], case_sensitive=False
    ),
    help="Compression type for output file (default: ZSTD)",
)
@click.option(
    "--compression-level",
    type=click.IntRange(1, 22),
    help="Compression level - GZIP: 1-9 (default: 6), ZSTD: 1-22 (default: 15), BROTLI: 1-11 (default: 6). Ignored for LZ4/SNAPPY.",
)
@click.option("--row-group-size", type=int, help="Exact number of rows per row group")
@click.option(
    "--row-group-size-mb", help="Target row group size (e.g. '256MB', '1GB', '128' assumes MB)"
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print SQL commands that would be executed without actually running them.",
)
@click.option("--verbose", is_flag=True, help="Print additional information.")
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
@click.option(
    "--compression",
    default="ZSTD",
    type=click.Choice(
        ["ZSTD", "GZIP", "BROTLI", "LZ4", "SNAPPY", "UNCOMPRESSED"], case_sensitive=False
    ),
    help="Compression type for output file (default: ZSTD)",
)
@click.option(
    "--compression-level",
    type=click.IntRange(1, 22),
    help="Compression level - GZIP: 1-9 (default: 6), ZSTD: 1-22 (default: 15), BROTLI: 1-11 (default: 6). Ignored for LZ4/SNAPPY.",
)
@click.option("--row-group-size", type=int, help="Exact number of rows per row group")
@click.option(
    "--row-group-size-mb", help="Target row group size (e.g. '256MB', '1GB', '128' assumes MB)"
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print SQL commands that would be executed without actually running them.",
)
@click.option("--verbose", is_flag=True, help="Print additional information.")
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
@click.option(
    "--compression",
    default="ZSTD",
    type=click.Choice(
        ["ZSTD", "GZIP", "BROTLI", "LZ4", "SNAPPY", "UNCOMPRESSED"], case_sensitive=False
    ),
    help="Compression type for output file (default: ZSTD)",
)
@click.option(
    "--compression-level",
    type=click.IntRange(1, 22),
    help="Compression level - GZIP: 1-9 (default: 6), ZSTD: 1-22 (default: 15), BROTLI: 1-11 (default: 6). Ignored for LZ4/SNAPPY.",
)
@click.option("--row-group-size", type=int, help="Exact number of rows per row group")
@click.option(
    "--row-group-size-mb", help="Target row group size (e.g. '256MB', '1GB', '128' assumes MB)"
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print SQL commands that would be executed without actually running them.",
)
@click.option("--verbose", is_flag=True, help="Print additional information.")
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
    "--iterations",
    default=9,
    type=click.IntRange(1, 20),
    help="Number of recursive splits (1-20). iterations=5: 32 partitions, iterations=9: 512 partitions. Default: 9",
)
@click.option(
    "--compression",
    default="ZSTD",
    type=click.Choice(
        ["ZSTD", "GZIP", "BROTLI", "LZ4", "SNAPPY", "UNCOMPRESSED"], case_sensitive=False
    ),
    help="Compression type for output file (default: ZSTD)",
)
@click.option(
    "--compression-level",
    type=click.IntRange(1, 22),
    help="Compression level - GZIP: 1-9 (default: 6), ZSTD: 1-22 (default: 15), BROTLI: 1-11 (default: 6). Ignored for LZ4/SNAPPY.",
)
@click.option("--row-group-size", type=int, help="Exact number of rows per row group")
@click.option(
    "--row-group-size-mb", help="Target row group size (e.g. '256MB', '1GB', '128' assumes MB)"
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print SQL commands that would be executed without actually running them.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force operation on large datasets (>50M rows) - not recommended, use hierarchical partitioning instead",
)
@click.option("--verbose", is_flag=True, help="Print additional information.")
def add_kdtree(
    input_parquet,
    output_parquet,
    kdtree_name,
    iterations,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    dry_run,
    force,
    verbose,
):
    """Add a KD-tree cell ID column to a GeoParquet file.

    Computes KD-tree partition IDs based on recursive spatial splits alternating
    between X and Y dimensions at medians. This creates balanced spatial partitions
    that adapt to data distribution.

    The partition ID is stored as a binary string (e.g., "01011001") where each bit
    represents a split decision (0=below median, 1=above/equal to median).

    Iterations determine the number of partitions: 2^iterations total partitions.

    Performance Note: Runtime scales with dataset size × iterations. For datasets
    > 50M rows, consider hierarchical partitioning (country/region + KD-tree).
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
@click.option(
    "--hive", is_flag=True, help="Use Hive-style partitioning in output folder structure."
)
@click.option("--verbose", is_flag=True, help="Print additional information.")
@click.option("--overwrite", is_flag=True, help="Overwrite existing country files.")
@click.option("--preview", is_flag=True, help="Preview partitions without creating files.")
@click.option(
    "--preview-limit",
    default=15,
    type=int,
    help="Number of partitions to show in preview (default: 15)",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force partitioning even if analysis detects potential issues",
)
@click.option(
    "--skip-analysis",
    is_flag=True,
    help="Skip partition strategy analysis (for performance-sensitive cases)",
)
def partition_admin(
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
@click.option("--hive", is_flag=True, help="Use Hive-style partitioning in output folder structure")
@click.option("--overwrite", is_flag=True, help="Overwrite existing partition files")
@click.option(
    "--preview",
    is_flag=True,
    help="Analyze and preview partitions without creating files (dry-run)",
)
@click.option(
    "--preview-limit",
    default=15,
    type=int,
    help="Number of partitions to show in preview (default: 15)",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force partitioning even if analysis detects potential issues",
)
@click.option(
    "--skip-analysis",
    is_flag=True,
    help="Skip partition strategy analysis (for performance-sensitive cases)",
)
@click.option("--verbose", is_flag=True, help="Print additional information")
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
@click.option("--hive", is_flag=True, help="Use Hive-style partitioning in output folder structure")
@click.option("--overwrite", is_flag=True, help="Overwrite existing partition files")
@click.option(
    "--preview",
    is_flag=True,
    help="Analyze and preview partitions without creating files (dry-run)",
)
@click.option(
    "--preview-limit",
    default=15,
    type=int,
    help="Number of partitions to show in preview (default: 15)",
)
@click.option(
    "--keep-h3-column",
    is_flag=True,
    help="Keep the H3 column in output files (default: excluded for non-Hive, included for Hive)",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force partitioning even if analysis detects potential issues",
)
@click.option(
    "--skip-analysis",
    is_flag=True,
    help="Skip partition strategy analysis (for performance-sensitive cases)",
)
@click.option("--verbose", is_flag=True, help="Print additional information")
def partition_h3(
    input_parquet,
    output_folder,
    h3_name,
    resolution,
    hive,
    overwrite,
    preview,
    preview_limit,
    keep_h3_column,
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
    "--iterations",
    type=click.IntRange(1, 20),
    default=9,
    help="Number of recursive splits for partitioning (1-20, default: 9)",
)
@click.option("--hive", is_flag=True, help="Use Hive-style partitioning in output folder structure")
@click.option("--overwrite", is_flag=True, help="Overwrite existing partition files")
@click.option(
    "--preview",
    is_flag=True,
    help="Analyze and preview partitions without creating files (dry-run)",
)
@click.option(
    "--preview-limit",
    default=15,
    type=int,
    help="Number of partitions to show in preview (default: 15)",
)
@click.option(
    "--keep-kdtree-column",
    is_flag=True,
    help="Keep the KD-tree column in output files (default: excluded for non-Hive, included for Hive)",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force partitioning even if analysis detects potential issues",
)
@click.option(
    "--skip-analysis",
    is_flag=True,
    help="Skip partition strategy analysis (for performance-sensitive cases)",
)
@click.option("--verbose", is_flag=True, help="Print additional information")
def partition_kdtree(
    input_parquet,
    output_folder,
    kdtree_name,
    iterations,
    hive,
    overwrite,
    preview,
    preview_limit,
    keep_kdtree_column,
    force,
    skip_analysis,
    verbose,
):
    """Partition a GeoParquet file by KD-tree cells at specified iteration depth.

    Creates separate GeoParquet files based on KD-tree partition IDs at the specified
    iteration depth. If the KD-tree column doesn't exist, it will be automatically added
    before partitioning.

    KD-tree partitioning recursively splits data on alternating X/Y dimensions at medians,
    creating balanced spatial partitions that adapt to data distribution.

    By default, the KD-tree column is excluded from output files (since it's redundant with
    the partition path) unless using Hive-style partitioning. Use --keep-kdtree-column to
    explicitly keep the column in all cases.

    Use --preview to see what partitions would be created without actually creating files.

    Performance Note: Runtime scales with dataset size × iterations. For datasets > 50M rows,
    consider hierarchical partitioning (country/region first, then KD-tree within each).

    Examples:

        # Preview partitions with 5 iterations (~32 partitions)
        gpio partition kdtree input.parquet --iterations 5 --preview

        # Partition by KD-tree at default 9 iterations (KD-tree column excluded from output)
        gpio partition kdtree input.parquet output/

        # Partition with KD-tree column kept in output files
        gpio partition kdtree input.parquet output/ --keep-kdtree-column

        # Partition with custom KD-tree column name
        gpio partition kdtree input.parquet output/ --kdtree-name my_kdtree

        # Use Hive-style partitioning at 12 iterations (KD-tree column included by default)
        gpio partition kdtree input.parquet output/ --iterations 12 --hive
    """
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
    )


if __name__ == "__main__":
    cli()
