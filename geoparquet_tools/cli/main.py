import click
from geoparquet_tools.core.check_parquet_structure import check_all as check_structure_impl
from geoparquet_tools.core.check_spatial_order import check_spatial_order as check_spatial_impl
from geoparquet_tools.core.hilbert_order import hilbert_order as hilbert_impl
from geoparquet_tools.core.add_country_codes import add_country_codes as add_country_codes_impl
from geoparquet_tools.core.split_by_country import split_by_country as split_country_impl
from geoparquet_tools.core.add_bbox_metadata import add_bbox_metadata as add_bbox_metadata_impl
from geoparquet_tools.core.add_bbox_column import add_bbox_column as add_bbox_column_impl
from geoparquet_tools.core.partition_by_string import partition_by_string as partition_by_string_impl

@click.group()
def cli():
    """GeoParquet Tools CLI for working with GeoParquet files."""
    pass

# Check commands group
@cli.group()
def check():
    """Commands for checking GeoParquet files for best practices."""
    pass

@check.command(name='all')
@click.argument("parquet_file")
@click.option("--verbose", is_flag=True, help="Print full metadata and details")
@click.option("--random-sample-size", default=100, show_default=True,
              help="Number of rows in each sample for spatial order check.")
@click.option("--limit-rows", default=500000, show_default=True,
              help="Max number of rows to read for spatial order check.")
def check_all(parquet_file, verbose, random_sample_size, limit_rows):
    """Run all checks on a GeoParquet file."""
    check_structure_impl(parquet_file, verbose)
    click.echo("\nSpatial Order Analysis:")
    ratio = check_spatial_impl(parquet_file, random_sample_size, limit_rows, verbose)
    if ratio is not None:
        if ratio < 0.5:
            click.echo(click.style("✓ Data appears to be spatially ordered", fg="green"))
        else:
            click.echo(click.style(
                "⚠️  Data may not be optimally spatially ordered\n"
                "Consider running 'gt sort hilbert' to improve spatial locality",
                fg="yellow"
            ))

@check.command(name='spatial')
@click.argument("parquet_file")
@click.option("--random-sample-size", default=100, show_default=True,
              help="Number of rows in each sample for spatial order check.")
@click.option("--limit-rows", default=500000, show_default=True,
              help="Max number of rows to read for spatial order check.")
@click.option("--verbose", is_flag=True, help="Print additional information.")
def check_spatial(parquet_file, random_sample_size, limit_rows, verbose):
    """Check if a GeoParquet file is spatially ordered."""
    ratio = check_spatial_impl(parquet_file, random_sample_size, limit_rows, verbose)
    if ratio is not None:
        if ratio < 0.5:
            click.echo(click.style("✓ Data appears to be spatially ordered", fg="green"))
        else:
            click.echo(click.style(
                "⚠️  Data may not be optimally spatially ordered\n"
                "Consider running 'gt sort hilbert' to improve spatial locality",
                fg="yellow"
            ))

@check.command(name='compression')
@click.argument("parquet_file")
@click.option("--verbose", is_flag=True, help="Print additional information.")
def check_compression_cmd(parquet_file, verbose):
    """Check compression settings for geometry column."""
    from geoparquet_tools.core.check_parquet_structure import check_compression
    check_compression(parquet_file, verbose)

@check.command(name='bbox')
@click.argument("parquet_file")
@click.option("--verbose", is_flag=True, help="Print additional information.")
def check_bbox_cmd(parquet_file, verbose):
    """Check GeoParquet metadata version and bbox structure."""
    from geoparquet_tools.core.check_parquet_structure import check_metadata_and_bbox
    check_metadata_and_bbox(parquet_file, verbose)

@check.command(name='row-group')
@click.argument("parquet_file")
@click.option("--verbose", is_flag=True, help="Print additional information.")
def check_row_group_cmd(parquet_file, verbose):
    """Check row group optimization."""
    from geoparquet_tools.core.check_parquet_structure import check_row_groups
    check_row_groups(parquet_file, verbose)

# Format commands group
@cli.group()
def format():
    """Commands for formatting GeoParquet files."""
    pass

@format.command(name='bbox-metadata')
@click.argument('parquet_file')
@click.option('--verbose', is_flag=True, help='Print detailed information')
def format_bbox_metadata(parquet_file, verbose):
    """Add bbox covering metadata to a GeoParquet file."""
    add_bbox_metadata_impl(parquet_file, verbose)

# Sort commands group
@cli.group()
def sort():
    """Commands for sorting GeoParquet files."""
    pass

@sort.command(name='hilbert')
@click.argument('input_parquet', type=click.Path(exists=True))
@click.argument('output_parquet', type=click.Path())
@click.option('--geometry-column', '-g', default='geometry',
              help='Name of the geometry column (default: geometry)')
@click.option('--add-bbox', is_flag=True, help='Automatically add bbox column and metadata if missing.')
@click.option('--verbose', '-v', is_flag=True,
              help='Print verbose output')
def hilbert_order(input_parquet, output_parquet, geometry_column, add_bbox, verbose):
    """
    Reorder a GeoParquet file using Hilbert curve ordering.

    Takes an input GeoParquet file and creates a new file with rows ordered
    by their position along a Hilbert space-filling curve.

    By default, applies optimal formatting (ZSTD compression, optimized row groups, bbox metadata)
    while preserving the CRS.
    """
    try:
        hilbert_impl(input_parquet, output_parquet, geometry_column, add_bbox, verbose)
    except Exception as e:
        raise click.ClickException(str(e))

# Add commands group
@cli.group()
def add():
    """Commands for enhancing GeoParquet files in various ways."""
    pass

@add.command(name='admin-divisions')
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option("--countries-file", default=None, help="Path or URL to countries parquet file. If not provided, uses default from source.coop")
@click.option("--add-bbox", is_flag=True, help="Automatically add bbox column and metadata if missing.")
@click.option("--verbose", is_flag=True, help="Print additional information.")
def add_country_codes(input_parquet, output_parquet, countries_file, add_bbox, verbose):
    """Add country ISO codes to a GeoParquet file based on spatial intersection.

    If --countries-file is not provided, will use the default countries file from
    https://data.source.coop/cholmes/admin-boundaries/countries.parquet and filter
    to only the subset that overlaps with the input data (may take longer).
    """
    add_country_codes_impl(input_parquet, countries_file, output_parquet, add_bbox, verbose)

@add.command(name='bbox')
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option("--bbox-name", default="bbox", help="Name for the bbox column (default: bbox)")
@click.option("--verbose", is_flag=True, help="Print additional information.")
def add_bbox(input_parquet, output_parquet, bbox_name, verbose):
    """Add a bbox struct column to a GeoParquet file.

    Creates a new column with bounding box coordinates (xmin, ymin, xmax, ymax)
    for each geometry feature. The bbox column improves spatial query performance
    and adds proper bbox covering metadata to the GeoParquet file.
    """
    add_bbox_column_impl(input_parquet, output_parquet, bbox_name, verbose)

# Partition commands group
@cli.group()
def partition():
    """Commands for partitioning GeoParquet files."""
    pass

@partition.command(name='admin')
@click.argument("input_parquet")
@click.argument("output_folder", required=False)
@click.option("--column", default="admin:country_code", help="Column name to partition by (default: admin:country_code)")
@click.option("--hive", is_flag=True, help="Use Hive-style partitioning in output folder structure.")
@click.option("--verbose", is_flag=True, help="Print additional information.")
@click.option("--overwrite", is_flag=True, help="Overwrite existing country files.")
@click.option("--preview", is_flag=True, help="Preview partitions without creating files.")
@click.option("--preview-limit", default=15, type=int, help="Number of partitions to show in preview (default: 15)")
def partition_admin(input_parquet, output_folder, column, hive, verbose, overwrite, preview, preview_limit):
    """Split a GeoParquet file into separate files by country code.

    By default, partitions by the 'admin:country_code' column, but you can specify
    a different column using the --column option.

    Use --preview to see what partitions would be created without actually creating files.
    """
    # If preview mode, output_folder is not required
    if not preview and not output_folder:
        raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

    split_country_impl(input_parquet, output_folder, column, hive, verbose, overwrite, preview, preview_limit)

@partition.command(name='string')
@click.argument("input_parquet")
@click.argument("output_folder", required=False)
@click.option("--column", required=True, help="Column name to partition by (required)")
@click.option("--chars", type=int, help="Number of characters to use as prefix for partitioning")
@click.option("--hive", is_flag=True, help="Use Hive-style partitioning in output folder structure")
@click.option("--overwrite", is_flag=True, help="Overwrite existing partition files")
@click.option("--preview", is_flag=True, help="Preview partitions without creating files")
@click.option("--preview-limit", default=15, type=int, help="Number of partitions to show in preview (default: 15)")
@click.option("--verbose", is_flag=True, help="Print additional information")
def partition_string(input_parquet, output_folder, column, chars, hive, overwrite, preview, preview_limit, verbose):
    """Partition a GeoParquet file by string column values.

    Creates separate GeoParquet files based on distinct values in the specified column.
    When --chars is provided, partitions by the first N characters of the column values.

    Use --preview to see what partitions would be created without actually creating files.

    Examples:

        # Preview partitions by first character of MGRS codes
        gt partition string input.parquet --column MGRS --chars 1 --preview

        # Partition by full column values
        gt partition string input.parquet output/ --column category

        # Partition by first character of MGRS codes
        gt partition string input.parquet output/ --column mgrs --chars 1

        # Use Hive-style partitioning
        gt partition string input.parquet output/ --column region --hive
    """
    # If preview mode, output_folder is not required
    if not preview and not output_folder:
        raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

    partition_by_string_impl(input_parquet, output_folder, column, chars, hive, overwrite, preview, preview_limit, verbose)

if __name__ == "__main__":
    cli() 