import click
from geoparquet_tools.core.check_parquet_structure import check_all as check_structure_impl
from geoparquet_tools.core.check_spatial_order import check_spatial_order as check_spatial_impl
from geoparquet_tools.core.hilbert_order import hilbert_order as hilbert_impl
from geoparquet_tools.core.add_country_codes import add_country_codes as add_country_codes_impl
from geoparquet_tools.core.split_by_country import split_by_country as split_country_impl
from geoparquet_tools.core.add_bbox_metadata import add_bbox_metadata as add_bbox_metadata_impl

@click.group()
def cli():
    """GeoParquet Tools CLI for working with GeoParquet files."""
    pass

# Check commands group
@cli.group()
def check():
    """Commands for checking GeoParquet files."""
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
@click.option('--verbose', '-v', is_flag=True,
              help='Print verbose output')
def hilbert_order(input_parquet, output_parquet, geometry_column, verbose):
    """
    Reorder a GeoParquet file using Hilbert curve ordering.
    
    Takes an input GeoParquet file and creates a new file with rows ordered
    by their position along a Hilbert space-filling curve.
    
    By default, applies optimal formatting (ZSTD compression, optimized row groups, bbox metadata)
    while preserving the CRS. Use --no-format to preserve all original formatting.
    """
    try:
        hilbert_impl(input_parquet, output_parquet, geometry_column, verbose)
    except Exception as e:
        raise click.ClickException(str(e))

# Add commands group
@cli.group()
def add():
    """Commands for adding data to GeoParquet files."""
    pass

@add.command(name='country-codes')
@click.argument("input_parquet")
@click.argument("countries_parquet")
@click.argument("output_parquet")
@click.option("--verbose", is_flag=True, help="Print additional information.")
def add_country_codes(input_parquet, countries_parquet, output_parquet, verbose):
    """Add country ISO codes to a GeoParquet file based on spatial intersection."""
    add_country_codes_impl(input_parquet, countries_parquet, output_parquet, verbose)

# Partition commands group
@cli.group()
def partition():
    """Commands for partitioning GeoParquet files."""
    pass

@partition.command(name='admin')
@click.argument("input_parquet")
@click.argument("output_folder")
@click.option("--hive", is_flag=True, help="Use Hive-style partitioning in output folder structure.")
@click.option("--verbose", is_flag=True, help="Print additional information.")
@click.option("--overwrite", is_flag=True, help="Overwrite existing country files.")
def partition_admin(input_parquet, output_folder, hive, verbose, overwrite):
    """Split a GeoParquet file into separate files by country code."""
    split_country_impl(input_parquet, output_folder, hive, verbose, overwrite)

if __name__ == "__main__":
    cli() 