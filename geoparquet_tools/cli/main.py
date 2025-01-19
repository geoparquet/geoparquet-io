import click
from geoparquet_tools.core.check_parquet_structure import check_parquet_structure as check_structure_impl
from geoparquet_tools.core.check_spatial_order import check_spatial_order as check_spatial_impl
from geoparquet_tools.core.hilbert_order import hilbert_order as hilbert_impl
from geoparquet_tools.core.add_country_codes import add_country_codes as add_country_codes_impl
from geoparquet_tools.core.split_by_country import split_by_country as split_country_impl

@click.group()
def cli():
    """GeoParquet Tools CLI for working with GeoParquet files."""
    pass

# Check commands group
@cli.group()
def check():
    """Commands for checking GeoParquet files."""
    pass

@check.command(name='structure')
@click.argument("parquet_file")
@click.option("--verbose", is_flag=True, help="Print full GeoParquet metadata")
def check_structure(parquet_file, verbose):
    """Analyze key GeoParquet file characteristics and provide recommendations."""
    check_structure_impl(parquet_file, verbose)

@check.command(name='spatial-order')
@click.argument("parquet_file")
@click.option("--random-sample-size", default=100, show_default=True,
              help="Number of rows in each sample for random-pairs cross-join.")
@click.option("--limit-rows", default=500000, show_default=True,
              help="Max number of rows to read from the file (avoid huge memory usage).")
@click.option("--verbose", is_flag=True, help="Print detailed metadata information.")
def check_spatial_order(parquet_file, random_sample_size, limit_rows, verbose):
    """Check approximate spatial ordering of a GeoParquet file."""
    check_spatial_impl(parquet_file, random_sample_size, limit_rows, verbose)

# Operations commands group
@cli.group()
def ops():
    """Operations for manipulating GeoParquet files."""
    pass

@ops.command(name='hilbert-sort')
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option("--geometry-column", default="geometry", show_default=True,
              help="Name of the geometry column to use for ordering.")
@click.option("--verbose", is_flag=True, help="Print additional information.")
def hilbert_sort(input_parquet, output_parquet, geometry_column, verbose):
    """Sort a GeoParquet file using Hilbert curve ordering."""
    hilbert_impl(input_parquet, output_parquet, geometry_column, verbose)

@ops.command(name='add-country-codes')
@click.argument("input_parquet")
@click.argument("countries_parquet")
@click.argument("output_parquet")
@click.option("--verbose", is_flag=True, help="Print additional information.")
def add_country_codes(input_parquet, countries_parquet, output_parquet, verbose):
    """Add country ISO codes to a GeoParquet file based on spatial intersection."""
    add_country_codes_impl(input_parquet, countries_parquet, output_parquet, verbose)

@ops.command(name='split-by-country')
@click.argument("input_parquet")
@click.argument("output_folder")
@click.option("--hive", is_flag=True, help="Use Hive-style partitioning in output folder structure.")
@click.option("--verbose", is_flag=True, help="Print additional information.")
@click.option("--overwrite", is_flag=True, help="Overwrite existing country files.")
def split_by_country(input_parquet, output_folder, hive, verbose, overwrite):
    """Split a GeoParquet file into separate files by country code."""
    split_country_impl(input_parquet, output_folder, hive, verbose, overwrite)

if __name__ == "__main__":
    cli() 