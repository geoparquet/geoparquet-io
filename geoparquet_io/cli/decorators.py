"""
Shared Click decorators for common CLI parameters.

This module provides reusable decorators to ensure consistency across commands
and reduce code duplication.
"""

import click


def compression_options(func):
    """
    Add compression-related options to a command.

    Adds:
    - --compression: Type of compression (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
    - --compression-level: Compression level for formats that support it
    """
    func = click.option(
        "--compression",
        default="ZSTD",
        type=click.Choice(
            ["ZSTD", "GZIP", "BROTLI", "LZ4", "SNAPPY", "UNCOMPRESSED"], case_sensitive=False
        ),
        help="Compression type for output file (default: ZSTD)",
    )(func)
    func = click.option(
        "--compression-level",
        type=click.IntRange(1, 22),
        help="Compression level - GZIP: 1-9 (default: 6), ZSTD: 1-22 (default: 15), BROTLI: 1-11 (default: 6). Ignored for LZ4/SNAPPY.",
    )(func)
    return func


def row_group_options(func):
    """
    Add row group sizing options to a command.

    Adds:
    - --row-group-size: Exact number of rows per row group
    - --row-group-size-mb: Target row group size in MB or with units (e.g., '256MB', '1GB')
    """
    func = click.option("--row-group-size", type=int, help="Exact number of rows per row group")(
        func
    )
    func = click.option(
        "--row-group-size-mb", help="Target row group size (e.g. '256MB', '1GB', '128' assumes MB)"
    )(func)
    return func


def output_format_options(func):
    """
    Add all output format options (compression + row groups).

    This is a convenience decorator that combines compression_options and row_group_options.
    """
    func = compression_options(func)
    func = row_group_options(func)
    return func


def dry_run_option(func):
    """
    Add --dry-run option to a command.

    Allows users to preview what would be done without actually executing.
    """
    return click.option(
        "--dry-run",
        is_flag=True,
        help="Print SQL commands that would be executed without actually running them.",
    )(func)


def verbose_option(func):
    """
    Add --verbose/-v option to a command.

    Enables detailed logging and information output.
    """
    return click.option("--verbose", "-v", is_flag=True, help="Print verbose output")(func)


def overwrite_option(func):
    """
    Add --overwrite option to a command.

    Allows overwriting existing files without prompting.
    """
    return click.option("--overwrite", is_flag=True, help="Overwrite existing files")(func)


def profile_option(func):
    """
    Add --profile option to a command.

    Allows specifying AWS profile name for S3 operations. This is a convenience
    wrapper that sets the AWS_PROFILE environment variable.
    """
    return click.option(
        "--profile",
        help="AWS profile name for S3 operations (sets AWS_PROFILE env var)",
    )(func)


def bbox_option(func):
    """
    Add --add-bbox option to a command.

    Automatically adds bbox column and metadata if missing.
    """
    return click.option(
        "--add-bbox", is_flag=True, help="Automatically add bbox column and metadata if missing."
    )(func)


def prefix_option(func):
    """
    Add --prefix option to a partitioning command.

    Allows users to add a custom prefix to partition filenames.
    Example: --prefix fields → fields_USA.parquet
    """
    return click.option(
        "--prefix",
        help="Custom prefix for partition filenames (e.g., 'fields' → fields_USA.parquet)",
    )(func)


def geoparquet_version_option(func):
    """
    Add --geoparquet-version option to a command.

    Allows specifying the GeoParquet version for output files:
    - 1: GeoParquet 1.0 (geo key only, no Parquet geo type)
    - 1.1: GeoParquet 1.1 (default, geo key only)
    - 2.0: GeoParquet 2.0 (geo key AND Parquet geo type with CRS)
    - parquet_geo_only: Only Parquet geo type, no geo key
    """
    return click.option(
        "--geoparquet-version",
        type=click.Choice(["1", "1.1", "2.0", "parquet_geo_only"]),
        default="1.1",
        show_default=True,
        help="GeoParquet version for output (1, 1.1, 2.0, or parquet_geo_only)",
    )(func)


def keep_bbox_option(func):
    """
    Add --keep-bbox option to a command.

    For GeoParquet 2.0 and parquet_geo_only, the separate bbox column is removed
    by default (since native bbox support is in the Parquet geo type).
    Use this flag to retain the bbox column in output.

    Uses flag_value=True with default=None so:
    - Not specified: None (auto mode - remove for v2.0, keep for v1.x)
    - --keep-bbox: True (always keep)
    """
    return click.option(
        "--keep-bbox",
        is_flag=True,
        flag_value=True,
        default=None,
        help="Keep bbox column in output (by default, removed for v2.0/parquet_geo_only which have native bbox)",
    )(func)


def partition_options(func):
    """
    Add standard partitioning options to a command.

    Adds:
    - --preview: Analyze and preview without creating files
    - --preview-limit: Number of partitions to show in preview
    - --force: Override analysis warnings
    - --skip-analysis: Skip partition strategy analysis
    - --hive: Use Hive-style partitioning
    - --overwrite: Overwrite existing partition files
    - --prefix: Custom filename prefix
    """
    func = click.option(
        "--hive", is_flag=True, help="Use Hive-style partitioning in output folder structure"
    )(func)
    func = click.option("--overwrite", is_flag=True, help="Overwrite existing partition files")(
        func
    )
    func = click.option(
        "--preview",
        is_flag=True,
        help="Analyze and preview partitions without creating files (dry-run)",
    )(func)
    func = click.option(
        "--preview-limit",
        default=15,
        type=int,
        help="Number of partitions to show in preview (default: 15)",
    )(func)
    func = click.option(
        "--force",
        is_flag=True,
        help="Force partitioning even if analysis detects potential issues",
    )(func)
    func = click.option(
        "--skip-analysis",
        is_flag=True,
        help="Skip partition strategy analysis (for performance-sensitive cases)",
    )(func)
    func = prefix_option(func)
    return func
