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


def show_sql_option(func):
    """
    Add --show-sql option to a command.

    Prints the exact SQL statements that will be executed.
    """
    return click.option(
        "--show-sql",
        is_flag=True,
        help="Print exact SQL statements as they are executed",
    )(func)


def overwrite_option(func):
    """
    Add --overwrite option to a command.

    Allows overwriting existing files without prompting.
    """
    return click.option("--overwrite", is_flag=True, help="Overwrite existing files")(func)


def any_extension_option(func):
    """
    Add --any-extension option to a command.

    Allows output files without .parquet extension. By default, commands
    that write parquet files require the output to have a .parquet extension.
    """
    return click.option(
        "--any-extension",
        is_flag=True,
        help="Allow output file without .parquet extension",
    )(func)


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
    - 1.0: GeoParquet 1.0 with WKB encoding
    - 1.1: GeoParquet 1.1 with WKB encoding
    - 2.0: GeoParquet 2.0 with native Parquet geo types
    - parquet-geo-only: Native Parquet geo types without GeoParquet metadata

    If not specified, auto-detects from input: preserves input version,
    upgrades native geo types to 2.0, defaults to 1.1.
    """
    return click.option(
        "--geoparquet-version",
        type=click.Choice(["1.0", "1.1", "2.0", "parquet-geo-only"]),
        default=None,
        help="GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only). "
        "Auto-detects from input if not specified: preserves input version, "
        "upgrades native geo types to 2.0, defaults to 1.1.",
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


def partition_input_options(func):
    """
    Add options for reading partitioned input data.

    Adds:
    - --allow-schema-diff: Combine files with different schemas (fills NULL for missing columns)
    - --hive-input: Explicitly enable hive partitioning on input
    """
    func = click.option(
        "--allow-schema-diff",
        is_flag=True,
        help="Combine files with different schemas (fills NULL for missing columns). "
        "Default: strict schema matching (all files must have same schema).",
    )(func)
    func = click.option(
        "--hive-input",
        is_flag=True,
        help="Enable hive-style partitioning when reading input (adds partition columns to data). "
        "Auto-detected for directories with key=value subdirectories.",
    )(func)
    return func


def check_partition_options(func):
    """
    Add options for check commands on partitioned data.

    Adds:
    - --all-files: Check every file in partition
    - --sample-files: Check first N files
    """
    func = click.option(
        "--all-files",
        "check_all_files",  # Use different param name to avoid conflict with function names
        is_flag=True,
        help="For partitioned data: check every file in the partition.",
    )(func)
    func = click.option(
        "--sample-files",
        "check_sample",  # Keep param name for backwards compatibility
        type=int,
        default=None,
        help="For partitioned data: check first N files (default: check first file only).",
    )(func)
    return func


class GlobAwareCommand(click.Command):
    """
    Command that detects shell-expanded glob patterns and provides helpful errors.

    When a shell expands a glob pattern (e.g., *.parquet) before passing it to
    the CLI, the command receives multiple file arguments instead of a single
    pattern. This class detects that situation and provides a helpful error
    message suggesting the user quote their glob pattern.

    For commands that support glob patterns (like extract), it suggests quoting.
    For commands that don't (like convert), it suggests using gpio extract first.

    Usage:
        @cli.command(cls=GlobAwareCommand)
        def my_command(...):
            ...

        # For single-file commands:
        @cli.command(cls=SingleFileCommand)
        def convert_command(...):
            ...
    """

    # Override in subclass or check command context
    supports_glob = True

    def make_context(self, info_name, args, parent=None, **extra):
        """Detect shell-expanded glob patterns and provide helpful errors."""
        # Count args that look like parquet files (not options)
        parquet_args = [a for a in args if a.endswith(".parquet") and not a.startswith("-")]

        # If more than 2 parquet files (input + output), likely shell-expanded glob
        if len(parquet_args) > 2:
            # Build full command path (e.g., "check all" instead of just "all")
            cmd_path = self._build_command_path(info_name, parent)

            if self.supports_glob:
                # Commands like extract that DO support globs
                raise click.UsageError(
                    f"Received {len(parquet_args)} parquet files as separate arguments.\n\n"
                    "This usually means the shell expanded a glob pattern.\n"
                    "Use quotes to pass the pattern to gpio:\n\n"
                    f'    gpio {cmd_path} "path/*.parquet" output.parquet'
                )
            else:
                # Commands like convert that DON'T support globs
                raise click.UsageError(
                    f"Received {len(parquet_args)} parquet files as separate arguments.\n\n"
                    f"The '{cmd_path}' command requires a single file.\n"
                    "To work with multiple files, first consolidate using:\n\n"
                    f'    gpio extract "path/*.parquet" consolidated.parquet\n\n'
                    f"Then run: gpio {cmd_path} consolidated.parquet ..."
                )

        return super().make_context(info_name, args, parent=parent, **extra)

    def _build_command_path(self, info_name, parent):
        """Build full command path like 'check all' from parent context chain."""
        parts = [info_name]
        ctx = parent
        while ctx is not None:
            # Skip the root 'gpio' command
            if ctx.parent is not None:
                parts.insert(0, ctx.info_name)
            ctx = ctx.parent
        return " ".join(parts)


class SingleFileCommand(GlobAwareCommand):
    """
    Command that requires a single input file (no glob/partition support).

    Use this for commands like convert, sort, add that don't support
    multiple input files natively.
    """

    supports_glob = False
