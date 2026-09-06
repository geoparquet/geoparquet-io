"""
Shared Click decorators for common CLI parameters.

This module provides reusable decorators to ensure consistency across commands
and reduce code duplication.
"""

import functools

import click


def handle_geoparquet_errors(func):
    """
    Decorator to convert GeoParquetError exceptions to user-friendly Click errors.

    Catches GeoParquetError from core functions and converts them to
    appropriate click exceptions (BadParameter for invalid params, ClickException
    for general errors) for clean error display without stack traces.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Import here to avoid circular imports
        from geoparquet_io.cli.exception_handler import handle_core_exception
        from geoparquet_io.core.exceptions import GeoParquetError

        try:
            return func(*args, **kwargs)
        except GeoParquetError as e:
            raise handle_core_exception(e) from None

    return wrapper


def parse_row_group_options(
    row_group_size: int | None,
    row_group_size_mb: str | None,
) -> float | None:
    """
    Parse and validate row group size options.

    Enforces mutual exclusivity between --row-group-size and --row-group-size-mb,
    and converts the size string to MB if provided.

    Args:
        row_group_size: Exact number of rows per row group (from --row-group-size)
        row_group_size_mb: Target size string like '256MB', '1GB' (from --row-group-size-mb)

    Returns:
        Row group size in MB as a float, or None if neither option provided.
        Note: When row_group_size (rows) is provided, this returns None since
        the caller should use row_group_size directly for row-based sizing.

    Raises:
        click.UsageError: If both options are provided or if size string is invalid
    """
    if row_group_size and row_group_size_mb:
        raise click.UsageError("--row-group-size and --row-group-size-mb are mutually exclusive")

    if not row_group_size_mb:
        return None

    from geoparquet_io.core.common import parse_size_string

    try:
        size_bytes = parse_size_string(row_group_size_mb)
        return size_bytes / (1024 * 1024)
    except ValueError as e:
        raise click.UsageError(f"Invalid row group size: {e}") from e


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


def _row_group_size_help(default_rows: int | None) -> str:
    """Help text for --row-group-size, naming a default only where one exists.

    Only commands that resolve their own default (the ``gpio sort`` family, via
    ``resolve_sort_row_group_rows``) may name a number here. Everywhere else the
    option falls through as ``None`` and the writer picks -- DuckDB's COPY uses
    122,880 rows -- so quoting a figure would repeat the #775 bug of advertising
    a default that nothing applies.
    """
    if default_rows is None:
        return (
            "Exact number of rows per row group. Mutually exclusive with "
            "--row-group-size-mb; if neither is given the Parquet writer's own "
            "default applies (122,880 rows for DuckDB-backed writes)."
        )
    return (
        f"Exact number of rows per row group (default: {default_rows} "
        "if --row-group-size-mb not set)"
    )


def row_group_options(func=None, *, default_rows: int | None = None):
    """
    Add row group sizing options to a command.

    Adds:
    - --row-group-size: Exact number of rows per row group
    - --row-group-size-mb: Target row group size in MB or with units (e.g., '256MB', '1GB')

    These options are mutually exclusive. Both default to ``None``: a Click default
    would make ``--row-group-size-mb`` collide with it and raise the
    mutually-exclusive usage error, so a command that wants a default resolves it
    downstream and declares the number here via ``default_rows`` purely so the help
    text matches what the command actually does.

    Usable bare (``@row_group_options``) or called
    (``@row_group_options(default_rows=50_000)``).
    """
    if func is None:
        return lambda inner: row_group_options(inner, default_rows=default_rows)
    func = click.option(
        "--row-group-size",
        type=int,
        default=None,
        help=_row_group_size_help(default_rows),
    )(func)
    func = click.option(
        "--row-group-size-mb", help="Target row group size (e.g. '256MB', '1GB', '128' assumes MB)"
    )(func)
    return func


def output_format_options(
    func=None, *, default_rows: int | None = None, write_memory_help: str | None = None
):
    """
    Add all output format options (compression + row groups + memory limit).

    This is a convenience decorator that combines compression_options, row_group_options,
    and write_memory_option. ``default_rows`` is forwarded to ``row_group_options``;
    ``write_memory_help`` is forwarded to ``write_memory_option`` for the one command
    where the limit governs a read rather than a write (see ``write_memory_option``).
    """
    if func is None:
        return lambda inner: output_format_options(
            inner, default_rows=default_rows, write_memory_help=write_memory_help
        )
    func = compression_options(func)
    func = row_group_options(func, default_rows=default_rows)
    func = write_memory_option(func, help=write_memory_help)
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


def repair_geometry_option(func):
    """
    Add --repair-geometry/--no-repair-geometry option to a command.

    Repairs invalid geometry with ST_MakeValid by default. Users who need to
    preserve invalid geometry exactly can opt out with --no-repair-geometry.
    """
    return click.option(
        "--repair-geometry/--no-repair-geometry",
        default=True,
        help=(
            "Repair invalid geometry with ST_MakeValid (default: on). "
            "Use --no-repair-geometry to preserve invalid geometry exactly."
        ),
    )(func)


def linearize_curves_options(func):
    """
    Add --linearize-curves/--no-linearize-curves and --max-angle-deg options.

    Curved geometries (CircularString through MultiSurface) cannot be
    represented in GeoParquet, so they are stroked into line segments by
    default. Users who want curved input to fail instead can opt out.
    """
    func = click.option(
        "--max-angle-deg",
        type=click.FloatRange(min=0, min_open=True),
        default=None,
        help="Maximum degrees per stroked arc segment when linearizing curves (default: 4).",
    )(func)
    return click.option(
        "--linearize-curves/--no-linearize-curves",
        default=True,
        help=(
            "Stroke curved geometries (CircularString..MultiSurface) into line "
            "segments (default: on). Use --no-linearize-curves to fail on curved input."
        ),
    )(func)


def _validate_write_memory(ctx, param, value):
    """Reject a --write-memory value that isn't a plain size.

    The value is interpolated into DuckDB's ``SET memory_limit = '…'``, so
    validating here both blocks SQL injection and turns garbage input into a
    clean parameter error instead of a raw DuckDB ParserException traceback.
    One shared decorator means one place to fix.
    """
    if value is None:
        return None

    from geoparquet_io.core.write_strategies.duckdb_kv import validate_memory_limit

    try:
        validate_memory_limit(value)
    except ValueError as e:
        raise click.BadParameter(str(e), ctx=ctx, param=param) from None
    return value


# Click does not printf-format help text, so a literal "%%" would be rendered
# verbatim.
_WRITE_MEMORY_HELP = (
    "Memory limit for streaming writes (e.g., '512MB', '2GB'). "
    "Default: 50% of available RAM (container-aware)."
)


def write_memory_option(func=None, *, help: str | None = None):
    """
    Add --write-memory option to a command.

    Allows specifying the DuckDB memory limit for streaming writes.
    When set, DuckDB uses single-threaded mode for memory control.
    Accepts values like '512MB', '2GB', '4GB'.

    Usable bare (``@write_memory_option``) or called
    (``@write_memory_option(help="…")``). The ``help`` override exists for
    commands where the limit does not govern a write: ``gpio extract bigquery``
    writes through PyArrow and applies the value to the DuckDB scan instead
    (gpio #760). Overriding it there must not change any other command, so the
    default lives in ``_WRITE_MEMORY_HELP`` and is never mutated.
    """
    if func is None:
        return lambda inner: write_memory_option(inner, help=help)
    return click.option(
        "--write-memory",
        type=str,
        default=None,
        callback=_validate_write_memory,
        help=help or _WRITE_MEMORY_HELP,
    )(func)


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


def aws_profile_option(func):
    """Add hidden --aws-profile option (now a global option)."""
    return click.option(
        "--aws-profile",
        help="AWS profile name for S3 operations (sets AWS_PROFILE env var)",
        hidden=True,
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
    - 1.1-geoarrow: GeoParquet 1.1 with native GeoArrow (nested-coordinate) encoding
      and no bbox column. Geometry is converted to native GeoArrow types from any
      input; columns mixing incompatible geometry types fall back to WKB.
    - 2.0: GeoParquet 2.0 with native Parquet geo types
    - parquet-geo-only: Native Parquet geo types without GeoParquet metadata

    If not specified, auto-detects from input: preserves 2.0, writes 1.1 for
    1.x inputs (1.0 upgrades to 1.1), upgrades bare native geo types to 2.0,
    defaults to 1.1.
    """
    return click.option(
        "--geoparquet-version",
        type=click.Choice(["1.0", "1.1", "1.1-geoarrow", "2.0", "parquet-geo-only"]),
        default=None,
        help="GeoParquet version to write (1.0, 1.1, 1.1-geoarrow, 2.0, parquet-geo-only). "
        "Auto-detects from input if not specified: preserves 2.0; 1.x inputs "
        "write 1.1; bare native geo types upgrade to 2.0; defaults to 1.1.",
    )(func)


def write_strategy_option(func):
    """
    Add --write-strategy option to a command.

    Allows specifying the write strategy for GeoParquet metadata writes:
    - duckdb-kv (default): Use DuckDB COPY TO with native KV_METADATA (fastest)
    - in-memory: Load entire dataset into memory, apply metadata, write once
    - streaming: Stream Arrow RecordBatches for constant memory usage
    - disk-rewrite: Write with DuckDB, then rewrite with PyArrow for metadata

    Note: When no metadata rewrite is needed (parquet-geo-only, some 2.0 ops),
    a plain DuckDB COPY TO is used regardless of this setting.
    """
    return click.option(
        "--write-strategy",
        type=click.Choice(["duckdb-kv", "in-memory", "streaming", "disk-rewrite"]),
        default="duckdb-kv",
        help="Write strategy for geo metadata. "
        "duckdb-kv (default): DuckDB COPY with native metadata (fastest). "
        "in-memory: load full dataset into memory. "
        "streaming: constant memory usage. "
        "disk-rewrite: reliable fallback.",
    )(func)


def partition_options_base(func):
    """
    Add base partitioning options to a command (without directory sub-partitioning).

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


def where_option(func):
    """Add the ``--where`` row-filter option (same wording as `gpio extract`)."""
    return click.option(
        "--where",
        help="DuckDB WHERE clause for filtering rows. Column names with special "
        'characters need double quotes in SQL (e.g., "crop:name"). Shell escaping varies.',
    )(func)


def metric_nodata_option(func):
    """Add the ``--metric-nodata`` NoData sentinel option for aggregate commands."""
    return click.option(
        "--metric-nodata",
        default=None,
        help='NoData sentinel value(s) in --metric columns, e.g. "-999" or "-999,-9999" '
        '("nan" matches NaN). Mapped to NULL before sum/avg/min/max; count is unaffected.',
    )(func)


def bucket_point_options(func):
    """Add the ``--bucket-point`` / ``--bbox-column`` keying options (#567)."""
    func = click.option(
        "--bbox-column",
        default=None,
        help="Bbox covering column for --bucket-point bbox (auto-detected if omitted).",
    )(func)
    func = click.option(
        "--bucket-point",
        default="geometry",
        help="Where the bucketing point comes from: 'geometry' (centroid, default), "
        "'bbox' (center of a bbox covering column -- skips reading the geometry "
        "column), or the name of an existing point column.",
    )(func)
    return func


def grid_aggregate_options(func):
    """Add the options shared by `gpio process aggregate <grid>` commands.

    Adds (the per-scheme ``--resolution`` is declared on each command, since its
    valid range differs between grids):
    - --auto, --target-per-cell, --max-cells
    - --metric, --metric-nodata, --breakdown, --breakdown-limit
    - --out-geometry, --where, --bucket-point, --bbox-column
    """
    func = bucket_point_options(func)
    func = where_option(func)
    func = click.option(
        "--out-geometry",
        type=click.Choice(["polygon", "centroid", "both", "none"]),
        default="polygon",
        help="Output geometry per cell (default: polygon).",
    )(func)
    func = click.option(
        "--breakdown-limit",
        type=int,
        default=20,
        help="Max breakdown values before remainder rolls into count_other (default: 20).",
    )(func)
    func = click.option(
        "--breakdown",
        default=None,
        help="Categorical column to pivot count by (one count_<value> column each).",
    )(func)
    func = metric_nodata_option(func)
    func = click.option(
        "--metric",
        default=None,
        help='Numeric rollups, e.g. "sum:area_ha,avg:yield". Bare column = sum.',
    )(func)
    func = click.option(
        "--max-cells",
        type=int,
        default=500000,
        help="Maximum output cells when using --auto (default: 500000).",
    )(func)
    func = click.option(
        "--target-per-cell",
        type=int,
        default=10000,
        help="Target features per cell when using --auto (default: 10000).",
    )(func)
    func = click.option("--auto", is_flag=True, help="Auto-select resolution from data size.")(func)
    return func


def partition_options(func):
    """
    Add standard partitioning options to a command with directory sub-partitioning support.

    Adds all base partition options plus:
    - --min-size: Only process files larger than this size (for directory input)
    - --in-place: Replace original files with sub-partitions
    """
    func = partition_options_base(func)
    func = click.option(
        "--min-size",
        default=None,
        help="Only process files larger than this size when input is a directory (e.g., '100MB', '1GB')",
    )(func)
    func = click.option(
        "--in-place",
        is_flag=True,
        help="Replace original files with sub-partitions (requires directory input with --min-size)",
    )(func)
    return func


def allow_schema_diff_option(func):
    """Add --allow-schema-diff to a command that reads multi-file input.

    One definition, shared: `extract` and the two sorts that accept a directory
    (`sort column`, `sort quadkey`) must spell this flag and its help text
    identically, or the same opt-in reads as two different features (#867).
    The sorts take no `--hive-input`, so this is a separate decorator rather
    than part of `partition_input_options`.
    """
    return click.option(
        "--allow-schema-diff",
        is_flag=True,
        help="Combine files with different schemas (fills NULL for missing columns). "
        "Default: strict schema matching (all files must have same schema).",
    )(func)


def partition_input_options(func):
    """
    Add options for reading partitioned input data.

    Adds:
    - --allow-schema-diff: Combine files with different schemas (fills NULL for missing columns)
    - --hive-input: Explicitly enable hive partitioning on input
    """
    func = allow_schema_diff_option(func)
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

    This class also handles GeoParquetError exceptions from core functions,
    converting them to user-friendly Click exceptions without stack traces.

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

    # Default subcommands that should be omitted from hints
    # Maps parent group name to their default subcommand name
    DEFAULT_SUBCOMMANDS = {
        "check": "all",
        "convert": "geoparquet",
        "extract": "geoparquet",
        "inspect": "summary",
    }

    def invoke(self, ctx):
        """Invoke the command with user-friendly error handling."""
        # Import here to avoid circular imports
        from geoparquet_io.cli.exception_handler import handle_core_exception
        from geoparquet_io.core.exceptions import GeoParquetError

        try:
            return super().invoke(ctx)
        except GeoParquetError as e:
            raise handle_core_exception(e) from None

    def make_context(self, info_name, args, parent=None, **extra):
        """Detect shell-expanded glob patterns and provide helpful errors."""
        # Count args that look like parquet files (not options)
        parquet_args = [a for a in args if a.endswith(".parquet") and not a.startswith("-")]

        # If more than 2 parquet files (input + output), likely shell-expanded glob
        if len(parquet_args) > 2:
            # Build full command path (e.g., "check all" instead of just "all")
            # Omits default subcommands for cleaner UX
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
        """Build full command path like 'check all' from parent context chain.

        Omits default subcommands for cleaner user-facing hints.
        For example, 'inspect summary' becomes just 'inspect' since
        'summary' is the default subcommand.
        """
        parts = [info_name]
        ctx = parent
        while ctx is not None:
            # Skip the root 'gpio' command
            if ctx.parent is not None:
                parts.insert(0, ctx.info_name)
            ctx = ctx.parent

        # Check if the last part is a default subcommand and should be omitted
        if len(parts) >= 2:
            parent_name = parts[-2]
            subcommand_name = parts[-1]
            if self.DEFAULT_SUBCOMMANDS.get(parent_name) == subcommand_name:
                # Omit the default subcommand from the path
                parts = parts[:-1]

        return " ".join(parts)


class SingleFileCommand(GlobAwareCommand):
    """
    Command that requires a single input file (no glob/partition support).

    Use this for commands like convert, sort, add that don't support
    multiple input files natively.
    """

    supports_glob = False


def _reject_single_file_only_options(
    partition_type: str,
    column_name: str | None,
    output_folder: str | None,
) -> None:
    """Fail loudly on options a directory --min-size run cannot honour (#790).

    The rule and its wording live in core, so `ops.sub_partition_by_*` refuses
    the same arguments for the same stated reason (#811); only the spelling of
    the argument names differs between the two front doors.
    """
    from geoparquet_io.core.sub_partition import (
        offending_single_file_only_options,
        single_file_only_option_message,
    )

    ignored = offending_single_file_only_options(partition_type, column_name, output_folder)
    if not ignored:
        return

    raise click.UsageError(single_file_only_option_message(partition_type, ignored))


def handle_directory_sub_partition(
    input_parquet: str,
    partition_type: str,
    min_size: str | None,
    resolution: int | None = None,
    level: int | None = None,
    in_place: bool = False,
    hive: bool = False,
    overwrite: bool = False,
    verbose: bool = False,
    force: bool = False,
    skip_analysis: bool = True,
    compression: str | None = None,
    compression_level: int | None = None,
    auto: bool = False,
    target_rows: int = 100000,
    max_partitions: int = 10000,
    preview: bool = False,
    column_name: str | None = None,
    output_folder: str | None = None,
    partition_resolution: int | None = None,
) -> bool:
    """
    Handle directory input with --min-size for partition commands.

    This function checks if the input is a directory and processes it with
    sub_partition_directory if --min-size is provided. It's extracted as a
    shared helper to avoid code duplication across partition commands.

    Args:
        input_parquet: Path to input file or directory
        partition_type: Type of partition ("a5", "h3", "s2", "quadkey")
        min_size: Size threshold string (e.g., "100MB") or None
        resolution: Resolution for A5/H3/quadkey
        partition_resolution: Quadkey partition prefix length
        level: Level for S2
        in_place: Delete originals after sub-partition
        hive: Use Hive-style partitioning
        overwrite: Overwrite existing output
        verbose: Print verbose output
        force: Force operation with warnings
        skip_analysis: Skip partition analysis
        compression: Compression codec
        compression_level: Compression level
        auto: Auto-calculate resolution
        target_rows: Target rows per partition (auto mode)
        max_partitions: Max partitions (auto mode)
        preview: List the files that would be processed and stop
        column_name: Index column name from the command's --<index>-name option
        output_folder: OUTPUT_FOLDER argument, if the user supplied one

    Returns:
        True if directory was handled, False if it's a file (continue to single-file logic)

    Raises:
        click.UsageError: If directory input provided without --min-size, or with
            options that only apply to single-file partitioning
    """
    import os

    if not os.path.isdir(input_parquet):
        return False

    if not min_size:
        raise click.UsageError(
            "Directory input requires --min-size to specify which files to process"
        )

    _reject_single_file_only_options(partition_type, column_name, output_folder)

    from geoparquet_io.core.common import parse_size_string
    from geoparquet_io.core.logging_config import info, progress, warn
    from geoparquet_io.core.sub_partition import plan_sub_partition, sub_partition_directory

    try:
        min_size_bytes = parse_size_string(min_size)
    except ValueError as e:
        raise click.UsageError(str(e)) from None

    if preview:
        # --preview used to be accepted, ignored, and the originals deleted
        # anyway under --in-place (#790). Show the plan and change nothing.
        candidates = plan_sub_partition(input_parquet, partition_type, min_size_bytes)
        if not candidates:
            info(f"No files found exceeding {min_size} in {input_parquet}")
            return True

        progress(f"Would sub-partition {len(candidates)} file(s) by {partition_type}:")
        for candidate in candidates:
            size_mb = candidate["size_bytes"] / (1024 * 1024)
            info(f"  {candidate['path']} ({size_mb:.1f}MB) -> {candidate['output_dir']}/")
        info("Preview only: no files were partitioned or removed.")
        return True

    try:
        result = sub_partition_directory(
            directory=input_parquet,
            partition_type=partition_type,
            min_size_bytes=min_size_bytes,
            resolution=resolution,
            partition_resolution=partition_resolution,
            level=level,
            in_place=in_place,
            hive=hive,
            overwrite=overwrite,
            verbose=verbose,
            force=force,
            skip_analysis=skip_analysis,
            compression=compression.upper() if compression else "ZSTD",
            compression_level=compression_level or 15,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
        )
    except ValueError as e:
        raise click.UsageError(str(e)) from None

    if result["errors"]:
        # Every per-file failure was caught and warned about, so without this the
        # command printed errors, partitioned nothing and still exited 0 (#778).
        for err in result["errors"]:
            warn(f"Error processing {err['file']}: {err['error']}")
        failed = len(result["errors"])
        raise click.ClickException(
            f"{failed} of {failed + result['processed']} file(s) failed to sub-partition; "
            f"see the errors above."
        )

    return True
