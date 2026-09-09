"""``gpio add`` - enhance GeoParquet files with extra columns and metadata.

Registered on the root ``gpio`` group by ``cli/main.py`` via
``cli.add_command(add)``. This module must not import ``cli.main``: the
dependency runs one way, ``main`` -> ``commands`` -> ``_shared``/``decorators``.
"""

import click
from click.core import ParameterSource

from geoparquet_io.cli._shared import _activate_s3
from geoparquet_io.cli.decorators import (
    SingleFileCommand,
    any_extension_option,
    dry_run_option,
    geoparquet_version_option,
    output_format_options,
    overwrite_option,
    parse_row_group_options,
    show_sql_option,
    verbose_option,
)
from geoparquet_io.core.add.a5 import add_a5_column as add_a5_column_impl
from geoparquet_io.core.add.bbox import add_bbox_column as add_bbox_column_impl
from geoparquet_io.core.add.bbox_metadata import add_bbox_metadata as add_bbox_metadata_impl
from geoparquet_io.core.add.h3 import add_h3_column as add_h3_column_impl
from geoparquet_io.core.add.kdtree import add_kdtree_column as add_kdtree_column_impl
from geoparquet_io.core.add.quadkey import add_quadkey_column as add_quadkey_column_impl
from geoparquet_io.core.add.s2 import add_s2_column as add_s2_column_impl
from geoparquet_io.core.file_utils import validate_parquet_extension
from geoparquet_io.core.logging_config import setup_cli_logging


@click.group()
@click.pass_context
def add(ctx):
    """Commands for enhancing GeoParquet files in various ways."""
    # Ensure logging is set up (in case this group is invoked directly in tests)
    ctx.ensure_object(dict)
    timestamps = ctx.obj.get("timestamps", False)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


@add.command(name="admin-divisions", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", required=False, default=None)
@click.option(
    "--dataset",
    type=click.Choice(["gaul", "overture"], case_sensitive=False),
    default="gaul",
    help="Admin boundaries dataset: 'gaul' (GAUL L2) or 'overture' (Overture Maps)",
)
@click.option(
    "--vecorel",
    is_flag=True,
    help="Output Vecorel-compliant columns (admin:country_code, admin:subdivision_code) "
    "with schema metadata. Automatically uses Overture dataset with country,region levels.",
)
@click.option(
    "--levels",
    help="Comma-separated hierarchical levels to add as columns (e.g., 'continent,country'). "
    "If not specified, adds all available levels for the dataset.",
)
@click.option(
    "--add-bbox", is_flag=True, help="Automatically add bbox column and metadata if missing."
)
@click.option(
    "--prefix",
    type=str,
    default=None,
    help="Column name prefix. Defaults to dataset name (gaul, overture). "
    "Use 'admin' for admin:level format.",
)
@click.option(
    "--no-cache",
    is_flag=True,
    help="Skip local cache and use remote dataset directly. "
    "Useful when you need the latest data or are troubleshooting.",
)
@click.option(
    "--clear-cache",
    is_flag=True,
    help="Delete all cached admin datasets before running. "
    "Shows size of deleted files and prompts for confirmation.",
)
@output_format_options
@geoparquet_version_option
@overwrite_option
@dry_run_option
@verbose_option
@any_extension_option
@show_sql_option
def add_country_codes(
    input_parquet,
    output_parquet,
    dataset,
    vecorel,
    levels,
    add_bbox,
    prefix,
    no_cache,
    clear_cache,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
    overwrite,
    dry_run,
    verbose,
    any_extension,
    show_sql,
):
    """Add admin division columns via spatial join with remote boundaries datasets.

    Performs spatial intersection to add administrative division columns to your data.

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.

    \b
    **Datasets:**
    - gaul: GAUL L2 (levels: continent, country, department)
    - overture: Overture Maps (levels: country, region, locality)

    \b
    **Column Naming (Breaking Change in v0.7):**
    By default, columns are prefixed with the dataset name to prevent conflicts:
    - GAUL: gaul_country, gaul_continent, gaul_department
    - Overture: overture_country, overture_region

    Use --prefix to customize:
    - --prefix admin: admin:country format (old behavior)
    - --prefix mycustom: mycustom_country format

    \b
    **Examples:**

    \b
    # Add GAUL levels (creates gaul_continent, gaul_country, gaul_department)
    gpio add admin-divisions input.parquet output.parquet --dataset gaul

    \b
    # Add Overture levels (creates overture_country, overture_region)
    gpio add admin-divisions input.parquet output.parquet --dataset overture

    \b
    # Add both datasets to same file (no conflicts!)
    gpio add admin-divisions input.parquet temp.parquet --dataset gaul
    gpio add admin-divisions temp.parquet output.parquet --dataset overture

    \b
    # Use admin: format (old behavior)
    gpio add admin-divisions input.parquet output.parquet --dataset gaul --prefix admin

    \b
    # Custom prefix
    gpio add admin-divisions input.parquet output.parquet --dataset gaul --prefix source1

    \b
    # Preview SQL before execution
    gpio add admin-divisions input.parquet output.parquet --dataset gaul --dry-run

    \b
    # Clear cached datasets to get fresh data
    gpio add admin-divisions input.parquet output.parquet --dataset gaul --clear-cache

    \b
    # Skip cache entirely (use remote directly)
    gpio add admin-divisions input.parquet output.parquet --dataset gaul --no-cache

    \b
    **Caching:**
    Admin datasets (GAUL, Overture) are cached locally on first use to speed up
    subsequent runs. Cache location: ~/.geoparquet-io/cache/admin/

    - First run: Downloads and caches the full dataset (~5-50MB depending on dataset)
    - Subsequent runs: Uses cached version (instant startup)
    - Warning shown if cache is older than 6 months
    - Use --no-cache to skip cache or --clear-cache to delete cached data

    \b
    **Note:** Requires internet connection to fetch remote boundaries datasets.
    Input data must have valid geometries in WGS84 or compatible CRS.
    """
    from geoparquet_io.core.admin_datasets import (
        clear_cache as clear_admin_cache,
    )
    from geoparquet_io.core.admin_datasets import (
        default_admin_levels,
        get_cache_dir,
    )
    from geoparquet_io.core.logging_config import info, success
    from geoparquet_io.core.streaming import is_stdin, should_stream_output

    # Handle --clear-cache flag first
    if clear_cache:
        cache_dir = get_cache_dir()
        if cache_dir.exists():
            # Get list of files to show size
            parquet_files = list(cache_dir.glob("*.parquet"))
            if parquet_files:
                total_size = sum(f.stat().st_size for f in parquet_files)
                size_mb = total_size / (1024 * 1024)
                click.echo(f"Cache directory: {cache_dir}")
                click.echo(f"Files to delete: {len(parquet_files)}")
                click.echo(f"Total size: {size_mb:.2f} MB")

                if click.confirm("Delete all cached admin datasets?"):
                    result = clear_admin_cache(confirm=True)
                    success(
                        f"Cleared cache: {result['files_deleted']} files, "
                        f"{result['bytes_freed'] / (1024 * 1024):.2f} MB freed"
                    )
                else:
                    info("Cache clear cancelled.")
            else:
                click.echo("No cached datasets found.")
        else:
            click.echo("No cache directory found.")

    # Check for streaming mode - not supported yet for admin-divisions
    if is_stdin(input_parquet) or should_stream_output(output_parquet):
        raise click.ClickException(
            "Streaming (stdin/stdout) is not yet supported for 'gpio add admin-divisions'.\n"
            "Please use file paths instead:\n"
            "  gpio add admin-divisions input.parquet output.parquet"
        )

    # Require output_parquet for non-streaming mode
    if output_parquet is None:
        raise click.UsageError("Missing argument 'OUTPUT_PARQUET'.")

    # Validate .parquet extension
    validate_parquet_extension(output_parquet, any_extension)

    # Parse row group options
    row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

    # Use new multi-dataset implementation
    from geoparquet_io.core.add.admin_divisions import add_admin_divisions_multi

    # Handle --vecorel flag: force Overture dataset with country,region levels
    if vecorel:
        if dataset != "gaul" and dataset != "overture":
            from geoparquet_io.core.logging_config import warn as log_warn

            log_warn(f"--vecorel overrides --dataset {dataset} to 'overture'")
        dataset = "overture"
        if levels:
            from geoparquet_io.core.logging_config import warn as log_warn

            log_warn("--vecorel overrides --levels to 'country,region'")
        level_list = ["country", "region"]
    elif levels:
        level_list = [level.strip() for level in levels.split(",")]
    else:
        # Use all available levels for the dataset (shared with the Python API)
        level_list = default_admin_levels(dataset)

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
        geoparquet_version=geoparquet_version,
        overwrite=overwrite,
        prefix=prefix,
        no_cache=no_cache,
        vecorel=vecorel,
        memory_limit=write_memory,
    )


@add.command(name="geometry-metrics", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", required=False, default=None)
@click.option(
    "--no-vecorel",
    is_flag=True,
    help="Skip adding Vecorel schema metadata to the output file.",
)
@output_format_options
@geoparquet_version_option
@overwrite_option
@dry_run_option
@verbose_option
@any_extension_option
@show_sql_option
def add_geometry_metrics_cmd(
    input_parquet,
    output_parquet,
    no_vecorel,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
    overwrite,
    dry_run,
    verbose,
    any_extension,
    show_sql,
):
    """Add geometry metrics (area and perimeter in meters) to a GeoParquet file.

    Calculates geodesic area (m²) and perimeter (m) for each geometry using
    spheroid-based calculations (WGS84). Results are stored as:

    \b
    - metrics:area — area in square meters (float)
    - metrics:perimeter — perimeter in meters (float)

    Follows the Vecorel geometry-metrics extension specification by default.
    Use --no-vecorel to skip adding schema metadata.

    \b
    **Examples:**

    \b
    # Add geometry metrics
    gpio add geometry-metrics input.parquet output.parquet

    \b
    # Preview SQL before execution
    gpio add geometry-metrics input.parquet output.parquet --dry-run

    \b
    # Without Vecorel metadata
    gpio add geometry-metrics input.parquet output.parquet --no-vecorel
    """
    from geoparquet_io.core.streaming import is_stdin, should_stream_output

    # Check for streaming mode
    if not is_stdin(input_parquet) and not should_stream_output(output_parquet):
        if output_parquet is None:
            raise click.UsageError("Missing argument 'OUTPUT_PARQUET'.")
        validate_parquet_extension(output_parquet, any_extension)

    row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

    from geoparquet_io.core.add.geometry_metrics import add_geometry_metrics

    add_geometry_metrics(
        input_parquet,
        output_parquet,
        vecorel=not no_vecorel,
        dry_run=dry_run,
        verbose=verbose,
        compression=compression.upper(),
        compression_level=compression_level,
        row_group_size_mb=row_group_mb,
        row_group_rows=row_group_size,
        profile=None,
        geoparquet_version=geoparquet_version,
        overwrite=overwrite,
        show_sql=show_sql,
        memory_limit=write_memory,
    )


@add.command(name="bbox", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", required=False, default=None)
@click.option("--bbox-name", default="bbox", help="Name for the bbox column (default: bbox)")
@click.option(
    "--force",
    is_flag=True,
    help="Recompute and replace an existing bbox column instead of copying the input",
)
@output_format_options
@geoparquet_version_option
@overwrite_option
@dry_run_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
def add_bbox(
    ctx,
    input_parquet,
    output_parquet,
    bbox_name,
    force,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
    overwrite,
    dry_run,
    verbose,
    any_extension,
    show_sql,
):
    """Add a bbox struct column to a GeoParquet file.

    Creates a new column with bounding box coordinates (xmin, ymin, xmax, ymax)
    for each geometry feature. Bbox covering metadata is automatically added to the
    GeoParquet file (GeoParquet 1.1 spec). The bbox column improves spatial query
    performance.

    If the file already has a bbox column, nothing is recomputed, but OUTPUT_FILE is
    still written: the input is copied to it verbatim and the copy is reported, so a
    pipeline step never ends with no output file. Explicitly asking for
    --geoparquet-version, --compression, --compression-level or --row-group-size
    recomputes instead, since a copy cannot honour them. Use --force to recompute and
    replace an existing bbox column.

    Reads local paths, s3://, gs:// and https:// URLs, and writes local
    paths, s3://, gs:// and az:// URLs. az:// is a write destination only --
    reading from Azure is not supported. An Azure output names the storage
    account first: az://<account>/<container>/<path>, with credentials from
    AZURE_STORAGE_ACCOUNT_KEY or AZURE_STORAGE_SAS_TOKEN. Remote work -- the
    copy included -- goes through the object store gpio is configured to use,
    so --s3-endpoint, --s3-region, --s3-no-ssl and --aws-profile apply.

    Examples:

        \b
        # Local to local
        gpio add bbox input.parquet output.parquet

        \b
        # Remote to remote
        gpio add bbox s3://bucket/in.parquet s3://bucket/out.parquet --aws-profile my-aws

        \b
        # Force replace existing bbox
        gpio add bbox input.parquet output.parquet --force
    """
    with _activate_s3(ctx):
        # Validate output early - provides helpful error if no output and not piping
        from geoparquet_io.core.streaming import StreamingError, validate_output

        try:
            validate_output(output_parquet)
        except StreamingError as e:
            raise click.ClickException(str(e)) from None

        # Validate .parquet extension
        validate_parquet_extension(output_parquet, any_extension)

        # Parse row group options
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        from geoparquet_io.core.streaming import StreamingError

        # An input that already has a bbox column is answered with a verbatim copy,
        # which cannot honour a --compression the user actually typed. Pass None
        # when they did not, so the copy branch can tell the two apart.
        compression_requested = (
            ctx.get_parameter_source("compression") is not ParameterSource.DEFAULT
        )

        try:
            add_bbox_column_impl(
                input_parquet,
                output_parquet,
                bbox_column_name=bbox_name,
                dry_run=dry_run,
                verbose=verbose,
                compression=compression.upper() if compression_requested else None,
                compression_level=compression_level,
                row_group_size_mb=row_group_mb,
                row_group_rows=row_group_size,
                profile=None,
                force=force,
                geoparquet_version=geoparquet_version,
                overwrite=overwrite,
                memory_limit=write_memory,
            )
        except StreamingError as e:
            raise click.ClickException(str(e)) from None


@add.command(name="bbox-metadata", cls=SingleFileCommand)
@click.argument("parquet_file")
@verbose_option
@click.pass_context
def add_bbox_metadata_cmd(ctx, parquet_file, verbose):
    """Add bbox covering metadata for an existing bbox column.

    Use this when you have a file with a bbox column but no covering metadata.
    This modifies the file in-place, preserving all data and file properties.

    If you need to add both the bbox column and metadata, use 'add bbox' instead.
    """
    with _activate_s3(ctx):
        from geoparquet_io.core.remote import setup_aws_profile_if_needed, validate_profile_for_urls

        # Validate profile is only used with S3
        validate_profile_for_urls(None, parquet_file)

        # Setup AWS profile if needed
        setup_aws_profile_if_needed(None, parquet_file)

        add_bbox_metadata_impl(parquet_file, verbose)


@add.command(name="h3", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", required=False, default=None)
@click.option("--h3-name", default="h3_cell", help="Name for the H3 column (default: h3_cell)")
@click.option(
    "--resolution",
    default=9,
    type=click.IntRange(0, 15),
    help="H3 resolution level (0-15). Res 7: ~5km², Res 9: ~105m², Res 11: ~2m², Res 13: ~0.04m². Default: 9",
)
@output_format_options
@geoparquet_version_option
@overwrite_option
@dry_run_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
def add_h3(
    ctx,
    input_parquet,
    output_parquet,
    h3_name,
    resolution,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
    overwrite,
    dry_run,
    verbose,
    any_extension,
    show_sql,
):
    """Add an H3 cell ID column to a GeoParquet file.

    Computes H3 hexagonal cell IDs based on geometry centroids. H3 is a hierarchical
    hexagonal geospatial indexing system that provides consistent cell sizes and shapes
    across the globe.

    The cell ID is stored as a VARCHAR (string) for maximum portability across tools.
    Resolution determines cell size - higher values mean smaller cells with more precision.

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.
    """
    with _activate_s3(ctx):
        # Validate output early - provides helpful error if no output and not piping
        from geoparquet_io.core.streaming import StreamingError, validate_output

        try:
            validate_output(output_parquet)
        except StreamingError as e:
            raise click.ClickException(str(e)) from None

        # Validate .parquet extension
        validate_parquet_extension(output_parquet, any_extension)

        # Parse row group options
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        try:
            add_h3_column_impl(
                input_parquet,
                output_parquet,
                h3_column_name=h3_name,
                h3_resolution=resolution,
                dry_run=dry_run,
                verbose=verbose,
                compression=compression.upper(),
                compression_level=compression_level,
                row_group_size_mb=row_group_mb,
                row_group_rows=row_group_size,
                profile=None,
                geoparquet_version=geoparquet_version,
                overwrite=overwrite,
                memory_limit=write_memory,
            )
        except StreamingError as e:
            raise click.ClickException(str(e)) from None


@add.command(name="a5", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", required=False, default=None)
@click.option("--a5-name", default="a5_cell", help="Name for the A5 column (default: a5_cell)")
@click.option(
    "--resolution",
    default=15,
    type=click.IntRange(0, 30),
    help="A5 resolution level (0-30). Res 10: ~41km², Res 15: ~39m², Res 20: ~39mm², Res 25: ~38μm². Default: 15",
)
@output_format_options
@geoparquet_version_option
@overwrite_option
@dry_run_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
def add_a5(
    ctx,
    input_parquet,
    output_parquet,
    a5_name,
    resolution,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
    overwrite,
    dry_run,
    verbose,
    any_extension,
    show_sql,
):
    """Add an A5 cell ID column to a GeoParquet file.

    Computes A5 cell IDs based on geometry centroids. A5 is a discrete global grid
    system that partitions the world into equal-area pentagonal cells based on a
    dodecahedron, providing minimal shape distortion across the globe.

    The cell ID is stored as a UBIGINT (unsigned 64-bit integer) for efficient storage.
    Resolution determines cell size - higher values mean smaller cells with more precision.

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.
    """
    with _activate_s3(ctx):
        # Validate output early - provides helpful error if no output and not piping
        from geoparquet_io.core.streaming import StreamingError, validate_output

        try:
            validate_output(output_parquet)
        except StreamingError as e:
            raise click.ClickException(str(e)) from None

        # Validate .parquet extension
        validate_parquet_extension(output_parquet, any_extension)

        # Parse row group options
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        try:
            add_a5_column_impl(
                input_parquet,
                output_parquet,
                a5_column_name=a5_name,
                a5_resolution=resolution,
                dry_run=dry_run,
                verbose=verbose,
                compression=compression.upper(),
                compression_level=compression_level,
                row_group_size_mb=row_group_mb,
                row_group_rows=row_group_size,
                profile=None,
                geoparquet_version=geoparquet_version,
                overwrite=overwrite,
                memory_limit=write_memory,
            )
        except StreamingError as e:
            raise click.ClickException(str(e)) from None


@add.command(name="s2", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", required=False, default=None)
@click.option("--s2-name", default="s2_cell", help="Name for the S2 column (default: s2_cell)")
@click.option(
    "--level",
    default=13,
    type=click.IntRange(0, 30),
    help="S2 level (0-30). Level 8: ~1,250km², Level 13: ~1.2km², Level 18: ~1,200m². Default: 13",
)
@output_format_options
@geoparquet_version_option
@overwrite_option
@dry_run_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
def add_s2(
    ctx,
    input_parquet,
    output_parquet,
    s2_name,
    level,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
    overwrite,
    dry_run,
    verbose,
    any_extension,
    show_sql,
):
    """Add an S2 cell ID column to a GeoParquet file.

    Computes S2 spherical cell IDs based on geometry centroids. S2 is Google's
    hierarchical spherical geospatial indexing system that provides consistent
    coverage across the globe using a quadtree structure.

    The cell ID is stored as a token (hex string) for maximum portability across tools.
    Level determines cell size - higher values mean smaller cells with more precision.

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.
    """
    with _activate_s3(ctx):
        # Validate output early - provides helpful error if no output and not piping
        from geoparquet_io.core.streaming import StreamingError, validate_output

        try:
            validate_output(output_parquet)
        except StreamingError as e:
            raise click.ClickException(str(e)) from None

        # Validate .parquet extension
        validate_parquet_extension(output_parquet, any_extension)

        # Parse row group options
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        try:
            add_s2_column_impl(
                input_parquet,
                output_parquet,
                s2_column_name=s2_name,
                s2_level=level,
                dry_run=dry_run,
                verbose=verbose,
                compression=compression.upper(),
                compression_level=compression_level,
                row_group_size_mb=row_group_mb,
                row_group_rows=row_group_size,
                profile=None,
                geoparquet_version=geoparquet_version,
                overwrite=overwrite,
                memory_limit=write_memory,
            )
        except StreamingError as e:
            raise click.ClickException(str(e)) from None


@add.command(name="kdtree", cls=SingleFileCommand)
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
@geoparquet_version_option
@overwrite_option
@dry_run_option
@click.option(
    "--force",
    is_flag=True,
    help="Force operation on large datasets without confirmation",
)
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
def add_kdtree(
    ctx,
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
    write_memory,
    geoparquet_version,
    overwrite,
    dry_run,
    force,
    verbose,
    any_extension,
    show_sql,
):
    """Add a KD-tree cell ID column to a GeoParquet file.

    Creates balanced spatial partitions using recursive splits alternating between
    X and Y dimensions at medians. Partition count must be a power of 2.

    By default, auto-selects partitions targeting ~120k rows each using approximate mode
    (O(n) with 100k sample). Use --partitions N for explicit control or --exact for
    deterministic computation.

    Performance Note: Approximate mode is O(n), exact mode is O(n × log2(partitions)).

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.

    Use --verbose to track progress with iteration-by-iteration updates.
    """
    with _activate_s3(ctx):
        import math

        # Validate .parquet extension
        validate_parquet_extension(output_parquet, any_extension)

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
            raise click.UsageError(
                f"Partitions must be a power of 2 (2, 4, 8, ...), got {partitions}"
            )

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

        # Parse row group options
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        add_kdtree_column_impl(
            input_parquet,
            output_parquet,
            kdtree_column_name=kdtree_name,
            iterations=iterations,
            dry_run=dry_run,
            verbose=verbose,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            force=force,
            sample_size=sample_size,
            auto_target_rows=auto_target,
            profile=None,
            geoparquet_version=geoparquet_version,
            overwrite=overwrite,
            memory_limit=write_memory,
        )


@add.command(name="quadkey", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", required=False, default=None)
@click.option(
    "--quadkey-name",
    default="quadkey",
    help="Name for the quadkey column (default: quadkey)",
)
@click.option(
    "--resolution",
    default=13,
    type=click.IntRange(0, 23),
    help="Quadkey zoom level (0-23). Higher = more precision. Default: 13",
)
@click.option(
    "--use-centroid",
    is_flag=True,
    help="Use geometry centroid instead of bbox midpoint for quadkey calculation",
)
@output_format_options
@geoparquet_version_option
@overwrite_option
@dry_run_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
def add_quadkey(
    ctx,
    input_parquet,
    output_parquet,
    quadkey_name,
    resolution,
    use_centroid,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
    overwrite,
    dry_run,
    verbose,
    any_extension,
    show_sql,
):
    """Add a quadkey column to a GeoParquet file.

    Computes quadkey tile IDs based on geometry location. By default, uses the
    bbox column midpoint if available, otherwise falls back to geometry centroid.

    Quadkeys are a way of encoding tile coordinates (x, y, zoom) into a single
    string, providing a compact spatial index that is particularly useful for
    mapping applications and tile-based systems.

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.
    """
    with _activate_s3(ctx):
        # Validate output early - provides helpful error if no output and not piping
        from geoparquet_io.core.streaming import StreamingError, validate_output

        try:
            validate_output(output_parquet)
        except StreamingError as e:
            raise click.ClickException(str(e)) from None

        # Validate .parquet extension
        validate_parquet_extension(output_parquet, any_extension)

        # Parse row group options
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        try:
            add_quadkey_column_impl(
                input_parquet,
                output_parquet,
                quadkey_column_name=quadkey_name,
                resolution=resolution,
                use_centroid=use_centroid,
                dry_run=dry_run,
                verbose=verbose,
                compression=compression.upper(),
                compression_level=compression_level,
                row_group_size_mb=row_group_mb,
                row_group_rows=row_group_size,
                geoparquet_version=geoparquet_version,
                overwrite=overwrite,
                memory_limit=write_memory,
            )
        except StreamingError as e:
            raise click.ClickException(str(e)) from None
