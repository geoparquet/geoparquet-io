from pathlib import Path

import click

from geoparquet_io.cli.decorators import (
    compression_options,
    dry_run_option,
    geoparquet_version_option,
    output_format_options,
    overwrite_option,
    partition_options,
    profile_option,
    show_sql_option,
    verbose_option,
)
from geoparquet_io.cli.fix_helpers import handle_fix_common
from geoparquet_io.core.add_bbox_column import add_bbox_column as add_bbox_column_impl
from geoparquet_io.core.add_bbox_metadata import add_bbox_metadata as add_bbox_metadata_impl
from geoparquet_io.core.add_h3_column import add_h3_column as add_h3_column_impl
from geoparquet_io.core.add_kdtree_column import add_kdtree_column as add_kdtree_column_impl
from geoparquet_io.core.check_parquet_structure import check_all as check_structure_impl
from geoparquet_io.core.check_spatial_order import check_spatial_order as check_spatial_impl
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.extract import extract as extract_impl
from geoparquet_io.core.hilbert_order import hilbert_order as hilbert_impl
from geoparquet_io.core.inspect_utils import (
    extract_file_info,
    extract_geo_info,
    format_json_output,
    format_markdown_output,
    format_terminal_output,
    get_column_statistics,
    get_preview_data,
)
from geoparquet_io.core.logging_config import configure_verbose, setup_cli_logging
from geoparquet_io.core.partition_admin_hierarchical import (
    partition_by_admin_hierarchical as partition_admin_hierarchical_impl,
)
from geoparquet_io.core.partition_by_h3 import partition_by_h3 as partition_by_h3_impl
from geoparquet_io.core.partition_by_kdtree import partition_by_kdtree as partition_by_kdtree_impl
from geoparquet_io.core.partition_by_string import (
    partition_by_string as partition_by_string_impl,
)
from geoparquet_io.core.upload import upload as upload_impl

# Version info
__version__ = "0.6.1"


@click.group()
@click.version_option(version=__version__, prog_name="geoparquet-io")
@click.option("--timestamps", is_flag=True, help="Show timestamps in output messages")
@click.pass_context
def cli(ctx, timestamps):
    """Fast I/O and transformation tools for GeoParquet files."""
    ctx.ensure_object(dict)
    ctx.obj["timestamps"] = timestamps
    # Setup logging for CLI output (default level INFO, verbose commands will set DEBUG)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


# Check commands group - use custom command class for default subcommand
class DefaultGroup(click.Group):
    """Custom Group that invokes a default command when no subcommand is provided."""

    def parse_args(self, ctx, args):
        # Special case: if --help is in args and no subcommand, show group help
        if "--help" in args and (not args or args[0] not in self.commands):
            # Remove --help and let the group handle it
            return super().parse_args(ctx, [a for a in args if a != "--help"] + ["--help"])

        # Check if first arg (if it exists and doesn't start with -) is a known subcommand
        if args and not args[0].startswith("-") and args[0] in self.commands:
            # Normal subcommand - use default parsing
            return super().parse_args(ctx, args)

        # No subcommand or unknown subcommand - default to 'all'
        # Insert 'all' as the subcommand
        return super().parse_args(ctx, ["all"] + args)


@cli.group(cls=DefaultGroup)
@click.pass_context
def check(ctx):
    """Check GeoParquet files for best practices.

    By default, runs all checks (compression, bbox, row groups, and spatial order).
    Use subcommands for specific checks.

    When run without a subcommand, all checks are performed. Options like --fix
    can be used directly without specifying 'all'.
    """
    # Ensure logging is set up (in case this group is invoked directly in tests)
    ctx.ensure_object(dict)
    timestamps = ctx.obj.get("timestamps", False)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


@check.command(name="all")
@click.argument("parquet_file")
@click.option("--verbose", is_flag=True, help="Print detailed diagnostics")
@click.option("--fix", is_flag=True, help="Fix detected issues")
@click.option(
    "--fix-output",
    type=click.Path(),
    help="Output path for fixed file (default: overwrites with .bak backup)",
)
@click.option(
    "--no-backup",
    is_flag=True,
    help="Skip .bak backup when fixing",
)
@overwrite_option
@click.option(
    "--random-sample-size",
    default=100,
    show_default=True,
    help="Sample size for spatial order check",
)
@click.option(
    "--limit-rows",
    default=500000,
    show_default=True,
    help="Max rows for spatial order check",
)
@profile_option
def check_all(
    parquet_file,
    verbose,
    fix,
    fix_output,
    no_backup,
    overwrite,
    random_sample_size,
    limit_rows,
    profile,
):
    """Check compression, bbox, row groups, and spatial order."""
    import os

    from geoparquet_io.core.check_fixes import apply_all_fixes
    from geoparquet_io.core.common import is_remote_url, show_remote_read_message

    configure_verbose(verbose)

    # Show single progress message for remote files
    show_remote_read_message(parquet_file, verbose=False)
    if is_remote_url(parquet_file):
        click.echo()  # Add blank line after remote message

    # Run all checks and collect results
    structure_results = check_structure_impl(parquet_file, verbose, return_results=True)

    click.echo("\nSpatial Order Analysis:")
    spatial_result = check_spatial_impl(
        parquet_file, random_sample_size, limit_rows, verbose, return_results=True
    )
    ratio = spatial_result["ratio"]

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

    # If --fix flag is set, apply fixes
    if fix:
        from geoparquet_io.cli.fix_helpers import (
            create_backup_if_needed,
            handle_fix_error,
            validate_remote_file_modification,
            verify_fixes,
        )

        # Aggregate all results
        all_results = {**structure_results, "spatial": spatial_result}

        # Check if any fixes are needed
        needs_fixes = any(
            result.get("fix_available", False)
            for result in all_results.values()
            if isinstance(result, dict)
        )

        if not needs_fixes:
            click.echo(click.style("\n✓ No fixes needed - file is already optimal!", fg="green"))
            return

        # Handle remote files
        is_remote = validate_remote_file_modification(parquet_file, fix_output, overwrite)

        # Determine output path
        output_path = fix_output or parquet_file
        backup_path = f"{parquet_file}.bak"

        # Confirm overwrite without backup for local files
        if no_backup and not fix_output and output_path == parquet_file and not is_remote:
            click.confirm(
                "This will overwrite the original file without backup. Continue?",
                abort=True,
            )

        # Create backup if needed (only for local files)
        backup_path = create_backup_if_needed(
            parquet_file, output_path, no_backup, is_remote, verbose
        )

        # Apply fixes
        click.echo("\n" + "=" * 60)
        click.echo("Applying fixes...")
        click.echo("=" * 60)

        try:
            fixes_summary = apply_all_fixes(
                parquet_file, output_path, all_results, verbose, profile
            )

            click.echo("\n" + "=" * 60)
            click.echo("Fixes applied:")
            for fix in fixes_summary["fixes_applied"]:
                click.echo(click.style(f"  ✓ {fix}", fg="green"))
            click.echo("=" * 60)

            # Re-run checks to verify
            verify_fixes(
                output_path,
                check_structure_impl,
                check_spatial_impl,
                random_sample_size,
                limit_rows,
            )

            click.echo(f"\nOptimized file: {output_path}")
            if (
                not no_backup
                and output_path == parquet_file
                and backup_path
                and os.path.exists(backup_path)
            ):
                click.echo(f"Backup: {backup_path}")

        except Exception as e:
            handle_fix_error(e, no_backup, output_path, parquet_file, backup_path)
            raise


@check.command(name="spatial")
@click.argument("parquet_file")
@click.option(
    "--random-sample-size",
    default=100,
    show_default=True,
    help="Sample size for spatial order check",
)
@click.option(
    "--limit-rows",
    default=500000,
    show_default=True,
    help="Max rows for spatial order check",
)
@click.option("--verbose", is_flag=True, help="Print detailed diagnostics")
@click.option("--fix", is_flag=True, help="Fix with Hilbert ordering")
@click.option(
    "--fix-output",
    type=click.Path(),
    help="Output path (default: overwrites with .bak backup)",
)
@click.option(
    "--no-backup",
    is_flag=True,
    help="Skip .bak backup when fixing",
)
@profile_option
def check_spatial(
    parquet_file, random_sample_size, limit_rows, verbose, fix, fix_output, no_backup, profile
):
    """Check spatial ordering."""

    from geoparquet_io.core.check_fixes import fix_spatial_ordering

    result = check_spatial_impl(
        parquet_file, random_sample_size, limit_rows, verbose, return_results=True
    )
    ratio = result["ratio"]

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

    if fix:
        if not result.get("fix_available", False):
            click.echo(click.style("\n✓ No fix needed - already spatially ordered!", fg="green"))
            return

        click.echo("\nApplying Hilbert spatial ordering...")
        output_path, backup_path = handle_fix_common(
            parquet_file, fix_output, no_backup, fix_spatial_ordering, verbose, False, profile
        )

        click.echo(click.style("\n✓ Spatial ordering applied successfully!", fg="green"))
        click.echo(f"Optimized file: {output_path}")
        if backup_path:
            click.echo(f"Backup: {backup_path}")


@check.command(name="compression")
@click.argument("parquet_file")
@click.option("--verbose", is_flag=True, help="Print detailed diagnostics")
@click.option("--fix", is_flag=True, help="Recompress geometry with ZSTD")
@click.option(
    "--fix-output",
    type=click.Path(),
    help="Output path (default: overwrites with .bak backup)",
)
@click.option(
    "--no-backup",
    is_flag=True,
    help="Skip .bak backup when fixing",
)
@overwrite_option
@profile_option
def check_compression_cmd(parquet_file, verbose, fix, fix_output, no_backup, overwrite, profile):
    """Check geometry column compression."""
    from geoparquet_io.core.check_fixes import fix_compression
    from geoparquet_io.core.check_parquet_structure import check_compression

    result = check_compression(parquet_file, verbose, return_results=True)

    if fix:
        if not result.get("fix_available", False):
            click.echo(click.style("\n✓ No fix needed - already using ZSTD!", fg="green"))
            return

        click.echo("\nRe-compressing with ZSTD...")
        output_path, backup_path = handle_fix_common(
            parquet_file, fix_output, no_backup, fix_compression, verbose, overwrite, profile
        )

        click.echo(click.style("\n✓ Compression optimized successfully!", fg="green"))
        click.echo(f"Optimized file: {output_path}")
        if backup_path:
            click.echo(f"Backup: {backup_path}")


@check.command(name="bbox")
@click.argument("parquet_file")
@click.option("--verbose", is_flag=True, help="Print detailed diagnostics")
@click.option("--fix", is_flag=True, help="Fix bbox (add for v1.x, remove for v2/parquet-geo)")
@click.option(
    "--fix-output",
    type=click.Path(),
    help="Output path (default: overwrites with .bak backup)",
)
@click.option(
    "--no-backup",
    is_flag=True,
    help="Skip .bak backup when fixing",
)
@overwrite_option
@profile_option
def check_bbox_cmd(parquet_file, verbose, fix, fix_output, no_backup, overwrite, profile):
    """Check bbox column and metadata (version-aware).

    For GeoParquet 1.x: bbox column is recommended for spatial filtering.
    For GeoParquet 2.0/parquet-geo-only: bbox column is NOT recommended
    (native Parquet geo types provide row group statistics).
    """
    from geoparquet_io.core.check_fixes import fix_bbox_all, fix_bbox_removal
    from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

    result = check_metadata_and_bbox(parquet_file, verbose, return_results=True)

    if fix:
        if not result.get("fix_available", False):
            click.echo(click.style("\n✓ No fix needed - bbox is optimal!", fg="green"))
            return

        # Check if this is a removal (v2/parquet-geo-only) or addition (v1.x)
        if result.get("needs_bbox_removal", False):
            # V2 or parquet-geo-only: remove bbox column
            bbox_column_name = result.get("bbox_column_name")

            def bbox_fix_func(input_path, output_path, verbose_flag, profile_name):
                return fix_bbox_removal(
                    input_path, output_path, bbox_column_name, verbose_flag, profile_name
                )

            output_path, backup_path = handle_fix_common(
                parquet_file, fix_output, no_backup, bbox_fix_func, verbose, overwrite, profile
            )

            click.echo(click.style("\n✓ Bbox column removed successfully!", fg="green"))
            click.echo(f"Optimized file: {output_path}")
            if backup_path:
                click.echo(f"Backup: {backup_path}")
        else:
            # V1.x: add bbox column/metadata (existing logic)
            needs_column = result.get("needs_bbox_column", False)
            needs_metadata = result.get("needs_bbox_metadata", False)

            def bbox_fix_func(input_path, output_path, verbose_flag, profile_name):
                return fix_bbox_all(
                    input_path,
                    output_path,
                    needs_column,
                    needs_metadata,
                    verbose_flag,
                    profile_name,
                )

            output_path, backup_path = handle_fix_common(
                parquet_file, fix_output, no_backup, bbox_fix_func, verbose, overwrite, profile
            )

            click.echo(click.style("\n✓ Bbox optimized successfully!", fg="green"))
            click.echo(f"Optimized file: {output_path}")
            if backup_path:
                click.echo(f"Backup: {backup_path}")


@check.command(name="row-group")
@click.argument("parquet_file")
@click.option("--verbose", is_flag=True, help="Print detailed diagnostics")
@click.option("--fix", is_flag=True, help="Optimize row group size")
@click.option(
    "--fix-output",
    type=click.Path(),
    help="Output path (default: overwrites with .bak backup)",
)
@click.option(
    "--no-backup",
    is_flag=True,
    help="Skip .bak backup when fixing",
)
@overwrite_option
@profile_option
def check_row_group_cmd(parquet_file, verbose, fix, fix_output, no_backup, overwrite, profile):
    """Check row group size."""
    from geoparquet_io.core.check_fixes import fix_row_groups
    from geoparquet_io.core.check_parquet_structure import check_row_groups

    result = check_row_groups(parquet_file, verbose, return_results=True)

    if fix:
        if not result.get("fix_available", False):
            click.echo(click.style("\n✓ No fix needed - row groups are optimal!", fg="green"))
            return

        click.echo("\nOptimizing row groups...")
        output_path, backup_path = handle_fix_common(
            parquet_file, fix_output, no_backup, fix_row_groups, verbose, overwrite, profile
        )

        click.echo(click.style("\n✓ Row groups optimized successfully!", fg="green"))
        click.echo(f"Optimized file: {output_path}")
        if backup_path:
            click.echo(f"Backup: {backup_path}")


# Convert command
@cli.command()
@click.argument("input_file")
@click.argument("output_file", type=click.Path())
@click.option(
    "--skip-hilbert",
    is_flag=True,
    help="Skip Hilbert spatial ordering (faster but less optimal for spatial queries)",
)
@click.option(
    "--wkt-column",
    help="CSV/TSV: Column name containing WKT geometry (auto-detected if not specified)",
)
@click.option(
    "--lat-column",
    help="CSV/TSV: Column name containing latitude values (requires --lon-column)",
)
@click.option(
    "--lon-column",
    help="CSV/TSV: Column name containing longitude values (requires --lat-column)",
)
@click.option(
    "--delimiter",
    help="CSV/TSV: Delimiter character (auto-detected if not specified). Common: ',' (comma), '\\t' (tab), ';' (semicolon), '|' (pipe)",
)
@click.option(
    "--crs",
    default="EPSG:4326",
    show_default=True,
    help="CSV/TSV: CRS for geometry data (WGS84 assumed for lat/lon)",
)
@click.option(
    "--skip-invalid",
    is_flag=True,
    help="CSV/TSV: Skip rows with invalid geometries instead of failing",
)
@geoparquet_version_option
@verbose_option
@compression_options
@profile_option
def convert(
    input_file,
    output_file,
    skip_hilbert,
    wkt_column,
    lat_column,
    lon_column,
    delimiter,
    crs,
    skip_invalid,
    geoparquet_version,
    verbose,
    compression,
    compression_level,
    profile,
):
    """
    Convert vector formats to optimized GeoParquet.

    Supports Shapefile, GeoJSON, GeoPackage, GDB, CSV/TSV with WKT or lat/lon columns.
    Applies ZSTD compression, bbox metadata, and Hilbert ordering by default.
    """
    convert_to_geoparquet(
        input_file,
        output_file,
        skip_hilbert=skip_hilbert,
        verbose=verbose,
        compression=compression,
        compression_level=compression_level,
        row_group_rows=100000,  # Best practice default
        wkt_column=wkt_column,
        lat_column=lat_column,
        lon_column=lon_column,
        delimiter=delimiter,
        crs=crs,
        skip_invalid=skip_invalid,
        profile=profile,
        geoparquet_version=geoparquet_version,
    )


# Inspect command
@cli.command()
@click.argument("parquet_file")
@click.option("--head", type=int, default=None, help="Show first N rows")
@click.option("--tail", type=int, default=None, help="Show last N rows")
@click.option(
    "--stats", is_flag=True, help="Show column statistics (nulls, min/max, unique counts)"
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON for scripting")
@click.option(
    "--markdown", "markdown_output", is_flag=True, help="Output as Markdown for README files"
)
@profile_option
def inspect(parquet_file, head, tail, stats, json_output, markdown_output, profile):
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

        \b
        # Markdown output for README files
        gpio inspect data.parquet --markdown
    """
    from geoparquet_io.core.common import (
        setup_aws_profile_if_needed,
        validate_profile_for_urls,
    )
    from geoparquet_io.core.duckdb_metadata import get_usable_columns

    # Validate mutually exclusive options
    if head and tail:
        raise click.UsageError("--head and --tail are mutually exclusive")

    if json_output and markdown_output:
        raise click.UsageError("--json and --markdown are mutually exclusive")

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, parquet_file)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, parquet_file)

    try:
        # Extract metadata
        file_info = extract_file_info(parquet_file)
        geo_info = extract_geo_info(parquet_file)

        # Get usable columns using DESCRIBE (handles nested struct wrappers correctly)
        usable_columns = get_usable_columns(parquet_file)
        primary_geom_col = geo_info.get("primary_column")

        # Build column info with geometry marking
        columns_info = [
            {
                "name": col["name"],
                "type": col["type"],
                "is_geometry": col["name"] == primary_geom_col,
            }
            for col in usable_columns
        ]

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
        elif markdown_output:
            output = format_markdown_output(
                file_info, geo_info, columns_info, preview_table, preview_mode, statistics
            )
            click.echo(output)
        else:
            format_terminal_output(
                file_info, geo_info, columns_info, preview_table, preview_mode, statistics
            )

    except Exception as e:
        raise click.ClickException(str(e)) from e


# Extract command
@cli.command()
@click.argument("input_file")
@click.argument("output_file", type=click.Path())
@click.option(
    "--include-cols",
    help="Comma-separated columns to include (geometry and bbox auto-added unless in --exclude-cols)",
)
@click.option(
    "--exclude-cols",
    help="Comma-separated columns to exclude (can be used with --include-cols to exclude geometry/bbox)",
)
@click.option(
    "--bbox",
    help="Bounding box filter: xmin,ymin,xmax,ymax",
)
@click.option(
    "--geometry",
    help="Geometry filter: GeoJSON, WKT, @filepath, or - for stdin",
)
@click.option(
    "--use-first-geometry",
    is_flag=True,
    help="Use first geometry if FeatureCollection contains multiple",
)
@click.option(
    "--where",
    help="DuckDB WHERE clause for filtering rows. Column names with special "
    'characters need double quotes in SQL (e.g., "crop:name"). Shell escaping varies.',
)
@click.option(
    "--limit",
    type=int,
    help="Maximum number of rows to extract.",
)
@click.option(
    "--skip-count",
    is_flag=True,
    help="Skip counting total matching rows before extraction (faster for large datasets).",
)
@output_format_options
@geoparquet_version_option
@dry_run_option
@show_sql_option
@verbose_option
@profile_option
def extract(
    input_file,
    output_file,
    include_cols,
    exclude_cols,
    bbox,
    geometry,
    use_first_geometry,
    where,
    limit,
    skip_count,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    geoparquet_version,
    dry_run,
    show_sql,
    verbose,
    profile,
):
    """
    Extract columns and rows from GeoParquet files.

    Supports column selection, spatial filtering, SQL filtering, and
    multiple input files via glob patterns (merged into single output).

    Column Selection:

      --include-cols: Select only specified columns (geometry and bbox
      columns are always included unless in --exclude-cols)

      --exclude-cols: Select all columns except those specified. Can be
      combined with --include-cols to exclude geometry/bbox columns only.

    Spatial Filtering:

      --bbox: Filter by bounding box. Uses bbox column for fast filtering
      when available, otherwise calculates from geometry.

      --geometry: Filter by intersection with a geometry. Accepts:
        - Inline GeoJSON or WKT
        - @filepath to read from file
        - "-" to read from stdin

    SQL Filtering:

      --where: Apply arbitrary DuckDB WHERE clause

    Examples:

        \b
        # Extract specific columns
        gpio extract data.parquet output.parquet --include-cols id,name,area

        \b
        # Exclude columns
        gpio extract data.parquet output.parquet --exclude-cols internal_id,temp

        \b
        # Filter by bounding box
        gpio extract data.parquet output.parquet --bbox -122.5,37.5,-122.0,38.0

        \b
        # Filter by geometry from file
        gpio extract data.parquet output.parquet --geometry @boundary.geojson

        \b
        # Filter by geometry from stdin
        cat boundary.geojson | gpio extract data.parquet output.parquet --geometry -

        \b
        # SQL WHERE filter
        gpio extract data.parquet output.parquet --where "population > 10000"

        \b
        # WHERE with special column names (double quotes in SQL)
        # Note: macOS may show harmless plist warnings with complex escaping
        gpio extract data.parquet output.parquet --where '"crop:name" = '\''wheat'\'''

        \b
        # Combined filters with glob pattern
        gpio extract "data/*.parquet" output.parquet \\
            --include-cols id,name \\
            --bbox -122.5,37.5,-122.0,38.0 \\
            --where "status = 'active'"

        \b
        # Remote file with spatial filter
        gpio extract s3://bucket/data.parquet output.parquet \\
            --profile my-aws \\
            --bbox -122.5,37.5,-122.0,38.0

        \b
        # Extract first 1000 rows
        gpio extract data.parquet output.parquet --limit 1000
    """
    # Validate mutually exclusive row group options
    if row_group_size and row_group_size_mb:
        raise click.UsageError("--row-group-size and --row-group-size-mb are mutually exclusive")

    # Parse row group size string if provided
    from geoparquet_io.core.common import parse_size_string

    row_group_mb = None
    if row_group_size_mb:
        try:
            size_bytes = parse_size_string(row_group_size_mb)
            row_group_mb = size_bytes / (1024 * 1024)
        except ValueError as e:
            raise click.UsageError(f"Invalid row group size: {e}") from e

    try:
        extract_impl(
            input_parquet=input_file,
            output_parquet=output_file,
            include_cols=include_cols,
            exclude_cols=exclude_cols,
            bbox=bbox,
            geometry=geometry,
            where=where,
            limit=limit,
            skip_count=skip_count,
            use_first_geometry=use_first_geometry,
            dry_run=dry_run,
            show_sql=show_sql,
            verbose=verbose,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            profile=profile,
            geoparquet_version=geoparquet_version,
        )
    except Exception as e:
        raise click.ClickException(str(e)) from e


# Meta command
def _get_primary_geometry_column(parquet_file: str):
    """Get primary geometry column for metadata highlighting."""
    from geoparquet_io.core.common import get_parquet_metadata, parse_geo_metadata

    metadata, _ = get_parquet_metadata(parquet_file, verbose=False)
    geo_meta = parse_geo_metadata(metadata, verbose=False)
    return geo_meta.get("primary_column") if geo_meta else None


def _handle_meta_display(
    parquet_file: str,
    parquet: bool,
    geoparquet: bool,
    parquet_geo: bool,
    row_groups: int,
    json_output: bool,
) -> None:
    """Handle metadata display logic based on flags."""
    from geoparquet_io.core.metadata_utils import (
        format_all_metadata,
        format_geoparquet_metadata,
        format_parquet_geo_metadata,
        format_parquet_metadata_enhanced,
    )

    # Count how many specific flags were set
    specific_flags = sum([parquet, geoparquet, parquet_geo])

    if specific_flags == 0:
        # Show all sections
        format_all_metadata(parquet_file, json_output, row_groups)
    elif specific_flags > 1:
        # Multiple specific flags - show each requested section
        primary_col = _get_primary_geometry_column(parquet_file)

        if parquet:
            format_parquet_metadata_enhanced(parquet_file, json_output, row_groups, primary_col)
        if parquet_geo:
            format_parquet_geo_metadata(parquet_file, json_output, row_groups)
        if geoparquet:
            format_geoparquet_metadata(parquet_file, json_output)
    else:
        # Single specific flag
        if parquet:
            primary_col = _get_primary_geometry_column(parquet_file)
            format_parquet_metadata_enhanced(parquet_file, json_output, row_groups, primary_col)
        elif geoparquet:
            format_geoparquet_metadata(parquet_file, json_output)
        elif parquet_geo:
            format_parquet_geo_metadata(parquet_file, json_output, row_groups)


@cli.command()
@click.argument("parquet_file")
@click.option("--parquet", is_flag=True, help="Show only Parquet file metadata")
@click.option("--geoparquet", is_flag=True, help="Show only GeoParquet metadata from 'geo' key")
@click.option("--parquet-geo", is_flag=True, help="Show only Parquet geospatial metadata")
@click.option(
    "--row-groups", type=int, default=1, help="Number of row groups to display (default: 1)"
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON for scripting")
@profile_option
def meta(parquet_file, parquet, geoparquet, parquet_geo, row_groups, json_output, profile):
    """
    Show comprehensive metadata for a GeoParquet file.

    Displays three types of metadata:
    1. Parquet File Metadata - File structure, schema, row groups, and column statistics
    2. Parquet Geo Metadata - Geospatial metadata from Parquet format specification
    3. GeoParquet Metadata - GeoParquet-specific metadata from 'geo' key

    By default, shows all three sections. Use flags to show specific sections only.

    Examples:

        \b
        # Show all metadata sections
        gpio meta data.parquet

        \b
        # Show only Parquet file metadata
        gpio meta data.parquet --parquet

        \b
        # Show only GeoParquet metadata
        gpio meta data.parquet --geoparquet

        \b
        # Show all row groups instead of just the first
        gpio meta data.parquet --row-groups 10

        \b
        # JSON output for scripting
        gpio meta data.parquet --json
    """
    from geoparquet_io.core.common import setup_aws_profile_if_needed, validate_profile_for_urls

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, parquet_file)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, parquet_file)

    try:
        _handle_meta_display(
            parquet_file, parquet, geoparquet, parquet_geo, row_groups, json_output
        )
    except Exception as e:
        raise click.ClickException(str(e)) from e


# Sort commands group
@cli.group()
@click.pass_context
def sort(ctx):
    """Commands for sorting GeoParquet files."""
    # Ensure logging is set up (in case this group is invoked directly in tests)
    ctx.ensure_object(dict)
    timestamps = ctx.obj.get("timestamps", False)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


@sort.command(name="hilbert")
@click.argument("input_parquet")
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
@click.option("--profile", help="AWS profile name (for S3 remote outputs)")
@output_format_options
@geoparquet_version_option
@verbose_option
def hilbert_order(
    input_parquet,
    output_parquet,
    geometry_column,
    add_bbox,
    profile,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    geoparquet_version,
    verbose,
):
    """
    Reorder a GeoParquet file using Hilbert curve ordering.

    Takes an input GeoParquet file and creates a new file with rows ordered
    by their position along a Hilbert space-filling curve.

    Applies optimal formatting (configurable compression, optimized row groups,
    bbox metadata) while preserving the CRS.

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.
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
            profile,
            geoparquet_version,
        )
    except Exception as e:
        raise click.ClickException(str(e)) from e


@cli.group()
@click.pass_context
def add(ctx):
    """Commands for enhancing GeoParquet files in various ways."""
    # Ensure logging is set up (in case this group is invoked directly in tests)
    ctx.ensure_object(dict)
    timestamps = ctx.obj.get("timestamps", False)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


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
@click.option("--profile", help="AWS profile name (for S3 remote outputs)")
@output_format_options
@geoparquet_version_option
@dry_run_option
@verbose_option
def add_country_codes(
    input_parquet,
    output_parquet,
    dataset,
    levels,
    add_bbox,
    profile,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    geoparquet_version,
    dry_run,
    verbose,
):
    """Add admin division columns via spatial join with remote boundaries datasets.

    Performs spatial intersection to add administrative division columns to your data.

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.

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
    # Remote to remote
    gpio add admin-divisions s3://in.parquet s3://out.parquet --profile my-aws

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
        profile=profile,
        geoparquet_version=geoparquet_version,
    )


@add.command(name="bbox")
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option("--bbox-name", default="bbox", help="Name for the bbox column (default: bbox)")
@click.option(
    "--force",
    is_flag=True,
    help="Replace existing bbox column instead of skipping",
)
@click.option("--profile", help="AWS profile name (for S3 remote outputs)")
@output_format_options
@geoparquet_version_option
@dry_run_option
@verbose_option
def add_bbox(
    input_parquet,
    output_parquet,
    bbox_name,
    force,
    profile,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    geoparquet_version,
    dry_run,
    verbose,
):
    """Add a bbox struct column to a GeoParquet file.

    Creates a new column with bounding box coordinates (xmin, ymin, xmax, ymax)
    for each geometry feature. Bbox covering metadata is automatically added to the
    GeoParquet file (GeoParquet 1.1 spec). The bbox column improves spatial query
    performance.

    If the file already has a bbox column with covering metadata, the command will
    inform you and exit successfully (no action needed). Use --force to replace an
    existing bbox column.

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.

    Examples:

        \b
        # Local to local
        gpio add bbox input.parquet output.parquet

        \b
        # Remote to remote
        gpio add bbox s3://bucket/in.parquet s3://bucket/out.parquet --profile my-aws

        \b
        # Force replace existing bbox
        gpio add bbox input.parquet output.parquet --force
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
        profile,
        force,
        geoparquet_version,
    )


@add.command(name="bbox-metadata")
@click.argument("parquet_file")
@profile_option
@verbose_option
def add_bbox_metadata_cmd(parquet_file, profile, verbose):
    """Add bbox covering metadata for an existing bbox column.

    Use this when you have a file with a bbox column but no covering metadata.
    This modifies the file in-place, preserving all data and file properties.

    If you need to add both the bbox column and metadata, use 'add bbox' instead.
    """
    from geoparquet_io.core.common import setup_aws_profile_if_needed, validate_profile_for_urls

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, parquet_file)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, parquet_file)

    add_bbox_metadata_impl(parquet_file, verbose)


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
@click.option("--profile", help="AWS profile name (for S3 remote outputs)")
@output_format_options
@geoparquet_version_option
@dry_run_option
@verbose_option
def add_h3(
    input_parquet,
    output_parquet,
    h3_name,
    resolution,
    profile,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    geoparquet_version,
    dry_run,
    verbose,
):
    """Add an H3 cell ID column to a GeoParquet file.

    Computes H3 hexagonal cell IDs based on geometry centroids. H3 is a hierarchical
    hexagonal geospatial indexing system that provides consistent cell sizes and shapes
    across the globe.

    The cell ID is stored as a VARCHAR (string) for maximum portability across tools.
    Resolution determines cell size - higher values mean smaller cells with more precision.

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.
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
        profile,
        geoparquet_version,
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
@click.option("--profile", help="AWS profile name (for S3 remote outputs)")
@output_format_options
@geoparquet_version_option
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
    profile,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    geoparquet_version,
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

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.

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
        profile,
        geoparquet_version,
    )


# Partition commands group
@cli.group()
@click.pass_context
def partition(ctx):
    """Commands for partitioning GeoParquet files."""
    # Ensure logging is set up (in case this group is invoked directly in tests)
    ctx.ensure_object(dict)
    timestamps = ctx.obj.get("timestamps", False)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


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
    "Overture levels: country,region.",
)
@partition_options
@verbose_option
@profile_option
@geoparquet_version_option
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
    prefix,
    verbose,
    profile,
    geoparquet_version,
):
    """Partition by administrative boundaries via spatial join with remote datasets.

    This command performs a two-step operation:
    1. Spatially joins input data with remote admin boundaries (GAUL or Overture)
    2. Partitions the enriched data by specified admin levels

    \b
    **Datasets:**
    - gaul: GAUL L2 Admin Boundaries (levels: continent, country, department)
    - overture: Overture Maps Divisions (levels: country, region)

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
        filename_prefix=prefix,
        profile=profile,
        geoparquet_version=geoparquet_version,
    )


@partition.command(name="string")
@click.argument("input_parquet")
@click.argument("output_folder", required=False)
@click.option("--column", required=True, help="Column name to partition by (required)")
@click.option("--chars", type=int, help="Number of characters to use as prefix for partitioning")
@partition_options
@verbose_option
@profile_option
@geoparquet_version_option
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
    prefix,
    verbose,
    profile,
    geoparquet_version,
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
        prefix,
        profile,
        geoparquet_version,
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
@profile_option
@geoparquet_version_option
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
    prefix,
    verbose,
    profile,
    geoparquet_version,
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
        prefix,
        profile,
        geoparquet_version,
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
@profile_option
@geoparquet_version_option
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
    prefix,
    verbose,
    profile,
    geoparquet_version,
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
        prefix,
        profile,
        geoparquet_version,
    )


# STAC commands
def _check_output_stac_item(output_path, output: str, overwrite: bool) -> None:
    """Check if output already exists and is a STAC Item, handle overwrite."""

    from geoparquet_io.core.stac import detect_stac

    if not output_path.exists():
        return

    existing_stac_type = detect_stac(str(output_path))
    if existing_stac_type == "Item":
        if not overwrite:
            raise click.ClickException(
                f"Output file already exists and is a STAC Item: {output}\n"
                "Use --overwrite to overwrite the existing file."
            )
        click.echo(
            click.style(
                f"⚠️  Overwriting existing STAC Item: {output}",
                fg="yellow",
            )
        )


def _check_output_stac_collection(output_path, collection_file, overwrite: bool) -> None:
    """Check if output directory already contains a STAC Collection, handle overwrite."""

    from geoparquet_io.core.stac import detect_stac

    if not collection_file.exists():
        return

    existing_stac_type = detect_stac(str(collection_file))
    if existing_stac_type == "Collection":
        if not overwrite:
            raise click.ClickException(
                f"Output directory already contains a STAC Collection: {collection_file}\n"
                "Use --overwrite to overwrite the existing collection and items."
            )
        click.echo(
            click.style(
                f"⚠️  Overwriting existing STAC Collection: {collection_file}",
                fg="yellow",
            )
        )


def _handle_stac_item(
    input_path,
    output: str,
    bucket: str,
    public_url: str,
    item_id: str,
    overwrite: bool,
    verbose: bool,
) -> None:
    """Handle STAC Item generation for single file."""
    from pathlib import Path

    from geoparquet_io.core.stac import generate_stac_item, write_stac_json

    if verbose:
        click.echo(f"Generating STAC Item for {input_path}")

    output_path = Path(output)
    _check_output_stac_item(output_path, output, overwrite)

    item_dict = generate_stac_item(str(input_path), bucket, public_url, item_id, verbose)
    write_stac_json(item_dict, output, verbose)
    click.echo(f"✓ Created STAC Item: {output}")


def _handle_stac_collection(
    input_path,
    output: str,
    bucket: str,
    public_url: str,
    collection_id: str,
    overwrite: bool,
    verbose: bool,
) -> None:
    """Handle STAC Collection generation for partitioned directory."""
    from pathlib import Path

    from geoparquet_io.core.stac import generate_stac_collection, write_stac_json

    if verbose:
        click.echo(f"Generating STAC Collection for {input_path}")

    # For collections, output can be:
    # 1. A directory path (write collection.json there, items alongside parquet files)
    # 2. None/same as input (write in-place alongside data)
    input_path_obj = Path(input_path)

    # Determine where to write collection.json
    if output:
        output_path = Path(output)
        collection_file = output_path / "collection.json"
    else:
        # Write in-place
        output_path = input_path_obj
        collection_file = output_path / "collection.json"

    _check_output_stac_collection(output_path, collection_file, overwrite)

    collection_dict, item_dicts = generate_stac_collection(
        str(input_path), bucket, public_url, collection_id, verbose
    )

    # Create output directory if needed
    output_path.mkdir(parents=True, exist_ok=True)

    # Write collection
    write_stac_json(collection_dict, str(collection_file), verbose)

    # Write items alongside their parquet files in the input directory
    # This follows STAC best practice of co-locating metadata with data
    for item_dict in item_dicts:
        item_id = item_dict["id"]
        # Find the parquet file in input directory
        parquet_file = input_path_obj / f"{item_id}.parquet"
        if not parquet_file.exists():
            # Check for hive-style partitions
            hive_partitions = list(input_path_obj.glob(f"*/{item_id}.parquet"))
            if hive_partitions:
                parquet_file = hive_partitions[0]

        # Write item JSON next to parquet file
        item_file = parquet_file.parent / f"{item_id}.json"

        # Check if we need to overwrite
        if item_file.exists() and not overwrite:
            from geoparquet_io.core.stac import detect_stac

            if detect_stac(str(item_file)):
                raise click.ClickException(
                    f"STAC Item already exists: {item_file}\nUse --overwrite to replace it."
                )

        write_stac_json(item_dict, str(item_file), verbose)

    click.echo(f"✓ Created STAC Collection: {collection_file}")
    click.echo(f"✓ Created {len(item_dicts)} STAC Items alongside data files in {input_path}")


@cli.command()
@click.argument("input")
@click.argument("output", type=click.Path())
@click.option(
    "--bucket",
    required=True,
    help="S3 bucket prefix for asset hrefs (e.g., s3://source.coop/org/dataset/)",
)
@click.option(
    "--public-url",
    help="Optional public HTTPS URL for assets (e.g., https://data.source.coop/org/dataset/)",
)
@click.option("--collection-id", help="Custom collection ID (for partitioned datasets)")
@click.option("--item-id", help="Custom item ID (for single files)")
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite existing STAC files in output location",
)
@verbose_option
def stac(input, output, bucket, public_url, collection_id, item_id, overwrite, verbose):
    """
    Generate STAC Item or Collection from GeoParquet file(s).

    Single file → STAC Item JSON

    Partitioned directory → STAC Collection + Items (co-located with data)

    For partitioned datasets, Items are written alongside their parquet files
    following STAC best practices. collection.json is written to OUTPUT.

    Automatically detects PMTiles overview files and includes them as assets.

    Examples:

      \b
      # Single file
      gpio stac input.parquet output.json --bucket s3://my-bucket/roads/

      \b
      # Partitioned dataset - Items written next to parquet files
      gpio stac partitions/ . --bucket s3://my-bucket/roads/

      \b
      # With public URL mapping
      gpio stac data.parquet output.json \\
        --bucket s3://my-bucket/roads/ \\
        --public-url https://data.example.com/roads/
    """
    from pathlib import Path

    from geoparquet_io.core.stac import (
        detect_stac,
    )

    input_path = Path(input)

    # Check if input is already a STAC file/collection
    stac_type = detect_stac(str(input_path))
    if stac_type:
        raise click.ClickException(
            f"Input is already a STAC {stac_type}: {input}\n"
            f"Use 'gpio check stac {input}' to validate it, or provide a GeoParquet file/directory."
        )

    if input_path.is_file():
        _handle_stac_item(input_path, output, bucket, public_url, item_id, overwrite, verbose)
    elif input_path.is_dir():
        _handle_stac_collection(
            input_path, output, bucket, public_url, collection_id, overwrite, verbose
        )
    else:
        raise click.BadParameter(f"Input must be file or directory: {input}")


@check.command(name="stac")
@click.argument("stac_file")
@profile_option
@verbose_option
def check_stac_cmd(stac_file, profile, verbose):
    """
    Validate STAC Item or Collection JSON.

    Checks:

      • STAC spec compliance

      • Required fields

      • Asset href resolution (local files)

      • Best practices

    Example:

      \b
      gpio check stac output.json
    """
    from geoparquet_io.core.common import setup_aws_profile_if_needed, validate_profile_for_urls
    from geoparquet_io.core.stac_check import check_stac

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, stac_file)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, stac_file)

    check_stac(stac_file, verbose)


# Benchmark command
@cli.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.option(
    "--iterations",
    default=3,
    type=int,
    help="Number of iterations per converter (default: 3)",
)
@click.option(
    "--converters",
    help="Comma-separated list of converters to run (default: all available)",
)
@click.option(
    "--output-json",
    type=click.Path(),
    help="Save results to JSON file",
)
@click.option(
    "--keep-output",
    type=click.Path(),
    help="Directory to save converted files (default: temp dir, cleaned up)",
)
@click.option(
    "--warmup/--no-warmup",
    default=True,
    help="Run warmup iteration before timing (default: enabled)",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format (default: table)",
)
@click.option(
    "--quiet",
    is_flag=True,
    help="Suppress progress output, show only results",
)
def benchmark(
    input_file,
    iterations,
    converters,
    output_json,
    keep_output,
    warmup,
    output_format,
    quiet,
):
    """
    Benchmark GeoParquet conversion performance.

    Tests different conversion methods (DuckDB, GeoPandas, GDAL) on an input
    geospatial file and reports time and memory usage.

    Available converters:

      \b
      - duckdb: DuckDB spatial extension (always available)
      - geopandas_fiona: GeoPandas with Fiona engine
      - geopandas_pyogrio: GeoPandas with PyOGRIO engine
      - gdal_ogr2ogr: GDAL ogr2ogr CLI

    Examples:

      \b
      # Basic benchmark with all available converters
      gpio benchmark input.geojson

      \b
      # Run specific converters with more iterations
      gpio benchmark input.shp --converters duckdb,geopandas_pyogrio --iterations 5

      \b
      # Save results to JSON and keep converted files
      gpio benchmark input.gpkg --output-json results.json --keep-output ./output

      \b
      # JSON output format
      gpio benchmark input.geojson --format json
    """
    from geoparquet_io.core.benchmark import run_benchmark

    # Parse converters string to list
    converter_list = None
    if converters:
        converter_list = [c.strip() for c in converters.split(",")]

    run_benchmark(
        input_file=input_file,
        iterations=iterations,
        converters=converter_list,
        output_json=output_json,
        keep_output=keep_output,
        warmup=warmup,
        output_format=output_format,
        quiet=quiet,
    )


@cli.command()
@click.argument("source", type=click.Path(exists=True, path_type=Path))
@click.argument("destination", type=str)
@click.option("--profile", help="AWS profile name (S3 only)")
@click.option("--pattern", help="Glob pattern for filtering files (e.g., '*.parquet', '**/*.json')")
@click.option(
    "--max-files", default=4, show_default=True, help="Max parallel file uploads for directories"
)
@click.option(
    "--chunk-concurrency",
    default=12,
    show_default=True,
    help="Max concurrent chunks per file",
)
@click.option("--chunk-size", type=int, help="Chunk size in bytes for multipart uploads")
@click.option("--fail-fast", is_flag=True, help="Stop immediately on first error")
@dry_run_option
def upload(
    source,
    destination,
    profile,
    pattern,
    max_files,
    chunk_concurrency,
    chunk_size,
    fail_fast,
    dry_run,
):
    """Upload file or directory to object storage.

    Supports S3, GCS, Azure, and HTTP destinations. Automatically handles
    multipart uploads and preserves directory structure.

    \b
    Examples:
      # Single file to S3
      gpio upload data.parquet s3://bucket/path/data.parquet --profile source-coop

      \b
      # Directory to GCS (preserves structure, uploads files in parallel)
      gpio upload output/ gs://bucket/dataset/

      \b
      # Only parquet files with increased parallelism
      gpio upload output/ s3://bucket/dataset/ --pattern "*.parquet" --max-files 8

      \b
      # Stop on first error instead of continuing
      gpio upload output/ s3://bucket/dataset/ --fail-fast
    """
    try:
        upload_impl(
            source=source,
            destination=destination,
            profile=profile,
            pattern=pattern,
            max_files=max_files,
            chunk_concurrency=chunk_concurrency,
            chunk_size=chunk_size,
            fail_fast=fail_fast,
            dry_run=dry_run,
        )
    except ValueError as e:
        raise click.ClickException(str(e)) from e


if __name__ == "__main__":
    cli()
