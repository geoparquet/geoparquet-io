"""``gpio inspect`` - show GeoParquet metadata, previews and statistics.

Registered on the root ``gpio`` group by ``cli/main.py`` via
``cli.add_command(inspect)``. This module must not import ``cli.main``: the
dependency runs one way, ``main`` -> ``commands`` -> ``_shared``/``decorators``.
"""

import os

import click

from geoparquet_io.cli._shared import _activate_s3, create_default_group
from geoparquet_io.cli.decorators import GlobAwareCommand, verbose_option
from geoparquet_io.core.inspect import (
    display_metadata,
    format_preview_output,
    format_stats_output,
    format_summary_output,
)
from geoparquet_io.core.inspect import (
    inspect_preview as _inspect_preview_core,
)
from geoparquet_io.core.inspect import (
    inspect_stats as _inspect_stats_core,
)
from geoparquet_io.core.inspect import (
    inspect_summary as _inspect_summary_core,
)
from geoparquet_io.core.logging_config import setup_cli_logging

# InspectDefaultGroup: defaults to 'summary' when no subcommand is provided
InspectDefaultGroup = create_default_group(
    "summary",
    "Custom Group that runs 'summary' when no subcommand is provided.",
)


# Inspect command group
@click.group(cls=InspectDefaultGroup)
@click.pass_context
def inspect(ctx):
    """Inspect GeoParquet files and show metadata, previews, or statistics.

    By default shows a quick metadata summary. Use subcommands for specific operations.

    Examples:

        \b
        # Quick metadata summary (default)
        gpio inspect data.parquet

        \b
        # Preview first 10 rows
        gpio inspect head data.parquet

        \b
        # Preview first 20 rows
        gpio inspect head data.parquet 20

        \b
        # Preview last 5 rows
        gpio inspect tail data.parquet 5

        \b
        # Show column statistics
        gpio inspect stats data.parquet

        \b
        # Comprehensive metadata
        gpio inspect meta data.parquet

        \b
        # GeoParquet 'geo' key metadata only
        gpio inspect meta data.parquet --geo
    """
    ctx.ensure_object(dict)
    timestamps = ctx.obj.get("timestamps", False)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


def _validate_parquet_input(file_path: str) -> None:
    """Validate that input file appears to be a Parquet file.

    Args:
        file_path: Path to the file (local or remote)

    Raises:
        click.ClickException: If file doesn't have .parquet extension
    """
    # Skip validation for directories (partition paths)
    if os.path.isdir(file_path):
        return

    # Extract filename from path (handles both local and remote URLs)
    if "://" in file_path:
        # Use urllib.parse to properly handle URLs with query strings (e.g., presigned URLs)
        from urllib.parse import urlparse

        parsed = urlparse(file_path)
        filename = os.path.basename(parsed.path)
    else:
        filename = os.path.basename(file_path)

    # Check extension (case-insensitive)
    _, ext = os.path.splitext(filename)
    if ext.lower() != ".parquet":
        raise click.ClickException(
            f"The 'inspect' command only works with Parquet files, "
            f"but got '{filename}' with extension '{ext or '(none)'}'. "
            f"Use 'gpio convert' to convert other formats to GeoParquet first."
        )


def _friendly_parquet_error(error: Exception, file_path: str) -> click.ClickException:
    """Convert low-level Parquet errors to user-friendly messages."""
    error_str = str(error)
    filename = os.path.basename(file_path)

    if "magic bytes" in error_str.lower() or "not a parquet file" in error_str.lower():
        return click.ClickException(
            f"The file '{filename}' has a .parquet extension but is not a valid "
            f"Parquet file. It may be a different format (CSV, JSON, etc.) that was "
            f"incorrectly named. Check the file contents or use 'gpio convert' to "
            f"create a proper GeoParquet file."
        )
    return click.ClickException(error_str)


def _inspect_summary_impl(parquet_file, json_output, markdown_output, check_all_files):
    """CLI wrapper for inspect summary - delegates to core function."""
    if json_output and markdown_output:
        raise click.UsageError("--json and --markdown are mutually exclusive")

    _validate_parquet_input(parquet_file)

    try:
        result = _inspect_summary_core(parquet_file, check_all_files)

        # Show partition notice if applicable
        if result.get("partition_notice"):
            click.echo(click.style(result["partition_notice"], fg="cyan"))
            click.echo()

        output = format_summary_output(result, json_output, markdown_output)
        if output:
            click.echo(output)

    except ValueError as e:
        raise click.ClickException(str(e)) from e
    except Exception as e:
        raise _friendly_parquet_error(e, parquet_file) from e


def _inspect_preview_impl(
    parquet_file, count, mode, json_output, markdown_output, max_columns=None, no_truncate=False
):
    """CLI wrapper for inspect head/tail - delegates to core function."""
    if json_output and markdown_output:
        raise click.UsageError("--json and --markdown are mutually exclusive")
    if no_truncate and max_columns is not None:
        click.echo("Note: --no-truncate overrides --max-columns; showing all columns.")
        max_columns = None

    _validate_parquet_input(parquet_file)

    try:
        result = _inspect_preview_core(parquet_file, count, mode)

        # Show partition notice if applicable
        if result.get("partition_notice"):
            click.echo(click.style(result["partition_notice"], fg="cyan"))
            click.echo()

        output = format_preview_output(
            result, json_output, markdown_output, max_columns=max_columns, no_truncate=no_truncate
        )
        if output:
            click.echo(output)

    except Exception as e:
        raise _friendly_parquet_error(e, parquet_file) from e


def _inspect_stats_impl(parquet_file, json_output, markdown_output):
    """CLI wrapper for inspect stats - delegates to core function."""
    if json_output and markdown_output:
        raise click.UsageError("--json and --markdown are mutually exclusive")

    _validate_parquet_input(parquet_file)

    try:
        result = _inspect_stats_core(parquet_file)

        # Show partition notice if applicable
        if result.get("partition_notice"):
            click.echo(click.style(result["partition_notice"], fg="cyan"))
            click.echo()

        output = format_stats_output(result, json_output, markdown_output)
        if output:
            click.echo(output)

    except Exception as e:
        raise _friendly_parquet_error(e, parquet_file) from e


# Meta command - delegates to core.inspect.display_metadata
def _handle_meta_display(
    parquet_file: str,
    parquet: bool,
    geoparquet: bool,
    parquet_geo: bool,
    row_groups: int,
    json_output: bool,
    geo_stats: bool = False,
) -> None:
    """CLI wrapper for metadata display - delegates to core function."""
    display_metadata(
        parquet_file, parquet, geoparquet, parquet_geo, row_groups, json_output, geo_stats
    )


@inspect.command(name="summary", cls=GlobAwareCommand)
@click.argument("parquet_file")
@click.option("--json", "json_output", is_flag=True, help="Output as JSON for scripting")
@click.option(
    "--markdown", "markdown_output", is_flag=True, help="Output as Markdown for README files"
)
@click.option(
    "--check-all",
    "check_all_files",
    is_flag=True,
    help="For partitioned data: aggregate info from all files",
)
@verbose_option
@click.pass_context
def inspect_summary(ctx, parquet_file, json_output, markdown_output, check_all_files, verbose):
    """Show quick metadata summary (default).

    Displays file size, row count, columns, geometry type, CRS, and bounding box.
    """
    with _activate_s3(ctx):
        _inspect_summary_impl(parquet_file, json_output, markdown_output, check_all_files)


@inspect.command(name="head", cls=GlobAwareCommand)
@click.argument("parquet_file")
@click.argument("count", type=int, default=10, required=False)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON for scripting")
@click.option(
    "--markdown", "markdown_output", is_flag=True, help="Output as Markdown for README files"
)
@click.option(
    "--max-columns",
    type=click.IntRange(min=1),
    default=None,
    help="Maximum number of columns to display (default: fit terminal width)",
)
@click.option(
    "--no-truncate",
    is_flag=True,
    help="Show all columns and full values (disable fit-to-width)",
)
@verbose_option
@click.pass_context
def inspect_head(
    ctx, parquet_file, count, json_output, markdown_output, max_columns, no_truncate, verbose
):
    """Show first N rows of data (default: 10).

    Examples:

        \b
        gpio inspect head data.parquet        # First 10 rows
        gpio inspect head data.parquet 20     # First 20 rows
    """
    with _activate_s3(ctx):
        _inspect_preview_impl(
            parquet_file,
            count,
            "head",
            json_output,
            markdown_output,
            max_columns=max_columns,
            no_truncate=no_truncate,
        )


@inspect.command(name="tail", cls=GlobAwareCommand)
@click.argument("parquet_file")
@click.argument("count", type=int, default=10, required=False)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON for scripting")
@click.option(
    "--markdown", "markdown_output", is_flag=True, help="Output as Markdown for README files"
)
@click.option(
    "--max-columns",
    type=click.IntRange(min=1),
    default=None,
    help="Maximum number of columns to display (default: fit terminal width)",
)
@click.option(
    "--no-truncate",
    is_flag=True,
    help="Show all columns and full values (disable fit-to-width)",
)
@verbose_option
@click.pass_context
def inspect_tail(
    ctx, parquet_file, count, json_output, markdown_output, max_columns, no_truncate, verbose
):
    """Show last N rows of data (default: 10).

    Examples:

        \b
        gpio inspect tail data.parquet        # Last 10 rows
        gpio inspect tail data.parquet 5      # Last 5 rows
    """
    with _activate_s3(ctx):
        _inspect_preview_impl(
            parquet_file,
            count,
            "tail",
            json_output,
            markdown_output,
            max_columns=max_columns,
            no_truncate=no_truncate,
        )


@inspect.command(name="stats", cls=GlobAwareCommand)
@click.argument("parquet_file")
@click.option("--json", "json_output", is_flag=True, help="Output as JSON for scripting")
@click.option(
    "--markdown", "markdown_output", is_flag=True, help="Output as Markdown for README files"
)
@verbose_option
@click.pass_context
def inspect_stats(ctx, parquet_file, json_output, markdown_output, verbose):
    """Show column statistics (nulls, min/max, unique counts)."""
    with _activate_s3(ctx):
        _inspect_stats_impl(parquet_file, json_output, markdown_output)


@inspect.command(name="meta", cls=GlobAwareCommand)
@click.argument("parquet_file")
@click.option("--geo", "meta_geoparquet", is_flag=True, help="Show only GeoParquet 'geo' metadata")
@click.option("--parquet", "meta_parquet", is_flag=True, help="Show only Parquet file metadata")
@click.option(
    "--parquet-geo", "meta_parquet_geo", is_flag=True, help="Show only Parquet geospatial metadata"
)
@click.option(
    "--row-groups",
    "meta_row_groups",
    type=int,
    default=None,
    help="Number of row groups to display (default: 1)",
)
@click.option(
    "--geo-stats",
    "meta_geo_stats",
    is_flag=True,
    help="Show per-row-group geo_bbox statistics",
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON for scripting")
@verbose_option
@click.pass_context
def inspect_meta(
    ctx,
    parquet_file,
    meta_geoparquet,
    meta_parquet,
    meta_parquet_geo,
    meta_row_groups,
    meta_geo_stats,
    json_output,
    verbose,
):
    """Show comprehensive metadata (Parquet, GeoParquet, row groups).

    Examples:

        \b
        gpio inspect meta data.parquet                # All metadata
        gpio inspect meta data.parquet --geo          # GeoParquet 'geo' key only
        gpio inspect meta data.parquet --parquet      # Parquet file metadata only
        gpio inspect meta data.parquet --row-groups 5 # Show 5 row groups
        gpio inspect meta data.parquet --geo-stats    # Per-row-group bbox stats
    """
    _validate_parquet_input(parquet_file)

    with _activate_s3(ctx):
        try:
            _handle_meta_display(
                parquet_file,
                meta_parquet,
                meta_geoparquet,
                meta_parquet_geo,
                meta_row_groups,
                json_output,
                meta_geo_stats,
            )
        except Exception as e:
            raise _friendly_parquet_error(e, parquet_file) from e


@inspect.command(name="layers", cls=GlobAwareCommand)
@click.argument("input_file")
@click.option("--json", "json_output", is_flag=True, help="Output as JSON for scripting")
@verbose_option
@click.pass_context
def inspect_layers(ctx, input_file, json_output, verbose):
    """List layers in multi-layer formats (GeoPackage, FileGDB).

    Returns layer names for files with 2+ layers. Single-layer files
    (GeoJSON, Shapefile, Parquet) or multi-layer files with only 1 layer
    return nothing.

    \b
    Examples:
        gpio inspect layers multi.gpkg          # List layers in GeoPackage
        gpio inspect layers data.gdb            # List layers in FileGDB
        gpio inspect layers multi.gpkg --json   # JSON output for scripting
    """
    import json

    from geoparquet_io.core.layers import list_layers

    with _activate_s3(ctx):
        try:
            layers = list_layers(input_file)
        except FileNotFoundError as e:
            raise click.ClickException(str(e)) from e
        except ValueError as e:
            raise click.ClickException(str(e)) from e
        except RuntimeError as e:
            raise click.ClickException(str(e)) from e

    if layers is None:
        if json_output:
            click.echo(json.dumps({"layers": None, "count": 0}))
        else:
            click.echo("No layers found (single-layer format or file with 0-1 layers)")
        return

    if json_output:
        click.echo(json.dumps({"layers": layers, "count": len(layers)}))
    else:
        click.echo(f"Found {len(layers)} layers:")
        for layer in layers:
            click.echo(f"  - {layer}")
