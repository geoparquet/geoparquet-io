import os
import sys
from contextlib import contextmanager
from importlib.metadata import entry_points
from pathlib import Path

import click
import duckdb
from click.core import ParameterSource
from click_plugins import with_plugins

from geoparquet_io.cli.decorators import (
    GlobAwareCommand,
    SingleFileCommand,
    allow_schema_diff_option,
    any_extension_option,
    aws_profile_option,
    bucket_point_options,
    check_partition_options,
    compression_options,
    dry_run_option,
    geoparquet_version_option,
    grid_aggregate_options,
    handle_directory_sub_partition,
    handle_geoparquet_errors,
    linearize_curves_options,
    metric_nodata_option,
    output_format_options,
    overwrite_option,
    parse_row_group_options,
    partition_input_options,
    partition_options,
    partition_options_base,
    repair_geometry_option,
    row_group_options,
    show_sql_option,
    verbose_option,
    where_option,
    write_strategy_option,
)
from geoparquet_io.cli.fix_helpers import handle_fix_common
from geoparquet_io.core.add.a5 import add_a5_column as add_a5_column_impl
from geoparquet_io.core.add.bbox import add_bbox_column as add_bbox_column_impl
from geoparquet_io.core.add.bbox_metadata import add_bbox_metadata as add_bbox_metadata_impl
from geoparquet_io.core.add.h3 import add_h3_column as add_h3_column_impl
from geoparquet_io.core.add.kdtree import add_kdtree_column as add_kdtree_column_impl
from geoparquet_io.core.add.quadkey import add_quadkey_column as add_quadkey_column_impl
from geoparquet_io.core.add.s2 import add_s2_column as add_s2_column_impl
from geoparquet_io.core.check_parquet_structure import CheckProfile
from geoparquet_io.core.check_parquet_structure import check_all as check_structure_impl
from geoparquet_io.core.check_spatial_order import check_spatial_order as check_spatial_impl
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.exceptions import InvalidParameterError, ValidationError
from geoparquet_io.core.extract import extract as extract_impl
from geoparquet_io.core.file_utils import validate_parquet_extension
from geoparquet_io.core.hilbert_order import hilbert_order as hilbert_impl
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
from geoparquet_io.core.logging_config import configure_verbose, setup_cli_logging
from geoparquet_io.core.parquet_writer import DEFAULT_SORT_ROW_GROUP_ROWS
from geoparquet_io.core.partition.admin_hierarchical import (
    partition_by_admin_hierarchical as partition_admin_hierarchical_impl,
)
from geoparquet_io.core.partition.by_a5 import partition_by_a5 as partition_by_a5_impl
from geoparquet_io.core.partition.by_h3 import partition_by_h3 as partition_by_h3_impl
from geoparquet_io.core.partition.by_kdtree import partition_by_kdtree as partition_by_kdtree_impl
from geoparquet_io.core.partition.by_quadkey import (
    partition_by_quadkey as partition_by_quadkey_impl,
)
from geoparquet_io.core.partition.by_s2 import partition_by_s2 as partition_by_s2_impl
from geoparquet_io.core.partition.by_string import (
    partition_by_string as partition_by_string_impl,
)
from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5 as aggregate_by_a5_impl
from geoparquet_io.core.process.aggregate.by_admin import (
    aggregate_by_admin as aggregate_by_admin_impl,
)
from geoparquet_io.core.process.aggregate.by_h3 import aggregate_by_h3 as aggregate_by_h3_impl
from geoparquet_io.core.process.overview import create_overviews as create_overviews_impl
from geoparquet_io.core.reproject import reproject as reproject_core
from geoparquet_io.core.sort_by_column import sort_by_column as sort_by_column_impl
from geoparquet_io.core.sort_quadkey import sort_by_quadkey as sort_by_quadkey_impl
from geoparquet_io.core.str_order import str_order as str_impl
from geoparquet_io.core.upload import check_credentials
from geoparquet_io.core.upload import upload as upload_impl
from geoparquet_io.core.wfs import DEFAULT_WFS_PAGE_SIZE


class OptionalIntCommand(GlobAwareCommand):
    """Custom Command that supports options with optional integer values.

    Inherits from GlobAwareCommand to also detect shell-expanded glob patterns
    and provide helpful error messages.

    Options listed in optional_int_options can be used as flags (defaulting to 10)
    or with an explicit integer value. For example:
        --head           -> uses default value of 10
        --head 5         -> uses value 5
        (no --head)      -> uses None
    """

    # Options that support optional integer values and their defaults
    optional_int_options = {"--head": 10, "--tail": 10}

    def make_context(self, info_name, args, parent=None, **extra):
        """Preprocess args to insert default values for optional int options."""
        args = list(args)  # Make a mutable copy
        for opt, default_val in self.optional_int_options.items():
            if opt in args:
                idx = args.index(opt)
                # Check if next arg exists and looks like an integer
                if idx + 1 < len(args):
                    next_arg = args[idx + 1]
                    # If next arg starts with - (another option) or doesn't look like int
                    if next_arg.startswith("-") or not next_arg.lstrip("-").isdigit():
                        args.insert(idx + 1, str(default_val))
                else:
                    # Option at end of args
                    args.insert(idx + 1, str(default_val))
        return super().make_context(info_name, args, parent=parent, **extra)


@contextmanager
def _activate_s3(ctx, aws_profile=None, s3_endpoint=None, s3_region=None, s3_no_ssl=False):
    """Resolve and activate S3 config from ctx + per-command overrides.

    Properly saves/restores AWS_PROFILE env var to avoid credential leaks.
    """
    from geoparquet_io.core.duckdb_utils import s3_config_scope
    from geoparquet_io.core.remote import resolve_s3_config

    config = resolve_s3_config(
        s3_endpoint=s3_endpoint or ctx.obj.get("s3_endpoint"),
        s3_region=s3_region or ctx.obj.get("s3_region"),
        s3_no_ssl=s3_no_ssl or ctx.obj.get("s3_no_ssl", False),
        aws_profile=aws_profile or ctx.obj.get("aws_profile"),
    )
    previous_profile = os.environ.get("AWS_PROFILE")
    try:
        if config["profile"]:
            os.environ["AWS_PROFILE"] = config["profile"]
        with s3_config_scope(config):
            yield config
    finally:
        if previous_profile is None:
            os.environ.pop("AWS_PROFILE", None)
        else:
            os.environ["AWS_PROFILE"] = previous_profile


@with_plugins(entry_points(group="gpio.plugins"))
@click.group()
@click.version_option(prog_name="geoparquet-io")
@click.option("--timestamps", is_flag=True, help="Show timestamps in output messages")
@click.option(
    "--s3-endpoint",
    default=None,
    help="Custom S3-compatible endpoint (e.g., 'minio.example.com:9000')",
)
@click.option(
    "--s3-region",
    default=None,
    help="S3 region for custom endpoints",
)
@click.option(
    "--s3-no-ssl",
    is_flag=True,
    default=False,
    help="Disable SSL for S3 endpoint (use HTTP instead of HTTPS)",
)
@click.option(
    "--aws-profile",
    default=None,
    help="AWS profile name for S3 operations",
)
@click.pass_context
def cli(ctx, timestamps, s3_endpoint, s3_region, s3_no_ssl, aws_profile):
    """Fast I/O and transformation tools for GeoParquet files."""
    # Ensure stdout/stderr can emit UTF-8 even on Windows, where the default
    # codec (cp1252) raises UnicodeEncodeError on non-ASCII GeoJSON content.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8")
            except Exception:
                pass
    ctx.ensure_object(dict)
    ctx.obj["timestamps"] = timestamps
    ctx.obj["s3_endpoint"] = s3_endpoint
    ctx.obj["s3_region"] = s3_region
    ctx.obj["s3_no_ssl"] = s3_no_ssl
    ctx.obj["aws_profile"] = aws_profile
    # Setup logging for CLI output (default level INFO, verbose commands will set DEBUG)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


# Check commands group - use custom command class for default subcommand
def create_default_group(default_subcommand: str, description: str) -> type:
    """Factory to create a click.Group subclass that defaults to a specific subcommand.

    Args:
        default_subcommand: The subcommand to invoke when none is provided
        description: The docstring for the generated class

    Returns:
        A click.Group subclass with the configured default behavior
    """

    class _DefaultGroup(click.Group):
        def parse_args(self, ctx, args):
            # Handle --help for group
            if "--help" in args and (not args or args[0] not in self.commands):
                return super().parse_args(ctx, [a for a in args if a != "--help"] + ["--help"])

            # If first arg is a known subcommand, use it
            if args and not args[0].startswith("-") and args[0] in self.commands:
                return super().parse_args(ctx, args)

            # Default to configured subcommand
            return super().parse_args(ctx, [default_subcommand] + args)

    _DefaultGroup.__doc__ = description
    return _DefaultGroup


# Create default group classes using the factory
DefaultGroup = create_default_group(
    "all",
    "Custom Group that invokes a default command when no subcommand is provided.",
)


class ConvertDefaultGroup(click.Group):
    """
    Custom Group for convert command with format auto-detection.

    Auto-detects output format from file extension when no subcommand is specified.
    Falls back to 'geoparquet' if extension is not recognized.

    Supported auto-detection:
    - .parquet -> geoparquet
    - .gpkg -> geopackage
    - .fgb -> flatgeobuf
    - .csv -> csv
    - .shp -> shapefile
    - .geojson, .json -> geojson

    Examples:
    - gpio convert input.parquet output.gpkg -> auto-detects 'geopackage'
    - gpio convert input.parquet output.fgb -> auto-detects 'flatgeobuf'
    - gpio convert geopackage input.parquet output.gpkg -> explicit subcommand
    """

    # Extension to subcommand mapping
    EXTENSION_TO_SUBCOMMAND = {
        ".parquet": "geoparquet",
        ".gpkg": "geopackage",
        ".fgb": "flatgeobuf",
        ".csv": "csv",
        ".shp": "shapefile",
        ".geojson": "geojson",
        ".json": "geojson",
    }

    def parse_args(self, ctx, args):
        """Parse args with format auto-detection from output file extension."""
        # Handle --help for group
        if "--help" in args and (not args or args[0] not in self.commands):
            return super().parse_args(ctx, [a for a in args if a != "--help"] + ["--help"])

        # If first arg is a known subcommand, use it directly
        if args and not args[0].startswith("-") and args[0] in self.commands:
            return super().parse_args(ctx, args)

        # Auto-detect format from output file extension
        subcommand = self._detect_format_from_args(args)
        return super().parse_args(ctx, [subcommand] + args)

    def _detect_format_from_args(self, args):
        """Extract output file from args and detect format from extension.

        Scans backwards through args to find the output file (last positional argument).
        This approach correctly handles options with values interspersed with positional args.
        """
        from pathlib import Path

        # Scan backwards to find first argument with a recognized extension
        # Skip tokens starting with "-" to avoid treating option values as file paths
        for arg in reversed(args):
            if arg.startswith("-"):
                continue

            ext = Path(arg).suffix.lower()
            if ext in self.EXTENSION_TO_SUBCOMMAND:
                return self.EXTENSION_TO_SUBCOMMAND[ext]

        return "geoparquet"  # Default fallback


ExtractDefaultGroup = create_default_group(
    "geoparquet",
    """Custom Group that invokes 'geoparquet' when no subcommand is provided.

This allows backwards compatibility:
- gpio extract input.parquet output.parquet  -> invokes geoparquet
- gpio extract geoparquet input.parquet output.parquet -> explicit
- gpio extract bigquery project.dataset.table output.parquet -> subcommand""",
)


# InspectDefaultGroup: defaults to 'summary' when no subcommand is provided
InspectDefaultGroup = create_default_group(
    "summary",
    "Custom Group that runs 'summary' when no subcommand is provided.",
)


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


class MultiFileCheckRunner:
    """Helper for running checks on multiple files with progress tracking and summary."""

    def __init__(self, files: list[str], verbose: bool = False, max_issues_shown: int = 3):
        self.files = files
        self.verbose = verbose
        self.max_issues_shown = max_issues_shown
        self.passed = 0
        self.warnings = 0
        self.failed = 0
        self.issues: list[tuple[str, str, str]] = []  # (file, level, message)
        self.current_index = 0
        self.is_multi_file = len(files) > 1

    def _update_progress(self):
        """Print progress line (overwrites previous line in non-verbose mode)."""
        if not self.is_multi_file or self.verbose:
            return
        total = len(self.files)
        msg = f"Checking files... {self.current_index}/{total} ({self.passed} passed)"
        if self.warnings:
            msg += f", {self.warnings} warnings"
        if self.failed:
            msg += f", {self.failed} failed"
        click.echo(f"\r{msg}", nl=False)

    def _record_issue(self, file_path: str, level: str, message: str):
        """Record an issue and print it if under the limit."""
        self.issues.append((file_path, level, message))
        if not self.verbose and len(self.issues) <= self.max_issues_shown:
            # Clear progress line and print issue
            click.echo("\r" + " " * 80 + "\r", nl=False)
            color = "yellow" if level == "warning" else "red"
            filename = Path(file_path).name
            click.echo(click.style(f"  {level.upper()}: {filename} - {message}", fg=color))

    def start_file(self, file_path: str):
        """Called before checking each file."""
        self.current_index += 1
        self._update_progress()
        if self.verbose and self.is_multi_file:
            click.echo(click.style(f"\n{'=' * 60}", fg="bright_black"))
            click.echo(
                click.style(f"File {self.current_index}/{len(self.files)}: {file_path}", fg="cyan")
            )
            click.echo(click.style(f"{'=' * 60}", fg="bright_black"))

    def record_result(self, file_path: str, result: dict):
        """Record the result of checking a file."""
        if result.get("passed", True):
            self.passed += 1
        else:
            # Determine if it's a warning or failure
            issues = result.get("issues", [])
            has_error_flag = bool(result.get("failed", False))
            has_error_issues = any("❌" in str(i) for i in issues)
            has_error = has_error_flag or has_error_issues
            if (
                has_error
                or result.get("size_status") == "poor"
                or result.get("row_status") == "poor"
            ):
                self.failed += 1
                for issue in issues:
                    self._record_issue(file_path, "error", issue)
            else:
                self.warnings += 1
                for issue in issues:
                    self._record_issue(file_path, "warning", issue)
        self._update_progress()

    def print_summary(self):
        """Print final summary after all files are checked."""
        if not self.is_multi_file:
            return

        # Clear progress line
        if not self.verbose:
            click.echo("\r" + " " * 80 + "\r", nl=False)

        # Show remaining issues hint
        extra_issues = len(self.issues) - self.max_issues_shown
        if extra_issues > 0 and not self.verbose:
            click.echo(
                click.style(
                    f"  ... and {extra_issues} more issues (use --verbose to see all)", fg="cyan"
                )
            )

        total = len(self.files)
        summary_parts = []
        if self.passed:
            summary_parts.append(click.style(f"{self.passed} passed", fg="green"))
        if self.warnings:
            summary_parts.append(click.style(f"{self.warnings} warnings", fg="yellow"))
        if self.failed:
            summary_parts.append(click.style(f"{self.failed} failed", fg="red"))

        summary = ", ".join(summary_parts) if summary_parts else "0 checked"
        click.echo(f"Summary: {summary} ({total} files checked)")


@check.command(name="all", cls=GlobAwareCommand)
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
@click.option(
    "--spec-details",
    is_flag=True,
    help="Show full spec validation results instead of summary",
)
@click.option(
    "--profile",
    type=click.Choice([c.value for c in CheckProfile], case_sensitive=False),
    required=False,
    default=None,
    help="Check best practices for specific use case",
)
@click.option(
    "--pmtiles",
    is_flag=True,
    help="Generate PMTiles after fixing (requires tippecanoe)",
)
@check_partition_options
@click.pass_context
def check_all(
    ctx,
    parquet_file,
    verbose,
    fix,
    fix_output,
    no_backup,
    overwrite,
    random_sample_size,
    limit_rows,
    spec_details,
    check_all_files,
    check_sample,
    profile,
    pmtiles,
):
    """Check compression, bbox, row groups, spatial order, and spec compliance."""
    from geoparquet_io.core.partition.reader import get_files_to_check
    from geoparquet_io.core.remote import is_remote_url, show_remote_read_message

    configure_verbose(verbose)
    with _activate_s3(ctx):
        # Get files to check based on partition options
        files_to_check, notice = get_files_to_check(
            parquet_file, check_all=check_all_files, check_sample=check_sample, verbose=verbose
        )

        if notice:
            click.echo(click.style(f"📁 {notice}", fg="cyan"))

        if not files_to_check:
            click.echo(click.style("No parquet files found", fg="red"))
            return

        # Validate fix_output for multi-file operations
        if fix and fix_output and len(files_to_check) > 1:
            from pathlib import Path

            fix_path = Path(fix_output)
            if not fix_path.is_dir():
                raise click.ClickException(
                    f"When fixing multiple files ({len(files_to_check)} files), "
                    f"--fix-output must be a directory, not a file path.\n"
                    f"Either:\n"
                    f"  1. Specify a directory: --fix-output /path/to/output_dir/\n"
                    f"  2. Omit --fix-output to fix files in-place (with .bak backups)"
                )

        # Check pmtiles requires --fix
        if pmtiles and not fix:
            raise click.UsageError("--pmtiles requires --fix to generate PMTiles from fixed files")

        # Check tippecanoe availability once before processing files
        if pmtiles:
            from geoparquet_io.core.pmtiles import _check_tippecanoe

            if not _check_tippecanoe():
                raise click.ClickException(
                    "--pmtiles requires tippecanoe.\n\n"
                    "Install tippecanoe:\n"
                    "  macOS:  brew install tippecanoe\n"
                    "  Ubuntu: sudo apt install tippecanoe"
                )

        # Create runner for multi-file progress tracking
        runner = MultiFileCheckRunner(files_to_check, verbose=verbose)

        # Process each file
        for file_path in files_to_check:
            runner.start_file(file_path)

            # Early skip for non-GeoParquet files when --pmtiles is passed
            # This avoids crashes in check_structure_impl which assumes geo metadata
            if pmtiles:
                from geoparquet_io.core.common import detect_geoparquet_file_type

                file_info = detect_geoparquet_file_type(file_path)
                if file_info["file_type"] == "unknown":
                    click.echo(
                        click.style(f"→ Skipping {file_path}: not a GeoParquet file", fg="yellow")
                    )
                    continue

            # Show single progress message for remote files (only in verbose mode for multi-file)
            if runner.verbose or not runner.is_multi_file:
                show_remote_read_message(file_path, verbose=False)
                if is_remote_url(file_path):
                    click.echo()  # Add blank line after remote message

            # Run all checks and collect results
            # In non-verbose multi-file mode, suppress detailed output
            show_output = runner.verbose or not runner.is_multi_file
            quiet = not show_output
            structure_results = check_structure_impl(
                file_path,
                verbose and show_output,
                return_results=True,
                quiet=quiet,
                profile=profile,
            )

            if show_output:
                click.echo("\nSpatial Order Analysis:")
            spatial_result = check_spatial_impl(
                file_path,
                random_sample_size,
                limit_rows,
                verbose and show_output,
                return_results=True,
                quiet=quiet,
            )

            from geoparquet_io.cli.fix_helpers import (
                aggregate_check_results,
                display_spatial_result,
            )

            display_spatial_result(spatial_result, show_output)

            # Run spec validation
            from geoparquet_io.core.validate import validate_geoparquet

            spec_result = validate_geoparquet(
                file_path, validate_data=True, sample_size=1000, verbose=False
            )

            # Display spec validation results
            if show_output:
                click.echo("\nSpec Validation:")
                if spec_details:
                    # Full output
                    from geoparquet_io.core.validate import format_terminal_output

                    format_terminal_output(spec_result)
                else:
                    # Summary only
                    if spec_result.failed_count > 0:
                        click.echo(
                            click.style(
                                f"  ✗ {spec_result.failed_count} failed, "
                                f"{spec_result.passed_count} passed",
                                fg="red",
                            )
                        )
                    elif spec_result.warning_count > 0:
                        click.echo(
                            click.style(
                                f"  ⚠ {spec_result.passed_count} passed, "
                                f"{spec_result.warning_count} warnings",
                                fg="yellow",
                            )
                        )
                    else:
                        click.echo(
                            click.style(f"  ✓ {spec_result.passed_count} checks passed", fg="green")
                        )

            # Aggregate results for runner tracking (include spec failures)
            combined_passed, combined_issues, _ = aggregate_check_results(
                structure_results, spatial_result
            )
            # Include spec failures in passed status and issues summary
            if spec_result.failed_count > 0:
                combined_passed = False
                combined_issues.append(f"Spec validation: {spec_result.failed_count} checks failed")
            runner.record_result(
                file_path,
                {"passed": combined_passed, "issues": combined_issues, **structure_results},
            )

            # If --fix flag is set, apply fixes
            if fix:
                from pathlib import Path

                from geoparquet_io.cli.fix_helpers import apply_check_all_fixes

                # If fix_output is a directory, generate per-file output path
                per_file_output = fix_output
                if fix_output and Path(fix_output).is_dir():
                    # Extract filename from file_path and place in output directory
                    filename = Path(file_path).name
                    per_file_output = str(Path(fix_output) / filename)

                all_results = {**structure_results, "spatial": spatial_result}
                applied = apply_check_all_fixes(
                    file_path=file_path,
                    all_results=all_results,
                    fix_output=per_file_output,
                    no_backup=no_backup,
                    overwrite=overwrite,
                    verbose=verbose,
                    profile=None,
                    check_structure_impl=check_structure_impl,
                    check_spatial_impl=check_spatial_impl,
                    random_sample_size=random_sample_size,
                    limit_rows=limit_rows,
                )
                if not applied:
                    continue

            # Generate PMTiles if requested (non-geo files already skipped at loop start)
            if pmtiles:
                from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

                # Generate PMTiles
                # Use fixed output path if available, otherwise original
                source_file = per_file_output if fix and per_file_output else file_path
                # Handle S3 URIs: Path().with_suffix() mangles s3:// schemes
                if source_file.startswith("s3://"):
                    output_pmtiles = source_file.rsplit(".", 1)[0] + ".pmtiles"
                else:
                    output_pmtiles = str(Path(source_file).with_suffix(".pmtiles"))

                try:
                    create_pmtiles_from_geoparquet(
                        input_path=source_file,
                        output_path=output_pmtiles,
                        verbose=verbose,
                    )
                    click.echo(click.style(f"✓ Generated {output_pmtiles}", fg="green"))
                except Exception as e:
                    click.echo(click.style(f"✗ PMTiles failed for {file_path}: {e}", fg="red"))

        # Print summary for multi-file checks
        runner.print_summary()


@check.command(name="spatial", cls=GlobAwareCommand)
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
@click.option(
    "--query-fraction",
    default=0.1,
    show_default=True,
    type=click.FloatRange(0.0, 1.0, min_open=True),
    help="Fraction of each dimension a sample query window spans",
)
@click.option(
    "--num-samples",
    default=20,
    show_default=True,
    type=click.IntRange(1),
    help="Number of sample query windows used to estimate the skip rate",
)
@click.option(
    "--seed",
    default=42,
    show_default=True,
    type=int,
    help="Random seed for the sample query windows (reproducible results)",
)
@click.option(
    "--min-efficiency",
    default=0.7,
    show_default=True,
    type=click.FloatRange(0.0, 1.0),
    help=(
        "Minimum skip rate as a fraction of what this row-group count allows. "
        "1.0 demands an ideal grid tiling; 0 accepts anything."
    ),
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
@check_partition_options
@click.pass_context
def check_spatial(
    ctx,
    parquet_file,
    random_sample_size,
    limit_rows,
    query_fraction,
    num_samples,
    seed,
    min_efficiency,
    verbose,
    fix,
    fix_output,
    no_backup,
    check_all_files,
    check_sample,
):
    """Check spatial ordering."""
    from geoparquet_io.core.check_fixes import fix_spatial_ordering
    from geoparquet_io.core.partition.reader import get_files_to_check

    configure_verbose(verbose)

    with _activate_s3(ctx):
        # Get files to check based on partition options
        files_to_check, notice = get_files_to_check(
            parquet_file, check_all=check_all_files, check_sample=check_sample, verbose=verbose
        )

        if notice:
            click.echo(click.style(f"📁 {notice}", fg="cyan"))

        if not files_to_check:
            click.echo(click.style("No parquet files found", fg="red"))
            return

        # Create runner for multi-file progress tracking
        runner = MultiFileCheckRunner(files_to_check, verbose=verbose)

        for file_path in files_to_check:
            runner.start_file(file_path)

            show_output = runner.verbose or not runner.is_multi_file
            quiet = not show_output
            result = check_spatial_impl(
                file_path,
                random_sample_size,
                limit_rows,
                verbose and show_output,
                return_results=True,
                quiet=quiet,
                num_samples=num_samples,
                query_fraction=query_fraction,
                seed=seed,
                efficiency_threshold=min_efficiency,
            )
            ratio = result["ratio"]
            passed = result.get("passed", ratio < 0.5 if ratio is not None else True)

            if show_output and ratio is not None:
                if passed:
                    click.echo(click.style("✓ Data appears to be spatially ordered", fg="green"))
                else:
                    click.echo(
                        click.style(
                            "⚠️  Data may not be optimally spatially ordered\n"
                            "Consider running 'gpio sort hilbert' to improve spatial locality",
                            fg="yellow",
                        )
                    )

            # Pushdown readiness. The ordering check above already measured the
            # locality of the same row groups, so reuse it rather than reading
            # the footer and running the sample queries a second time (#755).
            if show_output:
                from geoparquet_io.core.check_spatial_order import (
                    _SKIP_RATE_THRESHOLD,
                    check_spatial_pushdown_readiness,
                )

                pushdown = None
                if result.get("estimated_skip_rate") is not None:
                    pushdown = {
                        "has_geo_bbox": True,
                        "num_row_groups": result["total_pairs"] + 1,
                        "estimated_skip_rate": result["estimated_skip_rate"],
                        "ideal_skip_rate": result["ideal_skip_rate"],
                        "skip_rate_efficiency": result["skip_rate_efficiency"],
                        "avg_bbox_area_ratio": result["avg_bbox_area_ratio"],
                        # Absolute, unlike the ordering verdict: see the note in
                        # check_spatial_pushdown_readiness.
                        "passed": result["estimated_skip_rate"] >= _SKIP_RATE_THRESHOLD,
                    }
                else:
                    try:
                        pushdown = check_spatial_pushdown_readiness(
                            file_path,
                            verbose=verbose and show_output,
                            num_samples=num_samples,
                            query_fraction=query_fraction,
                            seed=seed,
                        )
                    except Exception as e:
                        if verbose:
                            click.echo(f"  Debug: Pushdown check failed: {e}", err=True)
                        pushdown = {"has_geo_bbox": False}

                click.echo("\nSpatial Filter Pushdown Readiness:")
                if pushdown.get("has_geo_bbox"):
                    skip_pct = pushdown["estimated_skip_rate"] * 100
                    click.echo(f"  Row groups: {pushdown['num_row_groups']}")
                    ideal = pushdown.get("ideal_skip_rate")
                    if ideal:
                        click.echo(
                            f"  Estimated skip rate: {skip_pct:.0f}% "
                            f"(of {ideal * 100:.0f}% achievable at this row-group count)"
                        )
                    else:
                        click.echo(f"  Estimated skip rate: {skip_pct:.0f}%")
                    click.echo(f"  Avg bbox area ratio: {pushdown['avg_bbox_area_ratio']:.2f}")
                    if pushdown["passed"]:
                        click.echo(click.style("  ✓ Good pushdown readiness", fg="green"))
                    else:
                        click.echo(click.style("  ⚠️  Low pushdown efficiency", fg="yellow"))
                else:
                    click.echo(
                        click.style(
                            "  ⚠️  No geo_bbox column found. "
                            "Add bbox with 'gpio add bbox' for pushdown support.",
                            fg="yellow",
                        )
                    )

            # Record result for summary
            runner.record_result(file_path, result)

            if fix:
                if not result.get("fix_available", False):
                    if show_output:
                        click.echo(
                            click.style(
                                "\n✓ No fix needed - already spatially ordered!", fg="green"
                            )
                        )
                    continue

                if show_output:
                    click.echo("\nApplying Hilbert spatial ordering...")
                output_path, backup_path = handle_fix_common(
                    file_path, fix_output, no_backup, fix_spatial_ordering, verbose, False, None
                )

                if show_output:
                    click.echo(
                        click.style("\n✓ Spatial ordering applied successfully!", fg="green")
                    )
                    click.echo(f"Optimized file: {output_path}")
                    if backup_path:
                        click.echo(f"Backup: {backup_path}")

        # Print summary for multi-file checks
        runner.print_summary()


@check.command(name="compression", cls=GlobAwareCommand)
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
@check_partition_options
@click.pass_context
def check_compression_cmd(
    ctx,
    parquet_file,
    verbose,
    fix,
    fix_output,
    no_backup,
    overwrite,
    check_all_files,
    check_sample,
):
    """Check geometry column compression."""
    from geoparquet_io.core.check_fixes import fix_compression
    from geoparquet_io.core.check_parquet_structure import check_compression
    from geoparquet_io.core.partition.reader import get_files_to_check

    configure_verbose(verbose)

    with _activate_s3(ctx):
        # Get files to check based on partition options
        files_to_check, notice = get_files_to_check(
            parquet_file, check_all=check_all_files, check_sample=check_sample, verbose=verbose
        )

        if notice:
            click.echo(click.style(f"📁 {notice}", fg="cyan"))

        if not files_to_check:
            click.echo(click.style("No parquet files found", fg="red"))
            return

        # Create runner for multi-file progress tracking
        runner = MultiFileCheckRunner(files_to_check, verbose=verbose)

        for file_path in files_to_check:
            runner.start_file(file_path)

            show_output = runner.verbose or not runner.is_multi_file
            quiet = not show_output
            result = check_compression(
                file_path, verbose and show_output, return_results=True, quiet=quiet
            )

            # Record result for summary
            runner.record_result(file_path, result)

            if fix:
                if not result.get("fix_available", False):
                    if show_output:
                        click.echo(
                            click.style("\n✓ No fix needed - already using ZSTD!", fg="green")
                        )
                    continue

                if show_output:
                    click.echo("\nRe-compressing with ZSTD...")
                output_path, backup_path = handle_fix_common(
                    file_path, fix_output, no_backup, fix_compression, verbose, overwrite, None
                )

                if show_output:
                    click.echo(click.style("\n✓ Compression optimized successfully!", fg="green"))
                    click.echo(f"Optimized file: {output_path}")
                    if backup_path:
                        click.echo(f"Backup: {backup_path}")

        # Print summary for multi-file checks
        runner.print_summary()


@check.command(name="bbox", cls=GlobAwareCommand)
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
@check_partition_options
@click.pass_context
def check_bbox_cmd(
    ctx,
    parquet_file,
    verbose,
    fix,
    fix_output,
    no_backup,
    overwrite,
    check_all_files,
    check_sample,
):
    """Check bbox column and metadata (version-aware).

    For GeoParquet 1.x: bbox column is recommended for spatial filtering.
    For GeoParquet 2.0/parquet-geo-only: bbox column is NOT recommended
    (native Parquet geo types provide row group statistics).
    """
    from geoparquet_io.core.check_fixes import fix_bbox_all, fix_bbox_removal
    from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox
    from geoparquet_io.core.partition.reader import get_files_to_check

    configure_verbose(verbose)

    with _activate_s3(ctx):
        # Get files to check based on partition options
        files_to_check, notice = get_files_to_check(
            parquet_file, check_all=check_all_files, check_sample=check_sample, verbose=verbose
        )

        if notice:
            click.echo(click.style(f"📁 {notice}", fg="cyan"))

        if not files_to_check:
            click.echo(click.style("No parquet files found", fg="red"))
            return

        # Create runner for multi-file progress tracking
        runner = MultiFileCheckRunner(files_to_check, verbose=verbose)

        for file_path in files_to_check:
            runner.start_file(file_path)

            show_output = runner.verbose or not runner.is_multi_file
            quiet = not show_output
            result = check_metadata_and_bbox(
                file_path, verbose and show_output, return_results=True, quiet=quiet
            )

            # Record result for summary
            runner.record_result(file_path, result)

            if fix:
                if not result.get("fix_available", False):
                    if show_output:
                        click.echo(click.style("\n✓ No fix needed - bbox is optimal!", fg="green"))
                    continue

                # Check if this is a removal (v2/parquet-geo-only) or addition (v1.x)
                if result.get("needs_bbox_removal", False):
                    # V2 or parquet-geo-only: remove bbox column
                    bbox_column_name = result.get("bbox_column_name")

                    def bbox_fix_func(
                        input_path, output_path, verbose_flag, profile_name, _col=bbox_column_name
                    ):
                        return fix_bbox_removal(
                            input_path, output_path, _col, verbose_flag, profile_name
                        )

                    output_path, backup_path = handle_fix_common(
                        file_path, fix_output, no_backup, bbox_fix_func, verbose, overwrite, None
                    )

                    if show_output:
                        click.echo(click.style("\n✓ Bbox column removed successfully!", fg="green"))
                        click.echo(f"Optimized file: {output_path}")
                        if backup_path:
                            click.echo(f"Backup: {backup_path}")
                else:
                    # V1.x: add bbox column/metadata (existing logic)
                    needs_column = result.get("needs_bbox_column", False)
                    needs_metadata = result.get("needs_bbox_metadata", False)

                    def bbox_fix_func(
                        input_path,
                        output_path,
                        verbose_flag,
                        profile_name,
                        _needs_col=needs_column,
                        _needs_meta=needs_metadata,
                    ):
                        return fix_bbox_all(
                            input_path,
                            output_path,
                            _needs_col,
                            _needs_meta,
                            verbose_flag,
                            profile_name,
                        )

                    output_path, backup_path = handle_fix_common(
                        file_path, fix_output, no_backup, bbox_fix_func, verbose, overwrite, None
                    )

                    if show_output:
                        click.echo(click.style("\n✓ Bbox optimized successfully!", fg="green"))
                        click.echo(f"Optimized file: {output_path}")
                        if backup_path:
                            click.echo(f"Backup: {backup_path}")

        # Print summary for multi-file checks
        runner.print_summary()


@check.command(name="row-group", cls=GlobAwareCommand)
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
@click.option(
    "--profile",
    type=click.Choice([c.value for c in CheckProfile], case_sensitive=False),
    required=False,
    default=None,
    help="Check best practices for specific use case",
)
@overwrite_option
@check_partition_options
@click.pass_context
def check_row_group_cmd(
    ctx,
    parquet_file,
    verbose,
    fix,
    fix_output,
    no_backup,
    overwrite,
    check_all_files,
    check_sample,
    profile,
):
    """Check row group size."""
    from geoparquet_io.core.check_fixes import fix_row_groups
    from geoparquet_io.core.check_parquet_structure import check_row_groups
    from geoparquet_io.core.partition.reader import get_files_to_check

    configure_verbose(verbose)

    with _activate_s3(ctx):
        # Get files to check based on partition options
        files_to_check, notice = get_files_to_check(
            parquet_file, check_all=check_all_files, check_sample=check_sample, verbose=verbose
        )

        if notice:
            click.echo(click.style(f"📁 {notice}", fg="cyan"))

        if not files_to_check:
            click.echo(click.style("No parquet files found", fg="red"))
            return

        # Create runner for multi-file progress tracking
        runner = MultiFileCheckRunner(files_to_check, verbose=verbose)

        for file_path in files_to_check:
            runner.start_file(file_path)

            show_output = runner.verbose or not runner.is_multi_file
            quiet = not show_output
            result = check_row_groups(
                file_path,
                verbose and show_output,
                return_results=True,
                quiet=quiet,
                profile=profile,
            )

            # Record result for summary
            runner.record_result(file_path, result)

            if fix:
                if not result.get("fix_available", False):
                    if show_output:
                        click.echo(
                            click.style("\n✓ No fix needed - row groups are optimal!", fg="green")
                        )
                    continue

                if show_output:
                    click.echo("\nOptimizing row groups...")
                output_path, backup_path = handle_fix_common(
                    file_path, fix_output, no_backup, fix_row_groups, verbose, overwrite, None
                )

                if show_output:
                    click.echo(click.style("\n✓ Row groups optimized successfully!", fg="green"))
                    click.echo(f"Optimized file: {output_path}")
                    if backup_path:
                        click.echo(f"Backup: {backup_path}")

        # Print summary for multi-file checks
        runner.print_summary()


# Convert commands group
@cli.group(cls=ConvertDefaultGroup)
@click.pass_context
def convert(ctx):
    """Convert between formats and coordinate systems.

    Auto-detects output format from file extension. Supports GeoParquet, GeoPackage,
    FlatGeobuf, CSV, Shapefile, and GeoJSON.

    \b
    Auto-detection examples:
        gpio convert input.shp output.parquet                    # → GeoParquet
        gpio convert data.parquet output.gpkg                    # → GeoPackage
        gpio convert data.parquet output.fgb                     # → FlatGeobuf
        gpio convert data.parquet output.csv                     # → CSV with WKT
        gpio convert data.parquet output.geojson                 # → GeoJSON

    \b
    Explicit subcommands:
        gpio convert geoparquet input.shp output.parquet         # Force GeoParquet
        gpio convert geopackage data.parquet output.gpkg         # Force GeoPackage
        gpio convert reproject input.parquet out.parquet -d EPSG:32610
        gpio convert geojson data.parquet | tippecanoe -P -o tiles.pmtiles
    """
    ctx.ensure_object(dict)
    timestamps = ctx.obj.get("timestamps", False)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


@convert.command(name="geoparquet", cls=SingleFileCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
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
    "--layer",
    help="GeoPackage/FileGDB: Layer name to read (reads first layer if not specified)",
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
    help="CSV/TSV: Skip rows whose geometry cannot be parsed instead of failing "
    "(rows with no geometry at all are kept, with NULL geometry)",
)
@click.option(
    "--csv-max-line-size",
    type=int,
    default=None,
    help="CSV/TSV: Maximum line size in bytes (default: 50MB). Increase for very large WKT geometries.",
)
@click.option(
    "--allow-no-geometry",
    is_flag=True,
    help="Allow conversion to plain Parquet when no geometry column is detected (default: error)",
)
@repair_geometry_option
@linearize_curves_options
@geoparquet_version_option
@verbose_option
@output_format_options
@aws_profile_option
@any_extension_option
@show_sql_option
@click.pass_context
def convert_to_geoparquet_cmd(
    ctx,
    input_file,
    output_file,
    skip_hilbert,
    wkt_column,
    lat_column,
    lon_column,
    delimiter,
    layer,
    crs,
    skip_invalid,
    csv_max_line_size,
    allow_no_geometry,
    repair_geometry,
    linearize_curves,
    max_angle_deg,
    geoparquet_version,
    verbose,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    aws_profile,
    any_extension,
    show_sql,
):
    """
    Convert vector formats to optimized GeoParquet.

    Supports Shapefile, GeoJSON, GeoPackage, GDB, CSV/TSV with WKT or lat/lon columns.
    Applies ZSTD compression, bbox metadata, and Hilbert ordering by default.
    Auto-streams Arrow IPC to stdout when piped (or use "-" as output).

    \b
    Examples:
      # Standard conversion
      gpio convert input.gpkg output.parquet

      \b
      # Pipe to another command (auto-streams when piped)
      gpio convert input.gpkg | gpio add bbox - | gpio upload - s3://bucket/data.parquet
    """
    from geoparquet_io.core.convert import set_csv_max_line_size
    from geoparquet_io.core.streaming import (
        StreamingError,
        should_stream_output,
        validate_output,
    )

    # Set CSV max line size if specified
    if csv_max_line_size is not None:
        set_csv_max_line_size(csv_max_line_size)

    # Validate output early - provides helpful error if no output and not piping
    try:
        validate_output(output_file)
    except StreamingError as e:
        raise click.ClickException(str(e)) from None

    # Validate .parquet extension
    validate_parquet_extension(output_file, any_extension)

    # Parse row group options
    row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

    # Check for streaming output
    with _activate_s3(ctx, aws_profile=aws_profile):
        if should_stream_output(output_file):
            # Suppress verbose for streaming
            verbose = False
            _convert_streaming(
                input_file,
                skip_hilbert=skip_hilbert,
                wkt_column=wkt_column,
                lat_column=lat_column,
                lon_column=lon_column,
                delimiter=delimiter,
                layer=layer,
                crs=crs,
                skip_invalid=skip_invalid,
                allow_no_geometry=allow_no_geometry,
                profile=aws_profile,
                geoparquet_version=geoparquet_version,
                compression=compression,
                compression_level=compression_level,
                row_group_rows=row_group_size,
                row_group_size_mb=row_group_mb,
                repair_geometry=repair_geometry,
                linearize_curves=linearize_curves,
                max_angle_deg=max_angle_deg,
                memory_limit=write_memory,
            )
        else:
            convert_to_geoparquet(
                input_file,
                output_file,
                skip_hilbert=skip_hilbert,
                verbose=verbose,
                compression=compression,
                compression_level=compression_level,
                row_group_rows=row_group_size,
                row_group_size_mb=row_group_mb,
                wkt_column=wkt_column,
                lat_column=lat_column,
                lon_column=lon_column,
                delimiter=delimiter,
                layer=layer,
                crs=crs,
                skip_invalid=skip_invalid,
                allow_no_geometry=allow_no_geometry,
                profile=aws_profile,
                geoparquet_version=geoparquet_version,
                repair_geometry=repair_geometry,
                linearize_curves=linearize_curves,
                max_angle_deg=max_angle_deg,
                memory_limit=write_memory,
            )


def _convert_streaming(
    input_file,
    skip_hilbert,
    wkt_column,
    lat_column,
    lon_column,
    delimiter,
    layer,
    crs,
    skip_invalid,
    allow_no_geometry,
    profile,
    geoparquet_version,
    compression="ZSTD",
    compression_level=15,
    row_group_rows=None,
    row_group_size_mb=None,
    repair_geometry=True,
    linearize_curves=True,
    max_angle_deg=None,
    memory_limit=None,
):
    """Handle streaming output for convert command."""
    import tempfile
    import uuid

    import pyarrow.parquet as pq

    from geoparquet_io.core.streaming import write_arrow_stream

    # Convert to temp file first, then stream
    temp_path = Path(tempfile.gettempdir()) / f"gpio_convert_{uuid.uuid4()}.parquet"

    try:
        convert_to_geoparquet(
            input_file,
            str(temp_path),
            skip_hilbert=skip_hilbert,
            verbose=False,
            allow_no_geometry=allow_no_geometry,
            compression=compression,
            compression_level=compression_level,
            row_group_rows=row_group_rows,
            row_group_size_mb=row_group_size_mb,
            wkt_column=wkt_column,
            lat_column=lat_column,
            lon_column=lon_column,
            delimiter=delimiter,
            layer=layer,
            crs=crs,
            skip_invalid=skip_invalid,
            profile=profile,
            geoparquet_version=geoparquet_version,
            repair_geometry=repair_geometry,
            linearize_curves=linearize_curves,
            max_angle_deg=max_angle_deg,
            memory_limit=memory_limit,
        )

        # Read and stream to stdout
        table = pq.read_table(temp_path)
        write_arrow_stream(table)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _reproject_impl_cli(
    input_file,
    output_file,
    dst_crs,
    src_crs,
    overwrite,
    verbose,
    aws_profile,
    compression,
    compression_level,
    geoparquet_version,
    row_group_size_mb=None,
    row_group_rows=None,
    memory_limit=None,
    assume_crs84=False,
):
    """Shared reproject CLI implementation."""
    from geoparquet_io.core.remote import validate_profile_for_urls

    # Configure verbose logging
    configure_verbose(verbose)

    # Validate profile is only used with S3
    validate_profile_for_urls(None, input_file, output_file)

    try:
        result = reproject_core(
            input_parquet=input_file,
            output_parquet=output_file,
            target_crs=dst_crs,
            source_crs=src_crs,
            overwrite=overwrite,
            compression=compression,
            compression_level=compression_level,
            verbose=verbose,
            geoparquet_version=geoparquet_version,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            memory_limit=memory_limit,
            assume_crs84=assume_crs84,
        )
    except ValueError as e:
        raise click.ClickException(str(e)) from None

    # result is None for streaming mode (stdout)
    if result:
        click.echo(f"\nReprojected {result.feature_count:,} features")
        click.echo(f"  Source CRS: {result.source_crs}")
        click.echo(f"  Destination CRS: {result.target_crs}")
        click.echo(f"  Output: {result.output_path}")


@convert.command(name="reproject", cls=SingleFileCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@click.option(
    "--dst-crs",
    "-d",
    default="EPSG:4326",
    show_default=True,
    help=(
        "Destination CRS (e.g., 'EPSG:4326', 'EPSG:32610'). The default "
        "(EPSG:4326 / OGC:CRS84) is written by omitting the crs key per the "
        "GeoParquet spec, so the output has no explicit crs."
    ),
)
@click.option(
    "--src-crs",
    "-s",
    default=None,
    help="Override source CRS (e.g., 'EPSG:4326'). If not provided, detected from file metadata.",
)
@click.option(
    "--assume-crs84",
    is_flag=True,
    help=(
        "Treat an unknown (explicit null) input CRS as OGC:CRS84 and rewrite the "
        "file so the crs key is omitted (the default). Use when data is really "
        "lon/lat WGS84 but the file declares crs:null."
    ),
)
@overwrite_option
@verbose_option
@aws_profile_option
@output_format_options
@geoparquet_version_option
@any_extension_option
@show_sql_option
@click.pass_context
def convert_reproject(
    ctx,
    input_file,
    output_file,
    dst_crs,
    src_crs,
    assume_crs84,
    overwrite,
    verbose,
    aws_profile,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
    any_extension,
    show_sql,
):
    """
    Reproject a GeoParquet file to a different CRS.

    Uses DuckDB's ST_Transform for fast, streaming reprojection.
    Automatically detects source CRS from GeoParquet metadata unless --src-crs is provided.

    If OUTPUT_FILE is not provided, creates <input>_<crs>.parquet.
    Use --overwrite to modify the input file in place.

    \b
    Examples:
        gpio convert reproject input.parquet output.parquet
        gpio convert reproject input.parquet -d EPSG:32610
        gpio convert reproject input.parquet --overwrite -d EPSG:4326
        gpio convert reproject input.parquet output.parquet --dst-crs EPSG:3857
    """
    # Validate .parquet extension
    validate_parquet_extension(output_file, any_extension)

    # Validate mutual exclusivity of row group options and get MB value
    row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

    with _activate_s3(ctx, aws_profile=aws_profile):
        _reproject_impl_cli(
            input_file,
            output_file,
            dst_crs,
            src_crs,
            overwrite,
            verbose,
            aws_profile,
            compression,
            compression_level,
            geoparquet_version,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            memory_limit=write_memory,
            assume_crs84=assume_crs84,
        )


@convert.command(name="geojson", cls=SingleFileCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@overwrite_option
@click.option(
    "--no-rs",
    is_flag=True,
    help="Disable RFC 8142 record separators (enabled by default for tippecanoe -P)",
)
@click.option(
    "--precision",
    type=int,
    default=7,
    help="Coordinate decimal precision for geometry and bbox (default: 7 per RFC 7946).",
)
@click.option(
    "--write-bbox",
    is_flag=True,
    help="Include bbox property for each feature",
)
@click.option(
    "--id-field",
    type=str,
    default=None,
    help="Source field to use as feature 'id' member",
)
@click.option(
    "--description",
    type=str,
    default=None,
    help="Description to add to the FeatureCollection",
)
@click.option(
    "--feature-collection",
    "no_seq",
    is_flag=True,
    help="Output a FeatureCollection instead of newline-delimited GeoJSONSeq (streaming only)",
)
@click.option(
    "--pretty",
    is_flag=True,
    help="Pretty-print the JSON output with indentation",
)
@click.option(
    "--keep-crs",
    is_flag=True,
    help="Keep original CRS instead of reprojecting to WGS84 (EPSG:4326)",
)
@repair_geometry_option
@verbose_option
@aws_profile_option
@show_sql_option
@click.pass_context
def convert_geojson(
    ctx,
    input_file,
    output_file,
    overwrite,
    no_rs,
    precision,
    write_bbox,
    id_field,
    description,
    no_seq,
    pretty,
    keep_crs,
    repair_geometry,
    verbose,
    aws_profile,
    show_sql,
):
    """
    Convert GeoParquet to GeoJSON format.

    Supports two modes based on whether OUTPUT_FILE is provided:

    \b
    STREAMING MODE (no output file):
      Streams newline-delimited GeoJSON (GeoJSONSeq) to stdout with RFC 8142
      record separators. Designed for piping to tippecanoe for PMTiles/MBTiles.
      Use --feature-collection to output a FeatureCollection instead.

    \b
    FILE MODE (with output file):
      Writes a standard GeoJSON FeatureCollection to the specified file.

    \b
    Either mode reads from stdin with "-" (Arrow IPC), for pipeline use.

    \b
    Examples:
      # Stream to tippecanoe for PMTiles generation
      gpio convert geojson buildings.parquet | tippecanoe -P -o buildings.pmtiles

      # Pipeline with filtering
      gpio extract data.parquet --bbox "-122.5,37.5,-122,38" | gpio convert geojson - | tippecanoe -P -o sf.pmtiles

      # Pipeline ending in a GeoJSON file
      gpio extract data.parquet --bbox "-122.5,37.5,-122,38" | gpio convert geojson - sf.geojson

      # Write to GeoJSON file
      gpio convert geojson data.parquet output.geojson

      # Pretty-print with description
      gpio convert geojson data.parquet output.geojson --pretty --description "My dataset"

    \b
    Note: GeoParquet input is automatically reprojected to WGS84 (EPSG:4326)
    for RFC 7946 compliance. Use --keep-crs to preserve the original CRS.
    """
    from geoparquet_io.core.format_writers import write_geojson
    from geoparquet_io.core.geojson_stream import convert_to_geojson
    from geoparquet_io.core.remote import validate_profile_for_urls

    configure_verbose(verbose)

    # Validate aws_profile is only used with S3
    validate_profile_for_urls(aws_profile, input_file, output_file)

    with _activate_s3(ctx, aws_profile=aws_profile):
        if output_file:
            # File mode - use write_geojson which handles no-geometry case
            write_geojson(
                input_path=input_file,
                output_path=output_file,
                precision=precision,
                write_bbox=write_bbox,
                id_field=id_field,
                description=description,
                pretty=pretty,
                keep_crs=keep_crs,
                overwrite=overwrite,
                verbose=verbose,
                profile=aws_profile,
                repair_geometry=repair_geometry,
            )
        else:
            # Streaming mode - use convert_to_geojson directly
            convert_to_geojson(
                input_path=input_file,
                output_path=output_file,
                rs=not no_rs,
                precision=precision,
                write_bbox=write_bbox,
                id_field=id_field,
                description=description,
                seq=not no_seq,
                pretty=pretty,
                verbose=verbose,
                profile=aws_profile,
                keep_crs=keep_crs,
                repair_geometry=repair_geometry,
            )


@convert.command(name="geopackage", cls=SingleFileCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@overwrite_option
@click.option(
    "--layer-name",
    default="features",
    show_default=True,
    help="Layer name in GeoPackage",
)
@verbose_option
@aws_profile_option
@show_sql_option
@click.pass_context
def convert_geopackage(
    ctx,
    input_file,
    output_file,
    overwrite,
    layer_name,
    verbose,
    aws_profile,
    show_sql,
):
    """
    Convert GeoParquet to GeoPackage format.

    GeoPackage is an OGC standard based on SQLite, supporting spatial indexing
    and multiple layers. Output includes a spatial index by default.

    \b
    Examples:
      # Convert to GeoPackage
      gpio convert geopackage data.parquet output.gpkg

      # With custom layer name
      gpio convert geopackage data.parquet output.gpkg --layer-name buildings

      # Overwrite existing file
      gpio convert geopackage data.parquet output.gpkg --overwrite

      # Auto-detection (no subcommand needed)
      gpio convert data.parquet output.gpkg
    """
    from geoparquet_io.core.format_writers import write_geopackage

    configure_verbose(verbose)

    if output_file is None:
        # Generate output filename
        output_file = Path(input_file).stem + ".gpkg"

    with _activate_s3(ctx, aws_profile=aws_profile):
        write_geopackage(
            input_path=input_file,
            output_path=output_file,
            overwrite=overwrite,
            layer_name=layer_name,
            verbose=verbose,
            profile=aws_profile,
        )


@convert.command(name="flatgeobuf", cls=SingleFileCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@overwrite_option
@verbose_option
@aws_profile_option
@show_sql_option
@click.pass_context
def convert_flatgeobuf(
    ctx,
    input_file,
    output_file,
    overwrite,
    verbose,
    aws_profile,
    show_sql,
):
    """
    Convert GeoParquet to FlatGeobuf format.

    FlatGeobuf is a cloud-native format with built-in spatial indexing, designed
    for efficient streaming and HTTP range requests. Spatial index is created
    automatically.

    \b
    Examples:
      # Convert to FlatGeobuf
      gpio convert flatgeobuf data.parquet output.fgb

      # Auto-detection (no subcommand needed)
      gpio convert data.parquet output.fgb
    """
    from geoparquet_io.core.format_writers import write_flatgeobuf

    configure_verbose(verbose)

    if output_file is None:
        # Generate output filename
        output_file = Path(input_file).stem + ".fgb"

    with _activate_s3(ctx, aws_profile=aws_profile):
        write_flatgeobuf(
            input_path=input_file,
            output_path=output_file,
            overwrite=overwrite,
            verbose=verbose,
            profile=aws_profile,
        )


@convert.command(name="csv", cls=SingleFileCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@overwrite_option
@click.option(
    "--no-wkt",
    is_flag=True,
    help="Exclude WKT geometry column (only non-spatial attributes)",
)
@click.option(
    "--no-bbox",
    is_flag=True,
    help="Exclude bbox column if present in input",
)
@verbose_option
@aws_profile_option
@show_sql_option
@click.pass_context
def convert_csv(
    ctx,
    input_file,
    output_file,
    overwrite,
    no_wkt,
    no_bbox,
    verbose,
    aws_profile,
    show_sql,
):
    """
    Convert GeoParquet to CSV format with optional WKT geometry.

    By default, includes geometry as WKT (Well-Known Text) and bbox column if present.
    Complex types (STRUCT, LIST, MAP) are JSON-encoded.

    \b
    Examples:
      # Convert to CSV with WKT geometry
      gpio convert csv data.parquet output.csv

      # Export only attributes (no geometry)
      gpio convert csv data.parquet output.csv --no-wkt

      # Exclude bbox column
      gpio convert csv data.parquet output.csv --no-bbox

      # Auto-detection (no subcommand needed)
      gpio convert data.parquet output.csv
    """
    from geoparquet_io.core.format_writers import write_csv

    configure_verbose(verbose)

    if output_file is None:
        # Generate output filename
        output_file = Path(input_file).stem + ".csv"

    with _activate_s3(ctx, aws_profile=aws_profile):
        write_csv(
            input_path=input_file,
            output_path=output_file,
            include_wkt=not no_wkt,
            include_bbox=not no_bbox,
            overwrite=overwrite,
            verbose=verbose,
            profile=aws_profile,
        )


@convert.command(name="shapefile", cls=SingleFileCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@overwrite_option
@click.option(
    "--encoding",
    default="UTF-8",
    show_default=True,
    help="Character encoding for attribute data",
)
@verbose_option
@aws_profile_option
@show_sql_option
@click.pass_context
def convert_shapefile(
    ctx,
    input_file,
    output_file,
    overwrite,
    encoding,
    verbose,
    aws_profile,
    show_sql,
):
    """
    Convert GeoParquet to Shapefile format.

    Note: Shapefiles have significant limitations:
    - Column names truncated to 10 characters
    - File size limit of 2GB
    - Limited data type support
    - Creates multiple files (.shp, .shx, .dbf, .prj)

    Consider using GeoPackage or FlatGeobuf instead for modern workflows.

    \b
    Examples:
      # Convert to Shapefile
      gpio convert shapefile data.parquet output.shp

      # With custom encoding
      gpio convert shapefile data.parquet output.shp --encoding Latin1

      # Overwrite existing file
      gpio convert shapefile data.parquet output.shp --overwrite

      # Auto-detection (no subcommand needed)
      gpio convert data.parquet output.shp
    """
    from geoparquet_io.core.format_writers import write_shapefile

    configure_verbose(verbose)

    if output_file is None:
        # Generate output filename
        output_file = Path(input_file).stem + ".shp"

    with _activate_s3(ctx, aws_profile=aws_profile):
        write_shapefile(
            input_path=input_file,
            output_path=output_file,
            overwrite=overwrite,
            encoding=encoding,
            verbose=verbose,
            profile=aws_profile,
        )


# Inspect command group
@cli.group(cls=InspectDefaultGroup)
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


# Extract commands group
@cli.group(cls=ExtractDefaultGroup)
@click.pass_context
def extract(ctx):
    """Extract data from files and services to GeoParquet.

    By default, extracts from GeoParquet files. Use subcommands for other sources.

    \b
    Examples:
        gpio extract data.parquet output.parquet --bbox -122,37,-121,38
        gpio extract geoparquet data.parquet output.parquet  # Explicit
        gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 out.parquet
        gpio extract bigquery project.dataset.table output.parquet
    """
    # Ensure logging is set up (in case this group is invoked directly in tests)
    ctx.ensure_object(dict)
    timestamps = ctx.obj.get("timestamps", False)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


@extract.command(name="geoparquet", cls=GlobAwareCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
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
@overwrite_option
@write_strategy_option
@partition_input_options
@repair_geometry_option
@dry_run_option
@show_sql_option
@verbose_option
@aws_profile_option
@any_extension_option
@click.pass_context
def extract_geoparquet(
    ctx,
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
    overwrite,
    write_strategy,
    write_memory,
    allow_schema_diff,
    hive_input,
    repair_geometry,
    dry_run,
    show_sql,
    verbose,
    aws_profile,
    any_extension,
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
            --aws_profile my-aws \\
            --bbox -122.5,37.5,-122.0,38.0

        \b
        # Extract first 1000 rows
        gpio extract data.parquet output.parquet --limit 1000
    """
    # Validate output early - provides helpful error if no output and not piping
    from geoparquet_io.core.streaming import StreamingError, validate_output

    try:
        validate_output(output_file)
    except StreamingError as e:
        raise click.ClickException(str(e)) from None

    # Validate .parquet extension
    validate_parquet_extension(output_file, any_extension)

    # Parse row group options
    row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

    with _activate_s3(ctx, aws_profile=aws_profile):
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
            geoparquet_version=geoparquet_version,
            allow_schema_diff=allow_schema_diff,
            hive_input=hive_input,
            write_strategy=write_strategy,
            memory_limit=write_memory,
            overwrite=overwrite,
            repair_geometry=repair_geometry,
        )


@extract.command(name="arcgis", cls=SingleFileCommand)
@click.argument("service_url")
@click.argument("output_file", type=click.Path())
@click.option(
    "--token",
    help="ArcGIS authentication token",
)
@click.option(
    "--token-file",
    type=click.Path(exists=True),
    help="Path to file containing authentication token",
)
@click.option(
    "--username",
    help="ArcGIS Online/Enterprise username (requires --password)",
)
@click.option(
    "--password",
    help="ArcGIS Online/Enterprise password (requires --username)",
)
@click.option(
    "--portal-url",
    help="Enterprise portal URL for token generation (default: ArcGIS Online)",
)
@click.option(
    "--where",
    default="1=1",
    help="SQL WHERE clause to filter features (pushed to server, default: '1=1' = all)",
)
@click.option(
    "--bbox",
    help="Bounding box filter: xmin,ymin,xmax,ymax in WGS84 (pushed to server)",
)
@click.option(
    "--include-cols",
    help="Comma-separated columns to include (pushed to server for efficiency)",
)
@click.option(
    "--exclude-cols",
    help="Comma-separated columns to exclude (applied after download)",
)
@click.option(
    "--limit",
    type=int,
    help="Maximum number of features to extract",
)
@click.option(
    "--output-crs",
    help="Output CRS such as EPSG:25830, or 'native' for the layer's "
    "advertised SR. Default reprojects to WGS84.",
)
@click.option(
    "--max-allowable-offset",
    type=float,
    default=None,
    help="Server-side geometry generalization tolerance in output CRS units "
    "(ArcGIS maxAllowableOffset). Reduces vertices per feature, useful for very "
    "large or dense polygons.",
)
@click.option(
    "--skip-hilbert",
    is_flag=True,
    help="Skip Hilbert spatial ordering (faster but less optimal for spatial queries)",
)
@click.option(
    "--skip-bbox",
    is_flag=True,
    help="Skip adding bbox column (bbox enables faster spatial filtering on remote files)",
)
@click.option(
    "--workers",
    type=click.IntRange(min=1, max=10),
    default=1,
    help="Number of concurrent requests (1-10). Default: 1 (sequential). Values 2-3 recommended for speedup. Higher values may trigger rate limits.",
)
@click.option(
    "--batch-size",
    type=click.IntRange(min=1, max=5000),
    default=None,
    help="Features per request. Default: server's maxRecordCount. Auto-reduces on server errors. Use smaller values for layers with complex geometries.",
)
@click.option(
    "--timeout",
    type=click.FloatRange(min=0, min_open=True),
    default=60.0,
    show_default=True,
    help="Per-request HTTP timeout in seconds. Increase for layers with very large or complex geometries that the server is slow to serialize.",
)
@geoparquet_version_option
@overwrite_option
@repair_geometry_option
@verbose_option
@compression_options
@row_group_options
@any_extension_option
@aws_profile_option
@show_sql_option
@click.pass_context
def extract_arcgis(
    ctx,
    service_url,
    output_file,
    token,
    token_file,
    username,
    password,
    portal_url,
    where,
    bbox,
    include_cols,
    exclude_cols,
    limit,
    output_crs,
    max_allowable_offset,
    skip_hilbert,
    skip_bbox,
    workers,
    batch_size,
    timeout,
    geoparquet_version,
    overwrite,
    repair_geometry,
    verbose,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    any_extension,
    aws_profile,
    show_sql,
):
    """
    Extract features from ArcGIS Feature Service to GeoParquet.

    Downloads features from an ArcGIS REST Feature Service and converts
    them to an optimized GeoParquet file with ZSTD compression, bbox metadata,
    and Hilbert spatial ordering.

    SERVICE_URL must be a full ArcGIS Feature Service layer URL including the
    layer ID (e.g., .../FeatureServer/0).

    \b
    Filtering options (pushed to server for efficiency):
      --where          SQL WHERE clause for attribute filtering
      --bbox           Spatial bounding box filter (xmin,ymin,xmax,ymax)
      --include-cols   Select specific columns to download
      --limit          Maximum number of features to return

    \b
    Authentication options (in priority order):
      --token          Direct token string
      --token-file     Path to file containing token
      --username/password  Generate token via ArcGIS REST API

    \b
    Examples:
      # Public service (no auth)
      gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 out.parquet

      \b
      # Filter by bounding box (server-side)
      gpio extract arcgis https://... out.parquet --bbox -122.5,37.5,-122.0,38.0

      \b
      # Filter by SQL WHERE clause (server-side)
      gpio extract arcgis https://... out.parquet --where "state='CA'"

      \b
      # Extract only specific columns (server-side)
      gpio extract arcgis https://... out.parquet --include-cols name,population

      \b
      # Limit number of features
      gpio extract arcgis https://... out.parquet --limit 1000

      \b
      # Combined filters
      gpio extract arcgis https://... out.parquet \\
          --bbox -122.5,37.5,-122.0,38.0 \\
          --where "population > 10000" \\
          --limit 500
    """
    from geoparquet_io.core.arcgis import convert_arcgis_to_geoparquet
    from geoparquet_io.core.file_utils import validate_parquet_extension

    configure_verbose(verbose)

    # Validate auth options
    if (username and not password) or (password and not username):
        raise click.BadParameter("Both --username and --password are required together")

    # Validate output extension
    if not any_extension:
        validate_parquet_extension(output_file)

    # Validate mutual exclusivity of row group options and get MB value
    row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

    # Parse bbox string if provided
    bbox_tuple = None
    if bbox:
        try:
            parts = [float(x.strip()) for x in bbox.split(",")]
            if len(parts) != 4:
                raise ValueError("bbox must have exactly 4 values")
            bbox_tuple = tuple(parts)
        except ValueError as e:
            raise click.BadParameter(f"Invalid bbox format: {e}. Use xmin,ymin,xmax,ymax") from e

    with _activate_s3(ctx, aws_profile=aws_profile):
        convert_arcgis_to_geoparquet(
            service_url=service_url,
            output_file=output_file,
            token=token,
            token_file=token_file,
            username=username,
            password=password,
            portal_url=portal_url,
            where=where,
            bbox=bbox_tuple,
            include_cols=include_cols,
            exclude_cols=exclude_cols,
            limit=limit,
            output_crs=output_crs,
            max_allowable_offset=max_allowable_offset,
            skip_hilbert=skip_hilbert,
            skip_bbox=skip_bbox,
            max_workers=workers,
            batch_size=batch_size,
            timeout=timeout,
            compression=compression.upper(),
            compression_level=compression_level,
            verbose=verbose,
            geoparquet_version=geoparquet_version,
            profile=aws_profile,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            overwrite=overwrite,
            repair_geometry=repair_geometry,
        )


@extract.command(name="bigquery")
@handle_geoparquet_errors
@click.argument("table_id", metavar="TABLE_ID")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@click.option(
    "--project",
    help="GCP project ID (overrides project in TABLE_ID if specified)",
)
@click.option(
    "--credentials-file",
    type=click.Path(exists=True),
    help="Path to GCP service account JSON file (otherwise uses gcloud auth or "
    "GOOGLE_APPLICATION_CREDENTIALS)",
)
@click.option(
    "--include-cols",
    help="Comma-separated columns to include",
)
@click.option(
    "--exclude-cols",
    help="Comma-separated columns to exclude",
)
@click.option(
    "--where",
    help="SQL WHERE clause for filtering (BigQuery SQL syntax)",
)
@click.option(
    "--bbox",
    help="Bounding box for spatial filter as minx,miny,maxx,maxy",
    type=str,
)
@click.option(
    "--bbox-mode",
    type=click.Choice(["auto", "server", "local"]),
    default="auto",
    help="Bbox filter mode: 'auto' (default) chooses based on table size, "
    "'server' forces BigQuery-side filtering, 'local' forces DuckDB-side filtering",
)
@click.option(
    "--bbox-threshold",
    type=click.IntRange(0, None),
    default=500000,
    help="Row count threshold for auto bbox mode. Tables with more rows use "
    "server-side filtering. Must be non-negative. Default: 500000",
)
@click.option(
    "--limit",
    type=click.IntRange(0, None),
    help="Maximum number of rows to extract. Must be non-negative.",
)
@click.option(
    "--geography-column",
    help="Column containing geometry data. Auto-detected for native GEOGRAPHY columns. "
    "Specify to parse a VARCHAR column as WKT or GeoJSON geometry.",
)
@click.option(
    "--geometry-format",
    type=click.Choice(["wkt", "geojson"], case_sensitive=False),
    default="wkt",
    help="Format of geometry data in VARCHAR columns (default: wkt). "
    "Only used when --geography-column points to a non-GEOGRAPHY column.",
)
@click.option(
    "--edges",
    type=click.Choice(["spherical", "planar"], case_sensitive=False),
    default=None,
    help="Edge interpretation for GeoParquet metadata. "
    "Native GEOGRAPHY columns default to 'spherical' (BigQuery uses S2). "
    "VARCHAR columns default to 'planar'. Use this to override.",
)
@output_format_options(
    write_memory_help=(
        "Memory limit for the DuckDB scan of the BigQuery result (e.g., '512MB', '4GB'). "
        "This command writes through PyArrow, so the limit bounds the read, not the write."
    )
)
@geoparquet_version_option
@overwrite_option
@repair_geometry_option
@dry_run_option
@show_sql_option
@verbose_option
@any_extension_option
def extract_bigquery_cmd(
    table_id,
    output_file,
    project,
    credentials_file,
    include_cols,
    exclude_cols,
    where,
    bbox,
    bbox_mode,
    bbox_threshold,
    limit,
    geography_column,
    geometry_format,
    edges,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
    overwrite,
    repair_geometry,
    dry_run,
    show_sql,
    verbose,
    any_extension,
):
    """
    Extract data from a BigQuery table to GeoParquet.

    TABLE_ID is the fully qualified BigQuery table identifier:
    PROJECT.DATASET.TABLE or DATASET.TABLE (if --project is set).

    Authentication (in order of precedence):

    \b
    1. --credentials-file: Path to service account JSON
    2. GOOGLE_APPLICATION_CREDENTIALS environment variable
    3. gcloud auth application-default credentials

    Native GEOGRAPHY columns are automatically converted to GeoParquet geometry
    with spherical edges. If your geometry is stored as a VARCHAR column
    (WKT or GeoJSON), use --geography-column and --geometry-format to parse it.
    If no geometry column is found, the output is plain Parquet.

    \b
    Limitations:
    - Cannot read BigQuery views or external tables (Storage Read API limitation)
    - BIGNUMERIC columns are not supported

    Examples:

        \b
        # Extract entire table
        gpio extract bigquery myproject.geodata.buildings output.parquet

        \b
        # Extract with filtering
        gpio extract bigquery myproject.geodata.buildings output.parquet \\
            --where "area > 1000" --limit 10000

        \b
        # Use service account credentials
        gpio extract bigquery myproject.geodata.buildings output.parquet \\
            --credentials-file /path/to/service-account.json

        \b
        # Select specific columns
        gpio extract bigquery myproject.geodata.buildings output.parquet \\
            --include-cols "id,name,geography"

        \b
        # Parse a VARCHAR column as WKT geometry
        gpio extract bigquery myproject.dataset.table output.parquet \\
            --geography-column geometry --geometry-format wkt

        \b
        # Parse a GeoJSON geometry column
        gpio extract bigquery myproject.dataset.table output.parquet \\
            --geography-column geojson_col --geometry-format geojson

        \b
        # Extract without geometry (plain Parquet)
        gpio extract bigquery myproject.dataset.table output.parquet
    """
    from geoparquet_io.core.extract_bigquery import extract_bigquery

    # Validate output early
    from geoparquet_io.core.streaming import StreamingError, validate_output

    try:
        validate_output(output_file)
    except StreamingError as e:
        raise click.ClickException(str(e)) from None

    # Validate .parquet extension
    validate_parquet_extension(output_file, any_extension)

    # Parse row group options
    row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

    extract_bigquery(
        table_id=table_id,
        output_parquet=output_file,
        project=project,
        credentials_file=credentials_file,
        where=where,
        bbox=bbox,
        bbox_mode=bbox_mode,
        bbox_threshold=bbox_threshold,
        limit=limit,
        include_cols=include_cols,
        exclude_cols=exclude_cols,
        geography_column=geography_column,
        geometry_format=geometry_format,
        edges=edges,
        dry_run=dry_run,
        show_sql=show_sql,
        verbose=verbose,
        compression=compression.upper(),
        compression_level=compression_level,
        row_group_size_mb=row_group_mb,
        row_group_rows=row_group_size,
        geoparquet_version=geoparquet_version,
        overwrite=overwrite,
        repair_geometry=repair_geometry,
        memory_limit=write_memory,
    )


def _deprecated_version_callback(ctx, param, value):
    """Callback to warn about deprecated --version flag."""
    if value is not None:
        import click

        click.echo(
            "Warning: --version is deprecated, use --wfs-version instead",
            err=True,
        )
    return value


@extract.command(name="wfs")
@handle_geoparquet_errors
@click.argument("service_url")
@click.argument("typename", required=False)
@click.argument("output_file", required=False, type=click.Path())
@click.option(
    "--wfs-version",
    "wfs_version",
    default="1.1.0",
    type=click.Choice(["auto", "2.0.0", "1.1.0", "1.0.0"]),
    help="WFS protocol version. 'auto' tries 2.0.0, then 1.1.0, then 1.0.0. Default: 1.1.0",
)
@click.option(
    "--version",
    "deprecated_version",
    type=click.Choice(["auto", "2.0.0", "1.1.0", "1.0.0"]),
    hidden=True,
    callback=_deprecated_version_callback,
    expose_value=True,
    is_eager=True,
    help="Deprecated: use --wfs-version instead",
)
@click.option(
    "--axis-order",
    type=click.Choice(["auto", "xy", "latlon"]),
    default="auto",
    help="Bbox axis order. 'auto' (default) detects from CRS format. "
    "'xy' forces lon,lat order. 'latlon' forces lat,lon order.",
)
@click.option(
    "--strict-crs",
    is_flag=True,
    help="Fail if server returns coordinates that don't match requested CRS. "
    "Without this flag, a warning is shown and detected CRS is used.",
)
@click.option(
    "--bbox",
    help="Bounding box: xmin,ymin,xmax,ymax in WGS84",
)
@click.option(
    "--bbox-mode",
    type=click.Choice(["auto", "server", "local"]),
    default="auto",
    help="Bbox filter mode: 'auto' (default) chooses based on server capabilities, "
    "'server' forces server-side filtering, 'local' forces client-side filtering",
)
@click.option(
    "--limit",
    type=click.IntRange(0, None),
    help="Maximum number of features to extract. Must be non-negative.",
)
@click.option(
    "--output-crs",
    help="Request specific CRS from server (e.g., EPSG:4326, urn:ogc:def:crs:EPSG::4326)",
)
@click.option(
    "--workers",
    type=click.IntRange(min=1, max=10),
    default=1,
    help="Parallel requests for large datasets. Default: 1 (single streaming request). "
    "Use 2-4 for datasets with 1M+ features to avoid timeouts.",
)
@click.option(
    "--page-size",
    type=click.IntRange(1000, 500000),
    default=DEFAULT_WFS_PAGE_SIZE,
    show_default=True,
    help="Features per page when using --workers > 1.",
)
@click.option(
    "--parallel-layers",
    type=click.IntRange(min=1, max=10),
    default=1,
    help="Number of layers to extract concurrently when extracting multiple layers. "
    "Default: 1 (sequential). Use with comma-separated typename for parallel extraction.",
)
@click.option(
    "--auto-tile/--no-auto-tile",
    default=True,
    help="Automatically subdivide into spatial tiles when server caps responses "
    "(e.g., maxFeatures or startIndex limits). Enabled by default. "
    "Use --no-auto-tile to disable and accept partial data.",
)
@click.option(
    "--sort-by",
    help="Attribute to sort by for stable pagination. Required for layers without a "
    "primary key on GeoServer. If not specified, auto-detected from DescribeFeatureType.",
)
@click.option(
    "--skip-hilbert",
    is_flag=True,
    help="Skip Hilbert curve sorting (faster, but no spatial clustering)",
)
@click.option(
    "--skip-bbox",
    is_flag=True,
    help="Skip adding bbox column (faster, but no per-geometry bbox)",
)
@repair_geometry_option
@compression_options
@row_group_options
@geoparquet_version_option
@overwrite_option
@verbose_option
@any_extension_option
def extract_wfs_cmd(
    service_url,
    typename,
    output_file,
    wfs_version,
    deprecated_version,
    axis_order,
    strict_crs,
    bbox,
    bbox_mode,
    limit,
    output_crs,
    workers,
    page_size,
    parallel_layers,
    auto_tile,
    sort_by,
    skip_hilbert,
    skip_bbox,
    repair_geometry,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    geoparquet_version,
    overwrite,
    verbose,
    any_extension,
):
    """
    Extract WFS (Web Feature Service) to GeoParquet.

    SERVICE_URL is the WFS service endpoint URL.

    TYPENAME is the layer to extract (e.g., 'roads', 'buildings').
    Use comma-separated names for multiple layers (e.g., 'roads,buildings,parcels').
    If omitted, lists available layers.

    OUTPUT_FILE is the output path. For single layer, this is the output file.
    For multiple layers, this is the output directory (each layer saved as typename.parquet).

    \b
    Examples:

        \b
        # List available layers
        gpio extract wfs https://geo.example.com/wfs

        \b
        # Extract single layer
        gpio extract wfs https://geo.example.com/wfs cities output.parquet

        \b
        # Extract multiple layers in parallel to a directory
        gpio extract wfs https://geo.example.com/wfs roads,buildings,parcels ./output/ \\
            --workers 2 --parallel-layers 3

        \b
        # With bbox filter (server-side when supported)
        gpio extract wfs https://geo.example.com/wfs roads output.parquet \\
            --bbox -122.5,37.5,-122.0,38.0

        \b
        # Force specific CRS and use parallel extraction
        gpio extract wfs https://geo.example.com/wfs buildings output.parquet \\
            --output-crs EPSG:4326 --workers 3

        \b
        # Limit features and skip optimizations for faster extraction
        gpio extract wfs https://geo.example.com/wfs parcels output.parquet \\
            --limit 10000 --skip-hilbert --skip-bbox
    """
    from geoparquet_io.core.wfs import (
        WFSError,
        convert_wfs_layers_to_directory,
        convert_wfs_to_geoparquet,
        list_available_layers,
        negotiate_wfs_version,
    )

    # Handle deprecated --version flag
    if deprecated_version is not None:
        wfs_version = deprecated_version

    # If no typename, list available layers
    if typename is None:
        try:
            # Handle auto version negotiation for listing
            if wfs_version == "auto":
                negotiated_version, _ = negotiate_wfs_version(service_url)
                layers = list_available_layers(service_url, version=negotiated_version)
            else:
                layers = list_available_layers(service_url, version=wfs_version)
        except WFSError as e:
            raise click.ClickException(str(e)) from None

        if not layers:
            click.echo("No layers found in WFS service.")
            return

        click.echo(f"Available layers in WFS service ({len(layers)} found):\n")
        for layer in layers:
            name = layer.get("typename", "unknown")
            title = layer.get("title", "")
            abstract = layer.get("abstract", "")

            click.echo(f"  {name}")
            if title and title != name:
                click.echo(f"    Title: {title}")
            if abstract:
                # Truncate long abstracts
                if len(abstract) > 100:
                    abstract = abstract[:97] + "..."
                click.echo(f"    Description: {abstract}")
            click.echo()
        return

    # Typename provided but no output file
    if output_file is None:
        raise click.ClickException(
            "OUTPUT_FILE is required when TYPENAME is specified.\n"
            f"Usage: gpio extract wfs {service_url} {typename} OUTPUT_FILE"
        )

    # Parse comma-separated typenames
    typenames = [t.strip() for t in typename.split(",") if t.strip()]
    if not typenames:
        raise click.ClickException(
            "No valid typename(s) provided. Specify one or more comma-separated layer names."
        )
    is_multi_layer = len(typenames) > 1

    # Validate output path based on single/multi layer mode
    if is_multi_layer:
        # Multi-layer mode: output_file is a directory
        if output_file.endswith(".parquet"):
            raise click.ClickException(
                f"Multiple layers specified ({len(typenames)}), but output looks like a file: {output_file}\n"
                "For multi-layer extraction, provide a directory path (e.g., ./output/)."
            )
    else:
        # Single layer mode: validate .parquet extension
        validate_parquet_extension(output_file, any_extension)

    # Parse row group options
    row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

    # Parse bbox if provided
    bbox_tuple = None
    if bbox:
        try:
            parts = [float(x.strip()) for x in bbox.split(",")]
            if len(parts) != 4:
                raise ValueError("Expected 4 values")
            bbox_tuple = tuple(parts)
        except ValueError as e:
            raise click.ClickException(
                f"Invalid bbox format: {bbox}\n"
                "Expected: xmin,ymin,xmax,ymax (e.g., -122.5,37.5,-122.0,38.0)"
            ) from e

    try:
        if is_multi_layer:
            # Multi-layer parallel extraction
            convert_wfs_layers_to_directory(
                service_url=service_url,
                typenames=typenames,
                output_dir=output_file,
                parallel_layers=parallel_layers,
                max_workers=workers,
                page_size=page_size,
                version=wfs_version,
                bbox=bbox_tuple,
                bbox_mode=bbox_mode,
                output_crs=output_crs,
                limit=limit,
                axis_order=axis_order,
                strict_crs=strict_crs,
                skip_hilbert=skip_hilbert,
                skip_bbox=skip_bbox,
                compression=compression.upper(),
                compression_level=compression_level,
                row_group_size_mb=row_group_mb,
                row_group_rows=row_group_size,
                geoparquet_version=geoparquet_version,
                overwrite=overwrite,
                verbose=verbose,
                auto_tile=auto_tile,
                sort_by=sort_by,
                repair_geometry=repair_geometry,
            )
        else:
            # Single layer extraction
            convert_wfs_to_geoparquet(
                service_url=service_url,
                typename=typenames[0],
                output_file=output_file,
                version=wfs_version,
                bbox=bbox_tuple,
                bbox_mode=bbox_mode,
                output_crs=output_crs,
                limit=limit,
                max_workers=workers,
                page_size=page_size,
                axis_order=axis_order,
                strict_crs=strict_crs,
                skip_hilbert=skip_hilbert,
                skip_bbox=skip_bbox,
                compression=compression.upper(),
                compression_level=compression_level,
                row_group_size_mb=row_group_mb,
                row_group_rows=row_group_size,
                geoparquet_version=geoparquet_version,
                overwrite=overwrite,
                verbose=verbose,
                auto_tile=auto_tile,
                sort_by=sort_by,
                repair_geometry=repair_geometry,
            )
    except WFSError as e:
        raise click.ClickException(str(e)) from None


@extract.command(name="carto")
@handle_geoparquet_errors
@click.argument("url")
@click.argument("table_name")
@click.argument("output_file", type=click.Path())
@click.option(
    "--where",
    help="SQL WHERE clause for filtering (e.g., \"status = 'active'\")",
)
@click.option(
    "--bbox",
    help="Bounding box filter: xmin,ymin,xmax,ymax in WGS84",
)
@click.option(
    "--limit",
    type=click.IntRange(0, None),
    help="Maximum number of rows to extract",
)
@click.option(
    "--include-cols",
    help="Comma-separated columns to include (default: all)",
)
@click.option(
    "--exclude-cols",
    help="Comma-separated columns to exclude",
)
@click.option(
    "--timeout",
    type=click.IntRange(1, 3600),
    default=120,
    help="Request timeout in seconds (default: 120)",
)
@click.option(
    "--skip-hilbert",
    is_flag=True,
    help="Skip Hilbert curve sorting (faster, but no spatial clustering)",
)
@click.option(
    "--skip-bbox",
    is_flag=True,
    help="Skip adding bbox column (faster, but no per-geometry bbox)",
)
@click.option(
    "--geometry/--no-geometry",
    "geometry",
    default=None,
    help="Force geometry (GeoParquet) or plain tabular (plain Parquet) "
    "extraction. Default: auto-detect from the table schema.",
)
@compression_options
@row_group_options
@geoparquet_version_option
@overwrite_option
@repair_geometry_option
@verbose_option
@any_extension_option
@aws_profile_option
@click.pass_context
def extract_carto_cmd(
    ctx,
    url,
    table_name,
    output_file,
    where,
    bbox,
    limit,
    include_cols,
    exclude_cols,
    timeout,
    skip_hilbert,
    skip_bbox,
    geometry,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    geoparquet_version,
    overwrite,
    repair_geometry,
    verbose,
    any_extension,
    aws_profile,
):
    """
    Extract Carto SQL API table to GeoParquet.

    URL is the Carto SQL API endpoint (e.g., https://phl.carto.com/api/v2/sql).
    You can also provide just the base domain (e.g., https://phl.carto.com).

    TABLE_NAME is the table to extract (e.g., 'opa_properties_public').

    OUTPUT_FILE is the output GeoParquet file path.

    \b
    Notes:
        - Geometry column 'the_geom' is renamed to 'geometry' for consistency
        - Tables with no geometry are written as plain Parquet (no geo metadata);
          use --no-geometry to force tabular extraction or --geometry to force
          GeoParquet (default: auto-detect)
        - Filters (--where, --bbox) are pushed to the server for efficiency
          (--bbox applies only to geometry tables)
        - For large tables, use --limit or --where to avoid timeouts
        - Set CARTO_API_KEY env var for authenticated endpoints

    \b
    Examples:

        \b
        # Extract entire table
        gpio extract carto https://phl.carto.com/api/v2/sql \\
            opa_properties_public output.parquet

        \b
        # With WHERE filter
        gpio extract carto https://phl.carto.com/api/v2/sql \\
            opa_properties_public output.parquet \\
            --where "category_code_description LIKE 'LAND%'"

        \b
        # With bbox filter
        gpio extract carto https://phl.carto.com/api/v2/sql \\
            opa_properties_public output.parquet \\
            --bbox "-75.2,39.9,-75.1,40.0"

        \b
        # Select specific columns and limit rows
        gpio extract carto https://phl.carto.com/api/v2/sql \\
            opa_properties_public output.parquet \\
            --include-cols "parcel_number,market_value,the_geom" \\
            --limit 10000
    """
    from geoparquet_io.core.carto import CartoError, convert_carto_to_geoparquet

    # Validate .parquet extension
    validate_parquet_extension(output_file, any_extension)

    # Parse row group options
    row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

    # Parse bbox if provided
    bbox_tuple = None
    if bbox:
        try:
            parts = [float(x.strip()) for x in bbox.split(",")]
            if len(parts) != 4:
                raise ValueError("Expected 4 values")
            bbox_tuple = tuple(parts)
        except ValueError as e:
            raise click.ClickException(
                f"Invalid bbox format: {bbox}\n"
                "Expected: xmin,ymin,xmax,ymax (e.g., -75.2,39.9,-75.1,40.0)"
            ) from e

    with _activate_s3(ctx, aws_profile=aws_profile):
        try:
            convert_carto_to_geoparquet(
                url=url,
                table_name=table_name,
                output_file=output_file,
                where=where,
                bbox=bbox_tuple,
                limit=limit,
                include_cols=include_cols,
                exclude_cols=exclude_cols,
                timeout=float(timeout),
                skip_hilbert=skip_hilbert,
                skip_bbox=skip_bbox,
                compression=compression.upper(),
                compression_level=compression_level,
                row_group_size_mb=row_group_mb,
                row_group_rows=row_group_size,
                geoparquet_version=geoparquet_version,
                overwrite=overwrite,
                verbose=verbose,
                repair_geometry=repair_geometry,
                geometry=geometry,
            )
        except CartoError as e:
            raise click.ClickException(str(e)) from None


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


# Sort commands group
@cli.group()
@click.pass_context
def sort(ctx):
    """Commands for sorting GeoParquet files."""
    # Ensure logging is set up (in case this group is invoked directly in tests)
    ctx.ensure_object(dict)
    timestamps = ctx.obj.get("timestamps", False)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


@sort.command(name="hilbert", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", type=click.Path(), required=False, default=None)
@click.option(
    "--geometry-column",
    "-g",
    default="geometry",
    help="Name of the geometry column (default: geometry)",
)
@click.option(
    "--add-bbox", is_flag=True, help="Automatically add bbox column and metadata if missing."
)
@output_format_options(default_rows=DEFAULT_SORT_ROW_GROUP_ROWS)
@geoparquet_version_option
@overwrite_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
def hilbert_order(
    ctx,
    input_parquet,
    output_parquet,
    geometry_column,
    add_bbox,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
    overwrite,
    verbose,
    any_extension,
    show_sql,
):
    """
    Reorder a GeoParquet file using Hilbert curve ordering.

    Takes an input GeoParquet file and creates a new file with rows ordered
    by their position along a Hilbert space-filling curve.

    Applies optimal formatting (configurable compression, optimized row groups,
    bbox metadata). A non-default CRS is carried through unchanged; a CRS that
    spells out the GeoParquet default (OGC:CRS84 / EPSG:4326) is normalized to
    how the spec writes it, an omitted crs key. Use --verbose to see when that
    happens.

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
            None,
            geoparquet_version,
            overwrite,
            memory_limit=write_memory,
        )


@sort.command(name="str", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", type=click.Path(), required=False, default=None)
@click.option(
    "--geometry-column",
    "-g",
    default="geometry",
    help="Name of the geometry column (default: geometry)",
)
@click.option(
    "--add-bbox", is_flag=True, help="Automatically add bbox column and metadata if missing."
)
@output_format_options(default_rows=DEFAULT_SORT_ROW_GROUP_ROWS)
@geoparquet_version_option
@overwrite_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
def str_order_command(
    ctx,
    input_parquet,
    output_parquet,
    geometry_column,
    add_bbox,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
    overwrite,
    verbose,
    any_extension,
    show_sql,
):
    """Pack GeoParquet rows with Sort-Tile-Recursive ordering.

    STR sorts geometry bounding-box centers into X strips, sorts each strip on
    Y, and alternates the Y direction between strips. This can produce tighter
    row-group bounding boxes than a space-filling curve.

    --row-group-size does double duty: it is the writer's row-group target, and
    it selects how many X strips STR builds, as
    ceil(sqrt(num_rows / row-group-size)). That makes it a coarse control -
    nearby values often produce an identical ordering. Rows are not packed into
    row-group-sized tiles, and because the writer rounds row groups up to a
    multiple of 2048, tiles and row groups only line up when --row-group-size
    is itself a multiple of 2048.
    """
    with _activate_s3(ctx):
        from geoparquet_io.core.streaming import StreamingError, validate_output

        try:
            validate_output(output_parquet)
        except StreamingError as e:
            raise click.ClickException(str(e)) from None

        validate_parquet_extension(output_parquet, any_extension)
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)
        str_impl(
            input_parquet,
            output_parquet,
            geometry_column=geometry_column,
            add_bbox_flag=add_bbox,
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


@sort.command(name="column", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", type=click.Path())
@click.argument("columns")
@click.option(
    "--descending",
    is_flag=True,
    help="Sort in descending order (default: ascending)",
)
@allow_schema_diff_option
@output_format_options(default_rows=DEFAULT_SORT_ROW_GROUP_ROWS)
@geoparquet_version_option
@overwrite_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
def sort_column(
    ctx,
    input_parquet,
    output_parquet,
    columns,
    descending,
    allow_schema_diff,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
    overwrite,
    verbose,
    any_extension,
    show_sql,
):
    """
    Sort a GeoParquet file by specified column(s).

    COLUMNS is a comma-separated list of column names to sort by.

    Examples:

        gpio sort column input.parquet output.parquet name

        gpio sort column input.parquet output.parquet name,date --descending

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.
    """
    with _activate_s3(ctx):
        # Validate .parquet extension
        validate_parquet_extension(output_parquet, any_extension)

        # Parse row group options
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        sort_by_column_impl(
            input_parquet,
            output_parquet,
            columns=columns,
            descending=descending,
            verbose=verbose,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            geoparquet_version=geoparquet_version,
            overwrite=overwrite,
            memory_limit=write_memory,
            allow_schema_diff=allow_schema_diff,
        )


@sort.command(name="quadkey", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", type=click.Path())
@click.option(
    "--quadkey-name",
    default="quadkey",
    help="Name of the quadkey column to sort by (default: quadkey)",
)
@click.option(
    "--resolution",
    default=13,
    type=click.IntRange(0, 23),
    help="Resolution when auto-adding quadkey column (0-23). Default: 13",
)
@click.option(
    "--use-centroid",
    is_flag=True,
    help="Use geometry centroid when auto-adding quadkey column",
)
@click.option(
    "--remove-quadkey-column",
    is_flag=True,
    help="Exclude quadkey column from output after sorting",
)
@allow_schema_diff_option
@output_format_options(default_rows=DEFAULT_SORT_ROW_GROUP_ROWS)
@geoparquet_version_option
@overwrite_option
@verbose_option
@any_extension_option
@show_sql_option
@click.pass_context
def sort_quadkey(
    ctx,
    input_parquet,
    output_parquet,
    quadkey_name,
    resolution,
    use_centroid,
    remove_quadkey_column,
    allow_schema_diff,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
    overwrite,
    verbose,
    any_extension,
    show_sql,
):
    """
    Sort a GeoParquet file by quadkey spatial index.

    If the quadkey column doesn't exist and using the default column name,
    it will be auto-added at the specified resolution. If using --quadkey-name
    and the column is missing, an error is raised.

    Use --remove-quadkey-column to exclude the quadkey column from output
    after sorting (useful when you only want the sorted order).

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.
    """
    with _activate_s3(ctx):
        # Validate .parquet extension
        validate_parquet_extension(output_parquet, any_extension)

        # Parse row group options
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        sort_by_quadkey_impl(
            input_parquet,
            output_parquet,
            quadkey_column_name=quadkey_name,
            resolution=resolution,
            use_centroid=use_centroid,
            remove_quadkey_column=remove_quadkey_column,
            verbose=verbose,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            geoparquet_version=geoparquet_version,
            overwrite=overwrite,
            memory_limit=write_memory,
            allow_schema_diff=allow_schema_diff,
        )


@cli.group()
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


# Partition commands group
@cli.group()
@click.pass_context
def partition(ctx):
    """Commands for partitioning GeoParquet files."""
    # Ensure logging is set up (in case this group is invoked directly in tests)
    ctx.ensure_object(dict)
    timestamps = ctx.obj.get("timestamps", False)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


@partition.command(name="admin", cls=SingleFileCommand)
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
    required=False,
    default=None,
    help="Comma-separated hierarchical levels to partition by. "
    "GAUL levels: continent,country,department. "
    "Overture levels: country,region. "
    "Not required when --vecorel is used.",
)
@click.option(
    "--vecorel",
    is_flag=True,
    help="Output Vecorel-compliant admin columns (admin:country_code, "
    "admin:subdivision_code) in each partition with schema metadata. "
    "Automatically uses the Overture dataset with country,region levels.",
)
@partition_options_base
@output_format_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def partition_admin(
    ctx,
    input_parquet,
    output_folder,
    dataset,
    levels,
    vecorel,
    hive,
    overwrite,
    preview,
    preview_limit,
    force,
    skip_analysis,
    prefix,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    verbose,
    geoparquet_version,
    show_sql,
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
    # Vecorel-compliant partitions (forces Overture country,region)
    gpio partition admin input.parquet output/ --vecorel

    \b
    **Note:** This command fetches remote boundaries and performs spatial intersection.
    Requires internet connection. Input data must have valid geometries in WGS84 or
    compatible CRS.
    """
    with _activate_s3(ctx):
        # If preview mode, output_folder is not required
        if not preview and not output_folder:
            raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

        # Validate mutual exclusivity of row group options and get MB value
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        # Handle --vecorel: force Overture dataset with country,region levels
        if vecorel:
            from geoparquet_io.core.logging_config import warn as log_warn

            if dataset != "overture":
                log_warn(f"--vecorel overrides --dataset {dataset} to 'overture'")
            dataset = "overture"
            if levels:
                log_warn("--vecorel overrides --levels to 'country,region'")
            level_list = ["country", "region"]
        elif levels:
            level_list = [level.strip() for level in levels.split(",")]
        else:
            raise click.UsageError("Either --levels or --vecorel is required")

        # Use hierarchical partitioning (spatial join + partition)
        partition_admin_hierarchical_impl(
            input_parquet,
            output_folder,
            dataset_name=dataset,
            levels=level_list,
            vecorel=vecorel,
            hive=hive,
            overwrite=overwrite,
            preview=preview,
            preview_limit=preview_limit,
            verbose=verbose,
            force=force,
            skip_analysis=skip_analysis,
            filename_prefix=prefix,
            geoparquet_version=geoparquet_version,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            memory_limit=write_memory,
        )


@partition.command(name="string", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_folder", required=False)
@click.option("--column", required=True, help="Column name to partition by (required)")
@click.option("--chars", type=int, help="Number of characters to use as prefix for partitioning")
@partition_options_base
@output_format_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def partition_string(
    ctx,
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
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    verbose,
    geoparquet_version,
    show_sql,
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
    with _activate_s3(ctx):
        from geoparquet_io.core.streaming import StreamingError

        # If preview mode, output_folder is not required
        if not preview and not output_folder:
            raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

        # Validate mutual exclusivity of row group options and get MB value
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        try:
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
                None,
                geoparquet_version,
                compression=compression.upper(),
                compression_level=compression_level,
                row_group_size_mb=row_group_mb,
                row_group_rows=row_group_size,
                memory_limit=write_memory,
            )
        except StreamingError as e:
            raise click.ClickException(str(e)) from None


@partition.command(name="h3", cls=SingleFileCommand)
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
    default=None,
    help="H3 resolution for partitioning (0-15). Required unless --auto is used.",
)
@click.option(
    "--auto",
    is_flag=True,
    help="Automatically calculate optimal resolution based on data size",
)
@click.option(
    "--target-rows",
    type=int,
    default=100000,
    help="Target rows per partition for auto mode (default: 100000)",
)
@click.option(
    "--max-partitions",
    type=int,
    default=10000,
    help="Maximum number of partitions for auto mode (default: 10000)",
)
@click.option(
    "--keep-h3-column",
    is_flag=True,
    help="Keep the H3 column in output files (default: excluded for non-Hive, included for Hive)",
)
@partition_options
@output_format_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def partition_h3(
    ctx,
    input_parquet,
    output_folder,
    h3_name,
    resolution,
    auto,
    target_rows,
    max_partitions,
    keep_h3_column,
    hive,
    overwrite,
    preview,
    preview_limit,
    force,
    skip_analysis,
    min_size,
    in_place,
    prefix,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Partition a GeoParquet file by H3 cells at specified resolution.

    Creates separate GeoParquet files based on H3 cell prefixes at the specified resolution.
    If the H3 column doesn't exist, it will be automatically added before partitioning.

    By default, the H3 column is excluded from output files (since it's redundant with the
    partition path) unless using Hive-style partitioning. Use --keep-h3-column to explicitly
    keep the column in all cases.

    Auto-resolution mode: Use --auto to automatically calculate the optimal H3 resolution
    based on your data size. Control partition sizing with --target-rows (default: 100K rows
    per partition) and --max-partitions (default: 10K partitions max).

    Use --preview to see what partitions would be created without actually creating files.

    Examples:

        # Auto-calculate optimal resolution for ~100K rows per partition
        gpio partition h3 input.parquet output/ --auto

        # Auto-calculate with custom target partition size
        gpio partition h3 input.parquet output/ --auto --target-rows 50000

        # Preview partitions at resolution 7 (~5km² cells)
        gpio partition h3 input.parquet --resolution 7 --preview

        # Partition by H3 cells at specific resolution 9
        gpio partition h3 input.parquet output/ --resolution 9

        # Partition with H3 column kept in output files
        gpio partition h3 input.parquet output/ --resolution 9 --keep-h3-column

        # Use Hive-style partitioning at resolution 8 (H3 column included by default)
        gpio partition h3 input.parquet output/ --resolution 8 --hive

        # Sub-partition all files over 100MB in a directory
        gpio partition h3 /data/partitions/ --min-size 100MB --resolution 4 --in-place
    """
    with _activate_s3(ctx):
        # Handle directory input with --min-size
        if handle_directory_sub_partition(
            input_parquet=input_parquet,
            partition_type="h3",
            min_size=min_size,
            resolution=resolution,
            preview=preview,
            column_name=h3_name,
            output_folder=output_folder,
            in_place=in_place,
            hive=hive,
            overwrite=overwrite,
            verbose=verbose,
            force=force,
            skip_analysis=skip_analysis,
            compression=compression,
            compression_level=compression_level,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
        ):
            return

        # Existing single-file logic continues below...

        # If preview mode, output_folder is not required
        if not preview and not output_folder:
            raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

        # Validate mutual exclusivity of row group options and get MB value
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

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
            None,
            geoparquet_version,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            memory_limit=write_memory,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
        )


@partition.command(name="s2", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_folder", required=False)
@click.option(
    "--s2-name",
    default="s2_cell",
    help="Name of S2 column to partition by (default: s2_cell)",
)
@click.option(
    "--level",
    type=click.IntRange(0, 30),
    default=None,
    help="S2 level for partitioning (0-30). Required unless --auto is used.",
)
@click.option(
    "--auto",
    is_flag=True,
    help="Automatically calculate optimal level based on data size",
)
@click.option(
    "--target-rows",
    type=int,
    default=100000,
    help="Target rows per partition when using --auto (default: 100000)",
)
@click.option(
    "--max-partitions",
    type=int,
    default=10000,
    help="Maximum partitions when using --auto (default: 10000)",
)
@click.option(
    "--keep-s2-column",
    is_flag=True,
    help="Keep the S2 column in output files (default: excluded for non-Hive, included for Hive)",
)
@partition_options
@output_format_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def partition_s2(
    ctx,
    input_parquet,
    output_folder,
    s2_name,
    level,
    auto,
    target_rows,
    max_partitions,
    keep_s2_column,
    hive,
    overwrite,
    preview,
    preview_limit,
    force,
    skip_analysis,
    min_size,
    in_place,
    prefix,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Partition a GeoParquet file by S2 cells at specified level.

    Creates separate GeoParquet files based on S2 cell tokens. If the S2 column
    doesn't exist, it will be automatically added before partitioning.

    S2 (Google's Spherical Geometry library) uses a hierarchical quadtree structure
    that divides Earth's surface into cells. Level 0 has 6 base cells, and each
    subsequent level subdivides by 4.

    By default, the S2 column is excluded from output files (since it's redundant with the
    partition path) unless using Hive-style partitioning. Use --keep-s2-column to explicitly
    keep the column in all cases.

    Use --preview to see what partitions would be created without actually creating files.

    Auto-resolution mode: Use --auto to automatically calculate the optimal S2 level
    based on your target partition size. Specify --target-rows (default: 100K) to control
    partition granularity.

    Examples:

        # Auto-calculate optimal level for ~100K rows per partition
        gpio partition s2 input.parquet output/ --auto

        # Auto with custom target size (fewer, larger partitions)
        gpio partition s2 input.parquet output/ --auto --target-rows 500000

        # Preview partitions at level 10 (~78km² cells)
        gpio partition s2 input.parquet --level 10 --preview

        # Partition by S2 cells at level 13 (~1.2km² cells)
        gpio partition s2 input.parquet output/ --level 13

        # Partition with S2 column kept in output files
        gpio partition s2 input.parquet output/ --level 12 --keep-s2-column

        # Use Hive-style partitioning (S2 column included by default)
        gpio partition s2 input.parquet output/ --auto --hive

        # Sub-partition all files over 100MB in a directory
        gpio partition s2 /data/partitions/ --min-size 100MB --level 10 --in-place
    """
    with _activate_s3(ctx):
        # Handle directory input with --min-size
        if handle_directory_sub_partition(
            input_parquet=input_parquet,
            partition_type="s2",
            min_size=min_size,
            level=level,  # S2 uses "level" not "resolution"
            preview=preview,
            column_name=s2_name,
            output_folder=output_folder,
            in_place=in_place,
            hive=hive,
            overwrite=overwrite,
            verbose=verbose,
            force=force,
            skip_analysis=skip_analysis,
            compression=compression,
            compression_level=compression_level,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
        ):
            return

        # If preview mode, output_folder is not required
        if not preview and not output_folder:
            raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

        # Validate mutual exclusivity of row group options and get MB value
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        # Convert flag to None if not explicitly set, so implementation can determine default
        keep_s2_col = True if keep_s2_column else None

        partition_by_s2_impl(
            input_parquet,
            output_folder,
            s2_name,
            level,
            hive,
            overwrite,
            preview,
            preview_limit,
            verbose,
            keep_s2_col,
            force,
            skip_analysis,
            prefix,
            None,
            geoparquet_version,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            memory_limit=write_memory,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
        )


@partition.command(name="a5", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_folder", required=False)
@click.option(
    "--a5-name",
    default="a5_cell",
    help="Name of A5 column to partition by (default: a5_cell)",
)
@click.option(
    "--resolution",
    type=click.IntRange(0, 30),
    default=None,
    help="A5 resolution for partitioning (0-30). Required unless --auto is used.",
)
@click.option(
    "--auto",
    is_flag=True,
    help="Automatically calculate optimal resolution based on data size",
)
@click.option(
    "--target-rows",
    type=int,
    default=100000,
    help="Target rows per partition when using --auto (default: 100000)",
)
@click.option(
    "--max-partitions",
    type=int,
    default=10000,
    help="Maximum partitions when using --auto (default: 10000)",
)
@click.option(
    "--keep-a5-column",
    is_flag=True,
    help="Keep the A5 column in output files (default: excluded for non-Hive, included for Hive)",
)
@partition_options
@output_format_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def partition_a5(
    ctx,
    input_parquet,
    output_folder,
    a5_name,
    resolution,
    auto,
    target_rows,
    max_partitions,
    keep_a5_column,
    hive,
    overwrite,
    preview,
    preview_limit,
    force,
    skip_analysis,
    min_size,
    in_place,
    prefix,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Partition a GeoParquet file by A5 cells at specified resolution.

    Creates separate GeoParquet files based on A5 cell IDs at the specified resolution.
    If the A5 column doesn't exist, it will be automatically added before partitioning.

    By default, the A5 column is excluded from output files (since it's redundant with the
    partition path) unless using Hive-style partitioning. Use --keep-a5-column to explicitly
    keep the column in all cases.

    Use --preview to see what partitions would be created without actually creating files.

    Auto-resolution mode: Use --auto to automatically calculate the optimal A5 resolution
    based on your target partition size. Specify --target-rows (default: 100K) to control
    partition granularity.

    Examples:

        # Auto-calculate optimal resolution for ~100K rows per partition
        gpio partition a5 input.parquet output/ --auto

        # Auto with custom target size (fewer, larger partitions)
        gpio partition a5 input.parquet output/ --auto --target-rows 500000

        # Preview partitions at resolution 10 (~41km² cells)
        gpio partition a5 input.parquet --resolution 10 --preview

        # Partition by A5 cells at resolution 15
        gpio partition a5 input.parquet output/ --resolution 15

        # Partition with A5 column kept in output files
        gpio partition a5 input.parquet output/ --resolution 12 --keep-a5-column

        # Use Hive-style partitioning (A5 column included by default)
        gpio partition a5 input.parquet output/ --auto --hive

        # Sub-partition all files over 100MB in a directory
        gpio partition a5 /data/partitions/ --min-size 100MB --resolution 10 --in-place
    """
    with _activate_s3(ctx):
        # Handle directory input with --min-size
        if handle_directory_sub_partition(
            input_parquet=input_parquet,
            partition_type="a5",
            min_size=min_size,
            resolution=resolution,
            preview=preview,
            column_name=a5_name,
            output_folder=output_folder,
            in_place=in_place,
            hive=hive,
            overwrite=overwrite,
            verbose=verbose,
            force=force,
            skip_analysis=skip_analysis,
            compression=compression,
            compression_level=compression_level,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
        ):
            return

        # If preview mode, output_folder is not required
        if not preview and not output_folder:
            raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

        # Validate mutual exclusivity of row group options and get MB value
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        # Convert flag to None if not explicitly set, so implementation can determine default
        keep_a5_col = True if keep_a5_column else None

        partition_by_a5_impl(
            input_parquet,
            output_folder,
            a5_name,
            resolution,
            hive,
            overwrite,
            preview,
            preview_limit,
            verbose,
            keep_a5_col,
            force,
            skip_analysis,
            prefix,
            None,
            geoparquet_version,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            memory_limit=write_memory,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
        )


@partition.command(name="kdtree", cls=SingleFileCommand)
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
@partition_options_base
@output_format_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def partition_kdtree(
    ctx,
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
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    verbose,
    geoparquet_version,
    show_sql,
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
    with _activate_s3(ctx):
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

        # Validate mutual exclusivity of row group options and get MB value
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

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
            None,
            geoparquet_version,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            memory_limit=write_memory,
        )


@partition.command(name="quadkey", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_folder", required=False)
@click.option(
    "--quadkey-column",
    default="quadkey",
    help="Name of quadkey column to partition by (default: quadkey)",
)
@click.option(
    "--resolution",
    type=click.IntRange(0, 23),
    default=None,
    help="Resolution for auto-adding quadkey column (0-23). Required unless --auto is used.",
)
@click.option(
    "--partition-resolution",
    type=click.IntRange(0, 23),
    default=None,
    help="Resolution for partitioning as prefix length (0-23). Required unless --auto is used.",
)
@click.option(
    "--auto",
    is_flag=True,
    help="Automatically calculate optimal resolution based on data size",
)
@click.option(
    "--target-rows",
    type=int,
    default=100000,
    help="Target rows per partition when using --auto (default: 100000)",
)
@click.option(
    "--max-partitions",
    type=int,
    default=10000,
    help="Maximum partitions when using --auto (default: 10000)",
)
@click.option(
    "--use-centroid",
    is_flag=True,
    help="Use geometry centroid when auto-adding quadkey column",
)
@click.option(
    "--keep-quadkey-column",
    is_flag=True,
    help="Keep the quadkey column in output files (default: excluded for non-Hive, included for Hive)",
)
@partition_options
@output_format_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def partition_quadkey(
    ctx,
    input_parquet,
    output_folder,
    quadkey_column,
    resolution,
    partition_resolution,
    auto,
    target_rows,
    max_partitions,
    use_centroid,
    keep_quadkey_column,
    hive,
    overwrite,
    preview,
    preview_limit,
    force,
    skip_analysis,
    min_size,
    in_place,
    prefix,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Partition a GeoParquet file by quadkey cells.

    Creates separate GeoParquet files based on quadkey prefixes at the specified
    partition resolution. If the quadkey column doesn't exist, it will be automatically
    added at the specified resolution before partitioning.

    The column is created at --resolution, but partitions are created using
    the first --partition-resolution characters of each quadkey. This allows
    for coarser partitioning while retaining full precision in the column.

    By default, the quadkey column is excluded from output files (since it's redundant
    with the partition path) unless using Hive-style partitioning. Use --keep-quadkey-column
    to explicitly keep the column in all cases.

    Use --preview to see what partitions would be created without actually creating files.

    Auto-resolution mode: Use --auto to automatically calculate the optimal quadkey zoom
    level based on your target partition size. Specify --target-rows (default: 100K) to
    control partition granularity.

    Examples:

        # Auto-calculate optimal resolution for ~100K rows per partition
        gpio partition quadkey input.parquet output/ --auto

        # Auto with custom target size (fewer, larger partitions)
        gpio partition quadkey input.parquet output/ --auto --target-rows 500000

        # Preview partitions with auto-resolution
        gpio partition quadkey input.parquet --auto --preview

        # Partition by quadkey cells at specific resolutions
        gpio partition quadkey input.parquet output/ --resolution 13 --partition-resolution 9

        # Partition with quadkey column kept in output files
        gpio partition quadkey input.parquet output/ --resolution 13 --partition-resolution 9 --keep-quadkey-column

        # Use Hive-style partitioning (quadkey column included by default)
        gpio partition quadkey input.parquet output/ --auto --hive

        # Sub-partition all files over 100MB in a directory
        gpio partition quadkey /data/partitions/ --min-size 100MB --auto --in-place
    """
    with _activate_s3(ctx):
        # Handle directory input with --min-size
        if handle_directory_sub_partition(
            input_parquet=input_parquet,
            partition_type="quadkey",
            min_size=min_size,
            resolution=resolution,
            partition_resolution=partition_resolution,
            use_centroid=use_centroid,
            preview=preview,
            column_name=quadkey_column,
            output_folder=output_folder,
            in_place=in_place,
            hive=hive,
            overwrite=overwrite,
            verbose=verbose,
            force=force,
            skip_analysis=skip_analysis,
            compression=compression,
            compression_level=compression_level,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
        ):
            return

        # If preview mode, output_folder is not required
        if not preview and not output_folder:
            raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

        # Validate mutual exclusivity of row group options and get MB value
        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

        # Convert flag to None if not explicitly set, so implementation can determine default
        keep_quadkey_col = True if keep_quadkey_column else None

        partition_by_quadkey_impl(
            input_parquet,
            output_folder,
            quadkey_column_name=quadkey_column,
            resolution=resolution,
            partition_resolution=partition_resolution,
            use_centroid=use_centroid,
            hive=hive,
            overwrite=overwrite,
            preview=preview,
            preview_limit=preview_limit,
            verbose=verbose,
            keep_quadkey_column=keep_quadkey_col,
            force=force,
            skip_analysis=skip_analysis,
            filename_prefix=prefix,
            geoparquet_version=geoparquet_version,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            memory_limit=write_memory,
            auto=auto,
            target_rows=target_rows,
            max_partitions=max_partitions,
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


def _stac_impl(input, output, bucket, public_url, collection_id, item_id, overwrite, verbose):
    """Shared STAC generation implementation for both command paths."""
    from pathlib import Path

    from geoparquet_io.core.stac import detect_stac

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


# Publish commands group
@cli.group()
@click.pass_context
def publish(ctx):
    """Commands for publishing GeoParquet data (STAC metadata, cloud uploads)."""
    ctx.ensure_object(dict)
    timestamps = ctx.obj.get("timestamps", False)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


@publish.command(name="stac")
@handle_geoparquet_errors
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
def publish_stac(input, output, bucket, public_url, collection_id, item_id, overwrite, verbose):
    """
    Generate STAC Item or Collection from GeoParquet file(s).

    Single file -> STAC Item JSON

    Partitioned directory -> STAC Collection + Items (co-located with data)

    For partitioned datasets, Items are written alongside their parquet files
    following STAC best practices. collection.json is written to OUTPUT.

    Automatically detects PMTiles overview files and includes them as assets.

    Examples:

      \b
      # Single file
      gpio publish stac input.parquet output.json --bucket s3://my-bucket/roads/

      \b
      # Partitioned dataset - Items written next to parquet files
      gpio publish stac partitions/ . --bucket s3://my-bucket/roads/

      \b
      # With public URL mapping
      gpio publish stac data.parquet output.json \\
        --bucket s3://my-bucket/roads/ \\
        --public-url https://data.example.com/roads/
    """
    _stac_impl(input, output, bucket, public_url, collection_id, item_id, overwrite, verbose)


@publish.command(name="upload")
@handle_geoparquet_errors
@click.argument("source", type=click.Path(exists=True, path_type=Path))
@click.argument("destination", type=str)
@aws_profile_option
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
@click.option(
    "--s3-endpoint",
    help="Custom S3-compatible endpoint (e.g., 'minio.example.com:9000')",
    hidden=True,
)
@click.option(
    "--s3-region",
    help="S3 region (default: us-east-1 when using custom endpoint)",
    hidden=True,
)
@click.option(
    "--s3-no-ssl",
    is_flag=True,
    help="Disable SSL for S3 endpoint (use HTTP instead of HTTPS)",
    hidden=True,
)
@verbose_option
@dry_run_option
@click.pass_context
def publish_upload(
    ctx,
    source,
    destination,
    aws_profile,
    pattern,
    max_files,
    chunk_concurrency,
    chunk_size,
    fail_fast,
    s3_endpoint,
    s3_region,
    s3_no_ssl,
    dry_run,
    verbose,
):
    """Upload file or directory to object storage.

    Supports S3, GCS, Azure, and HTTP destinations. Automatically handles
    multipart uploads and preserves directory structure.

    Azure destinations name the storage account first:
    az://<account>/<container>/<path>. The account comes from the URL; the
    credential comes from the environment (AZURE_STORAGE_ACCOUNT_KEY or its
    aliases, AZURE_STORAGE_SAS_TOKEN/AZURE_STORAGE_SAS_KEY, the
    AZURE_STORAGE_CLIENT_* client-secret vars, or AZURE_USE_AZURE_CLI=true
    to use an az login session -- az login alone is not enough).

    \b
    Examples:
      # Single file to S3
      gpio publish upload data.parquet s3://bucket/path/data.parquet --aws-profile source-coop

      \b
      # Single file to Azure Blob Storage (account first, then container)
      gpio publish upload data.parquet az://myaccount/mycontainer/data.parquet

      \b
      # Directory to GCS (preserves structure, uploads files in parallel)
      gpio publish upload output/ gs://bucket/dataset/

      \b
      # Only parquet files with increased parallelism
      gpio publish upload output/ s3://bucket/dataset/ --pattern "*.parquet" --max-files 8

      \b
      # Stop on first error instead of continuing
      gpio publish upload output/ s3://bucket/dataset/ --fail-fast
    """
    with _activate_s3(
        ctx,
        aws_profile=aws_profile,
        s3_endpoint=s3_endpoint,
        s3_region=s3_region,
        s3_no_ssl=s3_no_ssl,
    ) as s3_config:
        # Check credentials before attempting upload
        creds_ok, hint = check_credentials(destination, s3_config.get("profile"))
        if not creds_ok:
            raise click.ClickException(f"Authentication failed:\n\n{hint}")

        upload_impl(
            source=source,
            destination=destination,
            profile=s3_config["profile"],
            pattern=pattern,
            max_files=max_files,
            chunk_concurrency=chunk_concurrency,
            chunk_size=chunk_size,
            fail_fast=fail_fast,
            dry_run=dry_run,
            s3_endpoint=s3_config["s3_endpoint"],
            s3_region=s3_config["s3_region"],
            s3_use_ssl=s3_config["s3_use_ssl"],
        )


@check.command(name="stac")
@handle_geoparquet_errors
@click.argument("stac_file")
@verbose_option
@click.pass_context
def check_stac_cmd(ctx, stac_file, verbose):
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
    with _activate_s3(ctx):
        from geoparquet_io.core.remote import setup_aws_profile_if_needed, validate_profile_for_urls
        from geoparquet_io.core.stac_check import check_stac

        # Validate profile is only used with S3
        validate_profile_for_urls(None, stac_file)

        # Setup AWS profile if needed
        setup_aws_profile_if_needed(None, stac_file)

        check_stac(stac_file, verbose)


@check.command(name="spec", cls=GlobAwareCommand)
@click.argument("parquet_file")
@click.option(
    "--geoparquet-version",
    type=click.Choice(["1.0", "1.1", "2.0", "parquet-geo-only"]),
    default=None,
    help="Validate against specific GeoParquet version (default: auto-detect)",
)
@click.option(
    "--json",
    "json_output",
    is_flag=True,
    help="Output as JSON for machine parsing",
)
@click.option(
    "--skip-data-validation",
    is_flag=True,
    help="Skip validation of actual data values against metadata claims",
)
@click.option(
    "--sample-size",
    type=click.IntRange(0, None),
    default=1000,
    show_default=True,
    help="Number of rows to sample for data validation (0 = all rows)",
)
@verbose_option
@click.pass_context
def check_spec(
    ctx,
    parquet_file,
    geoparquet_version,
    json_output,
    skip_data_validation,
    sample_size,
    verbose,
):
    """
    Validate a GeoParquet file against specification requirements.

    Checks file structure, metadata, and optionally data consistency against
    the GeoParquet specification. Automatically detects the file version unless
    --geoparquet-version is specified.

    Supports GeoParquet 1.0, 1.1, 2.0, and Parquet native geo types.

    \b
    Exit codes:
      0 - All checks passed
      1 - One or more checks failed
      2 - Warnings only (all required checks passed)

    \b
    Examples:
      # Basic validation (auto-detect version)
      gpio check spec data.parquet

      \b
      # Validate against specific version
      gpio check spec data.parquet --geoparquet-version 1.1

      \b
      # JSON output for CI/CD
      gpio check spec data.parquet --json

      \b
      # Skip data validation for faster check
      gpio check spec data.parquet --skip-data-validation
    """
    from geoparquet_io.core.remote import (
        setup_aws_profile_if_needed,
        validate_profile_for_urls,
    )
    from geoparquet_io.core.validate import (
        format_json_output as format_json,
    )
    from geoparquet_io.core.validate import (
        format_terminal_output as format_terminal,
    )
    from geoparquet_io.core.validate import (
        validate_geoparquet,
    )

    configure_verbose(verbose)

    with _activate_s3(ctx):
        # Validate profile is only used with S3
        validate_profile_for_urls(None, parquet_file)
        setup_aws_profile_if_needed(None, parquet_file)

        result = validate_geoparquet(
            parquet_file,
            target_version=geoparquet_version,
            validate_data=not skip_data_validation,
            sample_size=sample_size,
            verbose=verbose,
        )

        if json_output:
            click.echo(format_json(result))
        else:
            format_terminal(result)

        # Exit codes: 0=passed, 1=failed, 2=warnings only
        if result.failed_count > 0:
            raise click.exceptions.Exit(1)
        elif result.warning_count > 0:
            raise click.exceptions.Exit(2)
        # Exit 0 is implicit when no exception is raised


@check.command(name="optimization", cls=GlobAwareCommand)
@click.argument("parquet_file")
@click.option("--verbose", is_flag=True, help="Print detailed diagnostics")
@check_partition_options
@click.pass_context
def check_optimization_cmd(
    ctx,
    parquet_file,
    verbose,
    check_all_files,
    check_sample,
):
    """Check combined spatial query optimization.

    Evaluates five factors that affect spatial query performance:

    \b
    1. Native geo types (v2.0 or parquet-geo-only)
    2. Per-row-group geo bbox statistics
    3. Spatial sorting (Hilbert or similar)
    4. Row group size (10k-50k rows optimal)
    5. ZSTD compression on geometry column

    \b
    Scoring:
      5/5  Fully optimized for spatial queries
      3-4  Partially optimized, improvements possible
      0-2  Not optimized for spatial queries

    \b
    Examples:
      gpio check optimization data.parquet
      gpio check optimization data.parquet --verbose
    """
    from geoparquet_io.core.check_optimization import check_optimization
    from geoparquet_io.core.partition.reader import get_files_to_check

    configure_verbose(verbose)

    with _activate_s3(ctx):
        files_to_check, notice = get_files_to_check(
            parquet_file, check_all=check_all_files, check_sample=check_sample, verbose=verbose
        )

        if notice:
            click.echo(click.style(f"\U0001f4c1 {notice}", fg="cyan"))

        if not files_to_check:
            click.echo(click.style("No parquet files found", fg="red"))
            return

        runner = MultiFileCheckRunner(files_to_check, verbose=verbose)

        for file_path in files_to_check:
            runner.start_file(file_path)

            show_output = runner.verbose or not runner.is_multi_file
            quiet = not show_output

            result = check_optimization(
                file_path, verbose=verbose and show_output, return_results=True, quiet=quiet
            )

            runner.record_result(file_path, result)

        runner.print_summary()


# Skills command (for LLM integration)
@cli.command()
@handle_geoparquet_errors
@click.option("--show", is_flag=True, help="Print skill content to stdout")
@click.option("--copy", "copy_to", type=click.Path(), help="Copy skill to directory")
@click.option("--name", default="geoparquet", help="Skill name (default: geoparquet)")
def skills(show: bool, copy_to: str | None, name: str):
    """List and access LLM skills for gpio.

    Skills are markdown files that help LLMs (ChatGPT, Claude, etc.) work
    effectively with the gpio CLI tool.

    \b
    Examples:
      gpio skills              # List available skills
      gpio skills --show       # Print skill to stdout (for piping to LLM)
      gpio skills --copy .     # Copy skill to current directory

    \b
    Using with LLMs:
      # Paste skill content into a conversation
      gpio skills --show | pbcopy

      # Or reference the installed file
      gpio skills  # Shows file path
    """
    from geoparquet_io.skills import get_skill_content, get_skill_path, list_skills

    try:
        if show:
            # Print content to stdout
            click.echo(get_skill_content(name))
        elif copy_to:
            # Copy skill to directory
            from pathlib import Path
            from shutil import copy2

            dest_dir = Path(copy_to)
            if not dest_dir.is_dir():
                raise click.ClickException(f"Not a directory: {copy_to}")

            src = get_skill_path(name)
            dest = dest_dir / f"{name}.md"
            copy2(src, dest)
            click.echo(f"Copied skill to: {dest}")
        else:
            # List available skills
            available = list_skills()
            click.echo("Available gpio skills:\n")
            for skill_name in available:
                skill_path = get_skill_path(skill_name)
                click.echo(f"  {skill_name}")
                click.echo(f"    Path: {skill_path}")
            click.echo("\nUsage:")
            click.echo("  gpio skills --show       # Print to stdout")
            click.echo("  gpio skills --copy .     # Copy to current directory")
    except FileNotFoundError as e:
        raise click.ClickException(str(e)) from e


# Benchmark commands group
@cli.group()
@click.pass_context
def benchmark(ctx):
    """Benchmark GeoParquet performance.

    Commands for measuring and comparing performance of GeoParquet operations.

    \b
    Subcommands:
      suite    Run comprehensive benchmark suite
      compare  Compare converter performance on a single file
      explain  Show DuckDB query plan analysis (EXPLAIN ANALYZE)
      report   View and compare benchmark results
    """
    ctx.ensure_object(dict)


@benchmark.command("compare")
@handle_geoparquet_errors
@click.argument("input_file", type=click.Path(exists=True))
@click.option(
    "--iterations",
    "-n",
    default=3,
    type=int,
    help="Number of iterations per converter (default: 3)",
)
@click.option(
    "--converters",
    "-c",
    help="Comma-separated list of converters to run (default: all available)",
)
@click.option(
    "--output-json",
    "-o",
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
    "-q",
    is_flag=True,
    help="Suppress progress output, show only results",
)
@verbose_option
def benchmark_compare(
    input_file,
    iterations,
    converters,
    output_json,
    keep_output,
    warmup,
    output_format,
    quiet,
    verbose,
):
    """
    Compare converter performance on a single file.

    Tests different conversion methods (DuckDB, GeoPandas, GDAL) on an input
    geospatial file and reports time and memory usage.

    \b
    Available converters:
      - duckdb: DuckDB spatial extension (always available)
      - geopandas_fiona: GeoPandas with Fiona engine
      - geopandas_pyogrio: GeoPandas with PyOGRIO engine
      - gdal_ogr2ogr: GDAL ogr2ogr CLI

    \b
    Example:
        gpio benchmark compare input.geojson --iterations 5
    """
    configure_verbose(verbose)
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


@benchmark.command("suite")
@handle_geoparquet_errors
@click.option(
    "--operations",
    type=click.Choice(["core", "full"]),
    default="core",
    help="Operation set to run (default: core)",
)
@click.option(
    "--files",
    multiple=True,
    help="Files to test (paths or size names: tiny, small, medium, large, xlarge)",
)
@click.option(
    "--iterations",
    "-n",
    default=3,
    help="Runs per operation (default: 3)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    help="Write results to JSON file",
)
@click.option(
    "--profile",
    is_flag=True,
    help="Enable cProfile profiling to diagnose performance bottlenecks",
)
@click.option(
    "--profile-dir",
    type=click.Path(),
    default="./profiles",
    help="Directory for profile output files (default: ./profiles)",
)
@verbose_option
def benchmark_suite(
    operations,
    files,
    iterations,
    output,
    profile,
    profile_dir,
    verbose,
):
    """
    Run comprehensive benchmark suite.

    Tests gpio operations across multiple file sizes with timing and memory tracking.

    \b
    Example:
        gpio benchmark suite --operations core --output results.json
        gpio benchmark suite --files input.parquet --output results.json
        gpio benchmark suite --profile --profile-dir ./my-profiles
    """
    from pathlib import Path

    configure_verbose(verbose)
    from geoparquet_io.benchmarks.config import CORE_OPERATIONS, FULL_OPERATIONS
    from geoparquet_io.core.benchmark_suite import run_benchmark_suite
    from geoparquet_io.core.logging_config import info, progress, success

    # Determine operations
    ops = CORE_OPERATIONS if operations == "core" else FULL_OPERATIONS

    # Resolve files
    if not files:
        raise click.ClickException("No files specified. Use --files with paths.")

    input_files = []
    for f in files:
        path = Path(f)
        if path.exists():
            input_files.append(path)
        else:
            raise click.ClickException(f"File not found: {f}")

    # Setup profiling if requested
    profile_path = None
    if profile:
        profile_path = Path(profile_dir)
        profile_path.mkdir(parents=True, exist_ok=True)
        info(f"Profiling enabled - output directory: {profile_path}")

    progress(
        f"Running benchmark suite: {len(ops)} operations, "
        f"{len(input_files)} files, {iterations} iterations"
    )

    result = run_benchmark_suite(
        input_files=input_files,
        operations=ops,
        iterations=iterations,
        verbose=verbose,
        profile=profile,
        profile_dir=profile_path,
    )

    # Display summary
    success_count = sum(1 for r in result.results if r.success)
    total_count = len(result.results)
    progress(f"\nCompleted: {success_count}/{total_count} benchmarks")

    # Show profile summary if profiling was enabled
    if profile:
        profile_files = [
            Path(r.details.get("profile_path"))
            for r in result.results
            if r.details.get("profile_path")
        ]

        if profile_files:
            info(f"\nGenerated {len(profile_files)} profile files in {profile_path}")
            info("\nTo view profile details, use:")
            info(f"  uv run python -m pstats {profile_files[0]}")
            info("\nOr generate a summary:")
            info("  from geoparquet_io.benchmarks.profile_report import format_profile_stats")
            info(f"  print(format_profile_stats('{profile_files[0]}'))")

    # Save if requested
    if output:
        Path(output).write_text(result.to_json())
        success(f"Results saved to {output}")


@benchmark.command("explain")
@handle_geoparquet_errors
@click.argument("input_file", type=click.Path(exists=True))
@click.option(
    "--query",
    "-q",
    type=str,
    default=None,
    help="Custom SQL query. Use {file} as placeholder for file path.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format (default: table)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Save results to JSON file",
)
@verbose_option
def benchmark_explain(
    input_file,
    query,
    output_format,
    output,
    verbose,
):
    """
    Show DuckDB query plan analysis (EXPLAIN ANALYZE).

    Runs EXPLAIN ANALYZE on a DuckDB query against a GeoParquet file to reveal
    per-operator timing, cardinality, filter pushdown, and row group pruning.

    \b
    Example:
        gpio benchmark explain input.parquet
        gpio benchmark explain input.parquet --format json
        gpio benchmark explain input.parquet --query "SELECT * FROM read_parquet('{file}') WHERE id > 10"
        gpio benchmark explain input.parquet --output plan.json
    """
    configure_verbose(verbose)
    from geoparquet_io.core.benchmark import (
        explain_analyze,
        format_explain_output,
    )
    from geoparquet_io.core.logging_config import progress

    result = explain_analyze(
        file_path=input_file,
        query=query,
    )

    formatted = format_explain_output(result, output_format=output_format)
    progress(formatted)

    if output:
        from pathlib import Path

        json_output = format_explain_output(result, output_format="json")
        Path(output).write_text(json_output)
        progress(f"\nResults saved to: {output}")


@benchmark.command("report")
@handle_geoparquet_errors
@click.argument("result_files", nargs=-1, type=click.Path(exists=True))
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format (default: table)",
)
@verbose_option
def benchmark_report(
    result_files,
    output_format,
    verbose,
):
    """
    View and compare benchmark results.

    \b
    Example:
        gpio benchmark report results.json
        gpio benchmark report results/*.json
    """
    import json

    configure_verbose(verbose)
    from geoparquet_io.core.benchmark_report import format_table
    from geoparquet_io.core.benchmark_suite import BenchmarkResult
    from geoparquet_io.core.logging_config import progress

    if not result_files:
        raise click.ClickException("No result files provided")

    # Load results
    all_results = []
    for rf in result_files:
        with open(rf) as f:
            data = json.load(f)
            for r in data.get("results", []):
                all_results.append(
                    BenchmarkResult(
                        operation=r["operation"],
                        file=r["file"],
                        time_seconds=r["time_seconds"],
                        peak_rss_memory_mb=r["peak_rss_memory_mb"],
                        success=r["success"],
                        error=r.get("error"),
                        details=r.get("details", {}),
                    )
                )

    if output_format == "json":
        click.echo(json.dumps([r.__dict__ for r in all_results], indent=2, default=str))
    else:
        progress(format_table(all_results))


# =============================================================================
# PMTiles Commands (requires tippecanoe)
# =============================================================================


@cli.group()
@click.pass_context
def pmtiles(ctx):
    """PMTiles generation commands.

    Generate PMTiles from GeoParquet files using tippecanoe.
    Requires tippecanoe to be installed and available in PATH.

    Install tippecanoe:
      macOS:  brew install tippecanoe
      Ubuntu: sudo apt install tippecanoe
    """
    pass


@pmtiles.command(name="create", cls=SingleFileCommand)
@click.argument("input_file", type=click.Path())
@click.argument("output_file", type=click.Path())
@click.option("--layer", "-l", help="Layer name in output (defaults to output filename)")
@click.option("--min-zoom", type=int, help="Minimum zoom level")
@click.option("--max-zoom", type=int, help="Maximum zoom level (auto-detected if not set)")
@click.option("--bbox", help="Bounding box filter: minx,miny,maxx,maxy")
@click.option("--where", help="SQL WHERE clause for filtering")
@click.option("--include-cols", help="Comma-separated list of columns to include")
@click.option(
    "--precision",
    type=int,
    default=6,
    show_default=True,
    help="Coordinate decimal precision",
)
@click.option("--src-crs", help="Source CRS for reprojection to WGS84")
@click.option("--attribution", help="Custom attribution HTML for tiles")
@click.option(
    "--layer-by-column",
    type=str,
    default=None,
    help="In the generated PMTiles, split tiles into layers based on the value of this column",
)
@click.option(
    "--simplify-only-low-zooms/--no-simplify-only-low-zooms",
    default=True,
    show_default=True,
    help="Pass tippecanoe --simplify-only-low-zooms",
)
@click.option(
    "--no-simplification-of-shared-nodes/--simplification-of-shared-nodes",
    default=True,
    show_default=True,
    help="Pass tippecanoe --no-simplification-of-shared-nodes",
)
@click.option(
    "--no-tile-size-limit/--tile-size-limit",
    default=True,
    show_default=True,
    help=(
        "Remove the tile size cap (--no-tile-size-limit). Use --tile-size-limit "
        "to keep tippecanoe's limit so --drop-densest-as-needed actually drops "
        "features on dense data."
    ),
)
@click.option(
    "--drop-densest-as-needed/--no-drop-densest-as-needed",
    default=True,
    show_default=True,
    help="Pass tippecanoe --drop-densest-as-needed (no effect while size limit is off)",
)
@click.option(
    "--maximum-tile-bytes",
    type=int,
    default=None,
    help="Explicit per-tile byte cap (--maximum-tile-bytes); takes precedence over --no-tile-size-limit",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Overwrite the output file if it already exists (tippecanoe --force)",
)
@repair_geometry_option
@verbose_option
@aws_profile_option
def pmtiles_create(
    input_file,
    output_file,
    layer,
    min_zoom,
    max_zoom,
    bbox,
    where,
    include_cols,
    precision,
    src_crs,
    attribution,
    verbose,
    aws_profile,
    layer_by_column,
    simplify_only_low_zooms,
    no_simplification_of_shared_nodes,
    no_tile_size_limit,
    drop_densest_as_needed,
    maximum_tile_bytes,
    force,
    repair_geometry,
):
    """Create PMTiles from a GeoParquet file.

    Streams GeoParquet through gpio and tippecanoe to generate PMTiles.
    All processing is done via subprocess pipelines - no intermediate files.

    Examples:

        gpio pmtiles create buildings.parquet buildings.pmtiles

        gpio pmtiles create roads.parquet roads.pmtiles -l roads --max-zoom 14

        gpio pmtiles create data.parquet tiles.pmtiles --bbox "-122.5,37.5,-122.0,38.0"

        gpio pmtiles create data.parquet tiles.pmtiles --where "population > 10000"

        gpio pmtiles create data.parquet tiles.pmtiles --layer-by-column owner

        gpio pmtiles create dense.parquet tiles.pmtiles --tile-size-limit --max-zoom 14

        gpio pmtiles create data.parquet tiles.pmtiles --force
    """
    from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

    try:
        create_pmtiles_from_geoparquet(
            input_path=input_file,
            output_path=output_file,
            layer=layer,
            min_zoom=min_zoom,
            max_zoom=max_zoom,
            bbox=bbox,
            where=where,
            include_cols=include_cols,
            precision=precision,
            verbose=verbose,
            profile=aws_profile,
            src_crs=src_crs,
            attribution=attribution,
            layer_by_column=layer_by_column,
            simplify_only_low_zooms=simplify_only_low_zooms,
            no_simplification_of_shared_nodes=no_simplification_of_shared_nodes,
            no_tile_size_limit=no_tile_size_limit,
            drop_densest_as_needed=drop_densest_as_needed,
            maximum_tile_bytes=maximum_tile_bytes,
            force=force,
            repair_geometry=repair_geometry,
        )
        click.echo(click.style(f"✓ Created {output_file}", fg="green"))
    except Exception as e:
        raise click.ClickException(str(e)) from e


@pmtiles.command(name="pyramid")
@click.argument("input_parquet", type=click.Path())
@click.argument("output_pmtiles", type=click.Path())
@click.option(
    "--levels",
    default=None,
    help=(
        "Comma-separated overview levels (grid resolutions like '5'; admin: "
        "'country'). Default: auto-select against --max-tile-kb."
    ),
)
@click.option(
    "--max-tile-kb",
    type=int,
    default=500,
    show_default=True,
    help="Tile-size budget in KB driving zoom-band selection.",
)
@click.option(
    "--bytes-per-cell",
    type=float,
    default=None,
    help="Override the estimated compressed bytes per cell used in band selection.",
)
@click.option(
    "--layer-mode",
    type=click.Choice(["single", "grouped", "per-level"]),
    default="grouped",
    show_default=True,
    help=(
        "Layer naming: 'single' puts everything in one layer, 'grouped' uses "
        "'aggregate' + 'features', 'per-level' uses r5/r10 (or country/region)."
    ),
)
@click.option(
    "--include-features",
    is_flag=True,
    help="Append the original features as the final zoom band.",
)
@click.option(
    "--features-source",
    type=click.Path(),
    default=None,
    help="GeoParquet source for the features band (required with --include-features).",
)
@click.option(
    "--features-min-zoom",
    type=int,
    default=None,
    help="First zoom of the features band (default: base band max zoom + 1).",
)
@click.option(
    "--max-zoom",
    type=int,
    default=None,
    help="Max zoom of the base aggregate band (auto-detected if not set).",
)
@click.option("--attribution", help="Custom attribution HTML for tiles")
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Overwrite the output archive if it already exists",
)
@verbose_option
def pmtiles_pyramid(
    input_parquet,
    output_pmtiles,
    levels,
    max_tile_kb,
    bytes_per_cell,
    layer_mode,
    include_features,
    features_source,
    features_min_zoom,
    max_zoom,
    attribution,
    force,
    verbose,
):
    """Create a multi-level PMTiles pyramid from an aggregate file.

    Detects the aggregate's scheme (a5/h3/admin) and base level, assigns each
    level a zoom band that fits the tile budget, runs tippecanoe once per band,
    and merges everything into one archive with tile-join. Existing overview
    siblings (from `gpio process overview`) are reused; missing levels are
    built automatically. Bands are recorded in the PMTiles metadata under
    `gpio:pyramid`.

    Requires tippecanoe and tile-join (ships with tippecanoe) in PATH.

    Examples:

        gpio pmtiles pyramid cells.parquet cells.pmtiles

        gpio pmtiles pyramid cells.parquet out.pmtiles --levels 5 --max-zoom 10

        gpio pmtiles pyramid cells.parquet out.pmtiles \\
            --include-features --features-source buildings.parquet --max-zoom 8

        gpio pmtiles pyramid by_region.parquet out.pmtiles --layer-mode per-level
    """
    from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

    try:
        create_pmtiles_pyramid(
            input_parquet,
            output_pmtiles,
            levels=levels,
            max_tile_kb=max_tile_kb,
            bytes_per_cell=bytes_per_cell,
            layer_mode=layer_mode,
            include_features=include_features,
            features_source=features_source,
            features_min_zoom=features_min_zoom,
            max_zoom=max_zoom,
            attribution=attribution,
            force=force,
            verbose=verbose,
        )
        click.echo(click.style(f"✓ Created {output_pmtiles}", fg="green"))
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Process Commands (aggregate, ...)
# =============================================================================


def _aggregate_error(exc: Exception, where: str | None) -> click.ClickException:
    """Turn an aggregation failure into a message a user can act on.

    A bad ``--where`` clause surfaces as a raw DuckDB binder/parser error that
    echoes the whole generated SQL, which buries the actual cause. Drop the SQL
    echo and name ``--where`` as the likely culprit (gpio #612). Non-DuckDB
    errors (validation, bad parameters) already read well and pass through.
    """
    if not isinstance(exc, duckdb.Error):
        return click.ClickException(str(exc))
    message = str(exc).split("\nLINE ")[0].strip()
    if where:
        message += (
            f'\n\nThis is most likely caused by --where "{where}" '
            "-- check the column names and SQL syntax."
        )
    return click.ClickException(message)


@cli.group()
@click.pass_context
def process(ctx):
    """Transform or reduce GeoParquet data (aggregate, overview, ...)."""
    pass


@process.command(name="overview")
@click.argument("input_parquet")
@click.option(
    "--levels",
    default=None,
    help=(
        "Comma-separated coarser levels to build (grid resolutions like '4,7'; "
        "admin: 'country'). Default: auto-select against --max-tile-kb."
    ),
)
@click.option(
    "--max-tile-kb",
    type=int,
    default=500,
    show_default=True,
    help="Tile-size budget in KB driving auto level selection.",
)
@click.option(
    "--bytes-per-cell",
    type=float,
    default=None,
    help="Override the estimated compressed bytes per cell used in auto selection.",
)
@click.option(
    "--cell-column",
    default=None,
    help="Cell id column when auto-detection fails (default: a5_cell/h3_cell/admin_code).",
)
@click.option(
    "--scheme",
    type=click.Choice(["a5", "h3", "admin"]),
    default=None,
    help=(
        "Bucketing scheme of the cell column when inference is ambiguous "
        "(e.g. H3 ids stored as integers)."
    ),
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default=None,
    help="Directory for overview files (default: alongside the input).",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Overwrite existing overview output files.",
)
@compression_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def process_overview(
    ctx,
    input_parquet,
    levels,
    max_tile_kb,
    bytes_per_cell,
    cell_column,
    scheme,
    output_dir,
    force,
    compression,
    compression_level,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Build coarser overview levels from an aggregate output.

    Reads a `gpio process aggregate` output, detects its scheme (a5/h3/admin)
    and base level, and writes one GeoParquet sibling per coarser level
    (`cells.parquet` -> `cells_r4.parquet`; admin -> `by_region_country.parquet`).
    Counts, sums, mins, maxes, and breakdown counts roll up exactly; averages
    are count-weighted.

    Examples:

        gpio process overview cells.parquet

        gpio process overview cells.parquet --levels 4,7

        gpio process overview by_region.parquet --levels country

        gpio process overview cells.parquet --max-tile-kb 300
    """
    with _activate_s3(ctx):
        try:
            create_overviews_impl(
                input_parquet,
                levels=levels,
                max_tile_kb=max_tile_kb,
                bytes_per_cell=bytes_per_cell,
                cell_column=cell_column,
                scheme=scheme,
                output_dir=output_dir,
                compression=compression.upper(),
                compression_level=compression_level,
                geoparquet_version=geoparquet_version,
                force=force,
                verbose=verbose,
                show_sql=show_sql,
            )
        except (InvalidParameterError, ValueError, duckdb.Error) as exc:
            raise click.ClickException(str(exc)) from exc


@process.group(name="aggregate")
@click.pass_context
def process_aggregate(ctx):
    """Aggregate features into spatial buckets with per-bucket statistics.

    Reduces large datasets into a small file of grid cells or admin regions,
    each carrying a count and optional metric/breakdown columns, for low-zoom
    visualization. Subcommands choose the bucketing scheme.
    """
    pass


@process_aggregate.command(name="a5")
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option(
    "--resolution",
    type=click.IntRange(0, 30),
    default=None,
    help="A5 resolution (0-30). Required unless --auto.",
)
@grid_aggregate_options
@compression_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def process_aggregate_a5(
    ctx,
    input_parquet,
    output_parquet,
    resolution,
    auto,
    target_per_cell,
    max_cells,
    metric,
    metric_nodata,
    breakdown,
    breakdown_limit,
    out_geometry,
    where,
    bucket_point,
    bbox_column,
    compression,
    compression_level,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Aggregate features into A5 grid cells.

    Examples:

        gpio process aggregate a5 fields.parquet cells.parquet --resolution 8
        gpio process aggregate a5 fields.parquet cells.parquet --auto \\
            --metric "sum:area_ha" --breakdown crop_type
        gpio process aggregate a5 fields.parquet cells.parquet --resolution 8 \\
            --where "\\"crop:name\\" = 'wheat'"
        gpio process aggregate a5 buildings.parquet cells.parquet --auto \\
            --metric "avg:height,max:height" --metric-nodata "-999"
        gpio process aggregate a5 buildings.parquet cells.parquet --auto \\
            --bucket-point bbox --metric "avg:height"
        gpio process aggregate a5 fields.parquet cells.csv-like.parquet \\
            --resolution 8 --out-geometry none
    """
    with _activate_s3(ctx):
        try:
            aggregate_by_a5_impl(
                input_parquet,
                output_parquet,
                resolution=resolution,
                auto=auto,
                target_per_cell=target_per_cell,
                max_cells=max_cells,
                metric=metric,
                breakdown=breakdown,
                breakdown_limit=breakdown_limit,
                out_geometry=out_geometry,
                compression=compression.upper(),
                compression_level=compression_level,
                geoparquet_version=geoparquet_version,
                verbose=verbose,
                show_sql=show_sql,
                where=where,
                metric_nodata=metric_nodata,
                bucket_point=bucket_point,
                bbox_column=bbox_column,
            )
        except (InvalidParameterError, ValidationError, ValueError, duckdb.Error) as exc:
            raise _aggregate_error(exc, where) from exc


@process_aggregate.command(name="h3")
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option(
    "--resolution",
    type=click.IntRange(0, 15),
    default=None,
    help="H3 resolution (0-15). Required unless --auto.",
)
@grid_aggregate_options
@compression_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def process_aggregate_h3(
    ctx,
    input_parquet,
    output_parquet,
    resolution,
    auto,
    target_per_cell,
    max_cells,
    metric,
    metric_nodata,
    breakdown,
    breakdown_limit,
    out_geometry,
    where,
    bucket_point,
    bbox_column,
    compression,
    compression_level,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Aggregate features into H3 grid cells.

    Examples:

        gpio process aggregate h3 fields.parquet cells.parquet --resolution 8
        gpio process aggregate h3 fields.parquet cells.parquet --auto \\
            --metric "sum:area_ha" --breakdown crop_type
        gpio process aggregate h3 fields.parquet cells.parquet --resolution 8 \\
            --where "confidence >= 50"
        gpio process aggregate h3 buildings.parquet cells.parquet --auto \\
            --metric "avg:height" --metric-nodata "-999"
        gpio process aggregate h3 buildings.parquet cells.parquet --auto \\
            --bucket-point bbox
        gpio process aggregate h3 fields.parquet cells.parquet \\
            --resolution 8 --out-geometry none
    """
    with _activate_s3(ctx):
        try:
            aggregate_by_h3_impl(
                input_parquet,
                output_parquet,
                resolution=resolution,
                auto=auto,
                target_per_cell=target_per_cell,
                max_cells=max_cells,
                metric=metric,
                breakdown=breakdown,
                breakdown_limit=breakdown_limit,
                out_geometry=out_geometry,
                compression=compression.upper(),
                compression_level=compression_level,
                geoparquet_version=geoparquet_version,
                verbose=verbose,
                show_sql=show_sql,
                where=where,
                metric_nodata=metric_nodata,
                bucket_point=bucket_point,
                bbox_column=bbox_column,
            )
        except (InvalidParameterError, ValidationError, ValueError, duckdb.Error) as exc:
            raise _aggregate_error(exc, where) from exc


@process_aggregate.command(name="admin")
@click.argument("input_parquet")
@click.argument("output_parquet")
@click.option(
    "--level",
    type=click.Choice(["country", "region"]),
    default="country",
    help="Administrative level to aggregate to (default: country).",
)
@click.option(
    "--metric",
    default=None,
    help='Numeric rollups, e.g. "sum:area_ha,avg:yield". Bare column = sum.',
)
@metric_nodata_option
@click.option(
    "--breakdown",
    default=None,
    help="Categorical column to pivot count by.",
)
@click.option(
    "--breakdown-limit",
    type=int,
    default=20,
    help="Max breakdown values before remainder rolls into count_other (default: 20).",
)
@click.option(
    "--out-geometry",
    type=click.Choice(["polygon", "centroid", "both", "none"]),
    default="polygon",
    help="Output geometry per region (default: polygon).",
)
@where_option
@bucket_point_options
@compression_options
@verbose_option
@geoparquet_version_option
@show_sql_option
@click.pass_context
def process_aggregate_admin(
    ctx,
    input_parquet,
    output_parquet,
    level,
    metric,
    metric_nodata,
    breakdown,
    breakdown_limit,
    out_geometry,
    where,
    bucket_point,
    bbox_column,
    compression,
    compression_level,
    verbose,
    geoparquet_version,
    show_sql,
):
    """Aggregate features into administrative regions.

    Examples:

        gpio process aggregate admin fields.parquet by_country.parquet --level country
        gpio process aggregate admin fields.parquet by_region.parquet \\
            --level region --metric "sum:area_ha" --breakdown crop_type
        gpio process aggregate admin fields.parquet by_country.parquet \\
            --level country --where "confidence >= 50"
        gpio process aggregate admin buildings.parquet by_country.parquet \\
            --level country --metric "avg:height" --metric-nodata "-999"
        gpio process aggregate admin buildings.parquet by_country.parquet \\
            --level country --bucket-point bbox
    """
    with _activate_s3(ctx):
        try:
            aggregate_by_admin_impl(
                input_parquet,
                output_parquet,
                level=level,
                metric=metric,
                breakdown=breakdown,
                breakdown_limit=breakdown_limit,
                out_geometry=out_geometry,
                compression=compression.upper(),
                compression_level=compression_level,
                geoparquet_version=geoparquet_version,
                verbose=verbose,
                show_sql=show_sql,
                where=where,
                metric_nodata=metric_nodata,
                bucket_point=bucket_point,
                bbox_column=bbox_column,
            )
        except (InvalidParameterError, ValidationError, ValueError, duckdb.Error) as exc:
            raise _aggregate_error(exc, where) from exc


if __name__ == "__main__":
    cli()
