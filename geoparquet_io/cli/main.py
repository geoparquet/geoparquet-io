from pathlib import Path

import click

from geoparquet_io.cli.decorators import (
    GlobAwareCommand,
    SingleFileCommand,
    any_extension_option,
    check_partition_options,
    compression_options,
    dry_run_option,
    geoparquet_version_option,
    output_format_options,
    overwrite_option,
    partition_input_options,
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
from geoparquet_io.core.add_quadkey_column import add_quadkey_column as add_quadkey_column_impl
from geoparquet_io.core.check_parquet_structure import check_all as check_structure_impl
from geoparquet_io.core.check_spatial_order import check_spatial_order as check_spatial_impl
from geoparquet_io.core.common import validate_parquet_extension
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
from geoparquet_io.core.partition_by_quadkey import (
    partition_by_quadkey as partition_by_quadkey_impl,
)
from geoparquet_io.core.partition_by_string import (
    partition_by_string as partition_by_string_impl,
)
from geoparquet_io.core.reproject import reproject_impl
from geoparquet_io.core.sort_by_column import sort_by_column as sort_by_column_impl
from geoparquet_io.core.sort_quadkey import sort_by_quadkey as sort_by_quadkey_impl
from geoparquet_io.core.upload import check_credentials
from geoparquet_io.core.upload import upload as upload_impl

# Version info
__version__ = "0.8.0"


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


class ConvertDefaultGroup(click.Group):
    """Custom Group that invokes 'geoparquet' when no subcommand is provided.

    This allows backwards compatibility:
    - gpio convert input.shp output.parquet  -> invokes geoparquet
    - gpio convert geoparquet input.shp output.parquet -> explicit
    - gpio convert reproject input.parquet output.parquet -> subcommand
    """

    def parse_args(self, ctx, args):
        # Handle --help for group
        if "--help" in args and (not args or args[0] not in self.commands):
            return super().parse_args(ctx, [a for a in args if a != "--help"] + ["--help"])

        # If first arg is a known subcommand, use it
        if args and not args[0].startswith("-") and args[0] in self.commands:
            return super().parse_args(ctx, args)

        # Default to 'geoparquet' subcommand for backwards compat
        return super().parse_args(ctx, ["geoparquet"] + args)


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
@check_partition_options
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
    spec_details,
    check_all_files,
    check_sample,
    profile,
):
    """Check compression, bbox, row groups, spatial order, and spec compliance."""
    from geoparquet_io.core.common import is_remote_url, show_remote_read_message
    from geoparquet_io.core.partition_reader import get_files_to_check

    configure_verbose(verbose)

    # Get files to check based on partition options
    files_to_check, notice = get_files_to_check(
        parquet_file, check_all=check_all_files, check_sample=check_sample, verbose=verbose
    )

    if notice:
        click.echo(click.style(f"📁 {notice}", fg="cyan"))

    if not files_to_check:
        click.echo(click.style("No parquet files found", fg="red"))
        return

    # For partitions, disable fix mode (requires single file)
    if len(files_to_check) > 1 and fix:
        click.echo(click.style("⚠️  --fix is only available for single files", fg="yellow"))
        click.echo("Use 'gpio extract' to consolidate partitions first, then run check --fix")
        fix = False

    # Create runner for multi-file progress tracking
    runner = MultiFileCheckRunner(files_to_check, verbose=verbose)

    # Process each file
    for file_path in files_to_check:
        runner.start_file(file_path)

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
            file_path, verbose and show_output, return_results=True, quiet=quiet
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
        ratio = spatial_result["ratio"]

        from geoparquet_io.cli.fix_helpers import (
            aggregate_check_results,
            display_spatial_result,
        )

        display_spatial_result(ratio, show_output)

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
            file_path, {"passed": combined_passed, "issues": combined_issues, **structure_results}
        )

        # If --fix flag is set, apply fixes (only for single files)
        if fix:
            from geoparquet_io.cli.fix_helpers import apply_check_all_fixes

            all_results = {**structure_results, "spatial": spatial_result}
            applied = apply_check_all_fixes(
                file_path=file_path,
                all_results=all_results,
                fix_output=fix_output,
                no_backup=no_backup,
                overwrite=overwrite,
                verbose=verbose,
                profile=profile,
                check_structure_impl=check_structure_impl,
                check_spatial_impl=check_spatial_impl,
                random_sample_size=random_sample_size,
                limit_rows=limit_rows,
            )
            if not applied:
                return

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
@profile_option
def check_spatial(
    parquet_file,
    random_sample_size,
    limit_rows,
    verbose,
    fix,
    fix_output,
    no_backup,
    check_all_files,
    check_sample,
    profile,
):
    """Check spatial ordering."""
    from geoparquet_io.core.check_fixes import fix_spatial_ordering
    from geoparquet_io.core.partition_reader import get_files_to_check

    configure_verbose(verbose)

    # Get files to check based on partition options
    files_to_check, notice = get_files_to_check(
        parquet_file, check_all=check_all_files, check_sample=check_sample, verbose=verbose
    )

    if notice:
        click.echo(click.style(f"📁 {notice}", fg="cyan"))

    if not files_to_check:
        click.echo(click.style("No parquet files found", fg="red"))
        return

    # For partitions, disable fix mode
    if len(files_to_check) > 1 and fix:
        click.echo(click.style("⚠️  --fix is only available for single files", fg="yellow"))
        fix = False

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
        )
        ratio = result["ratio"]

        if show_output and ratio is not None:
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

        # Record result for summary
        runner.record_result(file_path, result)

        if fix:
            if not result.get("fix_available", False):
                click.echo(
                    click.style("\n✓ No fix needed - already spatially ordered!", fg="green")
                )
                return

            click.echo("\nApplying Hilbert spatial ordering...")
            output_path, backup_path = handle_fix_common(
                file_path, fix_output, no_backup, fix_spatial_ordering, verbose, False, profile
            )

            click.echo(click.style("\n✓ Spatial ordering applied successfully!", fg="green"))
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
@profile_option
def check_compression_cmd(
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
    """Check geometry column compression."""
    from geoparquet_io.core.check_fixes import fix_compression
    from geoparquet_io.core.check_parquet_structure import check_compression
    from geoparquet_io.core.partition_reader import get_files_to_check

    configure_verbose(verbose)

    # Get files to check based on partition options
    files_to_check, notice = get_files_to_check(
        parquet_file, check_all=check_all_files, check_sample=check_sample, verbose=verbose
    )

    if notice:
        click.echo(click.style(f"📁 {notice}", fg="cyan"))

    if not files_to_check:
        click.echo(click.style("No parquet files found", fg="red"))
        return

    # For partitions, disable fix mode
    if len(files_to_check) > 1 and fix:
        click.echo(click.style("⚠️  --fix is only available for single files", fg="yellow"))
        fix = False

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
                click.echo(click.style("\n✓ No fix needed - already using ZSTD!", fg="green"))
                return

            click.echo("\nRe-compressing with ZSTD...")
            output_path, backup_path = handle_fix_common(
                file_path, fix_output, no_backup, fix_compression, verbose, overwrite, profile
            )

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
@profile_option
def check_bbox_cmd(
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
    """Check bbox column and metadata (version-aware).

    For GeoParquet 1.x: bbox column is recommended for spatial filtering.
    For GeoParquet 2.0/parquet-geo-only: bbox column is NOT recommended
    (native Parquet geo types provide row group statistics).
    """
    from geoparquet_io.core.check_fixes import fix_bbox_all, fix_bbox_removal
    from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox
    from geoparquet_io.core.partition_reader import get_files_to_check

    configure_verbose(verbose)

    # Get files to check based on partition options
    files_to_check, notice = get_files_to_check(
        parquet_file, check_all=check_all_files, check_sample=check_sample, verbose=verbose
    )

    if notice:
        click.echo(click.style(f"📁 {notice}", fg="cyan"))

    if not files_to_check:
        click.echo(click.style("No parquet files found", fg="red"))
        return

    # For partitions, disable fix mode
    if len(files_to_check) > 1 and fix:
        click.echo(click.style("⚠️  --fix is only available for single files", fg="yellow"))
        fix = False

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
                click.echo(click.style("\n✓ No fix needed - bbox is optimal!", fg="green"))
                return

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
                    file_path, fix_output, no_backup, bbox_fix_func, verbose, overwrite, profile
                )

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
                    file_path, fix_output, no_backup, bbox_fix_func, verbose, overwrite, profile
                )

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
@overwrite_option
@check_partition_options
@profile_option
def check_row_group_cmd(
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
    from geoparquet_io.core.partition_reader import get_files_to_check

    configure_verbose(verbose)

    # Get files to check based on partition options
    files_to_check, notice = get_files_to_check(
        parquet_file, check_all=check_all_files, check_sample=check_sample, verbose=verbose
    )

    if notice:
        click.echo(click.style(f"📁 {notice}", fg="cyan"))

    if not files_to_check:
        click.echo(click.style("No parquet files found", fg="red"))
        return

    # For partitions, disable fix mode
    if len(files_to_check) > 1 and fix:
        click.echo(click.style("⚠️  --fix is only available for single files", fg="yellow"))
        fix = False

    # Create runner for multi-file progress tracking
    runner = MultiFileCheckRunner(files_to_check, verbose=verbose)

    for file_path in files_to_check:
        runner.start_file(file_path)

        show_output = runner.verbose or not runner.is_multi_file
        quiet = not show_output
        result = check_row_groups(
            file_path, verbose and show_output, return_results=True, quiet=quiet
        )

        # Record result for summary
        runner.record_result(file_path, result)

        if fix:
            if not result.get("fix_available", False):
                click.echo(click.style("\n✓ No fix needed - row groups are optimal!", fg="green"))
                return

            click.echo("\nOptimizing row groups...")
            output_path, backup_path = handle_fix_common(
                file_path, fix_output, no_backup, fix_row_groups, verbose, overwrite, profile
            )

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

    By default, converts to optimized GeoParquet. Use subcommands for specific operations.

    \b
    Examples:
        gpio convert input.shp output.parquet                    # To GeoParquet (default)
        gpio convert geoparquet input.shp output.parquet         # Explicit subcommand
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
@any_extension_option
def convert_to_geoparquet_cmd(
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
    any_extension,
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
    from geoparquet_io.core.streaming import (
        StreamingError,
        should_stream_output,
        validate_output,
    )

    # Validate output early - provides helpful error if no output and not piping
    try:
        validate_output(output_file)
    except StreamingError as e:
        raise click.ClickException(str(e)) from None

    # Validate .parquet extension
    validate_parquet_extension(output_file, any_extension)

    # Check for streaming output
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
            crs=crs,
            skip_invalid=skip_invalid,
            profile=profile,
            geoparquet_version=geoparquet_version,
        )
    else:
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


def _convert_streaming(
    input_file,
    skip_hilbert,
    wkt_column,
    lat_column,
    lon_column,
    delimiter,
    crs,
    skip_invalid,
    profile,
    geoparquet_version,
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
            compression="ZSTD",
            compression_level=15,
            row_group_rows=100000,
            wkt_column=wkt_column,
            lat_column=lat_column,
            lon_column=lon_column,
            delimiter=delimiter,
            crs=crs,
            skip_invalid=skip_invalid,
            profile=profile,
            geoparquet_version=geoparquet_version,
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
    profile,
    compression,
    compression_level,
    geoparquet_version,
):
    """Shared reproject CLI implementation."""
    from geoparquet_io.core.common import validate_profile_for_urls

    # Configure verbose logging
    configure_verbose(verbose)

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, input_file, output_file)

    try:
        result = reproject_impl(
            input_parquet=input_file,
            output_parquet=output_file,
            target_crs=dst_crs,
            source_crs=src_crs,
            overwrite=overwrite,
            compression=compression,
            compression_level=compression_level,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
        )

        click.echo(f"\nReprojected {result.feature_count:,} features")
        click.echo(f"  Source CRS: {result.source_crs}")
        click.echo(f"  Destination CRS: {result.target_crs}")
        click.echo(f"  Output: {result.output_path}")
    except Exception as e:
        raise click.ClickException(str(e)) from e


@convert.command(name="reproject", cls=SingleFileCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@click.option(
    "--dst-crs",
    "-d",
    default="EPSG:4326",
    show_default=True,
    help="Destination CRS (e.g., 'EPSG:4326', 'EPSG:32610')",
)
@click.option(
    "--src-crs",
    "-s",
    default=None,
    help="Override source CRS (e.g., 'EPSG:4326'). If not provided, detected from file metadata.",
)
@overwrite_option
@verbose_option
@profile_option
@compression_options
@geoparquet_version_option
@any_extension_option
def convert_reproject(
    input_file,
    output_file,
    dst_crs,
    src_crs,
    overwrite,
    verbose,
    profile,
    compression,
    compression_level,
    geoparquet_version,
    any_extension,
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

    _reproject_impl_cli(
        input_file,
        output_file,
        dst_crs,
        src_crs,
        overwrite,
        verbose,
        profile,
        compression,
        compression_level,
        geoparquet_version,
    )


@convert.command(name="geojson", cls=SingleFileCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
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
@verbose_option
@profile_option
def convert_geojson(
    input_file,
    output_file,
    no_rs,
    precision,
    write_bbox,
    id_field,
    description,
    no_seq,
    pretty,
    keep_crs,
    verbose,
    profile,
):
    """
    Convert GeoParquet to GeoJSON format.

    Supports two modes based on whether OUTPUT_FILE is provided:

    \b
    STREAMING MODE (no output file):
      Streams newline-delimited GeoJSON (GeoJSONSeq) to stdout with RFC 8142
      record separators. Designed for piping to tippecanoe for PMTiles/MBTiles.
      Use --feature-collection to output a FeatureCollection instead.
      Supports reading from stdin with "-" for pipeline use.

    \b
    FILE MODE (with output file):
      Writes a standard GeoJSON FeatureCollection to the specified file.

    \b
    Examples:
      # Stream to tippecanoe for PMTiles generation
      gpio convert geojson buildings.parquet | tippecanoe -P -o buildings.pmtiles

      # Pipeline with filtering
      gpio extract data.parquet --bbox "-122.5,37.5,-122,38" | gpio convert geojson - | tippecanoe -P -o sf.pmtiles

      # Write to GeoJSON file
      gpio convert geojson data.parquet output.geojson

      # Pretty-print with description
      gpio convert geojson data.parquet output.geojson --pretty --description "My dataset"

    \b
    Note: GeoParquet input is automatically reprojected to WGS84 (EPSG:4326)
    for RFC 7946 compliance. Use --keep-crs to preserve the original CRS.
    """
    from geoparquet_io.core.common import validate_profile_for_urls
    from geoparquet_io.core.geojson_stream import convert_to_geojson

    configure_verbose(verbose)

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, input_file, output_file)

    try:
        feature_count = convert_to_geojson(
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
            profile=profile,
            keep_crs=keep_crs,
        )

        if output_file:
            click.echo(f"Converted {feature_count:,} features to {output_file}")
    except Exception as e:
        raise click.ClickException(str(e)) from e


# Deprecated reproject command
@cli.command(hidden=True)  # Deprecated: use 'gpio convert reproject' instead
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@click.option(
    "--dst-crs",
    "-d",
    default="EPSG:4326",
    show_default=True,
    help="Destination CRS (e.g., 'EPSG:4326', 'EPSG:32610')",
)
@click.option(
    "--src-crs",
    "-s",
    default=None,
    help="Override source CRS (e.g., 'EPSG:4326'). If not provided, detected from file metadata.",
)
@overwrite_option
@verbose_option
@profile_option
@compression_options
@geoparquet_version_option
def reproject(
    input_file,
    output_file,
    dst_crs,
    src_crs,
    overwrite,
    verbose,
    profile,
    compression,
    compression_level,
    geoparquet_version,
):
    """
    [DEPRECATED] Reproject a GeoParquet file to a different CRS.

    This command is deprecated. Use 'gpio convert reproject' instead.
    """
    from geoparquet_io.core.logging_config import warn

    warn(
        "'gpio reproject' is deprecated and will be removed in a future release. "
        "Use 'gpio convert reproject' instead."
    )
    _reproject_impl_cli(
        input_file,
        output_file,
        dst_crs,
        src_crs,
        overwrite,
        verbose,
        profile,
        compression,
        compression_level,
        geoparquet_version,
    )


# Inspect command - uses OptionalIntCommand for --head/--tail optional values
@cli.command(cls=OptionalIntCommand)
@click.argument("parquet_file")
@click.option("--head", type=int, default=None, help="Show first N rows (default: 10)")
@click.option("--tail", type=int, default=None, help="Show last N rows (default: 10)")
@click.option(
    "--stats", is_flag=True, help="Show column statistics (nulls, min/max, unique counts)"
)
@click.option(
    "--meta",
    is_flag=True,
    help="Show comprehensive metadata (Parquet, GeoParquet, row groups)",
)
@click.option(
    "--parquet",
    "meta_parquet",
    is_flag=True,
    help="With --meta: show only Parquet file metadata",
)
@click.option(
    "--geoparquet",
    "meta_geoparquet",
    is_flag=True,
    help="With --meta: show only GeoParquet metadata from 'geo' key",
)
@click.option(
    "--parquet-geo",
    "meta_parquet_geo",
    is_flag=True,
    help="With --meta: show only Parquet geospatial metadata",
)
@click.option(
    "--row-groups",
    "meta_row_groups",
    type=int,
    default=1,
    help="With --meta: number of row groups to display (default: 1)",
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON for scripting")
@click.option(
    "--markdown", "markdown_output", is_flag=True, help="Output as Markdown for README files"
)
@click.option(
    "--check-all",
    "check_all_files",
    is_flag=True,
    help="For partitioned data: aggregate info from all files (bounds, row counts, etc.)",
)
@profile_option
def inspect(
    parquet_file,
    head,
    tail,
    stats,
    meta,
    meta_parquet,
    meta_geoparquet,
    meta_parquet_geo,
    meta_row_groups,
    json_output,
    markdown_output,
    check_all_files,
    profile,
):
    """
    Inspect a GeoParquet file and show metadata summary.

    Provides quick examination of GeoParquet files without launching external tools.
    Default behavior shows metadata only (instant). Use --head/--tail to preview data,
    --stats to calculate column statistics, or --meta for comprehensive metadata.

    Examples:

        \b
        # Quick metadata inspection
        gpio inspect data.parquet

        \b
        # Preview first 10 rows (default when no value given)
        gpio inspect data.parquet --head

        \b
        # Preview first 20 rows
        gpio inspect data.parquet --head 20

        \b
        # Preview last 10 rows (default when no value given)
        gpio inspect data.parquet --tail

        \b
        # Preview last 5 rows
        gpio inspect data.parquet --tail 5

        \b
        # Show statistics
        gpio inspect data.parquet --stats

        \b
        # Comprehensive metadata (Parquet, GeoParquet, row groups)
        gpio inspect data.parquet --meta

        \b
        # Show only GeoParquet metadata
        gpio inspect data.parquet --meta --geoparquet

        \b
        # Show all row groups instead of just the first
        gpio inspect data.parquet --meta --row-groups 10

        \b
        # JSON output for scripting
        gpio inspect data.parquet --json

        \b
        # Markdown output for README files
        gpio inspect data.parquet --markdown

        \b
        # Aggregate info from all files in partition
        gpio inspect partitions/ --check-all
    """
    from geoparquet_io.core.common import (
        setup_aws_profile_if_needed,
        validate_profile_for_urls,
    )

    # Validate mutually exclusive options
    if head is not None and tail is not None:
        raise click.UsageError("--head and --tail are mutually exclusive")

    if json_output and markdown_output:
        raise click.UsageError("--json and --markdown are mutually exclusive")

    # Validate --meta related options require --meta flag
    meta_sub_options = meta_parquet or meta_geoparquet or meta_parquet_geo or meta_row_groups != 1
    if meta_sub_options and not meta:
        raise click.UsageError(
            "--parquet, --geoparquet, --parquet-geo, and --row-groups require --meta flag"
        )

    # Handle --meta mode
    if meta:
        if head is not None or tail is not None:
            raise click.UsageError("--meta cannot be used with --head or --tail")
        if stats:
            raise click.UsageError("--meta cannot be used with --stats")
        if markdown_output:
            raise click.UsageError("--meta does not support --markdown output")
        if check_all_files:
            raise click.UsageError("--meta cannot be used with --check-all")

        # Validate profile is only used with S3
        validate_profile_for_urls(profile, parquet_file)
        setup_aws_profile_if_needed(profile, parquet_file)

        try:
            _handle_meta_display(
                parquet_file,
                meta_parquet,
                meta_geoparquet,
                meta_parquet_geo,
                meta_row_groups,
                json_output,
            )
        except Exception as e:
            raise click.ClickException(str(e)) from e
        return

    # Continue with standard inspect mode
    from geoparquet_io.core.duckdb_metadata import get_usable_columns
    from geoparquet_io.core.inspect_utils import (
        extract_partition_summary,
        format_partition_json_output,
        format_partition_markdown_output,
        format_partition_terminal_output,
    )
    from geoparquet_io.core.partition_reader import get_partition_info

    # Check for partition and handle --check-all
    if check_all_files:
        # Validate --check-all is not used with preview/stats options
        if head is not None or tail is not None:
            raise click.UsageError("--check-all cannot be used with --head or --tail")
        if stats:
            raise click.UsageError("--check-all cannot be used with --stats")

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, parquet_file)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, parquet_file)

    try:
        # Check if this is a partition
        partition_info = get_partition_info(parquet_file, verbose=False)

        if partition_info["is_partition"] and check_all_files:
            # Partition mode with --check-all: aggregate all files
            all_files = partition_info["all_files"]

            if not all_files:
                raise click.ClickException("No parquet files found in partition")

            # Get partition summary
            partition_summary = extract_partition_summary(all_files, verbose=False)

            # Get geo info and columns from first file for column schema
            first_file = partition_info["first_file"]
            geo_info = extract_geo_info(first_file)
            usable_columns = get_usable_columns(first_file)
            primary_geom_col = geo_info.get("primary_column")

            columns_info = [
                {
                    "name": col["name"],
                    "type": col["type"],
                    "is_geometry": col["name"] == primary_geom_col,
                }
                for col in usable_columns
            ]

            # Output
            if json_output:
                output = format_partition_json_output(partition_summary, geo_info, columns_info)
                click.echo(output)
            elif markdown_output:
                output = format_partition_markdown_output(partition_summary, geo_info, columns_info)
                click.echo(output)
            else:
                format_partition_terminal_output(partition_summary, geo_info, columns_info)

            return

        # Single file mode (or partition without --check-all - inspect first file)
        file_to_inspect = parquet_file
        if partition_info["is_partition"]:
            # Partition without --check-all: inspect first file with notice
            file_to_inspect = partition_info["first_file"]
            file_count = partition_info["file_count"]
            click.echo(
                click.style(
                    f"Inspecting first file (of {file_count} total). "
                    "Use --check-all to aggregate all files.",
                    fg="cyan",
                )
            )
            click.echo()

        # Extract metadata
        file_info = extract_file_info(file_to_inspect)
        geo_info = extract_geo_info(file_to_inspect)

        # Get usable columns using DESCRIBE (handles nested struct wrappers correctly)
        usable_columns = get_usable_columns(file_to_inspect)
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
        if head is not None or tail is not None:
            preview_table, preview_mode = get_preview_data(file_to_inspect, head=head, tail=tail)

        # Get statistics if requested
        statistics = None
        if stats:
            statistics = get_column_statistics(file_to_inspect, columns_info)

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
@cli.command(cls=GlobAwareCommand)
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
@partition_input_options
@dry_run_option
@show_sql_option
@verbose_option
@profile_option
@any_extension_option
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
    allow_schema_diff,
    hive_input,
    dry_run,
    show_sql,
    verbose,
    profile,
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
            --profile my-aws \\
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
            allow_schema_diff=allow_schema_diff,
            hive_input=hive_input,
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


@cli.command(hidden=True)  # Deprecated: use 'gpio inspect --meta' instead
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
    [DEPRECATED] Show comprehensive metadata for a GeoParquet file.

    This command is deprecated. Use 'gpio inspect --meta' instead.
    """
    from geoparquet_io.core.common import setup_aws_profile_if_needed, validate_profile_for_urls
    from geoparquet_io.core.logging_config import warn

    # Show deprecation warning
    warn(
        "'gpio meta' is deprecated and will be removed in a future release. "
        "Use 'gpio inspect --meta' instead."
    )

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
@output_format_options
@geoparquet_version_option
@profile_option
@verbose_option
@any_extension_option
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
    any_extension,
):
    """
    Reorder a GeoParquet file using Hilbert curve ordering.

    Takes an input GeoParquet file and creates a new file with rows ordered
    by their position along a Hilbert space-filling curve.

    Applies optimal formatting (configurable compression, optimized row groups,
    bbox metadata) while preserving the CRS.

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.
    """
    # Validate output early - provides helpful error if no output and not piping
    from geoparquet_io.core.streaming import StreamingError, validate_output

    try:
        validate_output(output_parquet)
    except StreamingError as e:
        raise click.ClickException(str(e)) from None

    # Validate .parquet extension
    validate_parquet_extension(output_parquet, any_extension)

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
        raise click.ClickException(str(e)) from None


@sort.command(name="column", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", type=click.Path())
@click.argument("columns")
@click.option(
    "--descending",
    is_flag=True,
    help="Sort in descending order (default: ascending)",
)
@click.option("--profile", help="AWS profile name (for S3 remote outputs)")
@output_format_options
@geoparquet_version_option
@verbose_option
@any_extension_option
def sort_column(
    input_parquet,
    output_parquet,
    columns,
    descending,
    profile,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    geoparquet_version,
    verbose,
    any_extension,
):
    """
    Sort a GeoParquet file by specified column(s).

    COLUMNS is a comma-separated list of column names to sort by.

    Examples:

        gpio sort column input.parquet output.parquet name

        gpio sort column input.parquet output.parquet name,date --descending

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.
    """
    # Validate .parquet extension
    validate_parquet_extension(output_parquet, any_extension)

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
            profile=profile,
            geoparquet_version=geoparquet_version,
        )
    except Exception as e:
        raise click.ClickException(str(e)) from e


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
@click.option("--profile", help="AWS profile name (for S3 remote outputs)")
@output_format_options
@geoparquet_version_option
@verbose_option
@any_extension_option
def sort_quadkey(
    input_parquet,
    output_parquet,
    quadkey_name,
    resolution,
    use_centroid,
    remove_quadkey_column,
    profile,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    geoparquet_version,
    verbose,
    any_extension,
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
    # Validate .parquet extension
    validate_parquet_extension(output_parquet, any_extension)

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
        profile=profile,
        geoparquet_version=geoparquet_version,
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
    "--levels",
    help="Comma-separated hierarchical levels to add as columns (e.g., 'continent,country'). "
    "If not specified, adds all available levels for the dataset.",
)
@click.option(
    "--add-bbox", is_flag=True, help="Automatically add bbox column and metadata if missing."
)
@output_format_options
@geoparquet_version_option
@profile_option
@dry_run_option
@verbose_option
@any_extension_option
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
    any_extension,
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
    from geoparquet_io.core.streaming import is_stdin, should_stream_output

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


@add.command(name="bbox", cls=SingleFileCommand)
@click.argument("input_parquet")
@click.argument("output_parquet", required=False, default=None)
@click.option("--bbox-name", default="bbox", help="Name for the bbox column (default: bbox)")
@click.option(
    "--force",
    is_flag=True,
    help="Replace existing bbox column instead of skipping",
)
@output_format_options
@geoparquet_version_option
@profile_option
@dry_run_option
@verbose_option
@any_extension_option
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
    any_extension,
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
    # Validate output early - provides helpful error if no output and not piping
    from geoparquet_io.core.streaming import StreamingError, validate_output

    try:
        validate_output(output_parquet)
    except StreamingError as e:
        raise click.ClickException(str(e)) from None

    # Validate .parquet extension
    validate_parquet_extension(output_parquet, any_extension)

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

    from geoparquet_io.core.streaming import StreamingError

    try:
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
    except StreamingError as e:
        raise click.ClickException(str(e)) from None


@add.command(name="bbox-metadata", cls=SingleFileCommand)
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
@profile_option
@dry_run_option
@verbose_option
@any_extension_option
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
    any_extension,
):
    """Add an H3 cell ID column to a GeoParquet file.

    Computes H3 hexagonal cell IDs based on geometry centroids. H3 is a hierarchical
    hexagonal geospatial indexing system that provides consistent cell sizes and shapes
    across the globe.

    The cell ID is stored as a VARCHAR (string) for maximum portability across tools.
    Resolution determines cell size - higher values mean smaller cells with more precision.

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.
    """
    # Validate output early - provides helpful error if no output and not piping
    from geoparquet_io.core.streaming import StreamingError, validate_output

    try:
        validate_output(output_parquet)
    except StreamingError as e:
        raise click.ClickException(str(e)) from None

    # Validate .parquet extension
    validate_parquet_extension(output_parquet, any_extension)

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
@profile_option
@dry_run_option
@click.option(
    "--force",
    is_flag=True,
    help="Force operation on large datasets without confirmation",
)
@verbose_option
@any_extension_option
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
    any_extension,
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
@click.option("--profile", help="AWS profile name (for S3 remote outputs)")
@output_format_options
@geoparquet_version_option
@dry_run_option
@verbose_option
@any_extension_option
def add_quadkey(
    input_parquet,
    output_parquet,
    quadkey_name,
    resolution,
    use_centroid,
    profile,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    geoparquet_version,
    dry_run,
    verbose,
    any_extension,
):
    """Add a quadkey column to a GeoParquet file.

    Computes quadkey tile IDs based on geometry location. By default, uses the
    bbox column midpoint if available, otherwise falls back to geometry centroid.

    Quadkeys are a way of encoding tile coordinates (x, y, zoom) into a single
    string, providing a compact spatial index that is particularly useful for
    mapping applications and tile-based systems.

    Supports both local and remote (S3, GCS, Azure) inputs and outputs.
    """
    # Validate output early - provides helpful error if no output and not piping
    from geoparquet_io.core.streaming import StreamingError, validate_output

    try:
        validate_output(output_parquet)
    except StreamingError as e:
        raise click.ClickException(str(e)) from None

    # Validate .parquet extension
    validate_parquet_extension(output_parquet, any_extension)

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
            profile=profile,
            geoparquet_version=geoparquet_version,
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


@partition.command(name="string", cls=SingleFileCommand)
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
    from geoparquet_io.core.streaming import StreamingError

    # If preview mode, output_folder is not required
    if not preview and not output_folder:
        raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

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
            profile,
            geoparquet_version,
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
    default=13,
    help="Resolution for auto-adding quadkey column (0-23, default: 13)",
)
@click.option(
    "--partition-resolution",
    type=click.IntRange(0, 23),
    default=9,
    help="Resolution for partitioning as prefix length (0-23, default: 9)",
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
@verbose_option
@profile_option
@geoparquet_version_option
def partition_quadkey(
    input_parquet,
    output_folder,
    quadkey_column,
    resolution,
    partition_resolution,
    use_centroid,
    keep_quadkey_column,
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
    """Partition a GeoParquet file by quadkey cells.

    Creates separate GeoParquet files based on quadkey prefixes at the specified
    partition resolution. If the quadkey column doesn't exist, it will be automatically
    added at the specified resolution before partitioning.

    The column is created at --resolution (default 13), but partitions are created using
    the first --partition-resolution characters (default 9) of each quadkey. This allows
    for coarser partitioning while retaining full precision in the column.

    By default, the quadkey column is excluded from output files (since it's redundant
    with the partition path) unless using Hive-style partitioning. Use --keep-quadkey-column
    to explicitly keep the column in all cases.

    Use --preview to see what partitions would be created without actually creating files.

    Examples:

        # Preview partitions
        gpio partition quadkey input.parquet --preview

        # Partition by quadkey cells (column excluded from output by default)
        gpio partition quadkey input.parquet output/

        # Partition with quadkey column kept in output files
        gpio partition quadkey input.parquet output/ --keep-quadkey-column

        # Partition at higher resolution (zoom 12)
        gpio partition quadkey input.parquet output/ --partition-resolution 12

        # Use Hive-style partitioning (quadkey column included by default)
        gpio partition quadkey input.parquet output/ --hive
    """
    # If preview mode, output_folder is not required
    if not preview and not output_folder:
        raise click.UsageError("OUTPUT_FOLDER is required unless using --preview")

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
        profile=profile,
        geoparquet_version=geoparquet_version,
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
    try:
        _stac_impl(input, output, bucket, public_url, collection_id, item_id, overwrite, verbose)
    except click.exceptions.Exit:
        raise
    except Exception as e:
        raise click.ClickException(str(e)) from e


@publish.command(name="upload")
@click.argument("source", type=click.Path(exists=True, path_type=Path))
@click.argument("destination", type=str)
@profile_option
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
)
@click.option(
    "--s3-region",
    help="S3 region (default: us-east-1 when using custom endpoint)",
)
@click.option(
    "--s3-no-ssl",
    is_flag=True,
    help="Disable SSL for S3 endpoint (use HTTP instead of HTTPS)",
)
@dry_run_option
def publish_upload(
    source,
    destination,
    profile,
    pattern,
    max_files,
    chunk_concurrency,
    chunk_size,
    fail_fast,
    s3_endpoint,
    s3_region,
    s3_no_ssl,
    dry_run,
):
    """Upload file or directory to object storage.

    Supports S3, GCS, Azure, and HTTP destinations. Automatically handles
    multipart uploads and preserves directory structure.

    \b
    Examples:
      # Single file to S3
      gpio publish upload data.parquet s3://bucket/path/data.parquet --profile source-coop

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
    # Check credentials before attempting upload
    creds_ok, hint = check_credentials(destination, profile)
    if not creds_ok:
        raise click.ClickException(f"Authentication failed:\n\n{hint}")

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
            s3_endpoint=s3_endpoint,
            s3_region=s3_region,
            s3_use_ssl=not s3_no_ssl,
        )
    except click.exceptions.Exit:
        raise
    except Exception as e:
        raise click.ClickException(str(e)) from e


@cli.command(hidden=True)  # Deprecated: use 'gpio publish stac' instead
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
    [DEPRECATED] Generate STAC Item or Collection from GeoParquet file(s).

    This command is deprecated. Use 'gpio publish stac' instead.
    """
    from geoparquet_io.core.logging_config import warn

    warn(
        "'gpio stac' is deprecated and will be removed in a future release. "
        "Use 'gpio publish stac' instead."
    )
    try:
        _stac_impl(input, output, bucket, public_url, collection_id, item_id, overwrite, verbose)
    except click.exceptions.Exit:
        raise
    except Exception as e:
        raise click.ClickException(str(e)) from e


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
@profile_option
def check_spec(
    parquet_file,
    geoparquet_version,
    json_output,
    skip_data_validation,
    sample_size,
    verbose,
    profile,
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
    from geoparquet_io.core.common import (
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

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, parquet_file)
    setup_aws_profile_if_needed(profile, parquet_file)

    try:
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
    except click.exceptions.Exit:
        raise
    except Exception as e:
        raise click.ClickException(str(e)) from e


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


@cli.command(hidden=True)  # Deprecated: use 'gpio publish upload' instead
@click.argument("source", type=str)  # String type to support stdin ("-")
@click.argument("destination", type=str)
@profile_option
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
)
@click.option(
    "--s3-region",
    help="S3 region (default: us-east-1 when using custom endpoint)",
)
@click.option(
    "--s3-no-ssl",
    is_flag=True,
    help="Disable SSL for S3 endpoint (use HTTP instead of HTTPS)",
)
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
    s3_endpoint,
    s3_region,
    s3_no_ssl,
    dry_run,
):
    """
    [DEPRECATED] Upload file or directory to object storage.

    This command is deprecated. Use 'gpio publish upload' instead.
    """
    from geoparquet_io.core.logging_config import warn
    from geoparquet_io.core.streaming import is_stdin, read_stdin_to_temp_file

    warn(
        "'gpio upload' is deprecated and will be removed in a future release. "
        "Use 'gpio publish upload' instead."
    )

    # Check credentials before attempting upload
    creds_ok, hint = check_credentials(destination, profile)
    if not creds_ok:
        raise click.ClickException(f"Authentication failed:\n\n{hint}")

    try:
        # Handle stdin input
        if is_stdin(source):
            temp_path = read_stdin_to_temp_file()
            try:
                upload_impl(
                    source=Path(temp_path),
                    destination=destination,
                    profile=profile,
                    chunk_concurrency=chunk_concurrency,
                    chunk_size=chunk_size,
                    dry_run=dry_run,
                    s3_endpoint=s3_endpoint,
                    s3_region=s3_region,
                    s3_use_ssl=not s3_no_ssl,
                )
            finally:
                Path(temp_path).unlink(missing_ok=True)
        else:
            source_path = Path(source)
            if not source_path.exists():
                raise click.ClickException(f"Source path does not exist: {source}")
            upload_impl(
                source=source_path,
                destination=destination,
                profile=profile,
                pattern=pattern,
                max_files=max_files,
                chunk_concurrency=chunk_concurrency,
                chunk_size=chunk_size,
                fail_fast=fail_fast,
                dry_run=dry_run,
                s3_endpoint=s3_endpoint,
                s3_region=s3_region,
                s3_use_ssl=not s3_no_ssl,
            )
    except click.exceptions.Exit:
        raise
    except Exception as e:
        raise click.ClickException(str(e)) from e


@cli.command(hidden=True)  # Deprecated: use 'gpio check spec' instead
@click.argument("parquet_file")
@click.option(
    "--geoparquet-version",
    type=click.Choice(["1.0", "1.1", "2.0", "parquet-geo-only"]),
    default=None,
    help="Validate against specific GeoParquet version or parquet-geo-only (default: auto-detect)",
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
@profile_option
def validate(
    parquet_file,
    geoparquet_version,
    json_output,
    skip_data_validation,
    sample_size,
    verbose,
    profile,
):
    """
    [DEPRECATED] Validate a GeoParquet file against specification requirements.

    This command is deprecated. Use 'gpio check spec' instead.
    """
    from geoparquet_io.core.common import (
        setup_aws_profile_if_needed,
        validate_profile_for_urls,
    )
    from geoparquet_io.core.logging_config import warn
    from geoparquet_io.core.validate import (
        format_json_output as format_json,
    )
    from geoparquet_io.core.validate import (
        format_terminal_output as format_terminal,
    )
    from geoparquet_io.core.validate import (
        validate_geoparquet,
    )

    # Show deprecation warning
    warn(
        "'gpio validate' is deprecated and will be removed in a future release. "
        "Use 'gpio check spec' instead."
    )

    configure_verbose(verbose)

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, parquet_file)
    setup_aws_profile_if_needed(profile, parquet_file)

    try:
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
    except click.exceptions.Exit:
        raise
    except Exception as e:
        raise click.ClickException(str(e)) from e


if __name__ == "__main__":
    cli()
