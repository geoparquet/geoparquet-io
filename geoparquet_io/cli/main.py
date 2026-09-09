import sys
from importlib.metadata import entry_points
from pathlib import Path

import click
from click_plugins import with_plugins

from geoparquet_io.cli._shared import _activate_s3, create_default_group
from geoparquet_io.cli.commands.add import add
from geoparquet_io.cli.commands.benchmark import benchmark
from geoparquet_io.cli.commands.convert import convert
from geoparquet_io.cli.commands.inspect import inspect
from geoparquet_io.cli.commands.partition import partition
from geoparquet_io.cli.commands.pmtiles import pmtiles
from geoparquet_io.cli.commands.process import process
from geoparquet_io.cli.commands.publish import publish
from geoparquet_io.cli.commands.sort import sort
from geoparquet_io.cli.decorators import (
    GlobAwareCommand,
    SingleFileCommand,
    any_extension_option,
    aws_profile_option,
    check_partition_options,
    compression_options,
    dry_run_option,
    geoparquet_version_option,
    handle_geoparquet_errors,
    output_format_options,
    overwrite_option,
    parse_row_group_options,
    partition_input_options,
    repair_geometry_option,
    row_group_options,
    show_sql_option,
    verbose_option,
    write_strategy_option,
)
from geoparquet_io.cli.fix_helpers import handle_fix_common
from geoparquet_io.core.check_parquet_structure import CheckProfile
from geoparquet_io.core.check_parquet_structure import check_all as check_structure_impl
from geoparquet_io.core.check_spatial_order import check_spatial_order as check_spatial_impl
from geoparquet_io.core.extract import extract as extract_impl
from geoparquet_io.core.file_utils import validate_parquet_extension
from geoparquet_io.core.logging_config import configure_verbose, setup_cli_logging
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


# ---------------------------------------------------------------------------
# Command groups extracted into geoparquet_io/cli/commands/
#
# Each of those modules declares a standalone `@click.group()` and is attached
# here, explicitly, one line per group. Registration is deliberately not done by
# the group module itself (no `@cli.group()` there, no import-time
# self-registration): that would need `cli.main`, and `cli.main` imports the
# group -- a cycle. Keeping it here also means `grep add_command` lists the
# whole tree.
# ---------------------------------------------------------------------------
cli.add_command(add)
cli.add_command(benchmark)
cli.add_command(convert)
cli.add_command(inspect)
cli.add_command(partition)
cli.add_command(pmtiles)
cli.add_command(process)
cli.add_command(publish)
cli.add_command(sort)


# Create default group classes using the factory
DefaultGroup = create_default_group(
    "all",
    "Custom Group that invokes a default command when no subcommand is provided.",
)


ExtractDefaultGroup = create_default_group(
    "geoparquet",
    """Custom Group that invokes 'geoparquet' when no subcommand is provided.

This allows backwards compatibility:
- gpio extract input.parquet output.parquet  -> invokes geoparquet
- gpio extract geoparquet input.parquet output.parquet -> explicit
- gpio extract bigquery project.dataset.table output.parquet -> subcommand""",
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

            # Pushdown readiness metric
            if show_output:
                from geoparquet_io.core.check_spatial_order import check_spatial_pushdown_readiness

                try:
                    pushdown = check_spatial_pushdown_readiness(
                        file_path, verbose=verbose and show_output
                    )
                except Exception as e:
                    if verbose:
                        click.echo(f"  Debug: Pushdown check failed: {e}", err=True)
                    pushdown = {"has_geo_bbox": False}

                click.echo("\nSpatial Filter Pushdown Readiness:")
                if pushdown.get("has_geo_bbox"):
                    skip_pct = pushdown["estimated_skip_rate"] * 100
                    click.echo(f"  Row groups: {pushdown['num_row_groups']}")
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


if __name__ == "__main__":
    cli()
