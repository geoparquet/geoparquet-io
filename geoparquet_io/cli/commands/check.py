"""``gpio check`` - check GeoParquet files for best practices.

Registered on the root ``gpio`` group by ``cli/main.py`` via
``cli.add_command(check)``. This module must not import ``cli.main``: the
dependency runs one way, ``main`` -> ``commands`` -> ``_shared``/``decorators``.

``MultiFileCheckRunner`` below is the group's own progress/summary helper. It is
used by exactly this group, so it lives here rather than in ``cli/_shared.py``.
"""

from pathlib import Path

import click

from geoparquet_io.cli._shared import _activate_s3, create_default_group, init_group_context
from geoparquet_io.cli.decorators import (
    GlobAwareCommand,
    check_partition_options,
    handle_geoparquet_errors,
    overwrite_option,
    verbose_option,
)
from geoparquet_io.cli.fix_helpers import handle_fix_common
from geoparquet_io.core.check_parquet_structure import CheckProfile
from geoparquet_io.core.check_parquet_structure import check_all as check_structure_impl
from geoparquet_io.core.check_spatial_order import check_spatial_order as check_spatial_impl
from geoparquet_io.core.logging_config import configure_verbose

# CheckDefaultGroup: defaults to 'all' when no subcommand is provided
CheckDefaultGroup = create_default_group(
    "all",
    "Custom Group that invokes a default command when no subcommand is provided.",
)


@click.group(cls=CheckDefaultGroup)
@click.pass_context
def check(ctx):
    """Check GeoParquet files for best practices.

    By default, runs all checks (compression, bbox, row groups, and spatial order).
    Use subcommands for specific checks.

    When run without a subcommand, all checks are performed. Options like --fix
    can be used directly without specifying 'all'.
    """
    init_group_context(ctx)


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
