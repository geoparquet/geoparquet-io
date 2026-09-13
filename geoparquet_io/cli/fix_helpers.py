"""Helper functions for check --fix CLI commands."""

import os
import shutil

import click

from geoparquet_io.core.file_utils import is_same_file_path
from geoparquet_io.core.geo_metadata import BBOX_REWRITE_HINT
from geoparquet_io.core.remote import is_remote_url


class NoBackupConfirmation:
    """The one ``--no-backup`` prompt a whole ``--fix`` run gets.

    The confirmation used to live inside each per-file fix, so a glob matching
    N files asked N times: a non-interactive run aborted on the first and left
    the rest unexamined, and an interactive one could be answered "y" for
    ``a.parquet`` and "n" for ``b.parquet``, ending half-done with no backups
    of what it had already overwritten (#1041).

    It is asked *lazily* -- on the first file that is actually about to be
    rewritten in place -- so a run that finds nothing to fix, or one writing to
    ``--fix-output``, is never prompted at all. That is the behaviour the
    single-file case already had, and it is why the count below is the number
    of files matched rather than the number that will turn out to need work:
    the prompt has to come before the first rewrite, which is before gpio knows
    how many of the rest will need one.
    """

    def __init__(self, no_backup: bool, file_count: int = 1):
        self.no_backup = no_backup
        self.file_count = file_count
        self._asked = False

    def ensure(self, parquet_file: str, output_path: str) -> None:
        """Confirm, once, before the first in-place rewrite of a local file."""
        if not self.no_backup or self._asked:
            return
        if not is_same_file_path(output_path, parquet_file) or is_remote_url(parquet_file):
            return

        self._asked = True
        target = (
            "the original file"
            if self.file_count == 1
            else f"up to {self.file_count} original files under {os.path.dirname(parquet_file) or '.'}"
        )
        click.confirm(f"This will overwrite {target} without backup. Continue?", abort=True)


def validate_remote_file_modification(parquet_file, fix_output, overwrite):
    """Validate remote file modification parameters."""
    is_remote = is_remote_url(parquet_file)
    if not is_remote:
        return is_remote

    if not fix_output:
        click.echo(
            click.style(
                "⚠ Warning: Modifying remote files in-place cannot create .bak backups.",
                fg="yellow",
            )
        )

        if not overwrite:
            raise click.BadParameter(
                "Cannot modify remote file without --overwrite flag. "
                "Use --overwrite to confirm overwriting the remote file, "
                "or use --fix-output to specify a different output path."
            )

        click.echo("Proceeding with remote file overwrite (no backup will be created)...")

    return is_remote


def create_backup_if_needed(
    parquet_file, output_path, no_backup, is_remote, verbose, quiet: bool = False
):
    """Create backup file if needed for local files."""
    backup_path = f"{parquet_file}.bak"

    if (
        not no_backup
        and is_same_file_path(output_path, parquet_file)
        and os.path.exists(parquet_file)
        and not is_remote
    ):
        if verbose:
            click.echo(f"\nCreating backup: {backup_path}")
        shutil.copy2(parquet_file, backup_path)
        if not quiet:
            click.echo(click.style(f"✓ Created backup: {backup_path}", fg="green"))
        return backup_path
    return None


def verify_fixes(
    output_path,
    check_structure_impl,
    check_spatial_impl,
    random_sample_size,
    limit_rows,
    quiet: bool = False,
):
    """Re-run checks to verify fixes were successful; returns whether they all pass.

    ``quiet`` (a multi-file run) prints one line only when issues remain.
    """
    if not quiet:
        click.echo("\nRe-validating after fixes...")
        click.echo("=" * 60)

    final_structure_results = check_structure_impl(output_path, verbose=False, return_results=True)
    final_spatial_result = check_spatial_impl(
        output_path, random_sample_size, limit_rows, verbose=False, return_results=True
    )

    # Collect failing checks with their issues
    failing_checks = []
    check_names = {
        "row_groups": "Row Groups",
        "bbox": "Bbox/Metadata",
        "compression": "Compression",
    }

    for check_key, result in final_structure_results.items():
        if isinstance(result, dict) and not result.get("passed", False):
            check_name = check_names.get(check_key, check_key)
            issues = result.get("issues", [])
            failing_checks.append((check_name, issues))

    if isinstance(final_spatial_result, dict) and not final_spatial_result.get("passed", False):
        issues = final_spatial_result.get("issues", [])
        failing_checks.append(("Spatial Ordering", issues))

    all_passed = len(failing_checks) == 0

    if quiet:
        if not all_passed:
            remaining = "; ".join(name for name, _ in failing_checks)
            click.echo(
                click.style(
                    f"  ⚠ {os.path.basename(output_path)}: issues remain after fixes ({remaining})",
                    fg="yellow",
                )
            )
        return all_passed

    if all_passed:
        click.echo(click.style("\n✓ All checks passed after fixes!", fg="green", bold=True))
    else:
        click.echo(click.style("\n⚠️  Some issues remain after fixes:", fg="yellow", bold=True))
        for check_name, issues in failing_checks:
            if issues:
                for issue in issues:
                    click.echo(click.style(f"   - {check_name}: {issue}", fg="yellow"))
            else:
                click.echo(click.style(f"   - {check_name}: check did not pass", fg="yellow"))

    return all_passed


def handle_fix_error(e, no_backup, output_path, parquet_file, backup_path):
    """Put a failed in-place fix back the way it was, and remove the ``.bak``.

    The failure itself is reported by the caller (the runner names the file
    and the error once); this only undoes the backup step.
    """
    if (
        not no_backup
        and is_same_file_path(output_path, parquet_file)
        and backup_path
        and os.path.exists(backup_path)
    ):
        click.echo("Restoring from backup...")
        shutil.copy2(backup_path, parquet_file)
        os.remove(backup_path)


def handle_fix_common(
    parquet_file,
    fix_output,
    fix_func,
    verbose=False,
    overwrite=False,
    profile=None,
    *,
    confirmation: NoBackupConfirmation,
    quiet: bool = False,
):
    """Handle common fix logic: backup, output path, and fix application.

    Args:
        parquet_file: Input file path
        fix_output: Custom output path or None
        fix_func: Function to call for fixing (takes input_path, output_path, verbose, profile)
        verbose: Print verbose output
        overwrite: Whether to allow overwriting remote files
        profile: AWS profile name for S3 operations
        confirmation: the run's :class:`NoBackupConfirmation`, shared across
            every file a multi-file ``--fix`` touches; it also carries the
            ``--no-backup`` flag.
        quiet: a multi-file run -- no per-file "Created backup" line.

    Returns:
        tuple: (output_path, backup_path or None)
    """
    no_backup = confirmation.no_backup
    # Handle remote files
    if is_remote_url(parquet_file):
        if not fix_output:
            # Warn about remote file modification
            click.echo(
                click.style(
                    "⚠ Warning: Modifying remote files in-place cannot create .bak backups.",
                    fg="yellow",
                )
            )

            if not overwrite:
                raise click.BadParameter(
                    "Cannot modify remote file without --overwrite flag. "
                    "Use --overwrite to confirm overwriting the remote file, "
                    "or use --fix-output to specify a different output path."
                )

            click.echo("Proceeding with remote file overwrite (no backup will be created)...")

    output_path = fix_output or parquet_file
    backup_path = f"{parquet_file}.bak"

    # `--fix-output ./same.parquet` is an in-place fix: comparing the raw strings
    # sent it down the "different file" branch, so it got no .bak and no
    # confirmation, while handle_output_overwrite -- which compares resolve() --
    # refused the write anyway (#959).
    fixing_in_place = is_same_file_path(output_path, parquet_file)

    # Confirm overwrite without backup for local files -- once per run (#1041).
    confirmation.ensure(parquet_file, output_path)

    # Create backup if needed (only for local files)
    if (
        not no_backup
        and fixing_in_place
        and os.path.exists(parquet_file)
        and not is_remote_url(parquet_file)
    ):
        shutil.copy2(parquet_file, backup_path)
        if not quiet:
            click.echo(click.style(f"✓ Created backup: {backup_path}", fg="green"))
        created_backup = backup_path
    else:
        created_backup = None

    # A fix that fails leaves no orphan .bak beside an unrewritten file: the
    # staged rewrite never landed, so the backup is put back and removed.
    try:
        fix_func(parquet_file, output_path, verbose, profile)
    except Exception as e:
        handle_fix_error(e, no_backup, output_path, parquet_file, created_backup)
        raise

    return output_path, created_backup


def display_spatial_result(spatial_result, show_output):
    """Display spatial order check result.

    Args:
        spatial_result: Can be either a dict with 'ratio' and 'passed' keys,
                       or just a float ratio for backward compatibility
        show_output: Whether to display output
    """
    if not show_output:
        return

    # Handle both dict result and plain ratio for backward compatibility
    if isinstance(spatial_result, dict):
        ratio = spatial_result.get("ratio")
        passed = spatial_result.get("passed", ratio < 0.5 if ratio is not None else True)
    else:
        ratio = spatial_result
        passed = ratio < 0.5 if ratio is not None else True

    if ratio is None:
        return

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


def aggregate_check_results(structure_results, spatial_result):
    """Aggregate check results for runner tracking.

    Returns:
        tuple: (combined_passed, combined_issues, all_check_results)
    """
    all_check_results = {**structure_results, "spatial": spatial_result}
    combined_passed = all(
        r.get("passed", True) for r in all_check_results.values() if isinstance(r, dict)
    )
    combined_issues = []
    for r in all_check_results.values():
        if isinstance(r, dict):
            combined_issues.extend(r.get("issues", []))
    return combined_passed, combined_issues, all_check_results


def apply_check_all_fixes(
    file_path,
    all_results,
    fix_output,
    overwrite,
    verbose,
    profile,
    check_structure_impl,
    check_spatial_impl,
    random_sample_size,
    limit_rows,
    *,
    confirmation: NoBackupConfirmation,
    quiet: bool = False,
):
    """Apply all fixes for check_all command.

    Extracts the fix logic from check_all to reduce complexity.

    Args:
        file_path: Path to the file to fix
        all_results: Combined results from all checks
        fix_output: Custom output path or None
        overwrite: Allow overwriting remote files
        verbose: Print verbose output
        profile: AWS profile for S3
        check_structure_impl: Function to run structure checks
        check_spatial_impl: Function to run spatial checks
        random_sample_size: Sample size for spatial check
        limit_rows: Row limit for spatial check
        confirmation: the run's :class:`NoBackupConfirmation`, which also
            carries the ``--no-backup`` flag; see :func:`handle_fix_common`.
        quiet: a multi-file run -- the per-file banners are replaced by the
            runner's progress line and end-of-run summary.

    Returns:
        ``(output_path, backup_path or None)`` for a file this call rewrote, or
        ``None`` when the file needed no fixes. The caller records the pair in
        the run's summary, so a multi-file ``check all --fix`` names every file
        it touched (#1041).
    """
    from geoparquet_io.core.check_fixes import apply_all_fixes

    no_backup = confirmation.no_backup

    # Check if any fixes are needed
    needs_fixes = any(
        result.get("fix_available", False)
        for result in all_results.values()
        if isinstance(result, dict)
    )

    if not needs_fixes:
        problem = (all_results.get("bbox") or {}).get("covering_problem")
        if problem:
            click.echo(
                click.style(
                    f"\n⚠ Nothing --fix can repair here - {problem}. {BBOX_REWRITE_HINT}",
                    fg="yellow",
                )
            )
        elif not quiet:
            click.echo(click.style("\n✓ No fixes needed - file is already optimal!", fg="green"))
        return None

    # Handle remote files
    is_remote = validate_remote_file_modification(file_path, fix_output, overwrite)

    # Determine output path. An aliased --fix-output (`./in.parquet` for
    # `in.parquet`) is an in-place fix and gets the backup and the prompt;
    # comparing the raw strings here is what let `check all` overwrite an
    # input with neither (#1036, the #959 shape).
    output_path = fix_output or file_path
    fixing_in_place = is_same_file_path(output_path, file_path)

    # Confirm overwrite without backup for local files -- once per run (#1041).
    confirmation.ensure(file_path, output_path)

    # Create backup if needed (only for local files)
    backup_path = create_backup_if_needed(
        file_path, output_path, no_backup, is_remote, verbose, quiet=quiet
    )

    # Apply fixes
    if not quiet:
        click.echo("\n" + "=" * 60)
        click.echo("Applying fixes...")
        click.echo("=" * 60)

    try:
        fixes_summary = apply_all_fixes(file_path, output_path, all_results, verbose, profile)

        if not quiet:
            click.echo("\n" + "=" * 60)
            click.echo("Fixes applied:")
            for applied_fix in fixes_summary["fixes_applied"]:
                click.echo(click.style(f"  ✓ {applied_fix}", fg="green"))
            click.echo("=" * 60)

        # Re-run checks to verify
        verify_fixes(
            output_path,
            check_structure_impl,
            check_spatial_impl,
            random_sample_size,
            limit_rows,
            quiet=quiet,
        )

        if not quiet:
            click.echo(f"\nOptimized file: {output_path}")
            if not no_backup and fixing_in_place and backup_path and os.path.exists(backup_path):
                click.echo(f"Backup: {backup_path}")

    except Exception as e:
        handle_fix_error(e, no_backup, output_path, file_path, backup_path)
        raise

    return output_path, backup_path
