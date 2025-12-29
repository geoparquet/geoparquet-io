"""Helper functions for check --fix CLI commands."""

import os
import shutil

import click

from geoparquet_io.core.common import is_remote_url


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


def create_backup_if_needed(parquet_file, output_path, no_backup, is_remote, verbose):
    """Create backup file if needed for local files."""
    backup_path = f"{parquet_file}.bak"

    if (
        not no_backup
        and output_path == parquet_file
        and os.path.exists(parquet_file)
        and not is_remote
    ):
        if verbose:
            click.echo(f"\nCreating backup: {backup_path}")
        shutil.copy2(parquet_file, backup_path)
        click.echo(click.style(f"✓ Created backup: {backup_path}", fg="green"))
        return backup_path
    return None


def verify_fixes(
    output_path, check_structure_impl, check_spatial_impl, random_sample_size, limit_rows
):
    """Re-run checks to verify fixes were successful."""
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
    """Handle errors during fix application."""
    click.echo(click.style(f"\n❌ Fix failed: {str(e)}", fg="red"))
    # Restore from backup if it exists
    if (
        not no_backup
        and output_path == parquet_file
        and backup_path
        and os.path.exists(backup_path)
    ):
        click.echo("Restoring from backup...")
        shutil.copy2(backup_path, parquet_file)
        os.remove(backup_path)


def handle_fix_common(
    parquet_file, fix_output, no_backup, fix_func, verbose=False, overwrite=False, profile=None
):
    """Handle common fix logic: backup, output path, and fix application.

    Args:
        parquet_file: Input file path
        fix_output: Custom output path or None
        no_backup: Whether to skip backup
        fix_func: Function to call for fixing (takes input_path, output_path, verbose, profile)
        verbose: Print verbose output
        overwrite: Whether to allow overwriting remote files
        profile: AWS profile name for S3 operations

    Returns:
        tuple: (output_path, backup_path or None)
    """
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

    # Confirm overwrite without backup for local files
    if no_backup and not fix_output and not is_remote_url(parquet_file):
        click.confirm("This will overwrite the original file without backup. Continue?", abort=True)

    # Create backup if needed (only for local files)
    if (
        not no_backup
        and output_path == parquet_file
        and os.path.exists(parquet_file)
        and not is_remote_url(parquet_file)
    ):
        shutil.copy2(parquet_file, backup_path)
        click.echo(click.style(f"✓ Created backup: {backup_path}", fg="green"))
        created_backup = backup_path
    else:
        created_backup = None

    # Apply fix
    fix_func(parquet_file, output_path, verbose, profile)

    return output_path, created_backup


def display_spatial_result(ratio, show_output):
    """Display spatial order check result."""
    if not show_output or ratio is None:
        return

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
    no_backup,
    overwrite,
    verbose,
    profile,
    check_structure_impl,
    check_spatial_impl,
    random_sample_size,
    limit_rows,
):
    """Apply all fixes for check_all command.

    Extracts the fix logic from check_all to reduce complexity.

    Args:
        file_path: Path to the file to fix
        all_results: Combined results from all checks
        fix_output: Custom output path or None
        no_backup: Skip backup creation
        overwrite: Allow overwriting remote files
        verbose: Print verbose output
        profile: AWS profile for S3
        check_structure_impl: Function to run structure checks
        check_spatial_impl: Function to run spatial checks
        random_sample_size: Sample size for spatial check
        limit_rows: Row limit for spatial check

    Returns:
        True if fixes were applied, False if no fixes needed
    """
    from geoparquet_io.core.check_fixes import apply_all_fixes

    # Check if any fixes are needed
    needs_fixes = any(
        result.get("fix_available", False)
        for result in all_results.values()
        if isinstance(result, dict)
    )

    if not needs_fixes:
        click.echo(click.style("\n✓ No fixes needed - file is already optimal!", fg="green"))
        return False

    # Handle remote files
    is_remote = validate_remote_file_modification(file_path, fix_output, overwrite)

    # Determine output path
    output_path = fix_output or file_path

    # Confirm overwrite without backup for local files
    if no_backup and not fix_output and output_path == file_path and not is_remote:
        click.confirm(
            "This will overwrite the original file without backup. Continue?",
            abort=True,
        )

    # Create backup if needed (only for local files)
    backup_path = create_backup_if_needed(file_path, output_path, no_backup, is_remote, verbose)

    # Apply fixes
    click.echo("\n" + "=" * 60)
    click.echo("Applying fixes...")
    click.echo("=" * 60)

    try:
        fixes_summary = apply_all_fixes(file_path, output_path, all_results, verbose, profile)

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
        )

        click.echo(f"\nOptimized file: {output_path}")
        if (
            not no_backup
            and output_path == file_path
            and backup_path
            and os.path.exists(backup_path)
        ):
            click.echo(f"Backup: {backup_path}")

    except Exception as e:
        handle_fix_error(e, no_backup, output_path, file_path, backup_path)
        raise

    return True
