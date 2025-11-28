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

    # Check if all passed
    all_passed = all(
        result.get("passed", False)
        for result in [
            *final_structure_results.values(),
            final_spatial_result,
        ]
        if isinstance(result, dict)
    )

    if all_passed:
        click.echo(click.style("\n✓ All checks passed after fixes!", fg="green", bold=True))
    else:
        click.echo(click.style("\n⚠️  Some issues may remain after fixes", fg="yellow", bold=True))

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
