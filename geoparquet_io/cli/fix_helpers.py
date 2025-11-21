"""Helper functions for check --fix CLI commands."""

import os
import shutil

import click


def handle_fix_common(parquet_file, fix_output, no_backup, fix_func, verbose=False):
    """Handle common fix logic: backup, output path, and fix application.

    Args:
        parquet_file: Input file path
        fix_output: Custom output path or None
        no_backup: Whether to skip backup
        fix_func: Function to call for fixing (takes input_path, output_path, verbose)
        verbose: Print verbose output

    Returns:
        tuple: (output_path, backup_path or None)
    """
    # Handle remote files
    if parquet_file.startswith(("http://", "https://", "s3://")):
        if not fix_output:
            raise click.BadParameter(
                "Cannot fix remote file in-place. Use --fix-output to specify local output path."
            )

    output_path = fix_output or parquet_file
    backup_path = f"{parquet_file}.bak"

    # Confirm overwrite without backup
    if no_backup and not fix_output:
        click.confirm("This will overwrite the original file without backup. Continue?", abort=True)

    # Create backup if needed
    if not no_backup and output_path == parquet_file and os.path.exists(parquet_file):
        shutil.copy2(parquet_file, backup_path)
        click.echo(click.style(f"✓ Created backup: {backup_path}", fg="green"))
        created_backup = backup_path
    else:
        created_backup = None

    # Apply fix
    fix_func(parquet_file, output_path, verbose)

    return output_path, created_backup
