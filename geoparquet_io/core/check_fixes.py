#!/usr/bin/env python3

import os
import shutil
import tempfile

import click
import duckdb

from geoparquet_io.core.add_bbox_column import add_bbox_column
from geoparquet_io.core.add_bbox_metadata import add_bbox_metadata
from geoparquet_io.core.common import (
    detect_geoparquet_file_type,
    get_duckdb_connection,
    get_parquet_metadata,
    get_remote_error_hint,
    is_remote_url,
    needs_httpfs,
    safe_file_url,
    setup_aws_profile_if_needed,
    write_parquet_with_metadata,
)
from geoparquet_io.core.hilbert_order import hilbert_order


def fix_compression(
    parquet_file, output_file, verbose=False, profile=None, geoparquet_version=None
):
    """Re-compress file with ZSTD compression.

    Args:
        parquet_file: Path to input file
        output_file: Path to output file
        verbose: Print additional information
        profile: AWS profile name for S3 operations
        geoparquet_version: GeoParquet version to preserve (1.0, 1.1, 2.0, parquet-geo-only)

    Returns:
        dict with fix summary
    """
    if verbose:
        click.echo("Applying ZSTD compression...")

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, parquet_file, output_file)

    safe_url = safe_file_url(parquet_file, verbose)

    # Get original metadata (only needed for v1.x)
    original_metadata = None
    if geoparquet_version in (None, "1.0", "1.1"):
        original_metadata, _ = get_parquet_metadata(parquet_file, verbose)

    # Read and rewrite with ZSTD compression
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(parquet_file))

    try:
        query = f"SELECT * FROM '{safe_url}'"

        write_parquet_with_metadata(
            con=con,
            query=query,
            output_file=output_file,
            original_metadata=original_metadata,
            compression="ZSTD",
            compression_level=15,
            row_group_rows=100000,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
        )

        return {"fix_applied": "Re-compressed with ZSTD", "success": True}
    except duckdb.IOException as e:
        con.close()
        if is_remote_url(parquet_file):
            hints = get_remote_error_hint(str(e), parquet_file)
            raise click.ClickException(
                f"Failed to read remote file.\n\n{hints}\n\nOriginal error: {str(e)}"
            ) from e
        raise
    finally:
        con.close()


def fix_bbox_column(parquet_file, output_file, verbose=False, profile=None):
    """Add missing bbox column.

    Args:
        parquet_file: Path to input file
        output_file: Path to output file
        verbose: Print additional information
        profile: AWS profile name for S3 operations

    Returns:
        dict with fix summary
    """
    if verbose:
        click.echo("Adding bbox column...")

    add_bbox_column(
        input_parquet=parquet_file,
        output_parquet=output_file,
        bbox_column_name="bbox",
        dry_run=False,
        verbose=verbose,
        compression="ZSTD",
        compression_level=15,
        row_group_rows=100000,
        profile=profile,
    )

    return {"fix_applied": "Added bbox column", "success": True}


def fix_bbox_metadata(parquet_file, output_file, verbose=False, profile=None):
    """Add missing bbox covering metadata.

    Args:
        parquet_file: Path to input file
        output_file: Path to output file (modified in-place)
        verbose: Print additional information
        profile: AWS profile name for S3 operations (not used for metadata-only operation)

    Returns:
        dict with fix summary
    """
    if verbose:
        click.echo("Adding bbox covering metadata...")

    # If output is different from input, copy first
    if parquet_file != output_file:
        shutil.copy2(parquet_file, output_file)

    # add_bbox_metadata modifies in-place
    add_bbox_metadata(output_file, verbose=verbose)

    return {"fix_applied": "Added bbox covering metadata", "success": True}


def fix_bbox_removal(parquet_file, output_file, bbox_column_name, verbose=False, profile=None):
    """Remove bbox column from a file.

    Used for GeoParquet 2.0 and parquet-geo-only files where bbox is not needed
    because native Parquet geo types provide row group statistics for spatial filtering.

    Args:
        parquet_file: Path to input file
        output_file: Path to output file
        bbox_column_name: Name of the bbox column to remove
        verbose: Print additional information
        profile: AWS profile name for S3 operations

    Returns:
        dict with fix summary
    """
    # Always inform user when removing bbox column
    click.echo(f"Removing bbox column '{bbox_column_name}' (not needed for native geo types)")

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, parquet_file, output_file)

    safe_url = safe_file_url(parquet_file, verbose)

    # Detect file type to determine output version
    file_type_info = detect_geoparquet_file_type(parquet_file, verbose)

    # Determine GeoParquet version for output
    if file_type_info["file_type"] == "parquet_geo_only":
        gp_version = "parquet-geo-only"
    elif file_type_info["geo_version"] and file_type_info["geo_version"].startswith("2."):
        gp_version = "2.0"
    else:
        gp_version = "1.1"  # Fallback, shouldn't happen for removal

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(parquet_file))

    try:
        # Select all columns EXCEPT the bbox column
        query = f"SELECT * EXCLUDE ({bbox_column_name}) FROM '{safe_url}'"

        write_parquet_with_metadata(
            con=con,
            query=query,
            output_file=output_file,
            original_metadata=None,  # Don't preserve old metadata with bbox covering
            compression="ZSTD",
            compression_level=15,
            row_group_rows=100000,
            verbose=verbose,
            profile=profile,
            geoparquet_version=gp_version,
        )

        return {"fix_applied": f"Removed bbox column '{bbox_column_name}'", "success": True}
    except duckdb.IOException as e:
        con.close()
        if is_remote_url(parquet_file):
            hints = get_remote_error_hint(str(e), parquet_file)
            raise click.ClickException(
                f"Failed to read remote file.\n\n{hints}\n\nOriginal error: {str(e)}"
            ) from e
        raise
    finally:
        con.close()


def fix_bbox_all(
    parquet_file, output_file, needs_column, needs_metadata, verbose=False, profile=None
):
    """Fix both bbox column and metadata issues.

    Args:
        parquet_file: Path to input file
        output_file: Path to output file
        needs_column: Whether to add bbox column
        needs_metadata: Whether to add bbox metadata
        verbose: Print additional information
        profile: AWS profile name for S3 operations

    Returns:
        dict with fix summary
    """
    current_file = parquet_file
    temp_file = None

    if needs_column:
        temp_file = output_file + ".tmp" if output_file == parquet_file else output_file
        fix_bbox_column(current_file, temp_file, verbose, profile)
        current_file = temp_file

    if needs_metadata or needs_column:
        if current_file != output_file:
            shutil.move(current_file, output_file)
        fix_bbox_metadata(output_file, output_file, verbose, profile)
    elif temp_file and temp_file != output_file:
        shutil.move(temp_file, output_file)

    return {"fix_applied": "Fixed bbox issues", "success": True}


def fix_spatial_ordering(parquet_file, output_file, verbose=False, profile=None):
    """Apply Hilbert spatial ordering.

    Args:
        parquet_file: Path to input file
        output_file: Path to output file
        verbose: Print additional information
        profile: AWS profile name for S3 operations

    Returns:
        dict with fix summary
    """
    if verbose:
        click.echo("Applying Hilbert spatial ordering (this may take a while)...")

    hilbert_order(
        input_parquet=parquet_file,
        output_parquet=output_file,
        add_bbox_flag=False,  # bbox should already be added if needed
        verbose=verbose,
        compression="ZSTD",
        compression_level=15,
        row_group_rows=100000,
        profile=profile,
    )

    return {"fix_applied": "Applied Hilbert spatial ordering", "success": True}


def fix_row_groups(parquet_file, output_file, verbose=False, profile=None, geoparquet_version=None):
    """Rewrite with optimal row group size.

    Args:
        parquet_file: Path to input file
        output_file: Path to output file
        verbose: Print additional information
        profile: AWS profile name for S3 operations
        geoparquet_version: GeoParquet version to preserve (1.0, 1.1, 2.0, parquet-geo-only)

    Returns:
        dict with fix summary
    """
    if verbose:
        click.echo("Optimizing row groups...")

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, parquet_file, output_file)

    safe_url = safe_file_url(parquet_file, verbose)

    # Get original metadata (only needed for v1.x)
    original_metadata = None
    if geoparquet_version in (None, "1.0", "1.1"):
        original_metadata, _ = get_parquet_metadata(parquet_file, verbose)

    # Read and rewrite with optimal row groups
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(parquet_file))

    try:
        query = f"SELECT * FROM '{safe_url}'"

        write_parquet_with_metadata(
            con=con,
            query=query,
            output_file=output_file,
            original_metadata=original_metadata,
            compression="ZSTD",
            compression_level=15,
            row_group_rows=100000,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
        )

        return {"fix_applied": "Optimized row groups", "success": True}
    except duckdb.IOException as e:
        con.close()
        if is_remote_url(parquet_file):
            hints = get_remote_error_hint(str(e), parquet_file)
            raise click.ClickException(
                f"Failed to read remote file.\n\n{hints}\n\nOriginal error: {str(e)}"
            ) from e
        raise
    finally:
        con.close()


def get_geoparquet_version_from_check_results(check_results):
    """Determine the GeoParquet version to use based on check results.

    This helper ensures we preserve the original file's version when fixing.

    Args:
        check_results: Dict containing results from check functions

    Returns:
        str: GeoParquet version string (1.0, 1.1, 2.0, parquet-geo-only) or None for default
    """
    bbox_result = check_results.get("bbox", {})
    file_type = bbox_result.get("file_type", "unknown")

    if file_type == "geoparquet_v2":
        return "2.0"
    elif file_type == "parquet_geo_only":
        return "parquet-geo-only"
    elif file_type == "geoparquet_v1":
        # Check the specific version from metadata
        version = bbox_result.get("version", "1.1.0")
        if version and version.startswith("1.0"):
            return "1.0"
        return "1.1"
    else:
        # Unknown or no geo metadata - default to 1.1
        return None


def apply_all_fixes(parquet_file, output_file, check_results, verbose=False, profile=None):
    """Orchestrate all fixes based on check results.

    Args:
        parquet_file: Path to input file
        output_file: Path to output file
        check_results: Dict containing results from check functions
        verbose: Print additional information
        profile: AWS profile name for S3 operations

    Returns:
        dict with summary of all fixes applied
    """
    if verbose:
        click.echo("\n" + "=" * 60)
        click.echo("Starting fix process...")
        click.echo("=" * 60)

    fixes_applied = []
    current_file = parquet_file
    temp_files = []

    # Determine the GeoParquet version to preserve
    geoparquet_version = get_geoparquet_version_from_check_results(check_results)
    if verbose and geoparquet_version:
        click.echo(f"Preserving GeoParquet version: {geoparquet_version}")

    try:
        # Handle bbox based on file type
        bbox_result = check_results.get("bbox", {})

        # Step 1: Remove bbox column if needed (v2/parquet-geo-only)
        if bbox_result.get("needs_bbox_removal", False):
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet").name
            temp_files.append(temp_file)

            bbox_column_name = bbox_result.get("bbox_column_name")
            fix_bbox_removal(current_file, temp_file, bbox_column_name, verbose, profile)
            current_file = temp_file
            fixes_applied.append(f"Removed bbox column '{bbox_column_name}'")

        # Step 1 (alt): Add bbox column if needed (v1.x)
        elif bbox_result.get("needs_bbox_column", False):
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet").name
            temp_files.append(temp_file)

            if verbose:
                click.echo("\n[1/4] Adding bbox column...")

            fix_bbox_column(current_file, temp_file, verbose, profile)
            current_file = temp_file
            fixes_applied.append("Added bbox column")

        # Step 2: Add bbox metadata if needed (v1.x only, skip for v2/parquet-geo-only)
        if not bbox_result.get("needs_bbox_removal", False) and (
            bbox_result.get("needs_bbox_metadata", False)
            or (
                bbox_result.get("needs_bbox_column", False)
                and not bbox_result.get("has_bbox_metadata", False)
            )
        ):
            # For metadata, we can modify in-place
            if current_file == parquet_file:
                # Need to copy first if we haven't made changes yet
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet").name
                temp_files.append(temp_file)
                shutil.copy2(current_file, temp_file)
                current_file = temp_file

            if verbose:
                click.echo("\n[2/4] Adding bbox covering metadata...")

            fix_bbox_metadata(current_file, current_file, verbose, profile)
            fixes_applied.append("Added bbox covering metadata")

        # Step 3: Apply Hilbert sorting if needed
        spatial_result = check_results.get("spatial", {})
        if spatial_result and spatial_result.get("fix_available", False):
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet").name
            temp_files.append(temp_file)

            if verbose:
                click.echo("\n[3/4] Applying Hilbert spatial ordering...")
                click.echo("(This operation may take several minutes on large files)")

            fix_spatial_ordering(current_file, temp_file, verbose, profile)
            current_file = temp_file
            fixes_applied.append("Applied Hilbert spatial ordering")

        # Step 4: Fix compression + row groups (combined in final write)
        compression_result = check_results.get("compression", {})
        row_groups_result = check_results.get("row_groups", {})

        needs_compression_fix = compression_result.get("fix_available", False)
        needs_row_group_fix = row_groups_result.get("fix_available", False)

        if needs_compression_fix or needs_row_group_fix:
            if verbose:
                click.echo("\n[4/4] Optimizing compression and row groups...")

            # This is the final step, write to the actual output
            fix_compression(current_file, output_file, verbose, profile, geoparquet_version)

            if needs_compression_fix:
                fixes_applied.append("Optimized compression (ZSTD)")
            if needs_row_group_fix:
                fixes_applied.append("Optimized row groups (100k rows/group)")

        elif current_file != output_file:
            # No compression/row group fixes needed, just move to output
            if verbose:
                click.echo("\nMoving to final output location...")
            shutil.move(current_file, output_file)

        # Clean up temp files (except the one we moved to output)
        for temp_file in temp_files:
            if os.path.exists(temp_file) and temp_file != output_file:
                os.remove(temp_file)

        if verbose:
            click.echo("\n" + "=" * 60)
            click.echo("Fix process completed successfully")
            click.echo("=" * 60)

        return {
            "fixes_applied": fixes_applied,
            "output_file": output_file,
            "success": True,
        }

    except Exception as e:
        # Clean up all temp files on error
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception:
                    pass

        raise click.ClickException(f"Failed to apply fixes: {str(e)}") from e
