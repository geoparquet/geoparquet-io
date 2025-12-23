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
from geoparquet_io.core.logging_config import debug, info, progress


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
        debug("Applying ZSTD compression...")

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
        debug("Adding bbox column...")

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
        debug("Adding bbox covering metadata...")

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
    info(f"Removing bbox column '{bbox_column_name}' (not needed for native geo types)")

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
        debug("Applying Hilbert spatial ordering (this may take a while)...")

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
        debug("Optimizing row groups...")

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


def _apply_bbox_column_fix(bbox_result, current_file, temp_files, verbose, profile):
    """Handle bbox column addition or removal.

    Returns:
        tuple: (new_current_file, fixes_applied_list)
    """
    fixes = []

    # Remove bbox column if needed (v2/parquet-geo-only)
    if bbox_result.get("needs_bbox_removal", False):
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet").name
        temp_files.append(temp_file)
        bbox_column_name = bbox_result.get("bbox_column_name")
        fix_bbox_removal(current_file, temp_file, bbox_column_name, verbose, profile)
        fixes.append(f"Removed bbox column '{bbox_column_name}'")
        return temp_file, fixes

    # Add bbox column if needed (v1.x)
    if bbox_result.get("needs_bbox_column", False):
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet").name
        temp_files.append(temp_file)
        if verbose:
            progress("\n[1/4] Adding bbox column...")
        fix_bbox_column(current_file, temp_file, verbose, profile)
        fixes.append("Added bbox column")
        return temp_file, fixes

    return current_file, fixes


def _apply_bbox_metadata_fix(bbox_result, current_file, parquet_file, temp_files, verbose, profile):
    """Handle bbox metadata addition.

    Returns:
        tuple: (new_current_file, fixes_applied_list)
    """
    # Skip for v2/parquet-geo-only files
    if bbox_result.get("needs_bbox_removal", False):
        return current_file, []

    needs_metadata = bbox_result.get("needs_bbox_metadata", False)
    added_column_needs_metadata = bbox_result.get(
        "needs_bbox_column", False
    ) and not bbox_result.get("has_bbox_metadata", False)

    if not needs_metadata and not added_column_needs_metadata:
        return current_file, []

    # For metadata, we modify in-place; copy first if unchanged
    if current_file == parquet_file:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet").name
        temp_files.append(temp_file)
        shutil.copy2(current_file, temp_file)
        current_file = temp_file

    if verbose:
        progress("\n[2/4] Adding bbox covering metadata...")

    fix_bbox_metadata(current_file, current_file, verbose, profile)
    return current_file, ["Added bbox covering metadata"]


def _apply_spatial_ordering_fix(check_results, current_file, temp_files, verbose, profile):
    """Handle Hilbert spatial ordering.

    Returns:
        tuple: (new_current_file, fixes_applied_list)
    """
    spatial_result = check_results.get("spatial", {})
    if not spatial_result or not spatial_result.get("fix_available", False):
        return current_file, []

    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet").name
    temp_files.append(temp_file)

    if verbose:
        progress("\n[3/4] Applying Hilbert spatial ordering...")
        progress("(This operation may take several minutes on large files)")

    fix_spatial_ordering(current_file, temp_file, verbose, profile)
    return temp_file, ["Applied Hilbert spatial ordering"]


def _apply_compression_fix(check_results, current_file, output_file, gp_version, verbose, profile):
    """Handle compression and row group optimization.

    Returns:
        list: fixes_applied
    """
    compression_result = check_results.get("compression", {})
    row_groups_result = check_results.get("row_groups", {})

    needs_compression = compression_result.get("fix_available", False)
    needs_row_groups = row_groups_result.get("fix_available", False)

    if not needs_compression and not needs_row_groups:
        # No compression/row group fixes needed, move to output
        if current_file != output_file:
            if verbose:
                debug("\nMoving to final output location...")
            shutil.move(current_file, output_file)
        return []

    if verbose:
        progress("\n[4/4] Optimizing compression and row groups...")

    fix_compression(current_file, output_file, verbose, profile, gp_version)

    fixes = []
    if needs_compression:
        fixes.append("Optimized compression (ZSTD)")
    if needs_row_groups:
        fixes.append("Optimized row groups (100k rows/group)")
    return fixes


def _cleanup_temp_files(temp_files, output_file):
    """Clean up temporary files, excluding the output file."""
    for temp_file in temp_files:
        if os.path.exists(temp_file) and temp_file != output_file:
            try:
                os.remove(temp_file)
            except OSError:
                pass


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
        progress("\n" + "=" * 60)
        progress("Starting fix process...")
        progress("=" * 60)

    fixes_applied = []
    current_file = parquet_file
    temp_files = []

    geoparquet_version = get_geoparquet_version_from_check_results(check_results)
    if verbose and geoparquet_version:
        debug(f"Preserving GeoParquet version: {geoparquet_version}")

    try:
        bbox_result = check_results.get("bbox", {})

        # Step 1: Handle bbox column (add or remove)
        current_file, fixes = _apply_bbox_column_fix(
            bbox_result, current_file, temp_files, verbose, profile
        )
        fixes_applied.extend(fixes)

        # Step 2: Handle bbox metadata
        current_file, fixes = _apply_bbox_metadata_fix(
            bbox_result, current_file, parquet_file, temp_files, verbose, profile
        )
        fixes_applied.extend(fixes)

        # Step 3: Apply Hilbert sorting
        current_file, fixes = _apply_spatial_ordering_fix(
            check_results, current_file, temp_files, verbose, profile
        )
        fixes_applied.extend(fixes)

        # Step 4: Fix compression + row groups
        fixes = _apply_compression_fix(
            check_results, current_file, output_file, geoparquet_version, verbose, profile
        )
        fixes_applied.extend(fixes)

        _cleanup_temp_files(temp_files, output_file)

        if verbose:
            progress("\n" + "=" * 60)
            progress("Fix process completed successfully")
            progress("=" * 60)

        return {
            "fixes_applied": fixes_applied,
            "output_file": output_file,
            "success": True,
        }

    except Exception as e:
        _cleanup_temp_files(temp_files, output_file=None)
        raise click.ClickException(f"Failed to apply fixes: {str(e)}") from e
