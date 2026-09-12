#!/usr/bin/env python3

import contextlib
import os
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager

import duckdb

from geoparquet_io.core.add.bbox import add_bbox_column
from geoparquet_io.core.add.bbox_metadata import add_bbox_metadata
from geoparquet_io.core.common import (
    detect_geoparquet_file_type,
    get_parquet_metadata,
    write_parquet_with_metadata,
)
from geoparquet_io.core.duckdb_utils import (
    get_duckdb_connection,
    quote_identifier,
    sql_path,
)
from geoparquet_io.core.exceptions import GeoParquetError, RemoteAccessError
from geoparquet_io.core.file_utils import is_same_file_path, resolve_file_url
from geoparquet_io.core.hilbert_order import hilbert_order
from geoparquet_io.core.logging_config import debug, info, progress
from geoparquet_io.core.parquet_writer import DEFAULT_ROW_GROUP_ROWS
from geoparquet_io.core.remote import (
    get_remote_error_hint,
    is_remote_url,
    needs_httpfs,
    setup_aws_profile_if_needed,
)

# Every fix below writes DEFAULT_ROW_GROUP_ROWS rows per row group.
#
# `check --fix` is remedial, so the file it leaves behind has to pass the checks
# gpio runs next -- including the stricter of the two row-count bands, the
# 10,000-50,000 rows per group of SPATIAL_ROW_COUNT_RANGE that
# `check optimization` scores as one of its five factors. Until #972 all five
# write sites asked for a hardcoded 100,000, which the writer rounds up to
# 100,352: inside GENERAL_ROW_COUNT_RANGE, outside the spatial one. So
# `check all --fix` produced a file `check optimization` scored [fail] on its
# row-group factor and advised re-partitioning -- a repair that fails the check
# it exists to satisfy.
#
# Targeting the spatial band is the choice #972 asked for, and `--fix` is
# already committed to it: it Hilbert-sorts the file (fix_spatial_ordering), and
# sorting exists to make spatial predicates prune row groups. Groups too large
# to prune well would undo what the sort just bought. Of the two bands it is
# also the one with a measurement behind it (#775).
#
# The number is not typed here. DEFAULT_ROW_GROUP_ROWS is
# align_to_writer_vector(SPATIAL_BAND_TOP_ROWS) -- derived from the band, and
# already snapped to a whole 2,048-row writer vector, so what gpio asks for is
# what lands. It is also exactly what `gpio sort` writes since #967, which is
# what #795 meant by fix_spatial_order inheriting the sort default.
#
# Every rewrite below also hands the write facade the file it is rewriting --
# both `input_file=` and that file's own metadata. A remedial command is held to
# a stricter standard than an ordinary one: whatever `--fix` writes is the file
# gpio's own checks are run against next, so a fix that loses a fact is a fix
# that makes the report worse. Two were lost by withholding the input:
#
# * `input_file=` is the witness `resolve_output_geoparquet_version` and
#   `resolve_input_crs` both answer from. Without it a native-geo-only input --
#   Parquet GEOMETRY logical type, no `geo` key -- was rewritten as 1.1 WKB, and
#   since that logical type is the *only* place such a file keeps its CRS, the
#   CRS went with it. `check spec` then scored the output `✗ coordinates outside
#   valid range for CRS`: EPSG:5070 metres read as degrees, because an absent
#   `crs` means OGC:CRS84 (#1001).
# * `original_metadata` used to be read only for a 1.x output, on the reasoning
#   that 2.0 regenerates its `geo` block anyway. DuckDB does regenerate it, and
#   what it generates carries no `covering` -- so `check all --fix` on a 2.0
#   file with a bbox column discarded the covering and then complained that the
#   bbox column was not declared in one (#1003). Since #738/#772 the carried
#   block is what `_geo_block_to_carry_on_fast_path` substitutes for DuckDB's,
#   and these two rewrites keep every row of their input, so it still describes
#   the output exactly.
#
# The witness must be the file whose *rows* the write reads. Under
# `check all --fix` that is the previous step's scratch file rather than the
# user's input: the fixes chain, and naming the original while reading a
# rewrite is how a wrong CRS comes to be asserted rather than omitted (see
# `resolve_output_geoparquet_version`).


@contextmanager
def _staged_output(output_file: str) -> Iterator[str]:
    """The path a rewrite writes to, put over *output_file* once it is closed.

    A ``COPY`` whose destination already exists is not a plain write: DuckDB
    writes ``tmp_<name>`` beside it and then *moves* that onto the destination.
    Every ``--fix`` here defaults to rewriting the file in place, so the file
    DuckDB moved over was the file the same statement was reading, and nothing
    but the scheduler ordered the release of the scan handle against the move.
    POSIX does not care. Windows refuses to rename over a path any handle still
    holds open: ``IO Error: Could not move file: Access is denied.``, in some
    runs of the same commit and not others (#1032).

    So the rule here is *any existing local destination* is staged -- not only
    one that is the input. That is stronger than the ``is_same_file_path`` test
    the rest of the module asks, on purpose: the hazard is DuckDB moving over an
    existing file, whichever file it is, and it cannot be defeated by two
    spellings of one path. The swap is gpio's own ``os.replace()``, after the
    caller has closed its connection and every reader the write opened.

    A destination that does not exist yet needs none of it. Neither does a
    remote one: ``write_parquet_with_metadata`` stages a remote output locally
    and uploads it, and there is no file on this machine to rename over.
    """
    if is_remote_url(output_file) or not os.path.exists(output_file):
        yield output_file
        return

    staging = _staging_path_beside(output_file)
    try:
        yield staging
        # The rewrite takes the original's mode: DuckDB created the staging
        # file from the umask, and a 0600 file should not come out 0644.
        with contextlib.suppress(OSError):
            shutil.copymode(output_file, staging)
        # os.replace() is atomic on POSIX and Windows for two paths on one
        # filesystem, which _staging_path_beside() guarantees. The destination
        # is never unlinked or truncated first, so a failure here leaves the
        # original intact (#959).
        os.replace(staging, output_file)
    finally:
        # Gone after a successful replace. Still here after a failed write or a
        # failed move -- and discarded either way: the untouched original is the
        # good copy, and a dot-prefixed leftover in the user's data directory
        # would be invisible to `ls` and accumulate. Cleanup must not mask the
        # error that got us here.
        if os.path.exists(staging):
            with contextlib.suppress(OSError):
                os.remove(staging)


def _rewrite_through_staging(
    parquet_file: str,
    output_file: str,
    query: str,
    *,
    verbose: bool,
    profile: str | None,
    geoparquet_version: str | None = None,
    original_metadata: dict | None = None,
) -> None:
    """Run *query* over *parquet_file* and leave the result at *output_file*.

    The one rewrite the three ``COPY``-based fixes share: staged output, one
    DuckDB connection closed before the swap, and the input named as the write
    facade's witness.
    """
    with _staged_output(output_file) as destination:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(parquet_file))
        try:
            write_parquet_with_metadata(
                con=con,
                query=query,
                output_file=destination,
                original_metadata=original_metadata,
                compression="ZSTD",
                compression_level=15,
                row_group_rows=DEFAULT_ROW_GROUP_ROWS,
                verbose=verbose,
                profile=profile,
                geoparquet_version=geoparquet_version,
                # The rows come from `parquet_file` -- which under
                # `check all --fix` is the previous fix's scratch file, not the
                # user's input, and the witness has to be the file actually
                # read (#1001).
                input_file=parquet_file,
            )
        except duckdb.IOException as e:
            if is_remote_url(parquet_file):
                hints = get_remote_error_hint(str(e), parquet_file)
                raise RemoteAccessError(parquet_file, f"{hints}\n\nOriginal error: {str(e)}") from e
            raise
        finally:
            con.close()


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

    raw_url = resolve_file_url(parquet_file, verbose)

    # Both halves of what the input has to say about itself, at every version.
    # See the module docstring: the `geo` block carries a 2.0 input's `covering`
    # (#1003) and the file is the witness the facade resolves version and CRS
    # from (#1001).
    original_metadata, _ = get_parquet_metadata(parquet_file, verbose)

    # Preserve row order from input file (important after Hilbert sorting)
    _rewrite_through_staging(
        parquet_file,
        output_file,
        f"SELECT * FROM read_parquet({sql_path(raw_url)}, hive_partitioning=false)",
        verbose=verbose,
        profile=profile,
        geoparquet_version=geoparquet_version,
        original_metadata=original_metadata,
    )

    return {"fix_applied": "Re-compressed with ZSTD", "success": True}


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
        row_group_rows=DEFAULT_ROW_GROUP_ROWS,
        profile=profile,
        overwrite=True,  # check --fix manages file lifecycle
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

    # Copied, never moved: the input is read here and nothing else, so the
    # caller still has it afterwards.
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

    raw_url = resolve_file_url(parquet_file, verbose)

    # Detect file type to determine output version
    file_type_info = detect_geoparquet_file_type(parquet_file, verbose)

    # Determine GeoParquet version for output
    if file_type_info["file_type"] == "parquet_geo_only":
        gp_version = "parquet-geo-only"
    elif file_type_info["geo_version"] and file_type_info["geo_version"].startswith("2."):
        gp_version = "2.0"
    else:
        gp_version = "1.1"  # Fallback, shouldn't happen for removal

    # Select all columns EXCEPT the bbox column. The column name is read from
    # the file's own schema (see ``_detect_bbox_column_from_table``), so it is
    # attacker-controlled and must be quoted as an identifier -- a bare
    # interpolation lets a crafted column name inject arbitrary SQL into the
    # projection (#918).
    #
    # `original_metadata=None`: don't preserve old metadata with bbox covering.
    # The CRS still survives, from `input_file` -- `gp_version` above answers
    # the version question from the same file, but a native-geo-only input keeps
    # its CRS only in the Parquet logical type, so without the witness the
    # rewrite restates whatever DuckDB read and declares nothing (#1001).
    _rewrite_through_staging(
        parquet_file,
        output_file,
        f"SELECT * EXCLUDE ({quote_identifier(bbox_column_name)}) FROM {sql_path(raw_url)}",
        verbose=verbose,
        profile=profile,
        geoparquet_version=gp_version,
    )

    return {"fix_applied": f"Removed bbox column '{bbox_column_name}'", "success": True}


def fix_bbox_all(
    parquet_file, output_file, needs_column, needs_metadata, verbose=False, profile=None
):
    """Fix both bbox column and metadata issues.

    The input is only ever *read*. It used to be the thing that landed at
    ``output_file``: when a 1.1 file had a bbox column and only the ``covering``
    key was missing, no step rewrote anything, so ``current_file`` was still the
    user's own path when the function reached ``shutil.move(current_file,
    output_file)``. ``gpio check bbox in.parquet --fix --fix-output out.parquet``
    therefore *deleted* ``in.parquet`` -- and ``handle_fix_common`` had taken no
    ``.bak``, correctly, because the path it was asked to write was not the
    input. In place the two paths were equal and the move was skipped, which is
    why only ``--fix-output`` users ever saw it (#1036).

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
    if not needs_column and not needs_metadata:
        return {"fix_applied": "Fixed bbox issues", "success": True}

    # The same staging every other fix in this module uses (#1032): a scratch
    # path beside any destination that already exists, swapped in by gpio's own
    # os.replace() once the work is done. It covers the in-place case -- where
    # add_bbox_column would otherwise be reading the file it is writing -- and
    # the overwrite-an-existing-output case with one rule, and it cannot be
    # defeated by two spellings of one path.
    with _staged_output(output_file) as destination:
        if needs_column:
            fix_bbox_column(parquet_file, destination, verbose, profile)
            # A bbox column no `covering` points at is a bbox column clients
            # cannot find, so the covering follows the column whether or not the
            # checks asked for the two separately.
            fix_bbox_metadata(destination, destination, verbose, profile)
        else:
            # Metadata only, and fix_bbox_metadata COPIES its input before
            # editing the copy's key-value block.
            fix_bbox_metadata(parquet_file, destination, verbose, profile)

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

    # An in-place fix has to be routed through a staging file: hilbert_order()
    # reads the whole input while writing, so handle_output_overwrite() refuses
    # an output that resolves to its own input, and `overwrite=True` does not
    # lift that (#941). Same staging as the three COPY-based fixes; a remote
    # in-place fix is not staged, so hilbert_order's own refusal is what the
    # user sees rather than a half-finished upload.
    with _staged_output(output_file) as destination:
        hilbert_order(
            input_parquet=parquet_file,
            output_parquet=destination,
            add_bbox_flag=False,  # bbox should already be added if needed
            verbose=verbose,
            compression="ZSTD",
            compression_level=15,
            row_group_rows=DEFAULT_ROW_GROUP_ROWS,
            profile=profile,
            overwrite=True,  # check --fix manages file lifecycle
        )

    return {"fix_applied": "Applied Hilbert spatial ordering", "success": True}


def _staging_path_beside(output_file):
    """Reserve a temp path on the same filesystem as *output_file*.

    ``$TMPDIR`` is routinely a different filesystem (a Linux ``/tmp`` tmpfs, a
    container, an NFS home, an external volume), so staging there makes a
    multi-GB in-place fix exhaust a tmpfs the destination would have
    accommodated, and degrades the move back into a copy+unlink -- which is what
    makes it non-atomic. Co-locating keeps ``os.replace()`` a rename. The dot
    prefix keeps the in-flight file out of ``glob.glob("*.parquet")`` and out
    of a plain ``ls`` -- not out of ``pathlib.Path.glob``, which matches
    dotfiles, so an orphan left by a hard kill is still visible to gpio's own
    directory walks.
    """
    directory = None if is_remote_url(output_file) else (os.path.dirname(output_file) or ".")
    fd, temp_path = tempfile.mkstemp(dir=directory, prefix=".gpio-fix-", suffix=".parquet")
    os.close(fd)
    os.unlink(temp_path)
    return temp_path


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

    raw_url = resolve_file_url(parquet_file, verbose)

    # Both halves of what the input has to say about itself, at every version --
    # see fix_compression above.
    original_metadata, _ = get_parquet_metadata(parquet_file, verbose)

    # Read and rewrite with optimal row groups
    _rewrite_through_staging(
        parquet_file,
        output_file,
        f"SELECT * FROM {sql_path(raw_url)}",
        verbose=verbose,
        profile=profile,
        geoparquet_version=geoparquet_version,
        original_metadata=original_metadata,
    )

    return {"fix_applied": "Optimized row groups", "success": True}


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

    # Generate temp file path without creating the file (fixes issue #278)
    # Use mkstemp to get unique name, then close and delete to avoid collision
    fd, temp_file = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    os.unlink(temp_file)
    temp_files.append(temp_file)

    if verbose:
        progress("\n[3/4] Applying Hilbert spatial ordering...")
        progress("(This operation may take several minutes on large files)")

    fix_spatial_ordering(current_file, temp_file, verbose, profile)
    return temp_file, ["Applied Hilbert spatial ordering"]


def _place_result(result_file: str, output_file: str, parquet_file: str) -> None:
    """Leave the finished file at *output_file* without consuming the user's input.

    Two kinds of path arrive here and they are not interchangeable:

    * A scratch file this module produced. Nothing else refers to it, so it is
      **moved**. It comes from :mod:`tempfile` with no ``dir=`` -- the system
      temp directory, routinely a different filesystem from the user's data --
      so ``os.replace`` would raise ``EXDEV`` here and ``shutil.move``'s
      degradation to copy+unlink is the right behaviour rather than the hazard
      it is over a path someone owns. It lands through ``_staged_output`` all
      the same, so an existing destination is replaced atomically.
    * ``parquet_file`` itself, when every earlier step declined to rewrite
      anything. That has to be a **copy**. ``--fix-output`` names a path
      ``handle_fix_common`` does not back up -- correctly, since that path is
      not the file being written -- so moving the input away leaves the user
      with neither their file nor a ``.bak`` (#1036).
    """
    if is_same_file_path(result_file, output_file):
        return

    with _staged_output(output_file) as destination:
        if is_same_file_path(result_file, parquet_file):
            shutil.copy2(result_file, destination)
        else:
            shutil.move(result_file, destination)


def _apply_compression_fix(
    check_results, parquet_file, current_file, output_file, gp_version, verbose, profile
):
    """Handle compression and row group optimization.

    Returns:
        list: fixes_applied
    """
    compression_result = check_results.get("compression", {})
    row_groups_result = check_results.get("row_groups", {})

    needs_compression = compression_result.get("fix_available", False)
    needs_row_groups = row_groups_result.get("fix_available", False)

    if not needs_compression and not needs_row_groups:
        # No compression/row group fixes needed; whatever the earlier steps
        # produced is the answer, so put it at the output path.
        if verbose and not is_same_file_path(current_file, output_file):
            debug("\nMoving to final output location...")
        _place_result(current_file, output_file, parquet_file)
        return []

    if verbose:
        progress("\n[4/4] Optimizing compression and row groups...")

    fix_compression(current_file, output_file, verbose, profile, gp_version)

    fixes = []
    if needs_compression:
        fixes.append("Optimized compression (ZSTD)")
    if needs_row_groups:
        fixes.append(f"Optimized row groups ({DEFAULT_ROW_GROUP_ROWS:,} rows/group)")
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
            check_results,
            parquet_file,
            current_file,
            output_file,
            geoparquet_version,
            verbose,
            profile,
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
        raise GeoParquetError(f"Failed to apply fixes: {str(e)}") from e
