#!/usr/bin/env python3

import functools
import os
import shutil
import tempfile

from geoparquet_io.core.add.bbox import add_bbox_column
from geoparquet_io.core.add.bbox_metadata import add_bbox_metadata
from geoparquet_io.core.bbox_structure import bbox_covering_column_for
from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.duckdb_metadata import get_file_metadata
from geoparquet_io.core.duckdb_utils import (
    file_relation,
    file_schema,
    quote_identifier,
    uses_aws_profile,
)
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.file_utils import is_same_file_path, resolve_file_url
from geoparquet_io.core.geo_metadata import (
    GeoBlockOptions,
    SchemaSource,
    bboxless_covering_columns,
    compose_geo_block,
    covering_lacks_bbox,
    geo_key_value,
    sanitized_carried_geo,
)
from geoparquet_io.core.geometry_detection import primary_geometry_column, require_geometry_column
from geoparquet_io.core.hilbert_order import hilbert_order
from geoparquet_io.core.logging_config import debug, info, progress
from geoparquet_io.core.parquet_footer import patch_or_rewrite
from geoparquet_io.core.parquet_writer import (
    DEFAULT_ROW_GROUP_ROWS,
    WriteOptions,
    resolve_output_geoparquet_version,
    resolve_row_group_rows_for_rewrite,
)
from geoparquet_io.core.write_funnels import (
    rewrite_file_through_funnel,
    rewrite_footer_kv,
    staged_replacement,
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
#   block is what `_fast_path_geo_decision` substitutes for DuckDB's,
#   and these two rewrites keep every row of their input, so it still describes
#   the output exactly.
#
# The witness must be the file whose *rows* the write reads. Under
# `check all --fix` that is the previous step's scratch file rather than the
# user's input: the fixes chain, and naming the original while reading a
# rewrite is how a wrong CRS comes to be asserted rather than omitted (see
# `resolve_output_geoparquet_version`).


@uses_aws_profile
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

    raw_url = resolve_file_url(parquet_file, verbose)

    # Both halves of what the input has to say about itself, at every version.
    # See the module docstring: the `geo` block carries a 2.0 input's `covering`
    # (#1003) and the file is the witness the facade resolves version and CRS
    # from (#1001).
    original_metadata, _ = get_parquet_metadata(parquet_file, verbose)

    # Preserve row order from input file (important after Hilbert sorting)
    rewrite_file_through_funnel(
        parquet_file,
        output_file,
        f"SELECT * FROM {file_relation(raw_url)}",
        verbose=verbose,
        profile=profile,
        geoparquet_version=geoparquet_version,
        original_metadata=original_metadata,
        row_group_rows=DEFAULT_ROW_GROUP_ROWS,
    )

    return {"fix_applied": "Re-compressed with ZSTD", "success": True}


def _repaired_cover_block(parquet_file, original_metadata, geo_meta, verbose):
    """The block a bbox-less covering repair writes, and the column it declared.

    Composed, not edited: ``compose_geo_block`` over the file's schema and its
    own block, at the version the repair policy keeps (``preserve_input``), with the default
    covering request -- so a declarable bbox column gets the missing member
    and an h3/s2/a5/quadkey/kdtree entry beside it is kept, and every other
    bbox-less covering goes to the composer's gate. The footer patch and its
    fallback get this one block (``patch_or_rewrite``), so they cannot answer
    differently for one file. A remote file is asked the same question through
    DuckDB's schema probe (``duckdb_utils.file_schema``); the primary is the
    resolver's (``geometry_detection.primary_geometry_column``).
    """
    schema = file_schema(resolve_file_url(parquet_file))
    primary = require_geometry_column(
        primary_geometry_column(schema, geo=geo_meta, verbose=verbose), parquet_file
    )
    block = compose_geo_block(
        SchemaSource(schema, original_metadata),
        GeoBlockOptions(
            geometry_column=primary,
            version=resolve_output_geoparquet_version(
                None, original_metadata=original_metadata, preserve_input=True
            ),
            verbose=verbose,
        ),
    )
    lacked = covering_lacks_bbox((geo_meta.get("columns") or {}).get(primary))
    return block, (bbox_covering_column_for(block, primary) if lacked else None)


@uses_aws_profile
def fix_bboxless_cover(parquet_file, output_file, verbose=False, profile=None):
    """Repair a ``covering`` that carries no ``bbox`` member.

    The repair for the #954 shape in a file already published: a ``covering``
    whose only members are gpio's own index entries (h3/s2/a5/quadkey/kdtree).
    The spec defines no encoding but ``bbox``, and geopandas indexes
    ``covering["bbox"]`` unguarded, so such a file does not open at all -- which
    is why ``check spec`` fails it (``_check_cover_has_bbox``) and why there
    has to be something for ``--fix`` to do about it (#1173).

    Dropping is the repair for a file with nothing to declare. A file whose
    ``bbox`` struct is right there -- conventionally named, in the spec's field
    order -- gets the member *supplied* instead, by
    :func:`_repaired_cover_block`: the covering becomes usable and the
    index entry beside it survives. That is not this function's own policy but
    the write funnels': they do not simply drop, they run
    the composer's bbox-covering step *before* ``strip_bboxless_covering``, so a
    declarable column gets its chance first. Both arms below now ask that one
    gate (``bbox_column_to_declare``), which is what keeps them in step -- the
    footer patch used to hand down the stripped block while the fallback handed
    the funnel the original, and the same bytes came out with ``covering: None``
    one way and ``{quadkey, bbox}`` the other.

    Metadata only: the `geo` key is rewritten in the footer and every page below
    it is copied verbatim (#1141), so the compression codec *and level*, the
    encodings, the bloom filters, the page index, the row-group boundaries and
    the row order all survive a repair that was never about the data. A footer
    that cannot be patched -- or a remote file, which the patcher does not read
    -- falls back to the funnel's rewrite (``parquet_footer.patch_or_rewrite``
    decides; ``write_funnels.rewrite_footer_kv`` re-encodes), with the same
    repaired block either way.

    Recorded decision, not done here: in the NO-bbox-column shape at 1.x the
    ``check all --fix`` chain still drops the index entries in this step,
    *before* ``_apply_bbox_column_fix`` adds the column that would have made
    them declarable. ``add_bbox_column`` already carries other covering entries
    through, so making that shape lossless is a chain-ordering change, and it
    is deliberately left to a follow-up.

    Args:
        parquet_file: Path to input file
        output_file: Path to output file
        verbose: Print additional information
        profile: AWS profile name for S3 operations

    Returns:
        dict with fix summary; ``fix_applied`` is None when the file carries no
        such covering, and nothing is then written -- a legal covering is never
        touched, and the caller keeps using its input.
    """
    original_metadata, _ = get_parquet_metadata(parquet_file, verbose)
    geo_meta = sanitized_carried_geo(original_metadata, source=parquet_file)
    columns = bboxless_covering_columns(geo_meta)
    if geo_meta is None or not columns:
        if verbose:
            debug("No covering without a bbox member; nothing to drop.")
        return {"fix_applied": None, "success": True}

    repaired, declared = _repaired_cover_block(parquet_file, original_metadata, geo_meta, verbose)
    if declared:
        info(
            f"Declaring bbox column '{declared}' in the covering that had none "
            "(the entries beside it are kept)"
        )
        fix_applied = f"Added the missing bbox member to covering (column '{declared}')"
    else:
        info(
            "Dropping 'covering' with no bbox member from "
            + ", ".join(f"column '{column}'" for column in columns)
        )
        fix_applied = "Dropped covering with no bbox member"

    patch_or_rewrite(
        parquet_file,
        {"geo": geo_key_value(repaired)},
        output_file=output_file,
        rewrite=functools.partial(
            rewrite_footer_kv, write=WriteOptions(profile=profile), verbose=verbose
        ),
        verbose=verbose,
    )

    return {"fix_applied": fix_applied, "success": True}


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
        compression=None,
        compression_level=None,
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

    # The input is only read: the rewrite goes straight to output_file in one
    # pass (staged beside it when it already exists), so the caller still has
    # its file afterwards and nothing is copied only to be rewritten.
    add_bbox_metadata(parquet_file, verbose=verbose, output_file=output_file)

    return {"fix_applied": "Added bbox covering metadata", "success": True}


@uses_aws_profile
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

    raw_url = resolve_file_url(parquet_file, verbose)

    # A repair keeps the file's own version (2.0, or no geo key for a
    # native-geo-only file): the facade's preserve policy.
    gp_version = resolve_output_geoparquet_version(
        None, input_file=parquet_file, preserve_input=True, verbose=verbose
    )

    # Select all columns EXCEPT the bbox column. The column name is read from
    # the file's own schema (see ``_detect_bbox_column_from_table``), so it is
    # attacker-controlled and must be quoted as an identifier -- a bare
    # interpolation lets a crafted column name inject arbitrary SQL into the
    # projection (#918).
    #
    # The input's footer keys go to the funnel like every other rewrite's, and
    # the funnel decides which survive: every sidecar key and `epoch` do, and
    # the covering over the column this drops does not -- it is pruned with the
    # column (`_prune_metadata_to_output_columns`). Passing nothing here used to
    # drop every key but `geo` (#1015). The CRS also survives from `input_file`
    # -- `gp_version` above answers the version question from the same file, and
    # a native-geo-only input keeps its CRS only in the Parquet logical type
    # (#1001).
    original_metadata, _ = get_parquet_metadata(parquet_file, verbose)
    rewrite_file_through_funnel(
        parquet_file,
        output_file,
        f"SELECT * EXCLUDE ({quote_identifier(bbox_column_name)}) FROM {file_relation(raw_url)}",
        verbose=verbose,
        profile=profile,
        geoparquet_version=gp_version,
        original_metadata=original_metadata,
        row_group_rows=DEFAULT_ROW_GROUP_ROWS,
    )

    return {"fix_applied": f"Removed bbox column '{bbox_column_name}'", "success": True}


def fix_bbox_all(
    parquet_file, output_file, needs_column, needs_metadata, verbose=False, profile=None
):
    """Fix both bbox column and metadata issues.

    The input is only ever read. The result is staged beside ``output_file``
    and swapped in by ``os.replace``, like every other fix here (#1032); the
    covering-only branch used to ``shutil.move`` the user's own file to
    ``output_file``, which is how ``--fix --fix-output`` deleted inputs (#1036).

    Args:
        parquet_file: Path to input file
        output_file: Path to output file
        needs_column: Whether to add bbox column
        needs_metadata: Whether to add bbox metadata
        verbose: Print additional information
        profile: AWS profile name for S3 operations

    Returns:
        dict with fix summary; ``fix_applied`` is None when there was nothing to do
    """
    if not needs_column and not needs_metadata:
        return {"fix_applied": None, "success": True}

    with staged_replacement(output_file) as destination:
        if needs_column:
            # add_bbox_column writes the covering with the column, so a
            # separate metadata pass here would only re-read what it just wrote.
            fix_bbox_column(parquet_file, destination, verbose, profile)
        else:
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
    # A sort for spatial order must leave as many row groups as it found, or a
    # file the spatial check could judge comes back as one it cannot (#1098).
    file_meta = get_file_metadata(parquet_file)
    row_group_rows = resolve_row_group_rows_for_rewrite(
        file_meta.get("num_rows", 0), file_meta.get("num_row_groups", 0)
    )
    with staged_replacement(output_file) as destination:
        hilbert_order(
            input_parquet=parquet_file,
            output_parquet=destination,
            add_bbox_flag=False,  # bbox should already be added if needed
            verbose=verbose,
            compression=None,
            compression_level=None,
            row_group_rows=row_group_rows,
            profile=profile,
            overwrite=True,  # check --fix manages file lifecycle
        )

    return {"fix_applied": "Applied Hilbert spatial ordering", "success": True}


@uses_aws_profile
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

    raw_url = resolve_file_url(parquet_file, verbose)

    # Both halves of what the input has to say about itself, at every version --
    # see fix_compression above.
    original_metadata, _ = get_parquet_metadata(parquet_file, verbose)

    # Read and rewrite with optimal row groups
    rewrite_file_through_funnel(
        parquet_file,
        output_file,
        f"SELECT * FROM {file_relation(raw_url)}",
        verbose=verbose,
        profile=profile,
        geoparquet_version=geoparquet_version,
        original_metadata=original_metadata,
        row_group_rows=DEFAULT_ROW_GROUP_ROWS,
    )

    return {"fix_applied": "Optimized row groups", "success": True}


def _apply_cover_fix(check_results, current_file, temp_files, verbose, profile):
    """Drop a covering with no bbox member, before anything else reads the block.

    First, so the rest of the chain works from a file whose `geo` block is one
    the spec allows -- and so the repair happens even when it is the only thing
    wrong, which is the 2.0 case: a 2.0 file wants no bbox column, so no other
    step here has anything to do and the illegal covering used to survive the
    whole run (#1173).

    Returns:
        tuple: (new_current_file, fixes_applied_list)
    """
    if not (check_results.get("covering") or {}).get("fix_available", False):
        return current_file, []

    fd, temp_file = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    os.unlink(temp_file)
    temp_files.append(temp_file)

    summary = fix_bboxless_cover(current_file, temp_file, verbose, profile)
    if summary["fix_applied"] is None:
        # The check and the repair disagree about this file; keep the input.
        return current_file, []
    return temp_file, [summary["fix_applied"]]


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

    if verbose:
        progress("\n[2/4] Adding bbox covering metadata...")

    if is_same_file_path(current_file, parquet_file):
        # The user's file is only read: the covering goes onto a scratch
        # written in one pass, and current_file leaves the user's path here.
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet").name
        temp_files.append(temp_file)
        fix_bbox_metadata(current_file, temp_file, verbose, profile)
        return temp_file, ["Added bbox covering metadata"]

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


def _place_result(result_file: str, output_file: str, *, owned: bool) -> None:
    """Leave the finished file at *output_file* without consuming the user's input.

    ``owned`` says whether *result_file* is a scratch file this module made
    (moved: nothing else refers to it, and it lives in the system temp
    directory, so ``shutil.move``'s cross-device fallback is wanted) or the
    user's own input, when every earlier step declined to rewrite anything
    (copied: ``--fix-output`` names a path nobody backed up, so moving the
    input away left the user with neither file nor ``.bak``, #1036). Either
    way it lands through ``staged_replacement``, so an existing destination is
    replaced atomically.
    """
    if is_same_file_path(result_file, output_file):
        return

    with staged_replacement(output_file) as destination:
        if owned:
            shutil.move(result_file, destination)
        else:
            shutil.copy2(result_file, destination)


def _apply_compression_fix(
    check_results, current_file, output_file, gp_version, verbose, profile, *, owned=False
):
    """Handle compression and row group optimization.

    ``owned`` is whether *current_file* is a scratch file an earlier step
    produced, as opposed to the user's input; see :func:`_place_result`.

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
        _place_result(current_file, output_file, owned=owned)
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
        if os.path.exists(temp_file) and not is_same_file_path(temp_file, output_file):
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

    # A repair keeps the input's own version (the facade's preserve policy),
    # read off the bbox check, which already detected the file type.
    bbox_result = check_results.get("bbox", {})
    geoparquet_version = resolve_output_geoparquet_version(
        None,
        file_type={
            "file_type": bbox_result.get("file_type"),
            "geo_version": bbox_result.get("version"),
        },
        preserve_input=True,
    )
    if verbose and geoparquet_version:
        debug(f"Preserving GeoParquet version: {geoparquet_version}")

    try:
        # Step 0: Drop a covering the spec does not allow (#1173)
        current_file, fixes = _apply_cover_fix(
            check_results, current_file, temp_files, verbose, profile
        )
        fixes_applied.extend(fixes)

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
            current_file,
            output_file,
            geoparquet_version,
            verbose,
            profile,
            owned=current_file in temp_files,
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

    except BaseException as e:
        _cleanup_temp_files(temp_files, output_file=None)
        if not isinstance(e, Exception):  # Ctrl-C: clean up, then stop as asked
            raise
        raise GeoParquetError(f"Failed to apply fixes: {str(e)}") from e
