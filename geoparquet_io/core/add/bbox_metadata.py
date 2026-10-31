#!/usr/bin/env python3
"""Add bbox covering metadata to GeoParquet files.

This module adds bbox covering metadata to existing GeoParquet files,
enabling spatial filtering optimizations in readers that support it.

The file path patches the footer in place and copies the data pages verbatim
(`core/parquet_footer.py`, #1141). It used to re-encode the whole file with a
DuckDB COPY, which a Parquet file's own metadata cannot make exact: the
compression *level* is not recorded anywhere, so a file written above the
default came back inflated. A footer that cannot be read still falls back to a
rewrite, the funnel's (`parquet_footer.patch_or_rewrite`).

Note: This operation only supports local files. Remote URLs (S3, GCS, Azure)
are not supported for in-place metadata modification.
"""

import functools
from typing import TYPE_CHECKING

from geoparquet_io.core.bbox_structure import check_bbox_structure
from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geo_metadata import (
    GeoBlockOptions,
    SchemaSource,
    _bbox_claimed_by_another_column,
    arrow_bbox_covering_problem,
    compose_geo_block,
    covering_supported,
    declared_version,
    geo_key_value,
    sanitized_carried_geo,
    with_geo_key,
)
from geoparquet_io.core.geometry_detection import (
    primary_geometry_column,
    require_geometry_column,
    resolve_geometry_column,
)
from geoparquet_io.core.logging_config import debug, success
from geoparquet_io.core.parquet_footer import patch_or_rewrite
from geoparquet_io.core.remote import is_remote_url
from geoparquet_io.core.write_funnels import rewrite_footer_kv

if TYPE_CHECKING:
    import pyarrow as pa


def require_geo_metadata_for_cover(geo_meta: object, source: str | None = None) -> dict:
    """Refuse an input that carries no usable GeoParquet metadata (#713).

    A file with no `geo` key is not GeoParquet, and adding covering metadata
    cannot make it one: the covering key is written and nothing else, so the
    synthesised block used to go out with no `encoding` and no `geometry_types`
    -- a file that claims to be GeoParquet 1.1.0 and fails five of its own spec
    checks. Refuse instead, the same way a 1.0 input is refused (#686), and name
    the command that does produce valid metadata.

    A `geo` key holding valid JSON that is not an object (a bare string, a list)
    is treated as absent rather than crashing on the first `.get()`.

    Shared by the file path (`add_bbox_metadata`) and the Python API
    (`Table.add_bbox_metadata`, `ops.add_bbox_metadata`) so both refuse the same
    input with the same error. `source` names the file when there is one; the
    in-memory API has no path, so it falls back to naming the table, exactly as
    the 1.0 gate below says "this file" / "this table".

    Args:
        geo_meta: Parsed `geo` metadata, or None/whatever JSON produced
        source: Path of the file being modified, if any

    Returns:
        dict: The validated geo metadata

    Raises:
        GeoParquetError: If there is no usable `geo` object
    """
    if isinstance(geo_meta, dict) and geo_meta:
        return geo_meta

    subject = source if source else "this table"
    target = source if source else "input.parquet"
    raise GeoParquetError(
        f"Cannot add bbox covering metadata: {subject} has no GeoParquet "
        "metadata, so there is nothing to describe the geometry column "
        "(encoding, geometry types) that the 'covering' key attaches to.\n"
        f"Convert it first: gpio convert geoparquet {target} out.parquet"
    )


def _cannot_declare_message(problem: str, source: str | None) -> str:
    """The refusal: declaring the covering is this command's whole job, so it fails loudly."""
    rewrite = (
        f"Rewrite it in the spec's order: gpio add bbox --force {source} out.parquet"
        if source
        else "Rewrite it in the spec's order: gpio.read(...).add_bbox()"
    )
    return f"Cannot add bbox covering metadata: {problem}. {rewrite}"


def add_bbox_metadata_table(
    table: "pa.Table",
    bbox_column: str = "bbox",
    geometry_column: str | None = None,
) -> "pa.Table":
    """Add bbox covering metadata to an in-memory Arrow table.

    The in-memory counterpart of :func:`add_bbox_metadata`, and the single
    implementation behind `Table.add_bbox_metadata` and `ops.add_bbox_metadata`
    so the Python API refuses exactly what the CLI refuses (#713).

    Args:
        table: Arrow table carrying GeoParquet `geo` schema metadata
        bbox_column: Name of the existing bbox struct column
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        pa.Table: A new table whose `geo` metadata carries the covering key

    Raises:
        GeoParquetError: If the table carries no usable GeoParquet metadata
        ValueError: If the geometry or bbox column is missing, or the declared
            version predates GeoParquet 1.1
    """

    geom_col = resolve_geometry_column(table, geometry_column, where="the table")

    if bbox_column not in table.column_names:
        raise ValueError(f"Bbox column '{bbox_column}' not found. Use add_bbox() first.")

    problem = arrow_bbox_covering_problem(bbox_column, table.schema.field(bbox_column))
    if problem is not None:
        raise ValueError(_cannot_declare_message(problem, None))

    schema_metadata = dict(table.schema.metadata) if table.schema.metadata else {}

    # A write path: this block is re-serialized onto the returned table, so a
    # malformed carried one goes through the shared shape check first (#947).
    # Without it a `columns` mapping to a non-object let the covering be
    # assigned into a string, and a wrong-typed `crs` was carried straight into
    # the output.
    geo_meta: object = sanitized_carried_geo(schema_metadata)

    # Same refusal as the file path: a table with no usable `geo` block has
    # nothing describing the geometry column, and the skeleton this used to
    # invent claimed GeoParquet 1.1.0 while failing five of its own spec checks.
    geo_meta = require_geo_metadata_for_cover(geo_meta)

    # 'covering' was introduced in GeoParquet 1.1. This path exists solely to
    # write that key, so a 1.0 table gets a clear error naming the conflict
    # rather than silently returning a table without the metadata it asked for.
    table_version = declared_version(geo_meta)
    if not covering_supported(table_version):
        raise ValueError(
            f"Cannot add bbox covering metadata: this table declares GeoParquet "
            f"{table_version}, and the 'covering' key requires GeoParquet 1.1 or later. "
            f"Write the table at 1.1 first (e.g. write(..., geoparquet_version='1.1'))."
        )

    geo_meta = compose_geo_block(
        SchemaSource(table.schema, with_geo_key(None, geo_meta)),
        GeoBlockOptions(geometry_column=str(geom_col), covering=bbox_column),
    )

    # Metadata-only change, not a cast
    return table.replace_schema_metadata(with_geo_key(schema_metadata, geo_meta))


def _validate_requested_bbox_column(
    parquet_file: str, bbox_column: str, schema, geo_meta: object
) -> None:
    """Gate an explicit ``--bbox-name``: the escape hatch is from naming, not from #1171.

    Auto-detection vouches for only two columns (the primary's declared covering
    and the one self-evident name ``bbox``), which leaves a GDAL-written
    ``geometry_bbox`` undeclarable. Naming it here is the user asserting that its
    values bound the primary geometry -- the one thing gpio cannot read off a
    schema. Three things are still not theirs to assert:

    * a column the file does not have;
    * a struct the spec forbids a ``covering`` to point at (Overture's
      ``xmin, xmax, ymin, ymax``), because gpio does not write a file its own
      validator rejects (#1035);
    * a struct some NON-primary geometry column already declares as *its*
      covering -- that one is the secondary's envelope, and declaring it on the
      primary is exactly #1171/#953 by hand.
    """
    if bbox_column not in schema.names:
        raise GeoParquetError(
            f"Bbox column '{bbox_column}' not found in {parquet_file}.\n"
            f"Add one: gpio add bbox --bbox-name {bbox_column} {parquet_file}"
        )

    problem = arrow_bbox_covering_problem(bbox_column, schema.field(bbox_column))
    if problem is not None:
        raise GeoParquetError(_cannot_declare_message(problem, parquet_file))

    if _bbox_claimed_by_another_column(geo_meta, bbox_column):
        raise GeoParquetError(
            f"Cannot add bbox covering metadata: another geometry column's 'covering' "
            f"already declares '{bbox_column}', so those values bound that column's "
            f"geometry, not the primary's.\n"
            f"Compute the primary's own bbox column instead: gpio add bbox {parquet_file}"
        )


def add_bbox_metadata(
    parquet_file: str,
    verbose: bool = False,
    *,
    output_file: str | None = None,
    bbox_column: str | None = None,
) -> None:
    """Add bbox covering metadata to a GeoParquet file.

    Updates the GeoParquet metadata to include bbox covering information,
    which enables spatial filtering optimizations in readers that support it.

    The `geo` key is rewritten in the footer and every page below it is copied
    verbatim (#1141), so the data is not decoded and cannot change: compression
    codec *and level*, encodings, bloom filters, the page index, row-group
    boundaries, row order and every other footer key are all kept as they were.
    The work is proportional to the footer rather than to the file.

    A footer that cannot be read -- an encrypted one, or a thrift structure
    this does not recognise -- falls back to the funnel's rewrite
    (``write_funnels.rewrite_footer_kv``), which re-encodes the data at the
    facade's codec and row groups and keeps every sidecar key.

    Note: Only local files are supported. Remote URLs will raise an error.

    Args:
        parquet_file: Path to the parquet file. Modified in place unless
            ``output_file`` is given.
        verbose: Print verbose output
        output_file: Where to write the result instead of over the input. The
            input is then only read, and the rewrite is one pass straight from
            it -- ``check bbox --fix --fix-output`` used to copy the input onto
            the destination first and then rewrite the copy (#1036).
        bbox_column: Declare the covering over this existing struct column
            instead of auto-detecting one. The escape hatch for a column gpio
            will not vouch for by name -- a GDAL ``geometry_bbox``, a
            ``bounds`` -- since naming it is the user asserting that its values
            bound the primary geometry. See
            :func:`_validate_requested_bbox_column` for what naming it still
            cannot assert.

    Raises:
        GeoParquetError: If the file is remote or the operation fails
    """
    output_file = output_file or parquet_file
    # Reject remote URLs - the rewrite below needs a local filesystem
    for path in {parquet_file, output_file}:
        if is_remote_url(path):
            raise GeoParquetError(
                f"Remote URLs are not supported for in-place metadata modification: {path}\n"
                "Download the file locally, modify it, then upload."
            )

    # Refuse a missing input by name before anything reads it. RAW path
    # throughout: the metadata helpers each escape their own argument
    # (pre-escaping here sent ``bm''s.parquet`` on to pyarrow, issue #718).
    resolve_file_url(parquet_file, verbose)

    # The schema is read before the detection gates below, because an explicit
    # ``bbox_column`` is checked against it instead of against detection.
    metadata, schema = get_parquet_metadata(parquet_file)
    # This command rewrites the file from the block, so it is read through the
    # sanitized carried decoder and the malformed parts are dropped the way
    # every other write path drops them (#947). A `columns` entry that is not
    # an object used to fail with `'str' object does not support item
    # assignment` at the assignment below.
    geo_meta = sanitized_carried_geo(metadata, source=parquet_file)

    # Check current bbox structure
    bbox_info = check_bbox_structure(parquet_file, verbose)
    declared_column = bbox_info["bbox_column_name"] if bbox_info["has_bbox_metadata"] else None
    target_column = bbox_column or bbox_info["bbox_column_name"]

    # An existing covering settles it only when it already names the column
    # being asked for: with ``--bbox-name`` pointing somewhere else, the file's
    # own covering is not the answer to the question asked.
    if declared_column is not None and declared_column == target_column:
        success(f"Bbox covering metadata already exists for column '{declared_column}'")
        return

    if bbox_column is not None:
        _validate_requested_bbox_column(parquet_file, bbox_column, schema, geo_meta)
    else:
        # Reporting a failure and then exiting 0 is the same defect as #713, one
        # branch earlier: there is no covering to write without a bbox column, so
        # say so and fail, rather than letting a script read success from $?.
        if not bbox_info["has_bbox_column"]:
            raise GeoParquetError(
                "No valid bbox column found in the file. Please add a bbox column first.\n"
                f"Add one: gpio add bbox {parquet_file}\n"
                "Or declare an existing struct column deliberately: "
                f"gpio add bbox-metadata --bbox-name <column> {parquet_file}"
            )

        if bbox_info.get("covering_problem"):
            raise GeoParquetError(
                _cannot_declare_message(bbox_info["covering_problem"], parquet_file)
            )

    geo_meta = require_geo_metadata_for_cover(geo_meta, parquet_file)

    # 'covering' was introduced in GeoParquet 1.1. Unlike the write paths — where
    # covering is an implicit side effect of writing a bbox column and is simply
    # omitted — this command exists solely to write that key, so a 1.0 file gets a
    # clear error naming the conflict rather than a silent no-op reporting success.
    file_version = declared_version(geo_meta)
    if not covering_supported(file_version):
        raise GeoParquetError(
            f"Cannot add bbox covering metadata: this file declares GeoParquet "
            f"{file_version}, and the 'covering' key requires GeoParquet 1.1 or later.\n"
            f"Convert the file first: gpio convert geoparquet {parquet_file} out.parquet "
            f"--geoparquet-version 1.1"
        )

    # Find primary geometry column
    primary_col = require_geometry_column(
        primary_geometry_column(parquet_file, verbose=verbose), parquet_file
    )

    # The block is re-composed with the covering declared on the primary: the
    # composer creates a missing or non-object ``columns`` entry and runs every
    # covering gate, so the one key this command writes is the owner's answer.
    geo_meta = compose_geo_block(
        SchemaSource(schema, with_geo_key(None, geo_meta)),
        GeoBlockOptions(geometry_column=primary_col, covering=target_column, verbose=verbose),
    )

    if verbose:
        debug("\nUpdated geo metadata:")
        debug(geo_key_value(geo_meta))

    # One footer key does not need the data decoded. Patching the footer copies
    # every page verbatim, so the compression level, the encodings, the bloom
    # filters, the page index, the row-group boundaries and the row order are
    # all kept by construction rather than by re-derivation (#1141). A footer
    # that cannot be read falls back to the funnel's rewrite, which takes the
    # facade's codec and row groups (approved change 3) and keeps every sidecar
    # key; a race is never answered with one (`patch_or_rewrite`).
    patch_or_rewrite(
        parquet_file,
        {"geo": geo_key_value(geo_meta)},
        output_file=output_file,
        rewrite=functools.partial(rewrite_footer_kv, verbose=verbose),
        verbose=verbose,
    )

    success(f"Added bbox covering metadata for column '{target_column}'")
