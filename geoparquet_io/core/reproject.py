"""Core reprojection logic for GeoParquet files."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from geoparquet_io.core.bbox_structure import bbox_shaped_struct_columns, check_bbox_structure
from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.compression import resolve_codec
from geoparquet_io.core.crs_utils import (
    apply_target_crs_to_geo_meta,
    entry_crs_is_unknown,
    extract_crs_from_parquet,
    extract_crs_from_table,
    geoparquet_crs_is_null,
    is_geographic_crs,
    lonlat_source_name,
    parse_crs_string_to_projjson,
    table_crs_is_unknown,
    transform_source_crs,
    transform_source_label,
    unknown_crs_error,
    unknown_crs_reason,
)
from geoparquet_io.core.duckdb_metadata import get_schema_info
from geoparquet_io.core.duckdb_utils import (
    as_wkb_sql,
    fetch_one,
    get_duckdb_connection,
    quote_identifier,
    register_arrow,
    sql_literal,
    sql_path,
    uses_aws_profile,
)
from geoparquet_io.core.edges import edges_of, is_geodesic, strip_edges
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geo_metadata import (
    _bbox_claimed_by_another_column,
    sanitized_carried_geo,
    sanitized_carried_geo_of_file,
    strip_derived_stats,
)
from geoparquet_io.core.geometry_detection import (
    create_geometry_view,
    primary_geometry_column,
    require_geometry_column,
)
from geoparquet_io.core.geometry_repair import warn_planar_math, warn_planar_math_for_file
from geoparquet_io.core.logging_config import debug, info, success, warn
from geoparquet_io.core.parquet_footer import patch_or_rewrite
from geoparquet_io.core.parquet_writer import WriteOptions, resolve_output_geoparquet_version
from geoparquet_io.core.remote import (
    needs_httpfs,
    remote_write_context,
    upload_if_remote,
    validate_profile_for_urls,
)
from geoparquet_io.core.stream_io import write_output
from geoparquet_io.core.streaming import is_stdin, read_stdin_to_temp_file, should_stream_output
from geoparquet_io.core.write_funnels import (
    collect_nonplanar_edges,
    rewrite_footer_kv,
    write_parquet_with_metadata,
)

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass
class ReprojectResult:
    """Result of a reprojection operation."""

    output_path: Path
    source_crs: str
    target_crs: str
    feature_count: int


def _detect_crs_from_table(table: pa.Table, geom_col: str) -> str:
    """Detect the ``ST_Transform`` source CRS of ``geom_col`` in ``table``.

    Reads the ``geo`` block, then the GeoArrow field a native-geo-only table
    carries its CRS on (#1262), and resolves it through
    :func:`transform_source_crs`, which raises for a CRS it cannot use.

    Returns:
        ``"AUTH:CODE"``, canonical PROJJSON, or ``"EPSG:4326"`` when absent
    """
    crs = extract_crs_from_table(table, geom_col)
    # A missing CRS is WGS84 per the GeoParquet spec.
    return transform_source_crs(crs) or lonlat_source_name(crs)


def _table_geo_column_meta(table: pa.Table, geom_col: str) -> dict:
    """Return the geo-metadata dict for ``geom_col``, or ``{}`` if unavailable."""
    columns = sanitized_carried_geo(table.schema.metadata).get("columns", {})
    entry = columns.get(geom_col) if isinstance(columns, dict) else None
    return entry if isinstance(entry, dict) else {}


def _target_crs_is_projected(target_crs: str, con=None) -> bool:
    """True when reprojecting into ``target_crs`` lands in projected space.

    Resolves the CRS string to PROJJSON first: ``is_geographic_crs`` reads the
    CRS *type* from PROJJSON, while its string fallback only recognizes the
    WGS84 spellings and would call a geographic datum like EPSG:4269 projected.
    An unresolvable CRS falls back to that string heuristic.
    """
    return not is_geographic_crs(parse_crs_string_to_projjson(target_crs, con) or target_crs)


def _warn_edges_dropped(col_name: str, edges: str, target_crs: str) -> None:
    warn(
        f"Dropped 'edges: {edges}' on column '{col_name}': the reprojected "
        f"edges are straight lines in {target_crs}. Densify before reprojecting "
        "if great-circle edges must be preserved."
    )


def _edges_columns_to_drop(
    input_file: str | None, target_crs: str, geom_col: str, con=None
) -> set[str]:
    """Columns whose non-planar ``edges`` declaration this reprojection voids (#601).

    Reprojection transforms vertices, not edges: gpio does not densify along
    great circles, so in a projected destination the output's edges *are*
    straight lines and planar (the spec default) is the truthful description.
    A geographic destination is only a datum shift, so the declaration stands.

    Only ``geom_col`` — the one column reproject actually transforms — is ever
    dropped: an untransformed secondary geometry column keeps its geographic
    CRS, so its great-circle declaration still holds. Warns when ``geom_col``
    loses a declaration the input actually made.
    """
    if not _target_crs_is_projected(target_crs, con):
        return set()
    if input_file:
        edges = collect_nonplanar_edges(input_file).get(geom_col)
        if edges:
            _warn_edges_dropped(geom_col, edges, target_crs)
    return {geom_col}


def _drop_nonplanar_edges_from_geo_meta(geo_meta: dict, target_crs: str, geom_col: str) -> None:
    """Strip ``geom_col``'s non-planar ``edges`` from an in-memory geo dict (#601).

    The counterpart of ``_edges_columns_to_drop`` for the write paths that hand
    the input's geo metadata straight through (Arrow tables, streaming). Other
    columns are untransformed and keep their declaration.
    """
    edges = edges_of((geo_meta.get("columns") or {}).get(geom_col))
    if is_geodesic(edges):
        strip_edges(geo_meta, columns=[geom_col])
        _warn_edges_dropped(geom_col, edges, target_crs)


def _retarget_carried_geo(metadata: dict, geom_col: str, target_crs: str, con) -> bool:
    """Rewrite ``metadata[b"geo"]`` for ``target_crs``; False when there is nothing to rewrite.

    The single place the Arrow and streaming paths update the carried block, so
    the same sanitizing applies to both: the block written back out is the
    cleaned one, and ``apply_target_crs_to_geo_meta`` -- which indexes
    ``columns`` -- is never handed a list-, string- or null-shaped block (#887).
    """
    geo_meta = sanitized_carried_geo(metadata)
    if not geo_meta:
        debug("Could not update CRS in geo metadata, leaving as-is")
        return False
    apply_target_crs_to_geo_meta(geo_meta, geom_col, target_crs, con)
    if _target_crs_is_projected(target_crs, con):
        _drop_nonplanar_edges_from_geo_meta(geo_meta, target_crs, geom_col)
    metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
    return True


#: Raised by the Arrow/Python-API path on an unknown input CRS (``crs: null``,
#: an empty value, or a ``srid:0`` field).
NULL_CRS_TABLE_ERROR = (
    "Input table's CRS is unknown (an explicit null CRS, an empty one, or srid:0). "
    "Set source_crs to name the CRS of the coordinates, or pass assume_crs84=True "
    "to treat them as OGC:CRS84 (lon/lat WGS84)."
)


def reproject_table(
    table: pa.Table,
    target_crs: str = "EPSG:4326",
    source_crs: str | None = None,
    geometry_column: str | None = None,
    assume_crs84: bool = False,
) -> pa.Table:
    """
    Reproject an Arrow Table to a different CRS.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table with geometry column
        target_crs: Target CRS (default: EPSG:4326)
        source_crs: Source CRS. If None, detected from table metadata.
        geometry_column: Geometry column name. If None, detected from metadata.
        assume_crs84: If True, treat an unknown/null input CRS as OGC:CRS84
            instead of raising. Useful for fixing files that carry ``crs: null``.

    Returns:
        New table with reprojected geometry

    Raises:
        ValueError: If the input's CRS is unknown (``crs: null``, an empty
            value, or a ``srid:0`` field) and neither ``source_crs`` nor
            ``assume_crs84`` is set. This mirrors the CLI contract so the Python
            API never silently treats unknown data as the default.
    """
    # Detect geometry column
    geom_col = geometry_column or require_geometry_column(
        primary_geometry_column(table), "the table"
    )

    # Resolve source CRS. An unknown CRS is not the default, so mirror the CLI
    # contract: require source_crs or assume_crs84, else raise, rather than
    # silently transforming unknown coordinates as if lon/lat WGS84. One rule
    # with the file path, so a srid:0 field counts too (#1277 review).
    if source_crs:
        effective_source_crs = source_crs
    elif table_crs_is_unknown(table, geom_col):
        if not assume_crs84:
            raise ValueError(NULL_CRS_TABLE_ERROR)
        effective_source_crs = "OGC:CRS84"
    else:
        effective_source_crs = _detect_crs_from_table(table, geom_col)

    # Create connection and register table
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        warn_planar_math("reproject", table, geom_col)
        register_arrow(con, "__input_table", table)

        source_table = create_geometry_view(con, "__input_table", geom_col, "__input_view")

        # Build reprojection query
        # Use ST_AsWKB to convert back to WKB format for GeoParquet compatibility
        # The CRS values come from file metadata (or the caller) and may be
        # free-form strings, so they are escaped like any other SQL literal.
        source_crs_literal = sql_literal(effective_source_crs)
        target_crs_literal = sql_literal(target_crs)
        transformed = f"ST_Transform({quote_identifier(geom_col)}, {source_crs_literal}, {target_crs_literal})"
        query = f"""
            SELECT
                * EXCLUDE ({quote_identifier(geom_col)}),
                {as_wkb_sql(transformed)} AS {quote_identifier(geom_col)}
            FROM {source_table}
        """

        result = con.sql(query).arrow().read_all()

        # Update geo metadata with new CRS
        if table.schema.metadata:
            new_metadata = dict(table.schema.metadata)
            if b"geo" in new_metadata:
                if _retarget_carried_geo(new_metadata, geom_col, target_crs, con):
                    result = result.replace_schema_metadata(new_metadata)
            else:
                result = result.replace_schema_metadata(table.schema.metadata)

        return result
    finally:
        con.close()


def _detect_source_crs(input_path: str, verbose: bool, geom_col: str | None = None) -> str:
    """Detect the source CRS of ``geom_col`` (the primary when ``None``).

    Args:
        input_path: RAW (unescaped) path/URL to the input file —
            ``extract_crs_from_parquet`` escapes its own argument (#718)
        verbose: Whether to print verbose output

    Returns:
        CRS string like "EPSG:4326" or PROJJSON string for ST_Transform
    """
    crs_info = extract_crs_from_parquet(input_path, verbose=verbose, geometry_column=geom_col)

    resolved = transform_source_crs(crs_info)
    if resolved:
        return resolved

    # Default to WGS84 per GeoParquet spec (missing CRS = WGS84)
    if verbose and not crs_info:
        debug("No CRS found in metadata, assuming EPSG:4326 (WGS84)")
    return lonlat_source_name(crs_info)


def _get_bbox_column_name(input_path: str, verbose: bool) -> str | None:
    """Get bbox column name if it exists.

    Args:
        input_path: RAW (unescaped) path/URL to the input file —
            ``check_bbox_structure`` escapes its own argument (#718)
        verbose: Whether to print verbose output

    Returns:
        Bbox column name or None
    """
    bbox_info = check_bbox_structure(input_path, verbose=verbose)
    if bbox_info.get("has_bbox_column"):
        return bbox_info.get("bbox_column_name")
    return None


def _warn_carried_bbox_structs(input_path: str, bbox_col: str | None) -> None:
    """Say which bbox-shaped structs this reprojection will carry through as-is.

    Only the primary geometry's bbox column -- ``bbox_col``, the one
    :func:`_get_bbox_column_name` found -- is recomputed from the reprojected
    coordinates. Any OTHER bbox-shaped struct is copied verbatim, so its
    xmin/ymin/xmax/ymax stay in the source CRS beside geometry in the target
    one. Recomputing it would be a guess that those values bound the geometry,
    which is exactly the inference #738/#1171 removed; the fix is to say so.

    A struct a NON-primary geometry column declares as *its* covering is left
    out: those values describe the secondary geometry, which this reprojection
    does not touch either, so they are not stale.

    Not gated on ``verbose``: a user who does not pass it is the one most likely
    to ship the file. ``input_path`` is RAW -- the metadata readers escape their
    own argument (#718).
    """
    schema_info = get_schema_info(input_path)
    candidates = [name for name in bbox_shaped_struct_columns(schema_info) if name != bbox_col]
    if not candidates:
        return
    geo_meta = sanitized_carried_geo_of_file(input_path)
    for name in candidates:
        if _bbox_claimed_by_another_column(geo_meta, name):
            continue
        warn(
            f"Carrying '{name}' through unchanged: nothing declares it as the primary "
            f"geometry's covering, so its xmin/ymin/xmax/ymax stay in the source CRS. "
            f"To fix it before reprojecting, declare it with 'gpio add bbox-metadata' "
            f"or recompute it with 'gpio add bbox --force'."
        )


def _sanitize_null_crs_file(raw_path: str, verbose: bool = False) -> tuple[str, str | None]:
    """Strip an explicit ``crs: null`` so the file presents as default OGC:CRS84.

    DuckDB's GeoParquet reader rejects ``crs: null`` outright, so for the
    ``assume_crs84`` path the ``geo`` key is rewritten (no coordinate change)
    into a temp copy that is read instead. The copy is a footer patch, every
    page copied verbatim; a footer that cannot be patched falls back to the
    funnel's streaming rewrite (``patch_or_rewrite``), so memory stays bounded
    either way -- the rest of reproject is built for larger-than-RAM files.

    ``raw_path`` must be the *unescaped* path/URL (it is handed to PyArrow, not
    SQL).

    Returns:
        ``(path_to_read, temp_path_or_None)`` — the second element, when set, is a
        temp file the caller must delete.
    """
    if not geoparquet_crs_is_null(raw_path):
        return raw_path, None

    # The footer wins over the Arrow schema's view of it: a `geo` key appended
    # after the writer opened is in the footer only, and the guard above read
    # the footer to decide this file needs the rewrite (#913).
    geo_meta = sanitized_carried_geo_of_file(raw_path)
    for col in geo_meta.get("columns", {}).values():
        if entry_crs_is_unknown(col):
            col.pop("crs", None)

    # mkstemp creates the file atomically with 0600 perms (it holds a full copy
    # of the user's data) and a collision-free name. Only the `geo` key
    # changes, so the copy is a footer patch: every page is the input's.
    fd, temp_path = tempfile.mkstemp(suffix=".parquet", prefix="gpio_assume_crs84_")
    os.close(fd)
    try:
        patch_or_rewrite(
            raw_path,
            {"geo": json.dumps(geo_meta)},
            output_file=temp_path,
            rewrite=rewrite_footer_kv,
            verbose=verbose,
        )
    except BaseException:
        # Self-clean on any failure after creation so we never orphan the copy.
        Path(temp_path).unlink(missing_ok=True)
        raise
    if verbose:
        debug(f"Treating unknown (null) CRS as OGC:CRS84 for {raw_path}")
    return temp_path, temp_path


def _resolve_null_crs_input(
    raw_path: str, assume_crs84: bool, verbose: bool, source_crs: str | None = None
) -> tuple[str, str | None, str | None]:
    """Resolve an unknown input CRS before DuckDB reads the file.

    Unknown is ``crs: null``, an empty ``crs`` or a ``srid:0`` type, decided by
    :func:`unknown_crs_reason`. Reprojecting needs a source CRS, so without
    ``source_crs`` or ``assume_crs84`` it raises, naming both flags. With either,
    a null or empty ``geo`` crs is stripped into a temp copy (DuckDB's reader
    rejects ``crs: null``), and the caller transforms from ``source_crs``, or
    from OGC:CRS84 under ``assume_crs84`` (#1277 review).

    ``raw_path`` must be the *unescaped* path/URL.

    Returns:
        ``(path_to_read, temp_path_or_None, forced_source_or_None)``. The paths
        are raw (unescaped). The third is ``"OGC:CRS84"`` when ``assume_crs84``
        decides the source of an unknown input, else ``None``.
    """
    reason = unknown_crs_reason(raw_path)
    if reason is None:
        return raw_path, None, None
    if source_crs is None and not assume_crs84:
        raise ValueError(unknown_crs_error(reason))
    read_path, temp_path = _sanitize_null_crs_file(raw_path, verbose=verbose)
    return read_path, temp_path, (None if source_crs is not None else "OGC:CRS84")


@uses_aws_profile
def reproject_impl(
    input_parquet: str,
    output_parquet: str | None = None,
    target_crs: str = "EPSG:4326",
    source_crs: str | None = None,
    overwrite: bool = False,
    verbose: bool = False,
    on_progress: Callable[[str], None] | None = None,
    assume_crs84: bool = False,
    write: WriteOptions | None = None,
) -> ReprojectResult:
    """
    Reproject a GeoParquet file to a different CRS using DuckDB.

    Args:
        input_parquet: Path to input GeoParquet file (local or remote URL)
        output_parquet: Path to output file. If None, generates name from input.
        target_crs: Target CRS (default: EPSG:4326)
        source_crs: Override source CRS. If None, detected from metadata.
        assume_crs84: Treat an unknown/null input CRS as OGC:CRS84 (rewrites the
            metadata so DuckDB can read the file; no coordinate change).
        overwrite: If True and output_parquet is None, overwrite input file
        verbose: Whether to print verbose output
        on_progress: Optional callback for progress messages
        write: The write options (codec, level, row groups, profile, version,
            memory limit); ``None`` takes every default

    Returns:
        ReprojectResult with information about the operation

    Raises:
        ValueError: If CRS parsing fails or invalid parameters provided
        FileNotFoundError: If input file doesn't exist
        RuntimeError: If reprojection operation fails
    """

    def log(msg: str) -> None:
        if on_progress:
            on_progress(msg)
        elif verbose:
            info(msg)

    write = write or WriteOptions()
    # Validate profile usage
    validate_profile_for_urls(write.profile, input_parquet, output_parquet)

    # Resolve an unknown CRS before DuckDB reads the file: raise a friendly
    # error without --src-crs or --assume-crs84, or sanitize to a temp copy with
    # one. Pass the raw path (not the SQL-escaped URL) to the metadata helpers.
    read_source, temp_sanitized, forced_source = _resolve_null_crs_input(
        input_parquet, assume_crs84, verbose=verbose, source_crs=source_crs
    )

    # Safe URL for SQL interpolation, computed from the (possibly sanitized) path.
    input_url = resolve_file_url(read_source, verbose=verbose)

    # Create DuckDB connection with spatial extension
    con = get_duckdb_connection(
        load_spatial=True,
        load_httpfs=needs_httpfs(input_parquet),
    )

    try:
        # Detect geometry column
        geom_col = require_geometry_column(
            primary_geometry_column(read_source, verbose=verbose), read_source
        )
        warn_planar_math_for_file("reproject", read_source, geom_col)
        log(f"Geometry column: {geom_col}")

        # An explicit --src-crs wins without reading the file's CRS, so it also
        # works around one that cannot be resolved (#1258).
        if source_crs is not None:
            info(f"Using source CRS: {source_crs}")
            effective_source_crs = source_crs
        else:
            effective_source_crs = forced_source or _detect_source_crs(
                read_source, verbose, geom_col
            )
            log(f"Source CRS: {transform_source_label(effective_source_crs)}")

        log(f"Target CRS: {target_crs}")

        # Get feature count
        count = fetch_one(con.execute(f"SELECT COUNT(*) FROM {sql_path(input_url)}"))[0]
        log(f"Features: {count:,}")

        # Check for existing bbox column to exclude (will be regenerated)
        bbox_col = _get_bbox_column_name(read_source, verbose)
        _warn_carried_bbox_structs(read_source, bbox_col)
        exclude_cols = [geom_col]
        if bbox_col:
            exclude_cols.append(bbox_col)
            if verbose:
                debug(f"Excluding bbox column '{bbox_col}' (will be regenerated)")
        # Quote column names to handle special characters (colons, spaces, etc.)
        exclude_clause = ", ".join(quote_identifier(c) for c in exclude_cols)

        # Build SQL query with ST_Transform
        # geometry_always_xy is set at connection level (DuckDB 1.5+)
        log("Reprojecting...")

        # The detected source CRS may be a free-form string a file carried in
        # its metadata or Parquet geo type (#866), so both CRS values are
        # escaped like any other SQL literal.
        source_crs_literal = sql_literal(effective_source_crs)
        target_crs_literal = sql_literal(target_crs)

        # Build bbox regeneration clause if input had bbox column
        if bbox_col:
            # Use CTE to avoid computing ST_Transform multiple times
            query = f"""
                WITH reprojected AS (
                    SELECT
                        * EXCLUDE ({exclude_clause}),
                        ST_Transform(
                            {quote_identifier(geom_col)},
                            {source_crs_literal},
                            {target_crs_literal}
                        ) AS {quote_identifier(geom_col)}
                    FROM {sql_path(input_url)}
                )
                SELECT
                    *,
                    STRUCT_PACK(
                        xmin := ST_XMin({quote_identifier(geom_col)}),
                        ymin := ST_YMin({quote_identifier(geom_col)}),
                        xmax := ST_XMax({quote_identifier(geom_col)}),
                        ymax := ST_YMax({quote_identifier(geom_col)})
                    ) AS {quote_identifier(bbox_col)}
                FROM reprojected
            """
            if verbose:
                debug(f"Regenerating bbox column '{bbox_col}' with reprojected coordinates")
        else:
            query = f"""
                SELECT
                    * EXCLUDE ({exclude_clause}),
                    ST_Transform(
                        {quote_identifier(geom_col)},
                        {source_crs_literal},
                        {target_crs_literal}
                    ) AS {quote_identifier(geom_col)}
                FROM {sql_path(input_url)}
            """

        # Determine output path
        if output_parquet:
            out_path = Path(output_parquet).resolve()
        elif overwrite:
            out_path = Path(input_parquet).resolve()
        else:
            # Generate output name: input_epsg_4326.parquet
            input_path = Path(input_parquet)
            target_suffix = target_crs.replace(":", "_").lower()
            out_path = input_path.parent / f"{input_path.stem}_{target_suffix}.parquet"

        log(f"Output: {out_path}")

        # Validate compression settings
        codec = resolve_codec(
            write.compression, write.compression_level, context="write", verbose=verbose
        )
        write = replace(write, compression=codec.name, compression_level=codec.level)

        # Get target CRS as PROJJSON for metadata
        target_crs_projjson = parse_crs_string_to_projjson(target_crs, con)

        # Read original metadata to preserve non-geo KV metadata (e.g., vecorel)
        original_metadata, _ = get_parquet_metadata(input_parquet, verbose)

        # A projected destination invalidates the transformed column's
        # non-planar `edges` declaration: the reprojected edges are straight
        # lines there (#601). Untransformed columns keep theirs.
        drop_edges_columns = _edges_columns_to_drop(read_source, target_crs, geom_col, con)

        # Auto version mode (todo 040): the funnel below decides it from
        # `input_file=read_source`, through the facade, like every other write.

        # Handle in-place overwrite
        is_overwrite = str(out_path) == str(Path(input_parquet).resolve())

        if is_overwrite:
            # Write to temp file first, then replace
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_path = Path(tmp.name)

            try:
                write_parquet_with_metadata(
                    con,
                    query,
                    str(tmp_path),
                    original_metadata=original_metadata,
                    verbose=verbose,
                    input_crs=target_crs_projjson,
                    **write.funnel_kwargs(),
                    input_file=read_source,
                    invalidate_derived_stats=True,
                    invalidate_derived_stats_columns={geom_col},
                    drop_nonplanar_edges_columns=drop_edges_columns,
                )
                # Replace original with temp file
                shutil.move(str(tmp_path), str(out_path))
            except Exception:
                if tmp_path.exists():
                    tmp_path.unlink()
                raise
        else:
            # Write directly to output
            with remote_write_context(str(out_path), verbose=verbose) as (
                actual_output,
                is_remote,
            ):
                write_parquet_with_metadata(
                    con,
                    query,
                    actual_output,
                    original_metadata=original_metadata,
                    verbose=verbose,
                    input_crs=target_crs_projjson,
                    **write.funnel_kwargs(),
                    input_file=read_source,
                    invalidate_derived_stats=True,
                    invalidate_derived_stats_columns={geom_col},
                    drop_nonplanar_edges_columns=drop_edges_columns,
                )

                if is_remote:
                    upload_if_remote(
                        actual_output,
                        str(out_path),
                        profile=write.profile,
                        is_directory=False,
                        verbose=verbose,
                    )

        if verbose:
            success(
                f"Reprojected {count:,} features from "
                f"{transform_source_label(effective_source_crs)} to {target_crs}"
            )

        return ReprojectResult(
            output_path=out_path,
            source_crs=effective_source_crs,
            target_crs=target_crs,
            feature_count=count,
        )

    finally:
        con.close()
        # unlink(missing_ok) avoids a TOCTOU and can't mask the real exception.
        if temp_sanitized:
            Path(temp_sanitized).unlink(missing_ok=True)


def _reproject_streaming(
    input_path: str,
    output_path: str | None,
    target_crs: str,
    source_crs: str | None,
    verbose: bool,
    write: WriteOptions,
    assume_crs84: bool = False,
) -> None:
    """Handle streaming input/output for reproject."""
    # Suppress verbose when streaming to stdout
    if should_stream_output(output_path):
        verbose = False

    temp_input_file = None
    temp_sanitized = None

    try:
        # If reading from stdin, write to temp file first. The shared bridge
        # reconciles the stream's geo metadata with its schema on the way, so
        # the temp file is one DuckDB will actually open (#722).
        if is_stdin(input_path):
            temp_input_file = read_stdin_to_temp_file(verbose)
            working_file = temp_input_file
        else:
            working_file = input_path

        # Resolve an unknown CRS (raise without a flag, sanitize with one),
        # passing the raw path, not the SQL-escaped URL, to the PyArrow helpers.
        working_file, temp_sanitized, forced_source = _resolve_null_crs_input(
            working_file, assume_crs84, verbose=verbose, source_crs=source_crs
        )

        # Get safe URL for SQL interpolation
        working_url = resolve_file_url(working_file, verbose=False)

        # Create connection
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(working_file))

        try:
            # Detect geometry column
            geom_col = require_geometry_column(
                primary_geometry_column(working_file, verbose=False), working_file
            )
            warn_planar_math_for_file("reproject", working_file, geom_col)

            # Detect source CRS
            effective_source_crs = (
                source_crs
                or forced_source
                or _detect_source_crs(working_file, verbose=False, geom_col=geom_col)
            )

            # Check for existing bbox column to exclude (will be regenerated)
            bbox_col = _get_bbox_column_name(working_file, verbose=False)
            _warn_carried_bbox_structs(working_file, bbox_col)
            exclude_cols = [geom_col]
            if bbox_col:
                exclude_cols.append(bbox_col)
            # Quote column names to handle special characters (colons, spaces, etc.)
            exclude_clause = ", ".join(quote_identifier(c) for c in exclude_cols)

            # The detected source CRS may be a free-form string a file carried
            # in its metadata or Parquet geo type (#866), so both CRS values
            # are escaped like any other SQL literal.
            source_crs_literal = sql_literal(effective_source_crs)
            target_crs_literal = sql_literal(target_crs)

            # Build reprojection query with bbox regeneration if input had bbox
            if bbox_col:
                # Use CTE to compute reprojected geometry once, then regenerate bbox
                query = f"""
                    WITH reprojected AS (
                        SELECT
                            * EXCLUDE ({exclude_clause}),
                            ST_Transform(
                                {quote_identifier(geom_col)},
                                {source_crs_literal},
                                {target_crs_literal}
                            ) AS {quote_identifier(geom_col)}
                        FROM {sql_path(working_url)}
                    )
                    SELECT
                        *,
                        STRUCT_PACK(
                            xmin := ST_XMin({quote_identifier(geom_col)}),
                            ymin := ST_YMin({quote_identifier(geom_col)}),
                            xmax := ST_XMax({quote_identifier(geom_col)}),
                            ymax := ST_YMax({quote_identifier(geom_col)})
                        ) AS {quote_identifier(bbox_col)}
                    FROM reprojected
                """
            else:
                query = f"""
                    SELECT
                        * EXCLUDE ({exclude_clause}),
                        ST_Transform(
                            {quote_identifier(geom_col)},
                            {source_crs_literal},
                            {target_crs_literal}
                        ) AS {quote_identifier(geom_col)}
                    FROM {sql_path(working_url)}
                """

            # Get original metadata for preservation
            from geoparquet_io.core.common import get_parquet_metadata

            metadata, _ = get_parquet_metadata(working_file, verbose=False)

            # Auto version mode: same contract as the file-based path (todo 040).
            # The stream funnel only sees the metadata, so the file is asked here.
            write = replace(
                write,
                geoparquet_version=resolve_output_geoparquet_version(
                    write.geoparquet_version, input_file=working_file, original_metadata=metadata
                ),
            )

            # Update metadata with target CRS
            if metadata and b"geo" in metadata:
                _retarget_carried_geo(metadata, geom_col, target_crs, con)

            # Reprojection moves coordinates, so the carried bbox (and, for a
            # geometry-repairing transform, geometry_types) no longer describes
            # the output; drop them so they are recomputed from the written data.
            # Only the transformed column, though: a secondary geometry column
            # is passed through untouched, and its stats must survive (#890).
            metadata = strip_derived_stats(metadata, columns={geom_col})

            # Write output using stream_io
            write_output(
                con,
                query,
                output_path,
                original_metadata=metadata,
                verbose=verbose,
                **write.funnel_kwargs(),
            )

            if not should_stream_output(output_path):
                success(
                    f"Reprojected from {transform_source_label(effective_source_crs)} "
                    f"to {target_crs}: {output_path}"
                )

        finally:
            con.close()

    finally:
        # Clean up temp input file(s). unlink(missing_ok) avoids masking errors.
        if temp_input_file:
            Path(temp_input_file).unlink(missing_ok=True)
        if temp_sanitized:
            Path(temp_sanitized).unlink(missing_ok=True)


def reproject(
    input_parquet: str,
    output_parquet: str | None = None,
    target_crs: str = "EPSG:4326",
    source_crs: str | None = None,
    overwrite: bool = False,
    compression: str | None = None,
    compression_level: int | None = None,
    verbose: bool = False,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    on_progress: Callable[[str], None] | None = None,
    row_group_size_mb: int | None = None,
    row_group_rows: int | None = None,
    memory_limit: str | None = None,
    assume_crs84: bool = False,
) -> ReprojectResult | None:
    """
    Reproject a GeoParquet file to a different CRS.

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    Args:
        input_parquet: Path to input file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
        target_crs: Target CRS (default: EPSG:4326)
        source_crs: Override source CRS. If None, detected from metadata.
        overwrite: If True and output_parquet is None, overwrite input file
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        verbose: Whether to print verbose output
        profile: AWS profile name for S3 operations
        geoparquet_version: GeoParquet version to write
        on_progress: Optional callback for progress messages
        row_group_size_mb: Row group size in MB (mutually exclusive with row_group_rows)
        row_group_rows: Row group size in number of rows (mutually exclusive with row_group_size_mb)
        memory_limit: DuckDB memory limit for write operations (e.g., "2GB")

    Returns:
        ReprojectResult with information, or None for streaming output
    """
    # Check for streaming mode
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    write = WriteOptions.pick(locals())
    if is_streaming:
        _reproject_streaming(
            input_parquet,
            output_parquet,
            target_crs,
            source_crs,
            verbose,
            write,
            assume_crs84=assume_crs84,
        )
        return None

    # Use the original implementation for file-based mode
    return reproject_impl(
        input_parquet,
        output_parquet,
        target_crs,
        source_crs,
        overwrite,
        verbose,
        on_progress,
        assume_crs84=assume_crs84,
        write=write,
    )
