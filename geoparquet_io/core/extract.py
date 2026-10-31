"""
Extract columns and rows from GeoParquet files.

Supports column selection, spatial filtering (bbox, geometry),
SQL filtering, and multiple input files via glob patterns.

Also supports Arrow IPC streaming for Unix-style piping:
    gpio extract --bbox ... input.parquet | gpio add bbox - output.parquet
"""

from __future__ import annotations

import json
import sys
from collections.abc import Collection
from pathlib import Path

import pyarrow as pa

from geoparquet_io.core import bbox_parse
from geoparquet_io.core.bbox_structure import check_bbox_structure, dataset_bounds
from geoparquet_io.core.column_selection import (
    exclude_columns,
    resolve_columns_against_schema,
    split_column_list,
)
from geoparquet_io.core.common import get_parquet_metadata, should_skip_bbox
from geoparquet_io.core.crs_utils import crs_of, get_crs_display_name, is_projected
from geoparquet_io.core.duckdb_metadata import (
    get_row_count,
)
from geoparquet_io.core.duckdb_utils import (
    _get_query_columns,
    as_wkb_sql,
    describe_query,
    fetch_one,
    get_duckdb_connection,
    get_duckdb_connection_for_s3,
    quote_identifier,
    register_arrow,
    sql_literal,
    sql_path,
    validate_where_clause,
)
from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    GeometryError,
    GeoParquetError,
    InvalidParameterError,
)
from geoparquet_io.core.file_utils import (
    get_all_parquet_files,
    get_first_parquet_file,
    handle_output_overwrite,
    is_partition_path,
    resolve_file_url,
)
from geoparquet_io.core.geo_metadata import (
    backfill_derived_stats,
    prune_geo_metadata_to_columns,
    sanitized_carried_geo,
    strip_derived_stats,
    strip_orientation,
)
from geoparquet_io.core.geometry_detection import (
    create_geometry_view,
    primary_geometry_column,
    resolve_geometry_column,
)
from geoparquet_io.core.geometry_repair import (
    nonplanar_edges_of_arrow_table,
    repair_query_geometry,
)
from geoparquet_io.core.logging_config import debug, info, progress, success, warn
from geoparquet_io.core.parquet_writer import (
    WriteOptions,
    resolve_output_geoparquet_version,
    resolve_row_group_rows_for_query,
)
from geoparquet_io.core.partition.reader import build_read_parquet_expr, require_parquet_files
from geoparquet_io.core.remote import (
    display_path,
    is_remote_url,
    needs_httpfs,
)
from geoparquet_io.core.stream_io import open_input, write_output
from geoparquet_io.core.streaming import (
    is_stdin,
    should_stream_output,
)
from geoparquet_io.core.write_funnels import (
    declared_nonplanar_edges,
    write_parquet_with_metadata,
)


def get_parquet_row_count(parquet_file: str) -> int:
    """Get row count from parquet file metadata using DuckDB (O(1) - reads footer only)."""

    return get_row_count(parquet_file)


def looks_like_latlong_bbox(bbox: tuple[float, float, float, float]) -> bool:
    """Check if bbox values look like lat/long coordinates."""
    xmin, ymin, xmax, ymax = bbox
    # Lat/long: x (lon) is -180 to 180, y (lat) is -90 to 90
    return -180 <= xmin <= 180 and -180 <= xmax <= 180 and -90 <= ymin <= 90 and -90 <= ymax <= 90


def _get_data_bounds(input_parquet: str, geometry_col: str) -> tuple | None:
    """The file's data bounds through the one file entry point (covering first), or None.

    Only the CRS-mismatch warning reads them, so the owner's refusal of an
    infinite coordinate is reported there rather than ending the extract.
    """
    try:
        return dataset_bounds(input_parquet, geometry_col, advise=False)
    except GeometryError as exc:
        warn(f"Data bounds not shown: {exc}")
        return None


def _warn_if_crs_mismatch(
    bbox: tuple[float, float, float, float],
    input_parquet: str,
    geometry_col: str,
) -> None:
    """Warn if bbox looks like lat/long but data is in a projected CRS."""
    if not looks_like_latlong_bbox(bbox):
        return  # User's bbox doesn't look like lat/long, no warning needed

    crs_info = crs_of(input_parquet, geometry_column=geometry_col, quiet=True)
    if is_projected(crs_info):  # Definitely projected
        crs_name = get_crs_display_name(crs_info)
        data_bounds = _get_data_bounds(input_parquet, geometry_col)

        msg = (
            f"\nWarning: Your bbox appears to be in lat/long coordinates, but the data "
            f"uses a projected CRS ({crs_name})."
        )
        if data_bounds:
            msg += (
                f"\nData bounds: xmin={data_bounds[0]:.2f}, ymin={data_bounds[1]:.2f}, "
                f"xmax={data_bounds[2]:.2f}, ymax={data_bounds[3]:.2f}"
            )
        msg += "\nIf you get 0 results, try using coordinates in the data's CRS."

        warn(msg)


def convert_geojson_to_wkt(geojson: dict) -> str:
    """
    Convert GeoJSON geometry to WKT using DuckDB.

    Args:
        geojson: GeoJSON geometry dict

    Returns:
        str: WKT representation
    """
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        geojson_str = sql_literal(json.dumps(geojson))
        result = fetch_one(
            con.execute(f"""
            SELECT ST_AsText(ST_GeomFromGeoJSON({geojson_str}))
        """)
        )
        return result[0]
    finally:
        con.close()


def _read_geometry_from_stdin() -> str:
    """Read geometry from stdin."""
    if sys.stdin.isatty():
        raise InvalidParameterError(
            "geometry", "No geometry provided on stdin. Pipe geometry data or use @file syntax."
        )
    return sys.stdin.read().strip()


def _resolve_geometry_file(geometry_input: str) -> str | None:
    """
    Resolve file path from geometry input.

    Returns file path if input refers to a file, None otherwise.
    """
    if geometry_input.startswith("@"):
        return geometry_input[1:]

    # Check if it looks like a file path (not inline geometry)
    if not geometry_input.strip().startswith(("{", "POLYGON", "POINT", "LINESTRING", "MULTI")):
        potential_path = Path(geometry_input)
        if potential_path.exists() and potential_path.suffix.lower() in (
            ".geojson",
            ".json",
            ".wkt",
        ):
            return geometry_input

    return None


def _extract_geometry_from_geojson(geojson: dict, use_first: bool) -> dict:
    """
    Extract geometry from GeoJSON, handling Feature and FeatureCollection.

    Args:
        geojson: Parsed GeoJSON object
        use_first: If True, use first geometry from FeatureCollection

    Returns:
        dict: The geometry object

    Raises:
        InvalidParameterError: If geometry cannot be extracted
    """
    if geojson.get("type") == "FeatureCollection":
        features = geojson.get("features", [])
        if not features:
            raise InvalidParameterError(
                "geometry", "FeatureCollection is empty - no geometries found"
            )
        if len(features) > 1 and not use_first:
            raise InvalidParameterError(
                "geometry",
                f"Multiple geometries ({len(features)}) found in FeatureCollection. "
                "Use --use-first-geometry to use only the first geometry.",
            )
        geom = features[0].get("geometry")
        if not geom:
            raise InvalidParameterError("geometry", "First feature has no geometry")
        return geom

    if geojson.get("type") == "Feature":
        geom = geojson.get("geometry")
        if not geom:
            raise InvalidParameterError("geometry", "Feature has no geometry")
        return geom

    # Already a geometry object
    return geojson


def _parse_geojson_to_wkt(geometry_input: str, use_first: bool) -> str:
    """Parse GeoJSON string to WKT."""
    try:
        geojson = json.loads(geometry_input)
    except json.JSONDecodeError as e:
        raise InvalidParameterError("geometry", f"Invalid GeoJSON: {e}") from e

    geojson = _extract_geometry_from_geojson(geojson, use_first)

    try:
        return convert_geojson_to_wkt(geojson)
    except Exception as e:
        raise GeoParquetError(f"Failed to convert GeoJSON to WKT: {e}") from e


def _validate_wkt(wkt: str, original_input: str) -> str:
    """Validate WKT string."""
    valid_prefixes = (
        "POINT",
        "LINESTRING",
        "POLYGON",
        "MULTIPOINT",
        "MULTILINESTRING",
        "MULTIPOLYGON",
        "GEOMETRYCOLLECTION",
    )
    if not any(wkt.upper().startswith(prefix) for prefix in valid_prefixes):
        raise InvalidParameterError(
            "geometry",
            f"Could not parse geometry input as GeoJSON or WKT. "
            f"Input: {original_input[:100]}{'...' if len(original_input) > 100 else ''}",
        )
    return wkt


def parse_geometry_input(geometry_input: str, use_first: bool = False) -> str:
    """
    Parse geometry from various input formats.

    Supports:
    - Inline GeoJSON: {"type": "Polygon", ...}
    - Inline WKT: POLYGON((...))
    - File reference: @path/to/file.geojson or @path/to/file.wkt
    - Stdin: - (reads from sys.stdin)
    - Auto-detect file: path/to/file.geojson (if file exists)

    Args:
        geometry_input: Geometry string, file path, or "-" for stdin
        use_first: If True, use first geometry from FeatureCollection

    Returns:
        str: WKT representation of the geometry

    Raises:
        InvalidParameterError: If geometry cannot be parsed or multiple geometries found
    """
    original_input = geometry_input

    # Handle stdin
    if geometry_input == "-":
        geometry_input = _read_geometry_from_stdin()

    # Handle file reference
    file_path = _resolve_geometry_file(geometry_input)
    if file_path:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundGeoParquetError(file_path, "geometry file")
        geometry_input = path.read_text().strip()

    # Parse to WKT
    if geometry_input.strip().startswith("{"):
        return _parse_geojson_to_wkt(geometry_input, use_first)

    return _validate_wkt(geometry_input.strip(), original_input)


def get_schema_columns(input_parquet: str) -> list[str]:
    """
    Get list of column names from parquet file schema.

    Args:
        input_parquet: Path to parquet file (or glob pattern/directory - uses first file)

    Returns:
        list: Column names
    """

    # For partitions, use first file for schema
    file_to_check = input_parquet
    if is_partition_path(input_parquet):
        first_file = get_first_parquet_file(input_parquet)
        if first_file:
            file_to_check = first_file

    # Use auto-detecting S3 connection for S3 paths
    if needs_httpfs(file_to_check):
        con = get_duckdb_connection_for_s3(file_to_check, load_spatial=True)
    else:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        raw_url = resolve_file_url(file_to_check, verbose=False)
        return _get_query_columns(con, f"SELECT * FROM read_parquet({sql_path(raw_url)})")
    finally:
        con.close()


def build_column_selection(
    all_columns: list[str],
    include_cols: list[str] | None,
    exclude_cols: list[str] | None,
    geometry_col: str | None,
    bbox_col: str | None,
    *,
    include_option: str = "columns",
    exclude_option: str = "exclude_cols",
) -> list[str]:
    """
    Build list of columns to select.

    Rules:
    - If include_cols only: select those + geometry + bbox
    - If exclude_cols only: select all except those
    - If both: select include_cols, but exclude_cols can remove geometry/bbox
    - geometry and bbox always included unless in exclude_cols

    Names are resolved against the schema (:mod:`core.column_selection`), so an
    unknown or ambiguous one is reported here by name rather than surfacing later
    as a writer error about a different column (#731).

    Args:
        all_columns: All columns in schema
        include_cols: Columns to include (or None)
        exclude_cols: Columns to exclude (or None)
        geometry_col: Name of geometry column (or None for a table that has none)
        bbox_col: Name of bbox column (or None if not present)
        include_option: Option to name in an error about ``include_cols``
        exclude_option: Option to name in an error about ``exclude_cols``

    Returns:
        list: Columns to select (preserving original order)

    Raises:
        InvalidParameterError: If a name is blank, absent or ambiguous, or if the
            exclusion leaves no columns at all (``exclude_columns``' refusal;
            the empty list used to reach the SQL builders and come back as a raw
            ``ParserException``, #1176)
    """
    include = resolve_columns_against_schema(include_cols, all_columns, include_option)
    if not include:
        if exclude_cols:
            return exclude_columns(all_columns, exclude_cols, option_name=exclude_option)
        return list(all_columns)

    excluded = set(resolve_columns_against_schema(exclude_cols, all_columns, exclude_option) or [])
    selected = set(include)
    # Geometry and bbox ride along unless explicitly excluded
    for implied in (geometry_col, bbox_col):
        if implied and implied not in excluded:
            selected.add(implied)
    return [c for c in all_columns if c in selected]


def build_spatial_filter(
    bbox: tuple[float, float, float, float] | None,
    geometry_wkt: str | None,
    bbox_info: dict,
    geometry_col: str,
) -> str | None:
    """
    Build WHERE clause for spatial filtering.

    Uses bbox column for fast filtering when available (bbox covering),
    then applies precise geometry intersection.
    """
    conditions = []

    if bbox:
        # A wrapped bbox (xmin > xmax) is its two lobes, ORed (bbox_parse.any_piece).
        if bbox_info.get("has_bbox_column"):
            col = quote_identifier(bbox_info["bbox_column_name"])
            conditions.append(
                bbox_parse.any_piece(
                    bbox_parse.BBox(*bbox),
                    lambda b: (
                        f"({col}.xmax >= {b.xmin} AND {col}.xmin <= {b.xmax} "
                        f"AND {col}.ymax >= {b.ymin} AND {col}.ymin <= {b.ymax})"
                    ),
                )
            )
        else:
            geom = quote_identifier(geometry_col)
            conditions.append(
                bbox_parse.any_piece(
                    bbox_parse.BBox(*bbox),
                    lambda b: (
                        f"ST_Intersects({geom}, "
                        f"ST_MakeEnvelope({b.xmin}, {b.ymin}, {b.xmax}, {b.ymax}))"
                    ),
                )
            )

    if geometry_wkt:
        wkt_literal = sql_literal(geometry_wkt)
        conditions.append(
            f"ST_Intersects({quote_identifier(geometry_col)}, ST_GeomFromText({wkt_literal}))"
        )

    return " AND ".join(conditions) if conditions else None


def build_extract_query(
    input_path: str,
    columns: list[str],
    spatial_filter: str | None,
    where_clause: str | None,
    limit: int | None = None,
    allow_schema_diff: bool = False,
    hive_input: bool = False,
) -> str:
    """Build the complete extraction query."""

    col_list = ", ".join(quote_identifier(c) for c in columns)
    # Use partition reader to build read_parquet expression with proper options
    read_expr = build_read_parquet_expr(
        input_path,
        allow_schema_diff=allow_schema_diff,
        hive_input=hive_input,
        verbose=False,
    )
    query = f"SELECT {col_list} FROM {read_expr}"

    conditions = []
    if spatial_filter:
        conditions.append(f"({spatial_filter})")
    if where_clause:
        conditions.append(f"({where_clause})")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    if limit is not None:
        query += f" LIMIT {limit}"

    return query


def _build_query_for_source(
    source_ref: str,
    columns: list[str],
    spatial_filter: str | None,
    where_clause: str | None,
    limit: int | None = None,
) -> str:
    """Build extraction query for a DuckDB source reference (table or read_parquet)."""
    col_list = ", ".join(quote_identifier(c) for c in columns)
    query = f"SELECT {col_list} FROM {source_ref}"

    conditions = []
    if spatial_filter:
        conditions.append(f"({spatial_filter})")
    if where_clause:
        conditions.append(f"({where_clause})")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    if limit is not None:
        query += f" LIMIT {limit}"

    return query


def _get_table_column_info(
    table: pa.Table,
) -> tuple[list[str], str | None, dict]:
    """Get column information from an Arrow table."""
    all_columns = table.column_names
    geometry_col = primary_geometry_column(table)

    # Check for bbox column
    bbox_col = None
    for name in ["bbox", "bounds", "bounding_box"]:
        if name in all_columns:
            bbox_col = name
            break

    bbox_info = {
        "has_bbox_column": bbox_col is not None,
        "bbox_column_name": bbox_col,
    }

    return all_columns, geometry_col, bbox_info


def _build_query_with_wkb_conversion(
    source_ref: str,
    selected_columns: list[str],
    geom_col: str,
    spatial_filter: str | None,
    where: str | None,
    limit: int | None,
) -> str:
    """Build query with WKB conversion for geometry column."""
    cols_with_wkb = [
        f"{as_wkb_sql(quote_identifier(geom_col))} AS {quote_identifier(geom_col)}"
        if c == geom_col
        else quote_identifier(c)
        for c in selected_columns
    ]
    col_list = ", ".join(cols_with_wkb)

    conditions = []
    if spatial_filter:
        conditions.append(f"({spatial_filter})")
    if where:
        conditions.append(f"({where})")

    query = f"SELECT {col_list} FROM {source_ref}"
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    if limit is not None:
        query += f" LIMIT {limit}"
    return query


def extract_table(
    table: pa.Table,
    columns: list[str] | None = None,
    exclude_columns: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    where: str | None = None,
    limit: int | None = None,
    geometry_column: str | None = None,
    repair_geometry: bool = True,
) -> pa.Table:
    """
    Extract columns and rows from an Arrow Table.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table
        columns: Columns to include (None = all)
        exclude_columns: Columns to exclude
        bbox: Bounding box filter (xmin, ymin, xmax, ymax)
        where: SQL WHERE clause
        limit: Maximum rows to return
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        Filtered PyArrow Table

    Raises:
        InvalidParameterError: If a requested column does not exist, if a column
            is in both ``columns`` and ``exclude_columns``, if an explicitly
            named ``geometry_column`` is absent, or if ``bbox`` is requested on a
            table that has no geometry column.
    """
    all_columns, _, bbox_info = _get_table_column_info(table)
    # None for an attribute table; a named column the table lacks raises.
    geom_col = resolve_geometry_column(table, geometry_column, where="the table", required=False)
    bbox_col = bbox_info.get("bbox_column_name")

    # A table with no geometry column is a legitimate input (an attribute table
    # from an earlier exclude_columns=['geometry'], say): project and filter its
    # rows like any other table, and only refuse what actually needs geometry.
    if geom_col is None and bbox is not None:
        raise InvalidParameterError(
            "bbox",
            f"bbox filtering needs a geometry column; this table has none "
            f"(columns: {', '.join(all_columns)})",
        )

    # Validate before selecting, so an unknown name is reported here by name
    # rather than surfacing later as a writer error about a different column
    # (#731). These are the checks --include-cols/--exclude-cols already make.
    _validate_column_overlap(
        columns, exclude_columns, geom_col, bbox_col, param_name="columns/exclude_columns"
    )
    selected_columns = build_column_selection(
        all_columns,
        columns,
        exclude_columns,
        geom_col,
        bbox_col,
        exclude_option="exclude_columns",
    )
    spatial_filter = build_spatial_filter(bbox, None, bbox_info, geom_col) if bbox else None

    if where:
        validate_where_clause(where)

    keeps_geometry = geom_col is not None and geom_col in selected_columns

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        registered = register_arrow(con, "__input_table", table)
        source_ref = (
            create_geometry_view(con, "__input_table", geom_col, "__input_view")
            if geom_col
            else "__input_table"
        )
        needs_wkb = source_ref != "__input_table"

        if needs_wkb and keeps_geometry:
            query = _build_query_with_wkb_conversion(
                source_ref, selected_columns, geom_col, spatial_filter, where, limit
            )
        else:
            query = _build_query_for_source(
                source_ref, selected_columns, spatial_filter, where, limit
            )

        # Repair invalid geometry (issue #506) before materializing the result.
        geometry_repaired = False
        if keeps_geometry:
            query, geometry_repaired = _repair_geometry_in_query(
                con,
                query,
                geom_col,
                repair_geometry,
                nonplanar_edges=nonplanar_edges_of_arrow_table(table, geom_col),
            )

        result = con.sql(query).arrow().read_all()
        if table.schema.metadata:
            metadata = _carry_metadata_to_columns(table.schema.metadata, result.column_names)
            if geometry_repaired:
                # The repair rewrote rows, so the carried stats no longer
                # describe them (#812): drop them and recompute from the result,
                # which is what the file path asks the writer to do. A carried
                # orientation declaration is dropped outright — ST_MakeValid
                # can rewind rings and gpio does not re-orient.
                metadata = backfill_derived_stats(strip_derived_stats(metadata), result)
                metadata = strip_orientation(metadata, geom_col)
            result = result.replace_schema_metadata(metadata)
        # The geometry passes through unless the planar repair rewrote it; a
        # repaired column is planar GEOMETRY now and must not be relabelled (#1253).
        passed_through = [c for c in registered.fields if not (geometry_repaired and c == geom_col)]
        return registered.restore_edges(result, passed_through)
    finally:
        con.close()


def _repair_geometry_in_query(
    con, query: str, geometry_column: str, repair: bool, nonplanar_edges: str | None = None
) -> tuple[str, bool]:
    """Run the geometry repair pass, reporting whether it rewrote the query.

    :func:`repair_query_geometry` returns ``query`` unchanged when repair is off,
    when the column is not repairable in place, when the column declares
    non-planar edges (planar validity is the wrong test there — #1287), and when
    it counted zero invalid rows; it returns a rewritten query only when at
    least one row will actually be repaired. That distinction is what decides
    whether the input's derived stats and orientation survive into the output: a
    pass that changed nothing keeps them, so a file with no invalid geometry is
    never rescanned for nothing.
    """
    repaired_query = repair_query_geometry(
        con, query, geometry_column, repair=repair, nonplanar_edges=nonplanar_edges
    )
    return repaired_query, repaired_query != query


def _carry_metadata_to_columns(metadata: dict, columns: list[str]) -> dict | None:
    """Return the input's KV metadata rewritten to describe only ``columns``.

    A column projection can drop the bbox column a ``covering`` points at, a
    secondary geometry column, or the primary geometry column itself. Carrying
    the input's ``geo`` block over verbatim would stamp the result with metadata
    naming schema roots it no longer has, which readers and ``gpio check spec``
    both reject. The primary is re-pointed at a surviving declared geometry
    column when there is one, and the whole ``geo`` key is dropped when there is
    not — an attribute table is plain Parquet, not GeoParquet.
    """
    return prune_geo_metadata_to_columns(metadata, columns, repoint_primary=True)


#: ``_stale_stat_columns`` answer meaning "the carried stats still describe the
#: output" — a single-file, unfiltered, unrepaired column projection.
_NOTHING_STALE: tuple[str, ...] = ()


def _stale_stat_columns(
    input_path: str,
    spatial_filter: str | None,
    where: str | None,
    limit: int | None,
    geometry_column: str | None,
    geometry_repaired: bool = False,
) -> Collection[str] | None:
    """Which columns' carried bbox/geometry_types cannot describe the output.

    Three independent reasons, which do not all reach the same columns. The
    difference decides what a SECONDARY geometry column's output metadata says:
    no file-write path recomputes one, so whatever survives here is what ships.

    * A row filter (``--bbox``/``--geometry``/``--where``/``--limit``) keeps only
      part of the input, so the carried stats OVER-cover the result. Over-cover
      is safe rather than blessed: the spec asks that ``geometry_types`` be
      "strictly correct" and its worked example is about UNDER-declaring, but a
      list naming a type no row has, and a bbox wider than the rows, cost a
      reader a rejected candidate — never a skipped row. Every reader tolerates
      it and gpio's own ``check spec`` passes it. A secondary column's stats
      therefore still describe it and are left alone; only the primary is named,
      and only so the write path retightens it. Where the exact answer is free
      it is taken instead — see the stdout branch in ``_extract_streaming``.
    * The repair pass (``--repair-geometry``, on by default) rewrote at least one
      row: ``ST_MakeValid`` can change a geometry's type (a bowtie ``Polygon``
      comes back a ``MultiPolygon``) and its extent, so the carried
      ``geometry_types`` UNDER-declares what the output holds and ``gpio check
      spec`` fails the file gpio just wrote (#812). It rewrites the primary
      column and no other, so again only the primary is stale. A repair pass
      that found nothing to fix is not a reason: it left every row as it was.
    * A glob/directory input merges several files, but ``get_parquet_metadata``
      reads the footer of the FIRST file only — carrying its stats would
      UNDER-cover the merged output, which makes conformant readers skip data.
      That holds for every column, so every column is stale.

    Returns ``None`` for "every column" (the ``columns=`` convention of
    :func:`strip_derived_stats`) and :data:`_NOTHING_STALE` for "no column".
    """
    if is_partition_path(input_path):
        return None
    if spatial_filter or where or limit is not None or geometry_repaired:
        # Without a known primary column there is nothing to scope to, so fall
        # back to invalidating everything rather than guessing.
        return {geometry_column} if geometry_column else None
    return _NOTHING_STALE


def _extract_streaming(
    input_path: str,
    output_path: str | None,
    include_cols: list[str] | None,
    exclude_cols: list[str] | None,
    bbox_tuple: tuple[float, float, float, float] | None,
    geometry_wkt: str | None,
    where: str | None,
    limit: int | None,
    verbose: bool,
    write: WriteOptions,
    repair_geometry: bool = True,
) -> None:
    """Handle extraction with streaming input/output."""
    # Suppress verbose when streaming to stdout
    if should_stream_output(output_path):
        verbose = False

    with open_input(
        input_path, verbose=verbose, stream_output=should_stream_output(output_path)
    ) as (source, metadata, is_stream, con):
        # Get column names from query result (works with both table names and read_parquet)
        # One relation: its reader has already de-duplicated the names.
        described = describe_query(con, f"SELECT * FROM {source}")
        all_columns = [name for name, _ in described]

        # Find geometry column
        geom_col = primary_geometry_column(described, geo=sanitized_carried_geo(metadata))

        # Check for bbox column
        bbox_col = None
        for name in ["bbox", "bounds"]:
            if name in all_columns:
                bbox_col = name
                break

        bbox_info = {"has_bbox_column": bbox_col is not None, "bbox_column_name": bbox_col}

        # Build column selection
        selected_columns = build_column_selection(
            all_columns, include_cols, exclude_cols, geom_col, bbox_col
        )

        # Build spatial filter
        spatial_filter = build_spatial_filter(bbox_tuple, geometry_wkt, bbox_info, geom_col)

        # Build query
        query = _build_query_for_source(source, selected_columns, spatial_filter, where, limit)

        # Repair invalid geometry (issue #506) before streaming the write.
        query, geometry_repaired = _repair_geometry_in_query(
            con,
            query,
            geom_col,
            repair_geometry,
            # open_input carries the resolved field model in metadata before
            # DuckDB registration. It is also the only witness on later hops.
            nonplanar_edges=declared_nonplanar_edges(None, metadata).get(geom_col),
        )

        if verbose:
            debug(f"Streaming extraction query: {query}")

        # `write_output` goes two ways: the stdout stream recomputes the stats of
        # EVERY column from the rows it writes (`backfill_derived_stats`, which
        # reads the empty list below as the gap it is), while a file output
        # recomputes the primary column's only. So the primary is stripped
        # outright -- an absent key is what asks for the recompute -- and any
        # other stale column keeps `geometry_types` as the spec's empty "not
        # known" list, since deleting a REQUIRED key nothing puts back is what
        # made the output unreadable (#934).
        stale_columns = _stale_stat_columns(
            input_path, spatial_filter, where, limit, geom_col, geometry_repaired
        )
        if should_stream_output(output_path) and stale_columns:
            # The stdout stream can do better than the file path: it recomputes
            # EVERY column from the rows it writes, so widening the strip past
            # the primary costs nothing and buys the exact answer. Scoping it
            # here as the file path does would ship a secondary column's carried
            # over-cover when the precise stats were free.
            stale_columns = None
        if stale_columns is None or stale_columns:
            metadata = strip_derived_stats(
                metadata, columns=stale_columns, recomputed_columns={geom_col}
            )
        if geometry_repaired:
            # ST_MakeValid can rewind rings, so a carried orientation
            # declaration no longer holds; it is dropped, not recomputed.
            metadata = strip_orientation(metadata, geom_col)

        # Write output
        write_output(
            con,
            query,
            output_path,
            original_metadata=metadata,
            geometry_column=geom_col,
            verbose=verbose,
            **write.funnel_kwargs(),
        )

        if not should_stream_output(output_path):
            success(f"Extracted data to {output_path}")


def _extract_connection(input_parquet: str):
    """The connection an extraction reads ``input_parquet`` with (S3 auto-detected)."""
    if needs_httpfs(input_parquet):
        return get_duckdb_connection_for_s3(input_parquet, load_spatial=True)
    return get_duckdb_connection(load_spatial=True, load_httpfs=False)


def _print_dry_run_output(
    input_parquet: str,
    output_parquet: str,
    geometry_col: str,
    bbox_col: str | None,
    selected_columns: list[str],
    bbox: str | None,
    geometry: str | None,
    where: str | None,
    limit: int | None,
    query: str,
    compression: str,
    compression_level: int | None,
    row_group_rows: int | None = None,
    row_group_size_mb: float | None = None,
) -> None:
    """Print dry run output."""
    warn("\n=== DRY RUN MODE - SQL Commands that would be executed ===\n")
    display_input = display_path(input_parquet)
    display_output = display_path(output_parquet)
    info(f"-- Input: {display_input}")
    info(f"-- Output: {display_output}")
    info(f"-- Geometry column: {geometry_col}")
    if bbox_col:
        info(f"-- Bbox column: {bbox_col}")
    info(f"-- Selected columns: {len(selected_columns)}")
    if bbox:
        info(f"-- Bbox filter: {bbox}")
    if geometry:
        info("-- Geometry filter: (provided)")
    if where:
        info(f"-- WHERE clause: {where}")
    if limit:
        info(f"-- Limit: {limit}")
    progress("")

    from geoparquet_io.core.compression import print_copy_dry_run

    # The count the funnel will resolve, measured the same way for an MB target,
    # so the printed COPY writes what gpio writes.
    with _extract_connection(input_parquet) as con:
        rows = resolve_row_group_rows_for_query(con, query, row_group_rows, row_group_size_mb)
    info("-- Main query:")
    print_copy_dry_run(query, output_parquet, compression, compression_level, rows)


def _advise_spatial_stats_on_merge(
    input_parquet: str,
    selected_columns: list[str],
    bbox_col: str | None,
    geoparquet_version: str | None,
    geometry_col: str | None = None,
) -> None:
    """Tell a merge that will not prune that ``--geoparquet-version 2.0`` would.

    A merged file is the one later reads window into again and again, so it is
    the write that benefits most from per-row-group spatial statistics -- and
    the one where their absence costs most. 219 per-country inputs concatenated
    without them made every downstream ``--bbox`` shard job read 12,764 of
    12,764 row groups, twelve times over a 90 GB file (#1151). Writing the merge
    as 2.0 in the first place is one pass; the alternative the reporting
    pipeline was paying for is a second whole-file ``gpio convert`` afterwards.

    ``sort hilbert`` already names the flag when sorting to 1.1 buys no
    pushdown, and #1151 asks the merge path to advertise the same way. It is an
    advisory, not a default: promoting a 1.x merge to 2.0 unasked would hand the
    caller a file older readers cannot open.

    Scoped to the case that is actually unprunable. A single-file extract is not
    a merge. The versions :func:`should_skip_bbox` says need no bbox covering are
    exactly the versions that carry their own per-row-group spatial statistics,
    so they are already prunable. And a *surviving* bbox covering column carries
    ordinary Parquet min/max per row group, which prunes too -- #1151 accepts
    either -- so it only speaks up when neither is there.
    """
    if is_remote_url(input_parquet) or not is_partition_path(input_parquet):
        return
    # Nothing spatial to collect statistics over: merging plain Parquet writes
    # no geo key at all, and 2.0 is a verified no-op for it. Advising the flag
    # there sends the reader after a setting that cannot help (review of #1151).
    # A geometry column dropped by --exclude-cols is the same case (#1163).
    if not geometry_col or geometry_col not in selected_columns:
        return
    if bbox_col and bbox_col in selected_columns:
        return

    effective_version = resolve_output_geoparquet_version(
        geoparquet_version, input_file=input_parquet
    )
    if should_skip_bbox(effective_version):
        return

    inputs = get_all_parquet_files(input_parquet)
    if len(inputs) < 2:
        return

    warn(
        f"Merging {len(inputs):,} files to GeoParquet {effective_version} with no bbox covering "
        "column: the merged file carries no per-row-group spatial statistics, so a later --bbox "
        "read has to scan every row group. Consider --geoparquet-version 2.0 to enable native "
        "geo_bbox row group statistics."
    )


def _execute_extraction(
    input_parquet: str,
    output_parquet: str,
    query: str,
    spatial_filter: str | None,
    where: str | None,
    limit: int | None,
    skip_count: bool,
    selected_columns: list[str],
    verbose: bool,
    show_sql: bool,
    write: WriteOptions,
    write_strategy: str = "duckdb-kv",
    geometry_col: str | None = None,
    repair_geometry: bool = True,
) -> None:
    """Execute the extraction query and write output."""
    if verbose:
        debug(f"Input: {input_parquet}")
        debug(f"Output: {output_parquet}")
        debug(f"Selecting {len(selected_columns)} columns: {', '.join(selected_columns)}")
        if spatial_filter:
            debug("Applying spatial filter")
        if where:
            debug(f"Applying WHERE clause: {where}")
        if limit:
            debug(f"Limiting to {limit:,} rows")

    # Use auto-detecting S3 connection for S3 paths
    con = _extract_connection(input_parquet)

    try:
        # Get total row count from input file metadata (fast - reads footer only)
        # DuckDB handles glob patterns natively
        input_total_rows = None
        if not skip_count:
            try:
                input_total_rows = get_parquet_row_count(input_parquet)
            except Exception:
                pass  # Total count is optional

        progress("Extracting rows...")

        # Get metadata from input for preservation
        # DuckDB handles glob patterns natively
        metadata = None
        try:
            metadata, _ = get_parquet_metadata(input_parquet, verbose=False)
        except Exception as e:
            # Preservation is best-effort (a corrupt footer surfaces as anything
            # from OSError to GeoParquetError to an Arrow exception), but don't
            # silently drop all geo/kv metadata: warn so it is visible.
            warn(f"Could not read input metadata for preservation; skipping: {e}")

        # Repair invalid geometry (issue #506). Auto-detects GEOMETRY vs WKB
        # encoding and rewrites the query to repair in place before the write.
        # Runs before the staleness decision, which depends on its outcome.
        geometry_repaired = False
        if geometry_col:
            query, geometry_repaired = _repair_geometry_in_query(
                con,
                query,
                geometry_col,
                repair_geometry,
                nonplanar_edges=declared_nonplanar_edges(input_parquet, metadata).get(geometry_col),
            )

        stale_columns = _stale_stat_columns(
            input_parquet, spatial_filter, where, limit, geometry_col, geometry_repaired
        )
        if geometry_repaired:
            # ST_MakeValid can rewind rings, so a carried orientation
            # declaration no longer holds; it is dropped, not recomputed.
            metadata = strip_orientation(metadata, geometry_col)

        # Write output
        write_parquet_with_metadata(
            con,
            query,
            output_parquet,
            original_metadata=metadata,
            verbose=verbose,
            show_sql=show_sql,
            write_strategy=write_strategy,
            input_file=input_parquet,
            invalidate_derived_stats=stale_columns is None or bool(stale_columns),
            invalidate_derived_stats_columns=stale_columns,
            **write.funnel_kwargs(),
        )

        # Get extracted row count from output file metadata (fast - reads footer only)
        extracted_count = get_parquet_row_count(output_parquet)

        if input_total_rows is not None:
            success(
                f"Extracted {extracted_count:,} rows (out of {input_total_rows:,} total) to {output_parquet}"
            )
        else:
            success(f"Extracted {extracted_count:,} rows to {output_parquet}")

    finally:
        con.close()


def extract(
    input_parquet: str,
    output_parquet: str | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    bbox: str | None = None,
    geometry: str | None = None,
    where: str | None = None,
    limit: int | None = None,
    skip_count: bool = False,
    use_first_geometry: bool = False,
    dry_run: bool = False,
    show_sql: bool = False,
    verbose: bool = False,
    compression: str | None = None,
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    allow_schema_diff: bool = False,
    overwrite: bool = False,
    hive_input: bool = False,
    write_strategy: str = "duckdb-kv",
    memory_limit: str | None = None,
    repair_geometry: bool = True,
) -> None:
    """
    Extract columns and rows from GeoParquet files.

    Supports column selection, spatial filtering (bbox, geometry),
    SQL filtering, and multiple input files via glob patterns.

    Also supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    S3 access mode (anonymous vs authenticated) is auto-detected per bucket.
    """
    _extract_impl(
        input_parquet,
        output_parquet,
        include_cols,
        exclude_cols,
        bbox,
        geometry,
        where,
        limit,
        skip_count,
        use_first_geometry,
        dry_run,
        show_sql,
        verbose,
        compression,
        compression_level,
        row_group_size_mb,
        row_group_rows,
        profile,
        geoparquet_version,
        allow_schema_diff,
        overwrite,
        hive_input,
        write_strategy,
        memory_limit,
        repair_geometry,
    )


def _validate_column_overlap(
    include_list: list[str] | None,
    exclude_list: list[str] | None,
    geometry_col: str | None,
    bbox_col: str | None,
    param_name: str = "include_cols/exclude_cols",
) -> None:
    """Validate that only special columns appear in both include and exclude lists.

    ``param_name`` names the pair in the error message, so the Python API reports
    ``columns``/``exclude_columns`` and the CLI reports its own option names.
    """
    if not (include_list and exclude_list):
        return

    special_cols = {geometry_col, bbox_col} - {None}

    overlap = set(include_list) & set(exclude_list)
    non_special_overlap = overlap - special_cols
    if non_special_overlap:
        raise InvalidParameterError(
            param_name,
            f"Columns cannot be in both: {', '.join(sorted(non_special_overlap))}. "
            f"Only geometry ({geometry_col}) and bbox ({bbox_col}) columns can appear in both.",
        )


def _extract_impl(
    input_parquet: str,
    output_parquet: str | None,
    include_cols: str | None,
    exclude_cols: str | None,
    bbox: str | None,
    geometry: str | None,
    where: str | None,
    limit: int | None,
    skip_count: bool,
    use_first_geometry: bool,
    dry_run: bool,
    show_sql: bool,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
    allow_schema_diff: bool = False,
    overwrite: bool = False,
    hive_input: bool = False,
    write_strategy: str = "duckdb-kv",
    memory_limit: str | None = None,
    repair_geometry: bool = True,
) -> None:
    """Internal implementation of extract with auto-detecting S3 access."""
    include_list = split_column_list(include_cols, "--include-cols")
    exclude_list = split_column_list(exclude_cols, "--exclude-cols")
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    # Check if output file exists and handle overwrite (fixes issue #278)
    if output_parquet and not is_streaming and not dry_run:
        handle_output_overwrite(output_parquet, overwrite, input_parquet)

    write = WriteOptions.pick(locals())
    if is_streaming and not dry_run:
        bbox_tuple = bbox_parse.parse_bbox(bbox) if bbox else None
        geometry_wkt = parse_geometry_input(geometry, use_first_geometry) if geometry else None
        if where:
            validate_where_clause(where)
        return _extract_streaming(
            input_parquet,
            output_parquet,
            include_list,
            exclude_list,
            bbox_tuple,
            geometry_wkt,
            where,
            limit,
            verbose,
            write,
            repair_geometry=repair_geometry,
        )

    # File-based mode
    # Every read below goes at the first file's footer, so an empty dataset has
    # to be named here -- the shared resolver only sees it once the read
    # expression is built, several DuckDB errors too late (#867).
    require_parquet_files(input_parquet)
    all_columns = get_schema_columns(input_parquet)
    geometry_col = primary_geometry_column(input_parquet, verbose=verbose)
    bbox_info = check_bbox_structure(input_parquet, verbose=False)
    bbox_col = bbox_info.get("bbox_column_name")

    _validate_column_overlap(include_list, exclude_list, geometry_col, bbox_col)
    if where:
        validate_where_clause(where)

    selected_columns = build_column_selection(
        all_columns,
        include_list,
        exclude_list,
        geometry_col,
        bbox_col,
        include_option="--include-cols",
        exclude_option="--exclude-cols",
    )
    bbox_tuple = bbox_parse.parse_bbox(bbox) if bbox else None
    if bbox_tuple and not dry_run:
        _warn_if_crs_mismatch(bbox_tuple, input_parquet, geometry_col)

    geometry_wkt = parse_geometry_input(geometry, use_first_geometry) if geometry else None
    spatial_filter = build_spatial_filter(bbox_tuple, geometry_wkt, bbox_info, geometry_col)

    query = build_extract_query(
        input_parquet,
        selected_columns,
        spatial_filter,
        where,
        limit,
        allow_schema_diff=allow_schema_diff,
        hive_input=hive_input,
    )
    if dry_run:
        _print_dry_run_output(
            input_parquet,
            output_parquet,
            geometry_col,
            bbox_col,
            selected_columns,
            bbox,
            geometry,
            where,
            limit,
            query,
            compression,
            compression_level,
            row_group_rows,
            row_group_size_mb,
        )
    else:
        _advise_spatial_stats_on_merge(
            input_parquet, selected_columns, bbox_col, geoparquet_version, geometry_col
        )
        _execute_extraction(
            input_parquet,
            output_parquet,
            query,
            spatial_filter,
            where,
            limit,
            skip_count,
            selected_columns,
            verbose,
            show_sql,
            write,
            write_strategy,
            geometry_col=geometry_col,
            repair_geometry=repair_geometry,
        )
