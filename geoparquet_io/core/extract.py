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

from geoparquet_io.core.column_selection import (
    reject_blank_column_entries,
    split_column_list,
)
from geoparquet_io.core.common import (
    check_bbox_structure,
    get_parquet_metadata,
    write_parquet_with_metadata,
)
from geoparquet_io.core.crs_utils import get_crs_display_name
from geoparquet_io.core.duckdb_utils import (
    _escape_sql_string,
    get_duckdb_connection,
    get_duckdb_connection_for_s3,
    quote_identifier,
    sql_path,
    validate_where_clause,
)
from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    GeoParquetError,
    InvalidParameterError,
)
from geoparquet_io.core.file_utils import (
    handle_output_overwrite,
    is_partition_path,
    resolve_file_url,
)
from geoparquet_io.core.geo_metadata import (
    backfill_derived_stats,
    prune_geo_metadata_to_columns,
    strip_derived_stats,
    strip_orientation,
)
from geoparquet_io.core.geometry_detection import (
    STANDARD_GEOMETRY_NAMES,
    find_primary_geometry_column,
)
from geoparquet_io.core.geometry_repair import repair_query_geometry
from geoparquet_io.core.logging_config import debug, info, progress, success, warn
from geoparquet_io.core.partition.reader import require_parquet_files
from geoparquet_io.core.remote import (
    _sanitize_url_for_logging,
    is_remote_url,
    needs_httpfs,
)
from geoparquet_io.core.stream_io import open_input, write_output
from geoparquet_io.core.streaming import (
    find_geometry_column_from_metadata,
    find_geometry_column_from_table,
    is_stdin,
    should_stream_output,
)


def get_parquet_row_count(parquet_file: str) -> int:
    """Get row count from parquet file metadata using DuckDB (O(1) - reads footer only)."""
    from geoparquet_io.core.duckdb_metadata import get_row_count

    return get_row_count(parquet_file)


def looks_like_latlong_bbox(bbox: tuple[float, float, float, float]) -> bool:
    """Check if bbox values look like lat/long coordinates."""
    xmin, ymin, xmax, ymax = bbox
    # Lat/long: x (lon) is -180 to 180, y (lat) is -90 to 90
    return -180 <= xmin <= 180 and -180 <= xmax <= 180 and -90 <= ymin <= 90 and -90 <= ymax <= 90


def is_geographic_crs(crs_info: dict | str | None) -> bool | None:
    """
    Check if CRS is geographic (lat/long) vs projected.

    Returns:
        True if geographic, False if projected, None if unknown
    """
    if crs_info is None:
        return None

    if isinstance(crs_info, str):
        crs_upper = crs_info.upper()
        # Common geographic CRS codes
        if any(
            code in crs_upper for code in ["4326", "CRS84", "CRS:84", "OGC:CRS84", "4269", "4267"]
        ):
            return True
        return None

    if isinstance(crs_info, dict):
        # Check PROJJSON type
        crs_type = crs_info.get("type", "")
        if crs_type == "GeographicCRS":
            return True
        if crs_type == "ProjectedCRS":
            return False

        # Check EPSG code
        crs_id = crs_info.get("id", {})
        if isinstance(crs_id, dict):
            code = crs_id.get("code")
            if code in [4326, 4269, 4267]:  # Common geographic codes
                return True

    return None


def _get_crs_from_file(input_parquet: str, geometry_col: str) -> dict | str | None:
    """
    Get CRS info from GeoParquet metadata or Parquet geo logical type.

    Returns CRS info dict/string or None if not found.
    """
    from geoparquet_io.core.duckdb_metadata import (
        get_geo_metadata,
        get_schema_info,
        parse_geometry_logical_type,
    )

    # First try GeoParquet file-level metadata
    try:
        geo_meta = get_geo_metadata(input_parquet)
        if geo_meta:
            columns_meta = geo_meta.get("columns", {})
            if geometry_col in columns_meta:
                crs_info = columns_meta[geometry_col].get("crs")
                if crs_info:
                    return crs_info
    except Exception:
        # CRS extraction is optional
        pass

    # Fall back to Parquet geo logical type (for GeoParquet 2.0 / parquet-geo)
    try:
        schema_info = get_schema_info(input_parquet)
        for col in schema_info:
            name = col.get("name", "")
            if name != geometry_col:
                continue
            logical_type = col.get("logical_type") or ""
            # DuckDB returns GeometryType(...) and GeographyType(...) from parquet_schema()
            if logical_type and (
                logical_type.startswith("GeometryType(")
                or logical_type.startswith("GeographyType(")
            ):
                parsed = parse_geometry_logical_type(logical_type)
                if parsed and "crs" in parsed:
                    return parsed["crs"]
    except Exception:
        # CRS extraction is optional
        pass

    return None


def _get_data_bounds(input_parquet: str, geometry_col: str) -> tuple | None:
    """Get actual data bounds from file."""
    try:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))
        raw_url = resolve_file_url(input_parquet, verbose=False)
        result = con.execute(f"""
            SELECT
                MIN(ST_XMin({quote_identifier(geometry_col)})) as xmin,
                MIN(ST_YMin({quote_identifier(geometry_col)})) as ymin,
                MAX(ST_XMax({quote_identifier(geometry_col)})) as xmax,
                MAX(ST_YMax({quote_identifier(geometry_col)})) as ymax
            FROM read_parquet({sql_path(raw_url)})
        """).fetchone()
        con.close()
        if result and all(v is not None for v in result):
            return result
    except Exception:
        # Bounds extraction is optional - used only for warning messages
        pass
    return None


def _warn_if_crs_mismatch(
    bbox: tuple[float, float, float, float],
    input_parquet: str,
    geometry_col: str,
) -> None:
    """Warn if bbox looks like lat/long but data is in a projected CRS."""
    if not looks_like_latlong_bbox(bbox):
        return  # User's bbox doesn't look like lat/long, no warning needed

    crs_info = _get_crs_from_file(input_parquet, geometry_col)
    is_geographic = is_geographic_crs(crs_info)

    if is_geographic is False:  # Definitely projected
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


def parse_bbox(bbox_str: str) -> tuple[float, float, float, float]:
    """
    Parse bounding box string into tuple of floats.

    Args:
        bbox_str: Comma-separated string "xmin,ymin,xmax,ymax"

    Returns:
        tuple: (xmin, ymin, xmax, ymax)

    Raises:
        InvalidParameterError: If format is invalid or coordinates are reversed
    """
    try:
        parts = [float(x.strip()) for x in bbox_str.split(",")]
        if len(parts) != 4:
            raise InvalidParameterError(
                "bbox", f"expected 4 values (xmin,ymin,xmax,ymax), got {len(parts)}"
            )
        xmin, ymin, xmax, ymax = parts

        # Validate coordinate ordering
        if xmin > xmax or ymin > ymax:
            raise InvalidParameterError(
                "bbox",
                f"coordinates appear to be reversed. "
                f"xmin ({xmin}) must be <= xmax ({xmax}), and ymin ({ymin}) must be <= ymax ({ymax}). "
                "Expected order: xmin,ymin,xmax,ymax.",
            )

        return (xmin, ymin, xmax, ymax)
    except ValueError as e:
        raise InvalidParameterError(
            "bbox", f"expected numeric values: xmin,ymin,xmax,ymax. Error: {e}"
        ) from e


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
        geojson_str = _escape_sql_string(json.dumps(geojson))
        result = con.execute(f"""
            SELECT ST_AsText(ST_GeomFromGeoJSON('{geojson_str}'))
        """).fetchone()
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
    from geoparquet_io.core.file_utils import get_first_parquet_file, is_partition_path

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
        result = con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_path(raw_url)})").fetchall()
        return [row[0] for row in result]
    finally:
        con.close()


def build_column_selection(
    all_columns: list[str],
    include_cols: list[str] | None,
    exclude_cols: list[str] | None,
    geometry_col: str | None,
    bbox_col: str | None,
) -> list[str]:
    """
    Build list of columns to select.

    Rules:
    - If include_cols only: select those + geometry + bbox
    - If exclude_cols only: select all except those
    - If both: select include_cols, but exclude_cols can remove geometry/bbox
    - geometry and bbox always included unless in exclude_cols

    Args:
        all_columns: All columns in schema
        include_cols: Columns to include (or None)
        exclude_cols: Columns to exclude (or None)
        geometry_col: Name of geometry column (or None for a table that has none)
        bbox_col: Name of bbox column (or None if not present)

    Returns:
        list: Columns to select (preserving original order)
    """
    exclude_set = set(exclude_cols) if exclude_cols else set()

    if include_cols:
        selected = set(include_cols)
        # Always add geometry unless explicitly excluded
        if geometry_col and geometry_col not in exclude_set:
            selected.add(geometry_col)
        # Always add bbox unless explicitly excluded
        if bbox_col and bbox_col not in exclude_set:
            selected.add(bbox_col)
    elif exclude_cols:
        selected = set(all_columns) - exclude_set
    else:
        selected = set(all_columns)

    # Preserve original column order
    return [c for c in all_columns if c in selected]


def validate_columns(
    requested_cols: list[str] | None, all_columns: list[str], option_name: str
) -> None:
    """
    Validate that requested columns exist in schema.

    Matching is exact, because the caller selects from an Arrow schema, where
    a column name is case-sensitive. Contrast
    ``column_selection.resolve_columns_against_schema``, which the backends that
    build SQL or a service request use to *resolve* the spelling.

    Args:
        requested_cols: Columns requested by user
        all_columns: All columns in schema
        option_name: Name of the option for error message

    Raises:
        InvalidParameterError: If any entry is blank, or any column not found
    """
    if not requested_cols:
        return

    # A blank entry is not a missing column, and saying so beat reporting
    # "Columns not found in schema: ." at a caller who passed columns=["id", ""]
    # from the Python API (#980).
    reject_blank_column_entries(requested_cols, option_name)

    missing = set(requested_cols) - set(all_columns)
    if missing:
        raise InvalidParameterError(
            option_name,
            f"Columns not found in schema: {', '.join(sorted(missing))}. "
            f"Available columns: {', '.join(all_columns)}",
        )


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
        xmin, ymin, xmax, ymax = bbox
        if bbox_info.get("has_bbox_column"):
            bbox_col = bbox_info["bbox_column_name"]
            conditions.append(
                f"({quote_identifier(bbox_col)}.xmax >= {xmin} AND {quote_identifier(bbox_col)}.xmin <= {xmax} "
                f"AND {quote_identifier(bbox_col)}.ymax >= {ymin} AND {quote_identifier(bbox_col)}.ymin <= {ymax})"
            )
        else:
            conditions.append(
                f"ST_Intersects({quote_identifier(geometry_col)}, ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))"
            )

    if geometry_wkt:
        escaped_wkt = _escape_sql_string(geometry_wkt)
        conditions.append(
            f"ST_Intersects({quote_identifier(geometry_col)}, ST_GeomFromText('{escaped_wkt}'))"
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
    from geoparquet_io.core.partition.reader import build_read_parquet_expr

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
    geometry_col = find_geometry_column_from_table(table)

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


def _setup_geometry_view(
    con,
    table: pa.Table,
    geom_col: str,
) -> tuple[str, bool]:
    """
    Setup geometry view if needed for BLOB geometry columns.

    Returns (source_ref, needs_wkb_conversion).
    """
    columns_info = con.execute("DESCRIBE __input_table").fetchall()
    geom_is_blob = any(col[0] == geom_col and "BLOB" in col[1].upper() for col in columns_info)

    if not (geom_is_blob and geom_col in table.column_names):
        return "__input_table", False

    # Create view with geometry conversion (quote column names for special chars)
    other_cols = [quote_identifier(c) for c in table.column_names if c != geom_col]
    col_defs = other_cols + [
        f"ST_GeomFromWKB({quote_identifier(geom_col)}) AS {quote_identifier(geom_col)}"
    ]
    view_query = f"CREATE VIEW __input_view AS SELECT {', '.join(col_defs)} FROM __input_table"
    con.execute(view_query)
    return "__input_view", True


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
        f"ST_AsWKB({quote_identifier(geom_col)}) AS {quote_identifier(geom_col)}"
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
    all_columns, detected_geom, bbox_info = _get_table_column_info(table)
    geom_col = geometry_column or detected_geom
    bbox_col = bbox_info.get("bbox_column_name")

    if geom_col is not None and geom_col not in all_columns:
        raise InvalidParameterError(
            "geometry_column",
            f"geometry_column '{geom_col}' not found in table columns: {list(all_columns)}",
        )

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
    validate_columns(columns, list(all_columns), "columns")
    validate_columns(exclude_columns, list(all_columns), "exclude_columns")

    selected_columns = build_column_selection(
        all_columns, columns, exclude_columns, geom_col, bbox_col
    )
    spatial_filter = build_spatial_filter(bbox, None, bbox_info, geom_col) if bbox else None

    if where:
        validate_where_clause(where)

    keeps_geometry = geom_col is not None and geom_col in selected_columns

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.register("__input_table", table)
        source_ref, needs_wkb = (
            _setup_geometry_view(con, table, geom_col) if geom_col else ("__input_table", False)
        )

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
                con, query, geom_col, repair_geometry
            )

        result = con.execute(query).arrow().read_all()
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
        return result
    finally:
        con.close()


def _repair_geometry_in_query(
    con, query: str, geometry_column: str, repair: bool
) -> tuple[str, bool]:
    """Run the geometry repair pass, reporting whether it rewrote the query.

    :func:`repair_query_geometry` returns ``query`` unchanged when repair is off,
    when the column is not repairable in place, and when it counted zero invalid
    rows; it returns a rewritten query only when at least one row will actually
    be repaired. That distinction is what decides whether the input's derived
    stats survive into the output: a pass that changed nothing keeps them, so a
    file with no invalid geometry is never rescanned for nothing.
    """
    repaired_query = repair_query_geometry(con, query, geometry_column, repair=repair)
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
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None,
    repair_geometry: bool = True,
) -> None:
    """Handle extraction with streaming input/output."""
    # Suppress verbose when streaming to stdout
    if should_stream_output(output_path):
        verbose = False

    with open_input(input_path, verbose=verbose) as (source, metadata, is_stream, con):
        # Get column names from query result (works with both table names and read_parquet)
        sample = con.execute(f"SELECT * FROM {source} LIMIT 0").description
        all_columns = [col[0] for col in sample]

        # Find geometry column
        geom_col = find_geometry_column_from_metadata(metadata)
        if not geom_col:
            for name in STANDARD_GEOMETRY_NAMES:
                if name in all_columns:
                    geom_col = name
                    break
        if not geom_col:
            geom_col = "geometry"

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
        query, geometry_repaired = _repair_geometry_in_query(con, query, geom_col, repair_geometry)

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
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
        )

        if not should_stream_output(output_path):
            success(f"Extracted data to {output_path}")


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
) -> None:
    """Print dry run output."""
    warn("\n=== DRY RUN MODE - SQL Commands that would be executed ===\n")
    display_input = (
        _sanitize_url_for_logging(input_parquet) if is_remote_url(input_parquet) else input_parquet
    )
    display_output = (
        _sanitize_url_for_logging(output_parquet)
        if is_remote_url(output_parquet)
        else output_parquet
    )
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

    compression_desc = compression
    if compression in ["GZIP", "ZSTD", "BROTLI"] and compression_level:
        compression_desc = f"{compression}:{compression_level}"

    duckdb_compression = compression.lower() if compression != "UNCOMPRESSED" else "uncompressed"
    display_query = f"""COPY ({query})
TO {sql_path(output_parquet)}
(FORMAT PARQUET, COMPRESSION '{duckdb_compression}');"""

    info("-- Main query:")
    progress(display_query)
    info(f"\n-- Note: Using {compression_desc} compression")


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
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    geoparquet_version: str | None = None,
    write_strategy: str = "duckdb-kv",
    memory_limit: str | None = None,
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
    if needs_httpfs(input_parquet):
        con = get_duckdb_connection_for_s3(input_parquet, load_spatial=True)
    else:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)

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
                con, query, geometry_col, repair_geometry
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
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            show_sql=show_sql,
            profile=profile,
            geoparquet_version=geoparquet_version,
            write_strategy=write_strategy,
            memory_limit=memory_limit,
            input_file=input_parquet,
            invalidate_derived_stats=stale_columns is None or bool(stale_columns),
            invalidate_derived_stats_columns=stale_columns,
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
    compression: str = "ZSTD",
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

    if is_streaming and not dry_run:
        bbox_tuple = parse_bbox(bbox) if bbox else None
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
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            profile,
            geoparquet_version,
            repair_geometry=repair_geometry,
        )

    # File-based mode
    # Every read below goes at the first file's footer, so an empty dataset has
    # to be named here -- the shared resolver only sees it once the read
    # expression is built, several DuckDB errors too late (#867).
    require_parquet_files(input_parquet)
    all_columns = get_schema_columns(input_parquet)
    geometry_col = find_primary_geometry_column(input_parquet, verbose)
    bbox_info = check_bbox_structure(input_parquet, verbose=False)
    bbox_col = bbox_info.get("bbox_column_name")

    _validate_column_overlap(include_list, exclude_list, geometry_col, bbox_col)
    validate_columns(include_list, all_columns, "--include-cols")
    validate_columns(exclude_list, all_columns, "--exclude-cols")
    if where:
        validate_where_clause(where)

    selected_columns = build_column_selection(
        all_columns, include_list, exclude_list, geometry_col, bbox_col
    )
    bbox_tuple = parse_bbox(bbox) if bbox else None
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
        )
    else:
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
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            profile,
            geoparquet_version,
            write_strategy,
            memory_limit,
            geometry_col=geometry_col,
            repair_geometry=repair_geometry,
        )
