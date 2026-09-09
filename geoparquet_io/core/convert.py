#!/usr/bin/env python3

import gc
import os
import time
from pathlib import Path

import duckdb

from geoparquet_io.core.common import (
    format_size,
    read_preserved_kv_metadata,
    write_parquet_with_metadata,
)
from geoparquet_io.core.crs_utils import (
    _format_crs_display,
    detect_crs_from_spatial_file,
    extract_crs_from_parquet,
    is_default_crs,
    normalize_projjson_crs,
    note_default_crs_normalized,
    parse_crs_string_to_projjson,
)
from geoparquet_io.core.duckdb_utils import (
    _escape_sql_string,
    _geoarrow_coord_exprs,
    get_duckdb_connection,
    quote_identifier,
    sql_path,
)
from geoparquet_io.core.exceptions import (
    GeometryError,
    GeoParquetError,
    InvalidParameterError,
    RemoteAccessError,
)
from geoparquet_io.core.file_utils import (
    is_partition_path,
    resolve_file_url,
    validate_output_path,
)
from geoparquet_io.core.geo_metadata import build_bbox_covering
from geoparquet_io.core.geometry_repair import (
    repair_arrow_table_geometry,
    repair_query_geometry,
)
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success, warn
from geoparquet_io.core.partition.reader import require_single_file
from geoparquet_io.core.remote import (
    get_remote_error_hint,
    is_remote_url,
    needs_httpfs,
    setup_aws_profile_if_needed,
    show_remote_read_message,
    validate_profile_for_urls,
)


def _validate_layer_name(layer: str) -> str:
    """Validate and sanitize a layer name for use in SQL.

    Layer names in GeoPackage/FileGDB can contain letters, numbers, underscores,
    and spaces. We escape single quotes to prevent SQL injection.

    Args:
        layer: Layer name to validate

    Returns:
        Sanitized layer name safe for SQL interpolation

    Raises:
        ValueError: If layer name contains dangerous characters
    """
    # Block obviously malicious patterns
    dangerous_patterns = ["--", ";", "/*", "*/", "\\"]
    for pattern in dangerous_patterns:
        if pattern in layer:
            raise ValueError(
                f"Invalid layer name '{layer}': contains unsafe character sequence '{pattern}'"
            )

    # Escape single quotes (SQL standard: double them)
    return _escape_sql_string(layer)


def _build_st_read_expr(input_path: str, layer: str | None = None, keep_wkb: bool = False) -> str:
    """Build ST_Read expression with optional layer/keep_wkb parameters.

    Args:
        input_path: RAW (unescaped) path or URL to the spatial file. It is
            turned into a SQL literal here by ``sql_path`` -- exactly once, so
            do not pass an already-escaped path (issue #718).
        layer: Optional layer name for multi-layer formats (GeoPackage, FileGDB)
        keep_wkb: Return raw WKB blobs instead of parsed GEOMETRY (DuckDB's
            escape hatch for geometry subtypes it cannot represent)

    Returns:
        SQL expression for ST_Read

    Raises:
        ValueError: If layer name contains invalid characters

    Warning:
        DuckDB's ST_Read may segfault (not raise an exception) when given an
        invalid layer name that doesn't exist in the file. This is an upstream
        bug. Consider validating layer names against the file's available layers
        before calling this function if user input is involved.
    """
    # DuckDB uses := for named parameters
    params = ""
    if keep_wkb:
        params += ", keep_wkb := true"
    if layer:
        params += f", layer := '{_validate_layer_name(layer)}'"
    return f"ST_Read({sql_path(input_path)}{params})"


def _choose_read_strategy(input_path, layer=None, linearize_curves=True):
    """Pick how to read a spatial file: 'normal', 'linearized', or 'error'.

    ``input_path`` must be RAW: this stats the filesystem, and an escaped path
    never exists, so the pre-scan silently degrades to 'normal' (issue #718).

    Local GeoPackages are pre-scanned for curved geometry types with the
    stdlib (issue #643) so the strategy is known without provoking a DuckDB
    error. Formats without a cheap scan return 'normal' and rely on the
    error-triggered fallback in the caller.
    """
    from geoparquet_io.core.curved_geometry import find_non_linear_gpkg_types

    path = Path(input_path)
    if path.suffix.lower() != ".gpkg" or not path.exists():
        return "normal"
    if not find_non_linear_gpkg_types(path, layer):
        return "normal"
    return "linearized" if linearize_curves else "error"


def _validate_max_angle(max_angle_deg):
    """Reject non-positive linearization tolerances before they corrupt output."""
    if max_angle_deg is not None and not max_angle_deg > 0:
        raise InvalidParameterError("max_angle_deg", "must be a positive number of degrees")


def _detect_geometry_column(con, input_file, verbose, is_parquet=False, layer=None):
    """Detect geometry column name from input file.

    ``input_file`` is RAW: ``detect_parquet_geometry_column`` escapes its own
    argument, and ``_build_st_read_expr`` escapes at the SQL boundary.
    """
    from geoparquet_io.core.geometry_detection import (
        STANDARD_GEOMETRY_NAMES,
        detect_parquet_geometry_column,
    )

    if verbose:
        debug("Detecting geometry column from input...")

    # For parquet files, check GeoParquet metadata first, then fall back to names
    if is_parquet:
        result = detect_parquet_geometry_column(input_file, verbose=verbose)
        return result

    # For other formats, use schema-based detection with standard names
    table_expr = _build_st_read_expr(input_file, layer)
    detect_query = f"SELECT * FROM {table_expr} LIMIT 0"

    schema_result = con.execute(detect_query).description

    for col_info in schema_result:
        col_name = col_info[0].lower()
        if col_name in STANDARD_GEOMETRY_NAMES:
            if verbose:
                debug(f"Detected geometry column: {col_info[0]}")
            return col_info[0]

    if verbose:
        debug("No geometry column found in input file")
    return None


def detect_all_geometry_columns(input_file: str, verbose: bool = False) -> dict:
    """Detect all geometry columns from a GeoParquet file.

    Reads the GeoParquet metadata to find all geometry columns listed
    in the `columns` dict, distinguishing primary from secondary columns.

    For non-GeoParquet files, returns only the detected primary column
    (multi-geometry is only supported for GeoParquet input).

    Args:
        input_file: Path to input file
        verbose: Print debug output

    Returns:
        dict with keys:
            - "primary": str - name of primary geometry column
            - "secondary": list[str] - names of secondary geometry columns
            - "metadata": dict - per-column metadata from input (crs, encoding, etc.)
    """
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata

    result = {"primary": None, "secondary": [], "metadata": {}}

    # Only GeoParquet files can have multiple geometry columns with metadata
    if not _is_parquet_file(input_file):
        # For non-parquet, detect single geometry column the standard way
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_file))
        try:
            geom_col = _detect_geometry_column(con, input_file, verbose, is_parquet=False)
            if geom_col:
                result["primary"] = geom_col
                result["metadata"][geom_col] = {"encoding": "WKB"}
        finally:
            con.close()
        return result

    geo_meta = get_geo_metadata(input_file)

    if not geo_meta:
        # No GeoParquet metadata - detect single column from schema
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_file))
        try:
            geom_col = _detect_geometry_column(con, input_file, verbose, is_parquet=True)
            if geom_col:
                result["primary"] = geom_col
                result["metadata"][geom_col] = {"encoding": "WKB"}
        finally:
            con.close()
        return result

    # Extract from GeoParquet metadata
    primary_col = geo_meta.get("primary_column", "geometry")
    columns = geo_meta.get("columns", {})

    result["primary"] = primary_col

    for col_name, col_meta in columns.items():
        result["metadata"][col_name] = col_meta
        if col_name != primary_col:
            result["secondary"].append(col_name)

    if verbose:
        debug(
            f"Detected geometry columns - primary: {primary_col}, secondary: {result['secondary']}"
        )

    return result


def _calculate_bounds(
    con, input_file, geom_column, verbose, is_parquet=False, encoding="WKB", table_expr=None
):
    """Calculate dataset bounds from input file (or an explicit table_expr).

    Returns None for a dataset with nothing to measure — no rows, or every
    geometry empty or NULL — leaving the caller to convert without spatial
    ordering rather than fail (issue #649).
    """
    if verbose:
        debug("Calculating dataset bounds...")

    # For parquet files, read directly; for other formats use ST_Read
    if table_expr is None:
        if is_parquet:
            table_expr = f"read_parquet({sql_path(input_file)})"
        else:
            table_expr = f"ST_Read({sql_path(input_file)})"

    # Quote column name to handle special characters, spaces, and reserved words
    quoted_geom = quote_identifier(geom_column)

    # GeoArrow native encodings store geometry as nested structs/arrays that
    # DuckDB cannot pass to ST_XMin directly. Use list_min/list_max via
    # _geoarrow_coord_exprs — consistent with _build_conversion_query and
    # avoids UNNEST row explosion for large multipolygon datasets.
    geoarrow_native = encoding.lower() not in {"wkb", "wkt"}
    if geoarrow_native:
        xmin_e, ymin_e, xmax_e, ymax_e, _, _ = _geoarrow_coord_exprs(quoted_geom, encoding)
        bounds_query = f"""
            SELECT
                MIN({xmin_e}) as xmin,
                MIN({ymin_e}) as ymin,
                MAX({xmax_e}) as xmax,
                MAX({ymax_e}) as ymax
            FROM {table_expr}
            WHERE NOT isnan({xmax_e}) AND NOT isnan({ymax_e})
        """
    else:
        bounds_query = f"""
            SELECT
                MIN(ST_XMin({quoted_geom})) as xmin,
                MIN(ST_YMin({quoted_geom})) as ymin,
                MAX(ST_XMax({quoted_geom})) as xmax,
                MAX(ST_YMax({quoted_geom})) as ymax
            FROM {table_expr}
        """
    bounds_result = con.execute(bounds_query).fetchone()

    if not bounds_result or any(v is None for v in bounds_result):
        return None

    if verbose:
        xmin, ymin, xmax, ymax = bounds_result
        debug(f"Dataset bounds: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")

    return bounds_result


def _is_csv_file(input_file):
    """Check if input file is CSV/TSV format."""
    ext = os.path.splitext(input_file)[1].lower()
    return ext in [".csv", ".tsv", ".txt"]


def _is_parquet_file(input_file):
    """Check if input file is already Parquet format."""
    # Handle URLs by extracting path before query params
    path = input_file.split("?")[0]
    ext = os.path.splitext(path)[1].lower()
    return ext == ".parquet"


def _is_geojson_file(input_file):
    """Check if input file is GeoJSON format."""
    # Handle URLs by extracting path before query params
    path = input_file.split("?")[0]
    ext = os.path.splitext(path)[1].lower()
    return ext in [".geojson", ".json"]


# Default max line size for CSV reading: 50MB
# DuckDB defaults to 2MB, but geospatial CSVs often contain WKT geometries
# with complex polygons (coastlines, admin boundaries) that exceed this.
# 50MB should handle virtually any reasonable geospatial data.
# See: https://github.com/geoparquet/geoparquet-io/issues/301
CSV_MAX_LINE_SIZE_DEFAULT = 50 * 1024 * 1024  # 50 MB

# Module-level override (set by CLI --csv-max-line-size option)
_csv_max_line_size_override = None


def get_csv_max_line_size():
    """Get effective CSV max line size, checking override and env var."""
    import os

    # 1. Module-level override (from CLI)
    if _csv_max_line_size_override is not None:
        return _csv_max_line_size_override

    # 2. Environment variable (power-user escape hatch)
    env_val = os.environ.get("GPIO_CSV_MAX_LINE_SIZE")
    if env_val:
        try:
            return int(env_val)
        except ValueError:
            pass  # Fall through to default

    # 3. Default
    return CSV_MAX_LINE_SIZE_DEFAULT


def set_csv_max_line_size(value):
    """Set the CSV max line size override. Pass None to reset to default."""
    global _csv_max_line_size_override
    _csv_max_line_size_override = value


def _build_csv_read_expr(input_url, delimiter):
    """Build DuckDB CSV read expression with geospatial-appropriate max_line_size.

    Args:
        input_url: A RAW path or URL. ``sql_path()`` quotes and escapes it
            here, so callers must not pre-escape it (#802).
        delimiter: A RAW CSV delimiter, or None to auto-detect. It goes into a
            SQL string literal, so it is escaped here -- exactly once, at the
            boundary -- rather than by the caller (#937).
    """
    max_line_size = get_csv_max_line_size()
    if delimiter:
        return (
            f"read_csv({sql_path(input_url)}, delim='{_escape_sql_string(delimiter)}', "
            f"header=true, AUTO_DETECT=TRUE, max_line_size={max_line_size})"
        )
    return f"read_csv_auto({sql_path(input_url)}, max_line_size={max_line_size})"


def _get_csv_columns(con, csv_read):
    """Get column names from CSV, return (columns_list, col_names_lower_dict)."""
    columns = con.execute(f"SELECT * FROM {csv_read} LIMIT 0").description
    col_names_lower = {col[0].lower(): col[0] for col in columns}
    return columns, col_names_lower


def _validate_explicit_wkt_column(wkt_column, columns):
    """Validate explicitly specified WKT column exists."""
    actual_cols = [col[0] for col in columns]
    if wkt_column not in actual_cols:
        raise InvalidParameterError(
            "wkt_column",
            f"column '{wkt_column}' not found in CSV. Available columns: {', '.join(actual_cols)}",
        )


def _validate_explicit_latlon_columns(lat_column, lon_column, columns):
    """Validate explicitly specified lat/lon columns exist."""
    if not (lat_column and lon_column):
        raise InvalidParameterError("lat_column/lon_column", "both must be specified together")

    actual_cols = [col[0] for col in columns]
    if lat_column not in actual_cols:
        raise InvalidParameterError(
            "lat_column",
            f"column '{lat_column}' not found in CSV. Available columns: {', '.join(actual_cols)}",
        )
    if lon_column not in actual_cols:
        raise InvalidParameterError(
            "lon_column",
            f"column '{lon_column}' not found in CSV. Available columns: {', '.join(actual_cols)}",
        )


def _try_detect_wkt_column(con, csv_read, col_names_lower):
    """Try to auto-detect WKT column. Returns column name or None."""
    wkt_candidates = ["wkt", "geometry", "geom", "the_geom", "shape"]
    for candidate in wkt_candidates:
        if candidate in col_names_lower:
            actual_col = col_names_lower[candidate]
            try:
                # Validate by trying to parse sample row
                sample = con.execute(
                    f"SELECT {actual_col} FROM {csv_read} WHERE {actual_col} IS NOT NULL LIMIT 1"
                ).fetchone()
                if sample and sample[0]:
                    # Validate WKT by parsing it — execute without fetchone to avoid
                    # DuckDB 1.5+ GEOMETRY serialization error
                    con.execute("SELECT ST_GeomFromText(?)", [sample[0]])
                    return actual_col
            except Exception:
                continue
    return None


def _try_detect_latlon_columns(col_names_lower):
    """Try to auto-detect lat/lon columns. Returns (lat_col, lon_col) or (None, None)."""
    lat_candidates = ["lat", "latitude", "y"]
    lon_candidates = ["lon", "lng", "long", "longitude", "x"]

    found_lat = next(
        (col_names_lower[name] for name in lat_candidates if name in col_names_lower), None
    )
    found_lon = next(
        (col_names_lower[name] for name in lon_candidates if name in col_names_lower), None
    )

    return found_lat, found_lon


def _handle_explicit_columns(wkt_column, lat_column, lon_column, columns, csv_read):
    """Handle explicitly specified columns. Returns geom_info dict or None."""
    if wkt_column:
        _validate_explicit_wkt_column(wkt_column, columns)
        return {"type": "wkt", "wkt_column": wkt_column, "csv_read": csv_read}

    if lat_column or lon_column:
        _validate_explicit_latlon_columns(lat_column, lon_column, columns)
        return {
            "type": "latlon",
            "lat_column": lat_column,
            "lon_column": lon_column,
            "csv_read": csv_read,
        }

    return None


def _auto_detect_geometry(con, csv_read, col_names_lower, verbose):
    """Auto-detect geometry columns. Returns geom_info dict or None."""
    # Try WKT first
    wkt_col = _try_detect_wkt_column(con, csv_read, col_names_lower)
    if wkt_col:
        if verbose:
            debug(f"Auto-detected WKT column: {wkt_col}")
        return {"type": "wkt", "wkt_column": wkt_col, "csv_read": csv_read}

    # Try lat/lon
    found_lat, found_lon = _try_detect_latlon_columns(col_names_lower)
    if found_lat and found_lon:
        if verbose:
            debug(f"Auto-detected lat/lon columns: {found_lat}, {found_lon}")
        return {
            "type": "latlon",
            "lat_column": found_lat,
            "lon_column": found_lon,
            "csv_read": csv_read,
        }

    return None


def _detect_csv_geometry_column(
    con, input_file, delimiter, wkt_column, lat_column, lon_column, verbose
):
    """Detect geometry columns in CSV/TSV."""
    csv_read = _build_csv_read_expr(input_file, delimiter)
    columns, col_names_lower = _get_csv_columns(con, csv_read)

    if verbose:
        delim_msg = delimiter if delimiter else "auto-detected"
        debug(f"Reading CSV/TSV with delimiter: {delim_msg}")
        debug(f"Detected columns: {', '.join([col[0] for col in columns])}")

    # Try explicit columns first
    geom_info = _handle_explicit_columns(wkt_column, lat_column, lon_column, columns, csv_read)
    if geom_info:
        return geom_info

    # Auto-detect
    geom_info = _auto_detect_geometry(con, csv_read, col_names_lower, verbose)
    if geom_info:
        return geom_info

    # No geometry found
    if verbose:
        debug("No geometry columns found in CSV/TSV file")
    return None


def _check_coord_range(axis, parameter, low, high, measured_min, measured_max):
    """Raise if one axis's measured range falls outside its valid domain.

    ``measured_min`` is None when every value in that column is NULL — MIN/MAX
    ignore NULLs — leaving no range to check. Each axis is therefore checked
    independently: a column of nothing but empty values must not silence the
    *other* axis, which may still hold measurable, and invalid, coordinates
    (issue #655).
    """
    if measured_min is None:
        return
    if measured_min < low or measured_max > high:
        raise InvalidParameterError(
            parameter,
            f"invalid {axis} values (range: {measured_min:.6f} to {measured_max:.6f}). "
            f"{axis.capitalize()} must be between {low} and {high}.",
        )


def _validate_latlon_ranges(con, csv_read, lat_col, lon_col, verbose):
    """Validate lat/lon columns have valid numeric ranges."""
    if verbose:
        debug(f"Validating lat/lon ranges for columns: {lat_col}, {lon_col}")

    lat_col = quote_identifier(lat_col)
    lon_col = quote_identifier(lon_col)
    query = f"""
        SELECT
            MIN(CAST({lat_col} AS DOUBLE)) as min_lat,
            MAX(CAST({lat_col} AS DOUBLE)) as max_lat,
            MIN(CAST({lon_col} AS DOUBLE)) as min_lon,
            MAX(CAST({lon_col} AS DOUBLE)) as max_lon,
            COUNT(*) FILTER ({lat_col} IS NULL OR {lon_col} IS NULL) as null_count
        FROM {csv_read}
    """

    try:
        result = con.execute(query).fetchone()
        min_lat, max_lat, min_lon, max_lon, null_count = result

        if null_count > 0:
            warn(
                f"⚠️  Warning: {null_count} rows have NULL lat/lon values and will be "
                "written with NULL geometry"
            )

        _check_coord_range("latitude", "lat_column", -90, 90, min_lat, max_lat)
        _check_coord_range("longitude", "lon_column", -180, 180, min_lon, max_lon)

        if verbose and min_lat is not None and min_lon is not None:
            debug(
                f"Lat/lon ranges validated: lat=[{min_lat:.6f}, {max_lat:.6f}], "
                f"lon=[{min_lon:.6f}, {max_lon:.6f}]"
            )

    except duckdb.ConversionException as e:
        raise InvalidParameterError(
            "lat_column/lon_column",
            f"contains non-numeric values: {str(e)}. Ensure lat/lon columns contain only numbers.",
        ) from e


def _check_null_wkt_rows(con, csv_read, wkt_col):
    """Check and warn about NULL WKT values."""
    quoted_wkt = quote_identifier(wkt_col)
    null_count = con.execute(
        f"SELECT COUNT(*) FILTER ({quoted_wkt} IS NULL) FROM {csv_read}"
    ).fetchone()[0]

    if null_count > 0:
        warn(
            f"⚠️  Warning: {null_count} rows have NULL WKT values and will be "
            "written with NULL geometry"
        )


def _check_invalid_wkt_rows(con, csv_read, wkt_col):
    """Check and warn about invalid WKT rows when skip_invalid is True.

    Uses TRY(ST_GeomFromText(...)) to count rows where WKT parsing fails.
    DuckDB 1.5+ requires TRY() instead of TRY_CAST(... AS GEOMETRY) for
    geometry parsing error handling.

    Args:
        con: DuckDB connection with spatial extension loaded.
        csv_read: SQL expression for reading the CSV (e.g., "read_csv('file.csv')").
        wkt_col: Name of the WKT column to validate.
    """
    quoted_wkt = quote_identifier(wkt_col)
    try:
        # Use TRY() to catch WKT parse errors — returns NULL for invalid WKT
        invalid_count = con.execute(
            f"SELECT COUNT(*) FROM {csv_read} "
            f"WHERE {quoted_wkt} IS NOT NULL AND TRY(ST_GeomFromText({quoted_wkt})) IS NULL"
        ).fetchone()[0]

        if invalid_count > 0:
            warn(f"⚠️  Warning: {invalid_count} rows have invalid WKT and will be skipped")
    except Exception as e:
        # May fail on older DuckDB versions or connection issues; log for debugging
        debug(f"Could not count invalid WKT rows: {e}")


def _validate_wkt_strict(con, csv_read, wkt_col):
    """Strictly validate WKT column when skip_invalid is False.

    Attempts to parse one non-NULL WKT value to verify the column contains
    valid geometry. Raises GeometryError with helpful message on failure.

    Uses ::VARCHAR cast on the result to avoid DuckDB 1.5+ GEOMETRY type
    serialization errors when fetching results to Python.

    Args:
        con: DuckDB connection with spatial extension loaded.
        csv_read: SQL expression for reading the CSV (e.g., "read_csv('file.csv')").
        wkt_col: Name of the WKT column to validate.

    Raises:
        GeometryError: If WKT parsing fails, with suggestion to use --skip-invalid.
    """
    quoted_wkt = quote_identifier(wkt_col)
    try:
        # Use ::VARCHAR cast to avoid DuckDB 1.5+ GEOMETRY serialization error
        con.execute(
            f"SELECT ST_GeomFromText({quoted_wkt})::VARCHAR FROM {csv_read} "
            f"WHERE {quoted_wkt} IS NOT NULL LIMIT 1"
        ).fetchone()
    except Exception as e:
        raise GeometryError(
            f"Invalid WKT in column '{wkt_col}': {str(e)}. "
            f"Use --skip-invalid to skip rows with invalid geometries."
        ) from e


def _warn_if_projected_crs(con, csv_read, wkt_col):
    """Warn if coordinates suggest projected CRS instead of WGS84."""
    quoted_wkt = quote_identifier(wkt_col)
    try:
        result = con.execute(
            f"SELECT MAX(ABS(ST_XMax(ST_GeomFromText({quoted_wkt})))) as max_x, "
            f"MAX(ABS(ST_YMax(ST_GeomFromText({quoted_wkt})))) as max_y "
            f"FROM {csv_read} WHERE {quoted_wkt} IS NOT NULL LIMIT 1000"
        ).fetchone()

        if result and result[0] is not None:
            max_x, max_y = result
            if max_x > 180 or max_y > 90:
                warn(
                    f"⚠️  Large coordinate values detected (max X: {max_x:.2f}, max Y: {max_y:.2f}). "
                    f"Data may be in projected CRS, not WGS84. "
                    f"Verify CRS or use --crs flag if needed."
                )
    except Exception:
        pass


def _validate_wkt_and_check_crs(con, csv_read, wkt_col, skip_invalid, verbose):
    """Validate WKT column and warn if coordinates suggest non-WGS84 CRS."""
    if verbose:
        debug(f"Validating WKT column: {wkt_col}")

    _check_null_wkt_rows(con, csv_read, wkt_col)

    if skip_invalid:
        _check_invalid_wkt_rows(con, csv_read, wkt_col)
    else:
        _validate_wkt_strict(con, csv_read, wkt_col)

    _warn_if_projected_crs(con, csv_read, wkt_col)


def _build_csv_conversion_query(geom_info, skip_hilbert, bounds, skip_invalid, skip_bbox=False):
    """Build SQL query for CSV/TSV conversion with geometry construction.

    Args:
        geom_info: Dict with geometry detection info
        skip_hilbert: Skip Hilbert ordering
        bounds: Tuple of bounds for Hilbert ordering
        skip_invalid: Skip invalid geometries
        skip_bbox: Skip adding bbox column (for 2.0/parquet-geo-only)
    """
    csv_read = geom_info["csv_read"]

    # Build bbox expression (empty string if skipping)
    def bbox_expr(geom):
        if skip_bbox:
            return ""
        return f""",
                STRUCT_PACK(
                    xmin := ST_XMin({geom}),
                    ymin := ST_YMin({geom}),
                    xmax := ST_XMax({geom}),
                    ymax := ST_YMax({geom})
                ) AS bbox"""

    # Build geometry expression and exclusion list
    if geom_info["type"] == "wkt":
        wkt_col = quote_identifier(geom_info["wkt_column"])
        geom_expr = f"ST_GeomFromText({wkt_col})"
        exclude_cols = wkt_col

        # For skip_invalid, use TRY() to silently return NULL for invalid WKT.
        # A row whose WKT is absent is not invalid — it is a row without
        # geometry, and dropping it loses its attributes (issue #655), so only
        # rows that failed to parse are filtered out.
        if skip_invalid:
            # The WKT column rides along inside the CTE so the outer WHERE can
            # tell "no geometry given" from "geometry did not parse"; it is
            # excluded from the output there instead.
            query_base = f"""
                WITH parsed_geoms AS (
                    SELECT
                        *,
                        TRY(ST_GeomFromText({wkt_col})) AS geometry
                    FROM {csv_read}
                )
                SELECT
                    * EXCLUDE ({exclude_cols}, geometry),
                    geometry{bbox_expr("geometry")}
                FROM parsed_geoms
                WHERE {wkt_col} IS NULL OR geometry IS NOT NULL
            """
            return query_base
        else:
            # NULL WKT yields NULL geometry (ST_GeomFromText propagates it), so
            # the row survives with its attributes instead of being filtered.
            where_clause = ""

    elif geom_info["type"] == "latlon":
        lat_col = quote_identifier(geom_info["lat_column"])
        lon_col = quote_identifier(geom_info["lon_column"])
        # Note: ST_Point expects (lon, lat) order
        geom_expr = f"ST_Point(CAST({lon_col} AS DOUBLE), CAST({lat_col} AS DOUBLE))"
        exclude_cols = f"{lat_col}, {lon_col}"

        # A missing coordinate means no geometry, not no row: ST_Point yields
        # NULL and the row keeps its attributes (issue #655).
        where_clause = ""

    else:
        raise GeoParquetError("Unknown geometry type in CSV detection")

    # Build base query (for non-skip_invalid or lat/lon)
    if skip_hilbert:
        return f"""
            SELECT
                * EXCLUDE ({exclude_cols}),
                {geom_expr} AS geometry{bbox_expr(geom_expr)}
            FROM {csv_read}
            {where_clause}
        """

    # With Hilbert ordering - use subquery
    xmin, ymin, xmax, ymax = bounds
    bounds_box = f"ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))"
    # A WKT column can hold empty geometry, which ST_Hilbert rejects (#649).
    unorderable = f"{geom_expr} IS NULL OR ST_IsEmpty({geom_expr})"
    return f"""
        SELECT
            * EXCLUDE ({exclude_cols}),
            {geom_expr} AS geometry{bbox_expr(geom_expr)}
        FROM {csv_read}
        {where_clause}
        ORDER BY ({unorderable}),
            ST_Hilbert({_orderable_geom(geom_expr, xmin, ymin)}, {bounds_box})
    """


def _get_geom_expr_and_where(geom_info, skip_invalid):
    """Geometry expression and WHERE clause for the CSV *bounds* pass.

    The filters here exclude rows the envelope must not be measured from
    (missing or unparsable geometry). The conversion query deliberately keeps
    those rows — see :func:`_build_csv_conversion_query` and issue #655.
    """
    if geom_info["type"] == "wkt":
        wkt_col = quote_identifier(geom_info["wkt_column"])
        if skip_invalid:
            # Use TRY() to silently skip invalid WKT
            geom_expr = f"TRY(ST_GeomFromText({wkt_col}))"
            where_clause = f"WHERE {wkt_col} IS NOT NULL AND {geom_expr} IS NOT NULL"
        else:
            geom_expr = f"ST_GeomFromText({wkt_col})"
            where_clause = f"WHERE {wkt_col} IS NOT NULL"
        return geom_expr, where_clause

    # latlon
    lat_col = quote_identifier(geom_info["lat_column"])
    lon_col = quote_identifier(geom_info["lon_column"])
    geom_expr = f"ST_Point(CAST({lon_col} AS DOUBLE), CAST({lat_col} AS DOUBLE))"
    where_clause = f"WHERE {lat_col} IS NOT NULL AND {lon_col} IS NOT NULL"
    return geom_expr, where_clause


def _calculate_csv_bounds(con, geom_info, skip_invalid, verbose):
    """Calculate dataset bounds from CSV geometry.

    Returns None when there is nothing to measure, mirroring
    :func:`_calculate_bounds`: an all-empty or row-less CSV converts unordered
    instead of failing (issue #649).
    """
    if verbose:
        debug("Calculating dataset bounds from CSV...")

    csv_read = geom_info["csv_read"]
    geom_expr, where_clause = _get_geom_expr_and_where(geom_info, skip_invalid)

    bounds_query = f"""
        SELECT
            MIN(ST_XMin({geom_expr})) as xmin,
            MIN(ST_YMin({geom_expr})) as ymin,
            MAX(ST_XMax({geom_expr})) as xmax,
            MAX(ST_YMax({geom_expr})) as ymax
        FROM {csv_read}
        {where_clause}
    """

    try:
        bounds_result = con.execute(bounds_query).fetchone()
    except Exception as e:
        msg = (
            "Could not calculate bounds - no valid geometries found in CSV"
            if skip_invalid
            else str(e)
        )
        raise GeoParquetError(msg) from e

    if not bounds_result or any(v is None for v in bounds_result):
        return None  # nothing to measure: caller writes unordered (#649)

    if verbose:
        xmin, ymin, xmax, ymax = bounds_result
        debug(f"Dataset bounds: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")

    return bounds_result


def _build_plain_select_query(input_url, is_parquet=False, is_csv=False, delimiter=None):
    """Build a SELECT * query for non-geometry file conversion.

    Args:
        input_url: A RAW path or URL, escaped here by ``sql_path()``; not a
            raw path -- this function writes the surrounding quotes but does not
            escape. (A raw path would need ``sql_path()`` instead; this branch
            has not been migrated, see #718.)
        is_parquet: True if input is a parquet file
        is_csv: True if input is a CSV/TSV file
        delimiter: CSV delimiter (only used if is_csv=True)

    Returns:
        SQL SELECT query string
    """
    if is_parquet:
        return f"SELECT * FROM read_parquet({sql_path(input_url)})"
    if is_csv:
        csv_read = _build_csv_read_expr(input_url, delimiter)
        return f"SELECT * FROM {csv_read}"
    # Spatial formats (GeoJSON, Shapefile, GeoPackage, etc.) - use ST_Read
    return f"SELECT * FROM ST_Read({sql_path(input_url)})"


#: Warned when Hilbert ordering is skipped for want of an envelope (#649). Both
#: causes land here — a source with no rows at all, and one where every geometry
#: is empty or NULL — so the wording has to cover both.
_NO_BOUNDS_WARNING = (
    "No geometry to measure (no rows, or every geometry empty or NULL); "
    "writing without Hilbert ordering."
)


def _orderable_geom(geom_expr, xmin, ymin):
    """``geom_expr`` with empty/NULL geometry swapped for a keyable point.

    ``ST_Hilbert`` raises on empty geometries (issue #649), so a single
    ``POLYGON EMPTY`` used to fail an entire conversion — while ``gpio sort
    hilbert`` has always ordered the non-empty rows and appended the rest. The
    substitute is a corner of the dataset envelope, so it is always in range,
    and its key never matters: callers sort on an "unorderable" flag first,
    which pins those rows last exactly where NULL geometry already landed
    under DuckDB's NULLS LAST default.

    ``ST_Hilbert`` itself is evaluated for every row rather than from inside a
    branch. The substitution does put ``ST_Point`` in a THEN branch, but with
    constant arguments and over an already-decoded GEOMETRY, not the raw-blob
    ``IS NULL`` shape that misaligned the selection vector in #642 (see
    ``core/geometry_repair.py``); a 200k-row source with NULL and empty rows
    goes through both the ST_Read and parquet paths without incident.
    """
    return (
        f"CASE WHEN {geom_expr} IS NULL OR ST_IsEmpty({geom_expr}) "
        f"THEN ST_Point({xmin}, {ymin}) ELSE {geom_expr} END"
    )


def _build_conversion_query(
    input_file,
    geom_column,
    skip_hilbert,
    bounds=None,
    is_parquet=False,
    layer=None,
    skip_bbox=False,
    existing_bbox_col=None,
    preserve_existing_bbox=False,
    encoding="WKB",
    table_expr=None,
):
    """Build SQL query for conversion with optional Hilbert ordering.

    Args:
        input_file: Path to input file
        geom_column: Name of geometry column
        skip_hilbert: Skip Hilbert ordering
        bounds: Tuple of (xmin, ymin, xmax, ymax) for Hilbert ordering
        is_parquet: Whether input is a parquet file
        layer: Layer name for multi-layer formats (GeoPackage, FileGDB)
        skip_bbox: Skip adding bbox column (for 2.0/parquet-geo-only)
        existing_bbox_col: Name of existing bbox column to remove (for parquet input)
        preserve_existing_bbox: If True, keep existing bbox column instead of adding new one
        encoding: GeoParquet geometry encoding (e.g. "WKB", "multipolygon")
        table_expr: Explicit source expression overriding the input_file read
            (used for the linearized-curves view)
    """
    # For parquet files, read directly; for other formats use ST_Read
    if table_expr is None:
        if is_parquet:
            table_expr = f"read_parquet({sql_path(input_file)})"
        else:
            table_expr = _build_st_read_expr(input_file, layer)

    # Build exclusion list - only exclude existing bbox if needed
    # NOTE: We preserve the original geometry column name (no renaming to "geometry")
    # This fixes #328: non-standard geometry column names should be preserved
    # Quote column names to handle special characters, spaces, and reserved words
    quoted_geom = quote_identifier(geom_column)
    quoted_bbox = quote_identifier(existing_bbox_col) if existing_bbox_col else None

    geoarrow_native = encoding.lower() not in {"wkb", "wkt"}
    if geoarrow_native:
        xmin_e, ymin_e, xmax_e, ymax_e, cx_e, cy_e = _geoarrow_coord_exprs(quoted_geom, encoding)
    else:
        xmin_e = f"ST_XMin({quoted_geom})"
        ymin_e = f"ST_YMin({quoted_geom})"
        xmax_e = f"ST_XMax({quoted_geom})"
        ymax_e = f"ST_YMax({quoted_geom})"

    exclude_cols = []
    if existing_bbox_col and skip_bbox:
        # For 2.0: remove existing bbox column (not needed for native geo types)
        exclude_cols.append(quoted_bbox)

    exclude_clause = ", ".join(exclude_cols) if exclude_cols else None

    if skip_bbox:
        # For 2.0/parquet-geo-only: don't add bbox column, preserve original geometry name
        if exclude_clause:
            base_select = f"""
                SELECT * EXCLUDE ({exclude_clause})
                FROM {table_expr}
            """
        else:
            base_select = f"""
                SELECT *
                FROM {table_expr}
            """
    elif preserve_existing_bbox:
        # For 1.x with existing bbox: preserve existing bbox column, don't add new one
        base_select = f"""
            SELECT *
            FROM {table_expr}
        """
    else:
        # For 1.x without existing bbox: add bbox column, preserve original geometry name
        if existing_bbox_col:
            # Remove old bbox before adding new one
            base_select = f"""
                SELECT * EXCLUDE ({quoted_bbox}),
                    STRUCT_PACK(
                        xmin := {xmin_e},
                        ymin := {ymin_e},
                        xmax := {xmax_e},
                        ymax := {ymax_e}
                    ) AS bbox
                FROM {table_expr}
            """
        else:
            base_select = f"""
                SELECT *,
                    STRUCT_PACK(
                        xmin := {xmin_e},
                        ymin := {ymin_e},
                        xmax := {xmax_e},
                        ymax := {ymax_e}
                    ) AS bbox
                FROM {table_expr}
            """

    if skip_hilbert:
        return base_select

    xmin, ymin, xmax, ymax = bounds
    bounds_box = f"ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))"
    if geoarrow_native:
        # Native encodings key on centroid coordinates, which are NULL for a
        # geometry with no coordinates — ST_Hilbert returns NULL rather than
        # failing, so those rows only need the flag to pin them last.
        unorderable = f"{cx_e} IS NULL OR {cy_e} IS NULL"
        hilbert_expr = f"ST_Hilbert({cx_e}, {cy_e}, {bounds_box})"
    else:
        unorderable = f"{quoted_geom} IS NULL OR ST_IsEmpty({quoted_geom})"
        hilbert_expr = f"ST_Hilbert({_orderable_geom(quoted_geom, xmin, ymin)}, {bounds_box})"
    return f"""{base_select}
        ORDER BY ({unorderable}), {hilbert_expr}
    """


def _convert_csv_path(
    con,
    input_file,
    delimiter,
    wkt_column,
    lat_column,
    lon_column,
    crs,
    skip_hilbert,
    skip_invalid,
    verbose,
    geoparquet_version=None,
):
    """Handle CSV/TSV conversion path. Returns SQL query.

    When skip_invalid=True, materializes parsed geometries into a temp table
    to avoid re-evaluating TRY(ST_GeomFromText(...)) in downstream metadata
    queries. DuckDB 1.5 can segfault when ST_GeometryType() or spatial
    aggregates operate on inlined TRY() subqueries under parallel execution.
    """
    from geoparquet_io.core.common import should_skip_bbox

    # Determine if bbox should be skipped for this version
    skip_bbox = should_skip_bbox(geoparquet_version)

    geom_info = _detect_csv_geometry_column(
        con, input_file, delimiter, wkt_column, lat_column, lon_column, verbose
    )
    if geom_info is None:
        return None, None

    # Validate geometry
    if geom_info["type"] == "wkt":
        progress(f"Using WKT column: {geom_info['wkt_column']}")
        _validate_wkt_and_check_crs(
            con, geom_info["csv_read"], geom_info["wkt_column"], skip_invalid, verbose
        )
    else:  # latlon
        progress(f"Using lat/lon columns: {geom_info['lat_column']}, {geom_info['lon_column']}")
        _validate_latlon_ranges(
            con, geom_info["csv_read"], geom_info["lat_column"], geom_info["lon_column"], verbose
        )

    progress(f"Assuming CRS: {crs}")

    # Skip Hilbert if using skip_invalid
    effective_skip_hilbert = skip_hilbert or skip_invalid
    if skip_invalid and not skip_hilbert:
        warn("Note: Skipping Hilbert ordering due to --skip-invalid flag")

    # Calculate bounds if needed
    bounds = (
        None
        if effective_skip_hilbert
        else _calculate_csv_bounds(con, geom_info, skip_invalid, verbose)
    )

    if verbose:
        if skip_bbox:
            msg = "Reading CSV and creating geometries (skipping bbox for native geo types)..."
            if not effective_skip_hilbert:
                msg = "Reading CSV, creating geometries, and applying Hilbert ordering (skipping bbox)..."
        else:
            msg = "Reading CSV and creating geometries..."
            if not effective_skip_hilbert:
                msg = "Reading CSV, creating geometries, and applying Hilbert ordering..."
        debug(msg)

    if not effective_skip_hilbert and bounds is None:
        warn(_NO_BOUNDS_WARNING)
        effective_skip_hilbert = True

    query = _build_csv_conversion_query(
        geom_info, effective_skip_hilbert, bounds, skip_invalid, skip_bbox=skip_bbox
    )

    # Materialize skip_invalid queries into a temp table to avoid DuckDB <= 1.5.1
    # segfaults. TRY(ST_GeomFromText(...)) in CTE subqueries gets inlined by
    # the optimizer, causing repeated re-evaluation when downstream metadata
    # queries (ST_GeometryType, ST_XMin, etc.) wrap the query. Materializing
    # parses CSV once and eliminates the unsafe TRY() re-evaluation.
    # pyproject now floors DuckDB at 1.5.5, which does not have that bug, so
    # this full materialization is pure overhead on every supported version.
    # Left in place deliberately: removing it needs its own benchmarking.
    if skip_invalid and geom_info["type"] == "wkt":
        con.execute(f"CREATE OR REPLACE TEMP TABLE _gpio_csv_parsed AS {query}")
        query = "SELECT * FROM _gpio_csv_parsed"

    # The bbox column, when written, is computed from the geometry right here,
    # so this path can vouch for it. Report it rather than leaving a writer to
    # infer a covering from the column's name (#738).
    return query, (None if skip_bbox else "bbox")


def _bounds_with_curve_fallback(
    con,
    input_file,
    geom_column,
    verbose,
    *,
    is_parquet,
    encoding,
    table_expr,
    layer,
    linearize_curves,
    max_angle_deg,
):
    """Dataset bounds, linearizing curved sources the pre-scan cannot see.

    The GPKG pre-scan only covers local ``*.gpkg`` files, so other curved
    sources (FileGDB, a GeoPackage on S3) first reveal themselves here — the
    bounds pass is what parses every geometry. Falling back to the linearized
    view keeps ``gpio convert`` in step with the Python API instead of
    surfacing DuckDB's bare "Unsupported geometry type in WKB" (issue #643).

    Returns:
        tuple: (bounds, table_expr) — table_expr is the linearized view when
        the fallback fired, otherwise the caller's value unchanged.
    """
    kwargs = {"is_parquet": is_parquet, "encoding": encoding}
    try:
        bounds = _calculate_bounds(
            con, input_file, geom_column, verbose, table_expr=table_expr, **kwargs
        )
        return bounds, table_expr
    except duckdb.Error as e:
        already_linearized = table_expr is not None
        if (
            is_parquet
            or already_linearized
            or not linearize_curves
            or "Unsupported geometry type in WKB" not in str(e)
        ):
            raise
        if verbose:
            debug("Curved geometries detected while measuring bounds; linearizing")
        table_expr = _register_linearized_view(con, input_file, layer, geom_column, max_angle_deg)
        bounds = _calculate_bounds(
            con, input_file, geom_column, verbose, table_expr=table_expr, **kwargs
        )
        return bounds, table_expr


def _convert_spatial_path(
    con,
    input_file,
    skip_hilbert,
    verbose,
    is_parquet=False,
    layer=None,
    geoparquet_version=None,
    linearize_curves=True,
    max_angle_deg=None,
):
    """Handle standard spatial format conversion path.

    ``input_file`` is the **RAW** path throughout: the metadata and filesystem
    helpers each escape their own argument, and the SQL builders escape at the
    point of interpolation via ``sql_path``. Passing the escaped URL in was what
    made ``gpio convert geoparquet`` fail on an input path containing an
    apostrophe (issue #718).

    Returns:
        tuple: (query, geometry_info) where geometry_info contains primary/secondary columns
               and their metadata. Returns (None, None) if no geometry found.
    """
    from geoparquet_io.core.common import check_bbox_structure, should_skip_bbox

    # Use multi-geometry detection for parquet files
    if is_parquet:
        geom_info = detect_all_geometry_columns(input_file, verbose=verbose)
        geom_column = geom_info["primary"]
        secondary_columns = geom_info["secondary"]
    else:
        geom_column = _detect_geometry_column(
            con, input_file, verbose, is_parquet=False, layer=layer
        )
        secondary_columns = []
        geom_info = {
            "primary": geom_column,
            "secondary": [],
            "metadata": {geom_column: {"encoding": "WKB"}} if geom_column else {},
        }

    if geom_column is None:
        return None, None, None

    # Curved geometries cannot pass through the ST_Read-based query below;
    # detected up front (GPKG pre-scan, issue #643) they are read once via
    # the linearize path and exposed as a view the query can use instead.
    table_expr = None
    if not is_parquet:
        strategy = _choose_read_strategy(input_file, layer, linearize_curves)
        if strategy == "error":
            from geoparquet_io.core.curved_geometry import unsupported_wkb_error_message

            raise GeoParquetError(
                unsupported_wkb_error_message(input_file, layer, "curved types found in pre-scan")
            )
        if strategy == "linearized":
            if verbose:
                debug("Curved geometries detected; linearizing via keep_wkb read")
            table_expr = _register_linearized_view(
                con, input_file, layer, geom_column, max_angle_deg
            )

    # Determine if bbox should be skipped for this version
    skip_bbox = should_skip_bbox(geoparquet_version)

    # Check for existing bbox column if input is parquet
    existing_bbox_col = None
    preserve_existing_bbox = False
    bbox_info = {"has_bbox_metadata": False}
    if is_parquet:
        bbox_info = check_bbox_structure(input_file, verbose=False)
        if bbox_info["has_bbox_column"]:
            existing_bbox_col = bbox_info["bbox_column_name"]
            if skip_bbox:
                # For 2.0/parquet-geo-only: remove bbox (not needed for native geo types)
                progress(
                    f"Removing bbox column '{existing_bbox_col}' (not needed for native geo types)"
                )
            else:
                # For 1.x: preserve existing valid bbox column
                preserve_existing_bbox = True
                if verbose:
                    debug(f"Preserving existing bbox column: {existing_bbox_col}")

    geom_encoding = geom_info["metadata"].get(geom_column, {}).get("encoding", "WKB")
    bounds = None
    if not skip_hilbert:
        bounds, table_expr = _bounds_with_curve_fallback(
            con,
            input_file,
            geom_column,
            verbose,
            is_parquet=is_parquet,
            encoding=geom_encoding,
            table_expr=table_expr,
            layer=layer,
            linearize_curves=linearize_curves,
            max_angle_deg=max_angle_deg,
        )
        skip_hilbert = bounds is None
        if skip_hilbert:
            warn(_NO_BOUNDS_WARNING)

    if verbose:
        if secondary_columns:
            debug(f"Preserving secondary geometry columns: {secondary_columns}")
        if skip_bbox:
            msg = "Reading input (skipping bbox for native geo types)..."
            if not skip_hilbert:
                msg = "Pass 1: Reading input and applying Hilbert ordering (skipping bbox)..."
        elif preserve_existing_bbox:
            msg = "Reading input (preserving existing bbox)..."
            if not skip_hilbert:
                msg = "Pass 1: Reading input and applying Hilbert ordering (preserving bbox)..."
        else:
            msg = "Reading input and adding bbox column..."
            if not skip_hilbert:
                msg = "Pass 1: Reading input, adding bbox, and applying Hilbert ordering..."
        debug(msg)

    query = _build_conversion_query(
        input_file,
        geom_column,
        skip_hilbert,
        bounds,
        is_parquet=is_parquet,
        layer=layer,
        skip_bbox=skip_bbox,
        existing_bbox_col=existing_bbox_col,
        preserve_existing_bbox=preserve_existing_bbox,
        encoding=geom_encoding,
        table_expr=table_expr,
    )

    # Provenance for the covering. Two things justify declaring one: gpio
    # computed the column from the geometry in this query, or the input's own
    # metadata already declared it for the column being preserved. A column that
    # merely looks like a bbox justifies nothing (#738).
    if skip_bbox:
        bbox_covering_column = None
    elif preserve_existing_bbox:
        bbox_covering_column = existing_bbox_col if bbox_info["has_bbox_metadata"] else None
    else:
        bbox_covering_column = "bbox"

    return query, geom_info, bbox_covering_column


def read_spatial_to_arrow(
    input_file,
    *,
    verbose=False,
    wkt_column=None,
    lat_column=None,
    lon_column=None,
    delimiter=None,
    crs="EPSG:4326",
    skip_invalid=False,
    profile=None,
    geometry_column="geometry",
    layer=None,
    repair_geometry=True,
    linearize_curves=True,
    max_angle_deg=None,
):
    """
    Read a geospatial file and return an Arrow table with geometry.

    This is the core reading function used by both the Python API and CLI.
    Does NOT apply Hilbert sorting or bbox column - those are chainable operations.

    Args:
        input_file: Path to input file (GeoPackage, GeoJSON, Shapefile, CSV/TSV, etc.)
        verbose: Print detailed progress
        wkt_column: CSV/TSV only - WKT column name (auto-detected if not specified)
        lat_column: CSV/TSV only - Latitude column name (requires lon_column)
        lon_column: CSV/TSV only - Longitude column name (requires lat_column)
        delimiter: CSV/TSV only - Delimiter character (auto-detected if not specified)
        crs: CRS for CSV geometry data (default: EPSG:4326/WGS84)
        skip_invalid: Skip rows with invalid geometries instead of failing
        profile: AWS profile name for S3 operations
        geometry_column: Name for output geometry column (default: 'geometry')
        layer: Layer name for multi-layer formats (GeoPackage, FileGDB). If not specified,
               reads the first/default layer.
        repair_geometry: Repair invalid geometry with ST_MakeValid (default: True).
        linearize_curves: Stroke curved geometries (CircularString..MultiSurface)
            into their linear equivalents when DuckDB cannot parse them
            (default: True). False raises the actionable unsupported-geometry
            error instead.
        max_angle_deg: Maximum angular step per stroked arc segment in degrees
            (default: 4.0, GDAL's OGR_ARC_STEPSIZE default).

    Returns:
        tuple: (arrow_table, detected_crs_projjson, geometry_column_name)

    Raises:
        GeoParquetError: If input file not found or reading fails
    """
    _validate_max_angle(max_angle_deg)
    configure_verbose(verbose)

    # Validate profile is only used with S3
    validate_profile_for_urls(profile, input_file)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, input_file)

    # Show progress for remote files
    show_remote_read_message(input_file, verbose=False)

    # RAW path: every SQL interpolation escapes it through sql_path (#802).
    input_url = resolve_file_url(input_file, verbose)

    # Check input file type
    is_csv = _is_csv_file(input_file)
    is_parquet = _is_parquet_file(input_file)

    # Check for partitioned parquet input (not supported)
    if is_parquet and is_partition_path(input_file):
        require_single_file(input_file, "read_spatial_to_arrow")

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_file))

    # Determine CRS
    user_specified_crs = crs != "EPSG:4326"
    detected_crs = None

    try:
        if user_specified_crs:
            if not is_csv:
                raise InvalidParameterError(
                    "crs",
                    f"only valid for CSV/TSV files. "
                    f"For {os.path.splitext(input_file)[1]} files, CRS is read from the file metadata.",
                )
            detected_crs = parse_crs_string_to_projjson(crs, con)
            if verbose:
                debug(f"Using user-specified CRS: {crs}")
        elif is_csv:
            # CSV with default CRS - detected_crs stays None
            pass
        elif is_parquet:
            # RAW path: extract_crs_from_parquet escapes its own argument (#718).
            crs_from_file = extract_crs_from_parquet(input_file, verbose=verbose)
            if crs_from_file and not is_default_crs(crs_from_file):
                # Same repair-or-reject as _determine_effective_crs: the API
                # write path copies this CRS into the output verbatim (#705).
                detected_crs = normalize_projjson_crs(crs_from_file, input_file)
                if verbose:
                    debug(f"Preserving input CRS: {_format_crs_display(detected_crs)}")
        else:
            # Spatial files - detect CRS
            crs_from_file = detect_crs_from_spatial_file(input_file, con, verbose=verbose)
            if crs_from_file is None:
                if _is_geojson_file(input_file):
                    # RFC 7946: GeoJSON is always WGS84/EPSG:4326
                    if verbose:
                        debug("GeoJSON file with no detected CRS, assuming WGS84 per RFC 7946")
                else:
                    raise GeoParquetError(
                        f"No CRS found in input file: {input_file}. "
                        f"Spatial files (GeoPackage, Shapefile, GeoJSON, etc.) must have a defined CRS."
                    )
            if crs_from_file is not None and not is_default_crs(crs_from_file):
                detected_crs = normalize_projjson_crs(crs_from_file, input_file)
                if verbose:
                    debug(f"Detected input CRS: {_format_crs_display(detected_crs)}")

        # Build and execute query
        if is_csv:
            arrow_table = _read_csv_to_arrow(
                con, input_url, delimiter, wkt_column, lat_column, lon_column, skip_invalid, verbose
            )
        else:
            arrow_table = _read_spatial_to_arrow(
                con,
                input_file,
                verbose,
                is_parquet=is_parquet,
                layer=layer,
                linearize_curves=linearize_curves,
                max_angle_deg=max_angle_deg,
            )

        # No geometry found — read as plain table
        if arrow_table is None:
            if is_parquet:
                table_expr = f"read_parquet({sql_path(input_url)})"
            elif is_csv:
                table_expr = _build_csv_read_expr(input_url, delimiter)
            else:
                # Spatial formats (GeoJSON, Shapefile, GeoPackage, etc.)
                table_expr = _build_st_read_expr(input_file, layer)
            arrow_table = con.execute(f"SELECT * FROM {table_expr}").arrow().read_all()
            return arrow_table, None, None

        # Repair invalid geometry (issue #506). The geometry column is WKB-encoded
        # ("geometry") at this point; the helper preserves schema metadata.
        if arrow_table.num_rows > 0:
            arrow_table, _ = repair_arrow_table_geometry(
                arrow_table, "geometry", repair=repair_geometry
            )

        return arrow_table, detected_crs, geometry_column

    except duckdb.IOException as e:
        error_msg = str(e)
        if is_remote_url(input_file):
            hints = get_remote_error_hint(error_msg, input_file)
            raise RemoteAccessError(
                input_file, f"Failed to read remote file. {hints}. Original error: {error_msg}"
            ) from e
        raise GeoParquetError(f"Failed to read input file: {error_msg}") from e

    except duckdb.BinderException as e:
        raise GeometryError(f"Invalid geometry data: {str(e)}") from e

    except GeoParquetError:
        # Already actionable (e.g. the curved-geometry message from the
        # linearize path) — don't wrap it a second time.
        raise

    except Exception as e:
        if "Unsupported geometry type in WKB" in str(e):
            from geoparquet_io.core.curved_geometry import unsupported_wkb_error_message

            raise GeoParquetError(unsupported_wkb_error_message(input_file, layer, str(e))) from e
        raise GeoParquetError(f"Reading failed: {str(e)}") from e

    finally:
        con.close()
        # Force garbage collection to prevent segfaults/SIGABRT when reading
        # multiple layers sequentially. GDAL's internal handles may not be
        # fully released before the next connection opens.
        # See: https://github.com/geoparquet/geoparquet-io/issues/322, #401
        gc.collect()


def _read_csv_to_arrow(
    con, input_url, delimiter, wkt_column, lat_column, lon_column, skip_invalid, verbose
):
    """Read CSV/TSV to Arrow table with geometry as WKB. Returns None if no geometry."""
    geom_info = _detect_csv_geometry_column(
        con, input_url, delimiter, wkt_column, lat_column, lon_column, verbose
    )
    if geom_info is None:
        warn("No geometry columns found in CSV/TSV. Reading as plain table.")
        return None

    # Validate geometry
    if geom_info["type"] == "wkt":
        if verbose:
            progress(f"Using WKT column: {geom_info['wkt_column']}")
        _validate_wkt_and_check_crs(
            con, geom_info["csv_read"], geom_info["wkt_column"], skip_invalid, verbose
        )
    else:
        if verbose:
            progress(f"Using lat/lon columns: {geom_info['lat_column']}, {geom_info['lon_column']}")
        _validate_latlon_ranges(
            con, geom_info["csv_read"], geom_info["lat_column"], geom_info["lon_column"], verbose
        )

    csv_read = geom_info["csv_read"]

    # Build query based on geometry type
    if geom_info["type"] == "wkt":
        wkt_col = quote_identifier(geom_info["wkt_column"])
        if skip_invalid:
            # Use a CTE to evaluate TRY(ST_GeomFromText(...)) once, avoiding
            # repeated re-evaluation that can segfault in DuckDB <= 1.5.1.
            query = f"""
                WITH _parsed AS (
                    SELECT * EXCLUDE ({wkt_col}),
                           TRY(ST_GeomFromText({wkt_col})) AS _geom
                    FROM {csv_read}
                )
                SELECT * EXCLUDE (_geom),
                       ST_AsWKB(_geom) AS geometry
                FROM _parsed
                WHERE _geom IS NOT NULL
            """
        else:
            query = f"""
                SELECT * EXCLUDE ({wkt_col}),
                       ST_AsWKB(ST_GeomFromText({wkt_col})) AS geometry
                FROM {csv_read}
                WHERE {wkt_col} IS NOT NULL
            """
    else:  # latlon
        lat_col = quote_identifier(geom_info["lat_column"])
        lon_col = quote_identifier(geom_info["lon_column"])
        query = f"""
            SELECT * EXCLUDE ({lat_col}, {lon_col}),
                   ST_AsWKB(ST_Point(CAST({lon_col} AS DOUBLE), CAST({lat_col} AS DOUBLE))) AS geometry
            FROM {csv_read}
            WHERE {lat_col} IS NOT NULL AND {lon_col} IS NOT NULL
        """

    result = con.execute(query)
    return result.arrow().read_all()


def _read_spatial_to_arrow(
    con,
    input_file,
    verbose,
    is_parquet=False,
    layer=None,
    linearize_curves=True,
    max_angle_deg=None,
):
    """Read spatial file to Arrow table with geometry as WKB. Returns None if no geometry.

    ``input_file`` is RAW; every helper below either escapes its own argument or
    escapes at the SQL boundary via ``sql_path`` (issue #718).
    """
    geom_column = _detect_geometry_column(
        con, input_file, verbose, is_parquet=is_parquet, layer=layer
    )
    if geom_column is None:
        warn("No geometry column found in input file. Reading as plain table.")
        return None
    quoted_geom = quote_identifier(geom_column)

    if is_parquet:
        table_expr = f"read_parquet({sql_path(input_file)})"
    else:
        strategy = _choose_read_strategy(input_file, layer, linearize_curves)
        if strategy == "error":
            from geoparquet_io.core.curved_geometry import unsupported_wkb_error_message

            raise GeoParquetError(
                unsupported_wkb_error_message(input_file, layer, "curved types found in pre-scan")
            )
        if strategy == "linearized":
            if verbose:
                debug("Curved geometries detected; linearizing via keep_wkb read")
            return _read_spatial_linearized(con, input_file, layer, geom_column, max_angle_deg)
        table_expr = _build_st_read_expr(input_file, layer)

    # Convert geometry to WKB for geoarrow compatibility
    query = f"""
        SELECT * EXCLUDE ({quoted_geom}),
               ST_AsWKB({quoted_geom}) AS geometry
        FROM {table_expr}
    """

    try:
        result = con.execute(query)
        return result.arrow().read_all()
    except duckdb.Error as e:
        # Curved geometries (CIRCULARSTRING..MULTISURFACE) cannot pass through
        # DuckDB's GEOMETRY type; linearize them at the WKB boundary instead
        # (issue #643). The pre-scan above already caught local GeoPackages —
        # this fallback covers formats without a cheap scan (e.g. FileGDB).
        # Parquet inputs have no keep_wkb escape hatch.
        if is_parquet or not linearize_curves or "Unsupported geometry type in WKB" not in str(e):
            raise
        if verbose:
            debug("Curved geometries detected; linearizing via keep_wkb read")
        return _read_spatial_linearized(con, input_file, layer, geom_column, max_angle_deg)


#: Rows per batch for the linearized read. DuckDB's ``.arrow()`` defaults to
#: 1,000,000, which hands back most files as a single batch and defeats the
#: point of streaming. Stroking multiplies a geometry's size (an arc becomes
#: ~90 vertices), so the batch has to be small: on a 50k-row curved GeoPackage,
#: peak RSS was 503 MB at the default and 100k rows, 395 MB at 20k, 370 MB at
#: 5k, with no measurable difference in wall time. The repo's generic
#: ``DEFAULT_BATCH_SIZE`` (100k, write_strategies/arrow_streaming.py) is too
#: coarse here for that reason.
_LINEARIZE_BATCH_ROWS = 20_000


class _LinearizedRead:
    """A ``keep_wkb`` read whose curved WKB is stroked batch by batch.

    ``ST_Read(keep_wkb := true)`` hands back the raw WKB blobs untouched (the
    escape hatch DuckDB documents for geometry subtypes it cannot represent),
    and each curved blob is stroked into its linear equivalent in Python.
    Working per record batch keeps one chunk of geometry live at a time instead
    of the whole file (previously the read was materialized, then copied into a
    Python list of every blob, then rebuilt), and the geometry column keeps its
    source Arrow type — ``pa.binary()`` caps a column at 2 GB of blobs, which a
    large curved dataset can exceed.
    """

    def __init__(self, con, input_file, layer, geom_column, max_angle_deg=None):
        from geoparquet_io.core.linearize import DEFAULT_MAX_ANGLE_DEG

        self.con = con
        # RAW path: _build_st_read_expr escapes it at the SQL boundary, and the
        # error messages below quote it back to the user unmangled (#718).
        self.input_file = input_file
        self.layer = layer
        self.max_angle_deg = DEFAULT_MAX_ANGLE_DEG if max_angle_deg is None else max_angle_deg
        self._reader = self._open_reader()
        self.schema = self._reader.schema
        self.wkb_col = geom_column if geom_column in self.schema.names else "wkb_geometry"
        if self.wkb_col not in self.schema.names:
            raise GeoParquetError(
                f"keep_wkb read of {input_file} exposes no '{geom_column}' column"
            )
        self.linearized = 0
        self.arcs = 0

    def _open_reader(self):
        # A DuckDB streaming result is invalidated by the next statement on its
        # connection, and the caller runs plenty (the batches are inserted into
        # a temp relation as they arrive), so the read gets its own cursor. The
        # cursor is kept alive on self: dropping it would close the reader.
        #
        # GLOBAL settings survive the cursor — including arrow_large_buffer_size,
        # which is what makes DuckDB hand back large_binary blobs and lets this
        # path exceed the 2 GB a pa.binary() column can address. Connection-level
        # spatial settings (axis order) would not carry over, but the raw
        # keep_wkb read does not consult them.
        self._cursor = self.con.cursor()
        return self._cursor.execute(
            f"SELECT * FROM {_build_st_read_expr(self.input_file, self.layer, keep_wkb=True)}"
        ).arrow(rows_per_batch=_LINEARIZE_BATCH_ROWS)

    def batches(self):
        """Yield the source's record batches with curved WKB stroked in place."""
        import pyarrow as pa

        idx = self.schema.get_field_index(self.wkb_col)
        wkb_type = self.schema.field(idx).type
        for batch in self._reader:
            columns = list(batch.columns)
            columns[idx] = pa.array(
                [self._linearize(value) for value in batch.column(idx).to_pylist()],
                type=wkb_type,
            )
            yield pa.RecordBatch.from_arrays(columns, schema=batch.schema)
        self._report()

    def _linearize(self, value):
        from geoparquet_io.core.curved_geometry import unsupported_wkb_error_message
        from geoparquet_io.core.linearize import (
            LinearizeError,
            contains_curved_wkb,
            linearize_wkb_stats,
        )

        if value is None or not contains_curved_wkb(value):
            return value  # NULL, or linear at the top level: keep the bytes as-is
        try:
            linear, changed, arcs = linearize_wkb_stats(value, self.max_angle_deg)
        except LinearizeError as e:
            # e.g. the surface family (POLYHEDRALSURFACE/TIN/TRIANGLE) or
            # malformed blobs: fall back to the actionable error (#643).
            raise GeoParquetError(
                unsupported_wkb_error_message(self.input_file, self.layer, str(e))
            ) from e
        self.linearized += changed
        self.arcs += arcs
        return linear

    def _report(self):
        if not self.linearized:
            return
        warn(
            f"Linearized {self.linearized} curved "
            f"geometr{'y' if self.linearized == 1 else 'ies'} "
            f"({self.arcs} arc{'' if self.arcs == 1 else 's'} stroked at <= "
            f"{self.max_angle_deg} degrees per segment); GeoParquet cannot "
            "represent curves."
        )


def _read_spatial_linearized(con, input_file, layer, geom_column, max_angle_deg=None, read=None):
    """Linearized read shaped like the normal read path (WKB `geometry` column).

    ``input_file`` is a RAW path (see :class:`_LinearizedRead`).
    """
    import pyarrow as pa

    read = read or _LinearizedRead(con, input_file, layer, geom_column, max_angle_deg)
    table = pa.Table.from_batches(list(read.batches()), schema=read.schema)

    # Round-trip through DuckDB: validates the stroked WKB and yields the same
    # WKB-encoded `geometry` column shape as the normal read path.
    con.register("_gpio_linearized_src", table)
    quoted_wkb = quote_identifier(read.wkb_col)
    result = con.execute(
        f"SELECT * EXCLUDE ({quoted_wkb}), "
        f"ST_AsWKB(ST_GeomFromWKB({quoted_wkb})) AS geometry "
        f"FROM _gpio_linearized_src"
    )
    return result.arrow().read_all()


def _register_linearized_view(con, input_file, layer, geom_column, max_angle_deg=None, read=None):
    """Stream a linearized read into a temp relation shaped like ST_Read's output.

    The relation exposes a GEOMETRY-typed column under its original name and
    position, so the query-based conversion path (bounds, bbox, Hilbert) can use
    it as a drop-in replacement for the ST_Read expression. Batches are inserted
    one at a time, so DuckDB owns the data — and can spill it to disk — instead
    of Python holding the whole file. Returns the relation name.
    """
    import pyarrow as pa

    read = read or _LinearizedRead(con, input_file, layer, geom_column, max_angle_deg)
    quoted_wkb = quote_identifier(read.wkb_col)
    select = (
        f"SELECT * REPLACE (ST_GeomFromWKB({quoted_wkb}) AS {quoted_wkb}) "
        "FROM _gpio_linearized_batch"
    )
    con.execute("DROP TABLE IF EXISTS _gpio_linearized")

    created = False
    for batch in read.batches():
        con.register("_gpio_linearized_batch", pa.Table.from_batches([batch], schema=read.schema))
        if created:
            con.execute(f"INSERT INTO _gpio_linearized {select}")
        else:
            con.execute(f"CREATE TEMP TABLE _gpio_linearized AS {select}")
            created = True
        con.unregister("_gpio_linearized_batch")

    if not created:
        # A source with no batches at all still needs a relation of the right
        # shape for the conversion query to select from.
        con.register("_gpio_linearized_batch", read.schema.empty_table())
        con.execute(f"CREATE TEMP TABLE _gpio_linearized AS {select}")
        con.unregister("_gpio_linearized_batch")
    return "_gpio_linearized"


def _determine_effective_crs(
    input_file: str,
    crs: str,
    is_csv: bool,
    is_parquet: bool,
    con,
    verbose: bool,
) -> dict | None:
    """Determine the effective CRS for output based on input file type."""
    user_specified_crs = crs != "EPSG:4326"

    if user_specified_crs:
        if not is_csv:
            raise InvalidParameterError(
                "crs",
                f"only valid for CSV/TSV files. "
                f"For {os.path.splitext(input_file)[1]} files, CRS is read from the file metadata.",
            )
        if verbose:
            debug(f"Using user-specified CRS: {crs}")
        return parse_crs_string_to_projjson(crs, con)

    if is_csv:
        return None  # CSV with default CRS

    if is_parquet:
        # RAW path: extract_crs_from_parquet escapes its own argument (#718).
        detected = extract_crs_from_parquet(input_file, verbose=verbose)
        if not detected or is_default_crs(detected):
            return None
        # Repair or reject before writing: an input CRS that is not valid
        # PROJJSON would otherwise be copied verbatim into the output (#705).
        # After the default check, so an id-only default CRS (which gpio itself
        # synthesizes for some drivers) is skipped, not "repaired" and warned on.
        detected = normalize_projjson_crs(detected, input_file)
        if verbose:
            debug(f"Preserving input CRS: {_format_crs_display(detected)}")
        return detected

    # Spatial files (GPKG, GeoJSON, Shapefile) - CRS must be present
    detected = detect_crs_from_spatial_file(input_file, con, verbose=verbose)
    if detected is None:
        if _is_geojson_file(input_file):
            # RFC 7946: GeoJSON is always WGS84/EPSG:4326
            if verbose:
                debug("GeoJSON file with no detected CRS, assuming WGS84 per RFC 7946")
            return None
        raise GeoParquetError(
            f"No CRS found in input file: {input_file}. "
            f"Spatial files (GeoPackage, Shapefile, GeoJSON, etc.) must have a defined CRS."
        )
    if is_default_crs(detected):
        if verbose:
            debug("Input has default CRS (WGS84), not writing explicit CRS")
        return None

    detected = normalize_projjson_crs(detected, input_file)
    if verbose:
        debug(f"Detected input CRS: {_format_crs_display(detected)}")
    return detected


def _validate_output_metadata(output_file: str):
    """Cheap metadata-level validation of the final output; None if it can't run."""
    from geoparquet_io.core.validate import validate_geoparquet

    try:
        return validate_geoparquet(output_file, validate_data=False)
    except Exception as e:  # never let the report step fail the conversion
        debug(f"Post-conversion validation could not run: {e}")
        return None


def _report_conversion_results(output_file: str, start_time: float, is_geo: bool = True) -> None:
    """Report conversion results with timing and file size."""
    elapsed = time.time() - start_time
    if is_remote_url(output_file):
        file_size = None
    else:
        file_size = os.path.getsize(output_file)

    progress(f"Done in {elapsed:.1f}s")
    if file_size is not None:
        progress(f"Output: {output_file} ({format_size(file_size)})")
    else:
        progress(f"Output: {output_file}")

    if not is_geo:
        success("✓ Converted to optimized Parquet (no geometry)")
        return

    # Only claim a validation pass after actually validating the final output
    # (post any metadata repairs). Remote outputs skip the extra fetch.
    result = None if is_remote_url(output_file) else _validate_output_metadata(output_file)
    if result is None:
        progress("Conversion complete (run 'gpio check spec' to validate)")
    elif result.is_valid:
        success("✓ Output passes GeoParquet validation (metadata checks)")
    else:
        warn(
            "Output did not pass GeoParquet metadata validation — run 'gpio check spec' for details"
        )


def convert_to_geoparquet(
    input_file,
    output_file,
    skip_hilbert=False,
    verbose=False,
    compression="ZSTD",
    compression_level=15,
    row_group_rows=None,
    row_group_size_mb=None,
    wkt_column=None,
    lat_column=None,
    lon_column=None,
    delimiter=None,
    layer=None,
    crs="EPSG:4326",
    skip_invalid=False,
    allow_no_geometry=False,
    profile=None,
    geoparquet_version=None,
    repair_geometry=True,
    linearize_curves=True,
    max_angle_deg=None,
    memory_limit=None,
):
    """
    Convert vector format to optimized GeoParquet.

    Applies best practices:
    - ZSTD compression
    - 100k row groups
    - Bbox column with metadata
    - Hilbert spatial ordering (unless --skip-hilbert)
    - GeoParquet metadata (version configurable)

    Args:
        input_file: Path to input file (Shapefile, GeoJSON, GeoPackage, CSV/TSV, etc.)
        output_file: Path to output GeoParquet file
        skip_hilbert: Skip Hilbert ordering (faster, less optimal)
        verbose: Print detailed progress
        compression: Compression type (default: ZSTD)
        compression_level: Compression level (default: 15)
        row_group_rows: Rows per group (default: None)
        row_group_size_mb: Target row group size in MB (alternative to row_group_rows)
        wkt_column: CSV/TSV only - WKT column name (auto-detected if not specified)
        lat_column: CSV/TSV only - Latitude column name (requires lon_column)
        lon_column: CSV/TSV only - Longitude column name (requires lat_column)
        delimiter: CSV/TSV only - Delimiter character (auto-detected if not specified)
        layer: GeoPackage/FileGDB only - Layer name (reads first layer if not specified)
        crs: CRS for geometry data (default: EPSG:4326/WGS84)
        skip_invalid: Skip rows with invalid geometries instead of failing
        allow_no_geometry: Allow conversion to plain Parquet if no geometry detected
        profile: AWS profile name for S3 operations
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 1.1-geoarrow, 2.0, parquet-geo-only).
            1.1-geoarrow converts geometry from any input to native GeoArrow nested-coordinate
            encoding and omits the bbox column; columns with incompatible mixed types fall back to WKB.
        repair_geometry: Repair invalid geometry with ST_MakeValid (default: True).
            When False, invalid geometry is preserved and a warning reports the count.
        linearize_curves: Stroke curved geometries (CircularString..MultiSurface)
            into their linear equivalents when DuckDB cannot parse them
            (default: True, mirroring repair_geometry). Note this alters the
            geometry: arcs become line segments and a warning reports the count.
            False raises the actionable unsupported-geometry error instead.
        max_angle_deg: Maximum angular step per stroked arc segment in degrees
            (default: 4.0, GDAL's OGR_ARC_STEPSIZE default).
        memory_limit: DuckDB memory limit for the write, e.g. "2GB" (default: None,
            meaning half of available RAM).

    Raises:
        GeoParquetError: If input file not found or conversion fails
    """
    _validate_max_angle(max_angle_deg)
    configure_verbose(verbose)
    start_time = time.time()

    validate_profile_for_urls(profile, input_file, output_file)
    setup_aws_profile_if_needed(profile, input_file, output_file)
    show_remote_read_message(input_file, verbose=False)
    input_url = resolve_file_url(input_file, verbose)
    validate_output_path(output_file, verbose)

    progress(f"Converting {input_file}...")

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_file))
    is_csv = _is_csv_file(input_file)
    is_parquet = _is_parquet_file(input_file)

    if is_parquet and is_partition_path(input_file):
        require_single_file(input_file, "convert")

    try:
        # Auto version mode: preserve a parquet input's GeoParquet version
        # (and upgrade native-geo-only inputs to 2.0) instead of silently
        # falling back to the 1.1 default and stripping native types (#587).
        if geoparquet_version is None and is_parquet:
            from geoparquet_io.core.common import resolve_geoparquet_version_from_file

            geoparquet_version = resolve_geoparquet_version_from_file(input_file, verbose)
            if geoparquet_version:
                debug(f"Auto-detected GeoParquet version from input: {geoparquet_version}")
            else:
                debug("Could not detect input GeoParquet version; using writer default")

        effective_crs = _determine_effective_crs(input_file, crs, is_csv, is_parquet, con, verbose)

        if is_csv:
            query, bbox_covering_column = _convert_csv_path(
                con,
                input_url,
                delimiter,
                wkt_column,
                lat_column,
                lon_column,
                crs,
                skip_hilbert,
                skip_invalid,
                verbose,
                geoparquet_version=geoparquet_version,
            )
            geometry_info = None
        else:
            query, geometry_info, bbox_covering_column = _convert_spatial_path(
                con,
                input_file,
                skip_hilbert,
                verbose,
                is_parquet=is_parquet,
                layer=layer,
                geoparquet_version=geoparquet_version,
                linearize_curves=linearize_curves,
                max_angle_deg=max_angle_deg,
            )

        # No geometry detected — error unless explicitly allowed
        has_geometry = query is not None
        if not has_geometry:
            if not allow_no_geometry:
                raise GeoParquetError(
                    "No geometry column detected in input file. "
                    "Expected column named 'geom', 'geometry', 'wkb_geometry', or 'shape'. "
                    "Use --allow-no-geometry to convert as plain Parquet without GeoParquet metadata."
                )

            # Error if Hilbert sorting was requested but no geometry found
            if not skip_hilbert:
                raise GeoParquetError(
                    "Cannot apply Hilbert sorting - no geometry column found. "
                    "Use --skip-hilbert if you want to convert without spatial indexing."
                )

            warn(
                "No geometry column detected. "
                "Converting as plain Parquet without GeoParquet metadata."
            )
            query = _build_plain_select_query(
                input_url, is_parquet=is_parquet, is_csv=is_csv, delimiter=delimiter
            )
            geoparquet_version = "parquet-geo-only"
            effective_crs = None

        # Geometry repair (issue #506). At this point `query` exposes the geometry
        # column before WKB conversion (which happens later in the write strategy).
        # repair_query_geometry auto-detects native GEOMETRY vs WKB and skips
        # GeoArrow STRUCT encodings it cannot repair in place. It warns with the
        # invalid count (whether repairing or, on opt-out, leaving as-is).
        # ST_MakeValid never expands a geometry's envelope, so any bbox already
        # computed upstream stays correct.
        if has_geometry:
            geom_col = "geometry" if is_csv else geometry_info["primary"]
            query = repair_query_geometry(con, query, geom_col, repair=repair_geometry)

            # This convert rebuilds the output's `geo` block from the converted
            # data (`original_metadata=None` below, and at 2.0 DuckDB regenerates
            # the block outright on the plain-COPY fast path), so an input `crs`
            # that spelled out the default never reaches `apply_output_crs` and
            # its note never fired here. Emit the same note from the same helper
            # so the key does not vanish unannounced on this path alone (#844).
            note_default_crs_normalized(
                (geometry_info or {}).get("metadata", {}).get(geom_col, {}).get("crs")
            )

        # Sidecar KV payloads (fiboa, vecorel, STAC fragments) live next to the
        # 'geo' key and are rebuilt from scratch by every write strategy, so a
        # parquet→parquet convert has to hand them to the writer explicitly or
        # they vanish (#690). Only the non-geo keys travel: 'geo' is regenerated
        # from the converted data, never copied.
        preserved_kv = read_preserved_kv_metadata(input_file, verbose) if is_parquet else {}

        # A covering is declared only for a column this conversion computed, or
        # one the input's metadata already declared -- never inferred from a
        # column name by the writer. strip_unsupported_covering drops the key
        # for 1.0 output, and 2.0/parquet-geo-only carry no bbox column at all.
        custom_metadata = (
            {"covering": {"bbox": build_bbox_covering(bbox_covering_column)}}
            if has_geometry and bbox_covering_column
            else None
        )

        write_parquet_with_metadata(
            con,
            query,
            output_file,
            original_metadata=None,
            custom_metadata=custom_metadata,
            extra_kv_metadata=preserved_kv or None,
            compression=compression,
            compression_level=compression_level,
            row_group_rows=row_group_rows,
            row_group_size_mb=row_group_size_mb,
            verbose=verbose,
            profile=profile,
            geoparquet_version=geoparquet_version,
            input_crs=effective_crs,
            geometry_info=geometry_info,
            # Geography inputs: DuckDB demotes GEOGRAPHY to GEOMETRY and drops
            # the edges declaration; the shared write path restores it (#588).
            input_file=input_file if is_parquet and has_geometry else None,
            memory_limit=memory_limit,
        )

        _report_conversion_results(output_file, start_time, is_geo=has_geometry)

    except duckdb.IOException as e:
        con.close()
        error_msg = str(e)
        if is_remote_url(input_file):
            hints = get_remote_error_hint(error_msg, input_file)
            raise RemoteAccessError(
                input_file, f"Failed to read remote file. {hints}. Original error: {error_msg}"
            ) from e
        raise GeoParquetError(f"Failed to read input file: {error_msg}") from e

    except duckdb.BinderException as e:
        con.close()
        raise GeometryError(f"Invalid geometry data: {str(e)}") from e

    except OSError as e:
        con.close()
        if e.errno == 28:  # ENOSPC
            raise GeoParquetError("Not enough disk space for output file") from e
        raise GeoParquetError(f"File system error: {str(e)}") from e

    except Exception as e:
        con.close()
        if "Unsupported geometry type in WKB" in str(e):
            # Reached when nothing parsed the geometry early enough to linearize
            # it (e.g. --skip-hilbert skips the bounds pass): at least name the
            # offending types and the remedy instead of DuckDB's raw error.
            from geoparquet_io.core.curved_geometry import unsupported_wkb_error_message

            raise GeoParquetError(unsupported_wkb_error_message(input_file, layer, str(e))) from e
        raise GeoParquetError(f"Conversion failed: {str(e)}") from e

    finally:
        con.close()


if __name__ == "__main__":
    convert_to_geoparquet()
