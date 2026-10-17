"""
Writers for converting GeoParquet to various output formats.

Provides a unified interface for writing GeoParquet to multiple formats:
- GeoPackage, FlatGeobuf, Shapefile (via GDAL drivers)
- CSV with WKT (via DuckDB SQL)
- GeoJSON (via existing geojson_stream module)

All writers use DuckDB's spatial extension for maximum compatibility.
Writers handle local file output only; remote uploads are handled by the upload module.
"""

import json
import math
from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.crs_utils import (
    _extract_crs_identifier,
    extract_crs_from_parquet,
    is_default_crs,
)
from geoparquet_io.core.duckdb_utils import (
    _escape_sql_string,
    free_column_name,
    get_duckdb_connection,
    quote_identifier,
    sql_path,
    uses_aws_profile,
    validate_where_clause,
)
from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    GeoParquetError,
    InvalidParameterError,
    ValidationError,
)
from geoparquet_io.core.file_utils import resolve_file_url, validate_output_path
from geoparquet_io.core.geometry_detection import detect_parquet_geometry_column
from geoparquet_io.core.logging_config import configure_verbose, debug, progress, success, warn
from geoparquet_io.core.memory_limits import scoped_write_memory_limit
from geoparquet_io.core.remote import (
    is_remote_url,
    needs_httpfs,
    validate_profile_for_urls,
)
from geoparquet_io.core.sizing import format_size
from geoparquet_io.core.streaming import is_stdin

# Error message templates for consistency
ERROR_REMOTE_OUTPUT = "{format} output path must be local. Use upload() for cloud destinations."
ERROR_FILE_EXISTS = "{format} file already exists: {path}\nUse --overwrite to replace it."
ERROR_CONVERSION_FAILED = "Failed to create {format}: {error}"
ERROR_NO_GEOMETRY = "No geometry column found. Expected 'geometry', 'geom', or 'wkb_geometry'."
ERROR_NO_COMPATIBLE_COLUMNS = (
    "No compatible columns for {format} format. All columns are complex types (STRUCT, LIST, MAP)."
)
# `-` reaches these writers as an ordinary path and dies inside GDAL/DuckDB as
# "File not found: -", which describes neither what happened nor what to do.
# Among the converters only `convert geojson` consumes an Arrow IPC stream
# (#723, #746); the GDAL and CSV writers hand a URL to GDAL/DuckDB and have no
# stdin-consuming path (#749). The workaround materializes with `gpio extract -`
# because that command does read the stream -- `convert geoparquet` takes `-` as
# an output only, so suggesting it would hand back the very "File not found: -"
# this message exists to replace.
ERROR_STDIN_UNSUPPORTED = (
    "reading stdin ('-') is not supported for {format} output.\n"
    "Materialize the stream first:\n"
    "  gpio extract - tmp.parquet && gpio convert {command} tmp.parquet {output}"
)


def _reject_stdin_input(input_path: str, description: str, command: str, output: str) -> None:
    """Fail early and legibly when a non-streaming writer is handed ``-``."""

    if is_stdin(input_path):
        raise GeoParquetError(
            ERROR_STDIN_UNSUPPORTED.format(format=description, command=command, output=output)
        )


# Format configuration for GDAL-based writers
GDAL_FORMATS = {
    "geopackage": {
        "cli_command": "geopackage",
        "sample_output": "out.gpkg",
        "driver": "GPKG",
        "description": "GeoPackage",
        "check_overwrite": True,
        "layer_option": "LAYER_NAME",
    },
    "flatgeobuf": {
        "cli_command": "flatgeobuf",
        "sample_output": "out.fgb",
        "driver": "FlatGeobuf",
        "description": "FlatGeobuf",
        "check_overwrite": True,
        "layer_option": None,
    },
    "shapefile": {
        "cli_command": "shapefile",
        "sample_output": "out.shp",
        "driver": "ESRI Shapefile",
        "description": "Shapefile",
        "check_overwrite": True,
        "layer_option": None,
        "encoding_option": "ENCODING",
    },
}


def _get_srs_parameter(input_path: str, verbose: bool = False) -> str | None:
    """
    Extract CRS from GeoParquet and format for DuckDB GDAL SRS parameter.

    IMPORTANT: Always returns an explicit CRS string for GDAL formats.
    GDAL-based formats (Shapefile, FlatGeobuf, GeoPackage) don't have implicit
    CRS defaults - they require explicit CRS metadata to be written.

    Priority:
    1. For default CRS (None, EPSG:4326, OGC:CRS84), return "EPSG:4326"
    2. Extract EPSG code if available (e.g., "EPSG:5070")
    3. Fall back to serialized PROJJSON string

    Args:
        input_path: Path to GeoParquet file
        verbose: Whether to log debug info

    Returns:
        SRS string for GDAL (always returns a value for valid input)
    """

    crs = extract_crs_from_parquet(input_path, verbose)

    # For default CRS (None, EPSG:4326, OGC:CRS84), explicitly return EPSG:4326
    # GDAL formats don't have implicit defaults - CRS must be explicit
    # This fixes #189 (FlatGeobuf) and #190 (Shapefile) where .prj was missing
    if is_default_crs(crs):
        return "EPSG:4326"

    # Try EPSG code first (preferred format)
    epsg_info = _extract_crs_identifier(crs)
    if epsg_info:
        authority, code = epsg_info
        # Sanitize: authority should be alphanumeric, code should be int
        if authority.isalnum() and isinstance(code, int):
            return f"{authority}:{code}"  # e.g., "EPSG:5070"

    # Fallback to PROJJSON - serialize and it will be escaped later
    return json.dumps(crs)


#: The temp table a GDAL write stages its features into. COPY ... (FORMAT GDAL)
#: scans on one thread; staging first lets DuckDB read the row groups in
#: parallel (adapted from portolan-registry-qgis core/parquet_query.py,
#: Apache-2.0). The table lives and dies with the writer's own connection.
_STAGED_TABLE = "__gpio_convert_staged"

#: Single geometry type -> the multi type of its family.
_MULTI_TYPES = {"Point": "MultiPoint", "LineString": "MultiLineString", "Polygon": "MultiPolygon"}


def _normalize_bbox(
    bbox: str | Sequence[float] | None,
) -> tuple[float, float, float, float] | None:
    """Parse a bbox given as ``"xmin,ymin,xmax,ymax"`` text or four numbers."""
    if bbox is None:
        return None
    from geoparquet_io.core.extract import parse_bbox

    if not isinstance(bbox, str):
        try:
            bbox = ",".join(str(float(value)) for value in bbox)
        except (TypeError, ValueError) as e:
            raise InvalidParameterError(
                "bbox", f"expected 4 numeric values (xmin,ymin,xmax,ymax), got {bbox!r}"
            ) from e
    parsed = parse_bbox(bbox)
    if not all(math.isfinite(value) for value in parsed):
        # NaN slips past parse_bbox's reversed-coordinate check (NaN compares
        # False) and would be interpolated as the bare SQL token ``nan``.
        raise InvalidParameterError("bbox", f"coordinates must be finite, got {bbox!r}")
    return parsed


def _build_convert_filter(
    input_path: str,
    geometry_col: str | None,
    bbox: str | Sequence[float] | None,
    geometry: str | None,
    where: str | None,
    use_first_geometry: bool = False,
) -> str | None:
    """Compose the SQL filter for a filtered conversion (#1308).

    Reuses extract's covering-aware builder (:func:`core.extract.build_spatial_filter`):
    a bbox prunes on the file's bbox covering column when it has one (four-sided
    overlap on quoted identifiers) and falls back to ``ST_Intersects`` otherwise;
    ``geometry`` becomes an intersection test against the parsed WKT; ``where``
    is validated as a single filter expression. Returns the composed clause, or
    ``None`` when no filter was asked for.
    """
    if bbox is None and geometry is None and where is None:
        return None
    from geoparquet_io.core.bbox_structure import check_bbox_structure
    from geoparquet_io.core.extract import (
        _warn_if_crs_mismatch,
        build_spatial_filter,
        parse_geometry_input,
    )

    if where:
        validate_where_clause(where)
    bbox_tuple = _normalize_bbox(bbox)
    geometry_wkt = parse_geometry_input(geometry, use_first_geometry) if geometry else None
    spatial_filter = None
    if bbox_tuple or geometry_wkt:
        if not geometry_col:
            raise InvalidParameterError(
                "bbox" if bbox_tuple else "geometry",
                "spatial filtering requires a geometry column, and none was found",
            )
        bbox_info: dict = (
            dict(check_bbox_structure(input_path, verbose=False)) if bbox_tuple else {}
        )
        if bbox_tuple:
            _warn_if_crs_mismatch(bbox_tuple, input_path, geometry_col)
        spatial_filter = build_spatial_filter(bbox_tuple, geometry_wkt, bbox_info, geometry_col)
    conditions = [f"({condition})" for condition in (spatial_filter, where) if condition]
    return " AND ".join(conditions) if conditions else None


def _should_promote_to_multi(input_path: str, geometry_col: str) -> bool:
    """True when the geo metadata declares mixed single+multi types of one family.

    A GDAL layer holds one geometry type, so ``Polygon`` next to ``MultiPolygon``
    promotes to the multi type with ``ST_Multi`` (adapted from
    portolan-registry-qgis core/geoparquet.py, Apache-2.0). Mixed families, one
    declared type, or no metadata leave the geometry alone.
    """
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata

    geo = get_geo_metadata(input_path) or {}
    columns = geo.get("columns") or {}
    info = columns.get(geometry_col) or {}
    types = info.get("geometry_types") or []
    names = {t.split(" ")[0] for t in types if isinstance(t, str)}
    return len(names) > 1 and len({_MULTI_TYPES.get(name, name) for name in names}) == 1


def _gdal_select_exprs(schema: pa.Schema, geometry_col: str | None, input_path: str) -> list[str]:
    """Column expressions GDAL can hold: complex types dropped, mixed
    single/multi geometry promoted to its multi type."""
    promote = geometry_col is not None and _should_promote_to_multi(input_path, geometry_col)
    exprs = []
    for field in schema:
        if (
            pa.types.is_struct(field.type)
            or pa.types.is_list(field.type)
            or pa.types.is_map(field.type)
        ):
            continue
        ident = quote_identifier(field.name)
        if promote and field.name == geometry_col:
            exprs.append(f"ST_Multi({ident}) AS {ident}")
        else:
            exprs.append(ident)
    return exprs


def _gpkg_collision_options(column_names: list[str], geometry_col: str | None) -> list[str]:
    """``FID=``/``GEOMETRY_NAME=`` creation options that step around a source column.

    GDAL keeps the GeoPackage feature id in ``fid`` and names the written
    geometry column ``geom``; a source attribute with either name makes GDAL
    refuse the write or silently drop the attribute (donor regression,
    portolan-registry-qgis core/geoparquet.py, Apache-2.0). Only emitted on a
    collision, so the common case keeps GDAL's defaults. The generated names are
    ``fid``/``geom`` plus a numeric suffix, never raw user input.
    """
    attributes = [name for name in column_names if name != geometry_col]
    options = []
    fid = free_column_name("fid", attributes)
    if fid != "fid":
        options.append(f"FID={fid}")
    geometry_name = free_column_name("geom", [*attributes, fid])
    if geometry_name != "geom":
        options.append(f"GEOMETRY_NAME={geometry_name}")
    return options


def _drop_gpkg_geometry_column(path: str) -> None:
    """Turn the one layer of a geometry-less GeoPackage into an attribute table.

    DuckDB's GDAL writer always adds a geometry column; for a source with no
    geometry it stays NULL. Deregistering and dropping it lets GDAL and QGIS
    open the layer as a plain attribute table (adapted from
    portolan-registry-qgis core/parquet_query.py, Apache-2.0).
    """
    import sqlite3
    from contextlib import closing

    with closing(sqlite3.connect(path)) as gpkg:
        row = gpkg.execute("SELECT table_name, column_name FROM gpkg_geometry_columns").fetchone()
        if row is None:
            return
        table, column = (str(part) for part in row)
        with gpkg:
            gpkg.execute("DELETE FROM gpkg_geometry_columns WHERE table_name = ?", [table])
            gpkg.execute(
                "UPDATE gpkg_contents SET data_type = 'attributes' WHERE table_name = ?", [table]
            )
            # DROP COLUMN needs SQLite 3.35 (March 2021).
            gpkg.execute(
                f"ALTER TABLE {quote_identifier(table)} DROP COLUMN {quote_identifier(column)}"
            )


@uses_aws_profile
def write_gdal_format(
    input_path: str,
    output_path: str,
    format_name: str,
    overwrite: bool = False,
    layer_name: str = "features",
    encoding: str = "UTF-8",
    verbose: bool = False,
    profile: str | None = None,
    bbox: str | Sequence[float] | None = None,
    geometry: str | None = None,
    where: str | None = None,
    use_first_geometry: bool = False,
) -> str:
    """
    Write GeoParquet to a GDAL-supported format via DuckDB.

    Unified implementation for GeoPackage, FlatGeobuf, and Shapefile formats.

    Args:
        input_path: Path to input GeoParquet file
        output_path: Path to output file (must be local)
        format_name: Format key: 'geopackage', 'flatgeobuf', or 'shapefile'
        overwrite: Overwrite existing file if True
        layer_name: Layer name (for formats that support it)
        encoding: Character encoding (for Shapefile)
        verbose: Print verbose output
        profile: AWS profile for S3 input files
        bbox: Bounding box filter, "xmin,ymin,xmax,ymax" or four numbers.
            Prunes on the bbox covering column when the file has one.
        geometry: Geometry filter: GeoJSON, WKT, @filepath, or "-" for stdin
            (same forms as ``gpio extract --geometry``)
        where: DuckDB WHERE clause for filtering rows
        use_first_geometry: Use the first geometry when ``geometry`` is a
            FeatureCollection with several

    Returns:
        Path to output file

    Raises:
        GeoParquetError: If validation or conversion fails
    """
    configure_verbose(verbose)

    # Get format configuration
    if format_name not in GDAL_FORMATS:
        raise InvalidParameterError(
            "format_name",
            f"Unsupported GDAL format: {format_name}. Supported: {', '.join(GDAL_FORMATS.keys())}",
        )

    config = GDAL_FORMATS[format_name]

    # str(): GDAL_FORMATS mixes str/bool/None values, so mypy infers `object`.
    _reject_stdin_input(
        input_path,
        str(config["description"]),
        str(config["cli_command"]),
        str(config["sample_output"]),
    )

    # Validate inputs
    if is_remote_url(output_path):
        raise InvalidParameterError(
            "output_path", ERROR_REMOTE_OUTPUT.format(format=config["description"])
        )

    validate_profile_for_urls(profile, input_path)

    # Check if output exists
    from pathlib import Path

    output_file = Path(output_path)
    if config["check_overwrite"] and output_file.exists() and not overwrite:
        raise GeoParquetError(
            ERROR_FILE_EXISTS.format(format=config["description"], path=output_path)
        )

    validate_output_path(output_path, verbose)
    progress(f"Converting to {config['description']}: {output_path}")

    # Get DuckDB connection
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_path))

    try:
        # Raw (validated) URL for direct reads; SQL-escaped only where interpolated.
        input_url = resolve_file_url(input_path, verbose)

        # Extract CRS for SRS parameter
        srs_param = _get_srs_parameter(input_path, verbose)
        if srs_param:
            # SQL-escape the SRS parameter
            safe_srs = _escape_sql_string(srs_param)
            srs_clause = f", SRS '{safe_srs}'"
            debug(f"Setting SRS: {srs_param}")
        else:
            srs_clause = ""
            debug("No CRS metadata found or using default CRS")

        # GDAL formats don't support complex types (STRUCT, LIST, MAP), so select only compatible columns
        # Read schema to filter out incompatible columns
        # Use fsspec to support remote URLs (HTTP/HTTPS)
        import fsspec

        with fsspec.open(input_url, "rb") as f:
            pf = pq.ParquetFile(f)
            schema = pf.schema_arrow

        # Check for geometry column using GeoParquet metadata first, then name-based fallback
        geometry_col = detect_parquet_geometry_column(input_url, verbose=verbose)
        has_geometry = geometry_col is not None

        # FlatGeobuf requires geometry - fail early with a clear message
        if not has_geometry and format_name == "flatgeobuf":
            raise GeoParquetError(
                "FlatGeobuf requires geometry data but no geometry column was found. "
                "FlatGeobuf is a geospatial format that cannot store non-spatial data. "
                "Use 'gpio convert csv' for non-spatial data."
            )

        # Shapefile and GeoPackage can work without geometry, but warn the user
        if not has_geometry:
            warn(
                f"No geometry column found. The output {config['description']} will contain "
                f"attribute data only (no spatial features)."
            )

        # Build layer creation options, as RAW strings: the one escape happens
        # at the join below, so a future part cannot be appended unescaped.
        lco_parts = []
        if config.get("layer_option"):
            lco_parts.append(f"{config['layer_option']}={layer_name}")
        if config.get("encoding_option"):
            lco_parts.append(f"{config['encoding_option']}={encoding}")
        if format_name == "geopackage":
            lco_parts.extend(
                _gpkg_collision_options([field.name for field in schema], geometry_col)
            )
            if not has_geometry:
                # The placeholder geometry column is dropped below; writing it
                # without a spatial index lets it drop cleanly.
                lco_parts.append("SPATIAL_INDEX=NO")

        # The list form: DuckDB hands a bare string to GDAL as ONE key=value
        # pair, so space-separating several options silently corrupts them all.
        lco_options = ", ".join(f"'{_escape_sql_string(part)}'" for part in lco_parts)
        lco_clause = f", LAYER_CREATION_OPTIONS ({lco_options})" if lco_parts else ""

        compatible_cols = _gdal_select_exprs(schema, geometry_col, input_path)
        if not compatible_cols:
            raise GeoParquetError(ERROR_NO_COMPATIBLE_COLUMNS.format(format=config["description"]))

        where_filter = _build_convert_filter(
            input_path, geometry_col, bbox, geometry, where, use_first_geometry
        )

        # Note: DuckDB's COPY statement doesn't support parameterized paths,
        # so sql_path() supplies the quoted, escaped literal.
        source_query = (
            f"SELECT {', '.join(compatible_cols)} FROM read_parquet({sql_path(input_path)})"
        )
        if where_filter:
            source_query += f" WHERE {where_filter}"

        staging = f"CREATE OR REPLACE TEMP TABLE {_STAGED_TABLE} AS {source_query}"
        query = f"""
            COPY (SELECT * FROM {_STAGED_TABLE})
            TO {sql_path(output_path)}
            WITH (FORMAT GDAL, DRIVER '{config["driver"]}'{lco_clause}{srs_clause})
        """

        # No `--write-memory` on the export commands, so this is the
        # ceiling-based default; the export used to run at DuckDB's own
        # 80%-of-host default, blind to a container or job cgroup (#1174).
        with scoped_write_memory_limit(con, None, verbose):
            debug(f"Executing: {staging}")
            con.execute(staging)
            debug(f"Executing: {query}")
            con.execute(query)

        if format_name == "geopackage" and not has_geometry:
            _drop_gpkg_geometry_column(output_path)

        success(f"Created {config['description']}: {output_path}")
        return output_path

    except (InvalidParameterError, ValidationError):
        # Parameter problems (a reversed bbox, a multi-statement --where) are
        # the caller's to read as such, not a conversion failure.
        raise
    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower():
            raise GeoParquetError(
                ERROR_FILE_EXISTS.format(format=config["description"], path=output_path)
            ) from e
        raise GeoParquetError(
            ERROR_CONVERSION_FAILED.format(format=config["description"], error=error_msg)
        ) from e
    finally:
        con.close()


@uses_aws_profile
def write_csv(
    input_path: str,
    output_path: str,
    include_wkt: bool = True,
    include_bbox: bool = True,
    overwrite: bool = False,
    verbose: bool = False,
    profile: str | None = None,
    bbox: str | Sequence[float] | None = None,
    geometry: str | None = None,
    where: str | None = None,
    use_first_geometry: bool = False,
) -> str:
    """
    Convert GeoParquet to CSV format with optional WKT geometry.

    Converts geometry column to WKT text representation.
    Complex types (STRUCT, LIST, MAP) are JSON-encoded.

    Args:
        input_path: Path to input GeoParquet file
        output_path: Path to output CSV file (must be local)
        include_wkt: Include geometry as WKT column (default: True)
        include_bbox: Include bbox column if present (default: True)
        overwrite: Overwrite existing file if True (default: False)
        verbose: Print verbose output
        profile: AWS profile for S3 input files
        bbox: Bounding box filter, "xmin,ymin,xmax,ymax" or four numbers
        geometry: Geometry filter: GeoJSON, WKT, @filepath, or "-" for stdin
        where: DuckDB WHERE clause for filtering rows
        use_first_geometry: Use the first geometry when ``geometry`` is a
            FeatureCollection with several

    Returns:
        Path to output file

    Raises:
        GeoParquetError: If conversion fails
    """
    from pathlib import Path

    configure_verbose(verbose)

    _reject_stdin_input(input_path, "CSV", "csv", "out.csv")

    if is_remote_url(output_path):
        raise InvalidParameterError("output_path", ERROR_REMOTE_OUTPUT.format(format="CSV"))

    # Check if output exists
    output_file = Path(output_path)
    if output_file.exists() and not overwrite:
        raise GeoParquetError(ERROR_FILE_EXISTS.format(format="CSV", path=output_path))

    validate_profile_for_urls(profile, input_path)
    validate_output_path(output_path, verbose)

    progress(f"Converting to CSV: {output_path}")

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_path))

    try:
        input_url = resolve_file_url(input_path, verbose)

        # Read parquet to inspect schema
        # Use fsspec to support remote URLs (HTTP/HTTPS)
        import fsspec

        with fsspec.open(input_url, "rb") as f:
            pf = pq.ParquetFile(f)
            schema = pf.schema_arrow
            columns = [field.name for field in schema]

        # Find geometry column from GeoParquet metadata first, then name-based fallback
        geom_col = detect_parquet_geometry_column(input_url, verbose=verbose)

        if not geom_col:
            warn("No geometry column found. Converting as plain CSV without WKT.")

        # Build column list
        select_cols = []
        for col in columns:
            if geom_col and col == geom_col:
                if include_wkt:
                    select_cols.append(f"ST_AsText({quote_identifier(col)}) as wkt")
            elif col == "bbox":
                if include_bbox:
                    select_cols.append(f"to_json({quote_identifier(col)}) as bbox")
            else:
                # Check if column is complex type, JSON-encode if needed
                field = schema.field(col)
                if (
                    pa.types.is_struct(field.type)
                    or pa.types.is_list(field.type)
                    or pa.types.is_map(field.type)
                ):
                    select_cols.append(
                        f"to_json({quote_identifier(col)}) as {quote_identifier(col)}"
                    )
                else:
                    select_cols.append(quote_identifier(col))

        if not select_cols:
            raise GeoParquetError("No columns to export after filtering geometry.")

        where_filter = _build_convert_filter(
            input_path, geom_col, bbox, geometry, where, use_first_geometry
        )
        filter_clause = f"\n                WHERE {where_filter}" if where_filter else ""

        # Note: DuckDB's COPY statement doesn't support parameterized paths,
        # so sql_path() supplies the quoted, escaped literal.

        query = f"""
            COPY (
                SELECT {", ".join(select_cols)}
                FROM read_parquet({sql_path(input_path)}){filter_clause}
            )
            TO {sql_path(output_path)}
            WITH (HEADER TRUE, DELIMITER ',')
        """

        debug(f"Executing: {query}")
        with scoped_write_memory_limit(con, None, verbose):
            con.execute(query)

        success(f"Created CSV: {output_path}")
        return output_path

    except (InvalidParameterError, ValidationError):
        raise
    except Exception as e:
        raise GeoParquetError(ERROR_CONVERSION_FAILED.format(format="CSV", error=str(e))) from e
    finally:
        con.close()


@uses_aws_profile
def write_geojson(
    input_path: str,
    output_path: str,
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
    description: str | None = None,
    pretty: bool = False,
    keep_crs: bool = False,
    overwrite: bool = False,
    verbose: bool = False,
    profile: str | None = None,
    repair_geometry: bool = True,
) -> str:
    """
    Convert GeoParquet to GeoJSON format.

    Uses existing geojson_stream module for conversion.
    Automatically reprojects to WGS84 unless keep_crs is True.

    Args:
        input_path: Path to input GeoParquet file, or "-" to read an Arrow
            IPC stream from stdin (the FeatureCollection is still written
            to output_path)
        output_path: Path to output GeoJSON file (must be local)
        precision: Coordinate decimal precision (default: 7)
        write_bbox: Include bbox property for features (default: False)
        id_field: Field to use as feature 'id' member
        description: FeatureCollection description (default: None)
        pretty: Pretty-print JSON output (default: False)
        keep_crs: Keep original CRS instead of reprojecting to WGS84 (default: False)
        overwrite: Overwrite existing file if True (default: False)
        verbose: Print verbose output
        profile: AWS profile for S3 input files

    Returns:
        Path to output file

    Raises:
        GeoParquetError: If conversion fails
    """
    from pathlib import Path

    from geoparquet_io.core.geojson_stream import convert_to_geojson

    configure_verbose(verbose)

    if is_remote_url(output_path):
        raise InvalidParameterError(
            "output_path", "GeoJSON output path must be local. Use upload() for cloud destinations."
        )

    # Check if output exists
    output_file = Path(output_path)
    if output_file.exists() and not overwrite:
        raise GeoParquetError(ERROR_FILE_EXISTS.format(format="GeoJSON", path=output_path))

    validate_profile_for_urls(profile, input_path)

    # "-" is an Arrow IPC stream on stdin, not a path: there is nothing to
    # resolve or probe, and the stream can only be consumed once. Skip the
    # input inspection and let the streaming converter read it -- it writes a
    # FeatureCollection to a named output just as well, and raises its own
    # geometry-column error if the stream has none. Probing here instead
    # reported "File not found: -" about the input the user had just piped in
    # (#723).
    if not is_stdin(input_path):
        # Check if input has geometry column using GeoParquet metadata first
        input_url = resolve_file_url(input_path, verbose)
        has_geometry = detect_parquet_geometry_column(input_url, verbose=verbose) is not None

        if not has_geometry:
            # Reject GeoJSON export without geometry data
            raise GeoParquetError(
                "Cannot export to GeoJSON: no geometry column found. "
                "GeoJSON requires geometry data. Expected column named 'geom', 'geometry', 'wkb_geometry', or 'shape'. "
                "To export data without geometry, use CSV format instead: gpio convert input.parquet output.csv"
            )

    progress(f"Converting to GeoJSON: {output_path}")

    try:
        convert_to_geojson(
            input_path=input_path,
            output_path=output_path,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
            description=description,
            pretty=pretty,
            keep_crs=keep_crs,
            verbose=verbose,
            profile=profile,
            repair_geometry=repair_geometry,
        )

        success(f"Created GeoJSON: {output_path}")
        return output_path

    except Exception as e:
        raise GeoParquetError(f"Failed to create GeoJSON: {str(e)}") from e


# Convenience wrappers for specific formats
def write_geopackage(input_path: str, output_path: str, **kwargs) -> str:
    """Write GeoParquet to GeoPackage format."""
    return write_gdal_format(input_path, output_path, "geopackage", **kwargs)


def write_flatgeobuf(input_path: str, output_path: str, **kwargs) -> str:
    """Write GeoParquet to FlatGeobuf format."""
    return write_gdal_format(input_path, output_path, "flatgeobuf", **kwargs)


def write_shapefile(input_path: str, output_path: str, **kwargs) -> str:
    """Write GeoParquet to Shapefile format."""
    return write_gdal_format(input_path, output_path, "shapefile", **kwargs)


def write_format(
    input_path: str,
    output_path: str,
    format: str,
    verbose: bool = False,
    profile: str | None = None,
    **format_options,
) -> str:
    """
    Generic format writer that routes to appropriate writer function.

    Args:
        input_path: Path to input GeoParquet file
        output_path: Path to output file
        format: Output format ('geopackage', 'flatgeobuf', 'csv', 'shapefile', 'geojson')
        verbose: Print verbose output
        profile: AWS profile for S3 input files
        **format_options: Format-specific options passed to writer functions

    Returns:
        Path to output file

    Raises:
        GeoParquetError: If format is unsupported or conversion fails
    """
    format_lower = format.lower()

    if format_lower in GDAL_FORMATS:
        return write_gdal_format(
            input_path,
            output_path,
            format_lower,
            verbose=verbose,
            profile=profile,
            **format_options,
        )
    elif format_lower == "csv":
        return write_csv(
            input_path,
            output_path,
            include_wkt=format_options.get("include_wkt", True),
            include_bbox=format_options.get("include_bbox", True),
            overwrite=format_options.get("overwrite", False),
            verbose=verbose,
            profile=profile,
            bbox=format_options.get("bbox"),
            geometry=format_options.get("geometry"),
            where=format_options.get("where"),
            use_first_geometry=format_options.get("use_first_geometry", False),
        )
    elif format_lower == "geojson":
        return write_geojson(
            input_path,
            output_path,
            precision=format_options.get("precision", 7),
            write_bbox=format_options.get("write_bbox", False),
            id_field=format_options.get("id_field"),
            pretty=format_options.get("pretty", False),
            keep_crs=format_options.get("keep_crs", False),
            overwrite=format_options.get("overwrite", False),
            verbose=verbose,
            profile=profile,
        )
    else:
        supported = list(GDAL_FORMATS.keys()) + ["csv", "geojson"]
        raise InvalidParameterError(
            "format",
            f"Unsupported format: {format}. Supported formats: {', '.join(supported)}",
        )


def create_shapefile_zip(shapefile_path: str | Path, verbose: bool = False) -> Path:
    """
    Create a zip archive containing a shapefile and all its sidecar files.

    Shapefiles consist of multiple files with different extensions (.shp, .shx, .dbf, .prj, .cpg).
    This function creates a single .shp.zip archive containing all related files.

    Args:
        shapefile_path: Path to the main .shp file
        verbose: Print verbose output

    Returns:
        Path to the created .shp.zip file

    Raises:
        click.ClickException: If shapefile or required sidecars are missing
    """
    import zipfile

    configure_verbose(verbose)
    shapefile_path = Path(shapefile_path)

    if not shapefile_path.exists():
        raise FileNotFoundGeoParquetError(str(shapefile_path))

    # Shapefile extensions: .shp (main), .shx (index), .dbf (attributes) are required
    # Optional: .prj (projection), .cpg (encoding), .sbn/.sbx (spatial index)
    stem = shapefile_path.stem
    parent = shapefile_path.parent

    # Find all files with the same stem
    sidecar_files = list(parent.glob(f"{stem}.*"))

    # Filter to only shapefile-related extensions
    shapefile_extensions = {".shp", ".shx", ".dbf", ".prj", ".cpg", ".sbn", ".sbx"}
    sidecar_files = [f for f in sidecar_files if f.suffix.lower() in shapefile_extensions]

    if not sidecar_files:
        raise FileNotFoundGeoParquetError(str(shapefile_path), "no shapefile components found")

    # Verify required files exist
    required_extensions = {".shp", ".shx", ".dbf"}
    found_extensions = {f.suffix.lower() for f in sidecar_files}
    missing = required_extensions - found_extensions

    if missing:
        raise FileNotFoundGeoParquetError(
            str(shapefile_path), f"missing required shapefile components: {', '.join(missing)}"
        )

    # Create zip file
    zip_path = parent / f"{stem}.shp.zip"

    if verbose:
        debug(f"Creating shapefile archive: {zip_path}")
        debug(f"Including {len(sidecar_files)} files: {[f.name for f in sidecar_files]}")

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for sidecar in sidecar_files:
            zipf.write(sidecar, sidecar.name)
            if verbose:
                debug(f"  Added: {sidecar.name}")

    if verbose:
        zip_size = zip_path.stat().st_size
        success(f"Created shapefile archive: {zip_path} ({format_size(zip_size)})")

    return zip_path
