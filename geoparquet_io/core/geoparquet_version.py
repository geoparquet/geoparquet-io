"""
Centralized GeoParquet version handling.

All version-specific logic lives here. Other modules should NOT have
version-specific conditionals - they just pass geoparquet_version to these functions.

Version support:
- "1": GeoParquet 1.0 (DuckDB V1) - geo key only
- "1.1": GeoParquet 1.1 (current default) - geo key only, PyArrow rewrite
- "2.0": GeoParquet 2.0 (DuckDB V2) - geo key AND Parquet geo type with CRS
- "parquet_geo_only": Parquet geo type only, no geo key (DuckDB NONE)
"""

import json
from typing import Any, Optional

import click
import fsspec
import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.metadata_utils import parse_geometry_type_from_schema

# Version configuration - single source of truth
GEOPARQUET_VERSIONS = {
    "1": {
        "duckdb": "V1",
        "geo_key": True,
        "parquet_geo_type": False,
        "version_string": "1.0.0",
    },
    "1.1": {
        "duckdb": None,  # Use default, then rewrite with PyArrow
        "geo_key": True,
        "parquet_geo_type": False,
        "version_string": "1.1.0",
    },
    "2.0": {
        "duckdb": "V2",
        "geo_key": True,
        "parquet_geo_type": True,
        "version_string": "2.0.0",
    },
    "parquet_geo_only": {
        "duckdb": "NONE",
        "geo_key": False,
        "parquet_geo_type": True,
        "version_string": None,
    },
}

VALID_VERSIONS = list(GEOPARQUET_VERSIONS.keys())

# Minimum DuckDB version required for GEOPARQUET_VERSION option
MIN_DUCKDB_VERSION_FOR_V2 = (1, 4, 1)


def _parse_version(version_str: str) -> tuple:
    """Parse version string into tuple of ints."""
    parts = version_str.split(".")
    return tuple(int(p) for p in parts[:3])


def check_duckdb_version_for_geoparquet(geoparquet_version: str) -> None:
    """
    Check if DuckDB version supports the requested GeoParquet version.

    GeoParquet 2.0 and parquet_geo_only require DuckDB >= 1.4.1 for the
    GEOPARQUET_VERSION COPY option.

    Args:
        geoparquet_version: Requested GeoParquet version

    Raises:
        click.ClickException: If DuckDB version is too old for the requested version
    """
    import duckdb

    # Only v2.0 and parquet_geo_only need the GEOPARQUET_VERSION option
    if geoparquet_version not in ("2.0", "parquet_geo_only"):
        return

    current_version = _parse_version(duckdb.__version__)

    if current_version < MIN_DUCKDB_VERSION_FOR_V2:
        current_str = ".".join(str(p) for p in current_version)
        min_str = ".".join(str(p) for p in MIN_DUCKDB_VERSION_FOR_V2)
        raise click.ClickException(
            f"GeoParquet {geoparquet_version} requires DuckDB >= {min_str}, "
            f"but you have {current_str}.\n"
            f"Please upgrade DuckDB: pip install --upgrade duckdb\n"
            f"Or use --geoparquet-version 1.1 (default) for compatibility."
        )


def validate_version(geoparquet_version: str) -> None:
    """Validate that geoparquet_version is a supported value."""
    if geoparquet_version not in VALID_VERSIONS:
        raise ValueError(
            f"Invalid geoparquet_version '{geoparquet_version}'. "
            f"Must be one of: {', '.join(VALID_VERSIONS)}"
        )


def get_duckdb_version_option(geoparquet_version: str) -> Optional[str]:
    """
    Get DuckDB GEOPARQUET_VERSION option for COPY query.

    Args:
        geoparquet_version: Version string (1, 1.1, 2.0, parquet_geo_only)

    Returns:
        DuckDB option string (V1, V2, NONE) or None for default behavior
    """
    validate_version(geoparquet_version)
    return GEOPARQUET_VERSIONS[geoparquet_version]["duckdb"]


def should_write_geo_key(geoparquet_version: str) -> bool:
    """
    Whether to write the 'geo' metadata key.

    Args:
        geoparquet_version: Version string

    Returns:
        True if geo metadata key should be written
    """
    validate_version(geoparquet_version)
    return GEOPARQUET_VERSIONS[geoparquet_version]["geo_key"]


def should_write_parquet_geo_type(geoparquet_version: str) -> bool:
    """
    Whether to write Parquet Geometry/Geography type with CRS.

    Args:
        geoparquet_version: Version string

    Returns:
        True if Parquet geo type with CRS should be written
    """
    validate_version(geoparquet_version)
    return GEOPARQUET_VERSIONS[geoparquet_version]["parquet_geo_type"]


def get_version_string(geoparquet_version: str) -> Optional[str]:
    """
    Get version string for geo metadata (e.g., '1.1.0').

    Args:
        geoparquet_version: Version string

    Returns:
        Version string for metadata or None for parquet_geo_only
    """
    validate_version(geoparquet_version)
    return GEOPARQUET_VERSIONS[geoparquet_version]["version_string"]


def is_projjson(crs: Any) -> bool:
    """
    Check if CRS is a valid PROJJSON dict.

    PROJJSON is identified by having a "$schema" key pointing to a proj.org schema,
    or having required PROJJSON fields like "type" and "name".

    Args:
        crs: CRS value to check

    Returns:
        True if crs appears to be PROJJSON format
    """
    if not isinstance(crs, dict):
        return False

    # Check for $schema key (most reliable indicator)
    if "$schema" in crs:
        schema = crs["$schema"]
        if isinstance(schema, str) and "proj.org" in schema:
            return True

    # Fallback: check for typical PROJJSON structure
    # PROJJSON objects have "type" (like "GeographicCRS", "ProjectedCRS")
    # and usually "name"
    if "type" in crs and isinstance(crs.get("type"), str):
        valid_types = [
            "GeographicCRS",
            "ProjectedCRS",
            "CompoundCRS",
            "VerticalCRS",
            "EngineeringCRS",
            "BoundCRS",
            "DerivedGeographicCRS",
            "DerivedProjectedCRS",
        ]
        if crs["type"] in valid_types:
            return True

    return False


def extract_crs_from_file(
    parquet_file: str, geometry_column: Optional[str] = None, verbose: bool = False
) -> Optional[dict]:
    """
    Extract CRS from GeoParquet metadata or Parquet schema.

    Priority:
    1. GeoParquet 'geo' metadata (columns -> geometry_column -> crs)
    2. Parquet schema Geometry type (parsed from schema string)

    Args:
        parquet_file: Path to the parquet file
        geometry_column: Geometry column name (if None, uses primary_column or 'geometry')
        verbose: Whether to print verbose output

    Returns:
        PROJJSON dict if found, None if not found (defaults to OGC:CRS84)
    """
    from geoparquet_io.core.common import safe_file_url

    safe_url = safe_file_url(parquet_file, verbose=False)

    with fsspec.open(safe_url, "rb") as f:
        pf = pq.ParquetFile(f)
        metadata = pf.schema_arrow.metadata
        schema = pf.schema_arrow
        parquet_schema_str = str(pf.metadata.schema)

    # Determine geometry column name
    geom_col = geometry_column
    if not geom_col:
        # Try to get from geo metadata
        if metadata and b"geo" in metadata:
            try:
                geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
                geom_col = geo_meta.get("primary_column", "geometry")
            except (json.JSONDecodeError, KeyError):
                geom_col = "geometry"
        else:
            geom_col = "geometry"

    # Priority 1: Check GeoParquet 'geo' metadata
    if metadata and b"geo" in metadata:
        try:
            geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
            columns = geo_meta.get("columns", {})
            col_meta = columns.get(geom_col, {})
            crs = col_meta.get("crs")
            if crs:
                if verbose:
                    click.echo(f"Found CRS in GeoParquet metadata for column '{geom_col}'")
                return crs
        except (json.JSONDecodeError, KeyError):
            pass

    # Priority 2: Check Parquet schema Geometry type
    geom_details = parse_geometry_type_from_schema(geom_col, parquet_schema_str)
    if geom_details and geom_details.get("crs"):
        crs = geom_details["crs"]
        if verbose:
            click.echo(f"Found CRS in Parquet schema for column '{geom_col}'")
        return crs

    # Check field metadata as fallback
    for field in schema:
        if field.name == geom_col and field.metadata:
            for key, value in field.metadata.items():
                key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
                if "crs" in key_str.lower():
                    value_str = value.decode("utf-8") if isinstance(value, bytes) else str(value)
                    try:
                        crs = json.loads(value_str)
                        if verbose:
                            click.echo(f"Found CRS in field metadata for column '{geom_col}'")
                        return crs
                    except json.JSONDecodeError:
                        if verbose:
                            click.echo(f"Found CRS string in field metadata: {value_str}")
                        return value_str

    if verbose:
        click.echo("No CRS found - will use default (OGC:CRS84)")
    return None


def validate_crs_for_version(
    crs: Any, geoparquet_version: str, verbose: bool = False
) -> Optional[dict]:
    """
    Validate and normalize CRS for the target GeoParquet version.

    For v2.0 and parquet_geo_only:
    - If None/empty -> return None (will use default OGC:CRS84)
    - If PROJJSON dict -> return as-is
    - If EPSG string -> log warning and return None (can't embed in Parquet type)

    For v1/v1.1:
    - Pass through any format (PROJJSON or simple strings work in geo metadata)

    Args:
        crs: CRS value from input file
        geoparquet_version: Target version
        verbose: Whether to print verbose output

    Returns:
        Validated CRS (dict for PROJJSON, string for simple formats, None for default)
    """
    validate_version(geoparquet_version)

    # None/empty -> use default
    if crs is None:
        return None

    # For v1/v1.1, pass through any format
    if geoparquet_version in ("1", "1.1"):
        return crs

    # For v2.0/parquet_geo_only, CRS must be PROJJSON or None
    if is_projjson(crs):
        return crs

    # Check if it's an EPSG string or other non-PROJJSON format
    if isinstance(crs, str):
        if verbose:
            click.echo(
                click.style(
                    f"Warning: CRS '{crs}' is not PROJJSON format. "
                    f"GeoParquet {geoparquet_version} requires inline PROJJSON for non-default CRS. "
                    f"Using default CRS (OGC:CRS84) instead.",
                    fg="yellow",
                )
            )
        return None

    # Unknown format - warn and return None
    if verbose:
        click.echo(
            click.style(
                f"Warning: Unrecognized CRS format. Using default CRS (OGC:CRS84).",
                fg="yellow",
            )
        )
    return None


def build_geo_metadata(
    primary_column: str,
    columns_meta: dict,
    geoparquet_version: str,
) -> Optional[dict]:
    """
    Build complete geo metadata dict for the target version.

    Args:
        primary_column: Name of the primary geometry column
        columns_meta: Dict of column metadata (encoding, geometry_types, crs, bbox, covering, etc.)
        geoparquet_version: Target version

    Returns:
        Complete geo metadata dict, or None for parquet_geo_only (no geo key needed)
    """
    validate_version(geoparquet_version)

    # For parquet_geo_only, don't write geo key
    if not should_write_geo_key(geoparquet_version):
        return None

    version_string = get_version_string(geoparquet_version)

    geo_meta = {
        "version": version_string,
        "primary_column": primary_column,
        "columns": columns_meta,
    }

    return geo_meta


def apply_parquet_geo_type_crs(
    schema: pa.Schema,
    geometry_column: str,
    crs: Optional[dict],
    geoparquet_version: str,
) -> pa.Schema:
    """
    Apply CRS to Parquet Geometry type in schema (for v2.0/parquet_geo_only).

    For v1/v1.1, returns schema unchanged.

    For v2.0/parquet_geo_only, modifies the geometry field's metadata to include
    the CRS in the Parquet Geometry type annotation.

    Note: This is a complex operation that may require manipulating the
    Arrow schema's field metadata to encode the CRS in a way that DuckDB
    and other readers can understand.

    Args:
        schema: PyArrow schema
        geometry_column: Name of the geometry column
        crs: CRS as PROJJSON dict (or None for default)
        geoparquet_version: Target version

    Returns:
        Modified schema with CRS in geometry type (or unchanged for v1/v1.1)
    """
    validate_version(geoparquet_version)

    # For v1/v1.1, no Parquet geo type needed
    if not should_write_parquet_geo_type(geoparquet_version):
        return schema

    # For v2.0/parquet_geo_only, we need to embed CRS in the geometry column metadata
    # This is done by modifying the field's metadata
    if crs is None:
        # No CRS to embed - DuckDB already writes empty crs= which means default
        return schema

    # Find the geometry column index
    geom_idx = None
    for i, field in enumerate(schema):
        if field.name == geometry_column:
            geom_idx = i
            break

    if geom_idx is None:
        return schema

    # Get the current field
    geom_field = schema.field(geom_idx)

    # Create new metadata with CRS
    existing_metadata = geom_field.metadata or {}
    new_metadata = dict(existing_metadata)

    # Add CRS to field metadata
    # The exact format depends on how DuckDB/Arrow expects it
    # For now, we store it as a JSON string in a 'crs' key
    crs_json = json.dumps(crs)
    new_metadata[b"PARQUET:field_id"] = existing_metadata.get(b"PARQUET:field_id", b"-1")
    new_metadata[b"geo:crs"] = crs_json.encode("utf-8")

    # Create new field with updated metadata
    new_field = geom_field.with_metadata(new_metadata)

    # Rebuild schema with new field
    fields = list(schema)
    fields[geom_idx] = new_field

    return pa.schema(fields, metadata=schema.metadata)


def format_copy_options(geoparquet_version: str, compression: str) -> str:
    """
    Format DuckDB COPY options string including GEOPARQUET_VERSION.

    Args:
        geoparquet_version: Version string
        compression: Compression type (lowercase)

    Returns:
        Options string for DuckDB COPY command

    Raises:
        click.ClickException: If DuckDB version doesn't support requested GeoParquet version
    """
    validate_version(geoparquet_version)

    # Check DuckDB version supports the requested GeoParquet version
    check_duckdb_version_for_geoparquet(geoparquet_version)

    duckdb_version = get_duckdb_version_option(geoparquet_version)

    if duckdb_version:
        return f"FORMAT PARQUET, COMPRESSION '{compression}', GEOPARQUET_VERSION {duckdb_version}"
    else:
        return f"FORMAT PARQUET, COMPRESSION '{compression}'"


def should_remove_bbox_column(geoparquet_version: str) -> bool:
    """
    Whether to remove bbox column for the target version.

    GeoParquet 2.0 and parquet_geo_only use Parquet Geometry type which has
    native bounding box support, making a separate bbox column redundant.

    Args:
        geoparquet_version: Version string

    Returns:
        True if bbox column should be removed by default
    """
    validate_version(geoparquet_version)
    # Remove bbox for versions that use Parquet geo type (native bbox support)
    return GEOPARQUET_VERSIONS[geoparquet_version]["parquet_geo_type"]


def detect_bbox_column(parquet_file: str, verbose: bool = False) -> Optional[str]:
    """
    Detect bbox column from GeoParquet metadata or common column names.

    Checks:
    1. 'covering' -> 'bbox' in geo metadata
    2. Column named 'bbox' with struct type (xmin, ymin, xmax, ymax)

    Args:
        parquet_file: Path to the parquet file
        verbose: Whether to print verbose output

    Returns:
        Name of the bbox column if found, None otherwise
    """
    from geoparquet_io.core.common import safe_file_url

    safe_url = safe_file_url(parquet_file, verbose=False)

    with fsspec.open(safe_url, "rb") as f:
        pf = pq.ParquetFile(f)
        metadata = pf.schema_arrow.metadata
        schema = pf.schema_arrow

    # Check GeoParquet metadata for covering bbox
    if metadata and b"geo" in metadata:
        try:
            geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
            # Check for covering bbox metadata
            columns = geo_meta.get("columns", {})
            for col_meta in columns.values():
                covering = col_meta.get("covering", {})
                bbox = covering.get("bbox", {})
                if bbox:
                    bbox_col = None
                    # Check for xmin/ymin style
                    if "xmin" in bbox:
                        bbox_col = bbox["xmin"]
                        if isinstance(bbox_col, list) and len(bbox_col) > 0:
                            bbox_col = bbox_col[0]  # Extract column name from path
                    # Check for column key
                    elif "column" in bbox:
                        bbox_col = bbox["column"]
                    if bbox_col:
                        if verbose:
                            click.echo(f"Found bbox column in covering metadata: {bbox_col}")
                        return bbox_col
        except (json.JSONDecodeError, KeyError):
            pass

    # Fallback: check for common bbox column name with struct type
    for field in schema:
        if field.name == "bbox":
            # Check if it's a struct with bbox fields
            if pa.types.is_struct(field.type):
                field_names = [f.name for f in field.type]
                if all(name in field_names for name in ["xmin", "ymin", "xmax", "ymax"]):
                    if verbose:
                        click.echo("Found bbox column with standard struct type")
                    return "bbox"

    return None
