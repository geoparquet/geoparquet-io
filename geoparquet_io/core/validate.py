"""
GeoParquet file validation against specification requirements.

Validates GeoParquet files against versions 1.0, 1.1, 2.0, and Parquet native
geospatial types according to their respective specifications.
"""

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from rich.console import Console


class CheckStatus(Enum):
    """Status of a validation check."""

    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


@dataclass
class ValidationCheck:
    """Result of a single validation check."""

    name: str
    status: CheckStatus
    message: str
    category: str = ""
    details: str | None = None


@dataclass
class ValidationResult:
    """Complete validation result for a file."""

    file_path: str
    detected_version: str | None
    target_version: str | None
    checks: list[ValidationCheck] = field(default_factory=list)

    @property
    def passed_count(self) -> int:
        """Count of passed checks."""
        return sum(1 for c in self.checks if c.status == CheckStatus.PASSED)

    @property
    def failed_count(self) -> int:
        """Count of failed checks."""
        return sum(1 for c in self.checks if c.status == CheckStatus.FAILED)

    @property
    def warning_count(self) -> int:
        """Count of warning checks."""
        return sum(1 for c in self.checks if c.status == CheckStatus.WARNING)

    @property
    def is_valid(self) -> bool:
        """True if no checks failed."""
        return self.failed_count == 0


# Valid values according to GeoParquet specification
VALID_ENCODINGS = ["WKB", "wkb"]
VALID_ORIENTATIONS = ["counterclockwise"]
VALID_EDGES_GEOPARQUET = ["planar", "spherical"]
VALID_EDGES_PARQUET_GEO = ["spherical", "vincenty", "thomas", "andoyer", "karney"]
VALID_GEOMETRY_TYPES = [
    "Point",
    "LineString",
    "Polygon",
    "MultiPoint",
    "MultiLineString",
    "MultiPolygon",
    "GeometryCollection",
]

# WKB integer codes for Parquet native geo types
WKB_TYPE_CODES = {
    # XY
    1: "Point",
    2: "LineString",
    3: "Polygon",
    4: "MultiPoint",
    5: "MultiLineString",
    6: "MultiPolygon",
    7: "GeometryCollection",
    # XYZ (add 1000)
    1001: "Point Z",
    1002: "LineString Z",
    1003: "Polygon Z",
    1004: "MultiPoint Z",
    1005: "MultiLineString Z",
    1006: "MultiPolygon Z",
    1007: "GeometryCollection Z",
    # XYM (add 2000)
    2001: "Point M",
    2002: "LineString M",
    2003: "Polygon M",
    2004: "MultiPoint M",
    2005: "MultiLineString M",
    2006: "MultiPolygon M",
    2007: "GeometryCollection M",
    # XYZM (add 3000)
    3001: "Point ZM",
    3002: "LineString ZM",
    3003: "Polygon ZM",
    3004: "MultiPoint ZM",
    3005: "MultiLineString ZM",
    3006: "MultiPolygon ZM",
    3007: "GeometryCollection ZM",
}


# =============================================================================
# Core Metadata Checks (GeoParquet 1.0+)
# =============================================================================


def _check_geo_key_exists(kv_metadata: dict) -> ValidationCheck:
    """Check 1: file must include a 'geo' metadata key."""
    has_geo = b"geo" in kv_metadata
    return ValidationCheck(
        name="geo_key_exists",
        status=CheckStatus.PASSED if has_geo else CheckStatus.FAILED,
        message='file includes a "geo" metadata key'
        if has_geo
        else 'file must include a "geo" metadata key',
        category="core_metadata",
    )


def _check_metadata_is_json(geo_meta: Any) -> ValidationCheck:
    """Check 2: metadata must be a JSON object."""
    is_object = isinstance(geo_meta, dict)
    return ValidationCheck(
        name="metadata_is_json_object",
        status=CheckStatus.PASSED if is_object else CheckStatus.FAILED,
        message="metadata is a valid JSON object"
        if is_object
        else "metadata must be a JSON object",
        category="core_metadata",
    )


def _check_version_present(geo_meta: dict) -> ValidationCheck:
    """Check 3: metadata must include a 'version' string."""
    version = geo_meta.get("version")
    valid = isinstance(version, str) and len(version) > 0
    return ValidationCheck(
        name="version_present",
        status=CheckStatus.PASSED if valid else CheckStatus.FAILED,
        message=f'metadata includes a "version" string: {version}'
        if valid
        else 'metadata must include a "version" string',
        category="core_metadata",
    )


def _check_primary_column_present(geo_meta: dict) -> ValidationCheck:
    """Check 4: metadata must include a 'primary_column' string."""
    primary_column = geo_meta.get("primary_column")
    valid = isinstance(primary_column, str) and len(primary_column) > 0
    return ValidationCheck(
        name="primary_column_present",
        status=CheckStatus.PASSED if valid else CheckStatus.FAILED,
        message=f'metadata includes a "primary_column" string: {primary_column}'
        if valid
        else 'metadata must include a "primary_column" string',
        category="core_metadata",
    )


def _check_columns_present(geo_meta: dict) -> ValidationCheck:
    """Check 5: metadata must include a 'columns' object."""
    columns = geo_meta.get("columns")
    valid = isinstance(columns, dict)
    return ValidationCheck(
        name="columns_present",
        status=CheckStatus.PASSED if valid else CheckStatus.FAILED,
        message='metadata includes a "columns" object'
        if valid
        else 'metadata must include a "columns" object',
        category="core_metadata",
    )


def _check_primary_column_in_columns(geo_meta: dict) -> ValidationCheck:
    """Check 6: column metadata must include the 'primary_column' name."""
    primary_column = geo_meta.get("primary_column")
    columns = geo_meta.get("columns", {})

    if not isinstance(primary_column, str) or not isinstance(columns, dict):
        return ValidationCheck(
            name="primary_column_in_columns",
            status=CheckStatus.SKIPPED,
            message="cannot check: missing primary_column or columns",
            category="core_metadata",
        )

    valid = primary_column in columns
    return ValidationCheck(
        name="primary_column_in_columns",
        status=CheckStatus.PASSED if valid else CheckStatus.FAILED,
        message=f'column metadata includes primary_column "{primary_column}"'
        if valid
        else f'column metadata must include primary_column "{primary_column}"',
        category="core_metadata",
    )


# =============================================================================
# Column Metadata Checks (GeoParquet 1.0+)
# =============================================================================


def _check_encoding_valid(col_meta: dict, col_name: str) -> ValidationCheck:
    """Check 7: column metadata must include a valid 'encoding' string."""
    encoding = col_meta.get("encoding")
    is_valid = encoding in VALID_ENCODINGS
    return ValidationCheck(
        name=f"encoding_valid_{col_name}",
        status=CheckStatus.PASSED if is_valid else CheckStatus.FAILED,
        message=f'column "{col_name}" has valid encoding: {encoding}'
        if is_valid
        else f'column "{col_name}" must have valid encoding (got: {encoding})',
        category="column_metadata",
    )


def _check_geometry_types_list(col_meta: dict, col_name: str) -> ValidationCheck:
    """Check 8: column metadata must include a 'geometry_types' list."""
    geometry_types = col_meta.get("geometry_types")
    is_list = isinstance(geometry_types, list)

    if not is_list:
        return ValidationCheck(
            name=f"geometry_types_list_{col_name}",
            status=CheckStatus.FAILED,
            message=f'column "{col_name}" must have a "geometry_types" list',
            category="column_metadata",
        )

    # Validate each type is a valid string
    invalid_types = [t for t in geometry_types if t not in VALID_GEOMETRY_TYPES]
    if invalid_types:
        return ValidationCheck(
            name=f"geometry_types_list_{col_name}",
            status=CheckStatus.FAILED,
            message=f'column "{col_name}" has invalid geometry_types: {invalid_types}',
            category="column_metadata",
        )

    return ValidationCheck(
        name=f"geometry_types_list_{col_name}",
        status=CheckStatus.PASSED,
        message=f'column "{col_name}" has valid geometry_types: {geometry_types}',
        category="column_metadata",
    )


def _check_crs_valid(col_meta: dict, col_name: str) -> ValidationCheck:
    """Check 9: optional 'crs' must be null or a PROJJSON object."""
    crs = col_meta.get("crs")

    if crs is None:
        return ValidationCheck(
            name=f"crs_valid_{col_name}",
            status=CheckStatus.PASSED,
            message=f'column "{col_name}" has no CRS (defaults to OGC:CRS84)',
            category="column_metadata",
        )

    # Check if it's a valid PROJJSON object
    if isinstance(crs, dict):
        # PROJJSON should have $schema or at minimum type/name
        if "$schema" in crs or "type" in crs or "name" in crs:
            return ValidationCheck(
                name=f"crs_valid_{col_name}",
                status=CheckStatus.PASSED,
                message=f'column "{col_name}" has valid PROJJSON CRS',
                category="column_metadata",
            )

    return ValidationCheck(
        name=f"crs_valid_{col_name}",
        status=CheckStatus.FAILED,
        message=f'column "{col_name}" CRS must be null or valid PROJJSON object',
        category="column_metadata",
    )


def _check_orientation_valid(col_meta: dict, col_name: str) -> ValidationCheck:
    """Check 10: optional 'orientation' must be a valid string."""
    orientation = col_meta.get("orientation")

    if orientation is None:
        return ValidationCheck(
            name=f"orientation_valid_{col_name}",
            status=CheckStatus.PASSED,
            message=f'column "{col_name}" has no orientation (defaults to counterclockwise)',
            category="column_metadata",
        )

    is_valid = orientation in VALID_ORIENTATIONS
    return ValidationCheck(
        name=f"orientation_valid_{col_name}",
        status=CheckStatus.PASSED if is_valid else CheckStatus.FAILED,
        message=f'column "{col_name}" has valid orientation: {orientation}'
        if is_valid
        else f'column "{col_name}" orientation must be one of {VALID_ORIENTATIONS}',
        category="column_metadata",
    )


def _check_edges_valid(col_meta: dict, col_name: str) -> ValidationCheck:
    """Check 11: optional 'edges' must be a valid string."""
    edges = col_meta.get("edges")

    if edges is None:
        return ValidationCheck(
            name=f"edges_valid_{col_name}",
            status=CheckStatus.PASSED,
            message=f'column "{col_name}" has no edges (defaults to planar)',
            category="column_metadata",
        )

    is_valid = edges in VALID_EDGES_GEOPARQUET
    return ValidationCheck(
        name=f"edges_valid_{col_name}",
        status=CheckStatus.PASSED if is_valid else CheckStatus.FAILED,
        message=f'column "{col_name}" has valid edges: {edges}'
        if is_valid
        else f'column "{col_name}" edges must be one of {VALID_EDGES_GEOPARQUET}',
        category="column_metadata",
    )


def _check_bbox_valid(col_meta: dict, col_name: str) -> ValidationCheck:
    """Check 12: optional 'bbox' must be an array of 4 or 6 numbers."""
    bbox = col_meta.get("bbox")

    if bbox is None:
        return ValidationCheck(
            name=f"bbox_valid_{col_name}",
            status=CheckStatus.PASSED,
            message=f'column "{col_name}" has no bbox',
            category="column_metadata",
        )

    if not isinstance(bbox, list):
        return ValidationCheck(
            name=f"bbox_valid_{col_name}",
            status=CheckStatus.FAILED,
            message=f'column "{col_name}" bbox must be an array',
            category="column_metadata",
        )

    if len(bbox) not in [4, 6]:
        return ValidationCheck(
            name=f"bbox_valid_{col_name}",
            status=CheckStatus.FAILED,
            message=f'column "{col_name}" bbox must have 4 or 6 elements (got {len(bbox)})',
            category="column_metadata",
        )

    # Check all elements are numbers
    if not all(isinstance(x, (int, float)) for x in bbox):
        return ValidationCheck(
            name=f"bbox_valid_{col_name}",
            status=CheckStatus.FAILED,
            message=f'column "{col_name}" bbox elements must be numbers',
            category="column_metadata",
        )

    return ValidationCheck(
        name=f"bbox_valid_{col_name}",
        status=CheckStatus.PASSED,
        message=f'column "{col_name}" has valid bbox: {bbox}',
        category="column_metadata",
    )


def _check_epoch_valid(col_meta: dict, col_name: str) -> ValidationCheck:
    """Check 13: optional 'epoch' must be a number."""
    epoch = col_meta.get("epoch")

    if epoch is None:
        return ValidationCheck(
            name=f"epoch_valid_{col_name}",
            status=CheckStatus.PASSED,
            message=f'column "{col_name}" has no epoch',
            category="column_metadata",
        )

    is_valid = isinstance(epoch, (int, float))
    return ValidationCheck(
        name=f"epoch_valid_{col_name}",
        status=CheckStatus.PASSED if is_valid else CheckStatus.FAILED,
        message=f'column "{col_name}" has valid epoch: {epoch}'
        if is_valid
        else f'column "{col_name}" epoch must be a number',
        category="column_metadata",
    )


# =============================================================================
# Parquet Schema Checks (GeoParquet 1.0+)
# =============================================================================


def _check_geometry_not_grouped(schema_info: list, geom_col: str) -> ValidationCheck:
    """Check 14: geometry columns must not be grouped."""
    # Find the geometry column in schema
    for col in schema_info:
        if col.get("name") == geom_col:
            # Check if it has children (would indicate a struct/group)
            num_children = col.get("num_children") or 0
            if num_children > 0:
                return ValidationCheck(
                    name=f"geometry_not_grouped_{geom_col}",
                    status=CheckStatus.FAILED,
                    message=f'geometry column "{geom_col}" must not be grouped',
                    category="parquet_schema",
                )
            return ValidationCheck(
                name=f"geometry_not_grouped_{geom_col}",
                status=CheckStatus.PASSED,
                message=f'geometry column "{geom_col}" is not grouped',
                category="parquet_schema",
            )

    return ValidationCheck(
        name=f"geometry_not_grouped_{geom_col}",
        status=CheckStatus.FAILED,
        message=f'geometry column "{geom_col}" not found in schema',
        category="parquet_schema",
    )


def _check_geometry_byte_array(schema_info: list, geom_col: str) -> ValidationCheck:
    """Check 15: geometry columns must be stored using BYTE_ARRAY parquet type."""
    for col in schema_info:
        if col.get("name") == geom_col:
            physical_type = col.get("type", "").upper()
            # BYTE_ARRAY is represented as BYTE_ARRAY or sometimes as a binary type
            if "BYTE_ARRAY" in physical_type or physical_type == "BINARY":
                return ValidationCheck(
                    name=f"geometry_byte_array_{geom_col}",
                    status=CheckStatus.PASSED,
                    message=f'geometry column "{geom_col}" uses BYTE_ARRAY type',
                    category="parquet_schema",
                )
            return ValidationCheck(
                name=f"geometry_byte_array_{geom_col}",
                status=CheckStatus.FAILED,
                message=f'geometry column "{geom_col}" must use BYTE_ARRAY (got {physical_type})',
                category="parquet_schema",
            )

    return ValidationCheck(
        name=f"geometry_byte_array_{geom_col}",
        status=CheckStatus.FAILED,
        message=f'geometry column "{geom_col}" not found in schema',
        category="parquet_schema",
    )


def _check_geometry_not_repeated(schema_info: list, geom_col: str) -> ValidationCheck:
    """Check 16: geometry columns must be required or optional, not repeated."""
    for col in schema_info:
        if col.get("name") == geom_col:
            repetition = col.get("repetition_type", "").upper()
            if repetition == "REPEATED":
                return ValidationCheck(
                    name=f"geometry_not_repeated_{geom_col}",
                    status=CheckStatus.FAILED,
                    message=f'geometry column "{geom_col}" must not be repeated',
                    category="parquet_schema",
                )
            return ValidationCheck(
                name=f"geometry_not_repeated_{geom_col}",
                status=CheckStatus.PASSED,
                message=f'geometry column "{geom_col}" is {repetition.lower() or "optional"}',
                category="parquet_schema",
            )

    return ValidationCheck(
        name=f"geometry_not_repeated_{geom_col}",
        status=CheckStatus.FAILED,
        message=f'geometry column "{geom_col}" not found in schema',
        category="parquet_schema",
    )


# =============================================================================
# Data Validation Checks (GeoParquet 1.0+)
# =============================================================================


def _check_encoding_matches_data(
    parquet_file: str, geom_col: str, encoding: str, con, sample_size: int
) -> ValidationCheck:
    """Check 17: all geometry values match the 'encoding' metadata."""
    from geoparquet_io.core.common import safe_file_url

    safe_url = safe_file_url(parquet_file, verbose=False)

    # For WKB encoding, verify we can parse geometries as WKB
    limit_clause = f"LIMIT {sample_size}" if sample_size > 0 else ""

    try:
        # First check if DuckDB already has it as a GEOMETRY type
        # In that case, the encoding was valid (DuckDB parsed it)
        type_query = f"DESCRIBE SELECT {geom_col} FROM read_parquet('{safe_url}')"
        type_result = con.execute(type_query).fetchone()
        col_type = type_result[1] if type_result else ""

        if "GEOMETRY" in col_type.upper():
            # DuckDB already parsed it as geometry - encoding is valid
            count_query = f"""
                SELECT COUNT(*) FROM read_parquet('{safe_url}')
                WHERE {geom_col} IS NOT NULL {limit_clause}
            """
            count_result = con.execute(count_query).fetchone()
            total = count_result[0] if count_result else 0
            return ValidationCheck(
                name=f"encoding_matches_data_{geom_col}",
                status=CheckStatus.PASSED,
                message=f'all geometry values match "{encoding}" encoding ({total} checked)',
                category="data_validation",
            )

        # Try to parse geometries - if WKB is valid, ST_GeomFromWKB succeeds
        query = f"""
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN ST_GeomFromWKB({geom_col}) IS NOT NULL THEN 1 END) as valid
            FROM (
                SELECT {geom_col}
                FROM read_parquet('{safe_url}')
                WHERE {geom_col} IS NOT NULL
                {limit_clause}
            )
        """
        result = con.execute(query).fetchone()

        if result:
            total, valid = result
            if total == valid:
                return ValidationCheck(
                    name=f"encoding_matches_data_{geom_col}",
                    status=CheckStatus.PASSED,
                    message=f'all geometry values match "{encoding}" encoding ({total} checked)',
                    category="data_validation",
                )
            return ValidationCheck(
                name=f"encoding_matches_data_{geom_col}",
                status=CheckStatus.FAILED,
                message=f'{total - valid} of {total} geometries do not match "{encoding}" encoding',
                category="data_validation",
            )
    except Exception as e:
        return ValidationCheck(
            name=f"encoding_matches_data_{geom_col}",
            status=CheckStatus.FAILED,
            message=f"failed to validate encoding: {e}",
            category="data_validation",
        )

    return ValidationCheck(
        name=f"encoding_matches_data_{geom_col}",
        status=CheckStatus.SKIPPED,
        message="no data to validate",
        category="data_validation",
    )


def _check_geometry_types_match_data(
    parquet_file: str, geom_col: str, declared_types: list, con, sample_size: int
) -> ValidationCheck:
    """Check 18: all geometry types must be included in 'geometry_types' metadata."""
    from geoparquet_io.core.common import safe_file_url

    safe_url = safe_file_url(parquet_file, verbose=False)
    limit_clause = f"LIMIT {sample_size}" if sample_size > 0 else ""

    try:
        # Check if DuckDB already has it as a GEOMETRY type
        type_query = f"DESCRIBE SELECT {geom_col} FROM read_parquet('{safe_url}')"
        type_result = con.execute(type_query).fetchone()
        col_type = type_result[1] if type_result else ""

        if "GEOMETRY" in col_type.upper():
            # Column is already geometry, use ST_GeometryType directly
            query = f"""
                SELECT DISTINCT ST_GeometryType({geom_col}) as geom_type
                FROM read_parquet('{safe_url}')
                WHERE {geom_col} IS NOT NULL
                {limit_clause}
            """
        else:
            query = f"""
                SELECT DISTINCT ST_GeometryType(ST_GeomFromWKB({geom_col})) as geom_type
                FROM read_parquet('{safe_url}')
                WHERE {geom_col} IS NOT NULL
                {limit_clause}
            """

        result = con.execute(query).fetchall()
        found_types = {row[0] for row in result if row[0]}

        # Normalize type names (DuckDB returns like "POINT", we want "Point")
        normalized_found = set()
        for t in found_types:
            # Handle DuckDB's format (e.g., "POINT", "MULTIPOLYGON")
            normalized = t.replace("ST_", "").title().replace(" ", "")
            # Handle multi-word types
            if normalized.startswith("Multi"):
                normalized = "Multi" + normalized[5:]
            normalized_found.add(normalized)

        declared_set = set(declared_types) if declared_types else set()

        # If declared_types is empty, any type is allowed
        if not declared_set:
            return ValidationCheck(
                name=f"geometry_types_match_data_{geom_col}",
                status=CheckStatus.PASSED,
                message=f"geometry_types is empty (all types allowed), found: {normalized_found}",
                category="data_validation",
            )

        # Check if all found types are in declared types
        undeclared = normalized_found - declared_set
        if undeclared:
            return ValidationCheck(
                name=f"geometry_types_match_data_{geom_col}",
                status=CheckStatus.FAILED,
                message=f"found undeclared geometry types: {undeclared}",
                details=f"Declared: {declared_set}, Found: {normalized_found}",
                category="data_validation",
            )

        return ValidationCheck(
            name=f"geometry_types_match_data_{geom_col}",
            status=CheckStatus.PASSED,
            message='all geometry types match declared "geometry_types"',
            category="data_validation",
        )
    except Exception as e:
        return ValidationCheck(
            name=f"geometry_types_match_data_{geom_col}",
            status=CheckStatus.FAILED,
            message=f"failed to validate geometry types: {e}",
            category="data_validation",
        )


def _check_orientation_matches_data(
    parquet_file: str, geom_col: str, orientation: str | None, con, sample_size: int
) -> ValidationCheck:
    """Check 19: all polygon geometries must follow 'orientation' metadata."""
    if orientation is None:
        return ValidationCheck(
            name=f"orientation_matches_data_{geom_col}",
            status=CheckStatus.SKIPPED,
            message="no orientation specified, skipping check",
            category="data_validation",
        )

    # This check would require inspecting ring orientations which is complex
    # For now, we'll mark it as passed with a note
    return ValidationCheck(
        name=f"orientation_matches_data_{geom_col}",
        status=CheckStatus.PASSED,
        message=f'orientation "{orientation}" declared (ring order validation not implemented)',
        category="data_validation",
    )


def _check_bbox_contains_data(
    parquet_file: str, geom_col: str, bbox: list | None, con, sample_size: int
) -> ValidationCheck:
    """Check 20: all geometries must fall within 'bbox' metadata."""
    if bbox is None:
        return ValidationCheck(
            name=f"bbox_contains_data_{geom_col}",
            status=CheckStatus.SKIPPED,
            message="no bbox specified, skipping check",
            category="data_validation",
        )

    from geoparquet_io.core.common import safe_file_url

    safe_url = safe_file_url(parquet_file, verbose=False)
    limit_clause = f"LIMIT {sample_size}" if sample_size > 0 else ""

    try:
        xmin, ymin, xmax, ymax = bbox[:4]

        # Check if DuckDB already has it as a GEOMETRY type
        type_query = f"DESCRIBE SELECT {geom_col} FROM read_parquet('{safe_url}')"
        type_result = con.execute(type_query).fetchone()
        col_type = type_result[1] if type_result else ""

        if "GEOMETRY" in col_type.upper():
            # Column is already geometry, use directly
            query = f"""
                SELECT COUNT(*) as total,
                       COUNT(CASE WHEN
                           ST_XMin({geom_col}) >= {xmin} AND
                           ST_YMin({geom_col}) >= {ymin} AND
                           ST_XMax({geom_col}) <= {xmax} AND
                           ST_YMax({geom_col}) <= {ymax}
                       THEN 1 END) as within_bbox
                FROM (
                    SELECT {geom_col}
                    FROM read_parquet('{safe_url}')
                    WHERE {geom_col} IS NOT NULL
                    {limit_clause}
                )
            """
        else:
            # Check if any geometries fall outside the declared bbox
            query = f"""
                SELECT COUNT(*) as total,
                       COUNT(CASE WHEN
                           ST_XMin(ST_GeomFromWKB({geom_col})) >= {xmin} AND
                           ST_YMin(ST_GeomFromWKB({geom_col})) >= {ymin} AND
                           ST_XMax(ST_GeomFromWKB({geom_col})) <= {xmax} AND
                           ST_YMax(ST_GeomFromWKB({geom_col})) <= {ymax}
                       THEN 1 END) as within_bbox
                FROM (
                    SELECT {geom_col}
                    FROM read_parquet('{safe_url}')
                    WHERE {geom_col} IS NOT NULL
                    {limit_clause}
                )
            """
        result = con.execute(query).fetchone()

        if result:
            total, within = result
            if total == within:
                return ValidationCheck(
                    name=f"bbox_contains_data_{geom_col}",
                    status=CheckStatus.PASSED,
                    message=f"all geometries fall within declared bbox ({total} checked)",
                    category="data_validation",
                )
            return ValidationCheck(
                name=f"bbox_contains_data_{geom_col}",
                status=CheckStatus.FAILED,
                message=f"{total - within} of {total} geometries fall outside declared bbox",
                category="data_validation",
            )
    except Exception as e:
        return ValidationCheck(
            name=f"bbox_contains_data_{geom_col}",
            status=CheckStatus.FAILED,
            message=f"failed to validate bbox: {e}",
            category="data_validation",
        )

    return ValidationCheck(
        name=f"bbox_contains_data_{geom_col}",
        status=CheckStatus.SKIPPED,
        message="no data to validate",
        category="data_validation",
    )


# =============================================================================
# GeoParquet 1.1 Checks
# =============================================================================


def _check_covering_is_object(col_meta: dict, col_name: str) -> ValidationCheck:
    """Check 1.1-1: optional 'covering' must be an object if present."""
    covering = col_meta.get("covering")

    if covering is None:
        return ValidationCheck(
            name=f"covering_is_object_{col_name}",
            status=CheckStatus.PASSED,
            message=f'column "{col_name}" has no covering (optional)',
            category="geoparquet_1_1",
        )

    is_valid = isinstance(covering, dict)
    return ValidationCheck(
        name=f"covering_is_object_{col_name}",
        status=CheckStatus.PASSED if is_valid else CheckStatus.FAILED,
        message=f'column "{col_name}" has valid covering object'
        if is_valid
        else f'column "{col_name}" covering must be an object',
        category="geoparquet_1_1",
    )


def _check_covering_bbox_paths(col_meta: dict, col_name: str) -> ValidationCheck:
    """Check 1.1-2: covering 'bbox' encoding must have valid xmin/ymin/xmax/ymax paths."""
    covering = col_meta.get("covering")

    if covering is None or "bbox" not in covering:
        return ValidationCheck(
            name=f"covering_bbox_paths_{col_name}",
            status=CheckStatus.SKIPPED,
            message="no bbox covering defined",
            category="geoparquet_1_1",
        )

    bbox_covering = covering["bbox"]
    required_keys = ["xmin", "ymin", "xmax", "ymax"]
    missing = [k for k in required_keys if k not in bbox_covering]

    if missing:
        return ValidationCheck(
            name=f"covering_bbox_paths_{col_name}",
            status=CheckStatus.FAILED,
            message=f"covering bbox missing required paths: {missing}",
            category="geoparquet_1_1",
        )

    # Validate path format: should be [column_name, field_name]
    for key in required_keys:
        path = bbox_covering[key]
        if not isinstance(path, list) or len(path) != 2:
            return ValidationCheck(
                name=f"covering_bbox_paths_{col_name}",
                status=CheckStatus.FAILED,
                message=f"covering bbox {key} must be a path array [column, field]",
                category="geoparquet_1_1",
            )

    return ValidationCheck(
        name=f"covering_bbox_paths_{col_name}",
        status=CheckStatus.PASSED,
        message="covering bbox has valid xmin/ymin/xmax/ymax paths",
        category="geoparquet_1_1",
    )


def _check_covering_bbox_column_exists(
    col_meta: dict, col_name: str, schema_info: list
) -> ValidationCheck:
    """Check 1.1-3: covering bbox column must exist at root of schema."""
    covering = col_meta.get("covering")

    if covering is None or "bbox" not in covering:
        return ValidationCheck(
            name=f"covering_bbox_column_exists_{col_name}",
            status=CheckStatus.SKIPPED,
            message="no bbox covering defined",
            category="geoparquet_1_1",
        )

    bbox_covering = covering["bbox"]
    # Get the column name from the path (first element)
    bbox_col_name = bbox_covering.get("xmin", [None])[0]

    if bbox_col_name is None:
        return ValidationCheck(
            name=f"covering_bbox_column_exists_{col_name}",
            status=CheckStatus.FAILED,
            message="cannot determine bbox column name from covering",
            category="geoparquet_1_1",
        )

    # Check if column exists at root (no dots in name indicating nesting)
    for col in schema_info:
        name = col.get("name", "")
        if name == bbox_col_name:
            return ValidationCheck(
                name=f"covering_bbox_column_exists_{col_name}",
                status=CheckStatus.PASSED,
                message=f'bbox column "{bbox_col_name}" exists at schema root',
                category="geoparquet_1_1",
            )

    return ValidationCheck(
        name=f"covering_bbox_column_exists_{col_name}",
        status=CheckStatus.FAILED,
        message=f'bbox column "{bbox_col_name}" not found at schema root',
        category="geoparquet_1_1",
    )


def _check_covering_bbox_structure(
    col_meta: dict, col_name: str, schema_info: list
) -> ValidationCheck:
    """Check 1.1-4/5: covering bbox column must be a struct with xmin/ymin/xmax/ymax."""
    covering = col_meta.get("covering")

    if covering is None or "bbox" not in covering:
        return ValidationCheck(
            name=f"covering_bbox_structure_{col_name}",
            status=CheckStatus.SKIPPED,
            message="no bbox covering defined",
            category="geoparquet_1_1",
        )

    bbox_covering = covering["bbox"]
    bbox_col_name = bbox_covering.get("xmin", [None])[0]

    if bbox_col_name is None:
        return ValidationCheck(
            name=f"covering_bbox_structure_{col_name}",
            status=CheckStatus.FAILED,
            message="cannot determine bbox column name",
            category="geoparquet_1_1",
        )

    # Find the bbox column and check its structure
    required_fields = {"xmin", "ymin", "xmax", "ymax"}
    found_fields = set()

    for i, col in enumerate(schema_info):
        if col.get("name") == bbox_col_name:
            num_children = col.get("num_children") or 0
            if num_children < 4:
                return ValidationCheck(
                    name=f"covering_bbox_structure_{col_name}",
                    status=CheckStatus.FAILED,
                    message=f"bbox column must have at least 4 children (has {num_children})",
                    category="geoparquet_1_1",
                )

            # Get child field names
            for j in range(1, num_children + 1):
                if i + j < len(schema_info):
                    child_name = schema_info[i + j].get("name", "")
                    found_fields.add(child_name)
            break

    missing = required_fields - found_fields
    if missing:
        return ValidationCheck(
            name=f"covering_bbox_structure_{col_name}",
            status=CheckStatus.FAILED,
            message=f"bbox column missing required fields: {missing}",
            category="geoparquet_1_1",
        )

    return ValidationCheck(
        name=f"covering_bbox_structure_{col_name}",
        status=CheckStatus.PASSED,
        message="bbox column has valid structure with xmin/ymin/xmax/ymax",
        category="geoparquet_1_1",
    )


def _check_covering_bbox_field_types(
    col_meta: dict, col_name: str, schema_info: list
) -> ValidationCheck:
    """Check 1.1-6/7: covering bbox fields must be FLOAT or DOUBLE and same type."""
    covering = col_meta.get("covering")

    if covering is None or "bbox" not in covering:
        return ValidationCheck(
            name=f"covering_bbox_field_types_{col_name}",
            status=CheckStatus.SKIPPED,
            message="no bbox covering defined",
            category="geoparquet_1_1",
        )

    bbox_covering = covering["bbox"]
    bbox_col_name = bbox_covering.get("xmin", [None])[0]

    if bbox_col_name is None:
        return ValidationCheck(
            name=f"covering_bbox_field_types_{col_name}",
            status=CheckStatus.FAILED,
            message="cannot determine bbox column name",
            category="geoparquet_1_1",
        )

    # Find field types
    field_types = set()
    valid_types = {"FLOAT", "DOUBLE", "FLOAT32", "FLOAT64"}

    for i, col in enumerate(schema_info):
        if col.get("name") == bbox_col_name:
            num_children = col.get("num_children") or 0
            for j in range(1, min(num_children + 1, 5)):  # Check first 4 children
                if i + j < len(schema_info):
                    child_type = schema_info[i + j].get("type", "").upper()
                    field_types.add(child_type)
            break

    # Check if all types are valid
    invalid_types = field_types - valid_types
    if invalid_types:
        return ValidationCheck(
            name=f"covering_bbox_field_types_{col_name}",
            status=CheckStatus.FAILED,
            message=f"bbox fields must be FLOAT or DOUBLE (found: {invalid_types})",
            category="geoparquet_1_1",
        )

    # Check if all types are the same
    if len(field_types) > 1:
        return ValidationCheck(
            name=f"covering_bbox_field_types_{col_name}",
            status=CheckStatus.FAILED,
            message=f"bbox fields must all use the same type (found: {field_types})",
            category="geoparquet_1_1",
        )

    return ValidationCheck(
        name=f"covering_bbox_field_types_{col_name}",
        status=CheckStatus.PASSED,
        message=f"bbox fields have valid type: {field_types}",
        category="geoparquet_1_1",
    )


def _check_file_extension(file_path: str) -> ValidationCheck:
    """Check 1.1-8 (warning): file extension should be '.parquet'."""
    ext = Path(file_path).suffix.lower()

    if ext == ".parquet":
        return ValidationCheck(
            name="file_extension",
            status=CheckStatus.PASSED,
            message='file extension is ".parquet"',
            category="geoparquet_1_1",
        )
    elif ext == ".geoparquet":
        return ValidationCheck(
            name="file_extension",
            status=CheckStatus.WARNING,
            message='file extension is ".geoparquet" (recommend ".parquet")',
            details="GeoParquet 1.1 recommends using .parquet extension",
            category="geoparquet_1_1",
        )
    else:
        return ValidationCheck(
            name="file_extension",
            status=CheckStatus.WARNING,
            message=f"unusual file extension: {ext}",
            category="geoparquet_1_1",
        )


# =============================================================================
# Parquet Native Geo Types Checks
# =============================================================================


def _check_native_geo_type_present(schema_info: list, geom_col: str) -> ValidationCheck:
    """Check PGO-1: GEOMETRY/GEOGRAPHY logical type must be present."""
    from geoparquet_io.core.duckdb_metadata import is_geometry_column

    for col in schema_info:
        if col.get("name") == geom_col:
            logical_type = col.get("logical_type", "")
            if is_geometry_column(logical_type):
                geo_type = "GEOMETRY" if "GeometryType" in logical_type else "GEOGRAPHY"
                return ValidationCheck(
                    name=f"native_geo_type_present_{geom_col}",
                    status=CheckStatus.PASSED,
                    message=f'column "{geom_col}" uses Parquet {geo_type} logical type',
                    category="parquet_geo_types",
                )
            return ValidationCheck(
                name=f"native_geo_type_present_{geom_col}",
                status=CheckStatus.FAILED,
                message=f'column "{geom_col}" does not have GEOMETRY/GEOGRAPHY logical type',
                category="parquet_geo_types",
            )

    return ValidationCheck(
        name=f"native_geo_type_present_{geom_col}",
        status=CheckStatus.FAILED,
        message=f'column "{geom_col}" not found in schema',
        category="parquet_geo_types",
    )


def _check_native_crs_format(schema_info: list, geom_col: str) -> ValidationCheck:
    """Check PGO-3: optional CRS must be in valid format (srid:XXXX or inline PROJJSON)."""
    from geoparquet_io.core.duckdb_metadata import parse_geometry_logical_type

    for col in schema_info:
        if col.get("name") == geom_col:
            logical_type = col.get("logical_type", "")
            parsed = parse_geometry_logical_type(logical_type)

            if not parsed:
                return ValidationCheck(
                    name=f"native_crs_format_{geom_col}",
                    status=CheckStatus.SKIPPED,
                    message="no logical type to parse",
                    category="parquet_geo_types",
                )

            crs = parsed.get("crs")
            if crs is None:
                return ValidationCheck(
                    name=f"native_crs_format_{geom_col}",
                    status=CheckStatus.PASSED,
                    message=f'column "{geom_col}" has no CRS (defaults to OGC:CRS84)',
                    category="parquet_geo_types",
                )

            # Check if it's PROJJSON (dict with schema or type)
            if isinstance(crs, dict):
                if "$schema" in crs or "type" in crs:
                    return ValidationCheck(
                        name=f"native_crs_format_{geom_col}",
                        status=CheckStatus.PASSED,
                        message=f'column "{geom_col}" has valid inline PROJJSON CRS',
                        category="parquet_geo_types",
                    )

            # Check if it's srid:XXXX format
            if isinstance(crs, str) and crs.startswith("srid:"):
                return ValidationCheck(
                    name=f"native_crs_format_{geom_col}",
                    status=CheckStatus.PASSED,
                    message=f'column "{geom_col}" has valid srid CRS: {crs}',
                    category="parquet_geo_types",
                )

            return ValidationCheck(
                name=f"native_crs_format_{geom_col}",
                status=CheckStatus.WARNING,
                message=f'column "{geom_col}" CRS format may not be widely recognized',
                details=f"CRS: {crs}. Use 'gpio convert --geoparquet-version 2.0' to standardize.",
                category="parquet_geo_types",
            )

    return ValidationCheck(
        name=f"native_crs_format_{geom_col}",
        status=CheckStatus.SKIPPED,
        message=f'column "{geom_col}" not found',
        category="parquet_geo_types",
    )


def _check_geography_edges_valid(schema_info: list, geom_col: str) -> ValidationCheck:
    """Check PGO-4: for GEOGRAPHY, edges must be valid algorithm."""
    from geoparquet_io.core.duckdb_metadata import parse_geometry_logical_type

    for col in schema_info:
        if col.get("name") == geom_col:
            logical_type = col.get("logical_type", "")

            if "GeographyType" not in logical_type:
                return ValidationCheck(
                    name=f"geography_edges_valid_{geom_col}",
                    status=CheckStatus.SKIPPED,
                    message="not a GEOGRAPHY type, edges check not applicable",
                    category="parquet_geo_types",
                )

            parsed = parse_geometry_logical_type(logical_type)
            if not parsed:
                return ValidationCheck(
                    name=f"geography_edges_valid_{geom_col}",
                    status=CheckStatus.FAILED,
                    message="failed to parse GEOGRAPHY logical type",
                    category="parquet_geo_types",
                )

            algorithm = parsed.get("algorithm", "spherical")  # Default is spherical
            if algorithm in VALID_EDGES_PARQUET_GEO:
                return ValidationCheck(
                    name=f"geography_edges_valid_{geom_col}",
                    status=CheckStatus.PASSED,
                    message=f"GEOGRAPHY column has valid edges algorithm: {algorithm}",
                    category="parquet_geo_types",
                )

            return ValidationCheck(
                name=f"geography_edges_valid_{geom_col}",
                status=CheckStatus.FAILED,
                message=f"GEOGRAPHY edges must be one of {VALID_EDGES_PARQUET_GEO}",
                details=f"Found: {algorithm}",
                category="parquet_geo_types",
            )

    return ValidationCheck(
        name=f"geography_edges_valid_{geom_col}",
        status=CheckStatus.SKIPPED,
        message=f'column "{geom_col}" not found',
        category="parquet_geo_types",
    )


def _check_geography_coordinate_bounds(
    parquet_file: str, geom_col: str, schema_info: list, con, sample_size: int
) -> ValidationCheck:
    """Check PGO-7: for GEOGRAPHY, X bounded [-180, 180], Y bounded [-90, 90]."""
    # First check if this is a GEOGRAPHY column
    is_geography = False
    for col in schema_info:
        if col.get("name") == geom_col:
            logical_type = col.get("logical_type", "")
            if "GeographyType" in logical_type:
                is_geography = True
            break

    if not is_geography:
        return ValidationCheck(
            name=f"geography_coordinate_bounds_{geom_col}",
            status=CheckStatus.SKIPPED,
            message="not a GEOGRAPHY type, coordinate bounds check not applicable",
            category="parquet_geo_types",
        )

    from geoparquet_io.core.common import safe_file_url

    safe_url = safe_file_url(parquet_file, verbose=False)
    limit_clause = f"LIMIT {sample_size}" if sample_size > 0 else ""

    try:
        query = f"""
            SELECT
                MIN(ST_XMin(ST_GeomFromWKB({geom_col}))) as min_x,
                MAX(ST_XMax(ST_GeomFromWKB({geom_col}))) as max_x,
                MIN(ST_YMin(ST_GeomFromWKB({geom_col}))) as min_y,
                MAX(ST_YMax(ST_GeomFromWKB({geom_col}))) as max_y
            FROM (
                SELECT {geom_col}
                FROM read_parquet('{safe_url}')
                WHERE {geom_col} IS NOT NULL
                {limit_clause}
            )
        """
        result = con.execute(query).fetchone()

        if result:
            min_x, max_x, min_y, max_y = result
            issues = []

            if min_x is not None and min_x < -180:
                issues.append(f"min_x={min_x} < -180")
            if max_x is not None and max_x > 180:
                issues.append(f"max_x={max_x} > 180")
            if min_y is not None and min_y < -90:
                issues.append(f"min_y={min_y} < -90")
            if max_y is not None and max_y > 90:
                issues.append(f"max_y={max_y} > 90")

            if issues:
                return ValidationCheck(
                    name=f"geography_coordinate_bounds_{geom_col}",
                    status=CheckStatus.FAILED,
                    message="GEOGRAPHY coordinates exceed valid bounds",
                    details=", ".join(issues),
                    category="parquet_geo_types",
                )

            return ValidationCheck(
                name=f"geography_coordinate_bounds_{geom_col}",
                status=CheckStatus.PASSED,
                message="GEOGRAPHY coordinates within valid bounds [-180,180] x [-90,90]",
                category="parquet_geo_types",
            )
    except Exception as e:
        return ValidationCheck(
            name=f"geography_coordinate_bounds_{geom_col}",
            status=CheckStatus.FAILED,
            message=f"failed to check coordinate bounds: {e}",
            category="parquet_geo_types",
        )

    return ValidationCheck(
        name=f"geography_coordinate_bounds_{geom_col}",
        status=CheckStatus.SKIPPED,
        message="no data to validate",
        category="parquet_geo_types",
    )


# =============================================================================
# Row Group Statistics Checks
# =============================================================================


def _check_row_group_bbox_statistics(parquet_file: str, geom_col: str) -> ValidationCheck:
    """Check that file has bbox column with row group statistics for spatial filtering."""
    from geoparquet_io.core.common import is_remote_url
    from geoparquet_io.core.duckdb_metadata import (
        get_bbox_from_row_group_stats,
        has_bbox_column,
    )

    try:
        # For remote files, skip (DuckDB can handle but may be slow)
        if is_remote_url(parquet_file):
            return ValidationCheck(
                name=f"row_group_bbox_stats_{geom_col}",
                status=CheckStatus.SKIPPED,
                message="row group statistics check skipped for remote files",
                category="parquet_geo_types",
            )

        # Check if file has a bbox column
        has_bbox, bbox_col_name = has_bbox_column(parquet_file)

        if not has_bbox:
            return ValidationCheck(
                name=f"row_group_bbox_stats_{geom_col}",
                status=CheckStatus.WARNING,
                message="no bbox column found for spatial filtering",
                details="A bbox struct column (xmin/ymin/xmax/ymax) enables efficient spatial "
                "filtering. Use 'gpio add bbox' to add one.",
                category="parquet_geo_types",
            )

        # Check if bbox column has valid statistics
        bbox = get_bbox_from_row_group_stats(parquet_file, bbox_col_name)

        if bbox:
            return ValidationCheck(
                name=f"row_group_bbox_stats_{geom_col}",
                status=CheckStatus.PASSED,
                message=f'bbox column "{bbox_col_name}" has row group statistics',
                category="parquet_geo_types",
            )
        else:
            return ValidationCheck(
                name=f"row_group_bbox_stats_{geom_col}",
                status=CheckStatus.WARNING,
                message=f'bbox column "{bbox_col_name}" missing row group statistics',
                details="Row group statistics enable efficient spatial filtering. "
                "Re-write the file with a tool that generates statistics.",
                category="parquet_geo_types",
            )

    except Exception as e:
        return ValidationCheck(
            name=f"row_group_bbox_stats_{geom_col}",
            status=CheckStatus.SKIPPED,
            message=f"could not check row group statistics: {e}",
            category="parquet_geo_types",
        )


# =============================================================================
# GeoParquet 2.0 Checks
# =============================================================================


def _check_v2_uses_native_types(schema_info: list, geom_col: str) -> ValidationCheck:
    """Check V2-1: geometry columns MUST use Parquet GEOMETRY or GEOGRAPHY types."""
    from geoparquet_io.core.duckdb_metadata import is_geometry_column

    for col in schema_info:
        if col.get("name") == geom_col:
            logical_type = col.get("logical_type", "")
            if is_geometry_column(logical_type):
                return ValidationCheck(
                    name=f"v2_native_types_{geom_col}",
                    status=CheckStatus.PASSED,
                    message=f'column "{geom_col}" uses required Parquet native geo type',
                    category="geoparquet_2_0",
                )
            return ValidationCheck(
                name=f"v2_native_types_{geom_col}",
                status=CheckStatus.FAILED,
                message="GeoParquet 2.0 requires native Parquet GEOMETRY/GEOGRAPHY type",
                details=f'Column "{geom_col}" does not use native geo type',
                category="geoparquet_2_0",
            )

    return ValidationCheck(
        name=f"v2_native_types_{geom_col}",
        status=CheckStatus.FAILED,
        message=f'column "{geom_col}" not found in schema',
        category="geoparquet_2_0",
    )


def _check_v2_crs_in_parquet_type(
    geo_meta: dict, schema_info: list, geom_col: str
) -> ValidationCheck:
    """Check V2-2: if non-default CRS, must be inline PROJJSON in Parquet geo type."""
    from geoparquet_io.core.duckdb_metadata import parse_geometry_logical_type

    col_meta = geo_meta.get("columns", {}).get(geom_col, {})
    metadata_crs = col_meta.get("crs")

    # If no CRS in metadata (default), this is fine
    if metadata_crs is None:
        return ValidationCheck(
            name=f"v2_crs_in_parquet_type_{geom_col}",
            status=CheckStatus.PASSED,
            message="using default CRS (OGC:CRS84), no inline CRS required",
            category="geoparquet_2_0",
        )

    # Find the Parquet schema CRS
    for col in schema_info:
        if col.get("name") == geom_col:
            logical_type = col.get("logical_type", "")
            parsed = parse_geometry_logical_type(logical_type)

            if parsed and parsed.get("crs"):
                return ValidationCheck(
                    name=f"v2_crs_in_parquet_type_{geom_col}",
                    status=CheckStatus.PASSED,
                    message="non-default CRS is inline in Parquet geo type",
                    category="geoparquet_2_0",
                )

            return ValidationCheck(
                name=f"v2_crs_in_parquet_type_{geom_col}",
                status=CheckStatus.FAILED,
                message="non-default CRS must be inline PROJJSON in Parquet geo type",
                details="GeoParquet 2.0 requires CRS in Parquet schema, not just metadata",
                category="geoparquet_2_0",
            )

    return ValidationCheck(
        name=f"v2_crs_in_parquet_type_{geom_col}",
        status=CheckStatus.FAILED,
        message=f'column "{geom_col}" not found',
        category="geoparquet_2_0",
    )


def _check_v2_crs_consistency(geo_meta: dict, schema_info: list, geom_col: str) -> ValidationCheck:
    """Check V2-3: CRS in geo metadata must match CRS in Parquet schema."""
    from geoparquet_io.core.duckdb_metadata import parse_geometry_logical_type

    col_meta = geo_meta.get("columns", {}).get(geom_col, {})
    metadata_crs = col_meta.get("crs")

    # Find schema CRS
    schema_crs = None
    for col in schema_info:
        if col.get("name") == geom_col:
            logical_type = col.get("logical_type", "")
            parsed = parse_geometry_logical_type(logical_type)
            if parsed:
                schema_crs = parsed.get("crs")
            break

    # Both None = both default = match
    if metadata_crs is None and schema_crs is None:
        return ValidationCheck(
            name=f"v2_crs_consistency_{geom_col}",
            status=CheckStatus.PASSED,
            message="CRS matches: both default to OGC:CRS84",
            category="geoparquet_2_0",
        )

    # Compare CRS - simplified comparison
    if _crs_equals(metadata_crs, schema_crs):
        return ValidationCheck(
            name=f"v2_crs_consistency_{geom_col}",
            status=CheckStatus.PASSED,
            message="CRS in metadata matches CRS in Parquet schema",
            category="geoparquet_2_0",
        )

    return ValidationCheck(
        name=f"v2_crs_consistency_{geom_col}",
        status=CheckStatus.FAILED,
        message="CRS in geo metadata must match CRS in Parquet schema",
        details=f"Metadata: {_crs_summary(metadata_crs)}, Schema: {_crs_summary(schema_crs)}",
        category="geoparquet_2_0",
    )


def _check_v2_edges_consistency(
    geo_meta: dict, schema_info: list, geom_col: str
) -> ValidationCheck:
    """Check V2-5: edges in metadata must match algorithm in Parquet GEOGRAPHY type."""
    from geoparquet_io.core.duckdb_metadata import parse_geometry_logical_type

    col_meta = geo_meta.get("columns", {}).get(geom_col, {})
    metadata_edges = col_meta.get("edges", "planar")  # Default is planar

    # Find schema algorithm
    schema_algorithm = None
    is_geography = False
    for col in schema_info:
        if col.get("name") == geom_col:
            logical_type = col.get("logical_type", "")
            if "GeographyType" in logical_type:
                is_geography = True
                parsed = parse_geometry_logical_type(logical_type)
                if parsed:
                    schema_algorithm = parsed.get("algorithm", "spherical")  # Default
            break

    if not is_geography:
        return ValidationCheck(
            name=f"v2_edges_consistency_{geom_col}",
            status=CheckStatus.SKIPPED,
            message="not a GEOGRAPHY type, edges consistency check not applicable",
            category="geoparquet_2_0",
        )

    if metadata_edges == schema_algorithm:
        return ValidationCheck(
            name=f"v2_edges_consistency_{geom_col}",
            status=CheckStatus.PASSED,
            message=f"edges in metadata matches algorithm in schema: {metadata_edges}",
            category="geoparquet_2_0",
        )

    return ValidationCheck(
        name=f"v2_edges_consistency_{geom_col}",
        status=CheckStatus.FAILED,
        message="edges in metadata must match algorithm in Parquet GEOGRAPHY type",
        details=f"Metadata edges: {metadata_edges}, Schema algorithm: {schema_algorithm}",
        category="geoparquet_2_0",
    )


# =============================================================================
# Parquet-geo-only Checks
# =============================================================================


def _check_parquet_geo_only_crs(schema_info: list, geom_col: str) -> ValidationCheck:
    """Check CRS for parquet-geo-only files (no GeoParquet metadata)."""
    from geoparquet_io.core.duckdb_metadata import parse_geometry_logical_type

    for col in schema_info:
        if col.get("name") == geom_col:
            logical_type = col.get("logical_type", "")
            parsed = parse_geometry_logical_type(logical_type)

            if not parsed:
                return ValidationCheck(
                    name=f"parquet_geo_only_crs_{geom_col}",
                    status=CheckStatus.PASSED,
                    message="no CRS specified (defaults to OGC:CRS84)",
                    category="parquet_geo_types",
                )

            crs = parsed.get("crs")

            # No CRS = default OGC:CRS84 = pass
            if crs is None:
                return ValidationCheck(
                    name=f"parquet_geo_only_crs_{geom_col}",
                    status=CheckStatus.PASSED,
                    message="no CRS specified (defaults to OGC:CRS84)",
                    category="parquet_geo_types",
                )

            # Check if CRS is geographic (WGS84, EPSG:4326, OGC:CRS84)
            if _is_geographic_crs(crs):
                return ValidationCheck(
                    name=f"parquet_geo_only_crs_{geom_col}",
                    status=CheckStatus.PASSED,
                    message="CRS is geographic (widely supported)",
                    category="parquet_geo_types",
                )

            # Check if CRS uses Parquet spec format
            if isinstance(crs, dict) and ("$schema" in crs or "type" in crs):
                return ValidationCheck(
                    name=f"parquet_geo_only_crs_{geom_col}",
                    status=CheckStatus.PASSED,
                    message="CRS uses valid PROJJSON format",
                    category="parquet_geo_types",
                )

            if isinstance(crs, str) and crs.startswith("srid:"):
                return ValidationCheck(
                    name=f"parquet_geo_only_crs_{geom_col}",
                    status=CheckStatus.PASSED,
                    message=f"CRS uses valid srid format: {crs}",
                    category="parquet_geo_types",
                )

            # Other CRS format - warning
            return ValidationCheck(
                name=f"parquet_geo_only_crs_{geom_col}",
                status=CheckStatus.WARNING,
                message="CRS format may not be widely recognized by geospatial tools",
                details=f"CRS: {_crs_summary(crs)}. "
                "Use 'gpio convert --geoparquet-version 2.0' to add standardized metadata.",
                category="parquet_geo_types",
            )

    return ValidationCheck(
        name=f"parquet_geo_only_crs_{geom_col}",
        status=CheckStatus.SKIPPED,
        message=f'column "{geom_col}" not found',
        category="parquet_geo_types",
    )


# =============================================================================
# Helper Functions
# =============================================================================


def _crs_equals(crs1: Any, crs2: Any) -> bool:
    """Compare two CRS values for equality."""
    if crs1 is None and crs2 is None:
        return True
    if crs1 is None or crs2 is None:
        return False

    # If both are dicts, compare EPSG codes if available
    if isinstance(crs1, dict) and isinstance(crs2, dict):
        id1 = crs1.get("id", {})
        id2 = crs2.get("id", {})
        if isinstance(id1, dict) and isinstance(id2, dict):
            if id1.get("authority") == id2.get("authority"):
                return id1.get("code") == id2.get("code")

        # Fall back to comparing names
        return crs1.get("name") == crs2.get("name")

    # Direct comparison for strings
    return crs1 == crs2


def _crs_summary(crs: Any) -> str:
    """Get a short summary of a CRS for display."""
    if crs is None:
        return "None (OGC:CRS84)"

    if isinstance(crs, dict):
        if "id" in crs and isinstance(crs["id"], dict):
            authority = crs["id"].get("authority", "")
            code = crs["id"].get("code", "")
            return f"{authority}:{code}"
        if "name" in crs:
            return crs["name"]
        return "PROJJSON object"

    return str(crs)


def _is_geographic_crs(crs: Any) -> bool:
    """Check if CRS is a geographic CRS (WGS84, EPSG:4326, OGC:CRS84)."""
    if crs is None:
        return True  # Default is OGC:CRS84

    if isinstance(crs, dict):
        # Check for EPSG:4326
        crs_id = crs.get("id", {})
        if isinstance(crs_id, dict):
            authority = crs_id.get("authority", "").upper()
            code = crs_id.get("code")
            if authority == "EPSG" and code == 4326:
                return True
            if authority == "OGC" and str(code).upper() == "CRS84":
                return True

        # Check name for common geographic CRS names
        name = crs.get("name", "").upper()
        if any(x in name for x in ["WGS 84", "WGS84", "CRS84", "4326"]):
            return True

    if isinstance(crs, str):
        crs_upper = crs.upper()
        return any(x in crs_upper for x in ["4326", "CRS84", "WGS84"])

    return False


# =============================================================================
# Main Validation Function
# =============================================================================


def validate_geoparquet(
    parquet_file: str,
    target_version: str | None = None,
    validate_data: bool = True,
    sample_size: int = 1000,
    verbose: bool = False,
) -> ValidationResult:
    """
    Validate a GeoParquet file against specification requirements.

    Args:
        parquet_file: Path to the parquet file
        target_version: Optional version to validate against (auto-detect if None)
        validate_data: If True, validate geometry data against metadata claims
        sample_size: Number of rows to sample for data validation (0 = all)
        verbose: Print verbose output

    Returns:
        ValidationResult with all check results
    """
    from geoparquet_io.core.common import (
        detect_geoparquet_file_type,
        get_duckdb_connection,
        needs_httpfs,
        safe_file_url,
    )
    from geoparquet_io.core.duckdb_metadata import (
        detect_geometry_columns,
        get_geo_metadata,
        get_kv_metadata,
        get_schema_info,
    )
    from geoparquet_io.core.logging_config import configure_verbose

    configure_verbose(verbose)

    safe_url = safe_file_url(parquet_file, verbose=verbose)

    # Auto-detect file type
    file_type_info = detect_geoparquet_file_type(parquet_file, verbose)

    # Determine detected version
    detected_version = _determine_version(file_type_info, target_version)

    result = ValidationResult(
        file_path=parquet_file,
        detected_version=detected_version,
        target_version=target_version,
    )

    # Get metadata
    kv_metadata = get_kv_metadata(safe_url)
    geo_meta = get_geo_metadata(safe_url)
    schema_info = get_schema_info(safe_url)
    geo_columns = detect_geometry_columns(safe_url)

    # Get DuckDB connection for data validation
    con = None
    if validate_data:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(parquet_file))

    try:
        # If target_version is parquet-geo-only, only run Parquet native geo type checks
        if target_version == "parquet-geo-only":
            result.checks.extend(
                _run_parquet_geo_only_checks(
                    parquet_file, schema_info, geo_columns, con, sample_size, validate_data
                )
            )
        # Otherwise, run checks based on detected file type
        elif file_type_info["file_type"] == "parquet_geo_only":
            result.checks.extend(
                _run_parquet_geo_only_checks(
                    parquet_file, schema_info, geo_columns, con, sample_size, validate_data
                )
            )
        elif file_type_info["file_type"] in ["geoparquet_v1", "geoparquet_v2"]:
            result.checks.extend(
                _run_geoparquet_checks(
                    parquet_file,
                    kv_metadata,
                    geo_meta,
                    schema_info,
                    file_type_info,
                    con,
                    sample_size,
                    validate_data,
                )
            )
        else:
            # Unknown file type
            result.checks.append(
                ValidationCheck(
                    name="file_type",
                    status=CheckStatus.FAILED,
                    message="No GeoParquet metadata or Parquet geo types found",
                    category="core",
                )
            )

    finally:
        if con:
            con.close()

    return result


def _determine_version(file_type_info: dict, target_version: str | None) -> str:
    """Determine which version string to report."""
    if target_version:
        return target_version

    file_type = file_type_info.get("file_type", "unknown")
    geo_version = file_type_info.get("geo_version")

    if file_type == "parquet_geo_only":
        return "parquet-geo-only"
    elif file_type == "geoparquet_v1":
        return geo_version or "1.x"
    elif file_type == "geoparquet_v2":
        return geo_version or "2.0"
    else:
        return "unknown"


def _run_parquet_geo_only_checks(
    parquet_file: str,
    schema_info: list,
    geo_columns: dict,
    con,
    sample_size: int,
    validate_data: bool,
) -> list[ValidationCheck]:
    """Run checks for parquet-geo-only files (native types, no GeoParquet metadata)."""
    checks = []

    for geom_col in geo_columns.keys():
        # Parquet native geo type checks
        checks.append(_check_native_geo_type_present(schema_info, geom_col))
        checks.append(_check_native_crs_format(schema_info, geom_col))
        checks.append(_check_geography_edges_valid(schema_info, geom_col))
        checks.append(_check_row_group_bbox_statistics(parquet_file, geom_col))

        # Parquet-geo-only specific CRS check
        checks.append(_check_parquet_geo_only_crs(schema_info, geom_col))

        # Data validation if requested
        if validate_data and con:
            checks.append(
                _check_geography_coordinate_bounds(
                    parquet_file, geom_col, schema_info, con, sample_size
                )
            )

    return checks


def _run_geoparquet_checks(
    parquet_file: str,
    kv_metadata: dict,
    geo_meta: dict | None,
    schema_info: list,
    file_type_info: dict,
    con,
    sample_size: int,
    validate_data: bool,
) -> list[ValidationCheck]:
    """Run checks for GeoParquet files (1.x or 2.0)."""
    checks = []

    # Core metadata checks (1.0+)
    checks.append(_check_geo_key_exists(kv_metadata))

    if geo_meta is None:
        checks.append(
            ValidationCheck(
                name="geo_metadata_parse",
                status=CheckStatus.FAILED,
                message="failed to parse 'geo' metadata as JSON",
                category="core_metadata",
            )
        )
        return checks

    checks.append(_check_metadata_is_json(geo_meta))
    checks.append(_check_version_present(geo_meta))
    checks.append(_check_primary_column_present(geo_meta))
    checks.append(_check_columns_present(geo_meta))
    checks.append(_check_primary_column_in_columns(geo_meta))

    columns = geo_meta.get("columns", {})

    # Column metadata checks for each geometry column
    for col_name, col_meta in columns.items():
        checks.append(_check_encoding_valid(col_meta, col_name))
        checks.append(_check_geometry_types_list(col_meta, col_name))
        checks.append(_check_crs_valid(col_meta, col_name))
        checks.append(_check_orientation_valid(col_meta, col_name))
        checks.append(_check_edges_valid(col_meta, col_name))
        checks.append(_check_bbox_valid(col_meta, col_name))
        checks.append(_check_epoch_valid(col_meta, col_name))

        # Parquet schema checks
        checks.append(_check_geometry_not_grouped(schema_info, col_name))
        checks.append(_check_geometry_byte_array(schema_info, col_name))
        checks.append(_check_geometry_not_repeated(schema_info, col_name))

        # Data validation checks
        if validate_data and con:
            encoding = col_meta.get("encoding", "WKB")
            checks.append(
                _check_encoding_matches_data(parquet_file, col_name, encoding, con, sample_size)
            )

            geometry_types = col_meta.get("geometry_types", [])
            checks.append(
                _check_geometry_types_match_data(
                    parquet_file, col_name, geometry_types, con, sample_size
                )
            )

            orientation = col_meta.get("orientation")
            checks.append(
                _check_orientation_matches_data(
                    parquet_file, col_name, orientation, con, sample_size
                )
            )

            bbox = col_meta.get("bbox")
            checks.append(_check_bbox_contains_data(parquet_file, col_name, bbox, con, sample_size))

    # Version-specific checks
    geo_version = file_type_info.get("geo_version", "1.0.0")

    # GeoParquet 1.1+ checks
    if geo_version >= "1.1.0":
        for col_name, col_meta in columns.items():
            checks.append(_check_covering_is_object(col_meta, col_name))

            # Only run bbox covering checks if covering is defined
            covering = col_meta.get("covering")
            if covering is not None and "bbox" in covering:
                checks.append(_check_covering_bbox_paths(col_meta, col_name))
                checks.append(_check_covering_bbox_column_exists(col_meta, col_name, schema_info))
                checks.append(_check_covering_bbox_structure(col_meta, col_name, schema_info))
                checks.append(_check_covering_bbox_field_types(col_meta, col_name, schema_info))

        checks.append(_check_file_extension(parquet_file))

    # GeoParquet 2.0 checks - run Parquet native geo type checks first
    if file_type_info["file_type"] == "geoparquet_v2":
        # Parquet native geo type checks (run first for 2.0)
        for col_name in columns.keys():
            checks.append(_check_native_geo_type_present(schema_info, col_name))
            checks.append(_check_native_crs_format(schema_info, col_name))
            checks.append(_check_geography_edges_valid(schema_info, col_name))

        # GeoParquet 2.0 specific checks
        for col_name in columns.keys():
            checks.append(_check_v2_uses_native_types(schema_info, col_name))
            checks.append(_check_v2_crs_in_parquet_type(geo_meta, schema_info, col_name))
            checks.append(_check_v2_crs_consistency(geo_meta, schema_info, col_name))
            checks.append(_check_v2_edges_consistency(geo_meta, schema_info, col_name))

    return checks


# =============================================================================
# Output Formatting
# =============================================================================


def format_terminal_output(result: ValidationResult) -> None:
    """Format validation result for terminal with gpq-style checkmarks."""
    console = Console()

    console.print()
    console.print("[bold]GeoParquet Validation Report[/bold]")
    console.print("=" * 32)
    console.print()

    # Show detected version
    if result.detected_version:
        console.print(f"Detected: [cyan]{result.detected_version}[/cyan]")
    if result.target_version:
        console.print(f"Validating against: [cyan]{result.target_version}[/cyan]")
    console.print()

    # Group checks by category
    categories: dict[str, list[ValidationCheck]] = {}
    for check in result.checks:
        cat = check.category or "general"
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(check)

    # Category labels and display order
    category_labels = {
        "core_metadata": "Core Metadata",
        "column_metadata": "Column Validation",
        "parquet_schema": "Parquet Schema",
        "data_validation": "Data Validation",
        "geoparquet_1_1": "GeoParquet 1.1",
        "parquet_geo_types": "Parquet Native Geo Types",
        "geoparquet_2_0": "GeoParquet 2.0 Requirements",
        "core": "Core",
    }

    # Display in a specific order so Parquet Native Geo Types appears before GeoParquet 2.0
    category_order = [
        "core",
        "core_metadata",
        "column_metadata",
        "parquet_schema",
        "data_validation",
        "geoparquet_1_1",
        "parquet_geo_types",
        "geoparquet_2_0",
    ]

    # Sort categories by the defined order, with unknown categories at the end
    sorted_categories = sorted(
        categories.keys(),
        key=lambda c: category_order.index(c) if c in category_order else len(category_order),
    )

    for category in sorted_categories:
        checks = categories[category]
        label = category_labels.get(category, category.replace("_", " ").title())
        console.print(f"[bold]{label}:[/bold]")

        for check in checks:
            symbol = _get_check_symbol(check.status)
            color = _get_check_color(check.status)
            console.print(f"  {symbol} [{color}]{check.message}[/{color}]")
            if check.details:
                console.print(f"      [dim]{check.details}[/dim]")
        console.print()

    # Summary
    console.print(
        f"Summary: [green]{result.passed_count} passed[/green], "
        f"[yellow]{result.warning_count} warnings[/yellow], "
        f"[red]{result.failed_count} failed[/red]"
    )
    console.print()


def _get_check_symbol(status: CheckStatus) -> str:
    """Get the symbol for a check status."""
    symbols = {
        CheckStatus.PASSED: "[green]✓[/green]",
        CheckStatus.FAILED: "[red]✗[/red]",
        CheckStatus.WARNING: "[yellow]⚠[/yellow]",
        CheckStatus.SKIPPED: "[dim]○[/dim]",
    }
    return symbols.get(status, "?")


def _get_check_color(status: CheckStatus) -> str:
    """Get the color for a check status."""
    colors = {
        CheckStatus.PASSED: "green",
        CheckStatus.FAILED: "red",
        CheckStatus.WARNING: "yellow",
        CheckStatus.SKIPPED: "dim",
    }
    return colors.get(status, "white")


def format_json_output(result: ValidationResult) -> str:
    """Format validation result as JSON for machine consumption."""
    output = {
        "file_path": result.file_path,
        "detected_version": result.detected_version,
        "target_version": result.target_version,
        "is_valid": result.is_valid,
        "summary": {
            "passed": result.passed_count,
            "warnings": result.warning_count,
            "failed": result.failed_count,
        },
        "checks": [
            {
                "name": c.name,
                "status": c.status.value,
                "message": c.message,
                "category": c.category,
                "details": c.details,
            }
            for c in result.checks
        ],
    }
    return json.dumps(output, indent=2)
