"""
GeoParquet file validation against specification requirements.

Validates GeoParquet files against versions 1.0, 1.1, 2.0, and Parquet native
geospatial types according to their respective specifications.
"""

import json
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from rich.console import Console

from geoparquet_io.core.common import split_zm_suffix, zm_suffix_sql
from geoparquet_io.core.crs_utils import (
    CRS_ABSENT,
    NULL_CRS_HINT,
    PROJJSON_CRS_TYPES,
    _is_crs84_equivalent,
    _is_ogc_crs84,
    _parse_crs_value,
    crs_from_column_meta,
    get_crs_display_name,
    is_geographic_crs,
)
from geoparquet_io.core.duckdb_utils import (
    _escape_sql_string,
    _geoarrow_coord_exprs,
    quote_identifier,
)
from geoparquet_io.core.exceptions import GeoParquetError


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

# The single-geometry-type GeoArrow encodings. GeoParquet 1.1 permits these
# alongside "WKB" ("Supported values: "WKB"; one of "point", "linestring",
# "polygon", "multipoint", "multilinestring", "multipolygon""). 1.0 and the
# 2.0 draft are both WKB-only, so they stay rejected there.
GEOARROW_ENCODINGS = [
    "point",
    "linestring",
    "polygon",
    "multipoint",
    "multilinestring",
    "multipolygon",
]

# GeoArrow encoding -> the spec geometry_types name it can hold. The encoding
# fixes the geometry type of every value in the column, so a data scan does not
# need to reconstruct geometries to know what is in there.
_GEOARROW_ENCODING_TYPE = {
    "point": "Point",
    "linestring": "LineString",
    "polygon": "Polygon",
    "multipoint": "MultiPoint",
    "multilinestring": "MultiLineString",
    "multipolygon": "MultiPolygon",
}
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


def _is_geoarrow_encoding(encoding: Any) -> bool:
    """True for one of the spec's single-geometry-type GeoArrow encodings.

    The spec spells these lowercase, and unlike "WKB"/"wkb" there is no
    established lenient casing to honour, so the match is exact.
    """
    return isinstance(encoding, str) and encoding in GEOARROW_ENCODINGS


def _geoarrow_encoding_allowed(geo_version: Any) -> bool:
    """True for versions whose spec text permits the GeoArrow encodings.

    Only GeoParquet 1.1 does: 1.0 says WKB "is the only current option" and
    the 2.0 draft says "The only supported value is "WKB"".
    """
    return _version_at_least(geo_version, 1, 1) and not _version_at_least(geo_version, 2, 0)


def _check_encoding_valid(
    col_meta: dict, col_name: str, geo_version: Any = "1.0.0"
) -> ValidationCheck:
    """Check 7: column metadata must include a valid 'encoding' string."""
    encoding = col_meta.get("encoding")

    if _is_geoarrow_encoding(encoding) and not _geoarrow_encoding_allowed(geo_version):
        return ValidationCheck(
            name=f"encoding_valid_{col_name}",
            status=CheckStatus.FAILED,
            message=f'column "{col_name}" uses GeoArrow encoding "{encoding}", '
            f"which requires GeoParquet 1.1 (file declares {geo_version})",
            category="column_metadata",
        )

    is_valid = encoding in VALID_ENCODINGS or _is_geoarrow_encoding(encoding)
    return ValidationCheck(
        name=f"encoding_valid_{col_name}",
        status=CheckStatus.PASSED if is_valid else CheckStatus.FAILED,
        message=f'column "{col_name}" has valid encoding: {encoding}'
        if is_valid
        else f'column "{col_name}" must have valid encoding (got: {encoding})',
        category="column_metadata",
    )


def _strip_zm_suffix(geometry_type: str) -> str:
    """Return the base geometry type without a spec " Z"/" M"/" ZM" suffix."""
    return split_zm_suffix(geometry_type)[0]


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

    # Validate each type is a valid string; the spec adds a " Z"/" M"/" ZM"
    # suffix for 3D/measured geometries (e.g. "Point Z").
    invalid_types = [t for t in geometry_types if _strip_zm_suffix(t) not in VALID_GEOMETRY_TYPES]
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


# CRS type values allowed by the PROJJSON v0.7 schema's "crs" definition.
# Shared with the convert write path so gpio never writes a CRS its own
# validator rejects (see crs_utils.normalize_projjson_crs).
_PROJJSON_CRS_TYPES = PROJJSON_CRS_TYPES


def _check_crs_valid(col_meta: dict, col_name: str) -> ValidationCheck:
    """Check 9: optional 'crs' must be null or a PROJJSON object.

    Note the spec distinction: an omitted ``crs`` key defaults to OGC:CRS84,
    while an explicit ``"crs": null`` means the CRS is *unknown*.
    """
    # Key omitted entirely -> defaults to OGC:CRS84 (the common, correct case).
    if "crs" not in col_meta:
        return ValidationCheck(
            name=f"crs_valid_{col_name}",
            status=CheckStatus.PASSED,
            message=f'column "{col_name}" has no CRS (defaults to OGC:CRS84)',
            category="column_metadata",
        )

    crs = col_meta.get("crs")

    # Key present but null -> unknown CRS. Valid per spec, but usually a mistake
    # when the data is really default lon/lat, so surface it as a warning.
    if crs is None:
        return ValidationCheck(
            name=f"crs_valid_{col_name}",
            status=CheckStatus.WARNING,
            message=f'column "{col_name}" has an explicit null CRS. {NULL_CRS_HINT}',
            category="column_metadata",
        )

    # Check if it's a valid PROJJSON object. The PROJJSON schema requires a
    # "type" member drawn from its known CRS type set; JSON without it (or
    # with a made-up type) is not PROJJSON.
    if isinstance(crs, dict):
        if "type" not in crs:
            return ValidationCheck(
                name=f"crs_valid_{col_name}",
                status=CheckStatus.FAILED,
                message=f'column "{col_name}" CRS is missing the required PROJJSON "type" member',
                category="column_metadata",
            )
        if crs["type"] not in _PROJJSON_CRS_TYPES:
            return ValidationCheck(
                name=f"crs_valid_{col_name}",
                status=CheckStatus.FAILED,
                message=f'column "{col_name}" CRS has unknown PROJJSON type: {crs["type"]!r}',
                details=f"Known PROJJSON CRS types: {', '.join(sorted(_PROJJSON_CRS_TYPES))}",
                category="column_metadata",
            )
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


def _version_at_least(version: Any, major: int, minor: int = 0) -> bool:
    """True when a GeoParquet version string is >= major.minor.

    Parses instead of comparing lexicographically (which breaks on "1.10.0"
    vs "1.2.0"). Handles "2.0", "2.0.0", and pre-release forms like
    "1.0.0-beta.1" (the pre-release tag is ignored). Non-string or
    unparsable versions return False.
    """
    if not isinstance(version, str):
        return False
    core = version.split("-", 1)[0].split("+", 1)[0]
    parts = core.split(".")
    try:
        v_major = int(parts[0])
        v_minor = int(parts[1]) if len(parts) > 1 else 0
    except (ValueError, IndexError):
        return False
    return (v_major, v_minor) >= (major, minor)


def _check_edges_valid(
    col_meta: dict, col_name: str, geo_version: str = "1.0.0"
) -> ValidationCheck:
    """Check 11: optional 'edges' must be a valid string.

    GeoParquet 2.0 widened the vocabulary from planar/spherical to include the
    four ellipsoidal-geodesic algorithms; 1.x keeps the narrow set.
    """
    edges = col_meta.get("edges")

    if edges is None:
        return ValidationCheck(
            name=f"edges_valid_{col_name}",
            status=CheckStatus.PASSED,
            message=f'column "{col_name}" has no edges (defaults to planar)',
            category="column_metadata",
        )

    valid_edges = list(VALID_EDGES_GEOPARQUET)
    if _version_at_least(geo_version, 2, 0):
        valid_edges += [e for e in VALID_EDGES_PARQUET_GEO if e not in valid_edges]

    is_valid = edges in valid_edges
    return ValidationCheck(
        name=f"edges_valid_{col_name}",
        status=CheckStatus.PASSED if is_valid else CheckStatus.FAILED,
        message=f'column "{col_name}" has valid edges: {edges}'
        if is_valid
        else f'column "{col_name}" edges must be one of {valid_edges}',
        category="column_metadata",
    )


def _check_bbox_valid(col_meta: dict, col_name: str) -> ValidationCheck:
    """Check 12: optional 'bbox' must be an array of 4, 6 or 8 numbers."""
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

    if len(bbox) not in [4, 6, 8]:
        return ValidationCheck(
            name=f"bbox_valid_{col_name}",
            status=CheckStatus.FAILED,
            message=f'column "{col_name}" bbox must have 4, 6 or 8 elements (got {len(bbox)})',
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


def _resolve_datum_type(crs: Any) -> str | None:
    """Resolve a CRS value's datum type name via pyproj; None when unresolvable."""
    try:
        from pyproj import CRS as PyprojCRS

        if isinstance(crs, dict):
            try:
                pyproj_crs = PyprojCRS.from_json_dict(crs)
            except Exception:
                # Partial PROJJSON: resolve via its authority:code id instead
                crs_id = crs.get("id") or {}
                pyproj_crs = PyprojCRS.from_user_input(
                    f"{crs_id.get('authority')}:{crs_id.get('code')}"
                )
        else:
            pyproj_crs = PyprojCRS.from_user_input("OGC:CRS84")
        return pyproj_crs.datum.type_name if pyproj_crs.datum else ""
    except Exception:
        return None


def _check_epoch_valid(col_meta: dict, col_name: str) -> ValidationCheck:
    """Check 13: optional 'epoch' must be a number on a dynamic CRS."""

    def _result(status, message, details=None):
        return ValidationCheck(
            name=f"epoch_valid_{col_name}",
            status=status,
            message=message,
            details=details,
            category="column_metadata",
        )

    epoch = col_meta.get("epoch")
    if epoch is None:
        return _result(CheckStatus.PASSED, f'column "{col_name}" has no epoch')
    if not isinstance(epoch, (int, float)):
        return _result(CheckStatus.FAILED, f'column "{col_name}" epoch must be a number')

    # A coordinate epoch only makes sense for a dynamic CRS. The default
    # (absent crs = OGC:CRS84) and static datums do not support one. An
    # explicit null crs is distinct from absent: there is no CRS to judge.
    crs = crs_from_column_meta(col_meta)
    if crs is None:
        return _result(
            CheckStatus.WARNING,
            f'column "{col_name}" declares epoch {epoch} but CRS is null; cannot verify datum type',
        )
    if crs is CRS_ABSENT:
        crs = None  # _resolve_datum_type reads a non-dict as the OGC:CRS84 default

    datum_type = _resolve_datum_type(crs)
    if datum_type is None:
        return _result(
            CheckStatus.WARNING,
            f'column "{col_name}" declares epoch {epoch} but the CRS datum '
            "could not be resolved for epoch validation",
        )
    # Dynamic frames (ITRF...) fully support epochs. A datum ensemble
    # (EPSG:4326, OGC:CRS84) cannot carry one — there is no single frame
    # the epoch could refer to. A specific static frame (e.g. GDA2020) is
    # tolerated with a warning: epochs are commonly attached there in
    # practice (plate-motion workflows) even though the frame is static.
    if "Ensemble" in datum_type:
        return _result(
            CheckStatus.FAILED,
            f'column "{col_name}" declares epoch {epoch} on a datum ensemble',
            details=f"Datum type: {datum_type}. "
            "Coordinate epochs apply to dynamic reference frames (e.g. ITRF).",
        )
    if "Dynamic" not in datum_type:
        return _result(
            CheckStatus.WARNING,
            f'column "{col_name}" declares epoch {epoch} on a static CRS',
            details=f"Datum type: {datum_type or 'unknown'}. "
            "Epochs are only meaningful for dynamic reference frames.",
        )
    return _result(CheckStatus.PASSED, f'column "{col_name}" has valid epoch: {epoch}')


_KNOWN_VERSION_MAJORS = ("1.", "2.")


def _check_version_known(geo_meta: dict) -> ValidationCheck:
    """The declared version must be a known GeoParquet major version."""
    version = geo_meta.get("version")
    if version is None:
        # _check_version_present reports the absence; nothing to judge here.
        return ValidationCheck(
            name="version_known",
            status=CheckStatus.SKIPPED,
            message="no version to check",
            category="core_metadata",
        )
    if not isinstance(version, str):
        return ValidationCheck(
            name="version_known",
            status=CheckStatus.FAILED,
            message=f"version must be a string (got {type(version).__name__}: {version!r})",
            details="Known versions are 1.x and 2.x",
            category="core_metadata",
        )
    # Policy: prefix-match known majors so any 1.x/2.x patch release passes
    # (the corpus depends on this). Versions from a future major hard-FAIL:
    # their semantics are unknown and must not silently validate against
    # today's rules.
    if version.startswith(_KNOWN_VERSION_MAJORS):
        return ValidationCheck(
            name="version_known",
            status=CheckStatus.PASSED,
            message=f"version {version} is a known GeoParquet version",
            category="core_metadata",
        )
    return ValidationCheck(
        name="version_known",
        status=CheckStatus.FAILED,
        message=f"unknown GeoParquet version: {version!r}",
        details="Known versions are 1.x and 2.x",
        category="core_metadata",
    )


def _columns_declaring_covering(geo_meta: dict) -> list[str]:
    """Names of geometry columns whose metadata declares the 'covering' key (added in 1.1)."""
    return sorted(
        name
        for name, col in (geo_meta.get("columns") or {}).items()
        if isinstance(col, dict) and "covering" in col
    )


def _check_version_features(parquet_file: str, geo_meta: dict) -> ValidationCheck:
    """1.x metadata must not be combined with features from a newer version."""

    def _result(status, message, details=None):
        return ValidationCheck(
            name="version_features_match",
            status=status,
            message=message,
            details=details,
            category="core_metadata",
        )

    version = geo_meta.get("version")
    if not isinstance(version, str):
        # version_known already FAILs a non-string version; a PASS here would
        # contradict it, so decline to judge features instead.
        return _result(CheckStatus.SKIPPED, "version is not a string; feature check not applicable")
    if not version.startswith("1."):
        return _result(CheckStatus.PASSED, "version permits declared features")
    covering_cols = _columns_declaring_covering(geo_meta)
    if covering_cols and not _version_at_least(version, 1, 1):
        return _result(
            CheckStatus.FAILED,
            f"version {version} declared but columns use the 1.1-only "
            f"'covering' key ({', '.join(covering_cols)})",
            details="'covering' was introduced in GeoParquet 1.1",
        )
    try:
        import pyarrow.parquet as pq

        schema = pq.ParquetFile(parquet_file).metadata.schema
        native_cols = [
            schema.column(i).name
            for i in range(len(schema))
            if str(schema.column(i).logical_type).startswith(("Geometry", "Geography"))
        ]
    except Exception as e:
        return _result(CheckStatus.SKIPPED, f"could not inspect Parquet schema: {e}")
    if native_cols:
        return _result(
            CheckStatus.FAILED,
            f"version {version} declared but file uses native Parquet "
            f"GEOMETRY/GEOGRAPHY types ({', '.join(native_cols)})",
            details="Native geospatial logical types require GeoParquet 2.0",
        )
    return _result(CheckStatus.PASSED, "declared features match the declared version")


# =============================================================================
# Parquet Schema Checks (GeoParquet 1.0+)
# =============================================================================


def _check_geometry_not_grouped(
    schema_info: list, geom_col: str, encoding: Any = "WKB"
) -> ValidationCheck:
    """Check 14: geometry columns must not be grouped.

    The spec's "MUST NOT be a group field" rule is about nesting the geometry
    inside another column; the GeoArrow encodings store coordinates in a
    (repeated) group by design, so for those the requirement that survives is
    that the column sits at the root of the schema.
    """
    # Find the geometry column in schema
    for col in schema_info:
        if col.get("name") == geom_col:
            if _is_geoarrow_encoding(encoding):
                return ValidationCheck(
                    name=f"geometry_not_grouped_{geom_col}",
                    status=CheckStatus.PASSED,
                    message=f'geometry column "{geom_col}" is a "{encoding}" GeoArrow '
                    "group at the schema root",
                    category="parquet_schema",
                )
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


# GeoArrow encoding -> number of LIST levels wrapping the coordinate struct.
#
# Depth alone is not a unique key. Two pairs share a level and hold identical
# coordinate data:
#   linestring <-> multipoint      (depth 1)
#   polygon    <-> multilinestring (depth 2)
#
# What separates them is the set of names on the coordinate nesting, which
# geoarrow renders into the type string this module already reads via
# get_schema_info():
#   linestring       list<vertices: struct<x,y>>
#   multipoint       list<points:   struct<x,y>>
#   polygon          list<vertices: list<rings:       struct<x,y>>>
#   multilinestring  list<vertices: list<linestrings: struct<x,y>>>
#   multipolygon     list<vertices: list<rings: list<polygons: struct<x,y>>>>
# _GEOARROW_LIST_FIELDS below pins those, so a column labelled "multipoint" but
# stored as a linestring is rejected rather than passing on equal depth.
#
# The names are not stored in the Parquet file (its fields are the generic
# "element"); they are how geoarrow.pyarrow renders the extension type, which it
# resolves from the ARROW:extension:name field metadata gpio's writer emits. So
# both signals exist only on get_schema_info()'s local/pyarrow path. On the
# DuckDB path (remote files) parquet_schema() reports the group node with no
# type at all, and neither check can run — absence of the name is therefore
# never treated as a mismatch.
_GEOARROW_LIST_DEPTH = {
    "point": 0,
    "linestring": 1,
    "multipoint": 1,
    "polygon": 2,
    "multilinestring": 2,
    "multipolygon": 3,
}


# The set of LIST field names each encoding's coordinate nesting carries, as
# get_schema_info() renders it. This is what separates the two pairs that share
# a list depth and are therefore invisible to _GEOARROW_LIST_DEPTH alone:
# linestring/multipoint (depth 1) and polygon/multilinestring (depth 2).
#
# Matched as a SET, deliberately. get_schema_info()'s pyarrow path renders the
# names in the reverse of geoarrow.pyarrow's own nesting order -- a polygon
# reads as list<vertices: list<rings: ...>> here but as
# list<rings: list<vertices: ...>> from arr.type.storage_type -- so an ordered
# comparison would encode that quirk and break if it were ever normalised. The
# five sets are pairwise distinct, so order buys no discrimination anyway.
#
# "point" has no list level and needs no entry: depth 0 already identifies it.
_GEOARROW_LIST_FIELDS = {
    "linestring": frozenset({"vertices"}),
    "multipoint": frozenset({"points"}),
    "polygon": frozenset({"vertices", "rings"}),
    "multilinestring": frozenset({"vertices", "linestrings"}),
    "multipolygon": frozenset({"vertices", "rings", "polygons"}),
}

_GEOARROW_FIELDS_ENCODING = {v: k for k, v in _GEOARROW_LIST_FIELDS.items()}

# Every name the map above can produce. A type string naming anything outside
# this set came from a source that does not use geoarrow's names (the generic
# list<element: ...>, or DuckDB's typeless group node), and is not evidence of a
# wrong encoding either way.
_KNOWN_LIST_FIELDS = {name.upper() for names in _GEOARROW_LIST_FIELDS.values() for name in names}


def _list_field_names(physical_type: str) -> frozenset[str] | None:
    """The LIST field names in a type string, when they are geoarrow's own.

    Returns None when any LIST level carries a name geoarrow never emits, which
    must not be read as a mismatch.
    """
    found = re.findall(r"LIST<\s*([A-Za-z_][A-Za-z0-9_]*)\s*:", physical_type)
    if not found or any(name.upper() not in _KNOWN_LIST_FIELDS for name in found):
        return None
    return frozenset(name.lower() for name in found)


def _check_geoarrow_layout(physical_type: str, geom_col: str, encoding: str) -> ValidationCheck:
    """Check 15 for GeoArrow columns: a (repeated) group of DOUBLE coordinates.

    The BYTE_ARRAY requirement the spec states is scoped to WKB columns; native
    encodings instead MUST store "the actual coordinates ... as native numbers,
    i.e. using the DOUBLE parquet type in a (repeated) group of fields".
    """
    name = f"geometry_byte_array_{geom_col}"

    if "BYTE_ARRAY" in physical_type or physical_type == "BINARY":
        return ValidationCheck(
            name=name,
            status=CheckStatus.FAILED,
            message=f'geometry column "{geom_col}" declares GeoArrow encoding "{encoding}" '
            f"but is stored as {physical_type} (native encodings require a group of "
            "DOUBLE coordinate fields)",
            category="parquet_schema",
        )

    # Only Arrow-style type strings expose the nesting; the parquet_schema()
    # path reports the group node with no type, and cannot be checked here.
    if "STRUCT<" in physical_type:
        expected = _GEOARROW_LIST_DEPTH[encoding]
        found = physical_type.count("LIST<")
        if found != expected or "DOUBLE" not in physical_type:
            return ValidationCheck(
                name=name,
                status=CheckStatus.FAILED,
                message=f'geometry column "{geom_col}" layout does not match GeoArrow '
                f'encoding "{encoding}" (expected {expected} list level(s) around a '
                f"DOUBLE coordinate struct, got {physical_type})",
                category="parquet_schema",
            )

        # Depth ties linestring/multipoint and polygon/multilinestring, so the
        # coordinate nesting's field names are what actually separate them.
        # Only judge them when they are names geoarrow emits: a generic or
        # absent field name says nothing about the encoding either way.
        expected_fields = _GEOARROW_LIST_FIELDS.get(encoding)
        found_fields = _list_field_names(physical_type)
        if expected_fields and found_fields is not None and found_fields != expected_fields:
            mislabelled = _GEOARROW_FIELDS_ENCODING.get(found_fields)
            stored_as = f" (the layout of {mislabelled!r})" if mislabelled else ""
            return ValidationCheck(
                name=name,
                status=CheckStatus.FAILED,
                message=f"geometry column {geom_col!r} declares GeoArrow encoding "
                f"{encoding!r}, whose coordinates nest under "
                f"{sorted(expected_fields)}, but the stored column nests under "
                f"{sorted(found_fields)}{stored_as}",
                category="parquet_schema",
            )

    return ValidationCheck(
        name=name,
        status=CheckStatus.PASSED,
        message=f'geometry column "{geom_col}" uses the native "{encoding}" '
        "GeoArrow coordinate layout",
        category="parquet_schema",
    )


def _check_geometry_byte_array(
    schema_info: list, geom_col: str, encoding: Any = "WKB"
) -> ValidationCheck:
    """Check 15: WKB geometry columns must be stored using BYTE_ARRAY parquet type."""
    for col in schema_info:
        if col.get("name") == geom_col:
            # parquet_schema() reports a group node's type as an explicit None,
            # so the "" default never applies: `or ""` is the guard, not `get`.
            physical_type = (col.get("type") or "").upper()
            if _is_geoarrow_encoding(encoding):
                return _check_geoarrow_layout(physical_type, geom_col, encoding)
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


def _describe_geom_type(con, safe_url: str, geom_col: str) -> str:
    """DuckDB's type string for a geometry column ('' when unavailable)."""
    type_query = f"DESCRIBE SELECT {quote_identifier(geom_col)} FROM read_parquet('{safe_url}')"
    type_result = con.execute(type_query).fetchone()
    return type_result[1] if type_result else ""


def _geoarrow_zm_suffix(col_type: str) -> str:
    """Dimension suffix implied by a GeoArrow coordinate struct's fields.

    GeoArrow keeps dimensionality in the struct layout (x/y/z/m fields), not in
    a per-value header, so the suffix is a property of the column type.
    """
    match = re.search(r"STRUCT\(([^)]*)\)", col_type or "", re.IGNORECASE)
    if not match:
        return ""
    fields = {field.strip().split(" ")[0].strip('"').lower() for field in match.group(1).split(",")}
    has_z, has_m = "z" in fields, "m" in fields
    if has_z and has_m:
        return " ZM"
    if has_z:
        return " Z"
    if has_m:
        return " M"
    return ""


def _geoarrow_bounds_subquery(
    safe_url: str, geom_col: str, encoding: str, limit_clause: str
) -> str:
    """Per-row GeoArrow coordinate bounds, with empty geometries filtered out.

    Empty GeoArrow values are either an empty coordinate list (NULL bounds) or,
    for "point", NaN coordinates; neither has an extent to compare, so the
    isfinite() guard drops both instead of poisoning MIN/MAX.
    """
    quoted_geom = quote_identifier(geom_col)
    xmin, ymin, xmax, ymax, _, _ = _geoarrow_coord_exprs(quoted_geom, encoding)
    return f"""
        SELECT {xmin} AS xmin, {ymin} AS ymin, {xmax} AS xmax, {ymax} AS ymax
        FROM read_parquet('{safe_url}')
        WHERE {quoted_geom} IS NOT NULL
          AND isfinite({xmin}) AND isfinite({ymin})
        {limit_clause}
    """


def _geoarrow_layout_error(
    check_name: str, encoding: str, subject: str, error: Exception
) -> ValidationCheck:
    """Report a GeoArrow coordinate path that does not resolve, not the raw SQL error."""
    return ValidationCheck(
        name=check_name,
        status=CheckStatus.FAILED,
        message=f"cannot read {subject}: the stored column layout does not match "
        f'GeoArrow encoding "{encoding}"',
        details=str(error),
        category="data_validation",
    )


def _check_geoarrow_encoding_matches_data(
    safe_url: str, geom_col: str, encoding: str, con, limit_clause: str
) -> ValidationCheck:
    """Check 17 for GeoArrow columns: the stored nesting must fit the encoding.

    There is nothing to parse per value — a native column either exposes the
    coordinate path its encoding implies (a "polygon" column being a list of
    rings of x/y structs) or it does not, which is the mismatch this reports.
    """
    name = f"encoding_matches_data_{geom_col}"
    quoted_geom = quote_identifier(geom_col)
    xmin, _, _, _, _, _ = _geoarrow_coord_exprs(quoted_geom, encoding)
    query = f"""
        SELECT COUNT(*) FROM (
            SELECT {xmin} AS xmin
            FROM read_parquet('{safe_url}')
            WHERE {quoted_geom} IS NOT NULL
            {limit_clause}
        )
    """

    try:
        result = con.execute(query).fetchone()
    except Exception as e:
        return _geoarrow_layout_error(name, encoding, "geometry coordinates", e)

    total = result[0] if result else 0
    return ValidationCheck(
        name=name,
        status=CheckStatus.PASSED,
        message=f'all geometry values match "{encoding}" encoding ({total} checked)',
        category="data_validation",
    )


def _check_encoding_matches_data(
    parquet_file: str, geom_col: str, encoding: str, con, sample_size: int
) -> ValidationCheck:
    """Check 17: all geometry values match the 'encoding' metadata."""
    from geoparquet_io.core.file_utils import safe_file_url

    safe_url = safe_file_url(parquet_file, verbose=False)
    quoted_geom = quote_identifier(geom_col)

    # For WKB encoding, verify we can parse geometries as WKB
    limit_clause = f"LIMIT {sample_size}" if sample_size > 0 else ""

    if _is_geoarrow_encoding(encoding):
        return _check_geoarrow_encoding_matches_data(
            safe_url, geom_col, encoding, con, limit_clause
        )

    try:
        # First check if DuckDB already has it as a GEOMETRY type
        # In that case, the encoding was valid (DuckDB parsed it)
        type_query = f"DESCRIBE SELECT {quote_identifier(geom_col)} FROM read_parquet('{safe_url}')"
        type_result = con.execute(type_query).fetchone()
        col_type = type_result[1] if type_result else ""

        if "GEOMETRY" in col_type.upper():
            # DuckDB already parsed it as geometry - encoding is valid
            count_query = f"""
                SELECT COUNT(*) FROM read_parquet('{safe_url}')
                WHERE {quoted_geom} IS NOT NULL {limit_clause}
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
                   COUNT(CASE WHEN ST_GeomFromWKB({quoted_geom}) IS NOT NULL THEN 1 END) as valid
            FROM (
                SELECT {quoted_geom}
                FROM read_parquet('{safe_url}')
                WHERE {quoted_geom} IS NOT NULL
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


# Uppercase DuckDB geometry type names -> expected GeoParquet casing.
_GEOM_TYPE_DISPLAY = {
    "POINT": "Point",
    "LINESTRING": "LineString",
    "POLYGON": "Polygon",
    "MULTIPOINT": "MultiPoint",
    "MULTILINESTRING": "MultiLineString",
    "MULTIPOLYGON": "MultiPolygon",
    "GEOMETRYCOLLECTION": "GeometryCollection",
}


def _normalize_found_geometry_type(raw: str) -> str:
    """Map a DuckDB type + dimension suffix ("MULTIPOINT Z") to spec casing.

    Strips any ST_ prefix, splits off the Z/M/ZM suffix, maps the base name,
    then re-appends the suffix ("MultiPoint Z"). Unmapped base names keep the
    uppercase base (never the raw string, which still carries the suffix and
    would double it: "TRIANGLE Z Z").
    """
    base, suffix = split_zm_suffix(raw.replace("ST_", "").upper())
    return _GEOM_TYPE_DISPLAY.get(base, base) + suffix


def _compare_geometry_types(
    normalized_found: set, declared_types: list, total_count: int, geom_col: str
) -> ValidationCheck:
    """Compare the geometry types found in the data against the declared list."""
    name = f"geometry_types_match_data_{geom_col}"
    declared_set = set(declared_types) if declared_types else set()

    # If declared_types is empty, any type is allowed
    if not declared_set:
        return ValidationCheck(
            name=name,
            status=CheckStatus.PASSED,
            message=f"geometry_types is empty (all types allowed), "
            f"found: {normalized_found} ({total_count} checked)",
            category="data_validation",
        )

    # Check if all found types are in declared types
    undeclared = normalized_found - declared_set
    if undeclared:
        return ValidationCheck(
            name=name,
            status=CheckStatus.FAILED,
            message=f"found undeclared geometry types: {undeclared} ({total_count} checked)",
            details=f"Declared: {declared_set}, Found: {normalized_found}",
            category="data_validation",
        )

    return ValidationCheck(
        name=name,
        status=CheckStatus.PASSED,
        message=f'all geometry types match declared "geometry_types" ({total_count} checked)',
        category="data_validation",
    )


def _check_geoarrow_geometry_types(
    safe_url: str, geom_col: str, declared_types: list, con, limit_clause: str, encoding: str
) -> ValidationCheck:
    """Check 18 for GeoArrow columns.

    A single-geometry-type encoding pins the geometry type of every value in the
    column, and the coordinate struct pins its dimensionality, so the types
    present follow from the column type rather than from a per-value scan.

    The type therefore comes from the *declared* encoding, which check 15 has
    already reconciled against the stored nesting depth. Since depth does not
    separate linestring from multipoint (nor polygon from multilinestring, see
    _GEOARROW_LIST_DEPTH for both the limitation and the way to close it), a
    column mislabeled as its structural twin is reported here as that twin — a
    missed detection, never a false rejection.
    """
    quoted_geom = quote_identifier(geom_col)
    found = _GEOARROW_ENCODING_TYPE[encoding] + _geoarrow_zm_suffix(
        _describe_geom_type(con, safe_url, geom_col)
    )
    count_query = f"""
        SELECT COUNT(*) FROM read_parquet('{safe_url}')
        WHERE {quoted_geom} IS NOT NULL {limit_clause}
    """
    count_result = con.execute(count_query).fetchone()
    total_count = count_result[0] if count_result else 0
    return _compare_geometry_types({found}, declared_types, total_count, geom_col)


def _check_geometry_types_match_data(
    parquet_file: str,
    geom_col: str,
    declared_types: list,
    con,
    sample_size: int,
    encoding: Any = "WKB",
) -> ValidationCheck:
    """Check 18: all geometry types must be included in 'geometry_types' metadata."""
    from geoparquet_io.core.file_utils import safe_file_url

    safe_url = safe_file_url(parquet_file, verbose=False)
    limit_clause = f"LIMIT {sample_size}" if sample_size > 0 else ""

    try:
        if _is_geoarrow_encoding(encoding):
            return _check_geoarrow_geometry_types(
                safe_url, geom_col, declared_types, con, limit_clause, encoding
            )

        # Check if DuckDB already has it as a GEOMETRY type
        col_type = _describe_geom_type(con, safe_url, geom_col)

        # Build the geometry expression based on column type
        if "GEOMETRY" in col_type.upper():
            geom_expr = quote_identifier(geom_col)
        else:
            geom_expr = f"ST_GeomFromWKB({quote_identifier(geom_col)})"

        # Get both distinct types and total count in one query. The dimension
        # suffix matters: "LineString" and "LineString ZM" are distinct
        # geometry_types per spec, so the scan must not collapse them.
        typed_expr = f"ST_GeometryType({geom_expr}) || {zm_suffix_sql(geom_expr)}"
        query = f"""
            SELECT {typed_expr} as geom_type, COUNT(*) as cnt
            FROM (
                SELECT {quote_identifier(geom_col)}
                FROM read_parquet('{safe_url}')
                WHERE {quote_identifier(geom_col)} IS NOT NULL
                {limit_clause}
            )
            GROUP BY {typed_expr}
        """

        result = con.execute(query).fetchall()
        found_types = {}
        total_count = 0
        for row in result:
            if row[0]:
                found_types[row[0]] = row[1]
                total_count += row[1]

        normalized_found = {_normalize_found_geometry_type(t) for t in found_types.keys() if t}

        return _compare_geometry_types(normalized_found, declared_types, total_count, geom_col)
    except Exception as e:
        check_name = f"geometry_types_match_data_{geom_col}"
        if _is_geoarrow_encoding(encoding):
            return _geoarrow_layout_error(check_name, encoding, "geometry types", e)
        return ValidationCheck(
            name=check_name,
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


def _bbox_xy(bbox: list) -> tuple | None:
    """(xmin, ymin, xmax, ymax) of a 4-, 6- or 8-element GeoParquet bbox; None otherwise."""
    if len(bbox) not in (4, 6, 8):
        return None
    half = len(bbox) // 2
    return bbox[0], bbox[1], bbox[half], bbox[half + 1]


def _x_within_sql(geom_expr: str, xmin, xmax) -> str:
    """SQL predicate: every X of the geometry lies within the bbox X range.

    When xmin > xmax the bbox crosses the antimeridian (RFC 7946, 5.2): the
    allowed longitudes are [xmin, 180] and [-180, xmax], checked per vertex.
    """
    if xmin <= xmax:
        return f"ST_XMin({geom_expr}) >= {xmin} AND ST_XMax({geom_expr}) <= {xmax}"
    return (
        f"list_bool_and([ST_X(p.geom) <= {xmax} OR ST_X(p.geom) >= {xmin} "
        f"FOR p IN ST_Dump(ST_Points({geom_expr}))])"
    )


def _build_bbox_query(
    safe_url: str,
    geom_col: str,
    col_type: str,
    bbox: tuple,
    limit_clause: str,
    encoding: Any = "WKB",
) -> str:
    """Build SQL query to check if geometries fall within bbox.

    Args:
        safe_url: URL-safe file path
        geom_col: Name of geometry column
        col_type: DuckDB column type (to determine if GEOMETRY or binary)
        bbox: Tuple of (xmin, ymin, xmax, ymax)
        limit_clause: SQL LIMIT clause or empty string
        encoding: Declared GeoParquet encoding for the column

    Returns:
        SQL query string that returns (total, within_bbox) counts
    """
    xmin, ymin, xmax, ymax = bbox

    if _is_geoarrow_encoding(encoding):
        # Native columns carry coordinates, not serialized geometries: compare
        # the per-row coordinate bounds directly instead of parsing WKB.
        return f"""
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN
                       xmin >= {xmin} AND ymin >= {ymin} AND
                       xmax <= {xmax} AND ymax <= {ymax}
                   THEN 1 END) as within_bbox
            FROM ({_geoarrow_bounds_subquery(safe_url, geom_col, encoding, limit_clause)})
        """

    # Use geometry column directly if native type, otherwise convert from WKB
    quoted_geom = quote_identifier(geom_col)
    if "GEOMETRY" in col_type.upper():
        geom_expr = quoted_geom
    else:
        geom_expr = f"ST_GeomFromWKB({quoted_geom})"
    # Empties have no extent; the same expression filters them out pre-LIMIT.
    geom_expr_filter = geom_expr

    return f"""
        SELECT COUNT(*) as total,
               COUNT(CASE WHEN
                   {_x_within_sql(geom_expr, xmin, xmax)} AND
                   ST_YMin({geom_expr}) >= {ymin} AND
                   ST_YMax({geom_expr}) <= {ymax}
               THEN 1 END) as within_bbox
        FROM (
            SELECT {quoted_geom}
            FROM read_parquet('{safe_url}')
            WHERE {quoted_geom} IS NOT NULL
              AND NOT ST_IsEmpty({geom_expr_filter})
            {limit_clause}
        )
    """


def _interpret_bbox_result(result: tuple | None, geom_col: str) -> ValidationCheck:
    """Interpret bbox check result and return appropriate ValidationCheck.

    Args:
        result: Tuple of (total, within_bbox) or None
        geom_col: Name of geometry column for check naming

    Returns:
        ValidationCheck with PASSED, FAILED, or SKIPPED status
    """
    if result:
        total, within = result
        if total == 0:
            # All rows were NULL or EMPTY; claiming PASS "(0 checked)" would
            # vouch for a bbox nothing was tested against.
            return ValidationCheck(
                name=f"bbox_contains_data_{geom_col}",
                status=CheckStatus.SKIPPED,
                message="no non-empty geometries to check against declared bbox",
                category="data_validation",
            )
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

    return ValidationCheck(
        name=f"bbox_contains_data_{geom_col}",
        status=CheckStatus.SKIPPED,
        message="no data to validate",
        category="data_validation",
    )


def _check_bbox_contains_data(
    parquet_file: str,
    geom_col: str,
    bbox: list | None,
    con,
    sample_size: int,
    encoding: Any = "WKB",
) -> ValidationCheck:
    """Check 20: all geometries must fall within 'bbox' metadata."""
    if bbox is None:
        return ValidationCheck(
            name=f"bbox_contains_data_{geom_col}",
            status=CheckStatus.SKIPPED,
            message="no bbox specified, skipping check",
            category="data_validation",
        )

    from geoparquet_io.core.file_utils import safe_file_url

    safe_url = safe_file_url(parquet_file, verbose=False)
    limit_clause = f"LIMIT {sample_size}" if sample_size > 0 else ""

    xy = _bbox_xy(bbox)
    if xy is None:
        return ValidationCheck(
            name=f"bbox_contains_data_{geom_col}",
            status=CheckStatus.SKIPPED,
            message=f"bbox has {len(bbox)} elements, expected 4, 6 or 8; skipping data check",
            category="data_validation",
        )
    if xy[0] > xy[2] and _is_geoarrow_encoding(encoding):
        return ValidationCheck(
            name=f"bbox_contains_data_{geom_col}",
            status=CheckStatus.SKIPPED,
            message="antimeridian-crossing bbox is not checked for GeoArrow encodings",
            category="data_validation",
        )

    try:
        col_type = _describe_geom_type(con, safe_url, geom_col)
        query = _build_bbox_query(safe_url, geom_col, col_type, xy, limit_clause, encoding)
        result = con.execute(query).fetchone()
        return _interpret_bbox_result(result, geom_col)

    except Exception as e:
        check_name = f"bbox_contains_data_{geom_col}"
        if _is_geoarrow_encoding(encoding):
            return _geoarrow_layout_error(check_name, encoding, "geometry bounds", e)
        return ValidationCheck(
            name=check_name,
            status=CheckStatus.FAILED,
            message=f"failed to validate bbox: {e}",
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
                    # Same trap as _check_geometry_byte_array: a group child's
                    # type is an explicit None, so `or ""` is the guard here too.
                    child_type = (schema_info[i + j].get("type") or "").upper()
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
            logical_type = col.get("logical_type") or ""
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
            logical_type = col.get("logical_type") or ""
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
            logical_type = col.get("logical_type") or ""

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


def _is_geography_column(schema_info: list, geom_col: str) -> bool:
    """Check if a geometry column is a GEOGRAPHY type."""
    for col in schema_info:
        if col.get("name") == geom_col:
            logical_type = col.get("logical_type") or ""
            return "GeographyType" in logical_type
    return False


def _validate_geography_bounds(min_x, max_x, min_y, max_y) -> list[str]:
    """Check coordinate bounds and return list of issues found."""
    issues = []
    if min_x is not None and min_x < -180:
        issues.append(f"min_x={min_x} < -180")
    if max_x is not None and max_x > 180:
        issues.append(f"max_x={max_x} > 180")
    if min_y is not None and min_y < -90:
        issues.append(f"min_y={min_y} < -90")
    if max_y is not None and max_y > 90:
        issues.append(f"max_y={max_y} > 90")
    return issues


def _check_geography_coordinate_bounds(
    parquet_file: str, geom_col: str, schema_info: list, con, sample_size: int
) -> ValidationCheck:
    """Check PGO-7: for GEOGRAPHY, X bounded [-180, 180], Y bounded [-90, 90]."""
    if not _is_geography_column(schema_info, geom_col):
        return ValidationCheck(
            name=f"geography_coordinate_bounds_{geom_col}",
            status=CheckStatus.SKIPPED,
            message="not a GEOGRAPHY type, coordinate bounds check not applicable",
            category="parquet_geo_types",
        )

    from geoparquet_io.core.file_utils import safe_file_url

    safe_url = safe_file_url(parquet_file, verbose=False)
    limit_clause = f"LIMIT {sample_size}" if sample_size > 0 else ""

    try:
        result = _execute_bounds_query(con, safe_url, geom_col, limit_clause)
        if not result:
            return ValidationCheck(
                name=f"geography_coordinate_bounds_{geom_col}",
                status=CheckStatus.SKIPPED,
                message="no data to validate",
                category="parquet_geo_types",
            )

        min_x, max_x, min_y, max_y = result
        issues = _validate_geography_bounds(min_x, max_x, min_y, max_y)

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


def _execute_bounds_query(con, safe_url: str, geom_col: str, limit_clause: str):
    """Execute query to get coordinate bounds for a geometry column."""
    type_query = f"DESCRIBE SELECT {quote_identifier(geom_col)} FROM read_parquet('{safe_url}')"
    type_result = con.execute(type_query).fetchone()
    col_type = type_result[1] if type_result else ""

    if "GEOMETRY" in col_type.upper():
        geom_expr = quote_identifier(geom_col)
    else:
        geom_expr = f"ST_GeomFromWKB({quote_identifier(geom_col)})"

    query = f"""
        SELECT
            MIN(ST_XMin({geom_expr})) as min_x,
            MAX(ST_XMax({geom_expr})) as max_x,
            MIN(ST_YMin({geom_expr})) as min_y,
            MAX(ST_YMax({geom_expr})) as max_y
        FROM (
            SELECT {quote_identifier(geom_col)}
            FROM read_parquet('{safe_url}')
            WHERE {quote_identifier(geom_col)} IS NOT NULL
            {limit_clause}
        )
    """
    return con.execute(query).fetchone()


# =============================================================================
# Row Group Statistics Checks
# =============================================================================


def _check_row_group_bbox_statistics(parquet_file: str, geom_col: str) -> ValidationCheck:
    """Check that file has bbox column with row group statistics for spatial filtering."""
    from geoparquet_io.core.duckdb_metadata import (
        get_bbox_from_row_group_stats,
        has_bbox_column,
    )
    from geoparquet_io.core.remote import is_remote_url

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


def _is_bbox_valid(geo_bbox: dict) -> bool:
    """Check if bbox values are reasonable (not garbage from parsing errors).

    On some platforms (e.g., Windows with certain DuckDB versions), native geo
    statistics can be read incorrectly, resulting in extreme values like 10^300.
    This function validates that bbox values are within a reasonable range.
    """
    if not geo_bbox:
        return False

    # Maximum reasonable coordinate value (covers all projected CRS systems)
    # Even the most extreme projected CRS values are well under 10^8
    MAX_COORD = 1e15

    for key in ("xmin", "ymin", "xmax", "ymax"):
        val = geo_bbox.get(key)
        if val is None:
            continue
        try:
            # Check for extreme values that indicate parsing errors
            if abs(float(val)) > MAX_COORD:
                return False
        except (TypeError, ValueError):
            return False

    return True


def _check_native_geo_statistics(parquet_file: str, geom_col: str) -> ValidationCheck:
    """Check that geometry column has native Parquet GeospatialStatistics (geo_bbox)."""
    from geoparquet_io.core.duckdb_metadata import (
        aggregate_native_geo_stats,
        get_native_geo_stats_by_row_group,
    )
    from geoparquet_io.core.remote import is_remote_url

    try:
        # For remote files, skip (may be slow)
        if is_remote_url(parquet_file):
            return ValidationCheck(
                name=f"native_geo_stats_{geom_col}",
                status=CheckStatus.SKIPPED,
                message="geospatial statistics check skipped for remote files",
                category="parquet_geo_types",
            )

        chunks = get_native_geo_stats_by_row_group(parquet_file, geom_col)

        if chunks is None:
            return ValidationCheck(
                name=f"native_geo_stats_{geom_col}",
                status=CheckStatus.WARNING,
                message=f'geometry column "{geom_col}" not found in parquet metadata',
                category="parquet_geo_types",
            )

        # The whole file's bounds, not whichever row group comes first.
        bbox = aggregate_native_geo_stats(chunks).get("bbox")

        if bbox:
            geo_bbox = dict(zip(("xmin", "ymin", "xmax", "ymax"), bbox[:4], strict=True))
            # Validate that bbox values are reasonable (not garbage from parsing errors)
            if not _is_bbox_valid(geo_bbox):
                return ValidationCheck(
                    name=f"native_geo_stats_{geom_col}",
                    status=CheckStatus.SKIPPED,
                    message="geospatial statistics values appear invalid (possible parsing error)",
                    details="The bbox values read from native geo stats are out of range. "
                    "This may be a platform-specific issue with reading Parquet geo metadata.",
                    category="parquet_geo_types",
                )
            bbox_str = (
                f"[{geo_bbox['xmin']:.2f}, {geo_bbox['ymin']:.2f}, "
                f"{geo_bbox['xmax']:.2f}, {geo_bbox['ymax']:.2f}]"
            )
            return ValidationCheck(
                name=f"native_geo_stats_{geom_col}",
                status=CheckStatus.PASSED,
                message=f"geometry column has geospatial statistics: {bbox_str}",
                category="parquet_geo_types",
            )
        else:
            return ValidationCheck(
                name=f"native_geo_stats_{geom_col}",
                status=CheckStatus.WARNING,
                message=f'geometry column "{geom_col}" missing geospatial statistics',
                details="GeospatialStatistics (geo_bbox) enables efficient spatial filtering. "
                "Re-write with a tool that generates native geo statistics.",
                category="parquet_geo_types",
            )

    except Exception as e:
        return ValidationCheck(
            name=f"native_geo_stats_{geom_col}",
            status=CheckStatus.SKIPPED,
            message=f"could not check geospatial statistics: {e}",
            category="parquet_geo_types",
        )


def _check_native_geo_stats_contains_data(
    parquet_file: str, geom_col: str, con, sample_size: int
) -> ValidationCheck:
    """Check that sampled geometries fall within declared geospatial statistics (geo_bbox)."""
    from geoparquet_io.core.duckdb_metadata import get_aggregated_native_geo_stats
    from geoparquet_io.core.file_utils import safe_file_url
    from geoparquet_io.core.remote import is_remote_url

    try:
        # For remote files, skip (may be slow)
        if is_remote_url(parquet_file):
            return ValidationCheck(
                name=f"native_geo_stats_contains_data_{geom_col}",
                status=CheckStatus.SKIPPED,
                message="geospatial statistics data check skipped for remote files",
                category="parquet_geo_types",
            )

        safe_url = safe_file_url(parquet_file, verbose=False)

        # The sample below spans the whole file, so it is judged against the
        # whole file's statistics -- the union over every row group, not the
        # bounds of whichever one happens to come first.
        stats = get_aggregated_native_geo_stats(parquet_file, geom_col, con)

        if not stats:
            return ValidationCheck(
                name=f"native_geo_stats_contains_data_{geom_col}",
                status=CheckStatus.SKIPPED,
                message="no geospatial statistics to validate against",
                category="parquet_geo_types",
            )

        bbox = stats.get("bbox")
        if not bbox:
            return ValidationCheck(
                name=f"native_geo_stats_contains_data_{geom_col}",
                status=CheckStatus.SKIPPED,
                message="geospatial statistics has no bbox values",
                category="parquet_geo_types",
            )

        geo_bbox = dict(zip(("xmin", "ymin", "xmax", "ymax"), bbox[:4], strict=True))

        # Validate that bbox values are reasonable (not garbage from parsing errors)
        if not _is_bbox_valid(geo_bbox):
            return ValidationCheck(
                name=f"native_geo_stats_contains_data_{geom_col}",
                status=CheckStatus.SKIPPED,
                message="geospatial statistics values appear invalid (possible parsing error)",
                details="The bbox values read from native geo stats are out of range. "
                "This may be a platform-specific issue with reading Parquet geo metadata.",
                category="parquet_geo_types",
            )

        xmin = geo_bbox["xmin"]
        ymin = geo_bbox["ymin"]
        xmax = geo_bbox["xmax"]
        ymax = geo_bbox["ymax"]

        limit_clause = f"LIMIT {sample_size}" if sample_size > 0 else ""

        # Check if sampled geometries fall within the geo_bbox
        query = f"""
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN
                       ST_XMin({quote_identifier(geom_col)}) >= {xmin} AND
                       ST_YMin({quote_identifier(geom_col)}) >= {ymin} AND
                       ST_XMax({quote_identifier(geom_col)}) <= {xmax} AND
                       ST_YMax({quote_identifier(geom_col)}) <= {ymax}
                   THEN 1 END) as within_bbox
            FROM (
                SELECT {quote_identifier(geom_col)}
                FROM read_parquet('{safe_url}')
                WHERE {quote_identifier(geom_col)} IS NOT NULL
                  AND NOT ST_IsEmpty({quote_identifier(geom_col)})
                {limit_clause}
            )
        """
        result = con.execute(query).fetchone()

        if result:
            total, within = result
            if total == 0:
                # All rows were NULL or EMPTY; a PASS "(0 checked)" would
                # vouch for statistics nothing was tested against.
                return ValidationCheck(
                    name=f"native_geo_stats_contains_data_{geom_col}",
                    status=CheckStatus.SKIPPED,
                    message="no non-empty geometries to check against geospatial statistics",
                    category="parquet_geo_types",
                )
            if total == within:
                return ValidationCheck(
                    name=f"native_geo_stats_contains_data_{geom_col}",
                    status=CheckStatus.PASSED,
                    message=f"all geometries fall within geospatial statistics ({total} checked)",
                    category="parquet_geo_types",
                )
            return ValidationCheck(
                name=f"native_geo_stats_contains_data_{geom_col}",
                status=CheckStatus.FAILED,
                message=f"{total - within} of {total} geometries fall outside geospatial statistics",
                category="parquet_geo_types",
            )

    except Exception as e:
        return ValidationCheck(
            name=f"native_geo_stats_contains_data_{geom_col}",
            status=CheckStatus.SKIPPED,
            message=f"could not validate geospatial statistics: {e}",
            category="parquet_geo_types",
        )

    return ValidationCheck(
        name=f"native_geo_stats_contains_data_{geom_col}",
        status=CheckStatus.SKIPPED,
        message="no data to validate",
        category="parquet_geo_types",
    )


def _check_native_geo_types_match(
    parquet_file: str, geom_col: str, sample_size: int, con
) -> ValidationCheck:
    """Check that declared geo_types match actual geometry types in the data."""
    from geoparquet_io.core.file_utils import safe_file_url

    try:
        safe_url = safe_file_url(parquet_file, verbose=False)

        # Get declared geo_types from parquet metadata
        escaped_geom_col = _escape_sql_string(geom_col)
        meta_result = con.execute(f"""
            SELECT DISTINCT unnest(geo_types) as geo_type
            FROM parquet_metadata('{safe_url}')
            WHERE path_in_schema = '{escaped_geom_col}'
              AND geo_types IS NOT NULL
        """).fetchall()

        declared_types = {row[0].lower() for row in meta_result if row[0]}

        # Empty list means "not known" - this is valid per spec
        if not declared_types:
            return ValidationCheck(
                name=f"native_geo_types_match_{geom_col}",
                status=CheckStatus.PASSED,
                message="geo_types is empty (types not declared)",
                category="parquet_geo_types",
            )

        # Sample actual geometry types from the data. DuckDB's geo_types
        # naming is dimension-aware ("linestring_m"), so append the matching
        # suffix from ST_HasZ/ST_HasM instead of comparing base names only.
        quoted_col = quote_identifier(geom_col)
        typed_expr = f"ST_GeometryType({quoted_col}) || {zm_suffix_sql(quoted_col, sep='_')}"
        if sample_size == 0:
            # Check all rows
            actual_result = con.execute(f"""
                SELECT DISTINCT {typed_expr} as geom_type
                FROM read_parquet('{safe_url}')
                WHERE {quote_identifier(geom_col)} IS NOT NULL
            """).fetchall()
        else:
            # Sample rows
            actual_result = con.execute(f"""
                SELECT DISTINCT {typed_expr} as geom_type
                FROM (
                    SELECT {quote_identifier(geom_col)}
                    FROM read_parquet('{safe_url}')
                    WHERE {quote_identifier(geom_col)} IS NOT NULL
                    LIMIT {sample_size}
                )
            """).fetchall()

        actual_types = {row[0].lower() for row in actual_result if row[0]}

        # Check if actual types are a subset of declared types
        undeclared = actual_types - declared_types
        if undeclared:
            undeclared_list = ", ".join(sorted(undeclared))
            declared_list = ", ".join(sorted(declared_types))
            return ValidationCheck(
                name=f"native_geo_types_match_{geom_col}",
                status=CheckStatus.FAILED,
                message=f"data contains undeclared geometry types: {undeclared_list}",
                details=f"Declared: [{declared_list}]. Found: [{', '.join(sorted(actual_types))}]",
                category="parquet_geo_types",
            )

        # All actual types are in declared types
        # Capitalize types for display (polygon -> Polygon)
        display_types = [t.title() for t in sorted(declared_types)]
        types_list = ", ".join(display_types)
        checked_msg = f"{sample_size} sampled" if sample_size > 0 else "all"
        return ValidationCheck(
            name=f"native_geo_types_match_{geom_col}",
            status=CheckStatus.PASSED,
            message=f"geo_types [{types_list}] matches data ({checked_msg})",
            category="parquet_geo_types",
        )

    except Exception as e:
        return ValidationCheck(
            name=f"native_geo_types_match_{geom_col}",
            status=CheckStatus.SKIPPED,
            message=f"could not check geo_types: {e}",
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
            logical_type = col.get("logical_type") or ""
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
    # Extract absent-vs-null faithfully: they mean different things here.
    metadata_crs = crs_from_column_meta(col_meta)

    # No crs key at all -> the OGC:CRS84 default, so nothing to inline.
    if metadata_crs is CRS_ABSENT:
        return ValidationCheck(
            name=f"v2_crs_in_parquet_type_{geom_col}",
            status=CheckStatus.PASSED,
            message="using default CRS (OGC:CRS84), no inline CRS required",
            category="geoparquet_2_0",
        )

    # Explicit null -> the CRS is unknown, so there is no PROJJSON to inline and
    # this check has nothing to say. _check_crs_valid is what reports the null.
    if metadata_crs is None:
        return ValidationCheck(
            name=f"v2_crs_in_parquet_type_{geom_col}",
            status=CheckStatus.PASSED,
            message="CRS is explicitly null (unknown), so no inline CRS is possible",
            category="geoparquet_2_0",
        )

    # Explicit CRS84-equivalent metadata is still the default: the native type
    # may omit its crs (which the Parquet spec defines as OGC:CRS84).
    if _is_crs84_equivalent(metadata_crs):
        return ValidationCheck(
            name=f"v2_crs_in_parquet_type_{geom_col}",
            status=CheckStatus.PASSED,
            message="metadata CRS is the default (OGC:CRS84), no inline CRS required",
            category="geoparquet_2_0",
        )

    # Find the Parquet schema CRS
    for col in schema_info:
        if col.get("name") == geom_col:
            logical_type = col.get("logical_type") or ""
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


#: The Parquet logical type's spelling of "this column's CRS is unknown".
#:
#: The GeoParquet spec pairs it with a geo-metadata ``"crs": null``: "When the
#: GeoParquet column-metadata crs is null, the Parquet logical-type crs property
#: SHOULD be set to the string srid:0", and its conformance table lists
#: ``srid:0 | null`` as "CRS undefined or unknown". Since an explicit null is not
#: the OGC:CRS84 default, this pairing is the *only* way a GeoParquet 2.0 file
#: can declare an unknown CRS, so it has to compare equal.
_PARQUET_UNKNOWN_CRS = "srid:0"


def _schema_crs_for_consistency(schema_info: list, geom_col: str) -> Any:
    """The Parquet geo type's CRS, expressed in the geo-metadata vocabulary.

    Returns :data:`CRS_ABSENT` when the logical type declares no CRS (the
    Parquet spec defines that as OGC:CRS84, the same "absent means default" rule
    as the geo metadata), ``None`` for ``srid:0`` (unknown, the pair for an
    explicit ``"crs": null``), else the parsed CRS value.

    A bare ``<authority>:<code>`` (``crs=EPSG:32633``) and an ``srid:<id>``
    reference (which the Parquet spec defines as EPSG:<id>) are resolved to
    PROJJSON here, because the geo-metadata side of the comparison is always
    PROJJSON and a string would never compare equal to it (#814).

    A ``projjson:<key>`` reference names a key in the file's own metadata, which
    this function never sees, so it is deliberately left as the raw string: it
    compares unequal and the check reports a mismatch rather than guessing.
    Conservative, but never a silent false PASS.
    """
    from geoparquet_io.core.duckdb_metadata import (
        parse_geometry_logical_type,
        resolve_authority_code_crs,
    )

    for col in schema_info:
        if col.get("name") != geom_col:
            continue
        parsed = parse_geometry_logical_type(col.get("logical_type") or "")
        if not (parsed and "crs" in parsed):
            return CRS_ABSENT
        crs = parsed["crs"]
        if isinstance(crs, str):
            token = crs.strip()
            if token.lower() == _PARQUET_UNKNOWN_CRS:
                return None
            if token.lower().startswith("srid:"):
                # srid:<id> is EPSG:<id> per the Parquet spec; keep the raw
                # string (fail-closed) when the code doesn't resolve.
                resolved = resolve_authority_code_crs(f"EPSG:{token[len('srid:') :]}")
                return _parse_crs_value(resolved if isinstance(resolved, dict) else crs)
        return _parse_crs_value(resolve_authority_code_crs(crs))
    return CRS_ABSENT


def _check_v2_crs_consistency(geo_meta: dict, schema_info: list, geom_col: str) -> ValidationCheck:
    """Check V2-3: CRS in geo metadata must match CRS in Parquet schema."""
    col_meta = geo_meta.get("columns", {}).get(geom_col, {})
    metadata_crs = crs_from_column_meta(col_meta)
    schema_crs = _schema_crs_for_consistency(schema_info, geom_col)

    # An absent CRS on either side means the default, OGC:CRS84. Resolve both
    # sides before comparing so explicit-CRS84 vs absent doesn't false-fail.
    # An explicit null is NOT the default -- it says the CRS is unknown.
    if _is_crs84_equivalent(metadata_crs) and _is_crs84_equivalent(schema_crs):
        return ValidationCheck(
            name=f"v2_crs_consistency_{geom_col}",
            status=CheckStatus.PASSED,
            message="CRS matches: both are OGC:CRS84 (explicit or default)",
            category="geoparquet_2_0",
        )

    # Both sides unknown (metadata null, Parquet srid:0) lands here and matches,
    # as does every ordinary pair of named CRSs.
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
        details=f"Metadata: {get_crs_display_name(metadata_crs)}, Schema: {get_crs_display_name(schema_crs)}",
        category="geoparquet_2_0",
    )


def _edges_schema_facts(schema_info: list, geom_col: str) -> tuple[bool, bool, str | None]:
    """Return (is_geography, is_planar_geometry, schema_algorithm) for a column."""
    from geoparquet_io.core.duckdb_metadata import parse_geometry_logical_type

    for col in schema_info:
        if col.get("name") != geom_col:
            continue
        logical_type = col.get("logical_type") or ""
        if "GeographyType" in logical_type:
            parsed = parse_geometry_logical_type(logical_type)
            algorithm = parsed.get("algorithm", "spherical") if parsed else None
            return True, False, algorithm
        return False, "GeometryType" in logical_type, None
    return False, False, None


def _check_v2_edges_consistency(
    geo_meta: dict, schema_info: list, geom_col: str
) -> ValidationCheck:
    """Check V2-5: edges in metadata must match algorithm in Parquet GEOGRAPHY type."""
    col_meta = geo_meta.get("columns", {}).get(geom_col, {})
    metadata_edges = col_meta.get("edges", "planar")  # Default is planar

    is_geography, is_planar_geometry, schema_algorithm = _edges_schema_facts(schema_info, geom_col)

    if not is_geography:
        # The Parquet GEOMETRY logical type is defined as planar; a metadata
        # claim of spherical/ellipsoidal edges contradicts it for readers that
        # only honor the logical type. The 2.0 metadata schema still permits
        # this shape (most writers cannot emit the GEOGRAPHY logical type and
        # carry edges in geo metadata instead — gpio's own convert does), so
        # this is an interoperability warning, not a spec violation.
        if is_planar_geometry and metadata_edges != "planar":
            return ValidationCheck(
                name=f"v2_edges_consistency_{geom_col}",
                status=CheckStatus.WARNING,
                message=f"metadata declares non-planar edges '{metadata_edges}' "
                "but column has planar GEOMETRY logical type",
                details="Readers honoring only the Parquet logical type will "
                "treat edges as planar; the GEOGRAPHY logical type expresses "
                "this natively in GeoParquet 2.0",
                category="geoparquet_2_0",
            )
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


def _check_parquet_geo_only_crs(
    schema_info: list, geom_col: str, parquet_file: str
) -> ValidationCheck:
    """Check CRS for parquet-geo-only files (no GeoParquet metadata)."""
    from geoparquet_io.core.duckdb_metadata import (
        parse_geometry_logical_type,
        resolve_crs_reference,
    )

    for col in schema_info:
        if col.get("name") == geom_col:
            logical_type = col.get("logical_type") or ""
            parsed = parse_geometry_logical_type(logical_type)

            if not parsed:
                return ValidationCheck(
                    name=f"parquet_geo_only_crs_{geom_col}",
                    status=CheckStatus.PASSED,
                    message="no CRS specified (defaults to OGC:CRS84)",
                    category="parquet_geo_types",
                )

            raw_crs = parsed.get("crs")

            # No CRS = default OGC:CRS84 = pass
            if raw_crs is None:
                return ValidationCheck(
                    name=f"parquet_geo_only_crs_{geom_col}",
                    status=CheckStatus.PASSED,
                    message="no CRS specified (defaults to OGC:CRS84)",
                    category="parquet_geo_types",
                )

            # Check if CRS uses reference format - warn about compatibility
            # Both projjson: and srid: formats are not widely recognized
            if isinstance(raw_crs, str) and (
                raw_crs.startswith("projjson:") or raw_crs.startswith("srid:")
            ):
                return ValidationCheck(
                    name=f"parquet_geo_only_crs_{geom_col}",
                    status=CheckStatus.WARNING,
                    message=f'column "{geom_col}" CRS format may not be widely recognized',
                    details=f"CRS: {raw_crs}. "
                    "Use 'gpio convert --geoparquet-version 2.0' to standardize.",
                    category="parquet_geo_types",
                )

            # Resolve CRS reference if needed for further checks
            crs = resolve_crs_reference(parquet_file, raw_crs)

            # Check if CRS is geographic (WGS84, EPSG:4326, OGC:CRS84)
            if is_geographic_crs(crs):
                return ValidationCheck(
                    name=f"parquet_geo_only_crs_{geom_col}",
                    status=CheckStatus.PASSED,
                    message="CRS is geographic (widely supported)",
                    category="parquet_geo_types",
                )

            # Check if CRS uses Parquet spec format (inline PROJJSON)
            if isinstance(crs, dict) and ("$schema" in crs or "type" in crs):
                return ValidationCheck(
                    name=f"parquet_geo_only_crs_{geom_col}",
                    status=CheckStatus.PASSED,
                    message="CRS uses valid PROJJSON format",
                    category="parquet_geo_types",
                )

            # Other CRS format - warning
            return ValidationCheck(
                name=f"parquet_geo_only_crs_{geom_col}",
                status=CheckStatus.WARNING,
                message="CRS format may not be widely recognized by geospatial tools",
                details=f"CRS: {get_crs_display_name(crs)}. "
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
# CRS Coordinate Bounds Validation
# =============================================================================


def _extract_epsg_from_dict(crs: dict) -> int | None:
    """Extract EPSG code from PROJJSON dict."""
    crs_id = crs.get("id", {})
    if not isinstance(crs_id, dict):
        return None
    if str(crs_id.get("authority", "")).upper() != "EPSG":
        return None
    try:
        return int(crs_id.get("code", 0))
    except (ValueError, TypeError):
        return None


def _extract_epsg_from_string(crs: str) -> int | None:
    """Extract EPSG code from string CRS."""
    try:
        if crs.startswith("srid:"):
            return int(crs.split(":")[1])
        if crs.upper().startswith("EPSG:"):
            return int(crs.split(":")[1])
    except (ValueError, IndexError):
        pass
    return None


def _extract_epsg_code(crs: Any) -> int | None:
    """Extract EPSG code from CRS in various formats."""
    if crs is None:
        return None
    if isinstance(crs, dict):
        return _extract_epsg_from_dict(crs)
    if isinstance(crs, str):
        return _extract_epsg_from_string(crs)
    return None


def _get_bounds_from_pyproj(epsg_code: int) -> tuple[float, float, float, float] | None:
    """Get CRS bounds from pyproj."""
    try:
        from pyproj import CRS as PyprojCRS

        pyproj_crs = PyprojCRS.from_epsg(epsg_code)
        area = pyproj_crs.area_of_use
        if not area:
            return None

        if pyproj_crs.is_geographic:
            return (area.west, area.south, area.east, area.north)

        # For projected CRS, transform geographic bounds to projected coordinates
        return _transform_bounds_to_projected(pyproj_crs, area)
    except Exception:
        return None


def _transform_bounds_to_projected(
    pyproj_crs: Any, area: Any
) -> tuple[float, float, float, float] | None:
    """Transform geographic bounds to projected CRS coordinates."""
    try:
        from pyproj import CRS as PyprojCRS
        from pyproj import Transformer

        transformer = Transformer.from_crs(PyprojCRS.from_epsg(4326), pyproj_crs, always_xy=True)
        corners = [
            (area.west, area.south),
            (area.west, area.north),
            (area.east, area.south),
            (area.east, area.north),
        ]
        transformed = [transformer.transform(x, y) for x, y in corners]
        xs = [p[0] for p in transformed if p[0] != float("inf")]
        ys = [p[1] for p in transformed if p[1] != float("inf")]
        if xs and ys:
            return (min(xs), min(ys), max(xs), max(ys))
    except Exception:
        pass
    return None


# Standard geographic bounds
_GEOGRAPHIC_BOUNDS = (-180.0, -90.0, 180.0, 90.0)


def _get_crs_bounds(crs: Any) -> tuple[float, float, float, float] | None:
    """
    Get the valid coordinate bounds for a CRS.

    Returns (xmin, ymin, xmax, ymax) or None if bounds cannot be determined.
    """
    # Default CRS (absent key) or an explicit OGC:CRS84 - geographic.
    # An explicit null (unknown CRS) keeps the geographic bounds too: this is a
    # coordinate sanity check, and lon/lat is the only guess worth making. The
    # null itself is reported by _check_crs_valid.
    if crs is CRS_ABSENT or crs is None or _is_ogc_crs84(crs):
        return _GEOGRAPHIC_BOUNDS

    epsg_code = _extract_epsg_code(crs)

    # EPSG:4326 - standard geographic bounds
    if epsg_code == 4326:
        return _GEOGRAPHIC_BOUNDS

    # Try to get bounds from pyproj
    if epsg_code:
        return _get_bounds_from_pyproj(epsg_code)

    return None


def _check_geographic_bounds(
    actual: tuple[float, float, float, float],
    expected: tuple[float, float, float, float],
) -> list[str]:
    """Check if actual bounds are within expected geographic bounds.

    Uses a small tolerance to handle floating-point precision issues
    (e.g., -180.00000001 should be treated as valid -180.0).
    """
    actual_xmin, actual_xmax, actual_ymin, actual_ymax = actual
    expected_xmin, expected_ymin, expected_xmax, expected_ymax = expected

    # Small tolerance for floating-point comparison
    tolerance = 1e-6

    issues = []
    if actual_xmin < expected_xmin - tolerance:
        issues.append(f"min_x={actual_xmin:.4f} < {expected_xmin}")
    if actual_xmax > expected_xmax + tolerance:
        issues.append(f"max_x={actual_xmax:.4f} > {expected_xmax}")
    if actual_ymin < expected_ymin - tolerance:
        issues.append(f"min_y={actual_ymin:.4f} < {expected_ymin}")
    if actual_ymax > expected_ymax + tolerance:
        issues.append(f"max_y={actual_ymax:.4f} > {expected_ymax}")
    return issues


def _check_projected_bounds(
    actual: tuple[float, float, float, float],
    expected: tuple[float, float, float, float],
    tolerance: float = 1.5,
) -> list[str]:
    """Check if actual bounds are reasonably within expected projected bounds."""
    actual_xmin, actual_xmax, actual_ymin, actual_ymax = actual
    expected_xmin, expected_ymin, expected_xmax, expected_ymax = expected

    x_range = expected_xmax - expected_xmin
    y_range = expected_ymax - expected_ymin

    issues = []
    if actual_xmin < expected_xmin - (x_range * tolerance):
        issues.append(
            f"min_x={actual_xmin:.1f} far below expected "
            f"({expected_xmin:.1f} - {expected_xmax:.1f})"
        )
    if actual_xmax > expected_xmax + (x_range * tolerance):
        issues.append(
            f"max_x={actual_xmax:.1f} far above expected "
            f"({expected_xmin:.1f} - {expected_xmax:.1f})"
        )
    if actual_ymin < expected_ymin - (y_range * tolerance):
        issues.append(
            f"min_y={actual_ymin:.1f} far below expected "
            f"({expected_ymin:.1f} - {expected_ymax:.1f})"
        )
    if actual_ymax > expected_ymax + (y_range * tolerance):
        issues.append(
            f"max_y={actual_ymax:.1f} far above expected "
            f"({expected_ymin:.1f} - {expected_ymax:.1f})"
        )
    return issues


def _detect_geographic_in_projected(
    actual: tuple[float, float, float, float],
) -> str | None:
    """Detect if coordinates look like geographic coords in a projected CRS."""
    actual_xmin, actual_xmax, actual_ymin, actual_ymax = actual

    if -180 <= actual_xmin <= 180 and -180 <= actual_xmax <= 180:
        if -90 <= actual_ymin <= 90 and -90 <= actual_ymax <= 90:
            return (
                f"coordinates look geographic ({actual_xmin:.2f},{actual_ymin:.2f} - "
                f"{actual_xmax:.2f},{actual_ymax:.2f}) but CRS is projected"
            )
    return None


def _get_geometry_bounds(
    con, safe_url: str, geom_col: str, limit_clause: str, encoding: Any = "WKB"
) -> tuple[float, float, float, float, int] | None:
    """Query actual coordinate bounds from geometry data."""
    if _is_geoarrow_encoding(encoding):
        query = f"""
            SELECT MIN(xmin) as min_x, MAX(xmax) as max_x,
                   MIN(ymin) as min_y, MAX(ymax) as max_y,
                   COUNT(*) as total
            FROM ({_geoarrow_bounds_subquery(safe_url, geom_col, encoding, limit_clause)})
        """
        result = con.execute(query).fetchone()
        if result is None or result[0] is None:
            return None
        return result[0], result[1], result[2], result[3], result[4]

    # Check if DuckDB already has it as a GEOMETRY type
    col_type = _describe_geom_type(con, safe_url, geom_col)

    geom_expr = (
        quote_identifier(geom_col)
        if "GEOMETRY" in col_type.upper()
        else f"ST_GeomFromWKB({quote_identifier(geom_col)})"
    )

    query = f"""
        SELECT
            MIN(ST_XMin({geom_expr})) as min_x,
            MAX(ST_XMax({geom_expr})) as max_x,
            MIN(ST_YMin({geom_expr})) as min_y,
            MAX(ST_YMax({geom_expr})) as max_y,
            COUNT(*) as total
        FROM (
            SELECT {quote_identifier(geom_col)}
            FROM read_parquet('{safe_url}')
            WHERE {quote_identifier(geom_col)} IS NOT NULL
            {limit_clause}
        )
    """
    result = con.execute(query).fetchone()

    if result is None or result[0] is None:
        return None

    return result[0], result[1], result[2], result[3], result[4]


def _check_coordinates_valid_for_crs(
    parquet_file: str,
    geom_col: str,
    crs: Any,
    con,
    sample_size: int,
    encoding: Any = "WKB",
) -> ValidationCheck:
    """Check that geometry coordinates are within valid bounds for the declared CRS."""
    from geoparquet_io.core.file_utils import safe_file_url

    safe_url = safe_file_url(parquet_file, verbose=False)
    limit_clause = f"LIMIT {sample_size}" if sample_size > 0 else ""
    check_name = f"coordinates_valid_for_crs_{geom_col}"

    # Get expected bounds for the CRS
    crs_bounds = _get_crs_bounds(crs)
    if crs_bounds is None:
        return ValidationCheck(
            name=check_name,
            status=CheckStatus.SKIPPED,
            message="could not determine valid bounds for CRS",
            details=f"CRS: {get_crs_display_name(crs)}",
            category="data_validation",
        )

    try:
        bounds_result = _get_geometry_bounds(con, safe_url, geom_col, limit_clause, encoding)
        if bounds_result is None:
            return ValidationCheck(
                name=check_name,
                status=CheckStatus.SKIPPED,
                message="no geometry data to validate",
                category="data_validation",
            )

        actual_xmin, actual_xmax, actual_ymin, actual_ymax, total = bounds_result
        actual = (actual_xmin, actual_xmax, actual_ymin, actual_ymax)
        is_geo = is_geographic_crs(crs)

        # Check bounds based on CRS type
        geo_warning = None
        if is_geo:
            issues = _check_geographic_bounds(actual, crs_bounds)
        else:
            issues = _check_projected_bounds(actual, crs_bounds)
            # Geographic-looking coords in a projected CRS is only a heuristic:
            # small legitimate projected values (near the origin) look identical
            # to misdeclared lon/lat, so it warns instead of failing. The
            # deterministic mismatch detection is v2_crs_consistency.
            geo_warning = _detect_geographic_in_projected(actual)

        if issues:
            return ValidationCheck(
                name=check_name,
                status=CheckStatus.FAILED,
                message=f"coordinates outside valid range for CRS ({total} checked)",
                details="; ".join(issues),
                category="data_validation",
            )

        if geo_warning:
            return ValidationCheck(
                name=check_name,
                status=CheckStatus.WARNING,
                message=f"possible CRS mismatch ({total} checked)",
                details=geo_warning,
                category="data_validation",
            )

        # Passed
        crs_name = get_crs_display_name(crs)
        msg = "coordinates within valid bounds" if is_geo else "coordinates appear valid"
        return ValidationCheck(
            name=check_name,
            status=CheckStatus.PASSED,
            message=f"{msg} for {crs_name} ({total} checked)",
            category="data_validation",
        )

    except Exception as e:
        if _is_geoarrow_encoding(encoding):
            return _geoarrow_layout_error(check_name, encoding, "geometry coordinates", e)
        return ValidationCheck(
            name=check_name,
            status=CheckStatus.FAILED,
            message=f"failed to validate coordinates: {e}",
            category="data_validation",
        )


# =============================================================================
# Helper Functions
# =============================================================================


def _get_crs_from_schema(schema_info: list, geom_col: str) -> Any:
    """Extract CRS from schema logical type for a geometry column."""
    from geoparquet_io.core.duckdb_metadata import parse_geometry_logical_type

    for col in schema_info:
        if col.get("name") == geom_col:
            logical_type = col.get("logical_type") or ""
            parsed = parse_geometry_logical_type(logical_type)
            if parsed:
                return parsed.get("crs")
            return None
    return None


def _complete_crs_id(crs: dict) -> tuple[str, str] | None:
    """Normalized (authority, code) when both are present and non-empty, else None."""
    crs_id = crs.get("id")
    if not isinstance(crs_id, dict):
        return None
    authority = str(crs_id.get("authority") or "").upper()
    code = str(crs_id.get("code") or "").upper()
    if authority and code:
        return (authority, code)
    return None


def _crs_equals(crs1: Any, crs2: Any) -> bool:
    """Compare two CRS values for equality.

    Pass :data:`CRS_ABSENT` for a column with no ``crs`` key and ``None`` for an
    explicit ``"crs": null``; the spec gives them different meanings (default
    OGC:CRS84 vs unknown CRS) and this helper resolves them accordingly. Use
    :func:`~geoparquet_io.core.crs_utils.crs_from_column_meta` to extract them.
    """
    # An omitted crs key means the OGC:CRS84 default, so it equals anything
    # CRS84-equivalent -- and, being a *known* CRS, differs from an explicit null.
    if crs1 is CRS_ABSENT or crs2 is CRS_ABSENT:
        if crs1 is CRS_ABSENT and crs2 is CRS_ABSENT:
            return True
        return _is_crs84_equivalent(crs2 if crs1 is CRS_ABSENT else crs1)

    # An explicit null means the CRS is unknown: it matches only another
    # unknown, never a CRS the other side actually names.
    if crs1 is None or crs2 is None:
        return crs1 is None and crs2 is None

    # OGC:CRS84 and EPSG:4326 differ only in axis order, and GeoParquet fixes
    # the stored order to (x, y). They describe the same coordinates in every
    # GeoParquet file, in every spelling -- id stub, authority string, URN or
    # full PROJJSON -- so resolve them before the id fast path below, which
    # would otherwise report the single most common equivalent pair as unequal.
    if _is_crs84_equivalent(crs1) and _is_crs84_equivalent(crs2):
        return True

    if not (isinstance(crs1, dict) and isinstance(crs2, dict)):
        # Direct comparison for strings
        return crs1 == crs2

    # When both sides carry a complete authority:code id, trust it (the spec
    # treats the id as authoritative).
    id1 = _complete_crs_id(crs1)
    id2 = _complete_crs_id(crs2)
    if id1 and id2:
        return id1 == id2

    # Structurally identical PROJJSON is equal even if pyproj can't parse it.
    if crs1 == crs2:
        return True

    # At least one side lacks a usable id: compare semantically. Axis order is
    # ignored because GeoParquet fixes coordinate order to (x, y). Fail closed
    # on pyproj errors — an unverifiable CRS must not satisfy a consistency
    # check.
    try:
        from pyproj import CRS as PyprojCRS

        return PyprojCRS.from_json_dict(crs1).equals(
            PyprojCRS.from_json_dict(crs2), ignore_axis_order=True
        )
    except Exception:
        return False


# =============================================================================
# Main Validation Function
# =============================================================================


def _is_read_failure(exc: BaseException | None) -> bool:
    """True when an exception chain indicates an I/O/read failure.

    Distinguishes "the file could not be fetched" (missing path, permissions,
    network) from "the file's content is corrupt" so remote read failures are
    not misreported as corruption. Walks __cause__/__context__ because
    GeoParquetError wraps the underlying error.
    """
    import duckdb

    io_types: tuple[type, ...] = (OSError, duckdb.IOException)
    if hasattr(duckdb, "HTTPException"):
        io_types = (*io_types, duckdb.HTTPException)
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, io_types):
            return True
        exc = exc.__cause__ or exc.__context__
    return False


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
    from geoparquet_io.core.common import detect_geoparquet_file_type
    from geoparquet_io.core.duckdb_metadata import (
        detect_geometry_columns,
        get_geo_metadata,
        get_kv_metadata,
        get_schema_info,
    )
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.logging_config import configure_verbose
    from geoparquet_io.core.remote import needs_httpfs

    configure_verbose(verbose)

    def _metadata_failure_message(exc: Exception) -> str:
        """Honest failure text: an unreadable file is not evidence of corruption."""
        if _is_read_failure(exc):
            return "could not read file (I/O or network error)"
        return "file metadata is not readable (corrupt or invalid encoding)"

    # Auto-detect file type. Corrupt metadata (e.g. invalid UTF-8 in the geo
    # value) must yield a FAILED report, never an exception.
    try:
        file_type_info = detect_geoparquet_file_type(parquet_file, verbose)
    except (GeoParquetError, UnicodeDecodeError, ValueError) as e:
        result = ValidationResult(
            file_path=parquet_file,
            detected_version="unknown",
            target_version=target_version,
        )
        result.checks.append(
            ValidationCheck(
                name="geo_metadata_parse",
                status=CheckStatus.FAILED,
                message=_metadata_failure_message(e),
                details=str(e),
                category="core_metadata",
            )
        )
        return result

    # Determine detected version (always from file, not target)
    detected_version = _determine_version(file_type_info)

    result = ValidationResult(
        file_path=parquet_file,
        detected_version=detected_version,
        target_version=target_version,
    )

    # Check if target version matches detected version (if target specified)
    # Skip version check for parquet-geo-only - it tests Parquet geo types regardless of metadata
    if target_version != "parquet-geo-only":
        version_check = _check_version_matches(detected_version, target_version, file_type_info)
        if version_check:
            result.checks.append(version_check)
            # If version mismatch, return early without running other checks
            if version_check.status == CheckStatus.FAILED:
                return result

    # Get metadata. Corrupt metadata (e.g. invalid UTF-8 in the geo value) must
    # yield a FAILED report, never an exception — rejecting such files is this
    # function's job.
    try:
        kv_metadata = get_kv_metadata(parquet_file)
        geo_meta = get_geo_metadata(parquet_file)
        schema_info = get_schema_info(parquet_file)
        geo_columns = detect_geometry_columns(parquet_file)
    except (GeoParquetError, UnicodeDecodeError, ValueError) as e:
        result.checks.append(
            ValidationCheck(
                name="geo_metadata_parse",
                status=CheckStatus.FAILED,
                message=_metadata_failure_message(e),
                details=str(e),
                category="core_metadata",
            )
        )
        return result

    # A geo key that exists but doesn't parse to a JSON object must fail in
    # ANY mode; without this, auto-detect classifies the file as plain
    # parquet-geo-only and a corrupt file passes validation — and valid-JSON
    # non-object values (list, string, number) would crash downstream checks.
    if not isinstance(geo_meta, dict) and kv_metadata and b"geo" in kv_metadata:
        result.checks.append(
            ValidationCheck(
                name="geo_metadata_parse",
                status=CheckStatus.FAILED,
                message="'geo' metadata key exists but is not a valid JSON object",
                category="core_metadata",
            )
        )
        return result

    # Get DuckDB connection for data validation
    con = None
    try:
        if validate_data:
            con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(parquet_file))
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


def _determine_version(file_type_info: dict) -> str:
    """Determine the detected version string from file type info."""
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


def _versions_match(detected: str, target: str, file_type_info: dict) -> bool:
    """Check if detected version matches target version (strict matching)."""
    file_type = file_type_info.get("file_type", "unknown")

    # parquet-geo-only only matches parquet-geo-only
    if target == "parquet-geo-only":
        return file_type == "parquet_geo_only"

    # parquet-geo-only files don't match any GeoParquet version
    if file_type == "parquet_geo_only":
        return False

    # 1.0 matches 1.0.x files only
    if target == "1.0":
        return file_type == "geoparquet_v1" and detected.startswith("1.0")

    # 1.1 matches 1.1.x files only
    if target == "1.1":
        return file_type == "geoparquet_v1" and detected.startswith("1.1")

    # 2.0 only matches 2.0+ files
    if target == "2.0":
        return file_type == "geoparquet_v2"

    # Exact match fallback
    return detected == target


def _check_version_matches(
    detected_version: str,
    target_version: str | None,
    file_type_info: dict,
) -> ValidationCheck | None:
    """Check if detected version matches target version. Returns None if no target."""
    if not target_version:
        return None

    # Determine if versions match
    matches = _versions_match(detected_version, target_version, file_type_info)

    if matches:
        return ValidationCheck(
            name="version_match",
            status=CheckStatus.PASSED,
            message=f"file version matches requested {target_version}",
            category="version_check",
        )
    else:
        # Special message for parquet-geo-only files validated against GeoParquet versions
        file_type = file_type_info.get("file_type", "unknown")
        if file_type == "parquet_geo_only" and target_version in ["1.0", "1.1", "2.0"]:
            message = (
                "This file contains valid Parquet geo types, but does not implement "
                f"GeoParquet {target_version} metadata"
            )
        else:
            message = f"file is {detected_version}, not {target_version}"

        return ValidationCheck(
            name="version_match",
            status=CheckStatus.FAILED,
            message=message,
            details=f"Run 'gpio check spec' without --geoparquet-version to validate "
            f"as {detected_version}",
            category="version_check",
        )


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

    # If no geometry columns with native geo types were detected, fail
    if not geo_columns:
        checks.append(
            ValidationCheck(
                name="native_geo_type_present",
                status=CheckStatus.FAILED,
                message="no columns with Parquet GEOMETRY/GEOGRAPHY logical type found",
                details="This file does not contain Parquet native geo types. "
                "Use 'gpio convert --geoparquet-version 2.0' to add them.",
                category="parquet_geo_types",
            )
        )
        return checks

    for geom_col in geo_columns.keys():
        # Parquet native geo type checks
        checks.append(_check_native_geo_type_present(schema_info, geom_col))
        checks.append(_check_geography_edges_valid(schema_info, geom_col))
        checks.append(_check_native_geo_statistics(parquet_file, geom_col))

        # Parquet-geo-only specific CRS check (more detailed than _check_native_crs_format)
        checks.append(_check_parquet_geo_only_crs(schema_info, geom_col, parquet_file))

        # Data validation if requested
        if validate_data and con:
            checks.append(_check_native_geo_types_match(parquet_file, geom_col, sample_size, con))
            checks.append(
                _check_native_geo_stats_contains_data(parquet_file, geom_col, con, sample_size)
            )
            checks.append(
                _check_geography_coordinate_bounds(
                    parquet_file, geom_col, schema_info, con, sample_size
                )
            )

            # Check coordinates are valid for declared CRS
            # Get CRS from schema logical type for parquet-geo-only files
            crs = _get_crs_from_schema(schema_info, geom_col)
            checks.append(
                _check_coordinates_valid_for_crs(parquet_file, geom_col, crs, con, sample_size)
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

    if not isinstance(geo_meta, dict):
        checks.append(
            ValidationCheck(
                name="geo_metadata_parse",
                status=CheckStatus.FAILED,
                message="failed to parse 'geo' metadata as a JSON object",
                category="core_metadata",
            )
        )
        return checks

    checks.append(_check_metadata_is_json(geo_meta))
    checks.append(_check_version_present(geo_meta))
    checks.append(_check_version_known(geo_meta))
    checks.append(_check_version_features(parquet_file, geo_meta))
    checks.append(_check_primary_column_present(geo_meta))
    checks.append(_check_columns_present(geo_meta))
    checks.append(_check_primary_column_in_columns(geo_meta))

    columns = geo_meta.get("columns", {})

    # A missing version is reported by _check_version_present above; guard the
    # string comparisons below against None so validation never crashes on it.
    geo_version = file_type_info.get("geo_version") or "1.0.0"

    # Column metadata checks for each geometry column
    for col_name, col_meta in columns.items():
        # The declared encoding decides how the column is laid out, so the
        # schema and data checks below all have to read it.
        encoding = col_meta.get("encoding", "WKB")
        # A GeoArrow encoding the file's version does not permit is reported by
        # _check_encoding_valid; the layout-aware checks must not then treat the
        # column as native, or they would endorse the invalid combination.
        if _is_geoarrow_encoding(encoding) and not _geoarrow_encoding_allowed(geo_version):
            encoding = "WKB"

        checks.append(_check_encoding_valid(col_meta, col_name, geo_version))
        checks.append(_check_geometry_types_list(col_meta, col_name))
        checks.append(_check_crs_valid(col_meta, col_name))
        checks.append(_check_orientation_valid(col_meta, col_name))
        checks.append(_check_edges_valid(col_meta, col_name, geo_version))
        checks.append(_check_bbox_valid(col_meta, col_name))
        checks.append(_check_epoch_valid(col_meta, col_name))

        # Parquet schema checks
        checks.append(_check_geometry_not_grouped(schema_info, col_name, encoding))
        checks.append(_check_geometry_byte_array(schema_info, col_name, encoding))
        checks.append(_check_geometry_not_repeated(schema_info, col_name))

        # Data validation checks
        if validate_data and con:
            checks.append(
                _check_encoding_matches_data(parquet_file, col_name, encoding, con, sample_size)
            )

            geometry_types = col_meta.get("geometry_types", [])
            checks.append(
                _check_geometry_types_match_data(
                    parquet_file, col_name, geometry_types, con, sample_size, encoding
                )
            )

            orientation = col_meta.get("orientation")
            checks.append(
                _check_orientation_matches_data(
                    parquet_file, col_name, orientation, con, sample_size
                )
            )

            bbox = col_meta.get("bbox")
            checks.append(
                _check_bbox_contains_data(parquet_file, col_name, bbox, con, sample_size, encoding)
            )

            # Check coordinates are valid for declared CRS
            crs = col_meta.get("crs")
            checks.append(
                _check_coordinates_valid_for_crs(
                    parquet_file, col_name, crs, con, sample_size, encoding
                )
            )

    # Version-specific checks
    # GeoParquet 1.1 checks - covering was removed in 2.0, so only run for 1.x
    # `covering` was introduced in 1.1 and is not part of the 2.0 spec text, but
    # 2.0 readers must tolerate unknown fields -- so a 2.0 file may still carry
    # one. Where it does, it has to be *correct*: a covering naming a column that
    # does not exist makes readers prune away rows that genuinely match. Gating
    # these checks at "1.1 only" meant gpio validated coverings at 1.1 and
    # skipped the identical defect at 2.0 (#738).
    covering_checks_apply = _version_at_least(geo_version, 1, 1)
    if covering_checks_apply:
        for col_name, col_meta in columns.items():
            checks.append(_check_covering_is_object(col_meta, col_name))

            # Only run bbox covering checks if covering is defined
            covering = col_meta.get("covering")
            if covering is not None and "bbox" in covering:
                checks.append(_check_covering_bbox_paths(col_meta, col_name))
                checks.append(_check_covering_bbox_column_exists(col_meta, col_name, schema_info))
                checks.append(_check_covering_bbox_structure(col_meta, col_name, schema_info))
                checks.append(_check_covering_bbox_field_types(col_meta, col_name, schema_info))

    # File extension check applies to 1.1+
    if _version_at_least(geo_version, 1, 1):
        checks.append(_check_file_extension(parquet_file))

    # GeoParquet 2.0 checks - run Parquet native geo type checks first
    if file_type_info["file_type"] == "geoparquet_v2":
        # Parquet native geo type checks (run first for 2.0)
        for col_name in columns.keys():
            checks.append(_check_native_geo_type_present(schema_info, col_name))
            checks.append(_check_native_crs_format(schema_info, col_name))
            checks.append(_check_geography_edges_valid(schema_info, col_name))
            checks.append(_check_native_geo_statistics(parquet_file, col_name))

            # Data validation checks for native geo types
            if validate_data and con:
                checks.append(
                    _check_native_geo_types_match(parquet_file, col_name, sample_size, con)
                )
                checks.append(
                    _check_native_geo_stats_contains_data(parquet_file, col_name, con, sample_size)
                )
                checks.append(
                    _check_geography_coordinate_bounds(
                        parquet_file, col_name, schema_info, con, sample_size
                    )
                )

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

    console.print("[bold]GeoParquet Validation Report[/bold]")
    console.print("=" * 32)

    # Show detected version
    if result.detected_version:
        console.print(f"Detected: [cyan]{result.detected_version}[/cyan]")
    if result.target_version:
        console.print(f"Validating against: [cyan]{result.target_version}[/cyan]")

    # Group checks by category
    categories: dict[str, list[ValidationCheck]] = {}
    for check in result.checks:
        cat = check.category or "general"
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(check)

    # Category labels and display order
    category_labels = {
        "version_check": "Version Check",
        "core_metadata": "Core Metadata",
        "column_metadata": "Column Validation",
        "parquet_schema": "Parquet Schema",
        "data_validation": "Data Validation",
        "geoparquet_1_1": "GeoParquet 1.1",
        "parquet_geo_types": "Parquet Native Geo Types",
        "geoparquet_2_0": "GeoParquet 2.0 Requirements",
        "core": "Core",
    }

    # Display in a specific order
    # For 2.0 files, show Parquet Native Geo Types first (after Detected and version check)
    if result.detected_version and result.detected_version.startswith("2."):
        category_order = [
            "version_check",
            "parquet_geo_types",
            "core_metadata",
            "column_metadata",
            "parquet_schema",
            "data_validation",
            "geoparquet_1_1",
            "geoparquet_2_0",
        ]
    else:
        category_order = [
            "version_check",
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

    # Summary
    console.print(
        f"\nSummary: [green]{result.passed_count} passed[/green], "
        f"[yellow]{result.warning_count} warnings[/yellow], "
        f"[red]{result.failed_count} failed[/red]"
    )


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
