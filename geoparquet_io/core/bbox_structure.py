"""Does this file carry a bbox covering column, and does its metadata say so?

Two questions that must be answered together: a bbox struct column in the
schema is only *declared* when the ``geo`` block's ``covering.bbox`` points at
that same column, and a covering pointing somewhere else is a dangling
reference rather than a declaration (#738). ``check_bbox_structure`` answers
both; ``get_bbox_advice`` turns the answer, plus the file's type, into the
recommendation a command gives the user.
"""

import json
from typing import Literal, TypedDict

from geoparquet_io.core.duckdb_metadata import get_geo_metadata, get_schema_info
from geoparquet_io.core.file_type import detect_geoparquet_file_type
from geoparquet_io.core.logging_config import debug

#: Struct fields a bbox covering column must expose.
_BBOX_REQUIRED_FIELDS = frozenset({"xmin", "ymin", "xmax", "ymax"})


def _bbox_column_from_covering(geo_meta) -> str | None:
    """Return the bbox column name referenced by GeoParquet ``covering.bbox``.

    The covering metadata is the authoritative pointer to a file's bbox column
    (its name need not follow any convention). Returns ``None`` when absent or
    malformed.
    """
    if not isinstance(geo_meta, dict):
        return None
    columns = geo_meta.get("columns", {})
    if not isinstance(columns, dict):
        return None
    for col_info in columns.values():
        if not isinstance(col_info, dict):
            continue
        covering = col_info.get("covering")
        bbox_refs = covering.get("bbox") if isinstance(covering, dict) else None
        if (
            isinstance(bbox_refs, dict)
            and _BBOX_REQUIRED_FIELDS.issubset(bbox_refs)
            and all(isinstance(ref, list) and len(ref) == 2 for ref in bbox_refs.values())
        ):
            return bbox_refs["xmin"][0]
    return None


def _schema_struct_child_names(schema_info, column_name) -> set | None:
    """Child field names of struct column ``column_name`` from a flat
    ``parquet_schema()`` listing; ``None`` if the column is absent or not a struct."""
    for i, col in enumerate(schema_info):
        if col.get("name") != column_name:
            continue
        num_children = col.get("num_children") or 0
        if num_children < 1:
            return None
        return {
            schema_info[i + j].get("name", "")
            for j in range(1, num_children + 1)
            if i + j < len(schema_info)
        }
    return None


def _find_bbox_column_in_schema(schema_info, verbose):
    """Find bbox column in schema by conventional names or structure.

    Args:
        schema_info: List of column dicts from get_schema_info()
        verbose: Whether to print verbose output

    Note:
        DuckDB's parquet_schema() returns nested struct fields without parent prefix.
        For a struct column 'bbox' with fields xmin/ymin/xmax/ymax:
        - bbox appears with num_children=4
        - Child fields appear as 'xmin', 'ymin', 'xmax', 'ymax' (not 'bbox.xmin')
    """
    # Check for columns ending with these suffixes (e.g., geometry_bbox, bbox)
    conventional_suffixes = ["bbox", "bounds", "extent"]
    required_fields = {"xmin", "ymin", "xmax", "ymax"}

    for i, col in enumerate(schema_info):
        name = col.get("name", "")
        num_children = col.get("num_children", 0)

        if not name:
            continue

        # Check if column name ends with conventional suffixes and has struct children
        is_bbox_name = any(name.endswith(suffix) for suffix in conventional_suffixes)
        if is_bbox_name and num_children >= 4:
            # Get the next num_children entries as the struct's child fields
            child_names = set()
            for j in range(1, num_children + 1):
                if i + j < len(schema_info):
                    child_name = schema_info[i + j].get("name", "")
                    child_names.add(child_name)

            # Check if all required fields are present
            if required_fields.issubset(child_names):
                if verbose:
                    debug(f"Found bbox column: {name} with children: {child_names}")
                return name

    return None


def _check_bbox_metadata_covering(geo_meta, has_bbox_column, verbose, bbox_column_name=None):
    """Check if geo metadata contains proper bbox covering.

    Args:
        geo_meta: Parsed geo metadata dict (from get_geo_metadata())
        has_bbox_column: Whether a bbox column was found in schema
        verbose: Whether to print verbose output
        bbox_column_name: The bbox column actually present in the file. A
            covering only counts as declaring *this* file's bbox column when it
            names it; a covering pointing at some other (or absent) column is a
            dangling reference, and treating it as "declared" let a broken file
            pass `check` while the success message named a different column
            entirely (#738).
    """
    if not (geo_meta and has_bbox_column):
        return False

    if verbose:
        debug("\nParsed geo metadata:")
        debug(json.dumps(geo_meta, indent=2))

    # Validation-shared, so the block stays as the file really holds it (#883's
    # line) and this guards instead of sanitizing: `gpio check bbox` and `check
    # all` report through here. A `columns` that is not an object, or a
    # `covering` that is not one, simply declares no covering -- the truthful
    # answer, where iterating it raised an `AttributeError` from someone else's
    # file (#947).
    columns = geo_meta.get("columns") if isinstance(geo_meta, dict) else None
    if isinstance(columns, dict):
        for _col_name, col_info in columns.items():
            covering = col_info.get("covering") if isinstance(col_info, dict) else None
            if isinstance(covering, dict) and covering.get("bbox"):
                bbox_refs = covering["bbox"]
                # Check if the bbox covering has the required structure
                if (
                    isinstance(bbox_refs, dict)
                    and all(key in bbox_refs for key in ["xmin", "ymin", "xmax", "ymax"])
                    and all(isinstance(ref, list) and len(ref) == 2 for ref in bbox_refs.values())
                ):
                    referenced_bbox_column = bbox_refs["xmin"][0]
                    if bbox_column_name is not None and referenced_bbox_column != bbox_column_name:
                        if verbose:
                            debug(
                                f"Covering references column '{referenced_bbox_column}', "
                                f"but the file's bbox column is '{bbox_column_name}' - "
                                "treating the bbox column as undeclared"
                            )
                        continue
                    if verbose:
                        debug(
                            f"Found bbox covering in metadata referencing column: {referenced_bbox_column}"
                        )
                    return True

    return False


def _determine_bbox_status(has_bbox_column, bbox_column_name, has_bbox_metadata):
    """Determine bbox status and message."""
    if has_bbox_column and has_bbox_metadata:
        return "optimal", f"✓ Found bbox column '{bbox_column_name}' with proper metadata covering"
    elif has_bbox_column:
        return (
            "suboptimal",
            f"⚠️  Found bbox column '{bbox_column_name}' but no bbox covering metadata (recommended for better performance)",
        )
    else:
        return "poor", "❌ No valid bbox column found"


class BboxInfo(TypedDict, total=False):
    """Bbox structure information returned by check_bbox_structure."""

    has_bbox_column: bool
    bbox_column_name: str | None
    has_bbox_metadata: bool
    status: Literal["optimal", "suboptimal", "poor", "native"]
    message: str


def check_bbox_structure(parquet_file, verbose=False) -> BboxInfo:
    """
    Check bbox structure and metadata coverage in a GeoParquet file.

    Returns:
        dict: Results including:
            - has_bbox_column (bool): Whether a valid bbox struct column exists
            - bbox_column_name (str): Name of the bbox column if found
            - has_bbox_metadata (bool): Whether bbox covering is specified in metadata
            - status (str): "optimal", "suboptimal", or "poor"
            - message (str): Human readable description
    """
    # Get schema info using DuckDB
    schema_info = get_schema_info(parquet_file)

    if verbose:
        debug("\nSchema fields:")
        for col in schema_info:
            name = col.get("name", "")
            col_type = col.get("type", "")
            if name:  # Skip empty names
                debug(f"  {name}: {col_type}")

    # Find the bbox column: the authoritative covering metadata first (spec-valid
    # files may use non-conventional names), then the naming-convention fallback.
    geo_meta = get_geo_metadata(parquet_file)
    bbox_column_name = None
    covering_column = _bbox_column_from_covering(geo_meta)
    if covering_column:
        children = _schema_struct_child_names(schema_info, covering_column)
        if children and _BBOX_REQUIRED_FIELDS.issubset(children):
            bbox_column_name = covering_column
            if verbose:
                debug(f"Found bbox column from covering metadata: {covering_column}")
    if bbox_column_name is None:
        bbox_column_name = _find_bbox_column_in_schema(schema_info, verbose)
    has_bbox_column = bbox_column_name is not None

    # Check for bbox covering in the geo metadata
    has_bbox_metadata = _check_bbox_metadata_covering(
        geo_meta, has_bbox_column, verbose, bbox_column_name
    )

    # Determine status and message
    status, message = _determine_bbox_status(has_bbox_column, bbox_column_name, has_bbox_metadata)

    if verbose:
        debug("\nFinal results:")
        debug(f"  has_bbox_column: {has_bbox_column}")
        debug(f"  bbox_column_name: {bbox_column_name}")
        debug(f"  has_bbox_metadata: {has_bbox_metadata}")
        debug(f"  status: {status}")
        debug(f"  message: {message}")

    return {
        "has_bbox_column": has_bbox_column,
        "bbox_column_name": bbox_column_name if has_bbox_column else None,
        "has_bbox_metadata": has_bbox_metadata,
        "status": status,
        "message": message,
    }


def get_bbox_advice(
    parquet_file: str,
    operation: str,
    verbose: bool = False,
) -> dict:
    """
    Get version-aware bbox optimization advice.

    Provides context-aware recommendations based on file type and operation:
    - For GeoParquet 2.0/parquet-geo with spatial_filtering: No bbox pre-filter
      needed -- a bare ST_Intersects ON clause lets DuckDB's SPATIAL_JOIN operator
      engage, which is dramatically faster than a bbox-overlap NL join (issue #538).
    - For GeoParquet 2.0/parquet-geo with bounds_calculation: bbox still recommended (faster)
    - For GeoParquet 1.x without bbox: Suggest adding bbox OR upgrading to 2.0

    Args:
        parquet_file: Path to the parquet file
        operation: One of:
            - "spatial_filtering": For ST_Intersects, spatial joins, etc.
            - "bounds_calculation": For centroid, extent, quadkey, etc.
            - "check": For validation/inspection
        verbose: Whether to print verbose output

    Returns:
        dict with:
            - needs_warning: bool - Whether to show a warning to the user
            - skip_bbox_prefilter: bool - Whether to skip bbox pre-filtering in queries.
              Only True for spatial_filtering with native geometry. There a bare
              ST_Intersects ON clause is recognized by DuckDB's SPATIAL_JOIN operator
              and is much faster than ANDing a bbox-overlap test in front of it, which
              defeats SPATIAL_JOIN and forces a BLOCKWISE_NL_JOIN (issue #538). For
              bounds_calculation, always False since the bbox column provides
              pre-computed values that are faster than geometry stats.
            - has_native_geometry: bool - Whether file uses native Parquet geometry types
            - message: str - User-facing message (if needs_warning)
            - suggestions: list[str] - Suggested actions for the user
    """
    file_info = detect_geoparquet_file_type(parquet_file, verbose)
    bbox_info = check_bbox_structure(parquet_file, verbose)

    # Read native-geometry capability directly rather than inferring from file_type:
    # a 1.1-geoarrow file carries native geometry columns but is labelled
    # "geoparquet_v1" (version starts with "1."), so a file_type membership check
    # would misclassify it as non-native. .get() also avoids a KeyError.
    has_native_geo = file_info.get("has_native_geo_types", False)
    has_bbox = bbox_info["has_bbox_column"]

    # Only skip bbox pre-filtering for spatial_filtering operations with native geometry.
    # For bounds_calculation, bbox column provides pre-computed values that are faster.
    skip_bbox = has_native_geo and operation == "spatial_filtering"

    result = {
        "needs_warning": False,
        "skip_bbox_prefilter": skip_bbox,
        "has_native_geometry": has_native_geo,
        "has_bbox_column": has_bbox,
        "bbox_column_name": bbox_info.get("bbox_column_name"),
        "message": "",
        "suggestions": [],
    }

    if operation == "spatial_filtering":
        if has_native_geo:
            # Native geometry -> bare ST_Intersects ON clause engages DuckDB's
            # SPATIAL_JOIN operator (the fast path, issue #538) - no warning needed.
            if verbose:
                debug("Native geometry: bare ST_Intersects engages DuckDB SPATIAL_JOIN")
        elif not has_bbox:
            # 1.x without bbox - warn and suggest options
            result["needs_warning"] = True
            result["message"] = "No bbox column found"
            result["suggestions"] = [
                "Add a bbox column: gpio add bbox <file>",
                "Or upgrade to GeoParquet 2.0: gpio convert <file> --geoparquet-version 2.0",
            ]

    elif operation == "bounds_calculation":
        # bbox column is still faster for bounds/centroid calculation (pre-computed values)
        if not has_bbox:
            result["needs_warning"] = True
            result["message"] = "No bbox column - computing from geometry (slower)"
            result["suggestions"] = [
                "Add a bbox column for 3-4x faster bounds/centroid: gpio add bbox <file>"
            ]

    elif operation == "check":
        if has_native_geo:
            # Native geometry - bbox optional but can help with bounds queries
            if not has_bbox and verbose:
                debug("Native geometry type detected - bbox column optional for spatial queries")
        elif not has_bbox:
            # 1.x without bbox
            result["needs_warning"] = True
            result["message"] = "No bbox column found"
            result["suggestions"] = [
                "Add a bbox column: gpio add bbox <file>",
                "Or upgrade to GeoParquet 2.0: gpio convert <file> --geoparquet-version 2.0",
            ]

    return result
