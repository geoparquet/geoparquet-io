"""Does this file carry a bbox covering column, and does its metadata say so?

Two questions that must be answered together: a bbox struct column in the
schema is only *declared* when the ``geo`` block's ``covering.bbox`` points at
that same column, and a covering pointing somewhere else is a dangling
reference rather than a declaration (#738). ``check_bbox_structure`` answers
both; ``get_bbox_advice`` turns the answer, plus the file's type, into the
recommendation a command gives the user.

Both answers are about the PRIMARY geometry column, because that is what every
caller does with them -- pre-filter on, declare a covering over, regenerate
after a reprojection. A secondary geometry column's bbox struct is the
*secondary*'s envelope, so neither its name nor its own ``covering`` may put it
here (#1171); the rule is the one
:func:`~geoparquet_io.core.geo_metadata.bbox_column_to_declare` applies on the
write side (#953), and its two halves are imported from there rather than spelt
again.
"""

import json
from typing import Literal, TypedDict, cast

from geoparquet_io.core.duckdb_metadata import get_geo_metadata, get_schema_info
from geoparquet_io.core.duckdb_utils import free_column_name
from geoparquet_io.core.file_type import detect_geoparquet_file_type
from geoparquet_io.core.geo_metadata import (
    DEFAULT_GEOPARQUET_VERSION,
    SELF_EVIDENT_BBOX_COLUMN,
    _bbox_claimed_by_another_column,
    _declared_bbox_column,
    bbox_covering_problem,
    covering_supported,
    is_covering_path,
)
from geoparquet_io.core.logging_config import debug, warn
from geoparquet_io.core.parquet_schema import root_schema_columns, schema_direct_children

#: Struct fields a bbox covering column must expose.
_BBOX_REQUIRED_FIELDS = frozenset({"xmin", "ymin", "xmax", "ymax"})


def _covering_bbox_refs(col_info) -> dict | None:
    """One column's validated ``covering.bbox`` refs, or ``None``.

    All four bounds must be present and every ref must be a well-formed
    covering path; a partial or dangling covering is not a declaration (#738).
    """
    if not isinstance(col_info, dict):
        return None
    covering = col_info.get("covering")
    bbox_refs = covering.get("bbox") if isinstance(covering, dict) else None
    if (
        isinstance(bbox_refs, dict)
        and _BBOX_REQUIRED_FIELDS.issubset(bbox_refs)
        and all(is_covering_path(ref) for ref in bbox_refs.values())
    ):
        return bbox_refs
    return None


def _bbox_column_from_covering(geo_meta) -> str | None:
    """Return the bbox column the PRIMARY column's ``covering.bbox`` references.

    The covering metadata is the authoritative pointer to the primary
    geometry's bbox column (its name need not follow any convention), but only
    the primary column's *own* entry speaks for the primary. The lookup used to
    take the first well-formed ``covering.bbox`` of ANY column, so a
    multi-geometry file whose secondary ``boundary`` declares a
    ``boundary_bbox`` handed that struct to every caller as the primary's bbox:
    a Point column filtered, declared and regenerated on a Polygon column's
    extents (#1171, the #953 shape in this second detector). Returns ``None``
    when absent or malformed.
    """
    if _declared_bbox_column(geo_meta) is None:
        return None
    # Non-None means the walk above found the primary's entry, its ``covering``
    # and a ``bbox`` inside it, all objects; the shape of the four axis paths is
    # what is still open, and only a complete, well-formed one is a pointer.
    refs = _covering_bbox_refs(geo_meta["columns"][geo_meta["primary_column"]])
    return cast("str", refs["xmin"][0]) if refs is not None else None


def bbox_covering_column_for(geo_meta, geometry_column: str) -> str | None:
    """The bbox column ``geometry_column``'s ``covering.bbox`` declares, or ``None``.

    The column-scoped counterpart of :func:`_bbox_column_from_covering`, with
    the same validation, for callers that rewrite one geometry column and must
    only touch a covering that column actually declares.
    """
    if not isinstance(geo_meta, dict):
        return None
    columns = geo_meta.get("columns", {})
    if not isinstance(columns, dict):
        return None
    refs = _covering_bbox_refs(columns.get(geometry_column))
    return cast("str", refs["xmin"][0]) if refs is not None else None


def _schema_struct_children(
    schema_info: list[dict], column_name: str
) -> tuple[list[str], list[str]] | None:
    """``(field names, field types)`` of struct column ``column_name``, in schema order.

    None if the column is absent or not a struct. Direct children only: the
    flat ``parquet_schema()`` listing is depth-first, so a nested child's own
    children must not be read as siblings.
    """
    for index, col in enumerate(schema_info):
        if col.get("name") != column_name:
            continue
        if (col.get("num_children") or 0) < 1:
            return None
        children = schema_direct_children(schema_info, index)
        return [c.get("name", "") for c in children], [str(c.get("type", "")) for c in children]
    return None


def _schema_struct_child_field_names(schema_info, column_name) -> list[str] | None:
    """Child field names of struct column ``column_name``, in schema order, or None."""
    children = _schema_struct_children(schema_info, column_name)
    return children[0] if children else None


def bbox_shaped_struct_columns(schema_info: list[dict]) -> list[str]:
    """Every root-level struct column that *looks* like a bbox covering column.

    Shape alone: a root column whose direct children cover
    :data:`_BBOX_REQUIRED_FIELDS`, whatever it is called and whatever the ``geo``
    block says about it. It is deliberately NOT a detector -- answering "which
    column is the primary geometry's bbox" from shape would reopen #1171 the way
    name matching did. It exists so a command can *mention* a struct it is about
    to leave alone: ``convert reproject`` regenerates only the primary's
    declared (or self-evidently named) column, so any other bbox-shaped struct
    comes out of a reprojection still holding source-CRS numbers.
    """
    names = []
    for column in root_schema_columns(schema_info):
        name = column.get("name") or ""
        children = _schema_struct_child_field_names(schema_info, name)
        if children and _BBOX_REQUIRED_FIELDS.issubset(children):
            names.append(name)
    return names


def _find_bbox_column_in_schema(schema_info, verbose, geo_meta=None):
    """The one column name that is self-evidently the PRIMARY geometry's bbox.

    The fallback for a file whose primary declares no ``covering``: with no
    provenance, only the exact conventional name
    (:data:`~geoparquet_io.core.geo_metadata.SELF_EVIDENT_BBOX_COLUMN`) may be
    read as the primary geometry's envelope -- the #738 policy -- and not even
    that when another column's own ``covering`` already claims it. The broader
    read-side names (``bounds``, ``extent``, and any ``*_bbox`` suffix) used to
    match here, so a multi-geometry file's ``boundary_bbox`` -- the SECONDARY
    ``boundary`` column's envelope -- was reported as the primary's bbox column,
    and callers then pre-filtered, declared and regenerated on it (#1171, the
    #953 shape in this second detector).

    Args:
        schema_info: List of column dicts from get_schema_info()
        verbose: Whether to print verbose output
        geo_meta: The file's parsed ``geo`` block, consulted only to see whether
            a non-primary column's ``covering`` already names the candidate.

    Note:
        DuckDB's parquet_schema() returns nested struct fields without parent prefix.
        For a struct column 'bbox' with fields xmin/ymin/xmax/ymax:
        - bbox appears with num_children=4
        - Child fields appear as 'xmin', 'ymin', 'xmax', 'ymax' (not 'bbox.xmin')
    """
    children = _schema_struct_child_field_names(schema_info, SELF_EVIDENT_BBOX_COLUMN)
    if not children or not _BBOX_REQUIRED_FIELDS.issubset(children):
        return None
    if _bbox_claimed_by_another_column(geo_meta, SELF_EVIDENT_BBOX_COLUMN):
        if verbose:
            debug(
                f"Not reading '{SELF_EVIDENT_BBOX_COLUMN}' as the primary's bbox: "
                "another column's covering names it"
            )
        return None
    if verbose:
        debug(f"Found bbox column: {SELF_EVIDENT_BBOX_COLUMN} with children: {children}")
    return SELF_EVIDENT_BBOX_COLUMN


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
                    and _BBOX_REQUIRED_FIELDS.issubset(bbox_refs)
                    and all(is_covering_path(ref) for ref in bbox_refs.values())
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


def _determine_bbox_status(
    has_bbox_column: bool,
    bbox_column_name: str | None,
    has_bbox_metadata: bool,
    covering_problem: str | None = None,
) -> tuple[Literal["optimal", "suboptimal", "poor"], str]:
    """Determine bbox status and message."""
    if has_bbox_column and covering_problem:
        # Before "optimal": a covering over this struct is one `check spec`
        # rejects, whether the file declares it already or would get it (#1035).
        verb = "declares a" if has_bbox_metadata else "cannot get a"
        return (
            "suboptimal",
            f"⚠️  Found bbox column '{bbox_column_name}' that {verb} 'covering': {covering_problem}",
        )
    elif has_bbox_column and has_bbox_metadata:
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
    #: Why no 1.1 ``covering`` may point at that column, or None when one may (#1035).
    covering_problem: str | None
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

    # Find the primary's bbox column: the authoritative covering metadata on its
    # own entry first (spec-valid files may use non-conventional names), then the
    # one self-evident conventional name as the no-provenance fallback (#1171).
    geo_meta = get_geo_metadata(parquet_file)
    bbox_column_name = None
    covering_column = _bbox_column_from_covering(geo_meta)
    if covering_column:
        children = _schema_struct_child_field_names(schema_info, covering_column)
        if children and _BBOX_REQUIRED_FIELDS.issubset(children):
            bbox_column_name = covering_column
            if verbose:
                debug(f"Found bbox column from covering metadata: {covering_column}")
    if bbox_column_name is None:
        bbox_column_name = _find_bbox_column_in_schema(schema_info, verbose, geo_meta)
    has_bbox_column = bbox_column_name is not None

    # Check for bbox covering in the geo metadata
    has_bbox_metadata = _check_bbox_metadata_covering(
        geo_meta, has_bbox_column, verbose, bbox_column_name
    )

    covering_problem = None
    if has_bbox_column:
        struct = _schema_struct_children(schema_info, bbox_column_name)
        covering_problem = bbox_covering_problem(
            bbox_column_name, struct[0] if struct else None, struct[1] if struct else None
        )

    # Determine status and message
    status, message = _determine_bbox_status(
        has_bbox_column, bbox_column_name, has_bbox_metadata, covering_problem
    )

    if verbose:
        debug("\nFinal results:")
        debug(f"  has_bbox_column: {has_bbox_column}")
        debug(f"  bbox_column_name: {bbox_column_name}")
        debug(f"  has_bbox_metadata: {has_bbox_metadata}")
        debug(f"  covering_problem: {covering_problem}")
        debug(f"  status: {status}")
        debug(f"  message: {message}")

    return {
        "has_bbox_column": has_bbox_column,
        "bbox_column_name": bbox_column_name if has_bbox_column else None,
        "has_bbox_metadata": has_bbox_metadata,
        "covering_problem": covering_problem,
        "status": status,
        "message": message,
    }


def resolve_bbox_name(column_names, geoparquet_version, requested="bbox", announce=True) -> str:
    """The name a computed bbox column may take beside ``column_names``.

    ``requested`` when it is free, otherwise the first free ``bbox_<n>`` -- with
    a warning naming the column that took it, as spelled. This is the other half
    of :func:`check_bbox_structure`: when the file has no bbox column gpio can
    use but does have a column of that *name* (a string tile id, a label), the
    computed struct has to move aside or DuckDB renames it silently and the
    ``covering`` ends up pointing at the wrong column (#1079). Every path that
    computes one shares this decision so a collision means one thing across
    ``gpio convert``, ``gpio add bbox`` and ``Table.add_bbox`` (#1176).

    Args:
        column_names: The names the computed column is emitted beside (what the
            query emits, not the raw source's: a CSV's WKT or lat/lon columns
            are consumed and cannot collide)
        geoparquet_version: Output version, which decides whether the warning may
            promise a ``covering``; None reads as the writer's 1.1 default
        requested: The name asked for, ``--bbox-name``'s value where there is one
        announce: False to stay quiet, for a retry that resolves the name again

    Returns:
        The free name, which the caller must use for both the SQL alias and the
        covering it declares
    """
    bbox_name = free_column_name(requested, column_names)
    if bbox_name == requested or not announce:
        return bbox_name
    taken = next(str(name) for name in column_names if str(name).lower() == requested.lower())
    if covering_supported(geoparquet_version or DEFAULT_GEOPARQUET_VERSION):
        outcome = "and declaring the covering over it"
    else:
        outcome = f"(GeoParquet {geoparquet_version} has no covering metadata to declare it)"
    warn(
        f"Input already has a column named '{taken}' that gpio does not recognize "
        f"as a bbox column; writing the computed bbox column as '{bbox_name}' {outcome}"
    )
    return bbox_name


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
