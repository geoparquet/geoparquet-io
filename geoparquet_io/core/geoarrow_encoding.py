"""GeoArrow field introspection, and WKB-to-native conversion for 1.1-geoarrow output.

Pure pyarrow/geoarrow utilities (no DuckDB, no Click). The conversion half is
used by the arrow-streaming write strategy to emit nested-coordinate GeoArrow
encoding; the introspection half (``arrow_extension_name``,
``is_geoarrow_extension_field``, ``is_wkb_extension_field``) is the single
definition of "is this field GeoArrow?" -- and of the narrower "is it WKB
bytes?" -- shared by the write strategies, the metadata paths and streaming.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from geoparquet_io.core.crs_utils import PARQUET_UNKNOWN_CRS, is_default_crs, is_unknown_type_crs
from geoparquet_io.core.logging_config import debug

if TYPE_CHECKING:
    import pyarrow as pa


def arrow_extension_name(field: pa.Field) -> str | None:
    """Extension name of a field, whether PyArrow resolved it or left it as metadata.

    ``geoarrow.pyarrow`` registers its extension types process-globally on
    import, so the same column arrives either as a resolved extension type
    (registered) or as plain storage carrying ``ARROW:extension:name`` in the
    field metadata (not registered). Some producers -- DuckDB's Arrow export,
    and gpio's own ``add`` operations -- hand back the metadata-only shape even
    when the type is registered.

    Both shapes are the same column, and DuckDB honours the field metadata on
    ``register()``, so any decision keyed on "is this geoarrow?" has to read
    both (#688, #727, #792).

    Args:
        field: PyArrow field to inspect

    Returns:
        The extension name, or None when the field declares none.
    """
    name = getattr(field.type, "extension_name", None)
    if name is not None:
        return str(name)
    raw = (field.metadata or {}).get(b"ARROW:extension:name")
    return raw.decode("utf-8") if raw else None


# Extension names that mean "this column holds WKB bytes", as opposed to a
# native nested GeoArrow geometry (``geoarrow.point`` over ``struct<x, y>``) or a
# text one (``geoarrow.wkt`` over ``string``). Only these carry bytes that a
# plain-``binary`` field can hold unchanged.
WKB_EXTENSION_NAMES = frozenset({"geoarrow.wkb", "ogc.wkb"})


def is_geoarrow_extension_field(field: pa.Field) -> bool:
    """True when a field declares a GeoArrow extension type, in either carrier shape.

    Takes a *field* rather than a type on purpose: shape (2) puts the marker on
    the field's metadata, which a bare ``pa.DataType`` cannot see. Keying off
    the resolved type alone made the answer depend on whether anything in the
    process had imported ``geoarrow.pyarrow`` (#792).

    Deliberately broad -- *any* ``geoarrow.*`` name. This answers "is there
    native geo in this table?", which is what version detection needs. It is the
    wrong question for anything that then rewrites the column: use
    ``is_wkb_extension_field`` there.
    """
    name = arrow_extension_name(field)
    return name is not None and name.startswith("geoarrow.")


def native_wkb_type(crs: dict | str | None, edges: str | None = None):
    """The ``geoarrow.wkb`` type a 2.0 / parquet-geo-only geometry column is written as.

    PyArrow writes this extension type as a native Parquet GEOMETRY logical
    type, which 2.0 requires for every column in ``geo["columns"]`` and which is
    a parquet-geo-only column's *only* geometry identity (#706, #764).

    The CRS belongs in the type at these versions, and is taken from what the
    ``geo`` block declares for that column, so the two can never disagree
    (``v2_crs_consistency``). A default CRS is signalled by carrying none, the
    same way the metadata signals it by omitting the key.

    One definition, shared by every strategy that builds the type: the streaming
    writer's output schema and the disk-rewrite writer's.
    """
    import geoarrow.pyarrow as ga

    geoarrow_type = ga.wkb()
    if is_unknown_type_crs(crs):
        # An unknown CRS is `srid:0` in the type; bare would mean OGC:CRS84 (#1276).
        geoarrow_type = geoarrow_type.with_crs(PARQUET_UNKNOWN_CRS)
    elif crs and not is_default_crs(crs):
        geoarrow_type = geoarrow_type.with_crs(crs)
    # A column the block declares spherical is a GEOGRAPHY; typing it a planar
    # GEOMETRY beside `edges: spherical` makes the file disagree with itself.
    if edges == "spherical":
        geoarrow_type = geoarrow_type.with_edge_type(ga.EdgeType.SPHERICAL)
    return geoarrow_type


def is_wkb_extension_field(field: pa.Field) -> bool:
    """True when a field declares a WKB extension type, in either carrier shape.

    The narrow counterpart of ``is_geoarrow_extension_field``, and the right gate
    for every path that rewrites the column to plain ``binary``. A GeoArrow name
    alone does not mean WKB bytes, and casting on the broad predicate is
    destructive in two different ways:

    * ``geoarrow.point`` over ``struct<x, y>`` cannot be cast to binary at all --
      PyArrow raises ``ArrowNotImplementedError`` mid-write;
    * ``geoarrow.wkt`` over ``string`` casts happily, and leaves WKT *text* in a
      binary column whose ``geo`` block still says ``encoding: WKT``.
    """
    return arrow_extension_name(field) in WKB_EXTENSION_NAMES


# GeoParquet geometry_types base names (including Multi* types) -> geoarrow.pyarrow
# factory attribute names. geometry_type_common handles promotion and unification.
_BASE_NAME_TO_FACTORY = {
    "point": "point",
    "linestring": "linestring",
    "polygon": "polygon",
    "multipoint": "multipoint",
    "multilinestring": "multilinestring",
    "multipolygon": "multipolygon",
}

# geoarrow.types.Dimensions enum values (see Dimensions.value).
_DIM_XY = 1
# Dimension codes that map to a concrete native GeoArrow coordinate layout.
_NATIVE_DIM_CODES = {1, 2, 3, 4}  # XY, XYZ, XYM, XYZM


def _normalize_base_name(geometry_type: str) -> str:
    """Strip Z/M dimension suffixes and lowercase: 'MultiPolygon Z' -> 'multipolygon'."""
    return geometry_type.split(" ")[0].strip().lower()


def _resolve_dimension(dimensions: set[int] | None):
    """Resolve a set of geoarrow dimension codes to a single target dimension.

    Native GeoArrow types carry one fixed coordinate layout, so a column must use
    a single dimension. This avoids silently coercing Z/M ordinates away.

    Args:
        dimensions: geoarrow dimension codes present in the data (Dimensions.value),
            or None/empty when the dimensionality is unknown.

    Returns:
        (dimension_enum, ok). ``ok`` is False when the data cannot be represented
        by one native type (mixed dimensions, or an unknown/unspecified code) so the
        caller must fall back to WKB. ``dimension_enum`` is None for plain XY (keep
        the factory default) or a ``geoarrow.types.Dimensions`` member otherwise.
    """
    if not dimensions:
        return None, True  # unknown -> keep default 2D layout
    if not dimensions.issubset(_NATIVE_DIM_CODES):
        return None, False  # unspecified/unknown dimension -> WKB
    if len(dimensions) > 1:
        return None, False  # mixed XY/XYZ/XYM/XYZM -> WKB (no lossless single type)

    import geoarrow.types as gat

    code = next(iter(dimensions))
    if code == _DIM_XY:
        return None, True
    return gat.Dimensions(code), True


def determine_geoarrow_target_type(
    geometry_types: list[str],
    input_crs: dict | None = None,
    dimensions: set[int] | None = None,
):
    """Determine the single GeoArrow target type for a dataset.

    Args:
        geometry_types: GeoParquet geometry_types strings, e.g. ["Polygon", "MultiPolygon"].
        input_crs: PROJJSON dict to attach to the type (skipped when default CRS).
        dimensions: geoarrow dimension codes (Dimensions.value) present in the data.
            Used to select a Z/M-aware native type so 3D/measured coordinates are not
            dropped. When the data mixes dimensions (or has none representable as a
            single native type), the result falls back to WKB.

    Returns:
        (geoarrow_type, encoding_name). geoarrow_type is None and encoding_name
        is "WKB" when the set is empty or cannot be unified into one native type.
    """
    import geoarrow.pyarrow as ga

    base_names = {_normalize_base_name(g) for g in geometry_types if g}
    if not base_names or not base_names.issubset(_BASE_NAME_TO_FACTORY):
        return None, "WKB"

    dimension_enum, dimension_ok = _resolve_dimension(dimensions)
    if not dimension_ok:
        debug(f"GeoArrow dimensions not representable as one native type ({dimensions}); using WKB")
        return None, "WKB"

    types = []
    for name in base_names:
        factory = getattr(ga, _BASE_NAME_TO_FACTORY[name])
        types.append(factory())

    try:
        common = types[0] if len(types) == 1 else ga.geometry_type_common(types)
    except (ValueError, TypeError) as exc:  # incompatible mix (e.g. point + polygon)
        debug(f"GeoArrow types not unifiable ({base_names}); falling back to WKB: {exc}")
        return None, "WKB"

    if common.extension_name == "geoarrow.wkb":
        return None, "WKB"

    if dimension_enum is not None:
        common = common.with_dimensions(dimension_enum)

    if input_crs and not is_default_crs(input_crs) and not is_unknown_type_crs(input_crs):
        common = common.with_crs(input_crs)

    encoding = common.extension_name.replace("geoarrow.", "")
    return common, encoding


def detect_wkb_dimensions(arr) -> set[int]:
    """Detect the geoarrow dimension codes present in a WKB/binary/geoarrow array.

    Returns a set of ``geoarrow.types.Dimensions`` values (1=XY, 2=XYZ, 3=XYM,
    4=XYZM). Returns an empty set for empty/all-null input or on any detection
    failure, in which case the caller treats the data as plain 2D.
    """
    import geoarrow.pyarrow as ga

    try:
        if len(arr) == 0:
            return set()
        arr = arr.drop_null()  # geoarrow errors on null geometries
        if len(arr) == 0:
            return set()
        types_struct = ga.unique_geometry_types(ga.as_wkb(arr))
        return {d for d in types_struct.field("dimensions").to_pylist() if d is not None}
    except Exception as exc:  # geoarrow C++ errors, invalid WKB, etc.
        debug(f"Could not detect WKB dimensions; assuming 2D: {exc}")
        return set()


def wkb_array_to_geoarrow(arr, target_type):
    """Convert a WKB/binary Arrow array to the given native GeoArrow target type."""
    import geoarrow.pyarrow as ga

    return ga.as_geoarrow(ga.as_wkb(arr), type=target_type)
