import pyarrow as pa
import shapely.wkb
from shapely.geometry import Point, Polygon

from geoparquet_io.core.geoarrow_encoding import (
    determine_geoarrow_target_type,
    wkb_array_to_geoarrow,
)


def _wkb_array(*geoms):
    return pa.array([shapely.wkb.dumps(g) for g in geoms], type=pa.binary())


def test_single_type_maps_to_native_encoding():
    target, encoding = determine_geoarrow_target_type(["Polygon"])
    assert encoding == "polygon"
    assert target is not None
    assert target.extension_name == "geoarrow.polygon"


def test_polygon_and_multipolygon_promote_to_multipolygon():
    target, encoding = determine_geoarrow_target_type(["Polygon", "MultiPolygon"])
    assert encoding == "multipolygon"
    assert target.extension_name == "geoarrow.multipolygon"


def test_incompatible_types_fall_back_to_wkb():
    target, encoding = determine_geoarrow_target_type(["Point", "Polygon"])
    assert target is None
    assert encoding == "WKB"


def test_empty_geometry_types_falls_back_to_wkb():
    target, encoding = determine_geoarrow_target_type([])
    assert target is None
    assert encoding == "WKB"


def test_no_crs_attached_when_input_crs_default():
    """Default path (no input_crs) must not attach a CRS to the target type."""
    target, _ = determine_geoarrow_target_type(["Point"])
    assert target.crs is None


def test_crs_is_attached_to_target_type():
    crs = {"type": "GeographicCRS", "name": "Custom"}
    target, _ = determine_geoarrow_target_type(["Point"], input_crs=crs)
    assert target.extension_name == "geoarrow.point"
    # CRS round-trips through the geoarrow type metadata
    assert "Custom" in str(target.crs)


def test_wkb_array_converts_to_target_type():
    target, _ = determine_geoarrow_target_type(["Polygon"])
    arr = _wkb_array(Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]))
    out = wkb_array_to_geoarrow(arr, target)
    assert out.type.extension_name == "geoarrow.polygon"
    assert len(out) == 1


def test_wkb_array_preserves_nulls():
    target, _ = determine_geoarrow_target_type(["Point"])
    arr = pa.array([shapely.wkb.dumps(Point(1, 2)), None], type=pa.binary())
    out = wkb_array_to_geoarrow(arr, target)
    assert out.type.extension_name == "geoarrow.point"
    assert out.null_count == 1


def test_z_dimension_selects_xyz_native_type():
    """A Z-suffixed type must select an XYZ native type, not 2D (no data loss)."""
    import geoarrow.types as gat

    target, encoding = determine_geoarrow_target_type(["Point Z"])
    assert encoding == "point"
    assert target.dimensions == gat.Dimensions.XYZ


def test_mixed_dimensions_fall_back_to_wkb():
    """Mixing 2D and 3D types in one column cannot map to a single native type -> WKB."""
    target, encoding = determine_geoarrow_target_type(["Point", "Point Z"])
    assert target is None
    assert encoding == "WKB"


def test_z_wkb_converts_without_dropping_z():
    """End-to-end: 3D WKB -> XYZ native array preserves the Z ordinate."""
    import geoarrow.pyarrow as ga

    target, _ = determine_geoarrow_target_type(["Point Z"])
    arr = pa.array(
        [shapely.wkb.dumps(Point(1, 2, 3)), shapely.wkb.dumps(Point(4, 5, 6))],
        type=pa.binary(),
    )
    out = wkb_array_to_geoarrow(arr, target)
    assert "POINT Z (1 2 3)" in ga.format_wkt(out)[0].as_py()
