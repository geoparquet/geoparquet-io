"""The geodesic bounding box must contain the edges, not just the vertices (#1272).

A planar min/max over the vertices does not bound a geodesic edge: a long
east-west great-circle edge at high latitude bulges poleward. These tests hold
``core/geodesic_bbox`` to the spec's reading of a GEOGRAPHY bounding box --
canonical lon/lat ranges, ``xmin > xmax`` for an antimeridian wrap -- and to
the property that makes a bbox usable for pruning at all: every point a
reader would treat as part of the geometry is inside it.

The ground truth for SPHERICAL edges is the definition of a spherical edge:
the great-circle arc between the vertices, sampled densely by slerp.
"""

from __future__ import annotations

import math
import random

import pytest
import shapely

from geoparquet_io.core.geodesic_bbox import (
    ELLIPSOIDAL_LAT_EPSILON_DEG,
    geodesic_bbox_of_wkb,
    lon_range_contains,
)


def _wkb(wkt: str) -> bytes:
    return shapely.to_wkb(shapely.from_wkt(wkt))


def bbox_of(wkt: str, edges: str = "spherical"):
    return geodesic_bbox_of_wkb([_wkb(wkt)], edges=edges)


def _lon_in(bbox, lon: float) -> bool:
    xmin, _, xmax, _ = bbox
    if xmin <= xmax:
        return xmin - 1e-9 <= lon <= xmax + 1e-9
    return lon >= xmin - 1e-9 or lon <= xmax + 1e-9


def _contains(bbox, lon: float, lat: float) -> bool:
    return _lon_in(bbox, lon) and bbox[1] - 1e-9 <= lat <= bbox[3] + 1e-9


def _unit(lon: float, lat: float):
    lon, lat = math.radians(lon), math.radians(lat)
    return (
        math.cos(lat) * math.cos(lon),
        math.cos(lat) * math.sin(lon),
        math.sin(lat),
    )


def _slerp_lonlat(a, b, steps: int = 200):
    """Dense samples of the great-circle arc between two lon/lat points."""
    va, vb = _unit(*a), _unit(*b)
    dot = max(-1.0, min(1.0, sum(x * y for x, y in zip(va, vb, strict=True))))
    omega = math.acos(dot)
    if omega < 1e-12:
        return [a]
    out = []
    for i in range(steps + 1):
        t = i / steps
        sa = math.sin((1 - t) * omega) / math.sin(omega)
        sb = math.sin(t * omega) / math.sin(omega)
        x, y, z = (sa * va[j] + sb * vb[j] for j in range(3))
        out.append((math.degrees(math.atan2(y, x)), math.degrees(math.asin(max(-1, min(1, z))))))
    return out


class TestAcceptance:
    """The issue's acceptance cases, verbatim."""

    def test_a_high_latitude_edge_bulges_poleward(self):
        """LINESTRING(-30 60, 30 60): the great circle peaks at ~63.43 N."""
        bbox = bbox_of("LINESTRING (-30 60, 30 60)")
        assert bbox[3] == pytest.approx(63.4349, abs=0.01)
        assert bbox[1] == pytest.approx(60.0, abs=1e-9)
        assert bbox[0] == pytest.approx(-30.0)
        assert bbox[2] == pytest.approx(30.0)

    def test_a_southern_edge_bulges_toward_the_south_pole(self):
        bbox = bbox_of("LINESTRING (-30 -60, 30 -60)")
        assert bbox[1] == pytest.approx(-63.4349, abs=0.01)
        assert bbox[3] == pytest.approx(-60.0, abs=1e-9)

    def test_an_antimeridian_crossing_edge_wraps(self):
        bbox = bbox_of("LINESTRING (170 10, -170 15)")
        xmin, _, xmax, _ = bbox
        assert xmin > xmax, f"expected a wrapped box, got [{xmin}, {xmax}]"
        assert xmin == pytest.approx(170.0)
        assert xmax == pytest.approx(-170.0)

    def test_a_ring_enclosing_the_north_pole_reaches_it(self):
        bbox = bbox_of("POLYGON ((0 80, 90 80, 180 80, -90 80, 0 80))")
        assert bbox[3] == 90.0
        assert (bbox[0], bbox[2]) == (-180.0, 180.0)
        assert bbox[1] == pytest.approx(80.0, abs=1e-6)

    def test_a_ring_enclosing_the_south_pole_reaches_it(self):
        bbox = bbox_of("POLYGON ((0 -80, -90 -80, 180 -80, 90 -80, 0 -80))")
        assert bbox[1] == -90.0
        assert (bbox[0], bbox[2]) == (-180.0, 180.0)

    def test_results_are_clamped_to_canonical_ranges(self):
        bbox = bbox_of("LINESTRING (-179 89, 179 89)")
        assert -90.0 <= bbox[1] and bbox[3] <= 90.0
        assert -180.0 <= bbox[0] <= 180.0 and -180.0 <= bbox[2] <= 180.0


class TestGeometryKinds:
    def test_points_have_no_bulge(self):
        assert bbox_of("POINT (10 20)") == (10.0, 20.0, 10.0, 20.0)

    def test_multipoint_is_the_vertex_box(self):
        bbox = bbox_of("MULTIPOINT ((10 20), (30 -5))")
        assert bbox == (10.0, -5.0, 30.0, 20.0)

    def test_a_z_geometry_parses(self):
        bbox = bbox_of("LINESTRING Z (-30 60 5, 30 60 7)")
        assert bbox[3] == pytest.approx(63.4349, abs=0.01)

    def test_an_m_geometry_parses(self):
        bbox = bbox_of("LINESTRING M (-30 60 1, 30 60 2)")
        assert bbox[3] == pytest.approx(63.4349, abs=0.01)

    def test_a_zm_geometry_parses(self):
        bbox = bbox_of("POINT ZM (1 2 3 4)")
        assert bbox == (1.0, 2.0, 1.0, 2.0)

    def test_a_collection_unions_its_parts(self):
        bbox = bbox_of("GEOMETRYCOLLECTION (POINT (-10 0), LINESTRING (0 60, 20 60))")
        assert bbox[0] == pytest.approx(-10.0)
        assert bbox[3] > 60.0

    def test_a_multipolygon_with_an_interior_ring(self):
        bbox = bbox_of("MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 3 2, 3 3, 2 3, 2 2)))")
        assert bbox[0] == pytest.approx(0.0)
        assert bbox[2] == pytest.approx(10.0)

    def test_an_empty_geometry_bounds_nothing(self):
        assert geodesic_bbox_of_wkb([_wkb("LINESTRING EMPTY")]) is None

    def test_nulls_are_skipped(self):
        assert geodesic_bbox_of_wkb([None, _wkb("POINT (1 2)")]) == (1.0, 2.0, 1.0, 2.0)

    def test_ewkb_with_an_srid_parses(self):
        geom = shapely.from_wkt("LINESTRING (-30 60, 30 60)")
        shapely.set_srid(geom, 4326)
        ewkb = shapely.to_wkb(shapely.set_srid(geom, 4326), flavor="extended", include_srid=True)
        bbox = geodesic_bbox_of_wkb([ewkb])
        assert bbox[3] == pytest.approx(63.4349, abs=0.01)

    def test_unparsable_wkb_yields_none(self):
        assert geodesic_bbox_of_wkb([b"\x01\x99\x00\x00\x00"]) is None
        assert geodesic_bbox_of_wkb([b""]) is None

    def test_an_unknown_geometry_kind_yields_none(self):
        # A well-formed header whose kind (99) no WKB spec defines.
        crafted = bytes([1]) + (99).to_bytes(4, "little") + (0).to_bytes(4, "little")
        assert geodesic_bbox_of_wkb([crafted]) is None

    def test_a_dimension_code_past_zm_yields_none(self):
        crafted = bytes([1]) + (4001).to_bytes(4, "little") + bytes(16)
        assert geodesic_bbox_of_wkb([crafted]) is None

    def test_wkb_nested_past_the_depth_limit_yields_none(self):
        collection = shapely.from_wkt("GEOMETRYCOLLECTION (POINT (1 2))")
        for _ in range(40):
            collection = shapely.geometrycollections([collection])
        assert geodesic_bbox_of_wkb([shapely.to_wkb(collection)]) is None

    def test_an_empty_point_bounds_nothing(self):
        assert geodesic_bbox_of_wkb([_wkb("POINT EMPTY")]) is None

    def test_a_single_vertex_chain_is_a_point(self):
        # A one-point linestring is not valid WKT, so craft the WKB: kind 2,
        # one vertex at (5, 6).
        import struct as _struct

        crafted = (
            bytes([1])
            + (2).to_bytes(4, "little")
            + (1).to_bytes(4, "little")
            + _struct.pack("<2d", 5.0, 6.0)
        )
        assert geodesic_bbox_of_wkb([crafted]) == (5.0, 6.0, 5.0, 6.0)

    def test_a_repeated_vertex_adds_nothing(self):
        bbox = bbox_of("LINESTRING (0 0, 0 0, 10 10)")
        assert bbox == pytest.approx((0.0, 0.0, 10.0, 10.0))

    def test_geometry_after_a_pole_ring_stays_inside_the_globe_box(self):
        bbox = geodesic_bbox_of_wkb(
            [
                _wkb("POLYGON ((0 80, 90 80, 180 80, -90 80, 0 80))"),
                _wkb("LINESTRING (10 -20, 20 -25)"),
            ]
        )
        assert (bbox[0], bbox[2], bbox[3]) == (-180.0, 180.0, 90.0)
        assert bbox[1] == pytest.approx(-25.0, abs=0.1)

    def test_antipodal_endpoints_are_bounded_conservatively(self):
        """The geodesic between antipodes is undefined; the bound is the globe."""
        bbox = bbox_of("LINESTRING (0 0, 180 0)")
        assert _contains(bbox, 90.0, 0.0) or (bbox[0], bbox[2]) == (-180.0, 180.0)

    def test_a_pole_crossing_meridian_edge_includes_the_pole(self):
        """Same longitude 180 apart: the shortest path runs over a pole."""
        bbox = bbox_of("LINESTRING (0 80, 180 80)")
        assert bbox[3] == 90.0


class TestRowGroupReading:
    """`row_group_geography_bboxes` bounds one row group at a time."""

    @staticmethod
    def _native_file(tmp_path, values: list):
        import geoarrow.pyarrow as ga
        import pyarrow as pa
        import pyarrow.parquet as pq

        path = tmp_path / "native.parquet"
        array = pa.ExtensionArray.from_storage(ga.wkb(), pa.array(values, pa.binary()))
        pq.write_table(pa.table({"geometry": array}), str(path), row_group_size=1)
        return path

    def test_each_row_group_gets_its_own_box(self, tmp_path):
        from geoparquet_io.core.geodesic_bbox import row_group_geography_bboxes

        path = self._native_file(tmp_path, [_wkb("POINT (1 2)"), _wkb("POINT (3 4)")])
        per_group, whole = row_group_geography_bboxes(str(path), "geometry", "spherical")

        assert per_group == [(1.0, 2.0, 1.0, 2.0), (3.0, 4.0, 3.0, 4.0)]
        assert whole == (1.0, 2.0, 3.0, 4.0)

    def test_a_corrupt_row_group_poisons_the_file_bound(self, tmp_path):
        """A whole-file bound that skipped rows would under-cover; refuse it."""
        from geoparquet_io.core.geodesic_bbox import row_group_geography_bboxes

        path = self._native_file(tmp_path, [_wkb("POINT (1 2)"), b"\x07junk"])
        per_group, whole = row_group_geography_bboxes(str(path), "geometry", "spherical")

        assert per_group == [(1.0, 2.0, 1.0, 2.0), None]
        assert whole is None

    def test_an_unboundable_column_is_left_out_of_the_file_level_answer(self, tmp_path):
        from geoparquet_io.core.geodesic_bbox import geography_statistics

        path = self._native_file(tmp_path, [_wkb("POINT (1 2)"), b"\x07junk"])
        per_group, file_level = geography_statistics(str(path), {"geometry": "spherical"})

        assert per_group == {"geometry": [(1.0, 2.0, 1.0, 2.0), None]}
        assert file_level == {}

    def test_an_all_null_row_group_is_left_out_of_the_union(self, tmp_path):
        from geoparquet_io.core.geodesic_bbox import row_group_geography_bboxes

        path = self._native_file(tmp_path, [_wkb("POINT (1 2)"), None])
        per_group, whole = row_group_geography_bboxes(str(path), "geometry", "spherical")

        assert per_group == [(1.0, 2.0, 1.0, 2.0), None]
        assert whole == (1.0, 2.0, 1.0, 2.0)


class TestLonNormalization:
    def test_longitudes_beyond_the_range_are_wrapped(self):
        assert bbox_of("POINT (190 10)") == (-170.0, 10.0, -170.0, 10.0)
        assert bbox_of("POINT (-190 10)") == (170.0, 10.0, 170.0, 10.0)

    def test_a_positive_odd_multiple_of_180_stays_east(self):
        assert bbox_of("POINT (540 0)") == (180.0, 0.0, 180.0, 0.0)


class TestLonRangeContains:
    def test_a_full_range_contains_everything(self):
        assert lon_range_contains((-180.0, 180.0), (170.0, -170.0))

    def test_a_wrapped_outer_contains_a_wrapped_inner(self):
        assert lon_range_contains((160.0, -160.0), (170.0, -170.0))

    def test_a_wrapped_outer_rejects_the_far_side(self):
        assert not lon_range_contains((160.0, -160.0), (-10.0, 10.0))

    def test_a_plain_outer_rejects_a_wider_inner(self):
        assert not lon_range_contains((0.0, 10.0), (0.0, 20.0))
        assert lon_range_contains((0.0, 20.0), (5.0, 10.0))


class TestEllipsoidalAlgorithms:
    def test_the_spherical_bound_is_padded(self):
        spherical = bbox_of("LINESTRING (-30 60, 30 60)")
        vincenty = bbox_of("LINESTRING (-30 60, 30 60)", edges="vincenty")
        assert vincenty[3] == pytest.approx(spherical[3] + ELLIPSOIDAL_LAT_EPSILON_DEG)
        assert vincenty[1] == pytest.approx(spherical[1] - ELLIPSOIDAL_LAT_EPSILON_DEG)
        assert (vincenty[0], vincenty[2]) == (spherical[0], spherical[2])

    def test_the_pad_is_clamped_at_the_poles(self):
        bbox = bbox_of("POLYGON ((0 80, 90 80, 180 80, -90 80, 0 80))", edges="karney")
        assert bbox[3] == 90.0


class TestPruningSafety:
    """A bbox-pruning reader must never skip a row that intersects (#1272).

    Containment of every point of every edge is what makes pruning safe: if a
    query window intersects the geometry, it contains one of its points, and
    every point is inside the bbox -- so the bbox intersects the window and
    the row group is kept. The arc samples are the definition of SPHERICAL
    edges, so they are the ground truth.
    """

    def test_every_point_of_every_edge_is_inside_the_box(self):
        rng = random.Random(1272)
        for _ in range(150):
            coords = [
                (rng.uniform(-180, 180), rng.uniform(-85, 85)) for _ in range(rng.randint(2, 5))
            ]
            wkt = "LINESTRING (" + ", ".join(f"{lon} {lat}" for lon, lat in coords) + ")"
            bbox = bbox_of(wkt)
            for a, b in zip(coords, coords[1:], strict=False):
                for lon, lat in _slerp_lonlat(a, b):
                    assert _contains(bbox, lon, lat), (
                        f"({lon}, {lat}) of edge {a}->{b} escapes {bbox} for {wkt}"
                    )

    def test_high_latitude_edges_near_the_antimeridian(self):
        rng = random.Random(999)
        for _ in range(60):
            lon1 = rng.uniform(150, 180)
            lon2 = rng.uniform(-180, -150)
            lat1, lat2 = rng.uniform(55, 85), rng.uniform(55, 85)
            bbox = bbox_of(f"LINESTRING ({lon1} {lat1}, {lon2} {lat2})")
            for lon, lat in _slerp_lonlat((lon1, lat1), (lon2, lat2)):
                assert _contains(bbox, lon, lat)
