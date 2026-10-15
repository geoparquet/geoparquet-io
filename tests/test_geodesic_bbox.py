"""The geodesic bounding box must contain the edges, not just the vertices (#1272).

A planar min/max over the vertices does not bound a geodesic edge: a long
east-west great-circle edge at high latitude bulges poleward. These tests hold
``core/geodesic_bbox`` to the spec's reading of a GEOGRAPHY bounding box --
canonical lon/lat ranges, ``xmin > xmax`` for an antimeridian wrap -- and to
the property that makes a bbox usable for pruning at all: every point a
reader would treat as part of the geometry is inside it.

The ground truth is independent of the module under test: each edge is
densified with ``pyproj.Geod.npts`` -- a sphere for SPHERICAL edges, the WGS84
ellipsoid for the ellipsoidal algorithms -- and every sample must fall inside
the box. A bound that pads, guesses or skips a branch fails here, not only a
bound that is computed "the same way" as the expectation.
"""

from __future__ import annotations

import math
import random
import struct

import pytest
import shapely
from pyproj import Geod

from geoparquet_io.core.geodesic_bbox import (
    LAT_MARGIN_DEG,
    geodesic_bbox_of_wkb,
    row_group_geography_bboxes,
)

SPHERE = Geod(a=6371008.8, f=0.0)
WGS84 = Geod(ellps="WGS84")
GLOBE = (-180.0, -90.0, 180.0, 90.0)


def _wkb(wkt: str) -> bytes:
    return shapely.to_wkb(shapely.from_wkt(wkt))


def bbox_of(wkt: str, edges: str = "spherical", orientation: str | None = None):
    return geodesic_bbox_of_wkb([_wkb(wkt)], edges=edges, orientation=orientation)


def _line(points) -> bytes:
    out = struct.pack("<BII", 1, 2, len(points))
    for x, y in points:
        out += struct.pack("<dd", x, y)
    return out


def _lon_in(bbox, lon: float, tol: float = 1e-9) -> bool:
    xmin, _, xmax, _ = bbox
    if not -180.0 <= lon <= 180.0:
        lon = ((lon + 180.0) % 360.0) - 180.0
    if xmin <= xmax:
        if xmin - tol <= lon <= xmax + tol:
            return True
        # -180 and 180 are one meridian.
        return abs(lon) == 180.0 and (xmin <= -180.0 + tol or xmax >= 180.0 - tol)
    return lon >= xmin - tol or lon <= xmax + tol


def _contains(bbox, lon: float, lat: float, tol: float = 1e-9) -> bool:
    if not bbox[1] - tol <= lat <= bbox[3] + tol:
        return False
    # At a pole every longitude is the same point.
    return abs(lat) > 90.0 - 1e-7 or _lon_in(bbox, lon, tol)


def _edges(coords):
    return list(zip(coords, coords[1:], strict=False))


def _oracle_misses(bbox, edges, geod: Geod, npts: int = 1000):
    """Densified points of each edge that escape ``bbox`` (independent of the module)."""
    misses = []
    for a, b in edges:
        if (a[0], a[1]) == (b[0], b[1]):
            samples = [a]
        else:
            samples = [a, b, *geod.npts(a[0], a[1], b[0], b[1], npts)]
        for lon, lat in samples:
            if bbox is None or not _contains(bbox, lon, lat):
                misses.append((a, b, lon, lat))
    return misses


class TestAcceptance:
    """The issue's acceptance cases."""

    def test_a_high_latitude_edge_bulges_poleward(self):
        """LINESTRING(-30 60, 30 60): the great circle peaks at ~63.43 N."""
        bbox = bbox_of("LINESTRING (-30 60, 30 60)")
        true_max = math.degrees(math.atan(math.tan(math.radians(60)) / math.cos(math.radians(30))))
        assert true_max <= bbox[3] <= true_max + 2 * LAT_MARGIN_DEG
        assert bbox[1] == 60.0
        assert (bbox[0], bbox[2]) == (-30.0, 30.0)

    def test_a_southern_edge_bulges_toward_the_south_pole(self):
        bbox = bbox_of("LINESTRING (-30 -60, 30 -60)")
        assert bbox[1] == pytest.approx(-63.4349, abs=0.01)
        assert bbox[3] == -60.0

    def test_an_antimeridian_crossing_edge_wraps(self):
        bbox = bbox_of("LINESTRING (170 10, -170 15)")
        assert (bbox[0], bbox[2]) == (170.0, -170.0)

    def test_results_are_clamped_to_canonical_ranges(self):
        bbox = bbox_of("LINESTRING (-179 89, 179 89)")
        assert -90.0 <= bbox[1] and bbox[3] <= 90.0
        assert -180.0 <= bbox[0] <= 180.0 and -180.0 <= bbox[2] <= 180.0


class TestUnreadableCoordinates:
    """A non-finite or out-of-range coordinate bounds nothing -- never NaN, never a raise."""

    @pytest.mark.parametrize(
        "points",
        [
            [(0.0, 0.0), (float("nan"), 10.0), (20.0, 20.0)],
            [(0.0, 0.0), (10.0, float("nan"))],
            [(float("inf"), 0.0), (20.0, 10.0)],
            [(0.0, float("-inf")), (20.0, 10.0)],
            [(0.0, 100.0), (10.0, 0.0)],
        ],
        ids=["nan-lon", "nan-lat", "inf-lon", "inf-lat", "lat-100"],
    )
    @pytest.mark.parametrize("edges", ["spherical", "karney"])
    def test_a_bad_vertex_yields_none(self, points, edges):
        assert geodesic_bbox_of_wkb([_line(points)], edges=edges) is None

    def test_a_half_nan_point_yields_none(self):
        assert geodesic_bbox_of_wkb([struct.pack("<BIdd", 1, 1, float("nan"), 1.0)]) is None

    def test_a_bad_value_poisons_its_neighbours(self):
        """A bound that skipped the bad row would under-cover it."""
        good = _wkb("POINT (1 2)")
        assert geodesic_bbox_of_wkb([good, _line([(0, 0), (float("nan"), 1)])]) is None

    def test_a_row_group_with_a_nan_vertex_gets_no_box(self, tmp_path):
        path = _native_file(tmp_path, [_wkb("POINT (1 2)"), _line([(0, 0), (float("nan"), 1)])])
        bounds = row_group_geography_bboxes(str(path), "geometry", "spherical")
        assert bounds.per_group == [(1.0, 2.0, 1.0, 2.0), None]
        assert bounds.file_bbox is None
        assert bounds.unreadable_groups == 1


class TestGeometryKinds:
    def test_points_have_no_bulge(self):
        assert bbox_of("POINT (10 20)") == (10.0, 20.0, 10.0, 20.0)

    def test_points_get_no_ellipsoidal_pad(self):
        assert bbox_of("POINT (0 2)", edges="karney") == (0.0, 2.0, 0.0, 2.0)

    def test_multipoint_is_the_vertex_box(self):
        assert bbox_of("MULTIPOINT ((10 20), (30 -5))") == (10.0, -5.0, 30.0, 20.0)

    @pytest.mark.parametrize(
        "wkt",
        ["LINESTRING Z (-30 60 5, 30 60 7)", "LINESTRING M (-30 60 1, 30 60 2)"],
    )
    def test_z_and_m_geometries_parse(self, wkt):
        assert bbox_of(wkt)[3] == pytest.approx(63.4349, abs=0.01)

    def test_a_zm_point_parses(self):
        assert bbox_of("POINT ZM (1 2 3 4)") == (1.0, 2.0, 1.0, 2.0)

    def test_big_endian_wkb_parses(self):
        geom = shapely.from_wkt("POLYGON ((-30 60, 30 60, 30 50, -30 50, -30 60))")
        bbox = geodesic_bbox_of_wkb([shapely.to_wkb(geom, byte_order=0)])
        assert bbox[3] == pytest.approx(63.4349, abs=0.01)

    def test_a_collection_unions_its_parts(self):
        bbox = bbox_of(
            "GEOMETRYCOLLECTION (POINT (-10 0), MULTILINESTRING ((0 60, 20 60)), "
            "GEOMETRYCOLLECTION (POLYGON ((0 0, 1 0, 1 1, 0 0))))"
        )
        assert bbox[0] == -10.0
        assert bbox[1] == 0.0
        assert bbox[3] > 60.0

    def test_a_multipolygon_with_an_interior_ring(self):
        bbox = bbox_of("MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 3 2, 3 3, 2 3, 2 2)))")
        assert (bbox[0], bbox[2]) == (0.0, 10.0)
        assert bbox[1] < 1e-9 and bbox[3] >= 10.0

    @pytest.mark.parametrize(
        "wkt", ["LINESTRING EMPTY", "POINT EMPTY", "POLYGON EMPTY", "GEOMETRYCOLLECTION EMPTY"]
    )
    def test_an_empty_geometry_bounds_nothing(self, wkt):
        assert geodesic_bbox_of_wkb([_wkb(wkt)]) is None

    def test_nulls_are_skipped(self):
        assert geodesic_bbox_of_wkb([None, _wkb("POINT (1 2)")]) == (1.0, 2.0, 1.0, 2.0)

    def test_ewkb_with_an_srid_parses(self):
        geom = shapely.set_srid(shapely.from_wkt("LINESTRING (-30 60, 30 60)"), 4326)
        ewkb = shapely.to_wkb(geom, flavor="extended", include_srid=True)
        assert geodesic_bbox_of_wkb([ewkb])[3] == pytest.approx(63.4349, abs=0.01)

    def test_ewkb_z_polygon_parses(self):
        geom = shapely.set_srid(
            shapely.from_wkt("POLYGON Z ((-30 60 1, 30 60 1, 0 50 1, -30 60 1))"), 4326
        )
        ewkb = shapely.to_wkb(geom, flavor="extended", include_srid=True)
        assert geodesic_bbox_of_wkb([ewkb])[3] == pytest.approx(63.4349, abs=0.01)

    @pytest.mark.parametrize(
        "crafted",
        [
            b"\x01\x99\x00\x00\x00",
            b"",
            b"\x07junk",
            bytes([1]) + (99).to_bytes(4, "little") + (0).to_bytes(4, "little"),
            bytes([1]) + (4001).to_bytes(4, "little") + bytes(16),
            _line([(0, 0), (10, 10)])[:-5],
            struct.pack("<BII", 1, 2, 2**31) + bytes(32),
            struct.pack("<BIII", 1, 3, 1, 4) + bytes(16),
        ],
        ids=[
            "kind-99-short",
            "empty",
            "junk",
            "kind-99",
            "dim-4",
            "truncated",
            "huge-count",
            "short-ring",
        ],
    )
    def test_unparsable_wkb_yields_none(self, crafted):
        assert geodesic_bbox_of_wkb([crafted]) is None

    def test_wkb_nested_past_the_depth_limit_yields_none(self):
        collection = shapely.from_wkt("GEOMETRYCOLLECTION (POINT (1 2))")
        for _ in range(40):
            collection = shapely.geometrycollections([collection])
        assert geodesic_bbox_of_wkb([shapely.to_wkb(collection)]) is None

    def test_a_single_vertex_chain_is_a_point(self):
        assert geodesic_bbox_of_wkb([_line([(5.0, 6.0)])]) == (5.0, 6.0, 5.0, 6.0)

    def test_an_unclosed_ring_is_bounded_with_its_closing_edge(self):
        """A reader closes the ring; the closing edge's bulge must be inside the box."""
        crafted = struct.pack("<BIII", 1, 3, 1, 3) + struct.pack(
            "<6d", -30.0, 50.0, 0.0, 40.0, 30.0, 60.0
        )
        bbox = geodesic_bbox_of_wkb([crafted])
        assert not _oracle_misses(bbox, [((30.0, 60.0), (-30.0, 50.0))], SPHERE)

    def test_a_repeated_vertex_adds_nothing(self):
        assert bbox_of("LINESTRING (0 0, 0 0, 10 10)")[0::2] == (0.0, 10.0)

    def test_values_of_mixed_buffer_types(self):
        values = [bytearray(_wkb("POINT (1 2)")), memoryview(_wkb("POINT (3 4)"))]
        assert geodesic_bbox_of_wkb(values) == (1.0, 2.0, 3.0, 4.0)

    def test_a_non_bytes_value_yields_none(self):
        assert geodesic_bbox_of_wkb([12345]) is None

    def test_an_unknown_edges_name_is_a_caller_error(self):
        with pytest.raises(ValueError, match="edges"):
            geodesic_bbox_of_wkb([_wkb("POINT (1 2)")], edges="planar")


class TestDegenerateEdges:
    """Zero-length and pole edges are not antipodal; antipodal ones bound the globe."""

    def test_seam_duplicates_are_not_the_globe(self):
        bbox = bbox_of("LINESTRING (180 10, -180 10, -170 10)")
        assert (bbox[0], bbox[2]) == (-180.0, -170.0)
        assert bbox[1] == 10.0 and bbox[3] < 10.1

    def test_coincident_points_at_a_pole_are_not_the_globe(self):
        bbox = bbox_of("LINESTRING (10 90, -10 90, 0 80)")
        assert bbox[1] == 80.0 and bbox[3] == 90.0

    def test_exactly_antipodal_endpoints_bound_the_globe(self):
        """Every great circle through antipodes is "the" geodesic."""
        assert bbox_of("LINESTRING (0 0, 180 0)") == GLOBE
        assert bbox_of("LINESTRING (10 20, -170 -20)") == GLOBE

    def test_a_pole_crossing_meridian_edge_includes_the_pole(self):
        bbox = bbox_of("LINESTRING (0 80, 180 80)")
        assert bbox[3] == 90.0 and (bbox[0], bbox[2]) == (-180.0, 180.0)
        assert bbox[1] == 80.0

    def test_a_vertex_at_a_pole_covers_every_longitude(self):
        assert bbox_of("POINT (5 90)") == (-180.0, 90.0, 180.0, 90.0)


class TestPoleEnclosure:
    """Which side of a ring is the interior decides whether a pole is in the box.

    gpio's outputs usually carry no ``orientation``; the GeoParquet spec says
    readers then typically take the smaller of the two regions a ring bounds.
    A declared ``counterclockwise`` puts the interior on the left.
    """

    NORTH_EAST = "POLYGON ((0 80, 90 80, 180 80, -90 80, 0 80))"
    NORTH_WEST = "POLYGON ((0 80, -90 80, 180 80, 90 80, 0 80))"
    SOUTH_EAST = "POLYGON ((0 -70, 90 -70, 180 -70, -90 -70, 0 -70))"
    SOUTH_WEST = "POLYGON ((0 -80, -90 -80, 180 -80, 90 -80, 0 -80))"

    @pytest.mark.parametrize("ring", [NORTH_EAST, NORTH_WEST])
    def test_an_unoriented_north_cap_is_the_cap(self, ring):
        bbox = bbox_of(ring)
        assert bbox[3] == 90.0 and (bbox[0], bbox[2]) == (-180.0, 180.0)
        assert bbox[1] == 80.0

    @pytest.mark.parametrize("ring, top", [(SOUTH_EAST, -70.0), (SOUTH_WEST, -80.0)])
    def test_an_unoriented_south_cap_is_the_cap(self, ring, top):
        bbox = bbox_of(ring)
        assert bbox[1] == -90.0 and (bbox[0], bbox[2]) == (-180.0, 180.0)
        assert bbox[3] == top

    def test_a_ccw_ring_wound_eastward_encloses_the_north_pole(self):
        bbox = bbox_of(self.NORTH_EAST, orientation="counterclockwise")
        assert (bbox[1], bbox[3]) == (80.0, 90.0)

    def test_a_ccw_ring_wound_westward_is_everything_south_of_it(self):
        bbox = bbox_of(self.NORTH_WEST, orientation="counterclockwise")
        assert bbox[1] == -90.0
        assert 80.0 < bbox[3] < 90.0  # the edges' poleward bulge, not the pole

    def test_the_pole_triangle_keeps_its_cap(self):
        """POLYGON((0 80,-120 80,120 80,0 80)) is a cap around the north pole."""
        assert bbox_of("POLYGON ((0 80, -120 80, 120 80, 0 80))")[3] == 90.0

    def test_a_small_clockwise_square_is_the_square_when_unoriented(self):
        bbox = bbox_of("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")
        assert (bbox[0], bbox[2]) == (0.0, 1.0)
        assert bbox[1] == 0.0 and bbox[3] < 1.01

    def test_a_small_clockwise_square_is_its_complement_when_ccw(self):
        """Interior on the left of a clockwise square is the rest of the globe."""
        bbox = bbox_of("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", orientation="counterclockwise")
        assert bbox == GLOBE

    def test_a_hole_around_a_pole_does_not_add_the_other_pole(self):
        bbox = bbox_of(
            "POLYGON ((0 -60, 90 -60, 180 -60, -90 -60, 0 -60), "
            "(0 -80, -90 -80, 180 -80, 90 -80, 0 -80))"
        )
        assert bbox[3] == -60.0
        assert bbox[1] == -90.0

    @pytest.mark.parametrize("orientation", [None, "counterclockwise"])
    def test_antarctica_drawn_to_minus_ninety(self, orientation):
        """The planar-drawn Antarctica rectangle, counterclockwise in lon/lat."""
        ring = "POLYGON ((-180 -90, 180 -90, 180 -60, 90 -60, 0 -60, -90 -60, -180 -60, -180 -90))"
        bbox = bbox_of(ring, orientation=orientation)
        assert (bbox[1], bbox[3]) == (-90.0, -60.0)
        assert (bbox[0], bbox[2]) == (-180.0, 180.0)

    def test_antarctica_wound_clockwise_is_still_the_cap_when_unoriented(self):
        ring = "POLYGON ((-180 -90, -180 -60, -90 -60, 0 -60, 90 -60, 180 -60, 180 -90, -180 -90))"
        assert bbox_of(ring)[1::2] == (-90.0, -60.0)

    @pytest.mark.parametrize(
        "ring",
        [
            "POLYGON ((-180 90, -180 60, -90 60, 0 60, 90 60, 180 60, 180 90, -180 90))",
            "POLYGON ((-180 90, 180 90, 180 60, 90 60, 0 60, -90 60, -180 60, -180 90))",
        ],
        ids=["clockwise", "counterclockwise"],
    )
    def test_an_arctic_cap_drawn_to_ninety_is_the_cap_when_unoriented(self, ring):
        """The ring touches N, so only the fan anchored at N can read its area."""
        assert bbox_of(ring)[1::2] == (60.0, 90.0)

    def test_a_hemisphere_ring_covers_both_halves(self):
        """The equator splits the globe exactly in half: neither side is smaller."""
        assert bbox_of("POLYGON ((0 0, 90 0, 180 0, -90 0, 0 0))") == GLOBE


def _cap(lat: float, count: int, east: bool) -> str:
    lons = [-180.0 + 360.0 * i / count for i in range(count)]
    lons = lons if east else lons[::-1]
    ring = [*lons, lons[0]]
    return "POLYGON ((" + ", ".join(f"{lon!r} {lat!r}" for lon in ring) + "))"


def _tiny_ring(rng: random.Random, sign: float) -> str:
    """A small square near a pole that stays clear of it."""
    lat = sign * rng.uniform(89.9, 89.995)
    half = rng.uniform(1e-5, 2e-3)
    lon = rng.uniform(-170.0, 170.0)
    corners = [(lon - half, lat - half), (lon + half, lat - half), (lon + half, lat + half)]
    corners.append((lon - half, lat + half))
    if rng.random() < 0.5:
        corners.reverse()
    ring = [*corners, corners[0]]
    return "POLYGON ((" + ", ".join(f"{x!r} {y!r}" for x, y in ring) + "))"


_CAP_LATS = [89.9, 89.99, 89.993, 89.995, 89.998, 89.999, 89.9995, 89.99999]


class TestNearPoleConditioning:
    """A fan anchored at a ring's antipodal pole loses its area to rounding.

    A cap a few metres across around one pole made the fan anchored at the
    other pole read just below -4 pi, so the cap looked like the larger half
    and the box swapped the pole it holds for the opposite one.
    """

    @pytest.mark.parametrize("count", [4, 8, 32, 128])
    @pytest.mark.parametrize("east", [True, False], ids=["east", "west"])
    @pytest.mark.parametrize("lat", _CAP_LATS)
    def test_an_unoriented_north_cap_holds_only_the_north_pole(self, lat, east, count):
        bbox = bbox_of(_cap(lat, count, east))
        assert bbox[3] == 90.0
        assert bbox[1] > 89.0

    @pytest.mark.parametrize("count", [4, 8, 32, 128])
    @pytest.mark.parametrize("east", [True, False], ids=["east", "west"])
    @pytest.mark.parametrize("lat", _CAP_LATS)
    def test_an_unoriented_south_cap_holds_only_the_south_pole(self, lat, east, count):
        bbox = bbox_of(_cap(-lat, count, east))
        assert bbox[1] == -90.0
        assert bbox[3] < -89.0

    @pytest.mark.parametrize("count", [4, 8, 32, 128])
    # A metre-wide cap's area is under the fan's error bound, so a declared
    # orientation cannot tell it from the whole sphere and both poles go in.
    @pytest.mark.parametrize("lat", _CAP_LATS[:-1])
    def test_ccw_caps_hold_the_pole_on_their_left(self, lat, count):
        ccw = "counterclockwise"
        # Eastward at a northern latitude has the pole on the left.
        assert bbox_of(_cap(lat, count, True), orientation=ccw)[1::2] == (
            pytest.approx(lat, abs=1e-3),
            90.0,
        )
        north_west = bbox_of(_cap(lat, count, False), orientation=ccw)
        assert north_west[1] == -90.0
        # Westward at a southern latitude has the pole on the left.
        assert bbox_of(_cap(-lat, count, False), orientation=ccw)[1::2] == (
            -90.0,
            pytest.approx(-lat, abs=1e-3),
        )
        south_east = bbox_of(_cap(-lat, count, True), orientation=ccw)
        assert south_east[3] == 90.0

    @pytest.mark.parametrize("sign", [1.0, -1.0], ids=["north", "south"])
    def test_a_tiny_ring_near_a_pole_never_gets_the_opposite_pole(self, sign):
        rng = random.Random(1286)
        for _ in range(300):
            wkt = _tiny_ring(rng, sign)
            bbox = bbox_of(wkt)
            if sign > 0:
                assert bbox[1] > 89.0, wkt
            else:
                assert bbox[3] < -89.0, wkt


class TestIndependentOracle:
    """Every densified point of every edge is inside the box (pyproj, not this module)."""

    @staticmethod
    def _fuzz(edges: str, geod: Geod, seed: int, count: int, long_edges: bool):
        rng = random.Random(seed)
        failures = []
        for _ in range(count):
            a = (rng.uniform(-180, 180), rng.uniform(-89, 89))
            if long_edges:
                b = (a[0] + rng.choice([-1, 1]) * rng.uniform(150, 179.9), rng.uniform(-89, 89))
            else:
                b = (rng.uniform(-180, 180), rng.uniform(-89, 89))
            bbox = geodesic_bbox_of_wkb([_line([a, b])], edges=edges)
            failures += _oracle_misses(bbox, [(a, b)], geod, npts=400)
        return failures

    def test_spherical_random_edges(self):
        assert not self._fuzz("spherical", SPHERE, 1272, 400, long_edges=False)

    def test_spherical_long_edges(self):
        assert not self._fuzz("spherical", SPHERE, 7, 300, long_edges=True)

    @pytest.mark.parametrize("edges", ["karney", "vincenty", "thomas", "andoyer"])
    def test_ellipsoidal_random_edges(self, edges):
        assert not self._fuzz(edges, WGS84, 99, 300, long_edges=False)

    def test_karney_long_edges(self):
        assert not self._fuzz("karney", WGS84, 5, 300, long_edges=True)

    @pytest.mark.parametrize(
        "a, b",
        [
            ((0.0, 0.0), (179.5, 0.0)),
            ((0.0, 0.0), (179.8, 0.0)),
            ((0.0, -5.0), (178.0, 0.0)),
            ((-47.58, -8.65), (143.50, 0.43)),
            ((-89.9, 1.0), (89.9, 1.0)),
        ],
    )
    def test_karney_near_antipodal_edges(self, a, b):
        """The reviewers' cases: the old 0.2 degree pad missed each by degrees."""
        bbox = geodesic_bbox_of_wkb([_line([a, b])], edges="karney")
        assert not _oracle_misses(bbox, [(a, b)], WGS84)

    def test_karney_moderate_edges_stay_tight(self):
        """Conservative is not the globe: a 120 degree edge gets a real box."""
        a, b = (-60.0, 30.0), (60.0, 35.0)
        bbox = geodesic_bbox_of_wkb([_line([a, b])], edges="karney")
        true_max = max(lat for _, lat in WGS84.npts(*a, *b, 5000))
        assert true_max <= bbox[3] <= true_max + 1e-3
        assert (bbox[0], bbox[2]) == (-60.0, 60.0)

    def test_polygons_with_many_vertices(self):
        rng = random.Random(3)
        for _ in range(40):
            lon0, lat0 = rng.uniform(-170, 170), rng.uniform(-70, 70)
            radius = rng.uniform(1, 15)
            coords = [
                (lon0 + radius * math.cos(t), lat0 + radius * math.sin(t))
                for t in [2 * math.pi * k / 17 for k in range(17)]
            ]
            coords.append(coords[0])
            wkt = "POLYGON ((" + ", ".join(f"{x} {y}" for x, y in coords) + "))"
            bbox = bbox_of(wkt)
            assert not _oracle_misses(bbox, _edges(coords), SPHERE, npts=100), wkt


def _native_file(tmp_path, values: list, row_group_size: int = 1):
    import geoarrow.pyarrow as ga
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "native.parquet"
    array = pa.ExtensionArray.from_storage(ga.wkb(), pa.array(values, pa.binary()))
    pq.write_table(pa.table({"geometry": array}), str(path), row_group_size=row_group_size)
    return path


class TestRowGroupReading:
    """`row_group_geography_bboxes` bounds one row group at a time."""

    def test_each_row_group_gets_its_own_box(self, tmp_path):
        path = _native_file(tmp_path, [_wkb("POINT (1 2)"), _wkb("POINT (3 4)")])
        bounds = row_group_geography_bboxes(str(path), "geometry", "spherical")
        assert bounds.per_group == [(1.0, 2.0, 1.0, 2.0), (3.0, 4.0, 3.0, 4.0)]
        assert bounds.file_bbox == (1.0, 2.0, 3.0, 4.0)
        assert bounds.unreadable_groups == 0

    def test_a_corrupt_row_group_poisons_the_file_bound(self, tmp_path):
        """A whole-file bound that skipped rows would under-cover; refuse it."""
        path = _native_file(tmp_path, [_wkb("POINT (1 2)"), b"\x07junk"])
        bounds = row_group_geography_bboxes(str(path), "geometry", "spherical")
        assert bounds.per_group == [(1.0, 2.0, 1.0, 2.0), None]
        assert bounds.file_bbox is None
        assert bounds.unreadable_groups == 1

    def test_an_unboundable_column_is_left_out_of_the_file_level_answer(self, tmp_path):
        from geoparquet_io.core.geodesic_bbox import geography_statistics

        path = _native_file(tmp_path, [_wkb("POINT (1 2)"), b"\x07junk"])
        stats = geography_statistics(str(path), {"geometry": "spherical"})
        assert stats["geometry"].per_group == [(1.0, 2.0, 1.0, 2.0), None]
        assert stats["geometry"].file_bbox is None

    def test_an_all_null_row_group_is_left_out_of_the_union(self, tmp_path):
        path = _native_file(tmp_path, [_wkb("POINT (1 2)"), None])
        bounds = row_group_geography_bboxes(str(path), "geometry", "spherical")
        assert bounds.per_group == [(1.0, 2.0, 1.0, 2.0), None]
        assert bounds.file_bbox == (1.0, 2.0, 1.0, 2.0)
        assert bounds.unreadable_groups == 0

    def test_a_large_row_group_is_read_in_slices(self, tmp_path, monkeypatch):
        """Bounded memory: the row group is folded slice by slice, same answer."""
        import geoparquet_io.core.geodesic_bbox as module

        values = [_line([(i * 0.01 - 170, 60.0), (i * 0.01 - 169, 60.0)]) for i in range(3000)]
        path = _native_file(tmp_path, values, row_group_size=3000)
        whole = row_group_geography_bboxes(str(path), "geometry", "spherical").file_bbox
        monkeypatch.setattr(module, "_SLICE_BYTES", 512)
        monkeypatch.setattr(module, "_BATCH_ROWS", 100)
        sliced = row_group_geography_bboxes(str(path), "geometry", "spherical").file_bbox
        assert sliced == whole
        assert whole[0] == -170.0 and whole[2] == pytest.approx(-169.0 + 29.99)


class TestLongitudeFolding:
    def test_many_disjoint_longitudes_keep_the_widest_gap(self, monkeypatch):
        """The arc set is capped; the cap fills only the smallest gaps."""
        import geoparquet_io.core.geodesic_bbox as module

        monkeypatch.setattr(module, "_MAX_LON_ARCS", 8)
        lons = [float(x) for x in range(-100, 101, 3)]
        bbox = geodesic_bbox_of_wkb([_wkb(f"POINT ({lon} 0)") for lon in lons])
        assert (bbox[0], bbox[2]) == (-100.0, 98.0)
        assert all(_contains(bbox, lon, 0.0) for lon in lons)

    def test_the_cap_never_fills_the_widest_gap(self, monkeypatch):
        """Two clusters either side of the antimeridian: the 240 degree gap must survive."""
        import geoparquet_io.core.geodesic_bbox as module

        monkeypatch.setattr(module, "_MAX_LON_ARCS", 4)
        lons = [float(x) for x in range(-170, -119, 5)] + [float(x) for x in range(120, 171, 5)]
        bbox = geodesic_bbox_of_wkb([_wkb(f"POINT ({lon} 0)") for lon in lons])
        assert (bbox[0], bbox[2]) == (120.0, -120.0)

    def test_the_widest_gap_wraps_into_a_wrapped_box(self):
        values = [_wkb("POINT (170 0)"), _wkb("POINT (-170 0)"), _wkb("POINT (175 1)")]
        assert geodesic_bbox_of_wkb(values)[0::2] == (170.0, -170.0)


class TestLonNormalization:
    def test_longitudes_beyond_the_range_are_wrapped(self):
        assert bbox_of("POINT (190 10)") == (-170.0, 10.0, -170.0, 10.0)
        assert bbox_of("POINT (-190 10)") == (170.0, 10.0, 170.0, 10.0)

    def test_a_positive_odd_multiple_of_180_stays_east(self):
        assert bbox_of("POINT (540 0)") == (180.0, 0.0, 180.0, 0.0)


class TestEllipsoidForCrs:
    def test_no_crs_is_wgs84(self):
        from geoparquet_io.core.geodesic_bbox import WGS84_ELLIPSOID, ellipsoid_for_crs

        assert ellipsoid_for_crs(None) == WGS84_ELLIPSOID
        assert ellipsoid_for_crs("not a crs at all") == WGS84_ELLIPSOID

    def test_a_crs_names_its_own_ellipsoid(self):
        from geoparquet_io.core.geodesic_bbox import ellipsoid_for_crs

        semi_major, flattening = ellipsoid_for_crs("EPSG:4269")  # NAD83 / GRS80
        assert semi_major == 6378137.0
        assert flattening == pytest.approx(1 / 298.257222101)
        assert ellipsoid_for_crs("+proj=longlat +R=6371000 +no_defs") == (6371000.0, 0.0)


def test_a_binary_view_array_is_read(tmp_path):
    import pyarrow as pa

    from geoparquet_io.core.geodesic_bbox import _Bound, _bound_array, _context

    acc = _Bound()
    array = pa.array([_wkb("POINT (1 2)")], pa.binary()).cast(pa.binary_view())
    _bound_array(array, acc, _context("spherical", None, None))
    assert acc.result() == (1.0, 2.0, 1.0, 2.0)
