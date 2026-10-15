"""Geodesic bounding boxes for GEOGRAPHY columns (#1272).

A planar min/max over the vertices does not bound a geodesic edge: a long
east-west great-circle edge at high latitude bulges poleward, so a reader that
prunes row groups by a vertex box can skip rows that really match. #1254
therefore *dropped* the bounding box when it retyped a column GEOGRAPHY, which
is safe but gives up row-group pruning entirely. This module computes a box
that actually bounds the edges, so the statistics can be written instead.

What "bounds" means here follows the Parquet geospatial statistics and
RFC 7946 5.2: X and Y are canonical lon/lat ([-180, 180], [-90, 90]) and
``xmin > xmax`` marks a box that wraps the antimeridian. Every result is a
*bound*, never a guess: where the geometry cannot be bounded provably, the
answer is wider (up to the globe) or ``None`` (no box at all, which is always
legal -- it only gives up pruning).

**Latitude.** An edge's latitude extremes are its endpoints plus, when the
geodesic's vertex (its Clairaut turning point) falls inside the edge, that
vertex. ``SPHERICAL`` edges are minor great-circle arcs: the vertex latitude is
``acos(|n_z|)`` for the arc's unit normal ``n``, and it lies inside the arc
exactly when the arc's northward heading changes sign between its ends. The
ellipsoidal algorithms (``VINCENTY``, ``THOMAS``, ``ANDOYER``, ``KARNEY``) use
the same test on the geodesic's own azimuths -- ``pyproj.Geod.inv`` on the
column CRS's ellipsoid (WGS84 when it has none) -- and Clairaut's relation on
the auxiliary sphere: ``sin(alpha0) = sin(alpha1) cos(beta1)`` with
``beta = atan((1 - f) tan(phi))`` the reduced latitude, so the vertex is at
``beta0 = acos(|sin(alpha0)|)``, ``phi0 = atan(tan(beta0) / (1 - f))``. Vertex
latitudes get an outward margin, :data:`LAT_MARGIN_DEG`, orders of magnitude
above the floating-point error of either computation; the data's own vertices
and points get none. ``THOMAS`` and ``ANDOYER`` are first-order approximations
of the geodesic, so their margin is the wider
:data:`APPROXIMATE_ALGORITHM_MARGIN_DEG`.

**Longitude.** Longitude is monotonic along both a great circle and an
ellipsoidal geodesic, so an edge covers the lon interval between its endpoints
taken the short way round; the union of those intervals is folded into a
bounded set of arcs per row group and the box is the complement of the widest
gap (``merge_longitude_ranges``'s rule).

**Degenerate edges.** Coincident vertices -- repeated points, the two sides of
the antimeridian seam, different longitudes at one pole -- are skipped. Edges
whose endpoints are antipodal (or near enough that the direction of the
geodesic is numerically undefined), and ellipsoidal edges longer than
:data:`ELLIPSOIDAL_MAX_ARC_DEG` (where geodesics between nearby endpoints can
take very different paths), bound to the globe. A vertex or edge within
:data:`POLE_PROXIMITY_DEG` of a pole puts that pole, and every longitude, in
the box.

**Poles inside polygons.** A ring splits the sphere in two; a pole on the
interior side joins the box. Which side is the interior depends on the
column's ``orientation``: declared ``counterclockwise``, it is the left of the
traversal (RFC 7946's right-hand rule); absent -- the usual case -- the
GeoParquet spec notes that readers typically take the *smaller* of the two
regions, so that is used, and a ring that splits the sphere into halves too
close to call covers both. Membership comes from two fans of signed spherical
triangles (Van Oosterom-Strackee) anchored at each pole, which give the left
region's area and whether the opposite pole is in it. Holes only remove area,
so they never add a pole.

**Unreadable data.** WKB this reader cannot parse, a non-finite coordinate, or
a latitude outside [-90, 90] makes the whole row group unboundable (``None``):
a bound that skipped the bad row could under-cover it.

The computation is vectorized with numpy over each row group's WKB buffer,
read a bounded slice at a time; only multi-part and multi-ring geometries take
a per-part Python step, and the longitude state per row group is capped at
:data:`_MAX_LON_ARCS` arcs (filling only the smallest gaps, which can only
widen the box).
"""

from __future__ import annotations

import math
import struct
from collections.abc import Iterable, Mapping
from typing import Any, NamedTuple

from geoparquet_io.core.crs_utils import merge_longitude_ranges
from geoparquet_io.core.optional_deps import load_module

Box = tuple[float, float, float, float]

SPHERICAL = "spherical"
ELLIPSOIDAL_ALGORITHMS = frozenset({"vincenty", "thomas", "andoyer", "karney"})
_APPROXIMATE_ALGORITHMS = frozenset({"thomas", "andoyer"})
COUNTERCLOCKWISE = "counterclockwise"

#: Outward margin on a geodesic vertex latitude (~0.1 m). The floating-point
#: error of the vertex computation is below 1e-8 degrees for every edge not
#: already handled as short or antipodal.
LAT_MARGIN_DEG = 1e-6
#: Margin for the first-order approximate algorithms, whose path deviates from
#: the true geodesic by O(f^2) (~1e-5 rad).
APPROXIMATE_ALGORITHM_MARGIN_DEG = 0.01
#: A vertex or edge this close to a pole counts as touching it.
POLE_PROXIMITY_DEG = 1e-3
#: Ellipsoidal edges longer than this (as a central angle) bound to the globe.
ELLIPSOIDAL_MAX_ARC_DEG = 170.0
#: WGS84 semi-major axis (m) and flattening, for a column with no CRS ellipsoid.
WGS84_ELLIPSOID = (6378137.0, 1 / 298.257223563)

# |a x b| (the sine of the central angle) under which an edge is either very
# short or (nearly) antipodal and the arc normal is numerically meaningless.
_SHORT_EDGE_SINE = 1e-6
# An edge whose longitude step is this close to 180 degrees passes over a pole.
_NEAR_HALF_TURN_DEG = 1e-9
# Tolerance on "the ring splits the sphere into halves" for the ellipsoidal
# algorithms, whose areas the spherical fan only approximates (O(f)).
_ELLIPSOIDAL_HALF_TOLERANCE = 0.02 * 4 * math.pi
_EPS = 2.220446049250313e-16
_FOUR_PI = 4 * math.pi

_MAX_LON_ARCS = 1024
_SLICE_BYTES = 8 << 20
_BATCH_ROWS = 8192
_MAX_NESTING = 32

# Chain roles: a run of unconnected points, a linestring, polygon rings.
_POINTS, _LINE, _EXTERIOR, _HOLE = 0, 1, 2, 3
# EWKB dimension/SRID flag bits, tolerated alongside the ISO +1000/+2000 codes.
_EWKB_Z, _EWKB_M, _EWKB_SRID = 0x80000000, 0x40000000, 0x20000000


class _WkbError(ValueError):
    """The values cannot be bounded; the caller bounds nothing."""


class ColumnBounds(NamedTuple):
    """One column's geodesic bounds.

    ``per_group`` has a box (or ``None``) per row group; ``file_bbox`` is their
    union, ``None`` when nothing could be bounded or any row group was
    unreadable; ``unreadable_groups`` counts the row groups that held data the
    reader could not bound (as opposed to only nulls and empties).
    """

    per_group: list[Box | None]
    file_bbox: Box | None
    unreadable_groups: int


def _np() -> Any:
    return load_module("numpy")


def ellipsoid_for_crs(crs: Any) -> tuple[float, float]:
    """``(semi-major axis, flattening)`` of a CRS's ellipsoid; WGS84 by default."""
    if crs is None:
        return WGS84_ELLIPSOID
    try:
        from pyproj import CRS

        ellipsoid = CRS.from_user_input(crs).ellipsoid
        if ellipsoid is None:
            return WGS84_ELLIPSOID
        inverse = ellipsoid.inverse_flattening
        return float(ellipsoid.semi_major_metre), (1.0 / inverse if inverse else 0.0)
    except Exception:  # noqa: BLE001 - an unreadable CRS falls back to WGS84
        return WGS84_ELLIPSOID


# ---------------------------------------------------------------------------
# WKB decoding: chain descriptors, then one vectorized coordinate gather
# ---------------------------------------------------------------------------


class _Chains(NamedTuple):
    offset: Any  # byte offset of the first coordinate
    count: Any  # number of coordinates
    stride: Any  # bytes from one coordinate to the next
    little: Any  # little-endian doubles
    role: Any  # _POINTS / _LINE / _EXTERIOR / _HOLE


def _u32(np: Any, data: Any, pos: Any, little: Any) -> Any:
    raw = data[pos[:, None] + np.arange(4)]
    return np.where(little, raw.view("<u4").ravel(), raw.view(">u4").ravel()).astype(np.int64)


def _require(condition: Any, message: str) -> None:
    if not bool(condition):
        raise _WkbError(message)


def _fast_chains(np: Any, data: Any, starts: Any, lens: Any) -> tuple[list, Any]:
    """Chains of the little-endian ISO points, lines and one-ring polygons.

    Returns ``(chains, slow)``: descriptor arrays for the values handled here,
    and the mask of values left for :func:`_slow_parse`.
    """
    _require((lens >= 5).all(), "value shorter than a WKB header")
    order = data[starts]
    _require(((order == 0) | (order == 1)).all(), "not a WKB byte-order mark")
    little = order == 1
    raw = _u32(np, data, starts + 1, little)
    kind, dim = raw % 1000, raw // 1000
    fast = little & (raw < 4000) & (kind >= 1) & (kind <= 3)
    stride = 8 * (2 + ((dim == 1) | (dim == 3)) + ((dim == 2) | (dim == 3)))

    point = fast & (kind == 1)
    _require((lens[point] >= 5 + stride[point]).all(), "truncated WKB point")
    counted = fast & (kind > 1)
    _require((lens[counted] >= 9).all(), "truncated WKB count")
    count = np.zeros(len(starts), np.int64)
    count[counted] = _u32(np, data, starts[counted] + 5, True)
    line = fast & (kind == 2)
    _require((9 + count[line] * stride[line] <= lens[line]).all(), "truncated WKB linestring")
    ring = fast & (kind == 3) & (count == 1)
    _require((lens[ring] >= 13).all(), "truncated WKB ring count")
    ring_count = np.zeros(len(starts), np.int64)
    ring_count[ring] = _u32(np, data, starts[ring] + 9, True)
    _require((13 + ring_count[ring] * stride[ring] <= lens[ring]).all(), "truncated WKB ring")

    chains = [
        (starts[point] + 5, np.ones(int(point.sum()), np.int64), stride[point], _POINTS),
        (starts[line] + 9, count[line], stride[line], _LINE),
        (starts[ring] + 13, ring_count[ring], stride[ring], _EXTERIOR),
    ]
    slow = ~fast | (fast & (kind == 3) & (count > 1))
    return chains, slow


def _slow_count(buf: memoryview, fmt: str, pos: int, end: int) -> tuple[int, int]:
    _require(pos + 4 <= end, "truncated WKB count")
    (count,) = struct.unpack_from(f"{fmt}I", buf, pos)
    return count, pos + 4


def _slow_header(buf: memoryview, pos: int, end: int) -> tuple[str, int, int, int]:
    """One WKB header: the struct format, geometry kind, stride and next offset."""
    _require(pos + 5 <= end and buf[pos] in (0, 1), "not a WKB header")
    fmt = "<" if buf[pos] == 1 else ">"
    (raw,) = struct.unpack_from(f"{fmt}I", buf, pos + 1)
    pos += 9 if raw & _EWKB_SRID else 5
    code = raw & 0x0FFFFFFF
    kind, iso = code % 1000, code // 1000
    _require(iso <= 3, f"WKB dimension code {iso} is not ISO")
    ndims = 2 + (iso in (1, 3) or bool(raw & _EWKB_Z)) + (iso in (2, 3) or bool(raw & _EWKB_M))
    return fmt, kind, 8 * ndims, pos


def _slow_parse(buf: memoryview, pos: int, end: int, out: list, depth: int = 0) -> int:
    """Append one WKB geometry's chain descriptors to ``out``; returns the next offset."""
    _require(depth <= _MAX_NESTING, "WKB nests deeper than this reader follows")
    fmt, kind, stride, pos = _slow_header(buf, pos, end)
    little = fmt == "<"
    if kind == 1:
        _require(pos + stride <= end, "truncated WKB point")
        out.append((pos, 1, stride, little, _POINTS))
        return pos + stride
    count, pos = _slow_count(buf, fmt, pos, end)
    if kind == 2:
        _require(pos + count * stride <= end, "truncated WKB linestring")
        out.append((pos, count, stride, little, _LINE))
        return pos + count * stride
    if kind == 3:
        for index in range(count):
            points, pos = _slow_count(buf, fmt, pos, end)
            _require(pos + points * stride <= end, "truncated WKB ring")
            out.append((pos, points, stride, little, _EXTERIOR if index == 0 else _HOLE))
            pos += points * stride
        return pos
    _require(4 <= kind <= 7, f"WKB geometry kind {kind} is not one this reader knows")
    for _ in range(count):
        pos = _slow_parse(buf, pos, end, out, depth + 1)
    return pos


def _decode(np: Any, data: Any, starts: Any, ends: Any) -> _Chains:
    """Chain descriptors for the WKB values ``data[starts[i]:ends[i]]``."""
    chains, slow = _fast_chains(np, data, starts, ends - starts)
    offsets = [c[0] for c in chains]
    counts = [c[1] for c in chains]
    strides = [c[2] for c in chains]
    roles = [np.full(len(c[0]), c[3], np.int8) for c in chains]
    littles = [np.ones(len(c[0]), bool) for c in chains]
    if slow.any():
        buf = memoryview(data)
        found: list = []
        for start, end in zip(starts[slow].tolist(), ends[slow].tolist(), strict=True):
            _slow_parse(buf, start, end, found)
        if found:
            columns = list(zip(*found, strict=True))
            offsets.append(np.array(columns[0], np.int64))
            counts.append(np.array(columns[1], np.int64))
            strides.append(np.array(columns[2], np.int64))
            littles.append(np.array(columns[3], bool))
            roles.append(np.array(columns[4], np.int8))
    return _Chains(
        np.concatenate(offsets),
        np.concatenate(counts),
        np.concatenate(strides),
        np.concatenate(littles),
        np.concatenate(roles),
    )


def _coordinates(np: Any, data: Any, chains: _Chains) -> tuple[Any, Any, Any]:
    """``(lon, lat, chain_of_vertex)`` gathered from the WKB buffer in one pass."""
    total = int(chains.count.sum())
    if total == 0:
        empty = np.empty(0)
        return empty, empty, np.empty(0, np.int64)
    chain_of = np.repeat(np.arange(len(chains.count)), chains.count)
    first = np.cumsum(chains.count) - chains.count
    step = chains.stride[chain_of]
    position = chains.offset[chain_of] + (np.arange(total) - first[chain_of]) * step
    window = np.lib.stride_tricks.as_strided(
        data, shape=(data.size - 15, 16), strides=(1, 1), writeable=False
    )
    raw = window[position]
    little = chains.little[chain_of]
    if little.all():
        xy = raw.view("<f8")
    else:
        xy = np.empty((total, 2))
        xy[little] = raw[little].view("<f8")
        xy[~little] = raw[~little].view(">f8")
    return np.ascontiguousarray(xy[:, 0]), np.ascontiguousarray(xy[:, 1]), chain_of


def _normalized_lon(np: Any, lon: Any) -> Any:
    """``lon`` wrapped into [-180, 180] exactly (fmod), +180 kept as +180."""
    outside = (lon < -180.0) | (lon > 180.0)
    if not outside.any():
        return lon
    wrapped = np.fmod(lon, 360.0)
    wrapped = np.where(wrapped > 180.0, wrapped - 360.0, wrapped)
    wrapped = np.where(wrapped < -180.0, wrapped + 360.0, wrapped)
    return np.where(outside, wrapped, lon)


# ---------------------------------------------------------------------------
# The running bound
# ---------------------------------------------------------------------------


def _union_arcs(np: Any, lo: Any, hi: Any) -> tuple[Any, Any]:
    """Disjoint sorted arcs covering ``[lo, hi]`` (``lo > hi`` wraps)."""
    wrap = lo > hi
    count = int(wrap.sum())
    starts = np.concatenate([lo[~wrap], lo[wrap], np.full(count, -180.0)])
    ends = np.concatenate([hi[~wrap], np.full(count, 180.0), hi[wrap]])
    order = np.argsort(starts)
    starts, ends = starts[order], ends[order]
    reach = np.maximum.accumulate(ends)
    new = np.ones(len(starts), bool)
    new[1:] = starts[1:] > reach[:-1]
    heads = np.flatnonzero(new)
    return starts[heads], np.maximum.reduceat(ends, heads)


def _cap_arcs(np: Any, starts: Any, ends: Any, limit: int) -> tuple[Any, Any]:
    """At most ``limit`` arcs, filling the smallest gaps (only ever widens)."""
    if len(starts) <= limit:
        return starts, ends
    gaps = starts[1:] - ends[:-1]
    keep = np.sort(np.argsort(gaps, kind="stable")[len(gaps) - (limit - 1) :])
    return np.concatenate([starts[:1], starts[keep + 1]]), np.concatenate([ends[keep], ends[-1:]])


class _Bound:
    """The running bound of one row group: latitude extremes plus longitude arcs."""

    def __init__(self) -> None:
        self.lat_lo = math.inf
        self.lat_hi = -math.inf
        self.all_lons = False
        self.starts: Any = None
        self.ends: Any = None
        self.empty = True

    def lat(self, lo: float, hi: float) -> None:
        self.empty = False
        self.lat_lo = min(self.lat_lo, float(lo))
        self.lat_hi = max(self.lat_hi, float(hi))

    def pole(self, north: bool) -> None:
        self.all_lons = True
        if north:
            self.lat(90.0, 90.0)
        else:
            self.lat(-90.0, -90.0)

    def globe(self) -> None:
        self.all_lons = True
        self.lat(-90.0, 90.0)

    def arcs(self, np: Any, lo: Any, hi: Any) -> None:
        if self.all_lons or len(lo) == 0:
            return
        if self.starts is not None:
            lo, hi = np.concatenate([self.starts, lo]), np.concatenate([self.ends, hi])
        starts, ends = _union_arcs(np, lo, hi)
        self.starts, self.ends = _cap_arcs(np, starts, ends, _MAX_LON_ARCS)

    def _lon_range(self) -> tuple[float, float]:
        if self.all_lons or self.starts is None:
            return -180.0, 180.0
        starts, ends = self.starts, self.ends
        wrap_gap = starts[0] + 360.0 - ends[-1]
        gaps = starts[1:] - ends[:-1]
        if len(gaps) == 0 or wrap_gap >= gaps.max():
            return float(starts[0]), float(ends[-1])
        widest = int(gaps.argmax())
        xmin, xmax = float(starts[widest + 1]), float(ends[widest])
        # [180, 180] + [-180, x] is the arc [-180, x]: one meridian, two names.
        if xmin == 180.0:
            xmin = -180.0
        elif xmax == -180.0:
            xmax = 180.0
        return xmin, xmax

    def result(self) -> Box | None:
        """``(xmin, ymin, xmax, ymax)``, ``xmin > xmax`` wrapping, or None."""
        if self.empty:
            return None
        xmin, xmax = self._lon_range()
        return (xmin, max(self.lat_lo, -90.0), xmax, min(self.lat_hi, 90.0))


# ---------------------------------------------------------------------------
# Edge geometry
# ---------------------------------------------------------------------------


class _Edges(NamedTuple):
    a: Any  # start vertex index
    b: Any  # end vertex index
    chain: Any  # chain index
    consecutive: int  # edges before this index join consecutive vertices; the rest close rings


def _edge_index(np: Any, lon: Any, lat: Any, chain_of: Any, role: Any) -> _Edges:
    """Consecutive-vertex edges of every non-point chain, plus closing edges of open rings."""
    linked = role[chain_of] != _POINTS
    a = np.flatnonzero((chain_of[1:] == chain_of[:-1]) & linked[1:])
    b = a + 1
    chains = np.arange(len(role))
    first = np.searchsorted(chain_of, chains, side="left")
    last = np.searchsorted(chain_of, chains, side="right") - 1
    rings = ((role == _EXTERIOR) | (role == _HOLE)) & (last > first)
    head, tail = first[rings], last[rings]
    open_ = (lon[head] != lon[tail]) | (lat[head] != lat[tail])
    consecutive = len(a)
    a = np.concatenate([a, tail[open_]])
    b = np.concatenate([b, head[open_]])
    return _Edges(a, b, chain_of[a], consecutive)


class _Geometry(NamedTuple):
    """Per-edge unit-vector quantities shared by the latitude and pole passes."""

    xa: Any
    ya: Any
    za: Any
    xb: Any
    yb: Any
    zb: Any
    cross: tuple[Any, Any, Any]  # unnormalized a x b
    sine: Any  # |a x b|
    dot: Any
    coincident: Any
    short: Any
    normal: Any
    antipodal: bool


def _edge_geometry(np: Any, lon: Any, lat: Any, edges: _Edges) -> _Geometry:
    """The unit-vector geometry of every edge."""
    lon_r, lat_r = np.radians(lon), np.radians(lat)
    cos_lat = np.cos(lat_r)
    x, y, z = cos_lat * np.cos(lon_r), cos_lat * np.sin(lon_r), np.sin(lat_r)
    xa, ya, za = x[edges.a], y[edges.a], z[edges.a]
    xb, yb, zb = x[edges.b], y[edges.b], z[edges.b]
    cross = (ya * zb - za * yb, za * xb - xa * zb, xa * yb - ya * xb)
    sine = np.sqrt(cross[0] ** 2 + cross[1] ** 2 + cross[2] ** 2)
    dot = xa * xb + ya * yb + za * zb
    la, lb = lat[edges.a], lat[edges.b]
    # The same lon/lat, the two names of the antimeridian, or one pole.
    lon_gap = np.abs(lon[edges.a] - lon[edges.b])
    coincident = (la == lb) & ((lon_gap == 0.0) | (lon_gap == 360.0) | (np.abs(la) == 90.0))
    small = (sine < _SHORT_EDGE_SINE) & ~coincident
    return _Geometry(
        xa,
        ya,
        za,
        xb,
        yb,
        zb,
        cross,
        sine,
        dot,
        coincident,
        small & (dot > 0.0),
        ~coincident & ~small,
        bool((small & (dot <= 0.0)).any()),
    )


def _lon_arcs(
    np: Any, lon: Any, chain_of: Any, role: Any, edges: _Edges, geom: _Geometry
) -> tuple[Any, Any, bool]:
    """The lon intervals to cover, east-going; and whether an edge passes over a pole.

    Each moving edge covers its interval the short way round. A chain none of
    whose edges crosses the antimeridian covers exactly ``[min lon, max lon]``
    of its vertices -- its edges are contiguous intervals on the plain line --
    so it contributes that one arc instead of one per edge. Points, and the
    vertices of chains that do cross, cover their own longitude.
    """
    raw = lon[edges.b] - lon[edges.a]
    step = np.where(raw > 180.0, raw - 360.0, np.where(raw <= -180.0, raw + 360.0, raw))
    moving = ~geom.coincident & (step != 0.0)
    half_turn = bool((moving & (np.abs(step) >= 180.0 - _NEAR_HALF_TURN_DEG)).any())
    # A seam duplicate (180 then -180) is coincident but still a jump on the line.
    jumps = (moving & (raw != step)) | (geom.coincident & (raw != 0.0))
    crosses = np.bincount(edges.chain[jumps], minlength=len(role)) > 0
    simple = ~crosses & (role != _POINTS)

    runs = np.flatnonzero(np.concatenate([[True], chain_of[1:] != chain_of[:-1]]))
    run_simple = simple[chain_of[runs]]
    lo = [np.minimum.reduceat(lon, runs)[run_simple], lon[~simple[chain_of]]]
    hi = [np.maximum.reduceat(lon, runs)[run_simple], lon[~simple[chain_of]]]
    closing = np.arange(len(edges.a)) >= edges.consecutive
    each = moving & (closing | ~simple[edges.chain])
    east = step[each] > 0.0
    a, b = edges.a[each], edges.b[each]
    lo.append(np.where(east, lon[a], lon[b]))
    hi.append(np.where(east, lon[b], lon[a]))
    return np.concatenate(lo), np.concatenate(hi), half_turn


def _spherical_vertices(np: Any, geom: _Geometry) -> tuple[Any, Any]:
    """Interior latitude max/min of each normal edge (+-inf where there is none)."""
    cx, cy, cz = geom.cross
    safe = np.where(geom.normal, geom.sine, 1.0)
    nx, ny, nz = cx / safe, cy / safe, cz / safe
    start = nx * geom.ya - ny * geom.xa  # the sign of the northward heading at a
    end = nx * geom.yb - ny * geom.xb
    peak = np.degrees(np.arctan2(np.hypot(nx, ny), np.abs(nz))) + LAT_MARGIN_DEG
    top = geom.normal & (start > 0.0) & (end < 0.0)
    bottom = geom.normal & (start < 0.0) & (end > 0.0)
    return np.where(top, peak, -np.inf), np.where(bottom, -peak, np.inf)


class _Context(NamedTuple):
    algorithm: str
    orientation: str | None
    ellipsoid: tuple[float, float]


def _ellipsoidal_vertices(
    np: Any, lon: Any, lat: Any, edges: _Edges, geom: _Geometry, ctx: _Context
) -> tuple[Any, Any]:
    """Interior latitude max/min of each normal edge on the ellipsoid (Clairaut)."""
    from pyproj import Geod

    semi_major, flattening = ctx.ellipsoid
    hi = np.full(len(edges.a), -np.inf)
    lo = np.full(len(edges.a), np.inf)
    sel = np.flatnonzero(geom.normal)
    if sel.size == 0:
        return hi, lo
    a, b = edges.a[sel], edges.b[sel]
    az12, az21, _ = Geod(a=semi_major, f=flattening).inv(lon[a], lat[a], lon[b], lat[b])
    az12, az21 = np.radians(np.asarray(az12)), np.radians(np.asarray(az21))
    start, back = np.cos(az12), np.cos(az21)
    lat_a = np.radians(lat[a])
    beta1 = np.arctan2((1.0 - flattening) * np.sin(lat_a), np.cos(lat_a))
    sin_alpha0 = np.minimum(np.abs(np.sin(az12) * np.cos(beta1)), 1.0)
    margin = (
        APPROXIMATE_ALGORITHM_MARGIN_DEG
        if ctx.algorithm in _APPROXIMATE_ALGORITHMS
        else LAT_MARGIN_DEG
    )
    vertex = (
        np.degrees(np.arctan2(np.sqrt(1.0 - sin_alpha0**2), (1.0 - flattening) * sin_alpha0))
        + margin
    )
    # The forward heading at b is the back azimuth + 180: north means cos(back) < 0.
    hi[sel] = np.where((start > 0.0) & (back > 0.0), vertex, -np.inf)
    lo[sel] = np.where((start < 0.0) & (back < 0.0), -vertex, np.inf)
    return hi, lo


def _short_edge_pad(np: Any, lat: Any, edges: _Edges, geom: _Geometry, extremes: tuple) -> None:
    """Widen ``extremes`` in place: a short edge stays within its own length of its ends."""
    sel = np.flatnonzero(geom.short)
    if sel.size == 0:
        return
    hi, lo = extremes
    length = np.degrees(np.arctan2(geom.sine[sel], geom.dot[sel])) * 1.01 + LAT_MARGIN_DEG
    la, lb = lat[edges.a[sel]], lat[edges.b[sel]]
    hi[sel] = np.maximum(hi[sel], np.maximum(la, lb) + length)
    lo[sel] = np.minimum(lo[sel], np.minimum(la, lb) - length)


def _fan_terms(np: Any, geom: _Geometry, mask: Any) -> list[tuple[Any, Any]]:
    """``(numerator, denominator)`` of each triangle (S, a, b), then (N, a, b)."""
    cz = geom.cross[2][mask]
    za, zb, dot = geom.za[mask], geom.zb[mask], geom.dot[mask]
    return [(-cz, 1.0 - za - zb + dot), (cz, 1.0 + za + zb + dot)]


def _fan_errors(np: Any, chain: Any, size: int, terms: list, rings: Any) -> tuple[Any, Any]:
    """Per-ring error bounds of the fan sums, from atan2's conditioning."""
    errors = []
    for num, den in terms:
        bound = 32.0 * _EPS / np.maximum(np.hypot(num, den), 1e-300)
        errors.append((np.bincount(chain, weights=bound, minlength=size) + 64.0 * _EPS)[rings])
    return errors[0], errors[1]


def _pole_flags(
    np: Any, lat: Any, edges: _Edges, role: Any, extremes: tuple, geom: _Geometry, ctx: _Context
) -> tuple[bool, bool]:
    """Whether some polygon's interior holds the north / south pole.

    Anchored at S, the signed triangles (S, a, b) of a ring sum to the area of
    the region left of it, less 4 pi when N is inside that region; anchored at
    N, likewise for S. A fan loses its area to rounding for a ring near its
    anchor's antipode (a ring at 89.998 N read just below -4 pi from S), so
    each ring takes its area from the fan with the smaller error bound -- the
    one anchored at the pole nearer it -- and both poles' sides follow from
    that area and the two sums, whose 4 pi offsets survive the rounding.
    """
    exterior = role[edges.chain] == _EXTERIOR
    if not exterior.any():
        return False, False
    hi, lo = extremes
    chain, size = edges.chain[exterior], len(role)
    near = 90.0 - POLE_PROXIMITY_DEG
    near_n = (np.maximum(lat[edges.a], lat[edges.b]) >= near) | (hi >= near)
    near_s = (np.minimum(lat[edges.a], lat[edges.b]) <= -near) | (lo <= -near)
    rings = np.unique(chain)
    touch_n = (np.bincount(chain, weights=near_n[exterior], minlength=size) > 0)[rings]
    touch_s = (np.bincount(chain, weights=near_s[exterior], minlength=size) > 0)[rings]
    terms = _fan_terms(np, geom, exterior)
    from_s, from_n = (
        np.bincount(chain, weights=2.0 * np.arctan2(num, den), minlength=size)[rings]
        for num, den in terms
    )
    err_s, err_n = _fan_errors(np, chain, size, terms, rings)
    # Read the area from the better-conditioned fan (the one anchored at the
    # pole nearer the ring), and each pole's side relative to that area, so a
    # rounding slip across 0 / 4 pi moves the area and both sides together.
    use_n = err_n < err_s
    err = np.where(use_n, err_n, err_s)
    area = np.mod(np.where(use_n, from_n, from_s), _FOUR_PI)
    n_in_left = area - from_s > 2.0 * math.pi
    s_in_left = area - from_n > 2.0 * math.pi

    if ctx.orientation == COUNTERCLOCKWISE:
        # An area within its error of 0 or 4 pi cannot say whether the left
        # side is nothing or everything.
        unsure = np.minimum(area, _FOUR_PI - area) <= err
        north = touch_n | n_in_left | unsure
        south = touch_s | s_in_left | unsure
        return bool(north.any()), bool(south.any())
    # The smaller side. Only the half-sphere tie is ambiguous, and it covers
    # both sides.
    slack = err + (_ELLIPSOIDAL_HALF_TOLERANCE if ctx.algorithm != SPHERICAL else 1e-12)
    left = area <= 2.0 * math.pi + slack
    right = area >= 2.0 * math.pi - slack
    north = touch_n | (left & n_in_left) | (right & ~n_in_left)
    south = touch_s | (left & s_in_left) | (right & ~s_in_left)
    return bool(north.any()), bool(south.any())


def _is_long_ellipsoidal(np: Any, geom: _Geometry, ctx: _Context) -> bool:
    if ctx.algorithm == SPHERICAL:
        return False
    angle = np.degrees(np.arctan2(geom.sine, geom.dot))
    return bool((geom.normal & (angle > ELLIPSOIDAL_MAX_ARC_DEG)).any())


def _bound_vertices(
    np: Any, lon: Any, lat: Any, chain_of: Any, role: Any, acc: _Bound, ctx: _Context
) -> None:
    """Fold one slice's vertices and edges into ``acc``."""
    acc.lat(lat.min(), lat.max())
    if (lat >= 90.0 - POLE_PROXIMITY_DEG).any():
        acc.pole(north=True)
    if (lat <= -90.0 + POLE_PROXIMITY_DEG).any():
        acc.pole(north=False)
    edges = _edge_index(np, lon, lat, chain_of, role)
    geom = _edge_geometry(np, lon, lat, edges)
    if geom.antipodal or _is_long_ellipsoidal(np, geom, ctx):
        acc.globe()
        return
    lo_arc, hi_arc, half_turn = _lon_arcs(np, lon, chain_of, role, edges, geom)
    if half_turn:
        acc.all_lons = True
    acc.arcs(np, lo_arc, hi_arc)
    if len(edges.a) == 0:
        return

    if ctx.algorithm == SPHERICAL:
        hi, lo = _spherical_vertices(np, geom)
    else:
        hi, lo = _ellipsoidal_vertices(np, lon, lat, edges, geom, ctx)
    _short_edge_pad(np, lat, edges, geom, (hi, lo))
    acc.lat(float(lo.min()), float(hi.max()))
    north, south = _pole_flags(np, lat, edges, role, (hi, lo), geom, ctx)
    if north or (hi >= 90.0 - POLE_PROXIMITY_DEG).any():
        acc.pole(north=True)
    if south or (lo <= -90.0 + POLE_PROXIMITY_DEG).any():
        acc.pole(north=False)


def _bound_values(np: Any, data: Any, starts: Any, ends: Any, acc: _Bound, ctx: _Context) -> None:
    """Fold the WKB values ``data[starts[i]:ends[i]]`` into ``acc``."""
    if len(starts) == 0:
        return
    chains = _decode(np, data, starts, ends)
    lon, lat, chain_of = _coordinates(np, data, chains)
    # POINT EMPTY is NaN NaN; any other non-finite coordinate is unreadable.
    point_empty = (chains.role[chain_of] == _POINTS) & np.isnan(lon) & np.isnan(lat)
    if point_empty.any():
        keep = ~point_empty
        lon, lat, chain_of = lon[keep], lat[keep], chain_of[keep]
    if lon.size == 0:
        return
    _require(np.isfinite(lon).all() and np.isfinite(lat).all(), "non-finite coordinate")
    _require((np.abs(lat) <= 90.0).all(), "latitude outside [-90, 90]")
    _bound_vertices(np, _normalized_lon(np, lon), lat, chain_of, chains.role, acc, ctx)


def _binary_storage(array: Any) -> Any:
    import pyarrow as pa

    storage = array.storage if isinstance(array, pa.ExtensionArray) else array
    if not (pa.types.is_binary(storage.type) or pa.types.is_large_binary(storage.type)):
        storage = storage.cast(pa.large_binary())
    return storage


def _bound_array(array: Any, acc: _Bound, ctx: _Context) -> None:
    """Fold an Arrow array of WKB into ``acc``, a bounded slice of bytes at a time."""
    import pyarrow as pa

    np = _np()
    storage = _binary_storage(array)
    length = len(storage)
    if length == 0:
        return
    buffers = storage.buffers()
    width = np.int64 if pa.types.is_large_binary(storage.type) else np.int32
    offsets = np.frombuffer(buffers[1], dtype=width)[storage.offset : storage.offset + length + 1]
    offsets = offsets.astype(np.int64)
    data = np.frombuffer(buffers[2], np.uint8) if buffers[2] is not None else np.empty(0, np.uint8)
    valid = (
        storage.is_valid().to_numpy(zero_copy_only=False)
        if storage.null_count
        else np.ones(length, bool)
    )
    begin = 0
    while begin < length:
        stop = int(np.searchsorted(offsets, offsets[begin] + _SLICE_BYTES, side="right")) - 1
        stop = min(max(stop, begin + 1), length)
        keep = valid[begin:stop]
        _bound_values(
            np, data, offsets[begin:stop][keep], offsets[begin + 1 : stop + 1][keep], acc, ctx
        )
        begin = stop


def _context(edges: str, orientation: str | None, ellipsoid: tuple | None) -> _Context:
    if edges != SPHERICAL and edges not in ELLIPSOIDAL_ALGORITHMS:
        raise ValueError(f"edges must be spherical or an ellipsoidal algorithm, not {edges!r}")
    semi_major, flattening = ellipsoid if ellipsoid else WGS84_ELLIPSOID
    return _Context(edges, orientation, (float(semi_major), float(flattening)))


def geodesic_bbox_of_wkb(
    values: Iterable[Any],
    edges: str = SPHERICAL,
    *,
    orientation: str | None = None,
    ellipsoid: tuple[float, float] | None = None,
) -> Box | None:
    """The geodesic bound of some WKB values, or None.

    Args:
        values: WKB geometries (bytes-like); ``None`` entries are skipped.
        edges: The edge algorithm, a non-planar GeoParquet ``edges`` name.
        orientation: The column's ``orientation``; ``"counterclockwise"``
            puts each ring's interior on its left, anything else reads a ring
            as the smaller of the two regions it bounds.
        ellipsoid: ``(semi-major axis, flattening)`` for the ellipsoidal
            algorithms; WGS84 when omitted.

    Returns:
        ``(xmin, ymin, xmax, ymax)`` with ``xmin > xmax`` for an antimeridian
        wrap, or ``None`` when there is nothing to bound -- no non-empty
        geometry -- or a value cannot be bounded (unparsable WKB, a non-finite
        coordinate, a latitude outside [-90, 90]). Never raises for bad data.
    """
    import pyarrow as pa

    ctx = _context(edges, orientation, ellipsoid)
    items = [bytes(v) if isinstance(v, (bytearray, memoryview)) else v for v in values]
    try:
        array = pa.array(items, type=pa.binary())
    except (TypeError, ValueError, pa.ArrowException):
        return None
    acc = _Bound()
    try:
        _bound_array(array, acc, ctx)
    except _WkbError:
        return None
    return acc.result()


def row_group_geography_bboxes(
    parquet_file: str,
    column: str,
    edges: str,
    *,
    orientation: str | None = None,
    ellipsoid: tuple[float, float] | None = None,
) -> ColumnBounds:
    """Each row group's geodesic bound for one column, and the whole file's.

    Reads one batch of one row group of the one column at a time, so the
    memory bound is a batch, not the file. A row group that cannot be bounded
    gets ``None``; an unreadable one also makes the file-level answer
    ``None``, since a whole-file bound that skips rows would under-cover.
    """
    import pyarrow.parquet as pq

    ctx = _context(edges, orientation, ellipsoid)
    per_group: list[Box | None] = []
    boxes: list[Box] = []
    unreadable = 0
    with pq.ParquetFile(parquet_file) as reader:
        for index in range(reader.metadata.num_row_groups):
            acc = _Bound()
            try:
                for batch in reader.iter_batches(
                    batch_size=_BATCH_ROWS, row_groups=[index], columns=[column], use_threads=False
                ):
                    _bound_array(batch.column(0), acc, ctx)
            except _WkbError:
                per_group.append(None)
                unreadable += 1
                continue
            box = acc.result()
            per_group.append(box)
            if box is not None:
                boxes.append(box)
    if unreadable or not boxes:
        return ColumnBounds(per_group, None, unreadable)
    xmin, xmax = merge_longitude_ranges([(box[0], box[2]) for box in boxes])
    whole = (xmin, min(b[1] for b in boxes), xmax, max(b[3] for b in boxes))
    return ColumnBounds(per_group, whole, unreadable)


def geography_statistics(
    parquet_file: str,
    geography: Mapping[str, str],
    *,
    orientations: Mapping[str, str | None] | None = None,
    ellipsoids: Mapping[str, tuple[float, float]] | None = None,
) -> dict[str, ColumnBounds]:
    """Geodesic bounds for every column about to be retyped GEOGRAPHY.

    ``geography`` maps column -> edges; ``orientations`` and ``ellipsoids``
    give each column's declared ``orientation`` and its CRS's ellipsoid.
    """
    return {
        name: row_group_geography_bboxes(
            parquet_file,
            name,
            edges,
            orientation=(orientations or {}).get(name),
            ellipsoid=(ellipsoids or {}).get(name),
        )
        for name, edges in geography.items()
    }
