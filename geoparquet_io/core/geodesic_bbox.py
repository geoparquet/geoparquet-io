"""Geodesic bounding boxes for GEOGRAPHY columns (#1272).

A planar min/max over the vertices does not bound a geodesic edge: a long
east-west great-circle edge at high latitude bulges poleward, so a reader that
prunes row groups by a vertex box can skip rows that really match. #1254
therefore *dropped* the bounding box when it retyped a column GEOGRAPHY, which
is safe but gives up row-group pruning entirely. This module computes a box
that actually bounds the edges, so the statistics can be written instead.

What "bounds" means here follows the Parquet geospatial statistics and
RFC 7946 5.2: X and Y are canonical lon/lat ([-180, 180], [-90, 90]) and
``xmin > xmax`` marks a box that wraps the antimeridian.

For ``SPHERICAL`` edges the bound is exact and closed-form. Each edge is the
minor great-circle arc between its vertices; the arc's latitude extremes are
the endpoints plus, when the circle's vertex (Clairaut's northernmost point)
falls inside the arc, that vertex -- found here in 3-D, as the projection of
the pole axis onto the circle's plane, which sidesteps the bearing
trigonometry's pole singularities. Longitude along a great-circle arc is
monotonic, so an edge covers exactly the lon interval between its endpoints,
taken the short way round; the per-geometry union of those intervals is the
complement of the widest uncovered gap (``merge_longitude_ranges``). A polygon
ring whose longitude winding is a full turn encloses a pole; which pole follows
the ring's orientation (interior on the left, RFC 7946's right-hand rule), and
the enclosed pole joins the box. Degenerate edges -- antipodal endpoints, whose
geodesic is undefined -- bound to the whole globe rather than to a guess.

For the ellipsoidal algorithms (``VINCENTY``, ``THOMAS``, ``ANDOYER``,
``KARNEY``) the spherical bound is padded by
:data:`ELLIPSOIDAL_LAT_EPSILON_DEG`. An ellipsoidal geodesic obeys Clairaut's
relation in the *reduced* latitude beta, where tan(beta) = (1-f) tan(phi), so
it behaves as a great circle whose latitudes are off from the geographic ones
by at most ~f/2 radians (~0.1 degrees at WGS84 flattening); 0.2 degrees covers
that with margin. Longitude is monotonic along an ellipsoidal geodesic too, so
the lon interval needs no pad.

Everything is a bound, never an exact extent: a box may over-cover (a wrong
guess would under-cover), because the one property pruning needs is that every
point a reader treats as part of the geometry is inside the box.
"""

from __future__ import annotations

import math
import struct
from collections.abc import Iterable, Mapping

from geoparquet_io.core.crs_utils import merge_longitude_ranges

#: Latitude pad applied to the spherical bound for the ellipsoidal edge
#: algorithms. See the module docstring for the derivation.
ELLIPSOIDAL_LAT_EPSILON_DEG = 0.2

# ISO WKB geometry kinds (the type code modulo 1000).
_POINT, _LINESTRING, _POLYGON = 1, 2, 3
_MULTIPOINT, _MULTILINESTRING, _MULTIPOLYGON, _COLLECTION = 4, 5, 6, 7
# EWKB dimension/SRID flag bits, tolerated alongside the ISO +1000/+2000 codes.
_EWKB_Z, _EWKB_M, _EWKB_SRID = 0x80000000, 0x40000000, 0x20000000

#: A ring's longitude winding within this of a full turn means it circles a pole.
_FULL_TURN_TOLERANCE_DEG = 90.0

_MAX_NESTING = 32


class _WkbError(ValueError):
    """The bytes are not WKB this module can read; the caller bounds nothing."""


def _norm_lon(lon: float) -> float:
    """``lon`` wrapped into [-180, 180], keeping +180 as +180."""
    if -180.0 <= lon <= 180.0:
        return lon
    wrapped = math.remainder(lon, 360.0)
    # math.remainder returns -180.0 for every odd multiple of 180; keep the
    # sign the caller had so an eastward end at +180 stays the box's xmax.
    if wrapped == -180.0 and lon > 0:
        return 180.0
    return wrapped


def _unit(lon_deg: float, lat_deg: float) -> tuple[float, float, float]:
    lon, lat = math.radians(lon_deg), math.radians(lat_deg)
    cos_lat = math.cos(lat)
    return (cos_lat * math.cos(lon), cos_lat * math.sin(lon), math.sin(lat))


def _cross(a, b) -> tuple[float, float, float]:
    return (
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    )


def _dot(a, b) -> float:
    return a[0] * b[0] + a[1] * b[1] + a[2] * b[2]


def _on_arc(v1, v2, n, q) -> bool:
    """Whether ``q`` (on the great circle with normal ``n``) lies on arc v1->v2."""
    return _dot(_cross(v1, q), n) >= -1e-12 and _dot(_cross(q, v2), n) >= -1e-12


class _Accumulator:
    """The running bound: latitude extremes plus longitude intervals."""

    def __init__(self) -> None:
        self.lat_lo = math.inf
        self.lat_hi = -math.inf
        self.lon_ranges: list[tuple[float, float]] = []
        self.all_lons = False
        self.empty = True

    def point(self, lon: float, lat: float) -> None:
        lon = _norm_lon(lon)
        self.empty = False
        self.lat_lo = min(self.lat_lo, lat)
        self.lat_hi = max(self.lat_hi, lat)
        if not self.all_lons:
            self.lon_ranges.append((lon, lon))

    def pole(self, north: bool) -> None:
        self.empty = False
        self.all_lons = True
        if north:
            self.lat_hi = 90.0
        else:
            self.lat_lo = -90.0

    def globe(self) -> None:
        self.empty = False
        self.all_lons = True
        self.lat_lo, self.lat_hi = -90.0, 90.0

    def _lon_sweep(self, start: float, sweep: float) -> None:
        """Cover the arc going east from ``start`` by ``sweep`` degrees."""
        if self.all_lons:
            return
        end = start + sweep
        if end <= 180.0:
            self.lon_ranges.append((start, end))
        else:
            self.lon_ranges.append((start, end - 360.0))  # wraps the antimeridian

    def edge(self, a: tuple[float, float], b: tuple[float, float]) -> float:
        """Bound one geodesic edge; returns its signed lon step for ring winding."""
        lon1, lat1 = _norm_lon(a[0]), a[1]
        lon2, lat2 = _norm_lon(b[0]), b[1]
        self.point(lon1, lat1)
        if (lon1, lat1) == (lon2, lat2):
            return 0.0
        self.point(lon2, lat2)

        dlon = math.remainder(lon2 - lon1, 360.0)
        if abs(abs(dlon) - 180.0) > 1e-9:
            if dlon >= 0:
                self._lon_sweep(lon1, dlon)
            else:
                self._lon_sweep(lon2, -dlon)
        # 180 apart in lon: the minor arc runs along the two meridians, over a
        # pole the 3-D extremum below finds; the meridians themselves are
        # already covered as the endpoints' point-ranges.

        v1, v2 = _unit(lon1, lat1), _unit(lon2, lat2)
        normal = _cross(v1, v2)
        length = math.sqrt(_dot(normal, normal))
        if length < 1e-12:
            # Antipodal endpoints: every great circle through them is "the"
            # geodesic, so nothing smaller than the globe bounds the edge.
            self.globe()
            return dlon
        n = tuple(c / length for c in normal)

        # The circle's northernmost point is the pole axis projected onto its
        # plane; the southernmost is its antipode. Each bounds the arc only if
        # it lies on it. A circle whose plane is horizontal is the equator,
        # whose latitude extremes are its endpoints' already.
        tilt = (-n[2] * n[0], -n[2] * n[1], 1.0 - n[2] * n[2])
        tilt_len = math.sqrt(_dot(tilt, tilt))
        if tilt_len >= 1e-12:
            top = (tilt[0] / tilt_len, tilt[1] / tilt_len, tilt[2] / tilt_len)
            for q in (top, (-top[0], -top[1], -top[2])):
                if _on_arc(v1, v2, n, q):
                    lat = math.degrees(math.asin(max(-1.0, min(1.0, q[2]))))
                    self.lat_lo = min(self.lat_lo, lat)
                    self.lat_hi = max(self.lat_hi, lat)
        return dlon

    def chain(self, coords: list[tuple[float, float]], ring: bool) -> None:
        """Bound a vertex chain; a ring that winds a full turn encloses a pole."""
        if not coords:
            return
        if len(coords) == 1:
            self.point(*coords[0])
            return
        winding = 0.0
        for a, b in zip(coords, coords[1:], strict=False):
            winding += self.edge(a, b)
        if ring and abs(winding) > 360.0 - _FULL_TURN_TOLERANCE_DEG:
            # Interior on the left of the traversal (RFC 7946's right-hand
            # rule): an eastward turn circles the north pole, a westward one
            # the south pole.
            self.pole(north=winding > 0)

    def result(self, edges: str) -> tuple[float, float, float, float] | None:
        """``(xmin, ymin, xmax, ymax)``, ``xmin > xmax`` wrapping, or None."""
        if self.empty:
            return None
        lat_lo, lat_hi = self.lat_lo, self.lat_hi
        if edges != "spherical":
            lat_lo -= ELLIPSOIDAL_LAT_EPSILON_DEG
            lat_hi += ELLIPSOIDAL_LAT_EPSILON_DEG
        lat_lo, lat_hi = max(lat_lo, -90.0), min(lat_hi, 90.0)
        if self.all_lons:
            return (-180.0, lat_lo, 180.0, lat_hi)
        xmin, xmax = merge_longitude_ranges(self.lon_ranges)
        return (max(xmin, -180.0), lat_lo, min(xmax, 180.0), lat_hi)


def _read_points(buf, pos: int, fmt: str, ndims: int, count: int) -> tuple[list, int]:
    points = struct.unpack_from(f"{fmt}{count * ndims}d", buf, pos)
    pos += count * ndims * 8
    return [(points[i * ndims], points[i * ndims + 1]) for i in range(count)], pos


def _header(buf, pos: int) -> tuple[str, int, int, int]:
    """One WKB header: the struct format, geometry kind, ndims and next offset."""
    if pos >= len(buf) or buf[pos] not in (0, 1):
        raise _WkbError("not a WKB byte-order mark")
    fmt = "<" if buf[pos] == 1 else ">"
    (raw,) = struct.unpack_from(f"{fmt}I", buf, pos + 1)
    pos += 5
    if raw & _EWKB_SRID:
        pos += 4
    code = raw & 0x0FFFFFFF
    kind, iso_dims = code % 1000, code // 1000
    if iso_dims > 3:
        raise _WkbError(f"WKB dimension code {iso_dims} is not ISO")
    ndims = (
        2
        + (iso_dims in (1, 3) or bool(raw & _EWKB_Z))
        + (iso_dims in (2, 3) or bool(raw & _EWKB_M))
    )
    return fmt, kind, ndims, pos


def _parse(buf, pos: int, acc: _Accumulator, depth: int = 0) -> int:
    """Feed one WKB geometry at ``pos`` into ``acc``; returns the next offset."""
    if depth > _MAX_NESTING:
        raise _WkbError("WKB nests deeper than this reader follows")
    fmt, kind, ndims, pos = _header(buf, pos)
    if kind == _POINT:
        points, pos = _read_points(buf, pos, fmt, ndims, 1)
        lon, lat = points[0]
        if not (math.isnan(lon) or math.isnan(lat)):  # POINT EMPTY is NaN NaN
            acc.point(lon, lat)
        return pos
    (count,) = struct.unpack_from(f"{fmt}I", buf, pos)
    pos += 4
    if kind == _LINESTRING:
        points, pos = _read_points(buf, pos, fmt, ndims, count)
        acc.chain(points, ring=False)
        return pos
    if kind == _POLYGON:
        for _ in range(count):
            (ring_count,) = struct.unpack_from(f"{fmt}I", buf, pos)
            points, pos = _read_points(buf, pos + 4, fmt, ndims, ring_count)
            acc.chain(points, ring=True)
        return pos
    if kind in (_MULTIPOINT, _MULTILINESTRING, _MULTIPOLYGON, _COLLECTION):
        for _ in range(count):
            pos = _parse(buf, pos, acc, depth + 1)
        return pos
    raise _WkbError(f"WKB geometry kind {kind} is not one this reader knows")


def _accumulate(values: Iterable[bytes | None], acc: _Accumulator) -> None:
    for value in values:
        if value is None:
            continue
        try:
            _parse(bytes(value), 0, acc)
        except (_WkbError, struct.error, IndexError) as exc:
            raise _WkbError(f"unreadable WKB value: {exc}") from exc


def geodesic_bbox_of_wkb(
    values: Iterable[bytes | None], edges: str = "spherical"
) -> tuple[float, float, float, float] | None:
    """The geodesic bound of some WKB values, or None.

    Args:
        values: WKB geometries; ``None`` entries are skipped.
        edges: The edge algorithm, a GeoParquet ``edges`` name. ``spherical``
            is exact; the ellipsoidal names get the documented latitude pad.

    Returns:
        ``(xmin, ymin, xmax, ymax)`` with ``xmin > xmax`` for an antimeridian
        wrap, or ``None`` when there is nothing to bound -- no non-empty
        geometry, or a value this reader cannot parse (a bound computed from
        a misread geometry could under-cover, so none is written at all).
    """
    acc = _Accumulator()
    try:
        _accumulate(values, acc)
    except _WkbError:
        return None
    return acc.result(edges)


def _column_wkb_values(table, column: str):
    """The WKB bytes of one column of an Arrow table, extension storage unwrapped."""
    for chunk in table.column(0).chunks:
        storage = chunk.storage if hasattr(chunk, "storage") else chunk
        for value in storage:
            yield value.as_py()


def row_group_geography_bboxes(
    parquet_file: str, column: str, edges: str
) -> tuple[list[tuple[float, float, float, float] | None], tuple | None]:
    """Each row group's geodesic bound for one column, and the whole file's.

    Reads one row group's worth of the one column at a time, so the memory
    bound is a row group, not the file. A row group that cannot be bounded --
    no non-empty geometry, or WKB the reader cannot parse -- gets ``None``
    (its planar box is dropped, #1254's behavior); an unparsable row group
    also makes the file-level answer ``None``, since a whole-file bound that
    skips rows would under-cover.

    Returns:
        ``(per_row_group, file_bbox)``, every box ``(xmin, ymin, xmax, ymax)``
        with ``xmin > xmax`` for an antimeridian wrap.
    """
    import pyarrow.parquet as pq

    per_group: list[tuple | None] = []
    boxes: list[tuple] = []
    poisoned = False
    with pq.ParquetFile(parquet_file) as reader:
        for index in range(reader.metadata.num_row_groups):
            acc = _Accumulator()
            try:
                _accumulate(
                    _column_wkb_values(reader.read_row_group(index, columns=[column]), column),
                    acc,
                )
            except _WkbError:
                per_group.append(None)
                poisoned = True
                continue
            box = acc.result(edges)
            per_group.append(box)
            if box is not None:
                boxes.append(box)
    if poisoned or not boxes:
        return per_group, None
    xmin, xmax = merge_longitude_ranges([(box[0], box[2]) for box in boxes])
    return per_group, (xmin, min(b[1] for b in boxes), xmax, max(b[3] for b in boxes))


def geography_statistics(
    parquet_file: str, geography: Mapping[str, str]
) -> tuple[dict[str, list], dict[str, tuple]]:
    """Geodesic bounds for every column about to be retyped GEOGRAPHY.

    Returns ``(per_row_group, file_level)``: the first in ``patch_footer``'s
    ``geography_bbox`` shape, the second for the ``geo`` block's per-column
    ``bbox``, columns with nothing boundable left out of it.
    """
    per_group: dict[str, list] = {}
    file_level: dict[str, tuple] = {}
    for name, edges in geography.items():
        groups, whole = row_group_geography_bboxes(parquet_file, name, edges)
        per_group[name] = groups
        if whole is not None:
            file_level[name] = whole
    return per_group, file_level


def lon_range_contains(
    outer: tuple[float, float], inner: tuple[float, float], tolerance: float = 1e-6
) -> bool:
    """Whether one lon interval contains another, both possibly wrapped.

    Each interval runs east from its first value to its second; first > second
    wraps the antimeridian (RFC 7946 5.2). Used by ``check spec`` to ask
    whether a declared GEOGRAPHY box could contain the geodesic one.
    """

    def extent(lo: float, hi: float) -> float:
        return hi - lo if lo <= hi else hi - lo + 360.0

    outer_extent = extent(*outer)
    if outer_extent >= 360.0 - tolerance:
        return True
    offset = (inner[0] - outer[0]) % 360.0
    return offset <= outer_extent + tolerance and offset + extent(*inner) <= (
        outer_extent + tolerance
    )
