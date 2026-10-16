"""Extent decoding from partition file names, and the glob pruning built on it.

Some published datasets encode their spatial layout only in their file names:
S2 cell tokens (VIDA's ``by_country_s2`` buildings), Bing/Web Mercator
quadkeys (OpenBuildingMap) or lon/lat grid corners (GlobalBuildingAtlas).
DuckDB cannot prune those — no hive partitioning, no bbox covering — so a bbox
query over a thousand-file glob fetches a thousand Parquet footers before
reading a byte of data. Decoding the extent from the name skips the
non-overlapping files up front.

Pruning is a *fallback*, and correctness beats speed: it engages only when
every file matched by a glob decodes under one known naming convention
(:func:`prune_files_by_extent` returns ``None`` otherwise), so an unrecognized
name can never cause a file to be silently skipped. The registry of
conventions stays small and dataset-driven — each new convention is a new
decoder with its own tests, not a config language.

This module is pure Python over the standard library — no duckdb, no click,
no third-party imports — so it belongs to the embeddable core (#1311) and the
QGIS plugins can vendor it back. Ported from qgis_plugin_gpq_downloader's
``gpq_downloader/utils.py`` (GPL-2.0-or-later, single copyright holder), which
the copyright holder relicensed to Apache-2.0 by contributing it here (see the
pull request for issue #1309). Python >= 3.9 syntax on purpose: QGIS LTR still
runs 3.9 on some platforms.
"""

from __future__ import annotations

import math
import re
from collections.abc import Sequence

Extent = tuple[float, float, float, float]

#: Degrees added around the query bbox before testing overlap, so a box that
#: touches a tile edge also reads the neighbouring tile.
DEFAULT_PRUNE_BUFFER_DEG = 0.01

# building.002213.parquet (OpenBuildingMap): Bing/Web Mercator quadkey after a
# prefix. A prefix and at least 4 digits are required so names like 0.parquet
# or part_1.parquet are not mistaken for quadkeys.
_QUADKEY_FILE_RE = re.compile(r"^(.*[._])([0-3]{4,})\.parquet$")
# e100_n20_e105_n15.parquet (GlobalBuildingAtlas): lon/lat corners of a grid cell
_GRID_FILE_RE = re.compile(r"^([ew])(\d{3})_([ns])(\d{2})_([ew])(\d{3})_([ns])(\d{2})\.parquet$")
# 3517311308976357376.parquet (VIDA by_country_s2): an S2 cell id, sometimes
# written as a signed 64-bit integer
_S2_FILE_RE = re.compile(r"^(-?\d{4,20})\.parquet$")
# Coarsest-to-finest S2 levels accepted as partition cells. Requiring level <= 20
# keeps small numbered files (0.parquet, 17.parquet) from being read as cells.
_S2_MAX_LEVEL = 20
_S2_POS_TO_IJ = ((0, 1, 3, 2), (0, 2, 3, 1), (3, 2, 0, 1), (3, 1, 0, 2))
_S2_POS_TO_ORIENTATION = (1, 0, 0, 3)


def s2_cell_level(cell_id: int) -> int | None:
    """Level (0-30) of a valid S2 cell id, or None if it isn't one."""
    if cell_id < 0:
        cell_id += 1 << 64
    if cell_id <= 0 or cell_id >= 1 << 64 or cell_id >> 61 > 5:
        return None
    lowest_bit = (cell_id & -cell_id).bit_length() - 1
    if lowest_bit % 2:
        return None
    return 30 - lowest_bit // 2


def _s2_face_ij(cell_id: int, face: int, level: int) -> tuple[int, int]:
    """Decode the cell's (i, j) position on its face from the Hilbert curve position."""
    orientation = face & 1
    i = j = 0
    for k in range(level):
        pos = (cell_id >> (59 - 2 * k)) & 3
        ij = _S2_POS_TO_IJ[orientation][pos]
        i = (i << 1) | (ij >> 1)
        j = (j << 1) | (ij & 1)
        orientation ^= _S2_POS_TO_ORIENTATION[pos]
    return i, j


def _st_to_uv(st: float) -> float:
    return (4 * st * st - 1) / 3 if st >= 0.5 else (1 - 4 * (1 - st) ** 2) / 3


def s2_cell_bounds(cell_id: int) -> Extent:
    """Return (xmin, ymin, xmax, ymax) in WGS84 of an S2 cell.

    Cell edges are geodesics, so points along each edge are sampled, not
    just the corners. Partitions are coarse (level <= 20), so this is cheap.
    """
    if cell_id < 0:
        cell_id += 1 << 64
    level = s2_cell_level(cell_id)
    if level is None:
        raise ValueError(f"not a valid S2 cell id: {cell_id}")
    face = cell_id >> 61
    if level == 0:  # a whole cube face; faces 2 and 5 contain a pole
        return (-180.0, -90.0, 180.0, 90.0)
    i, j = _s2_face_ij(cell_id, face, level)
    n = 1 << level
    steps = 8
    lons, lats = [], []
    for a in range(steps + 1):
        for b in range(steps + 1):
            if a not in (0, steps) and b not in (0, steps):
                continue  # only the cell's edges
            u, v = _st_to_uv((i + a / steps) / n), _st_to_uv((j + b / steps) / n)
            x, y, z = ((1, u, v), (-u, 1, v), (-u, -v, 1), (-1, -v, -u), (v, -1, -u), (v, u, -1))[
                face
            ]
            lats.append(math.degrees(math.atan2(z, math.hypot(x, y))))
            lons.append(math.degrees(math.atan2(y, x)))
    if max(lats) > 89.9 or min(lats) < -89.9:  # touches a pole: every longitude
        return (-180.0, min(lats), 180.0, max(lats))
    if max(lons) - min(lons) > 180:
        # S2 cell edges lie along the antimeridian rather than crossing it, but
        # points on such an edge can come out as -180 instead of 180 (or the
        # reverse); put them on the same side as the rest of the cell
        east = sum(x > 0 for x in lons) >= sum(x < 0 for x in lons)
        lons = [(180.0 if east else -180.0) if abs(x) > 179.999999 else x for x in lons]
    return (min(lons), min(lats), max(lons), max(lats))


def quadkey_bounds(quadkey: str) -> Extent:
    """Return (xmin, ymin, xmax, ymax) in WGS84 of a Bing/Web Mercator quadkey tile."""
    zoom = len(quadkey)
    x = y = 0
    for i, digit in enumerate(quadkey):
        mask = 1 << (zoom - 1 - i)
        d = int(digit)
        if d & 1:
            x |= mask
        if d & 2:
            y |= mask
    n = 2**zoom

    def lat(tile_y: int) -> float:
        return math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * tile_y / n))))

    return (x / n * 360.0 - 180.0, lat(y + 1), (x + 1) / n * 360.0 - 180.0, lat(y))


def extent_from_file_name(name: str) -> Extent | None:
    """Extent encoded in a partition file name, or None if the name has no known pattern."""
    m = _GRID_FILE_RE.match(name)
    if m:
        ew1, x1, ns1, y1, ew2, x2, ns2, y2 = m.groups()
        x1 = int(x1) * (-1 if ew1 == "w" else 1)
        x2 = int(x2) * (-1 if ew2 == "w" else 1)
        y1 = int(y1) * (-1 if ns1 == "s" else 1)
        y2 = int(y2) * (-1 if ns2 == "s" else 1)
        return (min(x1, x2), min(y1, y2), max(x1, x2), max(y1, y2))
    m = _QUADKEY_FILE_RE.match(name)
    if m:
        return quadkey_bounds(m.group(2))
    m = _S2_FILE_RE.match(name)
    if m:
        level = s2_cell_level(int(m.group(1)))
        if level is not None and level <= _S2_MAX_LEVEL:
            return s2_cell_bounds(int(m.group(1)))
    return None


def _quadkey_tiling_is_ambiguous(names: Sequence[str]) -> bool:
    """Whether quadkey-looking names cannot be trusted as one tiling.

    Only treat names as quadkeys when they look like a single tiling: one
    prefix, one zoom level of at least 5, and no repeated names. Otherwise
    names like data_2021.parquet or country=*/part_0000.parquet would be
    mistaken for tiles and their data silently skipped.
    """
    quadkeys = [m for m in (_QUADKEY_FILE_RE.match(n) for n in names) if m]
    if not quadkeys:
        return False
    return (
        len({m.group(1) for m in quadkeys}) > 1
        or len({len(m.group(2)) for m in quadkeys}) > 1
        or len(quadkeys[0].group(2)) < 5
        or len(set(names)) < len(names)
    )


def prune_files_by_extent(
    files: Sequence[str],
    bbox: Extent,
    buffer_deg: float = DEFAULT_PRUNE_BUFFER_DEG,
) -> list | None:
    """The subset of ``files`` whose name-encoded WGS84 extent intersects ``bbox``.

    The pruning fallback for bbox queries over multi-file globs DuckDB cannot
    prune. Returns ``None`` whenever pruning must not engage — any file name
    that no known convention decodes, or a quadkey tiling that is ambiguous,
    disables it for the whole listing, and a selection that keeps every file
    gains nothing over the glob. Otherwise returns the sorted selection, which
    is empty when no file overlaps ``bbox``.

    Args:
        files: Every path a glob or directory listing matched; only the part
            after the last path separator is decoded.
        bbox: (xmin, ymin, xmax, ymax) in WGS84 — file-name conventions encode
            lon/lat, so a bbox in a projected CRS must not be passed here.
        buffer_deg: Degrees added around ``bbox`` before the overlap test.
    """
    named = [(f, str(f).replace("\\", "/").rsplit("/", 1)[-1]) for f in files]
    if _quadkey_tiling_is_ambiguous([name for _, name in named]):
        return None
    xmin, ymin = bbox[0] - buffer_deg, bbox[1] - buffer_deg
    xmax, ymax = bbox[2] + buffer_deg, bbox[3] + buffer_deg
    selected = []
    for f, name in named:
        extent = extent_from_file_name(name)
        if extent is None:
            return None
        fxmin, fymin, fxmax, fymax = extent
        if fxmin <= xmax and fxmax >= xmin and fymin <= ymax and fymax >= ymin:
            selected.append(f)
    if len(selected) == len(files):
        return None  # nothing pruned: let the caller keep the glob
    return sorted(selected)
