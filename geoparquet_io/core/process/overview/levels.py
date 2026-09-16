#!/usr/bin/env python3
"""Zoom-band and overview-level selection for `gpio process overview`.

Shared with `gpio pmtiles pyramid`: both need to know which aggregate level
should serve which WebMercator zoom range so that no tile blows past the
tile-size budget. The selection is driven by a *worst tile* probe -- for each
candidate level, the number of cells landing in the fullest tile per zoom --
multiplied by an estimated compressed bytes-per-cell.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from geoparquet_io.core.exceptions import InvalidParameterError

# Estimated compressed bytes a single cell contributes to an MVT tile,
# by output geometry mode. Deliberately conservative (budget heuristic,
# not a codec model); calibrated loosely against GlobalBuildingAtlas
# measurements (a5 res-5 z0 tile ~297 KB, res-6 z0 ~791 KB).
GEOMETRY_BYTES: dict[str, float] = {
    "polygon": 35.0,
    "centroid": 12.0,
    "both": 45.0,
    "none": 0.0,
}
BYTES_PER_ATTRIBUTE = 8.0
CELL_ID_BYTES = 8.0

DEFAULT_MAX_TILE_KB = 500  # tippecanoe's default per-tile cap
MAX_PROBE_ZOOM = 20


@dataclass(frozen=True)
class Band:
    """One zoom band of the pyramid: ``level`` serves zooms minzoom..maxzoom.

    ``maxzoom is None`` marks the final (open-ended) band.
    """

    level: int | str
    minzoom: int
    maxzoom: int | None


def parse_bands(spec: str) -> list[Band]:
    """Parse an explicit ``level:minzoom`` band plan, e.g. ``"5:0,8:6,10:9"``.

    :func:`select_bands` infers handovers from a tile-size budget, which is the
    right default but cannot express "put level 8 at z6 whatever it costs".
    That matters when several archives are browsed as one surface: if their
    handovers land on different zooms, cells visibly change size as the user
    switches between them, even though each archive is individually well
    tiled. Stating the plan is the only way to make two pyramids agree.

    Each entry gives a level and the zoom it starts at. Ends are implied by the
    next entry, and the last band is open-ended, so the bands are contiguous by
    construction and cannot leave a zoom unserved.
    """
    entries: list[tuple[int | str, int]] = []
    for part in (p.strip() for p in spec.split(",")):
        if not part:
            continue
        level_text, sep, zoom_text = part.partition(":")
        if not sep:
            raise InvalidParameterError(
                "bands", f"expected level:minzoom pairs like '5:0,8:6,10:9', got {part!r}"
            )
        level_text, zoom_text = level_text.strip(), zoom_text.strip()
        try:
            minzoom = int(zoom_text)
        except ValueError:
            raise InvalidParameterError("bands", f"zoom must be an integer in {part!r}") from None
        if minzoom < 0:
            raise InvalidParameterError("bands", f"zoom cannot be negative in {part!r}")
        if level_text == "":
            raise InvalidParameterError("bands", f"missing level in {part!r}")
        # Levels are ints for grid schemes and names for admin ones.
        level: int | str = int(level_text) if level_text.lstrip("-").isdigit() else level_text
        if isinstance(level, int) and level < 0:
            raise InvalidParameterError("bands", f"level cannot be negative in {part!r}")
        entries.append((level, minzoom))

    if not entries:
        raise InvalidParameterError("bands", "no bands given")
    _reject_reversed_plan(entries)
    if entries[0][1] != 0:
        raise InvalidParameterError(
            "bands", f"the first band must start at z0, got z{entries[0][1]}"
        )
    for (_, prev), (_, cur) in zip(entries, entries[1:], strict=False):
        if cur <= prev:
            raise InvalidParameterError(
                "bands", f"band zooms must strictly increase, got z{prev} then z{cur}"
            )
    levels = [lvl for lvl, _ in entries]
    if len(set(levels)) != len(levels):
        raise InvalidParameterError("bands", f"a level may appear only once, got {levels}")
    # A grid pyramid gets finer as it zooms in, which is what the probe always
    # produces. A plan that goes the other way serves coarse cells at the zooms
    # that need detail and is never what the caller meant -- it is a typo.
    grid_levels = [lvl for lvl in levels if isinstance(lvl, int)]
    if len(grid_levels) == len(levels):
        for prev, cur in zip(grid_levels, grid_levels[1:], strict=False):
            if cur < prev:
                raise InvalidParameterError(
                    "bands",
                    f"levels must get finer as zoom increases, got {prev} then {cur}",
                )

    return [
        Band(level, minzoom, entries[i + 1][1] - 1 if i + 1 < len(entries) else None)
        for i, (level, minzoom) in enumerate(entries)
    ]


def _reject_reversed_plan(entries: list[tuple[int | str, int]]) -> None:
    """Catch a plan written finest-first and name the fix.

    A ladder reads naturally from the detailed end -- "level 8 serves z11,
    level 7 serves z10, ..." -- but a plan is stated coarsest first, because
    each entry's end is implied by the next. Written the other way it trips the
    z0 check, which is true of the first pair and says nothing about the order
    of all of them (gpio#1103 spelled its own example that way).

    The suggestion is parsed before it is offered, so a plan that is reversed
    *and* otherwise unusable falls through to the error describing its real
    problem rather than being handed a spelling that also fails.
    """
    zooms = [z for _, z in entries]
    if len(entries) < 2 or any(b >= a for a, b in zip(zooms, zooms[1:], strict=False)):
        return
    corrected = ",".join(f"{lvl}:{z}" for lvl, z in reversed(entries))
    try:
        parse_bands(corrected)
    except InvalidParameterError:
        return
    raise InvalidParameterError(
        "bands",
        f"bands look reversed: the zooms run from z{zooms[0]} down to z{zooms[-1]}. "
        "A plan is written coarsest first, in ascending zoom order starting at z0, "
        f"because each band ends where the next begins -- try '{corrected}'",
    )


def estimate_bytes_per_cell(num_attributes: int, out_geometry: str) -> float:
    """Estimate compressed bytes one cell contributes to a tile.

    ``num_attributes`` counts the numeric attribute columns carried per cell
    (``count`` plus every rollup/breakdown column).
    """
    return CELL_ID_BYTES + GEOMETRY_BYTES[out_geometry] + BYTES_PER_ATTRIBUTE * num_attributes


def probe_worst_tile_counts(con, cells_sql: str, max_zoom: int = MAX_PROBE_ZOOM) -> dict[int, int]:
    """Worst (max) cells-per-tile for each zoom 0..max_zoom.

    ``cells_sql`` must select one row per cell with ``lon``/``lat`` columns.
    The connection must have the ``lat_lon_to_quadkey`` UDF registered (see
    :func:`geoparquet_io.core.partition.auto_resolution._register_quadkey_udf`).
    A quadkey prefix of length ``z`` identifies the WebMercator tile at zoom
    ``z``, so one quadkey per cell at ``max_zoom`` covers every zoom via
    ``substr``. Latitudes are clamped to the WebMercator domain.
    """
    con.execute(
        "CREATE OR REPLACE TEMP TABLE __overview_qk AS "
        f"SELECT lat_lon_to_quadkey(LEAST(GREATEST(lat, -85.05), 85.05), lon, {max_zoom}) AS qk "
        f"FROM ({cells_sql}) WHERE lon IS NOT NULL AND lat IS NOT NULL"
    )
    counts: dict[int, int] = {}
    for z in range(max_zoom + 1):
        row = con.execute(
            "SELECT COALESCE(MAX(cnt), 0) FROM "
            f"(SELECT COUNT(*) AS cnt FROM __overview_qk GROUP BY substr(qk, 1, {z}))"
        ).fetchone()
        counts[z] = int(row[0]) if row else 0
    return counts


def select_bands(
    worst_counts: Mapping[int | str, Mapping[int, int]],
    candidates: Sequence[int | str],
    bytes_per_cell: float,
    max_tile_kb: int = DEFAULT_MAX_TILE_KB,
) -> list[Band]:
    """Assign candidate levels to contiguous zoom bands within the tile budget.

    Walks zooms from 0 upward, at each zoom picking the finest candidate whose
    worst tile fits ``max_tile_kb`` (never coarser than the previous pick), and
    stops once the base level -- the last candidate -- fits. When nothing fits
    at a zoom the coarsest candidate is used, so the first band always starts
    at z0 (a single oversized z0 tile is acceptable). If the base never fits
    within the probed zooms it still gets the final open-ended band.
    """
    if not candidates:
        raise InvalidParameterError("levels", "no candidate levels to select from")
    budget = max_tile_kb * 1024.0
    base = candidates[-1]
    max_zoom = min(max(zooms) for zooms in (worst_counts[lvl] for lvl in candidates))

    picks: list[int | str] = []
    prev_idx = 0
    for z in range(max_zoom + 1):
        fitting = [
            i for i, lvl in enumerate(candidates) if worst_counts[lvl][z] * bytes_per_cell <= budget
        ]
        idx = max(max(fitting) if fitting else 0, prev_idx)
        picks.append(candidates[idx])
        prev_idx = idx
        if candidates[idx] == base:
            break
    else:
        # Probe range exhausted without the base fitting; hand the remaining
        # zooms to the base anyway -- it is the real data.
        picks.append(base)

    bands: list[Band] = []
    start = 0
    for i in range(1, len(picks) + 1):
        if i == len(picks) or picks[i] != picks[i - 1]:
            bands.append(Band(picks[i - 1], start, i - 1))
            start = i
    last = bands[-1]
    bands[-1] = Band(last.level, last.minzoom, None)
    return bands
