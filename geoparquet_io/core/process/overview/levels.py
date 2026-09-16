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


#: One ``level:minzoom`` entry as typed, before the plan is validated.
Entry = tuple[int | str, int]


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
    construction and cannot leave a zoom unserved. A plan is therefore written
    coarsest first; one written the other way round is told so, with the
    spelling that works (gpio#1103).
    """
    entries = _parse_entries(spec)
    if _is_finest_first(entries):
        raise InvalidParameterError("bands", _reversed_plan_reason(entries))
    return _validate_entries(entries)


def _parse_entries(spec: str) -> list[Entry]:
    entries: list[Entry] = []
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
        entries.append((_parse_level(level_text, part), minzoom))
    if not entries:
        raise InvalidParameterError("bands", "no bands given")
    return entries


def _parse_level(level_text: str, part: str) -> int | str:
    """Levels are ints for grid schemes and names for admin ones."""
    if not level_text.lstrip("-").isdigit():
        return level_text
    try:
        level = int(level_text)
    except ValueError:  # isdigit() is wider than int(): '²', '--5', 5000 digits
        raise InvalidParameterError("bands", f"level must be an integer in {part!r}") from None
    if level < 0:
        raise InvalidParameterError("bands", f"level cannot be negative in {part!r}")
    return level


def _validate_entries(entries: list[Entry]) -> list[Band]:
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


def _is_finest_first(entries: list[Entry]) -> bool:
    """Every zoom lower than the one before it: the plan was written from the detailed end."""
    zooms = [z for _, z in entries]
    return len(zooms) > 1 and all(b < a for a, b in zip(zooms, zooms[1:], strict=False))


def _reversed_plan_reason(entries: list[Entry]) -> str:
    """Why a finest-first plan is refused, with the coarsest-first spelling when that one works."""
    turned = list(reversed(entries))
    spelling = ",".join(f"{lvl}:{z}" for lvl, z in turned)
    diagnosis = f"bands look reversed: the zooms run from z{entries[0][1]} down to z{turned[0][1]}"
    try:
        _validate_entries(turned)
    except InvalidParameterError as inner:
        return f"{diagnosis}, and turned around they still fail: {inner.reason}"
    return f"{diagnosis}. A plan is written coarsest first, starting at z0 -- try {spelling!r}"


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
