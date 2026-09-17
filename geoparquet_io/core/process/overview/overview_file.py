"""Assemble a level ladder into one overview GeoParquet (#1117).

`gpio process overview` produces one file per level. tylertoo's overview format
describes exactly that shape in a single file -- levels are additional **rows**
tagged by a ``level`` column rather than parallel columns -- and
``tylertoo export-pmtiles`` tiles such a file directly, reading the levels as
written. Emitting it here removes the per-level tippecanoe run and the
hand-pinned zoom bands from the caller.

The contract implemented is OVERVIEWS_SPEC: the row model (2.1), coarse-to-fine
ordering with each level ending on a row-group boundary (4.2), the ``level``
column and its agreement with the footer (4.1), and the ``geo:overviews``
footer key (3.2).

Why gpio and not the tiler: a coarse level of a count layer is a
**re-aggregation**, not a generalization. Thinning would show some cells and
silently omit the rest instead of showing their sum. gpio owns the cell
hierarchy and the SQL, so it owns the rollup.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.logging_config import debug, success
from geoparquet_io.core.parquet_writer import resolve_row_group_rows

#: Footer key. The spec keeps this a single named constant so the eventual
#: merge into the official ``geo`` metadata is a rename (OVERVIEWS_SPEC 3.1).
OVERVIEWS_KEY = "geo:overviews"

#: Schema version of the emitted ``geo:overviews`` object.
OVERVIEWS_VERSION = "0.1.0"

#: Web Mercator equatorial circumference, metres (OVERVIEWS_SPEC 5.2).
_EARTH_CIRCUMFERENCE_M = 40075016.69

#: Tile-band reference in the spec's GSD formula; matches the cogp-rs convention.
_TILE_BAND_REFERENCE = 1024


#: tylertoo's ``--gsd-base`` default: the cogp-rs convention, ~4x a 256px tile
#: so sub-pixel features drop. Only used to report a ``zoom`` alongside the
#: authoritative ``gsd``.
DEFAULT_GSD_BASE = 1024.0

#: How many GSD units wide a cell should be at the level that serves it. The
#: detail knob: larger means a smaller GSD, so more cells survive tylertoo's
#: visibility gates and coarse levels get denser (and heavier).
DEFAULT_CELL_DETAIL = 4.0

#: Metres per degree for a geographic CRS (OVERVIEWS_SPEC 7.1). GSD is always
#: metres in the footer whatever the file's CRS.
METERS_PER_DEGREE = 111320.0


@dataclass(frozen=True)
class LevelInput:
    """One level of the ladder, coarse first.

    ``key`` is gpio's own level name (an H3 resolution, or ``country``) and is
    carried only for logging; the file identifies levels by ordinal. ``gsd`` is
    absolute metres -- what the footer records and what tylertoo reads.
    """

    key: int | str
    path: str
    gsd: float


def gsd_for_zoom(zoom: int) -> float:
    """Ground sample distance in metres for a Web Mercator zoom.

    OVERVIEWS_SPEC 5.2. GSD is always metres in the footer, whatever the file's
    CRS.
    """
    return _EARTH_CIRCUMFERENCE_M / _TILE_BAND_REFERENCE / (2**zoom)


def zoom_for_gsd(gsd: float, gsd_base: float = DEFAULT_GSD_BASE) -> int:
    """Invert :func:`gsd_for_zoom`, so a GSD ladder can report zooms too.

    ``gsd`` is authoritative in the footer; ``zoom`` is the convenience field
    (OVERVIEWS_SPEC 3.2, OPTIONAL). ``gsd_base`` matches tylertoo's
    ``--gsd-base`` so the two agree on which zoom a GSD denotes.
    """
    if gsd <= 0:
        raise ValueError(f"gsd must be positive, got {gsd}")
    return max(0, round(math.log2(_EARTH_CIRCUMFERENCE_M / gsd_base / gsd)))


def cell_width_meters(bounds: tuple[float, float, float, float], cell_count: int) -> float:
    """Characteristic width of one cell, in metres.

    Data-driven and scheme-agnostic: the extent divided among the cells that
    cover it. An H3-specific edge length would not serve a5 or admin levels,
    and this shrinks monotonically as levels refine, which is exactly the
    ordering the footer requires.
    """
    if cell_count <= 0:
        raise ValueError("cannot size a level with no cells")
    minx, miny, maxx, maxy = bounds
    mean_lat_rad = math.radians((miny + maxy) / 2.0)
    width_m = abs(maxx - minx) * METERS_PER_DEGREE * max(math.cos(mean_lat_rad), 0.01)
    height_m = abs(maxy - miny) * METERS_PER_DEGREE
    area = max(width_m * height_m, 1.0)
    return math.sqrt(area / cell_count)


def gsd_ladder(
    bounds: tuple[float, float, float, float],
    cell_counts: list[int],
    *,
    cell_detail: float = DEFAULT_CELL_DETAIL,
) -> list[float]:
    """GSDs for a coarse-to-fine ladder, strictly decreasing.

    Each level's GSD is its cell width divided by ``cell_detail`` -- the
    number of GSD units a cell spans at the scale it serves. Mirrors the
    direction of tylertoo's ``--gsd-base``: a larger value means a smaller
    GSD, so more cells clear the visibility gates and the level renders
    denser.

    Ties and inversions are nudged apart because the footer requires strictly
    decreasing GSDs (OVERVIEWS_SPEC 3.3); two levels with the same cell count
    are otherwise indistinguishable to a reader.
    """
    if cell_detail <= 0:
        raise ValueError(f"cell_detail must be positive, got {cell_detail}")
    gsds = [cell_width_meters(bounds, n) / cell_detail for n in cell_counts]
    for i in range(1, len(gsds)):
        if gsds[i] >= gsds[i - 1]:
            gsds[i] = gsds[i - 1] / 2.0
    return gsds


def _reject_case_colliding_level(schema: pa.Schema, path: str) -> None:
    """OVERVIEWS_SPEC 4.1: a source ``LEVEL`` would shadow the overview column.

    SQL engines resolve identifiers case-insensitively, so a case-colliding
    source column silently wins in a reader's ``WHERE level = k``.
    """
    collisions = [n for n in schema.names if n.lower() == "level"]
    if collisions:
        raise ValueError(
            f"{path} already has a column named {collisions[0]!r}; an overview file's "
            "`level` column would shadow it in reader predicates (OVERVIEWS_SPEC 4.1). "
            "Rename the source column first."
        )


def _with_level(table: pa.Table, level: int) -> pa.Table:
    """Append the NOT NULL INT32 ``level`` column (OVERVIEWS_SPEC 4.1)."""
    column = pa.array([level] * table.num_rows, type=pa.int32())
    field = pa.field("level", pa.int32(), nullable=False)
    return table.append_column(field, column)


def _zoom_ladder(gsds: list[float], gsd_base: float = DEFAULT_GSD_BASE) -> list[int]:
    """Zooms for a GSD ladder, forced strictly increasing.

    OVERVIEWS_SPEC 3.3 makes ``zoom`` optional but requires it to be strictly
    increasing when present. Rounding independently does not guarantee that:
    two adjacent levels whose GSDs are close land on the same zoom (a four
    level H3 ladder over a small extent rounds to 7, 7, 8, 8). ``gsd`` stays
    authoritative and the zoom is nudged up to keep the sequence legal.
    """
    zooms = [zoom_for_gsd(g, gsd_base) for g in gsds]
    for i in range(1, len(zooms)):
        if zooms[i] <= zooms[i - 1]:
            zooms[i] = zooms[i - 1] + 1
    return zooms


def _validate(levels: list[LevelInput]) -> None:
    if not levels:
        raise ValueError("an overview file needs at least one level; got an empty ladder")
    gsds = [lv.gsd for lv in levels]
    if any(g <= 0 for g in gsds):
        raise ValueError(f"gsd must be positive metres, got {gsds}")
    if len(gsds) > 1 and any(gsds[i] >= gsds[i - 1] for i in range(1, len(gsds))):
        raise ValueError(
            f"gsd must be strictly decreasing coarse->fine (OVERVIEWS_SPEC 3.3), got {gsds}"
        )


def write_overview_file(
    levels: list[LevelInput],
    output_path: str,
    *,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_rows: int | None = None,
    row_group_size_mb: float | None = None,
    verbose: bool = False,
) -> str:
    """Write ``levels`` (coarse first) as one overview GeoParquet.

    Each level is written with its own ``write_table`` call so it ends exactly
    on a row-group boundary, which is what lets the footer address levels by
    row-group range and a reader fetch one level band without touching the
    others (OVERVIEWS_SPEC 4.2).

    Returns the output path.
    """
    _validate(levels)

    rows_per_group = resolve_row_group_rows(row_group_rows, row_group_size_mb)
    zooms = _zoom_ladder([lv.gsd for lv in levels])

    first = pq.read_table(levels[0].path)
    _reject_case_colliding_level(first.schema, levels[0].path)

    # The source geo metadata travels with the file: CRS, primary column and
    # the 1.1 bbox covering the spec requires (4.4) are the input's, not ours.
    source_metadata = dict(first.schema.metadata or {})

    level_meta: list[dict] = []
    written = 0
    writer = None
    try:
        for ordinal, level in enumerate(levels):
            table = first if ordinal == 0 else pq.read_table(level.path)
            if ordinal:
                _reject_case_colliding_level(table.schema, level.path)
            table = _with_level(table, ordinal)

            if writer is None:
                writer = pq.ParquetWriter(
                    output_path,
                    table.schema,
                    compression=compression,
                    compression_level=compression_level,
                )
            writer.write_table(table, row_group_size=rows_per_group)

            # ParquetWriter starts a new row group per write_table call, so the
            # level boundary is the group boundary; count what this level added.
            groups = 1 if not rows_per_group else max(1, -(-table.num_rows // rows_per_group))
            written += groups
            level_meta.append(
                {
                    "row_group_end": written - 1,
                    "gsd": level.gsd,
                    "zoom": zooms[ordinal],
                }
            )
            debug(f"level {ordinal} ({level.key}): {table.num_rows} rows, {groups} row group(s)")
    finally:
        if writer is not None:
            writer.close()

    _stamp_footer(output_path, source_metadata, level_meta, compression, compression_level)
    success(f"Wrote {output_path} with {len(levels)} level(s)")
    return output_path


def _stamp_footer(
    output_path: str,
    source_metadata: dict,
    level_meta: list[dict],
    compression: str,
    compression_level: int | None,
) -> None:
    """Add the ``geo:overviews`` key without disturbing the row groups.

    The level boundaries *are* row-group boundaries (OVERVIEWS_SPEC 4.2), so
    the footer cannot be stamped by reading the table back and rewriting it --
    that recomputes the grouping and dissolves the very structure the footer
    describes. Each existing group is copied through as its own
    ``write_table`` call instead, which reproduces the layout exactly.
    """
    pf = pq.ParquetFile(output_path)
    actual_groups = pf.metadata.num_row_groups
    if level_meta[-1]["row_group_end"] != actual_groups - 1:
        level_meta = _levels_from_column(output_path, level_meta)

    overviews = {
        "version": OVERVIEWS_VERSION,
        "levels": level_meta,
        "mode": "duplicating",
        # 3.4: duplicating MUST name the finest level canonical. No `cogp` key
        # is emitted -- 3.1 says writers SHOULD omit it in this mode.
        "canonical_level": len(level_meta) - 1,
    }
    metadata = dict(source_metadata)
    metadata[OVERVIEWS_KEY.encode()] = json.dumps(overviews).encode()

    schema = pf.schema_arrow.with_metadata(metadata)
    tmp_path = f"{output_path}.stamping"
    writer = pq.ParquetWriter(
        tmp_path, schema, compression=compression, compression_level=compression_level
    )
    try:
        for rg in range(actual_groups):
            group = pf.read_row_group(rg)
            # One call, one group: row_group_size at least the group's row
            # count keeps pyarrow from splitting it further.
            writer.write_table(
                group.replace_schema_metadata(metadata),
                row_group_size=max(1, group.num_rows),
            )
    finally:
        writer.close()
        pf.close()
    os.replace(tmp_path, output_path)


def _levels_from_column(output_path: str, level_meta: list[dict]) -> list[dict]:
    """Derive each level's last row group from the written ``level`` column."""
    pf = pq.ParquetFile(output_path)
    ends: dict[int, int] = {}
    for rg in range(pf.metadata.num_row_groups):
        values = set(pf.read_row_group(rg, columns=["level"]).column("level").to_pylist())
        if len(values) != 1:
            raise RuntimeError(f"row group {rg} mixes levels {values}; cannot describe the file")
        ends[values.pop()] = rg
    return [dict(meta, row_group_end=ends[k]) for k, meta in enumerate(level_meta)]
