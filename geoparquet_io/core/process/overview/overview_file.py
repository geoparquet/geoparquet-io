"""Assemble a level ladder into one overview GeoParquet (#1117).

`gpio process overview` produces one file per level. tylertoo's overview format
describes exactly that shape in a single file -- levels are additional **rows**
tagged by a ``level`` column rather than parallel columns -- and
``tylertoo export-pmtiles`` tiles such a file directly, reading the levels as
written. Emitting it here removes the per-level tippecanoe run and the
hand-pinned zoom bands from the caller.

The contract implemented is OVERVIEWS_SPEC (tylertoo, ``context/OVERVIEWS_SPEC.md``):
the row model (2.1), coarse-to-fine ordering with each level ending on a
row-group boundary (4.2), the ``level`` column and its agreement with the
footer (4.1), the ``geo:overviews`` footer key (3.2), no empty level (7.3) and
no dictionary encoding on the geometry (4.5). Not yet met: the 1.1 bbox
covering (4.4) and a spatial order within a level (5.1), which belong to the
rollup that produces the levels.

The write is one streaming pass. Every fact the footer needs -- each level's
row count, hence its last row group -- is in the level files' own metadata
before the first byte is written, so the footer goes into the writer's schema
up front and the file is never read back or rewritten. Levels are read in
row-group-sized slices, so memory is bounded by one row group, not by the
finest level. Every slice is cast to one schema first: the base level is the
user's input and the built levels are the funnel's output, and the two differ
when the input is GeoParquet 2.0 (native geometry) or carries a bbox column
the rollup drops.

This module also owns the Web-Mercator GSD math (5.2) so that ``pmtiles*`` does
not grow a second copy.
"""

from __future__ import annotations

import json
import math
import os
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.arrow_geo_metadata import _apply_geoparquet_metadata
from geoparquet_io.core.geo_metadata import (
    OVERVIEW_LEVEL_COLUMN,
    OVERVIEWS_KEY,
    carried_geometry_column,
    sanitized_carried_geo,
)
from geoparquet_io.core.logging_config import debug
from geoparquet_io.core.parquet_writer import (
    DEFAULT_ROW_GROUP_ROWS,
    apply_output_kv_metadata,
    resolve_output_geoparquet_version,
    resolve_row_group_rows,
)
from geoparquet_io.core.write_funnels import extract_preserved_kv_metadata

__all__ = [
    "OVERVIEWS_KEY",
    "OVERVIEWS_VERSION",
    "DEFAULT_CELL_DETAIL",
    "LevelInput",
    "gsd_for_zoom",
    "zoom_for_gsd",
    "write_overview_file",
]

#: Version of OVERVIEWS_SPEC the emitted footer conforms to.
OVERVIEWS_VERSION = "0.2.0"

#: Web Mercator equatorial circumference, metres (OVERVIEWS_SPEC 5.2).
EARTH_CIRCUMFERENCE_M = 40075016.69

#: The spec's GSD formula divides a zoom's extent by 1024 -- the cogp-rs
#: convention, ~4x a 256px tile so sub-pixel features drop (OVERVIEWS_SPEC
#: 5.2 and 11 Q6; tylertoo's ``--gsd-base`` default). ``zoom`` is reported
#: alongside the authoritative ``gsd`` with this base.
GSD_BASE = 1024.0

#: How many GSD units wide a cell is at the level that serves it: a level's
#: GSD is its cell width divided by this. Larger means a smaller GSD, so a
#: level is served at a finer zoom.
DEFAULT_CELL_DETAIL = 4.0

#: Metres per degree for a geographic CRS: flat, per OVERVIEWS_SPEC 7.1, which
#: overstates east-west cell widths away from the equator by 1/cos(lat).
METERS_PER_DEGREE = 111320.0


@dataclass(frozen=True)
class LevelInput:
    """One level of the ladder, coarse first. ``gsd`` is absolute metres."""

    path: str
    gsd: float


def gsd_for_zoom(zoom: int) -> float:
    """Ground sample distance in metres for a Web Mercator zoom (OVERVIEWS_SPEC 5.2)."""
    return EARTH_CIRCUMFERENCE_M / (GSD_BASE * 2.0**zoom)


def zoom_for_gsd(gsd: float) -> int:
    """Invert :func:`gsd_for_zoom`. ``gsd`` must be positive and finite."""
    if not (gsd > 0 and math.isfinite(gsd)):
        raise ValueError(f"gsd must be positive metres, got {gsd}")
    return max(0, round(math.log2(EARTH_CIRCUMFERENCE_M / GSD_BASE / gsd)))


def _zoom_ladder(gsds: list[float]) -> list[int] | None:
    """Zooms for a GSD ladder, or ``None`` when two levels round to one zoom.

    ``zoom`` is OPTIONAL and must be strictly increasing when present
    (OVERVIEWS_SPEC 3.3), and tylertoo serves each level at exactly its
    ``zoom`` when the field is there. Nudging a tied zoom up would move a
    level to a zoom its ``gsd`` does not denote, so ties leave the field out
    and the consumer derives zooms from ``gsd`` itself.
    """
    zooms = [zoom_for_gsd(g) for g in gsds]
    if any(zooms[i] <= zooms[i - 1] for i in range(1, len(zooms))):
        return None
    return zooms


def _validate(levels: list[LevelInput], output_path: str) -> None:
    if not levels:
        raise ValueError("an overview file needs at least one level; got an empty ladder")
    gsds = [lv.gsd for lv in levels]
    if any(not (g > 0 and math.isfinite(g)) for g in gsds):
        raise ValueError(f"gsd must be positive metres, got {gsds}")
    if any(gsds[i] >= gsds[i - 1] for i in range(1, len(gsds))):
        raise ValueError(
            f"gsd must be strictly decreasing coarse->fine (OVERVIEWS_SPEC 3.3), got {gsds}"
        )
    for lv in levels:
        if os.path.exists(output_path) and os.path.samefile(lv.path, output_path):
            raise ValueError(f"the output {output_path} is also a level of the ladder")


def _check_level_schemas(files: list[pq.ParquetFile], levels: list[LevelInput]) -> None:
    """Every level non-empty (7.3) and free of a column named ``level`` (4.1).

    SQL engines resolve identifiers case-insensitively, so a case-colliding
    source column would silently win in a reader's ``WHERE level = k``.
    """
    for pf, lv in zip(files, levels, strict=True):
        if pf.metadata.num_rows == 0:
            raise ValueError(
                f"{lv.path} has no rows; an overview file must not carry an empty level "
                "(OVERVIEWS_SPEC 7.3)"
            )
        collisions = [n for n in pf.schema_arrow.names if n.lower() == OVERVIEW_LEVEL_COLUMN]
        if collisions:
            raise ValueError(
                f"{lv.path} already has a column named {collisions[0]!r}; an overview file's "
                f"`{OVERVIEW_LEVEL_COLUMN}` column would shadow it in reader predicates "
                "(OVERVIEWS_SPEC 4.1). Rename the source column first."
            )


def _empty(schema: pa.Schema) -> pa.Table:
    """A zero-row table with one (empty) chunk per column: geoarrow aborts the
    process on a chunked array with no chunks, which ``from_batches([])`` makes."""
    return pa.Table.from_pydict({name: [] for name in schema.names}, schema=schema)


def _cast_plan(files: list[pq.ParquetFile], levels: list[LevelInput]) -> pa.Schema:
    """The one schema every level is cast to: the coarsest level's columns.

    The rollup keeps exactly the columns it can roll up, so the coarsest level
    has the intersection; a base column the rollup dropped (a bbox struct, a
    pivot column with no bucket) is projected away. The cast is proven on an
    empty slice of each level here so a mismatch names the level and the
    column before anything is written.
    """
    target = pa.schema([files[0].schema_arrow.field(n) for n in files[0].schema_arrow.names])
    for pf, lv in zip(files, levels, strict=True):
        missing = set(target.names) - set(pf.schema_arrow.names)
        if missing:
            raise ValueError(
                f"{lv.path} lacks column(s) {sorted(missing)} present in the coarsest level"
            )
        try:
            _empty(pf.schema_arrow).select(target.names).cast(target)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
            raise ValueError(
                f"{lv.path}: columns cannot be cast to the coarsest level's types: {exc}"
            ) from exc
    return target


def _union_geo(
    files: list[pq.ParquetFile], geometry_column: str
) -> tuple[list[float] | None, list[str]]:
    """The bbox and geometry types over every level, from each level's own ``geo``.

    Level 0's block alone describes level 0: H3 children can poke a few metres
    outside their parent, and an admin level's country cut at the antimeridian
    is a MultiPolygon its regions are not (OVERVIEWS_SPEC 7.5).
    """
    bbox: list[float] | None = None
    types: set[str] = set()
    for pf in files:
        col = (
            sanitized_carried_geo(pf.schema_arrow.metadata)
            .get("columns", {})
            .get(geometry_column, {})
        )
        types.update(col.get("geometry_types") or [])
        b = col.get("bbox")
        if isinstance(b, list) and len(b) == 4 and all(isinstance(v, int | float) for v in b):
            bbox = (
                [float(v) for v in b]
                if bbox is None
                else [
                    min(bbox[0], b[0]),
                    min(bbox[1], b[1]),
                    max(bbox[2], b[2]),
                    max(bbox[3], b[3]),
                ]
            )
    return bbox, sorted(types)


def _row_group_ends(counts: list[int], rows_per_group: int) -> list[int]:
    ends, end = [], -1
    for n in counts:
        end += math.ceil(n / rows_per_group)
        ends.append(end)
    return ends


def _footer(levels: list[LevelInput], ends: list[int]) -> dict:
    zooms = _zoom_ladder([lv.gsd for lv in levels])
    entries = [
        {"row_group_end": end, "gsd": lv.gsd, **({"zoom": zooms[i]} if zooms else {})}
        for i, (lv, end) in enumerate(zip(levels, ends, strict=True))
    ]
    return {
        "version": OVERVIEWS_VERSION,
        "levels": entries,
        "mode": "duplicating",
        # 3.4: duplicating MUST name the finest level canonical. No `cogp` key
        # is emitted -- 3.1 says writers SHOULD omit it in this mode.
        "canonical_level": len(levels) - 1,
    }


def _output_schema(
    files: list[pq.ParquetFile],
    base_path: str,
    data_schema: pa.Schema,
    footer: dict,
    geoparquet_version: str | None,
    verbose: bool,
) -> pa.Schema:
    """The written schema: ``level`` appended, the ``geo`` block and footer decided by the facade.

    The base (finest) level is the user's file, so its CRS and sidecar keys
    (fiboa, STAC) are the ones carried. The facade decides the output version
    from that file, writes the geometry type that version calls for, and
    drops the ``geo`` key for ``parquet-geo-only``; bbox and geometry types
    are the union over the levels rather than one level's.
    """
    base = files[-1]
    base_meta = base.schema_arrow.metadata or {}
    version = resolve_output_geoparquet_version(
        geoparquet_version, input_file=base_path, verbose=verbose
    )
    level_field = pa.field(OVERVIEW_LEVEL_COLUMN, pa.int32(), nullable=False)
    empty = _empty(data_schema.append(level_field))

    geo_meta = sanitized_carried_geo(base_meta)
    geometry_column = carried_geometry_column(geo_meta, data_schema.names)
    if geometry_column is not None:
        col_meta = geo_meta.get("columns", {}).get(geometry_column, {})
        bbox, types = _union_geo(files, geometry_column)
        empty = _apply_geoparquet_metadata(
            empty,
            geometry_column=geometry_column,
            geoparquet_version=version,
            original_metadata=base_meta,
            input_crs=col_meta.get("crs") if isinstance(col_meta, dict) else None,
            verbose=verbose,
            geo_bbox=bbox,
        )
        meta = dict(empty.schema.metadata or {})
        if b"geo" in meta and types:
            geo = json.loads(meta[b"geo"])
            geo["columns"][geometry_column]["geometry_types"] = types
            empty = empty.replace_schema_metadata({**meta, b"geo": json.dumps(geo).encode()})
    else:
        empty = empty.replace_schema_metadata(base_meta)

    extras = extract_preserved_kv_metadata(base_meta)
    extras[OVERVIEWS_KEY] = json.dumps(footer)
    return apply_output_kv_metadata(empty, version, extra_kv_metadata=extras).schema


def _slices(pf: pq.ParquetFile, rows: int) -> Iterator[pa.Table]:
    """The file in batches of at most ``rows``, one row group each on write.

    pyarrow fills each batch across the source's own row groups, so every
    batch but the last is full and one ``write_table`` call per batch yields
    exactly ``ceil(n / rows)`` groups -- the count the footer was computed
    from. :func:`write_overview_file` verifies that against the written file
    rather than trusting it.
    """
    for batch in pf.iter_batches(batch_size=rows):
        yield pa.Table.from_batches([batch])


def _write_levels(
    writer: pq.ParquetWriter,
    files: list[pq.ParquetFile],
    data_schema: pa.Schema,
    schema: pa.Schema,
    rows_per_group: int,
) -> None:
    level_field = schema.field(OVERVIEW_LEVEL_COLUMN)
    for ordinal, pf in enumerate(files):
        groups = 0
        for piece in _slices(pf, rows_per_group):
            piece = piece.select(data_schema.names).cast(data_schema)
            level = pa.repeat(pa.scalar(ordinal, pa.int32()), piece.num_rows)
            piece = piece.append_column(level_field, level).replace_schema_metadata(schema.metadata)
            writer.write_table(piece, row_group_size=rows_per_group)
            groups += 1
        debug(f"level {ordinal}: {pf.metadata.num_rows} rows, {groups} row group(s)")


def write_overview_file(
    levels: list[LevelInput],
    output_path: str,
    *,
    geoparquet_version: str | None = None,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    verbose: bool = False,
) -> str:
    """Write ``levels`` (coarse first) as one overview GeoParquet.

    Each level is written in its own ``write_table`` calls so it ends exactly
    on a row-group boundary, which is what lets the footer address levels by
    row-group range and a reader fetch one level band without touching the
    others (OVERVIEWS_SPEC 4.2). The file is built under a temporary name in
    the output directory and moved into place, so a failure leaves nothing at
    ``output_path``. Returns the output path.
    """
    _validate(levels, output_path)
    files = [pq.ParquetFile(lv.path) for lv in levels]
    try:
        _check_level_schemas(files, levels)
        data_schema = _cast_plan(files, levels)
        rows_per_group = resolve_row_group_rows(None, None) or DEFAULT_ROW_GROUP_ROWS
        ends = _row_group_ends([pf.metadata.num_rows for pf in files], rows_per_group)
        schema = _output_schema(
            files, levels[-1].path, data_schema, _footer(levels, ends), geoparquet_version, verbose
        )
        data_schema = pa.schema(
            [schema.field(n) for n in data_schema.names]
        )  # the facade may have retyped the geometry for the output version

        fd, tmp_path = tempfile.mkstemp(
            prefix=".gpio-overview-",
            suffix=".parquet",
            dir=os.path.dirname(os.path.abspath(output_path)),
        )
        os.close(fd)
        try:
            geometry_column = carried_geometry_column(
                sanitized_carried_geo(schema.metadata), schema.names
            )
            with pq.ParquetWriter(
                tmp_path,
                schema,
                compression=compression,
                compression_level=compression_level,
                # OVERVIEWS_SPEC 4.5: no dictionary encoding on the geometry.
                use_dictionary=[n for n in schema.names if n != geometry_column],
            ) as writer:
                _write_levels(writer, files, data_schema, schema, rows_per_group)
            written = pq.read_metadata(tmp_path).num_row_groups
            if written != ends[-1] + 1:
                raise RuntimeError(
                    f"wrote {written} row groups but the footer describes {ends[-1] + 1}; "
                    "the level boundaries would be wrong, so nothing was kept"
                )
            os.replace(tmp_path, output_path)
        except BaseException:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise
    finally:
        for pf in files:
            pf.close()
    return output_path
