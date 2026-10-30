"""Small fixture builders shared by the #1367 guardrail tests (G7, G8, G9).

Everything here writes with ``pyarrow.parquet`` directly, so a fixture's shape
(a malformed ``geo`` block, a sidecar key, a ``geo`` key appended after the
writer opened) is exactly what the test says it is and owes nothing to the gpio
write path under test.
"""

from __future__ import annotations

import json
import struct
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

#: Sidecar file-level keys a rewrite is expected to carry through (#690, #1015).
SIDECAR_KEYS = {
    b"collection": b'{"id": "defrag-1367", "stac_version": "1.0.0"}',
    b"fiboa": b'{"fiboa_version": "0.2.0"}',
}
EPOCH = 2020.5


def point_wkb(x: float, y: float) -> bytes:
    return struct.pack("<BI2d", 1, 1, x, y)


def polygon_wkb(ring: list[tuple[float, float]]) -> bytes:
    """A single-ring polygon; ``ring`` is closed here if it is not already."""
    if ring[0] != ring[-1]:
        ring = [*ring, ring[0]]
    body = struct.pack("<BII", 1, 3, 1) + struct.pack("<I", len(ring))
    return body + b"".join(struct.pack("<2d", x, y) for x, y in ring)


def geo_block(
    version: str = "1.1.0",
    geometry_types: list[str] | None = None,
    **column_extra,
) -> dict:
    column = {"encoding": "WKB", "geometry_types": geometry_types or ["Point"], **column_extra}
    return {"version": version, "primary_column": "geometry", "columns": {"geometry": column}}


def write_with_geo(
    path: Path,
    table: pa.Table,
    geo: dict | list | str | bytes | None,
    extra_kv: dict[bytes, bytes] | None = None,
    **write_options,
) -> Path:
    """Write ``table`` with ``geo`` (any JSON shape, or raw bytes) as its ``geo`` key."""
    metadata = dict(table.schema.metadata or {})
    if geo is not None:
        metadata[b"geo"] = geo if isinstance(geo, bytes) else json.dumps(geo).encode("utf-8")
    metadata.update(extra_kv or {})
    pq.write_table(table.replace_schema_metadata(metadata), str(path), **write_options)
    return path


def footer_kv(path) -> dict[bytes, bytes]:
    """The raw footer key/value metadata (not the Arrow schema's view of it)."""
    return dict(pq.ParquetFile(str(path)).metadata.metadata or {})


def footer_geo(path) -> dict | None:
    raw = footer_kv(path).get(b"geo")
    return None if raw is None else json.loads(raw)


def points_table(n: int = 40, with_bbox: bool = False) -> pa.Table:
    """``n`` points on a diagonal in lon/lat, optionally with a bbox struct column."""
    xs = [-10.0 + i * 0.5 for i in range(n)]
    ys = [5.0 + i * 0.25 for i in range(n)]
    columns = {
        "id": pa.array(range(n), pa.int64()),
        "name": pa.array([f"p{i}" for i in range(n)]),
        "geometry": pa.array([point_wkb(x, y) for x, y in zip(xs, ys, strict=True)], pa.binary()),
    }
    if with_bbox:
        columns["bbox"] = pa.array(
            [{"xmin": x, "ymin": y, "xmax": x, "ymax": y} for x, y in zip(xs, ys, strict=True)],
            pa.struct([(k, pa.float64()) for k in ("xmin", "ymin", "xmax", "ymax")]),
        )
    return pa.table(columns)


def grid_points_table(n: int, with_bbox: bool = True) -> pa.Table:
    """``n`` distinct points on a deterministic grid over West Africa, lon/lat.

    Enough rows (pass more than ``DEFAULT_ROW_GROUP_ROWS``) make the row-group
    policy observable: one group for everything and the facade's 49,152 differ.
    """
    side = int(n**0.5) + 1
    xs = [-10.0 + (i % side) * (20.0 / side) for i in range(n)]
    ys = [0.0 + (i // side) * (20.0 / side) for i in range(n)]
    columns = {
        "id": pa.array(range(n), pa.int64()),
        "name": pa.array([f"feature-{i % 997}" for i in range(n)]),
        "value": pa.array([float(i % 113) for i in range(n)], pa.float64()),
        "geometry": pa.array([point_wkb(x, y) for x, y in zip(xs, ys, strict=True)], pa.binary()),
    }
    if with_bbox:
        columns["bbox"] = pa.array(
            [{"xmin": x, "ymin": y, "xmax": x, "ymax": y} for x, y in zip(xs, ys, strict=True)],
            pa.struct([(k, pa.float64()) for k in ("xmin", "ymin", "xmax", "ymax")]),
        )
    return pa.table(columns)
