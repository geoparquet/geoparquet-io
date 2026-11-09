"""A4 — write entry x strategy x two-geometry shape, against the INPUT's declaration.

The secondary-geometry / covering / CRS bugs are the longest recurrence chain
in the repo (#1000 -> #1175 -> #1214, #953 -> #1171 -> #1172 -> #1212, then
#1400: a secondary's CRS lost on every strategy after the owner refactor). Each
fix came with a test for the one path it fixed, so the next path regressed
unseen. This matrix runs every write entry and strategy over every
two-geometry shape and asserts what lands on disk against what the input
*declared* — not path-vs-path agreement, which two equally wrong paths pass.

Kept apart from ``tests/test_write_contract.py`` because that suite's shapes
are single-geometry and its oracle is a fixed ``Expect``; this one needs its
own two-column builders and a declared-vs-written comparison. It reuses that
suite's adjudication, and its divergences live in the same
``KNOWN_STRATEGY_DIVERGENCES`` table, keyed ``(entry, "<shape>@<version>")``.

Axes:

* **entry** — the query funnel (``write_parquet_with_metadata``) per strategy;
  the same with ``rebuild_geo_metadata=True`` (what ``gpio convert`` passes);
  ``write_geoparquet_table`` per strategy; and ``write_geoparquet_table`` with
  ``write_strategy=None``, its separate ``_apply_geoparquet_metadata`` route.
  ``Table.write`` stays in A1: it resolves its own CRS and covering first.
* **shape** — {primary first, secondary first} x {WKB, native GEOMETRY}; the
  #1400 grid, a primary declaring ``crs: null`` plus ``orientation`` or
  ``epoch`` beside a projected (EPSG:3857) or unknown-CRS secondary; a
  secondary with its own bbox struct, declared under the exact name ``bbox`` or
  undeclared as ``other_bbox`` (#953); spherical ``edges`` on the primary.
* **version** — 1.1 and 2.0.

Pruned: ``rebuild_geo_metadata`` on in-memory, streaming and disk-rewrite runs
one shape, not all of them. ``convert``, its only caller, always writes through
duckdb-kv, so the full shape axis runs there; the other three keep one cell
each per version, which is enough to hold their recorded divergence.
"""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass, field

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.crs_utils import crs_equal, crs_from_column_meta
from geoparquet_io.core.duckdb_metadata import get_kv_metadata
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
from geoparquet_io.core.parquet_footer import _patch_footer_file
from geoparquet_io.core.validate import CheckStatus, validate_geoparquet
from geoparquet_io.core.write_funnels import write_geoparquet_table, write_parquet_with_metadata
from tests.test_write_contract import (
    KNOWN_STRATEGY_DIVERGENCES,
    POINTS,
    STRATEGIES,
    _adjudicate,
    _geo_metadata,
    _write_via_query,
    _xfail_if_expected,
)

PRIMARY, SECONDARY = "geometry", "other"
#: The secondary's points, in EPSG:3857 metres.
PROJECTED = [(x * 1e5, y * 1e5) for x, y in POINTS]
VERSIONS = {"1.1": "1.1.0", "2.0": "2.0.0"}
AXES = ("xmin", "ymin", "xmax", "ymax")


def _projjson(epsg: int) -> dict:
    from pyproj import CRS

    return CRS.from_epsg(epsg).to_json_dict()


def _wkb(points) -> list[bytes]:
    return [struct.pack("<BI2d", 1, 1, x, y) for x, y in points]


def _bbox(points) -> list[float]:
    xs, ys = zip(*points, strict=True)
    return [min(xs), min(ys), max(xs), max(ys)]


@dataclass(frozen=True)
class Shape:
    secondary_first: bool = False
    #: native Parquet GEOMETRY columns under a 2.0 block, instead of WKB under 1.1
    native: bool = False
    #: keys merged into the primary's declared entry
    primary_extra: dict = field(default_factory=dict)
    #: the secondary's declared CRS as an EPSG code; None declares `crs: null`
    secondary_epsg: int | None = 3857
    #: name of a bbox struct holding the SECONDARY's extents
    secondary_bbox: str | None = None
    #: whether the secondary's entry declares that struct as its covering
    declare_secondary_covering: bool = False


ORIENTATION = {"crs": None, "orientation": "counterclockwise"}
EPOCH = {"crs": None, "epoch": 2020.0}

SHAPES = {
    "wkb_primary_first": Shape(),
    "wkb_secondary_first": Shape(secondary_first=True),
    "native_primary_first": Shape(native=True),
    "native_secondary_first": Shape(native=True, secondary_first=True),
    # #1400: a declaration on the primary sends the write down the carried-block
    # route, which composed a stripped block and lost the secondary's CRS.
    "orientation_projected_secondary": Shape(primary_extra=ORIENTATION),
    "orientation_unknown_secondary": Shape(primary_extra=ORIENTATION, secondary_epsg=None),
    "epoch_projected_secondary": Shape(primary_extra=EPOCH),
    "epoch_unknown_secondary": Shape(primary_extra=EPOCH, secondary_epsg=None),
    # #953: the secondary's envelope must never become the primary's covering,
    # neither through the exact conventional name nor a `*_bbox` suffix match.
    "secondary_declares_bbox": Shape(secondary_bbox="bbox", declare_secondary_covering=True),
    "secondary_bbox_undeclared": Shape(secondary_bbox="other_bbox"),
    "spherical_primary": Shape(primary_extra={"edges": "spherical"}),
}


def _declared(shape: Shape) -> dict:
    primary = {
        "encoding": "WKB",
        "geometry_types": ["Point"],
        "bbox": _bbox(POINTS),
        **shape.primary_extra,
    }
    secondary = {
        "encoding": "WKB",
        "geometry_types": ["Point"],
        "bbox": _bbox(PROJECTED),
        "crs": _projjson(shape.secondary_epsg) if shape.secondary_epsg else None,
    }
    if shape.declare_secondary_covering:
        secondary["covering"] = {"bbox": {ax: [shape.secondary_bbox, ax] for ax in AXES}}
    return {
        "version": "2.0.0" if shape.native else "1.1.0",
        "primary_column": PRIMARY,
        "columns": {PRIMARY: primary, SECONDARY: secondary},
    }


def _geometry_array(shape: Shape, points, crs: dict | None) -> pa.Array:
    storage = pa.array(_wkb(points), type=pa.binary())
    if not shape.native:
        return storage
    import geoarrow.pyarrow as ga

    geo_type = ga.wkb().with_crs(json.dumps(crs)) if crs else ga.wkb()
    return pa.ExtensionArray.from_storage(geo_type, storage)


def _build_source(path, shape: Shape) -> str:
    declared = _declared(shape)
    geometries = {
        PRIMARY: _geometry_array(shape, POINTS, None),
        SECONDARY: _geometry_array(shape, PROJECTED, declared["columns"][SECONDARY]["crs"]),
    }
    order = [SECONDARY, PRIMARY] if shape.secondary_first else [PRIMARY, SECONDARY]
    columns = {"id": pa.array(range(len(POINTS)), type=pa.int64())}
    columns.update({name: geometries[name] for name in order})
    if shape.secondary_bbox:
        columns[shape.secondary_bbox] = pa.array(
            [dict(zip(AXES, (x, y, x, y), strict=True)) for x, y in PROJECTED],
            type=pa.struct([(ax, pa.float64()) for ax in AXES]),
        )
    table = pa.table(columns).replace_schema_metadata({b"geo": json.dumps(declared).encode()})
    # No ARROW:schema: `geo` is the only kv key, as in a DuckDB-written file, so a
    # rebuild's stripped metadata can come out empty.
    pq.write_table(table, str(path))
    _patch_footer_file(str(path), {"ARROW:schema": None})
    return str(path)


@pytest.fixture(scope="module")
def two_geometry_sources(tmp_path_factory) -> dict[str, str]:
    tmp = tmp_path_factory.mktemp("write_contract_two_geometry")
    return {name: _build_source(tmp / f"{name}.parquet", shape) for name, shape in SHAPES.items()}


# ---------------------------------------------------------------------------
# Entries
# ---------------------------------------------------------------------------


def _via_query(strategy: str, rebuild: bool = False):
    def write(source: str, out: str, version: str) -> None:
        _write_via_query(
            source, out, version, write_strategy=strategy, rebuild_geo_metadata=rebuild
        )

    return write


def _via_table(strategy: str | None):
    def write(source: str, out: str, version: str) -> None:
        write_geoparquet_table(
            pq.read_table(source), out, geoparquet_version=version, write_strategy=strategy
        )

    return write


ENTRIES = {
    **{f"query[{s}]": _via_query(s) for s in STRATEGIES},
    **{f"query_rebuild[{s}]": _via_query(s, rebuild=True) for s in STRATEGIES},
    **{f"table[{s}]": _via_table(s) for s in STRATEGIES},
    "table[None]": _via_table(None),
}


def _runs(entry: str, shape: str) -> bool:
    """False for the pruned cells (see the module docstring)."""
    rebuild_off_default = entry.startswith("query_rebuild[") and "duckdb-kv" not in entry
    return not rebuild_off_default or shape == "wkb_primary_first"


CASES = [
    (entry, shape, version)
    for entry in ENTRIES
    for shape in SHAPES
    for version in VERSIONS
    if _runs(entry, shape)
]


# ---------------------------------------------------------------------------
# The declared-vs-written assertion
# ---------------------------------------------------------------------------


def _covering_refs(column_meta: dict) -> set[str]:
    bbox = (column_meta.get("covering") or {}).get("bbox") or {}
    return {ref[0] for ref in bbox.values() if isinstance(ref, list) and ref}


def _native_columns(path: str) -> set[str]:
    """Columns typed as a native Parquet GEOMETRY/GEOGRAPHY, read off the schema."""
    with pq.ParquetFile(path) as reader:
        schema = reader.schema
    return {
        schema.column(i).name
        for i in range(len(schema))
        if json.loads(schema.column(i).logical_type.to_json()).get("Type")
        in ("Geometry", "Geography")
    }


def _assert_column(name: str, got: dict, want: dict) -> None:
    assert crs_equal(crs_from_column_meta(got), crs_from_column_meta(want)), (
        f"{name}: crs {got.get('crs', '<absent>')!r} does not match the declared "
        f"{want.get('crs', '<absent>')!r}"
    )
    assert got.get("edges", "planar") == want.get("edges", "planar"), (
        f"{name}: edges {got.get('edges')!r}, declared {want.get('edges')!r}"
    )
    assert got.get("geometry_types") == want["geometry_types"], (
        f"{name}: geometry_types {got.get('geometry_types')!r}, declared {want['geometry_types']!r}"
    )
    for key in ("orientation", "epoch"):
        assert got.get(key) == want.get(key), (
            f"{name}: {key} {got.get(key)!r}, declared {want.get(key)!r}"
        )
    assert _covering_refs(got) == _covering_refs(want), (
        f"{name}: covering names {sorted(_covering_refs(got))}, "
        f"declared {sorted(_covering_refs(want))}"
    )


def assert_matches_declaration(path: str, declared: dict, version: str) -> list[str]:
    """Assert the written block keeps what the input declared; return oracle failures."""
    # Every source carries ``id`` = 0..n-1 (``_build_source``): rows kept, in order.
    written_ids = pq.read_table(path, columns=["id"])["id"].to_pylist()
    assert written_ids == list(range(len(POINTS))), f"id sequence {written_ids}"
    geo = _geo_metadata(path)
    assert geo is not None, "write produced no 'geo' metadata key"
    assert geo.get("version") == VERSIONS[version], f"geo version {geo.get('version')!r}"
    assert geo.get("primary_column") == declared["primary_column"], (
        f"primary_column {geo.get('primary_column')!r}, declared {declared['primary_column']!r}"
    )
    written = geo.get("columns", {})
    assert set(written) == set(declared["columns"]), (
        f"geo.columns {sorted(written)}, declared {sorted(declared['columns'])}"
    )
    for name, want in declared["columns"].items():
        _assert_column(name, written[name], want)
    # The carrier: 2.0 types every geometry column natively, 1.x none of them.
    native = _native_columns(path)
    expected_native = set(declared["columns"]) if version == "2.0" else set()
    assert native == expected_native, (
        f"native geo logical types on {sorted(native)}, expected {sorted(expected_native)}"
    )

    result = validate_geoparquet(path, validate_data=True, sample_size=0)
    return [c.name for c in result.checks if c.status == CheckStatus.FAILED]


@pytest.mark.parametrize(("entry", "shape", "version"), CASES)
def test_a4_two_geometry_contract(entry, shape, version, two_geometry_sources, tmp_path):
    """Every entry keeps every column's declared CRS, covering, edges and types."""
    out = str(tmp_path / "out.parquet")
    divergence = KNOWN_STRATEGY_DIVERGENCES.get((entry, f"{shape}@{version}"))
    try:
        ENTRIES[entry](two_geometry_sources[shape], out, version)
        failed = assert_matches_declaration(out, _declared(SHAPES[shape]), version)
    except AssertionError as exc:
        _xfail_if_expected(exc, divergence)
        raise  # unreachable; _xfail_if_expected always raises or xfails
    _adjudicate(failed, divergence)


@pytest.mark.parametrize("version", VERSIONS)
def test_rebuild_does_not_declare_a_projected_away_secondary(
    version, two_geometry_sources, tmp_path
):
    """A rebuild restores a secondary's declared CRS only if the query emits it."""
    source = two_geometry_sources["wkb_primary_first"]
    out = str(tmp_path / "out.parquet")
    con = get_duckdb_connection()
    try:
        write_parquet_with_metadata(
            con,
            f"SELECT id, {PRIMARY} FROM read_parquet({sql_path(source)})",
            out,
            original_metadata=get_kv_metadata(source),
            geoparquet_version=version,
            input_file=source,
            rebuild_geo_metadata=True,
        )
    finally:
        con.close()
    assert set(_geo_metadata(out)["columns"]) == {PRIMARY}
    result = validate_geoparquet(out, validate_data=True, sample_size=0)
    assert [c.name for c in result.checks if c.status == CheckStatus.FAILED] == []
