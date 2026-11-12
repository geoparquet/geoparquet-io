"""G8: derived ``geo`` metadata is rebuilt from the data, not patched in place (#1367).

``geo.bbox``, ``geometry_types``, the bbox ``covering`` and ``edges`` describe
the rows of the file they sit in. A rewrite that changes the rows has to state
them for the rows it wrote; carrying the input's block through with fields
overwritten is how #812, #1200, #1225 and #966 happened. Each case here starts
from a written file, changes the data through a real CLI rewrite, and compares
the output's geo block, field by field, with a *fresh* build: the output's own
rows (schema metadata stripped) written by ``gpio.Table(...).write``.

The ``edges`` case cannot be rebuilt from stripped rows, since edges are a
declaration and not a property of the coordinates; the fresh build gets the
declaration back as GeoArrow extension metadata on the geometry column, the
way the API carries it.

All cells must describe the written rows, including footer-patched files.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from tests._defrag_fixtures import footer_geo, geo_block, point_wkb, polygon_wkb, write_with_geo

N_POINTS = 30
#: The polygon holds the extreme coordinates, so dropping it must shrink the bbox.
POLYGON_RING = [(100.0, 50.0), (120.0, 50.0), (120.0, 60.0), (100.0, 60.0)]


def _mixed_table() -> pa.Table:
    """``N_POINTS`` points in [-10, 4.5] x [5, 12.25], then one far-away polygon."""
    wkbs = [point_wkb(-10.0 + i * 0.5, 5.0 + i * 0.25) for i in range(N_POINTS)]
    wkbs.append(polygon_wkb(POLYGON_RING))
    return pa.table(
        {
            "id": pa.array(range(N_POINTS + 1), pa.int64()),
            "geometry": pa.array(wkbs, pa.binary()),
        }
    )


def _invoke(*args) -> None:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, f"{args}: {result.output[-1500:]} {result.exception!r}"


def _fresh_geo(path: Path, tmp: Path, edges: str | None = None) -> dict:
    """The geo block ``Table.write`` gives the rows of ``path``, metadata stripped."""
    table = pq.read_table(str(path)).replace_schema_metadata(None)
    if edges is not None:
        import geoarrow.pyarrow as ga

        index = table.schema.get_field_index("geometry")
        storage = table.column(index)
        if hasattr(storage.type, "storage_type"):
            storage = pa.chunked_array(
                [c.storage for c in storage.chunks], storage.type.storage_type
            )
        wkb_type = ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL)
        geometry = pa.chunked_array(
            [pa.ExtensionArray.from_storage(wkb_type, c) for c in storage.chunks], wkb_type
        )
        table = table.set_column(index, "geometry", geometry)
    fresh = tmp / f"fresh_{path.stem}.parquet"
    gpio.Table(table, geometry_column="geometry").write(str(fresh), geoparquet_version="1.1")
    return footer_geo(fresh)


def _field(geo: dict, field: str):
    column = geo["columns"][geo["primary_column"]]
    value = column.get(field)
    if field == "geometry_types" and value is not None:
        return sorted(value)
    if field == "bbox" and value is not None:
        return [round(v, 9) for v in value]
    if field == "edges" and value == "planar":
        return None  # planar is the default; absent and explicit are the same claim
    return value


# ---------------------------------------------------------------------------
# The rewrite paths
# ---------------------------------------------------------------------------


def _extract_where(tmp: Path) -> tuple[Path, str | None]:
    """Drop the polygon, which held the extreme coordinates and the only Polygon type."""
    src = write_with_geo(
        tmp / "mixed.parquet",
        _mixed_table(),
        geo_block(geometry_types=["Point", "Polygon"], bbox=[-10.0, 5.0, 120.0, 60.0]),
    )
    out = tmp / "extract_where.parquet"
    _invoke("extract", src, out, "--where", f"id < {N_POINTS}")
    assert pq.read_metadata(str(out)).num_rows == N_POINTS
    return out, None


def _sort_hilbert(tmp: Path) -> tuple[Path, str | None]:
    src = write_with_geo(
        tmp / "mixed.parquet",
        _mixed_table(),
        geo_block(geometry_types=["Point", "Polygon"], bbox=[-10.0, 5.0, 120.0, 60.0]),
    )
    out = tmp / "sort_hilbert.parquet"
    _invoke("sort", "hilbert", src, out)
    return out, None


def _add_bbox_then_exclude(tmp: Path) -> tuple[Path, str | None]:
    """``add bbox`` declares a covering; ``extract --exclude bbox`` removes its column."""
    src = write_with_geo(
        tmp / "mixed.parquet", _mixed_table(), geo_block(geometry_types=["Point", "Polygon"])
    )
    with_bbox = tmp / "with_bbox.parquet"
    _invoke("add", "bbox", src, with_bbox)
    assert "covering" in footer_geo(with_bbox)["columns"]["geometry"], "add bbox wrote no covering"
    out = tmp / "excluded.parquet"
    _invoke("extract", with_bbox, out, "--exclude-cols", "bbox")
    assert "bbox" not in pq.read_schema(str(out)).names
    return out, None


def _extract_where_spherical(tmp: Path) -> tuple[Path, str | None]:
    """Points only, so the edge model does not change any coordinate or bbox."""
    table = _mixed_table().slice(0, N_POINTS)
    src = write_with_geo(tmp / "spherical.parquet", table, geo_block(edges="spherical"))
    out = tmp / "extract_where_spherical.parquet"
    _invoke("extract", src, out, "--where", "id < 10")
    return out, "spherical"


#: A ``geo`` block whose derived fields no longer describe the rows: what a
#: file looks like after its data changed under a path that patched the block.
#: A rewrite that rebuilds from the data drops every stale value; one that
#: copies the input's block through carries them.
STALE_BBOX = [0.0, 0.0, 1.0, 1.0]


def _stale_source(tmp: Path) -> Path:
    stale = geo_block(geometry_types=["MultiLineString"], bbox=STALE_BBOX)
    return write_with_geo(tmp / "stale.parquet", _mixed_table(), stale)


def _stale_through(*command: str):
    def build(tmp: Path) -> tuple[Path, str | None]:
        src = _stale_source(tmp)
        out = tmp / "out.parquet"
        if command == ("add", "bbox-metadata"):
            src = write_with_geo(
                tmp / "stale_bbox.parquet",
                _mixed_table().append_column(
                    "bbox",
                    pa.array(
                        [_vertex_bbox(w) for w in _mixed_table().column("geometry").to_pylist()],
                        pa.struct([(k, pa.float64()) for k in ("xmin", "ymin", "xmax", "ymax")]),
                    ),
                ),
                geo_block(geometry_types=["MultiLineString"], bbox=STALE_BBOX),
            )
            _invoke(*command, src)
            return src, None
        _invoke(*command, src, out)
        return out, None

    return build


def _vertex_bbox(wkb: bytes) -> dict:
    import struct

    kind = struct.unpack_from("<I", wkb, 1)[0]
    if kind == 1:
        x, y = struct.unpack_from("<2d", wkb, 5)
        return {"xmin": x, "ymin": y, "xmax": x, "ymax": y}
    count = struct.unpack_from("<I", wkb, 9)[0]
    coords = [struct.unpack_from("<2d", wkb, 13 + 16 * i) for i in range(count)]
    xs, ys = [c[0] for c in coords], [c[1] for c in coords]
    return {"xmin": min(xs), "ymin": min(ys), "xmax": max(xs), "ymax": max(ys)}


PATHS = {
    "extract_where": _extract_where,
    "sort_hilbert": _sort_hilbert,
    "add_bbox_then_exclude": _add_bbox_then_exclude,
    "extract_where_spherical": _extract_where_spherical,
    "stale_convert_geoparquet": _stale_through("convert", "geoparquet"),
    "stale_sort_hilbert": _stale_through("sort", "hilbert"),
    "stale_extract": _stale_through("extract"),
    "stale_add_quadkey": _stale_through("add", "quadkey"),
    "stale_add_bbox": _stale_through("add", "bbox"),
    "stale_add_bbox_metadata": _stale_through("add", "bbox-metadata"),
}
FIELDS = ("bbox", "geometry_types", "covering", "edges")


class _Rewritten(dict):
    """(output block, fresh block) per path, built the first time a cell asks.

    A worker runs only some of the cells, so it builds only their paths instead
    of all ten.
    """

    def __init__(self, tmp_path_factory):
        super().__init__()
        self._factory = tmp_path_factory

    def __missing__(self, name: str) -> tuple[dict, dict]:
        tmp = self._factory.mktemp(name)
        out, edges = PATHS[name](tmp)
        self[name] = (footer_geo(out), _fresh_geo(out, tmp, edges))
        return self[name]


@pytest.fixture(scope="module")
def rewritten(tmp_path_factory) -> dict[str, tuple[dict, dict]]:
    return _Rewritten(tmp_path_factory)


@pytest.mark.parametrize("path", PATHS)
def test_derived_field_matches_a_fresh_build(path, rewritten):
    actual, fresh = rewritten[path]
    for field in FIELDS:
        assert _field(actual, field) == _field(fresh, field), (
            f"{path}: {field} is {_field(actual, field)!r}, a fresh build of the same rows says "
            f"{_field(fresh, field)!r}"
        )


def test_the_extract_case_really_changes_the_derived_state(tmp_path):
    """Non-vacuity: the dropped polygon carried the extreme x and the only Polygon."""
    out, _ = _extract_where(tmp_path)
    fresh = _fresh_geo(out, tmp_path)
    column = fresh["columns"]["geometry"]
    assert column["geometry_types"] == ["Point"]
    if "bbox" in column:
        assert column["bbox"][2] < 100.0


def test_file_statistics_use_complete_footers_and_scan_missing_chunks(tmp_path, monkeypatch):
    """Incomplete footer stats must not silently under-cover the file."""
    import geoparquet_io.core.geo_metadata as owner

    table = _mixed_table().append_column(
        "bbox",
        pa.array(
            [_vertex_bbox(w) for w in _mixed_table().column("geometry").to_pylist()],
            pa.struct([(k, pa.float64()) for k in ("xmin", "ymin", "xmax", "ymax")]),
        ),
    )
    expected = owner.geometry_stats_of(_mixed_table(), "geometry")
    original = owner._query_stats
    scans = []

    def scan(*args, **kwargs):
        scans.append(args)
        return original(*args, **kwargs)

    import duckdb

    executed = []
    execute = duckdb.DuckDBPyConnection.execute

    def record(self, query, *args, **kwargs):
        executed.append(str(query))
        return execute(self, query, *args, **kwargs)

    monkeypatch.setattr(duckdb.DuckDBPyConnection, "execute", record)
    monkeypatch.setattr(owner, "_query_stats", scan)
    for statistics in (True, False):
        path = tmp_path / f"stats-{statistics}.parquet"
        pq.write_table(table, path, row_group_size=10, write_statistics=statistics)
        scans.clear()
        actual = owner.geometry_stats_of(str(path), "geometry", bbox_column="bbox")
        assert actual.bbox == expected.bbox
        assert bool(scans) is (not statistics)
        executed.clear()
        assert owner.geometry_types_of(str(path), "geometry") == expected.geometry_types
        assert not any("MIN(__x0)" in query for query in executed), (
            "a types-only scan must not aggregate geometry envelopes"
        )

    import geoparquet_io.core.duckdb_metadata as footer_owner

    path = str(tmp_path / "stats-True.parquet")
    original_footer = footer_owner.get_row_group_bbox_stats

    def incomplete(*args, **kwargs):
        chunks, origin = original_footer(*args, **kwargs)
        return chunks[:-1], origin

    monkeypatch.setattr(footer_owner, "get_row_group_bbox_stats", incomplete)
    scans.clear()
    assert owner.geometry_stats_of(path, "geometry", bbox_column="bbox").bbox == expected.bbox
    assert scans, "one missing row group must trigger a scan, not a partial extent"


def test_native_fast_path_refreshes_carried_statistics_after_filtering(tmp_path, monkeypatch):
    """Keeping a covering/epoch must not carry facts about rows we dropped."""
    from geoparquet_io.core import write_funnels
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
    from geoparquet_io.core.geo_metadata import geometry_stats_of

    src = _stale_source(tmp_path)
    native = tmp_path / "native.parquet"
    _invoke("convert", "geoparquet", src, native, "--geoparquet-version", "2.0")
    metadata = pq.read_metadata(native).metadata
    import json

    block = json.loads(metadata[b"geo"])
    block["columns"]["geometry"]["epoch"] = 2020.0
    metadata[b"geo"] = json.dumps(block).encode()
    out = tmp_path / "filtered.parquet"
    copies = []
    original = write_funnels._plain_copy_to

    def copy(*args, **kwargs):
        copies.append(True)
        return original(*args, **kwargs)

    monkeypatch.setattr(write_funnels, "_plain_copy_to", copy)
    import geoparquet_io.core.geo_metadata as owner

    def no_scan(*args, **kwargs):
        pytest.fail("native output statistics should be read from its footer")

    monkeypatch.setattr(owner, "_query_stats", no_scan)
    con = get_duckdb_connection(load_spatial=True)
    try:
        write_funnels.write_parquet_with_metadata(
            con,
            f"SELECT * FROM read_parquet({sql_path(str(native))}) WHERE id < {N_POINTS}",
            str(out),
            original_metadata=metadata,
            input_file=str(native),
            geoparquet_version="2.0",
        )
    finally:
        con.close()
    assert copies, "the native fast path must stay enabled"
    col = footer_geo(out)["columns"]["geometry"]
    fresh = geometry_stats_of(_mixed_table().slice(0, N_POINTS), "geometry")
    assert col["epoch"] == 2020.0
    assert col["bbox"] == fresh.bbox
    assert col["geometry_types"] == fresh.geometry_types


def test_rewrite_evaluates_user_query_once_per_output_row(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
    from geoparquet_io.core.write_funnels import write_parquet_with_metadata

    source = _stale_source(tmp_path)
    counts = {}
    calls = []
    for strategy in ("duckdb-kv", "in-memory", "streaming", "disk-rewrite"):
        calls.clear()
        con = get_duckdb_connection(load_spatial=True, isolated=True)

        def counted(value: bytes) -> bytes:
            calls.append(value)
            return value

        try:
            con.create_function("counted_geometry", counted, ["BLOB"], "BLOB")
            query = f"SELECT id, ST_GeomFromWKB(counted_geometry(ST_AsWKB(geometry))) AS geometry FROM read_parquet({sql_path(str(source))})"
            out = tmp_path / f"once-{strategy}.parquet"
            write_parquet_with_metadata(
                con,
                query,
                str(out),
                input_file=str(source),
                original_metadata=pq.read_metadata(source).metadata,
                write_strategy=strategy,
                geoparquet_version="1.1",
            )
            counts[strategy] = len(calls)
        finally:
            con.remove_function("counted_geometry")
            con.close()
        assert (
            footer_geo(out)["columns"]["geometry"]["bbox"]
            == _fresh_geo(out, tmp_path)["columns"]["geometry"]["bbox"]
        )
    assert counts == dict.fromkeys(counts, N_POINTS + 1)
