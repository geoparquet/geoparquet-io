"""The ``edges`` field has one owner, ``core/edges.py`` (#1367 concept 6).

Three layers, each failing first (see the T3d record):

* the owner's own rules -- the vocabulary, the version gate, the one
  precedence between an explicit request, a carried ``geo`` block and a native
  type, and the strip/restore pair -- as tables;
* a cross-path table: one input per (declared edges, output version, CRS kind)
  written by ``convert geoparquet``, ``sort hilbert``, every ``Table.write``
  strategy and a stdout->stdin pipe, and the output's ``edges`` (block and
  Parquet logical type) must be the same on every path and equal what the
  owner's version gate says;
* the defects fixed at the owner (#1289's mixed-edges glob).
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core import edges as owner
from geoparquet_io.core.duckdb_metadata import get_schema_info, parse_geometry_logical_type
from geoparquet_io.core.exceptions import InvalidParameterError
from tests._defrag_fixtures import footer_geo, geo_block, polygon_wkb, write_with_geo

# ---------------------------------------------------------------------------
# The owner's rules
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("edges", "version", "expected"),
    [
        ("planar", "1.0.0", "planar"),
        ("planar", "1.1.0", "planar"),
        ("spherical", "1.0.0", "spherical"),
        ("spherical", "1.1.0", "spherical"),
        ("vincenty", "1.1.0", "spherical"),
        ("karney", "1.0.0", "spherical"),
        ("spherical", "2.0.0", "spherical"),
        ("andoyer", "2.0.0", "andoyer"),
        ("thomas", None, "thomas"),  # no block (parquet-geo-only): kept
        ("planar", "2.0.0", "planar"),
    ],
)
def test_the_version_gate(edges, version, expected):
    assert owner.edges_for_version(edges, version) == expected


@pytest.mark.parametrize("version", ["1.1.0", "2.0.0", None])
def test_the_version_gate_refuses_an_unknown_value_once(version):
    with pytest.raises(InvalidParameterError, match="Unknown edges 'spherikal'"):
        owner.edges_for_version("spherikal", version)


@pytest.mark.parametrize(
    ("native", "block", "explicit", "expected"),
    [
        ({"g": "spherical"}, {}, None, {"g": "spherical"}),
        ({"g": "spherical"}, {"g": "planar"}, None, {}),  # the block's planar wins
        ({"g": "spherical"}, {"g": "spherikal"}, None, {"g": "spherical"}),  # a typo says nothing
        ({}, {"g": "spherikal"}, None, {}),
        ({}, {"g": "vincenty"}, None, {"g": "vincenty"}),
        ({"g": "spherical"}, {"g": "karney"}, None, {"g": "karney"}),
        ({"g": "planar"}, {}, None, {}),  # a native planar is not a declaration
        ({"g": "unspecified"}, {}, None, {}),
        ({"g": "spherical"}, {}, "planar", {}),  # an explicit planar drops the primary
        ({}, {"g": "planar"}, "spherical", {"g": "spherical"}),  # the request wins
        ({"h": "spherical"}, {}, "vincenty", {"g": "vincenty", "h": "spherical"}),
    ],
)
def test_the_one_precedence(native, block, explicit, expected):
    assert owner.detect_edges(native, block, explicit, primary="g") == expected


def test_an_unknown_explicit_request_is_refused():
    with pytest.raises(InvalidParameterError, match="expected one of: planar, spherical"):
        owner.detect_edges({}, {}, "great-circle", primary="g")


def test_files_that_agree_give_one_answer():
    assert owner.agree_across_files({"a": {"g": "spherical"}, "b": {"g": "spherical"}}) == {
        "g": "spherical"
    }
    assert owner.agree_across_files({"a": {}, "b": {}}) == {}
    assert owner.agree_across_files({}) == {}


def test_files_that_disagree_are_refused_by_name():
    with pytest.raises(owner.EdgesConflictError, match="planar in a.*spherical in b"):
        owner.agree_across_files({"a": {}, "b": {"g": "spherical"}})


def test_strip_and_restore():
    geo = {
        "columns": {
            "g": {"encoding": "WKB", "edges": "spherical"},
            "h": {"encoding": "WKB", "edges": "planar"},
            "k": {"encoding": "WKB", "edges": "vincenty"},
        }
    }
    owner.strip_edges(geo, columns=["g", "h"])
    assert geo["columns"] == {
        "g": {"encoding": "WKB"},
        "h": {"encoding": "WKB", "edges": "planar"},  # planar is the truth already
        "k": {"encoding": "WKB", "edges": "vincenty"},  # not named: kept
    }
    assert owner.restore_edges(geo, {"g": "spherical", "absent": "spherical"}) is True
    assert geo["columns"]["g"] == {"encoding": "WKB", "edges": "spherical"}
    assert owner.restore_edges(geo, {"g": "spherical"}) is False
    assert owner.restore_edges(None, {"g": "spherical"}) is False


def test_the_key_readers():
    assert owner.edges_of({"edges": "spherical"}) == "spherical"
    assert owner.edges_of({"encoding": "WKB"}) is None
    assert owner.edges_of(["edges"]) is None
    assert owner.declares_edges({"edges": None}) is True
    assert owner.declares_edges("edges") is False
    assert owner.without_edges({"crs": 1, "edges": "spherical"}) == {"crs": 1}
    assert owner.block_edges({"columns": {"g": {"edges": "x"}, "h": {}}}) == {"g": "x"}
    assert owner.logical_type_edges({"geo_type": "Geography"}) == "spherical"
    assert owner.logical_type_edges({"algorithm": "karney"}) == "karney"


@pytest.mark.parametrize(
    ("wkt", "lon", "lat"),
    [
        ("LINESTRING(-30 60, 30 60)", 0.0, 63.43494882292201),  # #1293's arc
        ("LINESTRING(170 10, -170 10)", 180.0, 10.151081711048134),  # across the antimeridian
        ("POINT(5 6)", 5.0, 6.0),
        ("MULTIPOINT(0 0, 90 0)", 45.0, 0.0),
    ],
)
def test_the_representative_point_of_spherical_geometry(wkt, lon, lat):
    import duckdb

    con = duckdb.connect()
    con.execute("LOAD spatial")
    expr = owner.representative_point_sql("ST_GeomFromText(wkt)", "spherical")
    x, y = con.execute(
        f"SELECT ST_X(p), ST_Y(p) FROM (SELECT {expr} AS p FROM (SELECT ? AS wkt))", [wkt]
    ).fetchall()[0]
    assert abs(abs(x) - abs(lon)) < 1e-9 and abs(y - lat) < 1e-9


def test_the_representative_point_of_a_polar_cap_is_the_pole():
    import duckdb

    con = duckdb.connect()
    con.execute("LOAD spatial")
    expr = owner.representative_point_sql(
        "ST_GeomFromText('POLYGON((0 80, 90 80, 180 80, -90 80, 0 80))')", "spherical"
    )
    assert abs(con.execute(f"SELECT ST_Y({expr})").fetchall()[0][0] - 90.0) < 1e-9
    assert owner.representative_point_sql("geometry", "planar") == "ST_Centroid(geometry)"
    assert owner.representative_point_sql("geometry", None) == "ST_Centroid(geometry)"


# ---------------------------------------------------------------------------
# Every write path agrees
# ---------------------------------------------------------------------------

GEOGRAPHIC = {"id": {"authority": "EPSG", "code": 4326}}
PROJECTED = {"id": {"authority": "EPSG", "code": 3857}}
CRS_KINDS = {"geographic": GEOGRAPHIC, "projected": PROJECTED}
DECLARED = ["planar", "spherical", "vincenty"]
VERSIONS = ["1.0", "1.1", "2.0"]
RINGS = [
    [(10.0, 10.0), (20.0, 10.0), (20.0, 20.0), (10.0, 20.0)],
    [(30.0, 40.0), (35.0, 40.0), (35.0, 45.0), (30.0, 45.0)],
]


def _input(tmp_path, declared: str, crs_kind: str):
    table = pa.table(
        {"id": [1, 2], "geometry": pa.array([polygon_wkb(r) for r in RINGS], pa.binary())}
    )
    block = geo_block("1.1.0", ["Polygon"], crs=CRS_KINDS[crs_kind], edges=declared)
    return write_with_geo(tmp_path / f"in_{declared}_{crs_kind}.parquet", table, block)


def _edges_of_output(path) -> tuple[object, object]:
    """``(block edges, logical-type algorithm)`` of the output's geometry column."""
    block = footer_geo(path) or {}
    column = (block.get("columns") or {}).get("geometry") or {}
    logical = next(
        (c.get("logical_type") for c in get_schema_info(str(path)) if c["name"] == "geometry"),
        None,
    )
    parsed = parse_geometry_logical_type(logical or "") or {}
    native = parsed.get("algorithm") or parsed.get("geo_type")
    return column.get("edges"), native


def _cli(args, **kwargs):
    result = CliRunner().invoke(cli, args, catch_exceptions=False, **kwargs)
    assert result.exit_code == 0, result.output
    return result


def _write_convert(src, out, version):
    _cli(["convert", "geoparquet", str(src), str(out), "--geoparquet-version", version])


def _write_sort(src, out, version):
    _cli(["sort", "hilbert", str(src), str(out), "--geoparquet-version", version])


def _write_table(strategy):
    def write(src, out, version):
        gpio.read(str(src)).write(str(out), geoparquet_version=version, write_strategy=strategy)

    return write


def _write_pipe(src, out, version):
    first = CliRunner().invoke(cli, ["extract", "geoparquet", str(src), "-"])
    assert first.exit_code == 0, first.output
    _cli(
        ["extract", "geoparquet", "-", str(out), "--geoparquet-version", version],
        input=first.stdout_bytes,
    )


PATHS = {
    "convert": _write_convert,
    "sort-hilbert": _write_sort,
    "table-duckdb-kv": _write_table("duckdb-kv"),
    "table-in-memory": _write_table("in-memory"),
    "table-streaming": _write_table("streaming"),
    "table-disk-rewrite": _write_table("disk-rewrite"),
    "pipe": _write_pipe,
}


#: What the output block's ``edges`` must be, by declared edges and version: 1.x
#: can say only planar or spherical, so an ellipsoidal algorithm degrades to
#: spherical there (with a warning); 2.0 keeps the algorithm. Written out by
#: hand, not computed with the owner, so a fault in the owner shows here.
EXPECTED_BLOCK = {
    ("planar", "1.0"): "planar",
    ("planar", "1.1"): "planar",
    ("planar", "2.0"): "planar",
    ("spherical", "1.0"): "spherical",
    ("spherical", "1.1"): "spherical",
    ("spherical", "2.0"): "spherical",
    ("vincenty", "1.0"): "spherical",
    ("vincenty", "1.1"): "spherical",
    ("vincenty", "2.0"): "vincenty",
}

#: The paths that spell a planar declaration out (``"edges": "planar"``) where
#: the others leave the key out, which the spec reads the same. A disagreement
#: between copies that is not on #1367's approved list, so its bytes are kept
#: and it is pinned here rather than hidden by the normalisation below.
PLANAR_SPELLED_OUT = {"sort-hilbert", "pipe"}


def _expected(declared: str, version: str, crs_kind: str) -> tuple[str, object]:
    block = EXPECTED_BLOCK[(declared, version)]
    if version != "2.0":
        return block, None  # a 1.x column is plain WKB: no logical type
    if block != "planar" and crs_kind == "geographic":
        return block, block  # GEOGRAPHY(algorithm=...)
    return block, "Geometry"  # GEOGRAPHY needs a geographic CRS


@pytest.mark.parametrize("crs_kind", sorted(CRS_KINDS))
@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("declared", DECLARED)
def test_every_write_path_writes_the_same_edges(tmp_path, declared, version, crs_kind):
    src = _input(tmp_path, declared, crs_kind)
    seen, spelled_out = {}, set()
    for name, write in PATHS.items():
        out = tmp_path / f"{name}.parquet"
        write(src, out, version)
        block_edges, native = _edges_of_output(out)
        if block_edges == "planar":
            spelled_out.add(name)
        seen[name] = (block_edges or "planar", native)

    assert seen == dict.fromkeys(PATHS, _expected(declared, version, crs_kind))
    assert spelled_out == (PLANAR_SPELLED_OUT if declared == "planar" else set())


# ---------------------------------------------------------------------------
# #1289: a glob whose files disagree on edges
# ---------------------------------------------------------------------------

CAP = [(0.0, 80.0), (90.0, 80.0), (180.0, 80.0), (-90.0, 80.0)]
BOWTIE = [(0.0, 0.0), (10.0, 10.0), (10.0, 0.0), (0.0, 10.0)]


def _part(path, ring, edges):
    table = pa.table({"id": [1], "geometry": pa.array([polygon_wkb(ring)], pa.binary())})
    extra = {"edges": edges} if edges else {}
    return write_with_geo(path, table, geo_block("1.1.0", ["Polygon"], **extra))


@pytest.mark.parametrize("planar_first", [True, False], ids=["planar-first", "spherical-first"])
def test_1289_a_glob_whose_files_disagree_on_edges_is_refused(tmp_path, planar_first):
    """Edges were read off the first file only: a planar first file had the
    spherical cap repaired as planar, a spherical first file relabelled the
    planar bowtie spherical. One column cannot be both, so the read refuses
    and names the files, before anything is written."""
    parts = tmp_path / "parts"
    parts.mkdir()
    planar, spherical = ("a", "b") if planar_first else ("b", "a")
    _part(parts / f"{planar}_planar.parquet", BOWTIE, None)
    _part(parts / f"{spherical}_spherical.parquet", CAP, "spherical")
    out = tmp_path / "out.parquet"

    result = CliRunner().invoke(cli, ["extract", "geoparquet", str(parts / "*.parquet"), str(out)])

    assert result.exit_code != 0, result.output
    assert "edges" in result.output
    assert f"{planar}_planar.parquet" in result.output
    assert f"{spherical}_spherical.parquet" in result.output
    assert not out.exists()


def test_1289_a_glob_whose_files_agree_keeps_their_edges(tmp_path):
    parts = tmp_path / "parts"
    parts.mkdir()
    _part(parts / "a.parquet", CAP, "spherical")
    _part(parts / "b.parquet", CAP, "spherical")
    out = tmp_path / "out.parquet"

    _cli(["extract", "geoparquet", str(parts / "*.parquet"), str(out)])

    assert _edges_of_output(out)[0] == "spherical"


# ---------------------------------------------------------------------------
# #1293: a cell key of spherical geometry comes from a point on the sphere
# ---------------------------------------------------------------------------

#: The great-circle midpoint of LINESTRING(-30 60, 30 60) (#1293's repro). The
#: planar centroid is (0, 60).
ARC = [(-30.0, 60.0), (30.0, 60.0)]
ARC_MIDPOINT = (0.0, 63.43494882292201)

CELL_OPS = {
    "quadkey": (lambda t: t.add_quadkey(resolution=8), "quadkey"),
    "h3": (lambda t: t.add_h3(resolution=5), "h3_cell"),
    "s2": (lambda t: t.add_s2(level=10), "s2_cell"),
    "a5": (lambda t: t.add_a5(resolution=5), "a5_cell"),
}


def _line_table(coords, edges):
    import struct

    import geoarrow.pyarrow as ga
    import geoarrow.types as gt

    wkb = struct.pack("<BII", 1, 2, len(coords)) + b"".join(
        struct.pack("<2d", x, y) for x, y in coords
    )
    if len(coords) == 1:
        wkb = struct.pack("<BI2d", 1, 1, *coords[0])
    type_ = ga.wkb().with_edge_type(getattr(gt.EdgeType, edges.upper()))
    geometry = pa.ExtensionArray.from_storage(type_, pa.array([wkb], pa.binary()))
    return gpio.Table(pa.table({"geometry": geometry}), geometry_column="geometry")


def _cell(op, table):
    build, column = CELL_OPS[op]
    result = build(table).table
    name = column if column in result.column_names else result.column_names[-1]
    return result.column(name).to_pylist()


@pytest.mark.parametrize("op", sorted(CELL_OPS))
def test_1293_a_spherical_line_gets_the_cell_of_its_great_circle_midpoint(op):
    """The cell builders keyed every geometry from ``ST_Centroid``, a planar
    centroid ((0, 60) here). Non-planar edges now key from the owner's
    ``representative_point_sql``, a point on the sphere."""
    expected = _cell(op, _line_table([ARC_MIDPOINT], "planar"))
    assert _cell(op, _line_table(ARC, "spherical")) == expected


@pytest.mark.parametrize("lat", [90.0, -90.0, 89.9999999])
def test_1293_a_polar_point_keys_to_the_edge_row(lat):
    """A spherical polar cap's point is the pole, where mercantile raises; it
    keys to the edge row every latitude past Web Mercator's edge already does."""
    import mercantile

    from geoparquet_io.core.add.quadkey import _lat_lon_to_quadkey

    edge = 89.0 if lat > 0 else -89.0
    assert _lat_lon_to_quadkey(lat, 10.0, 12) == mercantile.quadkey(mercantile.tile(10.0, edge, 12))
