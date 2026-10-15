"""A GEOGRAPHY column must come back GEOGRAPHY from every 2.0 write (#999).

DuckDB reads a Parquet ``GEOGRAPHY`` column as a planar ``GEOMETRY`` and has no
geography type to write one back, so every gpio write that goes through it --
``add``, ``sort``, ``convert``, ``extract``, ``partition`` -- used to emit a
planar ``GEOMETRY`` logical type while ``_preserve_edges_after_write`` restored
``"edges": "spherical"`` in the ``geo`` block. The two halves of the output then
described different shapes over the same bytes, and ``gpio check spec`` warned::

    metadata declares non-planar edges 'spherical' but column has planar
    GEOMETRY logical type

Every assertion here reads the two carriers separately -- the Parquet logical
type plus the GeoArrow edge type pyarrow materialises from it, and the ``geo``
block -- and then asks ``gpio check spec`` whether they agree.

Refs: https://github.com/geoparquet/geoparquet-io/issues/999
"""

from __future__ import annotations

import json
from pathlib import Path

# Registers the GeoArrow extension types, without which pyarrow reads a
# GEOGRAPHY as plain binary and the edge type half of every pair reads None.
import geoarrow.pyarrow  # noqa: F401
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
from tests.native_geo_probes import (
    conus_wkb,
    geo_block,
    logical_geo_types,
    native_geo_edges,
    spec_problems,
    write_native_geo_only,
)

SPHERICAL = ("Geography", "EdgeType.SPHERICAL")
PLANAR = ("Geometry", "EdgeType.PLANAR")


def _run_cli(*args) -> None:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output


def _one_file(out: Path) -> Path:
    """The output itself, or the first file of a partitioned output."""
    if out.is_dir():
        return sorted(out.rglob("*.parquet"))[0]
    return out


def _edges_in_geo_block(path, column="geometry"):
    return ((geo_block(path) or {}).get("columns") or {}).get(column, {}).get("edges")


@pytest.fixture(scope="module")
def rows():
    return conus_wkb("cell", "ST_Centroid(cell)")


@pytest.fixture(scope="module")
def native_geography(tmp_path_factory, rows) -> Path:
    """Native-geo-only: the edges live only in the GEOGRAPHY logical type."""
    import geoarrow.pyarrow as ga
    import geoarrow.types as gt

    return write_native_geo_only(
        tmp_path_factory.mktemp("native") / "in.parquet",
        rows,
        {"geometry": (2, ga.wkb().with_edge_type(gt.EdgeType.SPHERICAL))},
    )


@pytest.fixture(scope="module")
def declared_spherical_wkb(tmp_path_factory, rows) -> Path:
    """A 1.1 WKB file whose edges live only in the ``geo`` block."""
    table = pa.table(
        {
            "id": [r[0] for r in rows],
            "grp": [r[1] for r in rows],
            "geometry": pa.array([bytes(r[2]) for r in rows], pa.binary()),
        }
    )
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": "WKB", "geometry_types": ["Polygon"], "edges": "spherical"}
        },
    }
    path = tmp_path_factory.mktemp("declared") / "in.parquet"
    pq.write_table(table.replace_schema_metadata({"geo": json.dumps(geo)}), str(path))
    return path


@pytest.fixture(scope="module")
def planar_primary_spherical_secondary(tmp_path_factory, rows) -> Path:
    import geoarrow.pyarrow as ga
    import geoarrow.types as gt

    return write_native_geo_only(
        tmp_path_factory.mktemp("mixed") / "in.parquet",
        rows,
        {
            "geometry": (3, ga.wkb()),
            "geog": (2, ga.wkb().with_edge_type(gt.EdgeType.SPHERICAL)),
        },
    )


@pytest.fixture(scope="module")
def native_planar(tmp_path_factory, rows) -> Path:
    import geoarrow.pyarrow as ga

    return write_native_geo_only(
        tmp_path_factory.mktemp("planar") / "in.parquet", rows, {"geometry": (2, ga.wkb())}
    )


#: Every CLI write path that rewrites the rows. ``IN``/``OUT`` are substituted.
WRITE_PATHS = {
    "add-bbox": ["add", "bbox", "IN", "OUT"],
    "add-quadkey": ["add", "quadkey", "IN", "OUT"],
    "sort-hilbert": ["sort", "hilbert", "IN", "OUT"],
    "sort-column": ["sort", "column", "IN", "OUT", "id"],
    "convert-geoparquet": ["convert", "geoparquet", "IN", "OUT"],
    "extract-geoparquet": ["extract", "geoparquet", "IN", "OUT"],
    "partition-string": ["partition", "string", "IN", "OUT", "--column", "grp", "--force"],
}


def _run_path(name, source, out, *extra):
    args = [str(source) if a == "IN" else str(out) if a == "OUT" else a for a in WRITE_PATHS[name]]
    _run_cli(*args, *extra)
    return _one_file(out)


def test_the_fixtures_are_what_they_claim(native_geography, declared_spherical_wkb):
    """Non-vacuity: one carries the edges in the logical type, one in the block."""
    assert native_geo_edges(native_geography) == SPHERICAL
    assert geo_block(native_geography) is None
    assert logical_geo_types(declared_spherical_wkb) == {}
    assert _edges_in_geo_block(declared_spherical_wkb) == "spherical"


@pytest.mark.parametrize("path_name", list(WRITE_PATHS), ids=list(WRITE_PATHS))
def test_a_native_geography_stays_geography(path_name, native_geography, tmp_path):
    out = _run_path(path_name, native_geography, tmp_path / "out.parquet")

    assert native_geo_edges(out) == SPHERICAL
    assert _edges_in_geo_block(out) == "spherical"
    assert spec_problems(out) == []
    assert spec_problems(out, status="warning") == []


@pytest.mark.parametrize("path_name", list(WRITE_PATHS), ids=list(WRITE_PATHS))
def test_declared_spherical_edges_become_a_geography_at_2_0(
    path_name, declared_spherical_wkb, tmp_path
):
    """A 1.1 input asked for 2.0: the block's edges must reach the logical type too."""
    out = _run_path(
        path_name, declared_spherical_wkb, tmp_path / "out.parquet", "--geoparquet-version", "2.0"
    )

    assert native_geo_edges(out) == SPHERICAL
    assert _edges_in_geo_block(out) == "spherical"
    assert spec_problems(out) == []
    assert spec_problems(out, status="warning") == []


def test_a_1_1_output_keeps_wkb_with_edges_in_the_block(native_geography, tmp_path):
    """1.x has no logical type to carry edges: WKB plus ``edges`` is correct."""
    out = _run_path(
        "add-bbox", native_geography, tmp_path / "out.parquet", "--geoparquet-version", "1.1"
    )

    assert logical_geo_types(out) == {}
    assert _edges_in_geo_block(out) == "spherical"
    assert spec_problems(out) == []


@pytest.mark.parametrize(
    "path_name", ["add-bbox", "convert-geoparquet"], ids=["add-bbox", "convert"]
)
def test_a_planar_input_stays_planar(path_name, native_planar, tmp_path):
    out = _run_path(path_name, native_planar, tmp_path / "out.parquet")

    assert native_geo_edges(out) == PLANAR
    assert _edges_in_geo_block(out) in (None, "planar")
    assert spec_problems(out, status="warning") == []


@pytest.mark.parametrize(
    "path_name", ["add-bbox", "convert-geoparquet"], ids=["add-bbox", "convert"]
)
def test_only_the_spherical_secondary_becomes_a_geography(
    path_name, planar_primary_spherical_secondary, tmp_path
):
    out = _run_path(path_name, planar_primary_spherical_secondary, tmp_path / "out.parquet")

    assert native_geo_edges(out, "geometry") == PLANAR
    assert native_geo_edges(out, "geog") == SPHERICAL
    assert _edges_in_geo_block(out, "geog") == "spherical"
    assert spec_problems(out) == []
    assert spec_problems(out, status="warning") == []


def test_an_ellipsoidal_algorithm_is_kept_verbatim(rows, tmp_path):
    """A 2.0 block's ``vincenty`` becomes the GEOGRAPHY's algorithm, not spherical.

    pyarrow cannot write a vincenty GEOGRAPHY, so the input declares it the way a
    2.0 writer may: a native GEOMETRY whose ``geo`` block names the edges.
    """
    import geoarrow.pyarrow as ga

    from geoparquet_io.core.parquet_footer import patch_footer_kv

    source = tmp_path / "in.parquet"
    write_native_geo_only(source, rows, {"geometry": (2, ga.wkb())})
    geo = {
        "version": "2.0.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": "WKB", "geometry_types": ["Polygon"], "edges": "vincenty"}
        },
    }
    patch_footer_kv(str(source), {"geo": json.dumps(geo)})

    out = _run_path("add-bbox", source, tmp_path / "out.parquet")

    assert native_geo_edges(out) == ("Geography", "EdgeType.VINCENTY")
    assert _edges_in_geo_block(out) == "vincenty"
    assert spec_problems(out, status="warning") == []


def test_the_crs_survives_the_family_change(rows, tmp_path):
    """The GEOGRAPHY takes over the GEOMETRY's CRS, not a defaulted one."""
    import geoarrow.pyarrow as ga
    import geoarrow.types as gt

    from tests.native_geo_probes import projjson

    source = write_native_geo_only(
        tmp_path / "in.parquet",
        rows,
        {"geometry": (2, ga.wkb().with_edge_type(gt.EdgeType.SPHERICAL).with_crs(projjson(4269)))},
    )
    out = _run_path("add-bbox", source, tmp_path / "out.parquet")

    assert logical_geo_types(out)["geometry"] == ("Geography", {"authority": "EPSG", "code": 4269})
    # Whatever `check spec` thinks of these coordinates under NAD83, it thinks
    # it of the input too: the write must add no failure of its own.
    assert set(spec_problems(out)) <= set(spec_problems(source))


@pytest.fixture(scope="module")
def projected_spherical_wkb(tmp_path_factory) -> Path:
    """A 1.1 WKB file in EPSG:3857 whose ``geo`` block declares spherical edges."""
    import shapely
    from pyproj import CRS

    points = shapely.points([(1e6, 4e6), (2e6, 5e6)])
    table = pa.table(
        {
            "id": [0, 1],
            "geometry": pa.array(list(shapely.to_wkb(points)), pa.binary()),
        }
    )
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "edges": "spherical",
                "crs": CRS.from_epsg(3857).to_json_dict(),
            }
        },
    }
    path = tmp_path_factory.mktemp("projected") / "in.parquet"
    pq.write_table(table.replace_schema_metadata({"geo": json.dumps(geo)}), str(path))
    return path


@pytest.mark.parametrize(
    "path_name", ["convert-geoparquet", "sort-hilbert"], ids=["convert", "sort-hilbert"]
)
def test_a_projected_crs_is_not_retyped_geography(
    path_name, projected_spherical_wkb, tmp_path, caplog
):
    """GEOGRAPHY requires a geographic CRS: a projected one stays a GEOMETRY.

    The input's ``edges`` claim is incoherent on a plane (``check spec`` warns
    about it either way), but retyping the column made it a spec-invalid
    GEOGRAPHY that ``check spec`` fails. The column keeps its planar type, the
    block keeps what the input declared, and the user is told why.
    """
    out = _run_path(
        path_name, projected_spherical_wkb, tmp_path / "out.parquet", "--geoparquet-version", "2.0"
    )

    assert logical_geo_types(out)["geometry"][0] == "Geometry"
    assert _edges_in_geo_block(out) == "spherical"
    assert spec_problems(out) == []
    assert "requires a geographic CRS" in caplog.text


def test_a_projected_1_1_output_has_no_type_to_keep_planar(
    projected_spherical_wkb, tmp_path, caplog
):
    """1.x writes plain WKB: there is no GEOGRAPHY to refuse, so nothing to say."""
    out = _run_path(
        "sort-hilbert",
        projected_spherical_wkb,
        tmp_path / "out.parquet",
        "--geoparquet-version",
        "1.1",
    )

    assert logical_geo_types(out) == {}
    assert _edges_in_geo_block(out) == "spherical"
    assert "requires a geographic CRS" not in caplog.text


def test_duckdb_still_reads_the_output(native_geography, tmp_path):
    out = _run_path("add-bbox", native_geography, tmp_path / "out.parquet")

    con = get_duckdb_connection(load_spatial=True)
    try:
        count, area_ok = con.execute(
            f"SELECT count(*), bool_and(ST_Area(geometry) > 0) FROM {sql_path(str(out))}"
        ).fetchone()
    finally:
        con.close()
    assert (count, area_ok) == (200, True)


def test_a_pyarrow_geography_without_a_bbox_is_not_warned_about(native_geography):
    """Arrow writes no bbox for a GEOGRAPHY: a vertex box does not bound its edges."""
    assert spec_problems(native_geography, status="warning") == []


# ---------------------------------------------------------------------------
# The funnel hook itself: `_apply_nonplanar_edges`
# ---------------------------------------------------------------------------


@pytest.fixture
def duckdb_v2(tmp_path) -> Path:
    path = tmp_path / "v2.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            "COPY (SELECT 1 AS id, ST_GeomFromText('LINESTRING (0 0, 10 10)') AS geometry) "
            f"TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
        )
    finally:
        con.close()
    return path


def test_an_unpatchable_footer_falls_back_to_the_block_rewrite(duckdb_v2, monkeypatch, caplog):
    from geoparquet_io.core import write_funnels
    from geoparquet_io.core.parquet_footer import FooterPatchUnsupported

    def refuse(*args, **kwargs):
        raise FooterPatchUnsupported("refused for the test")

    monkeypatch.setattr(write_funnels, "patch_footer", refuse)
    write_funnels._apply_nonplanar_edges({"geometry": "spherical"}, str(duckdb_v2))

    assert _edges_in_geo_block(duckdb_v2) == "spherical"
    assert logical_geo_types(duckdb_v2)["geometry"][0] == "Geometry"
    assert "stay planar in the Parquet schema" in caplog.text


def test_a_race_propagates_without_a_rewrite(duckdb_v2, monkeypatch):
    from geoparquet_io.core import write_funnels
    from geoparquet_io.core.parquet_footer import FooterPatchRaced

    def race(*args, **kwargs):
        raise FooterPatchRaced("changed for the test")

    monkeypatch.setattr(write_funnels, "patch_footer", race)
    monkeypatch.setattr(
        write_funnels,
        "_rewrite_file_with_geo_metadata",
        lambda *a, **k: pytest.fail("a raced patch must not fall back to a rewrite"),
    )
    with pytest.raises(FooterPatchRaced):
        write_funnels._apply_nonplanar_edges({"geometry": "spherical"}, str(duckdb_v2))


def test_a_geo_key_less_output_is_carried_by_the_logical_type(duckdb_v2, caplog):
    """Parquet-geo-only: the GEOGRAPHY type is the one carrier, and it is enough."""
    from geoparquet_io.core import write_funnels
    from geoparquet_io.core.parquet_footer import patch_footer_kv

    patch_footer_kv(str(duckdb_v2), {"geo": None})
    write_funnels._apply_nonplanar_edges({"geometry": "spherical"}, str(duckdb_v2), verbose=True)

    assert native_geo_edges(duckdb_v2) == SPHERICAL
    assert "Edge interpretation is lost" not in caplog.text


def test_an_output_with_no_carrier_at_all_says_the_edges_are_lost(tmp_path, caplog):
    from geoparquet_io.core import write_funnels

    path = tmp_path / "plain.parquet"
    pq.write_table(pa.table({"geometry": pa.array([b"\x01"], pa.binary())}), str(path))
    write_funnels._apply_nonplanar_edges({"geometry": "spherical"}, str(path))

    assert "Edge interpretation is lost" in caplog.text


def test_an_edges_value_parquet_cannot_name_stays_in_the_block_only(duckdb_v2):
    """A malformed input ``edges`` must not crash the write in the retype."""
    from geoparquet_io.core import write_funnels

    write_funnels._apply_nonplanar_edges({"geometry": "geodesic"}, str(duckdb_v2))

    assert _edges_in_geo_block(duckdb_v2) == "geodesic"
    assert logical_geo_types(duckdb_v2)["geometry"][0] == "Geometry"
