"""Tests for gpio process simplify (core/process/simplify.py).

The dependency-free tests (validation, geo-block stripping) run on every CI
leg; the tests that exercise coarsen itself skip where the ``simplify`` extra
is not installed. The invariant tests double as an upstream regression signal
for coarsen (ADR-0007).
"""

import json
from importlib.util import find_spec
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.simplify import (
    simplify_file,
    simplify_table,
    strip_stale_geometry_stats,
)

requires_coarsen = pytest.mark.skipif(
    find_spec("coarsen") is None, reason="requires the simplify extra (coarsen)"
)

TEST_DATA = Path(__file__).parent / "data"


def _shapely():
    return pytest.importorskip("shapely")


def _geo_block(crs=None, **column_extras):
    column = {"encoding": "WKB", "geometry_types": ["Polygon"], **column_extras}
    if crs is not None:
        column["crs"] = crs
    return {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": column},
    }


def _wkb_table(geometries, names=None):
    shapely = _shapely()
    wkb = [None if g is None else shapely.to_wkb(g) for g in geometries]
    names = names or [f"f{i}" for i in range(len(geometries))]
    table = pa.table({"name": names, "geometry": pa.array(wkb, type=pa.binary())})
    return table.replace_schema_metadata(
        {b"geo": json.dumps(_geo_block()).encode("utf-8")}
    )


class TestStripStaleGeometryStats:
    def test_drops_geometry_types_and_bbox_keeps_rest(self):
        geo = _geo_block(crs={"id": {"authority": "EPSG", "code": 4326}})
        geo["columns"]["geometry"]["bbox"] = [0, 0, 1, 1]
        geo["columns"]["geometry"]["edges"] = "planar"
        stripped = strip_stale_geometry_stats(geo)
        col = stripped["columns"]["geometry"]
        assert "geometry_types" not in col
        assert "bbox" not in col
        assert col["crs"] == {"id": {"authority": "EPSG", "code": 4326}}
        assert col["edges"] == "planar"
        assert stripped["primary_column"] == "geometry"

    def test_tolerates_malformed_columns(self):
        assert strip_stale_geometry_stats({"columns": None}) == {"columns": None}
        assert strip_stale_geometry_stats({}) == {}


class TestValidation:
    """These must fail before coarsen is imported: they run without it."""

    def test_negative_tolerance_raises(self):
        table = pa.table({"geometry": pa.array([b""], type=pa.binary())})
        with pytest.raises(InvalidParameterError, match="tolerance"):
            simplify_table(table, -0.5)

    def test_missing_geometry_column_raises(self):
        table = pa.table({"foo": [1]})
        with pytest.raises(InvalidParameterError, match="geometry"):
            simplify_table(table, 0.1, geometry_column="nope")


@requires_coarsen
class TestSimplifyTable:
    def test_reduces_vertices_and_preserves_attributes(self):
        shapely = _shapely()
        circle = shapely.Point(0, 0).buffer(1, quad_segs=64)  # 257 vertices
        table = _wkb_table([circle], names=["a"])
        result = simplify_table(table, 0.1)
        assert result.num_rows == 1
        assert result.column("name").to_pylist() == ["a"]
        out = shapely.from_wkb(result.column("geometry")[0].as_py())
        assert out.is_valid and not out.is_empty
        assert shapely.get_num_coordinates(out) < shapely.get_num_coordinates(circle)

    def test_zero_tolerance_is_identity(self):
        shapely = _shapely()
        circle = shapely.Point(2, 3).buffer(1, quad_segs=8)
        result = simplify_table(_wkb_table([circle]), 0.0)
        out = shapely.from_wkb(result.column("geometry")[0].as_py())
        assert shapely.equals(out, circle)

    def test_vertex_count_monotone_in_tolerance(self):
        shapely = _shapely()
        circle = shapely.Point(0, 0).buffer(1, quad_segs=64)
        counts = []
        for tol in (0.001, 0.01, 0.1, 0.5):
            result = simplify_table(_wkb_table([circle]), tol)
            out = shapely.from_wkb(result.column("geometry")[0].as_py())
            counts.append(shapely.get_num_coordinates(out))
        assert counts == sorted(counts, reverse=True)

    def test_nulls_preserved(self):
        shapely = _shapely()
        square = shapely.box(0, 0, 1, 1)
        result = simplify_table(_wkb_table([square, None]), 0.1)
        assert result.column("geometry")[1].as_py() is None
        assert result.num_rows == 2

    def test_preserve_topology_keeps_validity(self):
        shapely = _shapely()
        geoms = [shapely.Point(i, 0).buffer(0.6, quad_segs=32) for i in range(5)]
        result = simplify_table(_wkb_table(geoms), 0.5, preserve_topology=True)
        for value in result.column("geometry").to_pylist():
            out = shapely.from_wkb(value)
            assert out.is_valid and not out.is_empty

    def test_strips_stale_stats_from_carried_geo(self):
        shapely = _shapely()
        circle = shapely.Point(0, 0).buffer(1, quad_segs=64)
        table = _wkb_table([circle])
        geo = json.loads(table.schema.metadata[b"geo"])
        geo["columns"]["geometry"]["bbox"] = [-1, -1, 1, 1]
        table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
        result = simplify_table(table, 0.1)
        out_geo = json.loads(result.schema.metadata[b"geo"])
        col = out_geo["columns"]["geometry"]
        assert "bbox" not in col
        assert "geometry_types" not in col


@requires_coarsen
class TestCoverageMode:
    def _jittered_grid(self, n=3):
        """Adjacent unit squares whose shared edges carry extra vertices."""
        shapely = _shapely()
        squares = []
        for i in range(n):
            for j in range(n):
                base = shapely.box(i, j, i + 1, j + 1)
                squares.append(shapely.segmentize(base, 0.25))
        return squares

    def test_preserves_union_and_avoids_overlaps(self):
        shapely = _shapely()
        squares = self._jittered_grid()
        result = simplify_table(_wkb_table(squares), 0.2, coverage=True)
        outs = [shapely.from_wkb(v) for v in result.column("geometry").to_pylist()]
        assert len(outs) == len(squares)
        union = shapely.unary_union(outs)
        assert union.area == pytest.approx(9.0, rel=1e-6)
        for i in range(len(outs)):
            for j in range(i + 1, len(outs)):
                assert outs[i].intersection(outs[j]).area == pytest.approx(0.0, abs=1e-9)


@requires_coarsen
class TestSimplifyFile:
    def test_roundtrip_recomputes_file_stats(self, tmp_path):
        shapely = _shapely()
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        circle = shapely.Point(10, 20).buffer(1, quad_segs=64)
        src = tmp_path / "src.parquet"
        out = tmp_path / "out.parquet"
        write_geoparquet_table(_wkb_table([circle]), str(src))

        simplify_file(str(src), str(out), tolerance=0.1)

        f = pq.ParquetFile(str(out))
        geo = json.loads(f.schema_arrow.metadata[b"geo"])
        col = geo["columns"]["geometry"]
        written = shapely.from_wkb(f.read().column("geometry")[0].as_py())
        assert col["bbox"] == pytest.approx(list(written.bounds))
        assert col["geometry_types"] == ["Polygon"]
        assert shapely.get_num_coordinates(written) < shapely.get_num_coordinates(circle)

    def test_bbox_covering_column_recomputed(self, tmp_path):
        shapely = _shapely()
        src = TEST_DATA / "austria_bbox_covering.parquet"
        out = tmp_path / "out.parquet"
        simplify_file(str(src), str(out), tolerance=50.0)

        result = pq.read_table(str(out))
        geo = json.loads(result.schema.metadata[b"geo"])
        covering = geo["columns"]["geometry"]["covering"]["bbox"]
        assert covering["xmin"] == ["geometry_bbox", "xmin"]
        bboxes = result.column("geometry_bbox").to_pylist()
        geoms = [shapely.from_wkb(v) for v in result.column("geometry").to_pylist()]
        for row, geom in zip(bboxes, geoms):
            xmin, ymin, xmax, ymax = geom.bounds
            # The stored box must CONTAIN the geometry (float32 rounds outward)
            # and still sit tight against it.
            assert row["xmin"] <= xmin and row["ymin"] <= ymin
            assert row["xmax"] >= xmax and row["ymax"] >= ymax
            assert row["xmin"] == pytest.approx(xmin, abs=0.1)
            assert row["ymin"] == pytest.approx(ymin, abs=0.1)
            assert row["xmax"] == pytest.approx(xmax, abs=0.1)
            assert row["ymax"] == pytest.approx(ymax, abs=0.1)

class TestCliSimplify:
    """CLI surface: option validation is dependency-free."""

    def _invoke(self, args):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        return CliRunner().invoke(cli, ["process", "simplify", *args])

    def test_requires_tolerance(self, tmp_path):
        result = self._invoke(
            [str(TEST_DATA / "buildings_test.parquet"), str(tmp_path / "o.parquet")]
        )
        assert result.exit_code != 0
        assert "tolerance" in result.output.lower()

    def test_coverage_rejects_preserve_topology_flags(self, tmp_path):
        result = self._invoke(
            [
                str(TEST_DATA / "buildings_test.parquet"),
                str(tmp_path / "o.parquet"),
                "--tolerance",
                "0.1",
                "--coverage",
                "--no-preserve-topology",
            ]
        )
        assert result.exit_code != 0
        assert "preserve-topology" in result.output
        assert "coverage" in result.output

    def test_simplify_boundary_requires_coverage(self, tmp_path):
        result = self._invoke(
            [
                str(TEST_DATA / "buildings_test.parquet"),
                str(tmp_path / "o.parquet"),
                "--tolerance",
                "0.1",
                "--no-simplify-boundary",
            ]
        )
        assert result.exit_code != 0
        assert "simplify-boundary" in result.output
        assert "coverage" in result.output

    def test_missing_coarsen_gives_install_hint(self, tmp_path, monkeypatch):
        import sys as _sys

        monkeypatch.setitem(_sys.modules, "coarsen", None)
        result = self._invoke(
            [
                str(TEST_DATA / "buildings_test.parquet"),
                str(tmp_path / "o.parquet"),
                "--tolerance",
                "0.1",
            ]
        )
        assert result.exit_code != 0
        assert "pip install 'geoparquet-io[simplify]'" in result.output
        assert "Traceback" not in result.output

    @requires_coarsen
    def test_roundtrip(self, tmp_path):
        out = tmp_path / "o.parquet"
        result = self._invoke(
            [
                str(TEST_DATA / "buildings_test.parquet"),
                str(out),
                "--tolerance",
                "0.00001",
            ]
        )
        assert result.exit_code == 0, result.output
        assert pq.ParquetFile(str(out)).metadata.num_rows > 0


@requires_coarsen
class TestSimplifyFileCrs:
    def test_crs_preserved(self, tmp_path):
        src = TEST_DATA / "austria_bbox_covering.parquet"
        in_geo = json.loads(pq.ParquetFile(str(src)).schema_arrow.metadata[b"geo"])
        out = tmp_path / "out.parquet"
        simplify_file(str(src), str(out), tolerance=50.0)
        out_geo = json.loads(pq.ParquetFile(str(out)).schema_arrow.metadata[b"geo"])
        in_crs = in_geo["columns"]["geometry"].get("crs")
        out_crs = out_geo["columns"]["geometry"].get("crs")
        assert out_crs == in_crs
        assert pq.ParquetFile(str(out)).metadata.num_rows == 30
