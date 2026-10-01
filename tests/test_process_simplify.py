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
    return table.replace_schema_metadata({b"geo": json.dumps(_geo_block()).encode("utf-8")})


class TestStripStaleGeometryStats:
    def test_drops_geometry_types_and_bbox_keeps_rest(self):
        geo = _geo_block(crs={"id": {"authority": "EPSG", "code": 4326}})
        geo["columns"]["geometry"]["bbox"] = [0, 0, 1, 1]
        geo["columns"]["geometry"]["edges"] = "planar"
        stripped = strip_stale_geometry_stats(geo, "geometry")
        col = stripped["columns"]["geometry"]
        assert "geometry_types" not in col
        assert "bbox" not in col
        assert col["crs"] == {"id": {"authority": "EPSG", "code": 4326}}
        assert col["edges"] == "planar"
        assert stripped["primary_column"] == "geometry"

    def test_tolerates_malformed_columns(self):
        assert strip_stale_geometry_stats({"columns": None}, "geometry") == {"columns": None}
        assert strip_stale_geometry_stats({}, "geometry") == {}

    def test_only_the_named_column_is_stripped(self):
        geo = {
            "columns": {
                "geometry": {"encoding": "WKB", "geometry_types": ["Polygon"]},
                "geom2": {"encoding": "WKB", "geometry_types": ["Point"], "bbox": [0, 0, 1, 1]},
            }
        }
        strip_stale_geometry_stats(geo, "geometry")
        assert "geometry_types" not in geo["columns"]["geometry"]
        assert geo["columns"]["geom2"]["geometry_types"] == ["Point"]
        assert geo["columns"]["geom2"]["bbox"] == [0, 0, 1, 1]


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
        for row, geom in zip(bboxes, geoms, strict=True):
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


@requires_coarsen
class TestPythonApi:
    def test_ops_simplify(self):
        shapely = _shapely()
        from geoparquet_io.api import ops

        circle = shapely.Point(0, 0).buffer(1, quad_segs=64)
        result = ops.simplify(_wkb_table([circle]), 0.1)
        assert isinstance(result, pa.Table)
        out = shapely.from_wkb(result.column("geometry")[0].as_py())
        assert shapely.get_num_coordinates(out) < shapely.get_num_coordinates(circle)

    def test_table_simplify_chains(self):
        shapely = _shapely()
        from geoparquet_io.api.table import Table

        circle = shapely.Point(0, 0).buffer(1, quad_segs=64)
        result = Table(_wkb_table([circle])).simplify(0.1)
        assert isinstance(result, Table)
        out = shapely.from_wkb(result.to_arrow().column("geometry")[0].as_py())
        assert out.is_valid


@requires_coarsen
class TestMultiGeometryColumns:
    """Simplify must strip/recompute only the simplified column's stats."""

    def _two_geom_file(self, tmp_path):
        shapely = _shapely()
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        g1 = shapely.Point(0, 0).buffer(1, quad_segs=32)
        g2 = shapely.Point(5, 5).buffer(1, quad_segs=32)
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {"encoding": "WKB"},
                "geom2": {
                    "encoding": "WKB",
                    "geometry_types": ["Polygon"],
                    "bbox": [4.0, 4.0, 6.0, 6.0],
                },
            },
        }
        table = pa.table(
            {
                "geometry": pa.array([shapely.to_wkb(g1)], pa.binary()),
                "geom2": pa.array([shapely.to_wkb(g2)], pa.binary()),
            }
        ).replace_schema_metadata({b"geo": json.dumps(geo).encode()})
        src = tmp_path / "two.parquet"
        write_geoparquet_table(table, str(src))
        return src

    def _geo_of(self, path):
        return json.loads(pq.ParquetFile(str(path)).schema_arrow.metadata[b"geo"])

    def test_secondary_stats_survive_simplifying_primary(self, tmp_path):
        src = self._two_geom_file(tmp_path)
        in_geo = self._geo_of(src)
        out = tmp_path / "out.parquet"
        simplify_file(str(src), str(out), tolerance=0.1)
        out_geo = self._geo_of(out)
        assert (
            out_geo["columns"]["geom2"]["geometry_types"]
            == (in_geo["columns"]["geom2"]["geometry_types"])
        )
        assert out_geo["columns"]["geom2"]["bbox"] == in_geo["columns"]["geom2"]["bbox"]
        assert "geometry_types" in out_geo["columns"]["geometry"]

    def test_simplifying_secondary_keeps_primary_stats(self, tmp_path):
        shapely = _shapely()
        src = self._two_geom_file(tmp_path)
        in_geo = self._geo_of(src)
        out = tmp_path / "out.parquet"
        simplify_file(str(src), str(out), tolerance=0.1, geometry_column="geom2")
        out_geo = self._geo_of(out)
        assert out_geo["primary_column"] == "geometry"
        assert out_geo["columns"]["geometry"]["bbox"] == (in_geo["columns"]["geometry"]["bbox"])
        # the simplified secondary's stats were recomputed, and from fewer vertices
        assert "geometry_types" in out_geo["columns"]["geom2"]
        result = pq.read_table(str(out))
        geom2 = shapely.from_wkb(result.column("geom2")[0].as_py())
        assert shapely.get_num_coordinates(geom2) < 33


class TestCleanErrors:
    """The commonest user errors must not print tracebacks."""

    def test_missing_input_file_cli(self, tmp_path):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        result = CliRunner().invoke(
            cli,
            [
                "process",
                "simplify",
                str(tmp_path / "nope.parquet"),
                str(tmp_path / "o.parquet"),
                "--tolerance",
                "1",
            ],
        )
        assert result.exit_code != 0
        assert "not found" in result.output.lower() or "nope.parquet" in result.output
        assert "Traceback" not in result.output

    @requires_coarsen
    def test_corrupt_wkb_is_a_clean_error(self):
        from geoparquet_io.core.exceptions import GeoParquetError

        table = pa.table({"geometry": pa.array([b"\x00\x00not wkb"], pa.binary())})
        with pytest.raises(GeoParquetError, match="WKB"):
            simplify_table(table, 0.1)


@requires_coarsen
class TestStreamingPlainMode:
    """Plain mode must stream: memory bounded by a row group, not the file.

    Coverage mode inherently needs the whole column (shared edges) and keeps
    the in-memory path; its behavior is pinned by TestCoverageMode above.
    """

    def _many_group_file(self, tmp_path, groups=5, rows_per=200, name="src.parquet"):
        shapely = _shapely()
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        geoms, names = [], []
        for i in range(groups * rows_per):
            geoms.append(shapely.Point(i % 360 - 180, (i * 7) % 140 - 70).buffer(0.4, quad_segs=48))
            names.append(f"f{i}")
        table = _wkb_table(geoms, names=names)
        src = tmp_path / name
        write_geoparquet_table(table, str(src), row_group_rows=rows_per)
        assert pq.ParquetFile(str(src)).metadata.num_row_groups == groups
        return src

    def test_streaming_matches_in_memory_reference(self, tmp_path):
        """The streamed file is byte-equivalent in data and geo metadata to
        simplify_table + the write funnel on the same input."""
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        src = self._many_group_file(tmp_path)
        streamed = tmp_path / "streamed.parquet"
        simplify_file(str(src), str(streamed), tolerance=0.05, row_group_rows=200)

        reference = tmp_path / "reference.parquet"
        ref_table = simplify_table(pq.read_table(str(src)), 0.05)
        write_geoparquet_table(ref_table, str(reference), row_group_rows=200)

        out_a, out_b = pq.read_table(str(streamed)), pq.read_table(str(reference))
        assert out_a.column("geometry").to_pylist() == out_b.column("geometry").to_pylist()
        assert out_a.column("name").to_pylist() == out_b.column("name").to_pylist()
        geo_a = json.loads(out_a.schema.metadata[b"geo"])
        geo_b = json.loads(out_b.schema.metadata[b"geo"])
        assert geo_a == geo_b
        assert pq.ParquetFile(str(streamed)).metadata.num_row_groups == 5

    def test_covering_column_refreshed_while_streaming(self, tmp_path):
        shapely = _shapely()
        src = TEST_DATA / "austria_bbox_covering.parquet"
        out = tmp_path / "out.parquet"
        simplify_file(str(src), str(out), tolerance=50.0)
        result = pq.read_table(str(out))
        geoms = [shapely.from_wkb(v) for v in result.column("geometry").to_pylist()]
        for row, geom in zip(result.column("geometry_bbox").to_pylist(), geoms, strict=True):
            xmin, ymin, xmax, ymax = geom.bounds
            assert row["xmin"] <= xmin and row["ymax"] >= ymax

    def test_memory_bounded_by_a_row_group(self, tmp_path):
        """Measured in a subprocess (pa.total_allocated_bytes is process-wide,
        following tests/test_v2_zm_geo_metadata.py's #1178 pattern): growth
        while simplifying a 10-group file stays under half the file."""
        import subprocess
        import sys as _sys

        src = self._many_group_file(tmp_path, groups=10, rows_per=400)
        out = tmp_path / "out.parquet"
        probe = f"""
import json
import pyarrow as pa
import pyarrow.parquet as pq
from unittest.mock import patch
import geoparquet_io.core.process.simplify as simp

peaks = []
real = simp._simplify_batch_values if hasattr(simp, "_simplify_batch_values") else None
orig_write = pq.ParquetWriter.write_table
def spy(self, table, **kw):
    peaks.append(pa.total_allocated_bytes())
    return orig_write(self, table, **kw)
pq.ParquetWriter.write_table = spy
baseline = pa.total_allocated_bytes()
simp.simplify_file({str(src)!r}, {str(out)!r}, tolerance=0.05, row_group_rows=400)
growth = max(peaks) - baseline if peaks else -1
one_group = pq.ParquetFile({str(src)!r}).metadata.row_group(0).total_byte_size
print(json.dumps({{"writes": len(peaks), "growth": growth, "one_group": one_group}}))
"""
        measured = subprocess.run(
            [_sys.executable, "-c", probe], capture_output=True, text=True, check=True
        )
        result = json.loads(measured.stdout.strip().splitlines()[-1])
        assert result["writes"] >= 10, "the streaming path never wrote per batch"
        file_bytes = sum(
            pq.ParquetFile(str(src)).metadata.row_group(g).total_byte_size for g in range(10)
        )
        assert result["growth"] < file_bytes // 2, (
            f"growth {result['growth']:,} suggests the whole file was materialized "
            f"(file holds {file_bytes:,} bytes uncompressed)"
        )

    def test_zero_row_file_streams(self, tmp_path):
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        empty = _wkb_table([]).slice(0, 0)
        src = tmp_path / "empty.parquet"
        write_geoparquet_table(empty, str(src))
        out = tmp_path / "out.parquet"
        simplify_file(str(src), str(out), tolerance=0.1)
        assert pq.ParquetFile(str(out)).metadata.num_rows == 0

    def test_native_2_0_input_falls_back_to_in_memory(self, tmp_path):
        """An explicit 2.0 output request means native types,
        which the streaming writer does not produce; the in-memory funnel
        path must be taken and still yield a valid file. (A native-2.0
        INPUT reads back as a geoarrow extension column, which simplify
        rejects today — a separate, pre-existing limitation.)"""
        shapely = _shapely()
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        circle = shapely.Point(1, 2).buffer(1, quad_segs=16)
        table = _wkb_table([circle])
        src = tmp_path / "v11.parquet"
        write_geoparquet_table(table, str(src))
        out = tmp_path / "out.parquet"
        simplify_file(str(src), str(out), tolerance=0.0, geoparquet_version="2.0")
        assert pq.ParquetFile(str(out)).metadata.num_rows == 1


@requires_coarsen
class TestDropEmpty:
    """--drop-empty (#1199): rows whose geometry is empty after
    simplification are dropped; nulls are kept either way."""

    def _table_with_empty(self):
        shapely = _shapely()
        square = shapely.box(0, 0, 1, 1)
        empty = shapely.Polygon()
        return _wkb_table([square, empty, None], names=["a", "b", "c"])

    def test_default_keeps_empties(self):
        result = simplify_table(self._table_with_empty(), 0.1)
        assert result.num_rows == 3

    def test_drop_empty_removes_only_empties(self):
        shapely = _shapely()
        result = simplify_table(self._table_with_empty(), 0.1, drop_empty=True)
        assert result.column("name").to_pylist() == ["a", "c"]
        assert result.column("geometry")[1].as_py() is None  # null survives
        out = shapely.from_wkb(result.column("geometry")[0].as_py())
        assert not out.is_empty

    def test_streaming_file_honors_drop_empty(self, tmp_path):
        shapely = _shapely()
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        geoms = [shapely.box(i, 0, i + 1, 1) for i in range(6)]
        geoms[2] = shapely.Polygon()  # empty in batch 1
        geoms[5] = shapely.Polygon()  # empty in batch 2
        src = tmp_path / "src.parquet"
        write_geoparquet_table(_wkb_table(geoms), str(src), row_group_rows=3)
        out = tmp_path / "out.parquet"
        simplify_file(str(src), str(out), tolerance=0.1, drop_empty=True)
        assert pq.ParquetFile(str(out)).metadata.num_rows == 4

    def test_cli_flag(self, tmp_path):
        shapely = _shapely()
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        src = tmp_path / "src.parquet"
        write_geoparquet_table(_wkb_table([shapely.box(0, 0, 1, 1), shapely.Polygon()]), str(src))
        out = tmp_path / "out.parquet"
        result = CliRunner().invoke(
            cli,
            ["process", "simplify", str(src), str(out), "--tolerance", "0.1", "--drop-empty"],
        )
        assert result.exit_code == 0, result.output
        assert pq.ParquetFile(str(out)).metadata.num_rows == 1


@requires_coarsen
class TestNative20Input:
    """#1198: native GeoParquet 2.0 inputs read back as geoarrow.wkb
    extension columns; simplify must accept them and write 2.0 back."""

    def _v2_file(self, tmp_path):
        shapely = _shapely()
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        circle = shapely.Point(10, 20).buffer(1, quad_segs=64)
        src = tmp_path / "v2.parquet"
        write_geoparquet_table(_wkb_table([circle]), str(src), geoparquet_version="2.0")
        return src, circle

    def test_simplify_table_accepts_extension_column(self, tmp_path):
        shapely = _shapely()
        src, circle = self._v2_file(tmp_path)
        table = pq.read_table(str(src))
        assert isinstance(table.schema.field("geometry").type, pa.ExtensionType)
        result = simplify_table(table, 0.1)
        out_field = result.schema.field("geometry")
        assert not isinstance(out_field.type, pa.ExtensionType)
        out = shapely.from_wkb(result.column("geometry")[0].as_py())
        assert out.is_valid
        assert shapely.get_num_coordinates(out) < shapely.get_num_coordinates(circle)

    def test_file_roundtrip_stays_native_2_0(self, tmp_path):
        shapely = _shapely()
        src, circle = self._v2_file(tmp_path)
        out = tmp_path / "out.parquet"
        simplify_file(str(src), str(out), tolerance=0.1)
        back = pq.read_table(str(out))
        geo = json.loads(back.schema.metadata[b"geo"])
        assert geo["version"].startswith("2.")
        column = back.column("geometry")
        wkb = (
            column.chunk(0).storage[0].as_py()
            if isinstance(column.type, pa.ExtensionType)
            else column[0].as_py()
        )
        out_geom = shapely.from_wkb(wkb)
        assert shapely.get_num_coordinates(out_geom) < shapely.get_num_coordinates(circle)
        assert geo["columns"]["geometry"]["geometry_types"] == ["Polygon"]


class TestUtmEpsg:
    """Dependency-free: the auto-utm zone math (#1197)."""

    def test_northern_zone(self):
        from geoparquet_io.core.process.simplify import _utm_epsg

        assert _utm_epsg(105.0, 15.0) == 32648  # Vietnam, zone 48N

    def test_southern_zone(self):
        from geoparquet_io.core.process.simplify import _utm_epsg

        assert _utm_epsg(-58.4, -34.6) == 32721  # Buenos Aires, zone 21S

    def test_edges_clamp(self):
        from geoparquet_io.core.process.simplify import _utm_epsg

        assert _utm_epsg(-180.0, 10.0) == 32601
        assert _utm_epsg(180.0, 10.0) == 32660


@requires_coarsen
class TestSimplifyCrs:
    """#1197: --simplify-crs projects to a metric CRS, simplifies with the
    tolerance in that CRS's units, and projects back."""

    def _degree_circle(self):
        shapely = _shapely()
        # ~1.1 km circle at lon 105 / lat 15 (UTM zone 48N), in degrees
        return shapely.Point(105.0, 15.0).buffer(0.01, quad_segs=64)

    def test_metric_tolerance_via_explicit_crs(self):
        shapely = _shapely()
        circle = self._degree_circle()
        result = simplify_table(_wkb_table([circle]), 50.0, simplify_crs="EPSG:32648")
        out = shapely.from_wkb(result.column("geometry")[0].as_py())
        assert out.is_valid and not out.is_empty
        # 50 m on a ~1.1 km circle: real reduction, but nowhere near collapse
        assert 4 < shapely.get_num_coordinates(out) < shapely.get_num_coordinates(circle)
        # ...and the output is still in degrees at the original location
        xmin, ymin, xmax, ymax = out.bounds
        assert 104.98 < xmin < 105.02 and 14.98 < ymin < 15.02

    def test_zero_tolerance_round_trip_is_noise_only(self):
        shapely = _shapely()
        circle = self._degree_circle()
        result = simplify_table(_wkb_table([circle]), 0.0, simplify_crs="EPSG:32648")
        out = shapely.from_wkb(result.column("geometry")[0].as_py())
        assert shapely.equals_exact(out, circle, tolerance=1e-8)

    def test_auto_utm_matches_explicit_zone(self):
        circle = self._degree_circle()
        explicit = simplify_table(_wkb_table([circle]), 50.0, simplify_crs="EPSG:32648")
        auto = simplify_table(_wkb_table([circle]), 50.0, simplify_crs="auto-utm")
        assert auto.column("geometry").to_pylist() == explicit.column("geometry").to_pylist()

    def test_invalid_crs_is_a_clean_error(self):
        with pytest.raises(InvalidParameterError, match="simplify_crs"):
            simplify_table(_wkb_table([self._degree_circle()]), 1.0, simplify_crs="EPSG:999999")

    def test_streaming_file_with_simplify_crs(self, tmp_path):
        shapely = _shapely()
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        circles = [
            shapely.Point(105.0 + i * 0.05, 15.0).buffer(0.01, quad_segs=48) for i in range(6)
        ]
        src = tmp_path / "src.parquet"
        write_geoparquet_table(_wkb_table(circles), str(src), row_group_rows=3)
        out = tmp_path / "out.parquet"
        simplify_file(str(src), str(out), tolerance=50.0, simplify_crs="auto-utm")
        back = pq.read_table(str(out))
        assert back.num_rows == 6
        for i, wkb in enumerate(back.column("geometry").to_pylist()):
            geom = shapely.from_wkb(wkb)
            assert geom.is_valid
            assert shapely.get_num_coordinates(geom) < 49
            assert abs(geom.centroid.x - (105.0 + i * 0.05)) < 0.001

    def test_cli_flag(self, tmp_path):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        src = tmp_path / "src.parquet"
        write_geoparquet_table(_wkb_table([self._degree_circle()]), str(src))
        out = tmp_path / "out.parquet"
        result = CliRunner().invoke(
            cli,
            [
                "process",
                "simplify",
                str(src),
                str(out),
                "--tolerance",
                "50",
                "--simplify-crs",
                "auto-utm",
            ],
        )
        assert result.exit_code == 0, result.output
        assert pq.ParquetFile(str(out)).metadata.num_rows == 1
