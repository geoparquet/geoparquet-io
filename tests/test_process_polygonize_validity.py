"""Polygonize's self-touching rings go through gpio's repair owner (#1301).

Where a class region pinches to a single shared pixel corner, contourrs closes
it as one ring that revisits that vertex. rasterio and GDAL emit the enclosed
part as an interior ring of the outer polygon instead, so from the same raster
they produce 100% OGC-valid output and gpio produced 1.7-2.9% invalid --
measured as 29 / 1,354 / 5,776 invalid polygons at 1024² / 4096² / 8192², every
one of them ``Ring Self-intersection``. Nothing else differed: polygon count
and per-class count and area matched both references exactly.

Polygonize was the one geometry-producing command that never consulted
``core/geometry_repair.py``, which convert, extract and pmtiles all use and
which repairs by default. These tests pin that it now does, that the default
matches the rest of gpio, and that ``--no-repair-geometry`` still reports the
count -- the cheap half of the work.

The fixture is a hand-built self-touching ring, not a real raster, so these run
on every CI leg (contourrs has no wheels below Python 3.12) and keep passing
when contourrs fixes its ring assembly: the fixture stays invalid either way.
"""

import json
import logging
import sys
import types

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely

from geoparquet_io.core.process.raster.polygonize import polygonize_array, polygonize_file

#: One of the 5,776 invalid polygons contourrs produced from the 8192²
#: WorldCover crop, copied verbatim as WKB rather than hand-built: a synthetic
#: self-touching ring repairs into a MultiPolygon or a GeometryCollection,
#: while the real pixel-corner pinch stays a Polygon and gains one interior
#: ring. Keeping the real bytes is what makes the repair assertions mean
#: something, and it outlives the scratch file the benchmark wrote.
#: 19 coordinates, ``Ring Self-intersection[6.99216666666667 46.9055]``.
_SELF_TOUCHING_WKB = bytes.fromhex(
    "01030000000100000013000000184b7eb1e4f71b409134a5e2ec734740184b7eb1e4f71b40"
    "133c9827ea734740184b7eb1e4f71b4096438b6ce7734740050fe689faf71b4096438b6ce7"
    "734740050fe689faf71b40133c9827ea734740f2d24d6210f81b40133c9827ea734740f2d2"
    "4d6210f81b4096438b6ce7734740050fe689faf71b4096438b6ce7734740050fe689faf71b"
    "40184b7eb1e4734740f2d24d6210f81b40184b7eb1e4734740de96b53a26f81b40184b7eb1"
    "e4734740de96b53a26f81b4096438b6ce7734740cb5a1d133cf81b4096438b6ce7734740cb"
    "5a1d133cf81b40133c9827ea734740cb5a1d133cf81b409134a5e2ec734740de96b53a26f8"
    "1b409134a5e2ec734740f2d24d6210f81b409134a5e2ec734740050fe689faf71b409134a5"
    "e2ec734740184b7eb1e4f71b409134a5e2ec734740"
)


def _self_touching():
    """The real invalid polygon: valid WKB, not OGC-valid."""
    return shapely.from_wkb(_SELF_TOUCHING_WKB)


def _valid_square(x=10.0):
    """A plain valid polygon, far from the fixture's own coordinates."""
    return shapely.box(x, 0, x + 1, 1)


def _geo_meta():
    return {
        b"geo": json.dumps(
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Polygon"]}},
            }
        ).encode()
    }


def _invalid_count(table, column="geometry"):
    geoms = shapely.from_wkb(table.column(column).to_numpy(zero_copy_only=False))
    return int((~shapely.is_valid(geoms)).sum())


class TestTheFixtureItself:
    """If the fixture stops being invalid, every test below is vacuous."""

    def test_the_ring_is_valid_wkb_but_not_ogc_valid(self):
        geom = _self_touching()
        assert not geom.is_valid
        assert "Ring Self-intersection" in shapely.is_valid_reason(geom)
        # Round-trips through WKB, so it is the shape of a real output row.
        assert not shapely.from_wkb(shapely.to_wkb(geom)).is_valid

    def test_repair_fixes_it_without_changing_the_area_or_type(self):
        geom = _self_touching()
        fixed = shapely.make_valid(geom)
        assert fixed.is_valid
        assert fixed.area == pytest.approx(geom.area)
        assert fixed.geom_type == "Polygon"
        # The self-touch becomes its own interior ring -- what GDAL emits.
        assert len(fixed.interiors) > len(geom.interiors)

    def test_the_square_is_valid(self):
        assert _valid_square().is_valid


@pytest.fixture
def fake_contourrs(monkeypatch):
    """A contourrs stand-in whose output holds one self-touching ring.

    ``require_contourrs`` imports the module on every call, so a ``sys.modules``
    entry decides what the raster code sees, installed or not (the pattern in
    tests/test_process_raster_without_contourrs.py).
    """
    module = types.ModuleType("contourrs")
    module.rows = [_self_touching(), _valid_square()]

    def shapes_arrow(array, **kwargs):
        geom_field = pa.field(
            "geometry",
            pa.binary(),
            nullable=False,
            metadata={
                b"ARROW:extension:name": b"geoarrow.wkb",
                b"ARROW:extension:metadata": b"{}",
            },
        )
        schema = pa.schema(
            [geom_field, pa.field("value", pa.float64(), nullable=False)],
            metadata=_geo_meta(),
        )
        return pa.table(
            {
                "geometry": [shapely.to_wkb(g) for g in module.rows],
                "value": [float(i) for i in range(len(module.rows))],
            },
            schema=schema,
        )

    module.shapes_arrow = shapes_arrow
    monkeypatch.setitem(sys.modules, "contourrs", module)
    return module


def _array():
    import numpy as np

    return np.zeros((2, 2), dtype="uint8")


def _raster_band():
    """A RasterBand the stubbed reader hands back; contourrs is faked anyway."""
    from geoparquet_io.core.process.raster.reader import RasterBand

    return RasterBand(
        array=_array(),
        mask=None,
        nodata=None,
        transform=(1.0, 0.0, 0.0, 0.0, -1.0, 0.0),
        crs=None,
    )


class TestPolygonizeArray:
    def test_repairs_by_default(self, fake_contourrs, caplog):
        with caplog.at_level(logging.WARNING):
            table = polygonize_array(_array())
        assert _invalid_count(table) == 0
        assert "1" in caplog.text and "invalid" in caplog.text.lower()

    def test_the_repair_keeps_every_row_and_the_class_column(self, fake_contourrs):
        table = polygonize_array(_array())
        assert table.num_rows == 2
        assert table.column("value").to_pylist() == [0.0, 1.0]

    def test_the_repair_preserves_area_and_type(self, fake_contourrs):
        table = polygonize_array(_array())
        repaired = shapely.from_wkb(table.column("geometry")[0].as_py())
        assert repaired.is_valid
        assert repaired.area == pytest.approx(_self_touching().area)
        assert repaired.geom_type == "Polygon"

    def test_valid_rows_stay_byte_identical(self, fake_contourrs):
        """The owner patches only broken rows; untouched WKB must not be rewritten."""
        table = polygonize_array(_array())
        assert table.column("geometry")[1].as_py() == shapely.to_wkb(_valid_square())

    def test_opting_out_counts_but_does_not_repair(self, fake_contourrs, caplog):
        with caplog.at_level(logging.WARNING):
            table = polygonize_array(_array(), repair_geometry=False)
        assert _invalid_count(table) == 1
        assert "1" in caplog.text
        assert "no-repair-geometry" in caplog.text

    def test_a_fully_valid_output_says_nothing(self, fake_contourrs, caplog):
        fake_contourrs.rows = [_valid_square(), _valid_square(20.0)]
        with caplog.at_level(logging.WARNING):
            table = polygonize_array(_array())
        assert _invalid_count(table) == 0
        assert "invalid" not in caplog.text.lower()

    def test_the_geo_block_survives_the_repair(self, fake_contourrs):
        table = polygonize_array(_array())
        geo = json.loads(table.schema.metadata[b"geo"])
        assert geo["primary_column"] == "geometry"
        assert geo["columns"]["geometry"]["encoding"] == "WKB"

    def test_a_renamed_class_column_is_untouched(self, fake_contourrs):
        table = polygonize_array(_array(), value_column="landcover")
        assert "landcover" in table.column_names
        assert _invalid_count(table) == 0

    def test_the_values_filter_runs_before_the_repair(self, fake_contourrs, caplog):
        """A row the filter drops must not be counted, warned about or repaired."""
        with caplog.at_level(logging.WARNING):
            table = polygonize_array(_array(), values=[1.0])
        assert table.num_rows == 1
        assert "invalid" not in caplog.text.lower()

    def test_an_empty_output_is_not_a_problem(self, fake_contourrs):
        fake_contourrs.rows = []
        table = polygonize_array(_array())
        assert table.num_rows == 0


class TestPolygonizeFile:
    @pytest.fixture
    def stub_reader(self, monkeypatch):
        from geoparquet_io.core.process.raster import polygonize as mod

        monkeypatch.setattr(mod, "read_band", lambda *a, **k: _raster_band())

    def test_the_written_file_is_repaired(self, fake_contourrs, stub_reader, tmp_path):
        out = tmp_path / "out.parquet"
        polygonize_file("in.tif", str(out))
        assert _invalid_count(pq.read_table(str(out))) == 0

    def test_opting_out_writes_the_invalid_ring(self, fake_contourrs, stub_reader, tmp_path):
        out = tmp_path / "out.parquet"
        polygonize_file("in.tif", str(out), repair_geometry=False)
        assert _invalid_count(pq.read_table(str(out))) == 1


class TestCliAndApi:
    def _help(self, args):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        result = CliRunner().invoke(cli, args + ["--help"])
        assert result.exit_code == 0, result.output
        return result.output

    def test_polygonize_exposes_the_house_flag(self):
        text = self._help(["process", "polygonize"])
        assert "--repair-geometry / --no-repair-geometry" in " ".join(text.split("\n"))

    def test_contour_does_not_grow_the_flag(self):
        """Contour output measured 100% valid on every test DEM; the scan would be noise."""
        assert "repair-geometry" not in self._help(["process", "contour"])

    def test_the_cli_passes_the_flag_through(self, fake_contourrs, stub_reader_cli, tmp_path):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        out = tmp_path / "out.parquet"
        result = CliRunner().invoke(
            cli,
            ["process", "polygonize", "in.tif", str(out), "--no-repair-geometry"],
        )
        assert result.exit_code == 0, result.output
        assert _invalid_count(pq.read_table(str(out))) == 1

    @pytest.fixture
    def stub_reader_cli(self, monkeypatch):
        from geoparquet_io.core.process.raster import polygonize as mod

        monkeypatch.setattr(mod, "read_band", lambda *a, **k: _raster_band())

    def test_ops_polygonize_repairs_by_default(self, fake_contourrs, monkeypatch):
        from geoparquet_io.api import ops
        from geoparquet_io.core.process.raster import polygonize as mod

        monkeypatch.setattr(mod, "read_band", lambda *a, **k: _raster_band())
        assert _invalid_count(ops.polygonize("in.tif")) == 0
        assert _invalid_count(ops.polygonize("in.tif", repair_geometry=False)) == 1

    def test_table_polygonize_takes_the_flag(self, fake_contourrs, monkeypatch):
        import geoparquet_io as gpio
        from geoparquet_io.core.process.raster import polygonize as mod

        monkeypatch.setattr(mod, "read_band", lambda *a, **k: _raster_band())
        assert _invalid_count(gpio.Table.polygonize("in.tif").to_arrow()) == 0
        assert (
            _invalid_count(gpio.Table.polygonize("in.tif", repair_geometry=False).to_arrow()) == 1
        )
