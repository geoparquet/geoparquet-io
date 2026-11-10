"""The planar operations #1289 left unguarded: the sibling exports, and overview.

#1297 made ``gpio convert geojson`` (and therefore ``gpio pmtiles``) refuse an
input whose geometry column declares non-planar edges: GeoJSON gives its
segments straight Cartesian interpretation, so the same coordinates mean a
different shape once the declaration is gone. GeoPackage, FlatGeobuf, Shapefile
and a WKT CSV say exactly as little about edge interpolation, yet they wrote the
file silently -- a BigQuery GEOGRAPHY export became a planar GeoPackage with no
warning and no record that its rings had been reinterpreted. The refusal is now
one policy (``edges.require_planar_export``) asked by one file-level gatherer
(``geometry_repair.require_planar_export_file``), so the five export formats,
the CLI and the Python API all answer alike.

``gpio process overview`` is the other end of #1289's direction: its rollup is
planar (cells unioned and centroided on the plane) and the planar answer is the
one gpio gives until geodesic support lands, but it was the last such operation
that did not say so. It joins ``add bbox``, ``add kdtree``, ``simplify``,
``reproject`` and ``aggregate`` on ``warn_planar_math_for_file``.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1289
"""

from __future__ import annotations

import json

import geoarrow.pyarrow as ga
import geoarrow.types as gt
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner
from shapely import from_wkt, to_wkb

from geoparquet_io.cli.main import cli
from geoparquet_io.core.edges import require_planar_export
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.format_writers import (
    write_csv,
    write_flatgeobuf,
    write_geopackage,
    write_shapefile,
)
from geoparquet_io.core.geometry_repair import require_planar_export_file

#: A cap around the north pole: the canonical shape whose meaning depends on the
#: edge interpretation (the whole polar region on the sphere, a degenerate
#: collinear ring on the plane).
CAP = "POLYGON((0 80,90 80,180 80,-90 80,0 80))"

#: Each planar-only writer under test, with the label its refusal must name and
#: the output name the CLI/API call is given.
WRITERS = {
    "geopackage": (write_geopackage, "GeoPackage", "out.gpkg"),
    "flatgeobuf": (write_flatgeobuf, "FlatGeobuf", "out.fgb"),
    "shapefile": (write_shapefile, "Shapefile", "out.shp"),
    "csv": (write_csv, "CSV", "out.csv"),
}


def _table(wkt: str = CAP, edges: str | int | None = "spherical", carrier: str = "geo"):
    """One-row table declaring ``edges`` through the geo block or the GeoArrow type."""
    storage = pa.array([to_wkb(from_wkt(wkt))], type=pa.binary())
    if carrier == "native":
        geom_type = ga.wkb().with_edge_type(getattr(gt.EdgeType, str(edges).upper()))
        return pa.table(
            {"id": pa.array([1]), "geometry": pa.ExtensionArray.from_storage(geom_type, storage)}
        )
    column: dict = {"encoding": "WKB", "geometry_types": ["Polygon"]}
    if edges is not None:
        column["edges"] = edges
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": column}}
    return pa.table({"id": pa.array([1]), "geometry": storage}).replace_schema_metadata(
        {b"geo": json.dumps(geo).encode()}
    )


def _source(tmp_path, *, name: str = "cap.parquet", **kwargs):
    path = tmp_path / name
    pq.write_table(_table(**kwargs), path)
    return path


# ---------------------------------------------------------------------------
# The policy itself
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("edges", ["spherical", "vincenty", "thomas", "andoyer", "karney"])
def test_the_owner_refuses_every_known_algorithm_and_names_it(edges):
    with pytest.raises(GeoParquetError, match=edges) as raised:
        require_planar_export("GeoPackage", "geometry", edges)
    assert "GeoPackage" in str(raised.value)
    assert "non-planar" in str(raised.value)


@pytest.mark.parametrize("edges", [None, "planar", "Planar", "bogus", 7, ""])
def test_the_owner_passes_anything_that_is_not_a_known_algorithm(edges):
    """A typo, a number or an explicit planar is no declaration gpio can honor."""
    require_planar_export("GeoPackage", "geometry", edges)


# ---------------------------------------------------------------------------
# The writers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fmt", sorted(WRITERS))
@pytest.mark.parametrize("carrier", ["geo", "native"])
def test_export_refuses_before_writing_anything(tmp_path, fmt, carrier):
    writer, label, out_name = WRITERS[fmt]
    source = _source(tmp_path, carrier=carrier)
    output = tmp_path / out_name
    with pytest.raises(GeoParquetError, match="non-planar") as raised:
        writer(str(source), str(output))
    assert label in str(raised.value)
    assert not output.exists()


@pytest.mark.parametrize("fmt", sorted(WRITERS))
def test_planar_input_still_exports(tmp_path, fmt):
    """The control: nothing changes for a column that does not declare edges."""
    writer, _, out_name = WRITERS[fmt]
    source = _source(tmp_path, wkt="POINT(1 2)", edges=None)
    output = tmp_path / out_name
    writer(str(source), str(output))
    assert output.exists()


@pytest.mark.parametrize("fmt", sorted(WRITERS))
def test_export_refuses_an_overwrite_of_an_existing_output(tmp_path, fmt):
    """A refusal must not cost the user the file they already had."""
    writer, _, out_name = WRITERS[fmt]
    source = _source(tmp_path)
    output = tmp_path / out_name
    output.write_text("keep existing output")
    with pytest.raises(GeoParquetError, match="non-planar"):
        writer(str(source), str(output), overwrite=True)
    assert output.read_text() == "keep existing output"


@pytest.mark.parametrize("edges", ["vincenty", "karney", "thomas", "andoyer"])
def test_ellipsoidal_declarations_are_refused_too(tmp_path, edges):
    source = _source(tmp_path, edges=edges)
    with pytest.raises(GeoParquetError, match=edges):
        write_geopackage(str(source), str(tmp_path / "out.gpkg"))


def test_explicit_planar_block_overrides_a_native_edge_type(tmp_path):
    table = _table("POINT(1 2)", carrier="native")
    table = table.replace_schema_metadata(_table(edges="planar").schema.metadata)
    source = tmp_path / "override.parquet"
    pq.write_table(table, source)
    output = tmp_path / "out.gpkg"
    write_geopackage(str(source), str(output))
    assert output.exists()


def test_a_nonplanar_secondary_column_does_not_block_a_planar_export(tmp_path):
    """The gate judges the exported geometry, not a column carried along beside it.

    The guard is asked directly: the GDAL writers refuse two geometry columns
    for their own reasons, which would hide whether this one let the file past.
    """
    table = _table("POINT(1 2)", edges=None)
    table = table.append_column("secondary", _table(carrier="native").column("geometry"))
    source = tmp_path / "mixed.parquet"
    pq.write_table(table, source)
    require_planar_export_file(str(source), "FlatGeobuf")


def test_csv_without_wkt_exports_a_nonplanar_input(tmp_path):
    """``--no-wkt`` leaves the geometry out of the SELECT: nothing to refuse.

    The attribute-only CSV carries no coordinates at all, so no edge
    interpretation is lost and the export must go through -- while the same
    file with its WKT column still refuses.
    """
    source = _source(tmp_path)
    attributes_only = tmp_path / "attrs.csv"
    write_csv(str(source), str(attributes_only), include_wkt=False)
    assert attributes_only.exists()
    assert "wkt" not in attributes_only.read_text().splitlines()[0]
    with pytest.raises(GeoParquetError, match="non-planar"):
        write_csv(str(source), str(tmp_path / "with_wkt.csv"))


def test_cli_csv_no_wkt_exports_a_nonplanar_input(tmp_path):
    source = _source(tmp_path)
    output = tmp_path / "out.csv"
    result = CliRunner().invoke(cli, ["convert", "csv", str(source), str(output), "--no-wkt"])
    assert result.exit_code == 0, result.output
    assert output.exists()


def test_a_glob_is_checked_past_its_first_file(tmp_path):
    """A planar first file cannot hide a later spherical declaration."""
    pq.write_table(_table("POINT(1 2)", edges=None), tmp_path / "a.parquet")
    pq.write_table(_table(), tmp_path / "b.parquet")
    output = tmp_path / "out.gpkg"
    with pytest.raises(GeoParquetError, match="non-planar"):
        write_geopackage(str(tmp_path / "*.parquet"), str(output))
    assert not output.exists()


# ---------------------------------------------------------------------------
# The two surfaces users reach it through
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fmt", sorted(WRITERS))
def test_cli_refuses_without_writing_the_output(tmp_path, fmt):
    _, _, out_name = WRITERS[fmt]
    source = _source(tmp_path)
    output = tmp_path / out_name
    result = CliRunner().invoke(cli, ["convert", fmt, str(source), str(output)])
    assert result.exit_code != 0, result.output
    assert "non-planar" in result.output
    assert not output.exists()


@pytest.mark.parametrize("fmt", sorted(WRITERS))
def test_python_api_shares_the_guard(tmp_path, fmt):
    """``ops`` stages the table to Parquet and exports it: the same guard runs."""
    from geoparquet_io.api import ops

    _, _, out_name = WRITERS[fmt]
    output = tmp_path / out_name
    convert = getattr(ops, f"convert_to_{fmt}")
    with pytest.raises(GeoParquetError, match="non-planar"):
        convert(_table(), str(output))
    assert not output.exists()


@pytest.mark.parametrize("fmt", sorted(WRITERS))
def test_table_write_shares_the_guard(tmp_path, fmt):
    from geoparquet_io.api.table import Table

    _, _, out_name = WRITERS[fmt]
    output = tmp_path / out_name
    with pytest.raises(GeoParquetError, match="non-planar"):
        Table(_table()).write(str(output))
    assert not output.exists()


# ---------------------------------------------------------------------------
# The planar operation that ran without announcing itself
# ---------------------------------------------------------------------------


class _RollupStarted(RuntimeError):
    """Raised in place of the rollup, to mark the point the warning precedes."""


@pytest.mark.parametrize("edges", ["spherical", None])
def test_overview_says_its_rollup_is_planar_before_it_starts(tmp_path, monkeypatch, caplog, edges):
    """A warning, not a refusal: the rollup goes on, having said what it is.

    The rollup itself is not what is under test here (and a real one reads an
    administrative dataset), so it is replaced by a marker: the warning has to
    be out before ``create_overviews`` opens its connection at all.
    """
    import logging

    from geoparquet_io.core.process.overview import create_overviews, run

    def refuse_to_start(*args, **kwargs):
        raise _RollupStarted

    monkeypatch.setattr(run, "aggregate_connection", refuse_to_start)
    source = _source(tmp_path, name="by_region.parquet", wkt=CAP, edges=edges)
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"), pytest.raises(_RollupStarted):
        create_overviews(str(source), levels="country")
    planar = [r.getMessage() for r in caplog.records if "planar math" in r.getMessage()]
    if edges is None:
        assert planar == []
    else:
        assert len(planar) == 1 and "spherical" in planar[0]
