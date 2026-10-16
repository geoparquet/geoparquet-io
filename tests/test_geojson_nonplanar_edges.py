"""GeoJSON cannot express the non-planar edges declared by its input (#1289)."""

import json
from unittest.mock import Mock

import geoarrow.pyarrow as ga
import geoarrow.types as gt
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner
from shapely import from_wkt, to_wkb

from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.geojson_stream import convert_to_geojson
from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

CAP = "POLYGON((0 80,90 80,180 80,-90 80,0 80))"


def _table(wkt=CAP, edges="spherical", carrier="geo"):
    values = [] if wkt == "no rows" else [None if wkt is None else to_wkb(from_wkt(wkt))]
    storage = pa.array(values, type=pa.binary())
    if carrier == "native":
        geom_type = ga.wkb().with_edge_type(getattr(gt.EdgeType, edges.upper()))
        return pa.table({"geometry": pa.ExtensionArray.from_storage(geom_type, storage)})
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": [], "edges": edges}},
    }
    return pa.table({"geometry": storage}).replace_schema_metadata(
        {b"geo": json.dumps(geo).encode()}
    )


@pytest.mark.parametrize("carrier", ["geo", "native"])
@pytest.mark.parametrize("repair", [True, False])
@pytest.mark.parametrize("keep_crs", [True, False])
def test_file_export_refuses_before_truncating_output(tmp_path, carrier, repair, keep_crs):
    """Repair opt-out and keeping the CRS cannot make GeoJSON preserve edge semantics."""
    source = tmp_path / "cap.parquet"
    pq.write_table(_table(carrier=carrier), source)
    output = tmp_path / "out.geojson"
    output.write_text("keep existing output")
    with pytest.raises(GeoParquetError, match="non-planar"):
        convert_to_geojson(str(source), str(output), repair_geometry=repair, keep_crs=keep_crs)
    assert output.read_text() == "keep existing output"


@pytest.mark.parametrize("carrier", ["geo", "native"])
@pytest.mark.parametrize("output_mode", ["file", "seq", "collection"])
def test_stream_export_refuses_before_emitting_output(
    tmp_path, monkeypatch, capsys, carrier, output_mode
):
    """An IPC declaration has the same export contract as a Parquet declaration."""
    table = _table(carrier=carrier)
    monkeypatch.setattr("geoparquet_io.core.streaming.read_arrow_stream", lambda: table)
    output = tmp_path / "out.geojson"
    with pytest.raises(GeoParquetError, match="non-planar"):
        convert_to_geojson(
            "-", str(output) if output_mode == "file" else None, seq=output_mode == "seq"
        )
    assert not output.exists()
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("wkt", ["POINT(1 2)", None, "POLYGON EMPTY", "no rows"])
def test_export_contract_is_based_on_declared_edges(tmp_path, wkt):
    """Even point-only, null and empty inputs do not opt into non-planar conversion."""
    source = tmp_path / "declared.parquet"
    pq.write_table(_table(wkt), source)
    output = tmp_path / "out.geojson"
    with pytest.raises(GeoParquetError, match="non-planar"):
        convert_to_geojson(str(source), str(output))
    assert not output.exists()


@pytest.mark.parametrize("edges", ["vincenty", "karney", "thomas", "andoyer"])
def test_ellipsoidal_declarations_are_also_refused(tmp_path, edges):
    source = tmp_path / "cap.parquet"
    pq.write_table(_table(edges=edges), source)
    with pytest.raises(GeoParquetError, match=edges):
        convert_to_geojson(str(source))


@pytest.mark.parametrize(
    "options", [{}, {"src_crs": "EPSG:4326"}, {"chunks": "2x2", "max_zoom": 5}]
)
def test_pmtiles_refuses_before_starting_preprocessing(tmp_path, monkeypatch, options):
    """Preprocessing must not erase native edges before the GeoJSON export guard."""
    source = tmp_path / "native.parquet"
    pq.write_table(_table(carrier="native"), source)
    output = tmp_path / "out.pmtiles"
    output.write_bytes(b"keep existing archive")
    probe = Mock(return_value=True)
    run = Mock()
    chunk = Mock()
    monkeypatch.setattr("geoparquet_io.core.pmtiles._check_tippecanoe", probe)
    monkeypatch.setattr("geoparquet_io.core.pmtiles._run_pipeline", run)
    monkeypatch.setattr("geoparquet_io.core.pmtiles_chunks.create_pmtiles_chunked", chunk)
    with pytest.raises(GeoParquetError, match="non-planar"):
        create_pmtiles_from_geoparquet(str(source), str(output), force=True, **options)
    probe.assert_called_once()
    run.assert_not_called()
    chunk.assert_not_called()
    assert output.read_bytes() == b"keep existing archive"


@pytest.mark.parametrize("api", ["ops", "table", "write"])
def test_python_api_uses_same_export_guard(tmp_path, api):
    from geoparquet_io.api import ops
    from geoparquet_io.api.table import Table

    table = _table()
    output = tmp_path / "out.geojson"
    with pytest.raises(GeoParquetError, match="non-planar"):
        if api == "ops":
            ops.convert_to_geojson(table, str(output))
        elif api == "table":
            Table(table).to_geojson(str(output))
        else:
            # Format writers wrap the exporter error as a GeoParquetError.
            Table(table).write(str(output))
    assert not output.exists()


def test_cli_rejects_without_writing_geojson(tmp_path):
    source = tmp_path / "cap.parquet"
    pq.write_table(_table(), source)
    result = CliRunner().invoke(cli, ["convert", "geojson", str(source)])
    assert result.exit_code != 0
    assert "non-planar" in result.output
    assert '"Feature"' not in result.output


@pytest.mark.parametrize("edges", ["planar", "Planar", "bogus", 7])
def test_planar_and_unrecognized_declarations_keep_existing_behavior(tmp_path, edges):
    source = tmp_path / "point.parquet"
    pq.write_table(_table("POINT(1.123456789 2)", edges=edges), source)
    output = tmp_path / "out.geojson"
    convert_to_geojson(str(source), str(output), precision=3)
    geometry = json.loads(output.read_text())["features"][0]["geometry"]
    assert geometry == {"type": "Point", "coordinates": [1.123, 2.0]}


@pytest.mark.parametrize("source_mode", ["file", "stream", "pmtiles"])
def test_unused_nonplanar_secondary_does_not_block_planar_export(
    tmp_path, monkeypatch, source_mode
):
    """The gate checks the exported geometry, not a secondary used only as a property."""
    table = _table("POINT(1 2)", edges="planar")
    secondary = _table("POINT(3 4)", carrier="native").column("geometry")
    table = table.append_column("secondary", secondary)
    output = tmp_path / "out.geojson"
    if source_mode == "stream":
        monkeypatch.setattr("geoparquet_io.core.streaming.read_arrow_stream", lambda: table)
        source = "-"
    else:
        path = tmp_path / "mixed.parquet"
        pq.write_table(table, path)
        source = str(path)
    if source_mode == "pmtiles":
        run = Mock()
        monkeypatch.setattr("geoparquet_io.core.pmtiles._check_tippecanoe", lambda: True)
        monkeypatch.setattr("geoparquet_io.core.pmtiles._run_pipeline", run)
        create_pmtiles_from_geoparquet(source, str(output))
        run.assert_called_once()
    else:
        convert_to_geojson(source, str(output))
        assert json.loads(output.read_text())["features"][0]["geometry"]["type"] == "Point"


@pytest.mark.parametrize("source_mode", ["file", "stream"])
def test_explicit_planar_block_overrides_native_edge_type(tmp_path, monkeypatch, source_mode):
    table = _table("POINT(1 2)", carrier="native")
    table = table.replace_schema_metadata(_table(edges="planar").schema.metadata)
    output = tmp_path / "out.geojson"
    if source_mode == "stream":
        monkeypatch.setattr("geoparquet_io.core.streaming.read_arrow_stream", lambda: table)
        source = "-"
    else:
        path = tmp_path / "override.parquet"
        pq.write_table(table, path)
        source = str(path)
    convert_to_geojson(source, str(output))
    assert json.loads(output.read_text())["features"][0]["geometry"]["type"] == "Point"


def test_metadata_only_geoarrow_field_is_refused(tmp_path, monkeypatch):
    table = _table().replace_schema_metadata(None)
    field = pa.field(
        "geometry",
        pa.binary(),
        metadata={
            b"ARROW:extension:name": b"geoarrow.wkb",
            b"ARROW:extension:metadata": b'{"edges":"spherical"}',
        },
    )
    table = table.cast(pa.schema([field]))
    monkeypatch.setattr("geoparquet_io.core.streaming.read_arrow_stream", lambda: table)
    with pytest.raises(GeoParquetError, match="spherical"):
        convert_to_geojson("-", str(tmp_path / "out.geojson"))


@pytest.mark.parametrize("nonplanar_source", ["aggregate", "features"])
def test_pyramid_refuses_before_building_overviews(tmp_path, monkeypatch, nonplanar_source):
    from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

    source = tmp_path / "aggregate.parquet"
    features = tmp_path / "features.parquet"
    pq.write_table(
        _table(edges="spherical" if nonplanar_source == "aggregate" else "planar"), source
    )
    pq.write_table(_table(), features)
    plan = Mock()
    monkeypatch.setattr("geoparquet_io.core.pmtiles_pyramid._check_tippecanoe", lambda: True)
    monkeypatch.setattr("geoparquet_io.core.pmtiles_pyramid._check_tile_join", lambda: True)
    monkeypatch.setattr("geoparquet_io.core.pmtiles_pyramid._detect_and_plan", plan)
    output = tmp_path / "out.pmtiles"
    with pytest.raises(GeoParquetError, match="non-planar"):
        create_pmtiles_pyramid(
            str(source),
            str(output),
            include_features=True,
            features_source=str(features),
            max_zoom=5,
        )
    plan.assert_not_called()
    assert not output.exists()


def test_pmtiles_missing_binary_keeps_existing_error_precedence(monkeypatch):
    from geoparquet_io.core.pmtiles import TippecanoeNotFoundError

    guard = Mock()
    monkeypatch.setattr("geoparquet_io.core.pmtiles._check_tippecanoe", lambda: False)
    monkeypatch.setattr("geoparquet_io.core.pmtiles.require_planar_geojson_file", guard)
    with pytest.raises(TippecanoeNotFoundError):
        create_pmtiles_from_geoparquet("missing.parquet", "out.pmtiles")
    guard.assert_not_called()


def test_pmtiles_preflight_receives_explicit_profile(monkeypatch):
    """The in-process metadata check shares the profile sent to gpio children."""
    from geoparquet_io.core.duckdb_utils import get_active_s3_config

    seen = []

    def collect(path):
        seen.append((path, get_active_s3_config()))
        return {}

    monkeypatch.setattr("geoparquet_io.core.pmtiles._check_tippecanoe", lambda: True)
    monkeypatch.setattr("geoparquet_io.core.pmtiles._run_pipeline", Mock())
    monkeypatch.setattr("geoparquet_io.core.geojson_stream.collect_nonplanar_edges", collect)
    create_pmtiles_from_geoparquet(
        "s3://fake-bucket/input.parquet", "out.pmtiles", profile="test-only"
    )
    assert seen == [
        ("s3://fake-bucket/input.parquet", {"profile": "test-only", "profile_from_env": False})
    ]


@pytest.mark.parametrize("suffix", [".parquet", ".pq", ".PARQUET"])
def test_glob_checks_more_than_the_first_file(tmp_path, suffix):
    """A planar first file cannot hide a later spherical declaration."""
    pq.write_table(_table("POINT(1 2)", edges="planar"), tmp_path / f"a{suffix}")
    pq.write_table(_table(), tmp_path / f"b{suffix}")
    output = tmp_path / "out.geojson"
    output.write_text("keep existing output")
    with pytest.raises(GeoParquetError, match="non-planar"):
        convert_to_geojson(str(tmp_path / f"*{suffix}"), str(output), keep_crs=True)
    assert output.read_text() == "keep existing output"


def test_remote_glob_checks_each_matched_footer(monkeypatch):
    """DuckDB expands the remote glob under the caller's scoped profile."""
    from geoparquet_io.core.duckdb_utils import get_active_s3_config
    from geoparquet_io.core.geojson_stream import require_planar_geojson_file

    con = Mock()
    con.execute.return_value.fetchall.return_value = [
        ("s3://fake/a.parquet",),
        ("s3://fake/b.parquet",),
    ]
    monkeypatch.setattr("geoparquet_io.core.duckdb_utils.get_duckdb_connection", lambda **kw: con)
    seen = []

    def collect(path):
        seen.append((path, get_active_s3_config()))
        return {} if path.endswith("a.parquet") else {"geometry": "spherical"}

    monkeypatch.setattr("geoparquet_io.core.geojson_stream.collect_nonplanar_edges", collect)
    with pytest.raises(GeoParquetError, match="spherical"):
        require_planar_geojson_file("s3://fake/*.parquet", "geometry", profile="test-only")
    assert [path for path, _ in seen] == ["s3://fake/a.parquet", "s3://fake/b.parquet"]
    assert all(config["profile"] == "test-only" for _, config in seen)
    con.execute.assert_called_once_with("SELECT file FROM glob(?)", ["s3://fake/*.parquet"])
    con.close.assert_called_once()


@pytest.mark.parametrize(
    "options",
    [
        {"include_cols": "id"},
        {"src_crs": "EPSG:4326"},
        {"src_crs": ""},
        {"chunks": "2x2", "max_zoom": 5},
    ],
)
def test_pmtiles_checks_primary_selected_by_preprocessing(tmp_path, monkeypatch, options):
    """Extract and reproject select the declared primary, not the first standard name."""
    table = _table("POINT(1 2)", edges="planar").append_column("id", pa.array([1]))
    table = table.append_column("other", _table().column("geometry"))
    geo = json.loads(table.schema.metadata[b"geo"])
    geo["primary_column"] = "other"
    geo["columns"]["other"] = {
        "encoding": "WKB",
        "geometry_types": ["Polygon"],
        "edges": "spherical",
    }
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    source = tmp_path / "selected.parquet"
    pq.write_table(table, source)
    output = tmp_path / "out.pmtiles"
    output.write_bytes(b"keep existing archive")
    run = Mock()
    chunk = Mock()
    monkeypatch.setattr("geoparquet_io.core.pmtiles._check_tippecanoe", lambda: True)
    monkeypatch.setattr("geoparquet_io.core.pmtiles._run_pipeline", run)
    monkeypatch.setattr("geoparquet_io.core.pmtiles_chunks.create_pmtiles_chunked", chunk)
    with pytest.raises(GeoParquetError, match="non-planar"):
        create_pmtiles_from_geoparquet(str(source), str(output), force=True, **options)
    run.assert_not_called()
    chunk.assert_not_called()
    assert output.read_bytes() == b"keep existing archive"


def test_pmtiles_stdin_refused_before_starting_subprocesses(tmp_path, monkeypatch):
    """The source must be inspectable before a tiler can overwrite its archive."""
    output = tmp_path / "out.pmtiles"
    output.write_bytes(b"keep existing archive")
    probe = Mock(return_value=True)
    run = Mock()
    monkeypatch.setattr("geoparquet_io.core.pmtiles._check_tippecanoe", probe)
    monkeypatch.setattr("geoparquet_io.core.pmtiles._run_pipeline", run)
    with pytest.raises(GeoParquetError, match="save.*GeoParquet"):
        create_pmtiles_from_geoparquet("-", str(output), force=True)
    probe.assert_not_called()
    run.assert_not_called()
    assert output.read_bytes() == b"keep existing archive"


@pytest.mark.parametrize("columns", [["geometry"], "broken"])
def test_malformed_block_cannot_bypass_native_pmtiles_preflight(tmp_path, monkeypatch, columns):
    """Preserve readable native evidence even when the optional geo block is malformed."""
    table = _table(carrier="native")
    geo = {"version": "2.0.0", "primary_column": "geometry", "columns": columns}
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    source = tmp_path / "malformed.parquet"
    pq.write_table(table, source)
    output = tmp_path / "out.pmtiles"
    output.write_bytes(b"keep existing archive")
    run = Mock()
    monkeypatch.setattr("geoparquet_io.core.pmtiles._check_tippecanoe", lambda: True)
    monkeypatch.setattr("geoparquet_io.core.pmtiles._run_pipeline", run)
    with pytest.raises(GeoParquetError, match="spherical"):
        create_pmtiles_from_geoparquet(
            str(source), str(output), include_cols="geometry", force=True
        )
    run.assert_not_called()
    assert output.read_bytes() == b"keep existing archive"
