"""Every write path reads GeoArrow-native geometry as what it is.

The duckdb-kv table path wrapped the primary column in ``ST_GeomFromWKB``
whatever it held, so every native encoding (a coordinate struct, or a list of
them) died with a DuckDB binder error, while the CLI wrote the same file.

#1438: four more sites hand-rolled the same WKB assumption -- the Hilbert sort
(``ST_IsEmpty`` on the raw column), the disk-rewrite strategy (``ST_AsWKB`` on
it), the in-memory and streaming strategies (which labelled a coordinate struct
``encoding: WKB``), and an explicit WKB-carrier version, which declared one over
data it cannot hold. ``geometry_types`` came out ``[]`` on every path.
"""

from pathlib import Path

import geoarrow.pyarrow as ga
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.geo_metadata import sanitized_carried_geo

DATA = Path(__file__).parent / "data"
ENCODINGS = ["point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon"]

#: The one ``geometry_types`` entry each native encoding guarantees: a GeoArrow
#: coordinate struct holds exactly one geometry type, so its encoding names it.
SPEC_TYPE = {
    "point": "Point",
    "linestring": "LineString",
    "polygon": "Polygon",
    "multipoint": "MultiPoint",
    "multilinestring": "MultiLineString",
    "multipolygon": "MultiPolygon",
}

#: Every ``Table.write`` strategy, by its public name.
STRATEGIES = ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"]

#: The versions whose primary carrier is WKB bytes (or a native Parquet GEOMETRY
#: type built from them) and whose spec permits nothing else: none of them can
#: hold a GeoArrow coordinate struct. 1.1 is absent on purpose -- it permits the
#: six GeoArrow encodings beside WKB, so it is satisfied, not refused.
WKB_CARRIER_VERSIONS = ["1.0", "2.0", "parquet-geo-only"]


def _fixture(encoding: str) -> Path:
    return DATA / f"data-{encoding}-encoding_native.parquet"


def _wkb(path: Path) -> list:
    """The primary geometry as WKB, whatever encoding the file stores it in.

    Sorted: ``convert`` Hilbert-orders its output and ``Table.write`` keeps the
    table's order, so the two agree on the rows, not on their order.
    """
    table = pq.read_table(path)
    column = table.column("geometry").combine_chunks()
    encoding = sanitized_carried_geo(table.schema.metadata)["columns"]["geometry"]["encoding"]
    wkb = ga.as_wkb(getattr(ga, encoding.lower())().wrap_array(column))
    return sorted(wkb.storage.to_pylist(), key=lambda value: value or b"")


def _encoding_semantics(path: Path) -> tuple:
    column = sanitized_carried_geo(pq.read_schema(path).metadata)["columns"]["geometry"]
    return column["encoding"].lower(), column.get("crs")


@pytest.mark.parametrize("encoding", ENCODINGS)
def test_table_write_matches_convert(encoding, tmp_path):
    source = _fixture(encoding)
    api_out = tmp_path / "api.parquet"
    cli_out = tmp_path / "cli.parquet"

    gpio.read(source).write(api_out)
    result = CliRunner().invoke(cli, ["convert", "geoparquet", str(source), str(cli_out)])
    assert result.exit_code == 0, result.output

    rows = pq.read_metadata(source).num_rows
    assert pq.read_metadata(api_out).num_rows == rows == pq.read_metadata(cli_out).num_rows
    assert gpio.validate(str(api_out)).verdict() != "failure"
    assert _wkb(api_out) == _wkb(cli_out) == _wkb(source)
    assert _encoding_semantics(api_out) == _encoding_semantics(cli_out) == (encoding, None)


def test_a_head_of_a_native_table_writes(tmp_path):
    out = tmp_path / "head.parquet"
    gpio.read(_fixture("point")).head(3).write(out)
    assert pq.read_metadata(out).num_rows == 3
    assert isinstance(pq.read_schema(out).field("geometry").type, pa.StructType)


def _geo_column(path: Path) -> dict:
    return sanitized_carried_geo(pq.read_schema(path).metadata)["columns"]["geometry"]


def _measured_bbox(encoding: str) -> list:
    """The fixture's own extent, measured off its WKB rather than its block.

    The fixtures declare no ``bbox``, so the expectation comes from the data:
    whatever the write path reads the coordinate struct with, the answer is the
    extent of the WKB those coordinates stand for.
    """
    wkb = pa.array([value for value in _wkb(_fixture(encoding)) if value], pa.binary())
    box = ga.box(ga.as_wkb(wkb)).storage
    return [
        min(pc.struct_field(box, "xmin").to_pylist()),
        min(pc.struct_field(box, "ymin").to_pylist()),
        max(pc.struct_field(box, "xmax").to_pylist()),
        max(pc.struct_field(box, "ymax").to_pylist()),
    ]


def _assert_native_output(out: Path, encoding: str) -> None:
    """The output is the same native encoding, named and measured (#1438)."""
    assert not pa.types.is_binary(pq.read_schema(out).field("geometry").type)
    column = _geo_column(out)
    assert column["encoding"].lower() == encoding
    assert column.get("geometry_types") == [SPEC_TYPE[encoding]]
    # Every path measures the extent: the Arrow-side scans used to refuse a
    # coordinate struct and declare no bbox at all.
    assert column.get("bbox") == _measured_bbox(encoding)
    assert gpio.validate(str(out)).verdict() != "failure"


# -- the write strategies (#1438 item 4) --------------------------------------


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_every_strategy_keeps_a_native_point_column(strategy, tmp_path):
    out = tmp_path / f"{strategy}.parquet"
    gpio.read(_fixture("point")).write(out, write_strategy=strategy)
    assert pq.read_metadata(out).num_rows == pq.read_metadata(_fixture("point")).num_rows
    _assert_native_output(out, "point")
    assert _wkb(out) == _wkb(_fixture("point"))


@pytest.mark.slow
@pytest.mark.parametrize("encoding", ENCODINGS)
@pytest.mark.parametrize("strategy", STRATEGIES)
def test_every_strategy_keeps_every_native_encoding(strategy, encoding, tmp_path):
    out = tmp_path / f"{strategy}-{encoding}.parquet"
    gpio.read(_fixture(encoding)).write(out, write_strategy=strategy)
    _assert_native_output(out, encoding)
    assert _wkb(out) == _wkb(_fixture(encoding))


# -- geometry_types (#1438 item 3) --------------------------------------------


@pytest.mark.parametrize("encoding", ENCODINGS)
def test_measured_geometry_types_survive_a_native_write(encoding, tmp_path):
    """The encoding names the one type the column holds; ``[]`` lost it."""
    api_out = tmp_path / "api.parquet"
    cli_out = tmp_path / "cli.parquet"
    gpio.read(_fixture(encoding)).write(api_out)
    result = CliRunner().invoke(
        cli, ["convert", "geoparquet", str(_fixture(encoding)), str(cli_out)]
    )
    assert result.exit_code == 0, result.output
    assert _geo_column(api_out)["geometry_types"] == [SPEC_TYPE[encoding]]
    assert _geo_column(cli_out)["geometry_types"] == [SPEC_TYPE[encoding]]


# -- an explicit WKB-carrier version (#1438 item 2) ---------------------------


@pytest.mark.parametrize("version", WKB_CARRIER_VERSIONS)
def test_an_explicit_wkb_version_is_refused_by_the_api(version, tmp_path):
    out = tmp_path / "out.parquet"
    with pytest.raises(Exception, match="1.1-geoarrow") as excinfo:
        gpio.read(_fixture("polygon")).write(out, geoparquet_version=version)
    assert "polygon" in str(excinfo.value)
    assert not out.exists()


@pytest.mark.parametrize("version", WKB_CARRIER_VERSIONS)
def test_an_explicit_wkb_version_is_refused_by_the_cli(version, tmp_path):
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli,
        [
            "convert",
            "geoparquet",
            str(_fixture("polygon")),
            str(out),
            "--geoparquet-version",
            version,
        ],
    )
    assert result.exit_code != 0, result.output
    assert "1.1-geoarrow" in result.output


@pytest.mark.parametrize("version", ["1.1", "1.1-geoarrow"])
def test_a_1_1_request_keeps_the_native_encoding(version, tmp_path):
    """1.1 permits the six GeoArrow encodings, so the request is satisfiable.

    It used to be honoured in the block and nowhere else: the struct passed
    through under ``encoding: WKB`` (#1438).
    """
    api_out = tmp_path / "api.parquet"
    cli_out = tmp_path / "cli.parquet"
    gpio.read(_fixture("polygon")).write(api_out, geoparquet_version=version)
    result = CliRunner().invoke(
        cli,
        [
            "convert",
            "geoparquet",
            str(_fixture("polygon")),
            str(cli_out),
            "--geoparquet-version",
            version,
        ],
    )
    assert result.exit_code == 0, result.output
    _assert_native_output(api_out, "polygon")
    _assert_native_output(cli_out, "polygon")
    for out in (api_out, cli_out):
        geo = sanitized_carried_geo(pq.read_schema(out).metadata)
        assert geo["version"] == "1.1.0"


# -- the Hilbert sort and check --fix (#1438 item 1) --------------------------


def test_sort_hilbert_reads_a_native_point_column(tmp_path):
    out = tmp_path / "sorted.parquet"
    result = CliRunner().invoke(cli, ["sort", "hilbert", str(_fixture("point")), str(out)])
    assert result.exit_code == 0, result.output
    assert pq.read_metadata(out).num_rows == pq.read_metadata(_fixture("point")).num_rows
    _assert_native_output(out, "point")


def test_the_table_hilbert_sort_reads_a_native_point_column():
    source = gpio.read(_fixture("point"))
    assert source.sort_hilbert().to_arrow().num_rows == source.to_arrow().num_rows


@pytest.mark.slow
@pytest.mark.parametrize("encoding", ENCODINGS)
def test_sort_hilbert_reads_every_native_encoding(encoding, tmp_path):
    out = tmp_path / "sorted.parquet"
    result = CliRunner().invoke(cli, ["sort", "hilbert", str(_fixture(encoding)), str(out)])
    assert result.exit_code == 0, result.output
    _assert_native_output(out, encoding)
    assert _wkb(out) == _wkb(_fixture(encoding))


def test_check_all_fix_repairs_a_native_point_file(tmp_path):
    source = tmp_path / "in.parquet"
    source.write_bytes(_fixture("point").read_bytes())
    out = tmp_path / "fixed.parquet"
    result = CliRunner().invoke(
        cli, ["check", "all", "--fix", str(source), "--fix-output", str(out)]
    )
    assert result.exit_code == 0, result.output
    assert out.exists()
    _assert_native_output(out, "point")


def test_the_table_fix_repairs_a_native_point_file(tmp_path):
    out = tmp_path / "fixed.parquet"
    gpio.read(_fixture("point")).fix(output=out)
    _assert_native_output(out, "point")


@pytest.mark.slow
@pytest.mark.parametrize("encoding", ENCODINGS)
def test_check_all_fix_repairs_every_native_encoding(encoding, tmp_path):
    source = tmp_path / "in.parquet"
    source.write_bytes(_fixture(encoding).read_bytes())
    out = tmp_path / "fixed.parquet"
    result = CliRunner().invoke(
        cli, ["check", "all", "--fix", str(source), "--fix-output", str(out)]
    )
    assert result.exit_code == 0, result.output
    _assert_native_output(out, encoding)
