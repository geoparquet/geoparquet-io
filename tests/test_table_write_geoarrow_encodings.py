"""Every write path reads GeoArrow-native geometry as what it is.

The duckdb-kv table path wrapped the primary column in ``ST_GeomFromWKB``
whatever it held, so every native encoding (a coordinate struct, or a list of
them) died with a DuckDB binder error, while the CLI wrote the same file.

#1438: four more sites hand-rolled the same WKB assumption -- the Hilbert sort
(``ST_IsEmpty`` on the raw column), the disk-rewrite strategy (``ST_AsWKB`` on
it), the in-memory and streaming strategies (which labelled a coordinate struct
``encoding: WKB``), and an explicit WKB-carrier version, which declared one over
data it cannot hold. ``geometry_types`` came out ``[]`` on every path.

Each case runs over both carrier shapes a GeoArrow column arrives in (see
``CARRIERS``): the bare coordinate struct, and the same struct resolved into its
extension type, which is what gpio's own GeoArrow output reads back as.
"""

from pathlib import Path

import geoarrow.pyarrow as ga
import geoarrow.types as gt
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

#: The two shapes a GeoArrow column reaches a write in. ``bare-struct`` is the
#: coordinate struct alone, as the checked-in fixtures hold it.
#: ``extension-type`` is the same column carrying ``ARROW:extension:name``,
#: which pyarrow resolves into a ``pa.ExtensionType`` once ``geoarrow.pyarrow``
#: is imported -- and gpio imports it, so this is the shape its own
#: ``--geoparquet-version 1.1-geoarrow`` output reads back as. The witness
#: judged the extension type, never its ``storage_type``, so every native
#: encoding read as "not GeoArrow" and the block said ``WKB`` over a struct.
CARRIERS = ["bare-struct", "extension-type"]

#: The versions whose primary carrier is WKB bytes (or a native Parquet GEOMETRY
#: type built from them) and whose spec permits nothing else: none of them can
#: hold a GeoArrow coordinate struct. 1.1 is absent on purpose -- it permits the
#: six GeoArrow encodings beside WKB, so it is satisfied, not refused.
WKB_CARRIER_VERSIONS = ["1.0", "2.0", "parquet-geo-only"]


def _fixture(encoding: str) -> Path:
    return DATA / f"data-{encoding}-encoding_native.parquet"


@pytest.fixture(params=CARRIERS)
def carrier(request) -> str:
    """Which of the two GeoArrow carrier shapes the input under test holds."""
    return request.param


def _native(encoding: str, carrier: str, tmp_path: Path) -> Path:
    """The ``encoding`` fixture, in the requested carrier shape.

    ``extension-type`` is built by wrapping the fixture's own coordinate struct
    in its GeoArrow extension type, which is what writing it tags the field
    with -- the same file ``gpio convert geoparquet ... --geoparquet-version
    1.1-geoarrow`` produces (#1438).
    """
    source = _fixture(encoding)
    if carrier == "bare-struct":
        return source
    table = pq.read_table(source)
    column = getattr(ga, encoding)().wrap_array(table.column("geometry").combine_chunks())
    index = table.schema.get_field_index("geometry")
    wrapped = table.set_column(index, pa.field("geometry", column.type), column)
    out = tmp_path / f"{encoding}-{carrier}.parquet"
    pq.write_table(wrapped, out)
    assert isinstance(pq.read_schema(out).field("geometry").type, pa.ExtensionType)
    return out


def _wkb(path: Path) -> list:
    """The primary geometry as WKB, whatever encoding the file stores it in.

    Sorted: ``convert`` Hilbert-orders its output and ``Table.write`` keeps the
    table's order, so the two agree on the rows, not on their order.
    """
    table = pq.read_table(path)
    column = table.column("geometry").combine_chunks()
    encoding = sanitized_carried_geo(table.schema.metadata)["columns"]["geometry"]["encoding"]
    if not isinstance(column.type, pa.ExtensionType):
        column = getattr(ga, encoding.lower())().wrap_array(column)
    wkb = ga.as_wkb(column)
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
def test_every_strategy_keeps_a_native_point_column(strategy, carrier, tmp_path):
    source = _native("point", carrier, tmp_path)
    out = tmp_path / f"{strategy}.parquet"
    gpio.read(source).write(out, write_strategy=strategy)
    assert pq.read_metadata(out).num_rows == pq.read_metadata(source).num_rows
    _assert_native_output(out, "point")
    assert _wkb(out) == _wkb(source)


@pytest.mark.slow
@pytest.mark.parametrize("encoding", ENCODINGS)
@pytest.mark.parametrize("strategy", STRATEGIES)
def test_every_strategy_keeps_every_native_encoding(strategy, encoding, carrier, tmp_path):
    source = _native(encoding, carrier, tmp_path)
    out = tmp_path / f"{strategy}-{encoding}.parquet"
    gpio.read(source).write(out, write_strategy=strategy)
    _assert_native_output(out, encoding)
    assert _wkb(out) == _wkb(source)


# -- geometry_types (#1438 item 3) --------------------------------------------


@pytest.mark.parametrize("encoding", ENCODINGS)
def test_measured_geometry_types_survive_a_native_write(encoding, carrier, tmp_path):
    """The encoding names the one type the column holds; ``[]`` lost it."""
    source = _native(encoding, carrier, tmp_path)
    api_out = tmp_path / "api.parquet"
    cli_out = tmp_path / "cli.parquet"
    gpio.read(source).write(api_out)
    result = CliRunner().invoke(cli, ["convert", "geoparquet", str(source), str(cli_out)])
    assert result.exit_code == 0, result.output
    assert _geo_column(api_out)["geometry_types"] == [SPEC_TYPE[encoding]]
    assert _geo_column(cli_out)["geometry_types"] == [SPEC_TYPE[encoding]]


# -- an explicit WKB-carrier version (#1438 item 2) ---------------------------


@pytest.mark.parametrize("version", WKB_CARRIER_VERSIONS)
def test_an_explicit_wkb_version_is_refused_by_the_api(version, carrier, tmp_path):
    source = _native("polygon", carrier, tmp_path)
    out = tmp_path / "out.parquet"
    with pytest.raises(Exception, match="1.1-geoarrow") as excinfo:
        gpio.read(source).write(out, geoparquet_version=version)
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
def test_a_1_1_request_keeps_the_native_encoding(version, carrier, tmp_path):
    """1.1 permits the six GeoArrow encodings, so the request is satisfiable.

    It used to be honoured in the block and nowhere else: the struct passed
    through under ``encoding: WKB`` (#1438).
    """
    source = _native("polygon", carrier, tmp_path)
    api_out = tmp_path / "api.parquet"
    cli_out = tmp_path / "cli.parquet"
    gpio.read(source).write(api_out, geoparquet_version=version)
    result = CliRunner().invoke(
        cli,
        [
            "convert",
            "geoparquet",
            str(source),
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


# -- the type-naming rule, and the guards around it ---------------------------


@pytest.mark.parametrize(
    ("encoding", "arrow_type", "expected"),
    [
        ("point", pa.struct([("x", pa.float64()), ("y", pa.float64())]), ["Point"]),
        (
            "point",
            pa.struct([("x", pa.float64()), ("y", pa.float64()), ("z", pa.float64())]),
            ["Point Z"],
        ),
        (
            "linestring",
            pa.list_(pa.struct([("x", pa.float64()), ("y", pa.float64()), ("m", pa.float64())])),
            ["LineString M"],
        ),
        (
            "multipolygon",
            pa.list_(
                pa.list_(
                    pa.list_(
                        pa.struct(
                            [
                                ("x", pa.float64()),
                                ("y", pa.float64()),
                                ("z", pa.float64()),
                                ("m", pa.float64()),
                            ]
                        )
                    )
                )
            ),
            ["MultiPolygon ZM"],
        ),
        # The same coordinate struct resolved into its GeoArrow extension type,
        # which is what pyarrow hands back once geoarrow.pyarrow is imported --
        # and gpio imports it. Judged without unwrapping `storage_type` it read
        # as "not a struct", so the block declared WKB over it (#1438).
        ("polygon", ga.polygon(), ["Polygon"]),
        ("point", ga.point().with_dimensions(gt.Dimensions.XYZM), ["Point ZM"]),
        # The dimension is why the Arrow type is read at all: "LineString" and
        # "LineString ZM" are distinct geometry_types entries.
        ("WKB", pa.binary(), []),
        ("point", pa.binary(), []),
        ("point", pa.struct([("a", pa.float64()), ("b", pa.float64())]), []),
    ],
)
def test_geoarrow_geometry_types_reads_the_encoding_and_the_coordinates(
    encoding, arrow_type, expected
):
    from geoparquet_io.core.geo_metadata import geoarrow_geometry_types

    assert geoarrow_geometry_types(encoding, arrow_type) == expected


def test_a_native_encoding_over_wkb_bytes_keeps_the_measured_types():
    """A block that claims an encoding its column does not hold names no type.

    The encoding is only a type witness while the column really is a coordinate
    struct; over WKB bytes (what a transform leaves a stale block on) the
    measured answer stands.
    """
    from geoparquet_io.core.geo_metadata import (
        GeoBlockOptions,
        SchemaSource,
        compose_geo_block,
    )

    schema = pa.schema([pa.field("geometry", pa.binary())])
    block = compose_geo_block(
        SchemaSource(schema, None),
        GeoBlockOptions("geometry", version="1.1", encoding="point", covering=None),
    )
    assert "geometry_types" not in block["columns"]["geometry"]


@pytest.mark.parametrize("primary", ["geometry", "absent"])
def test_an_unreadable_output_schema_leaves_the_measured_types(primary):
    """A query the composer cannot probe, and a primary the schema lacks (#1438)."""
    from geoparquet_io.core.geo_metadata import _name_geoarrow_geometry_types

    class _Unprobeable:
        @property
        def schema(self):
            raise RuntimeError("this query does not bind")

    class _Other:
        schema = pa.schema([pa.field("geometry", pa.binary())])

    for view in (_Unprobeable(), _Other()):
        col_meta = {"encoding": "point", "geometry_types": []}
        _name_geoarrow_geometry_types(col_meta, view, primary)
        assert col_meta["geometry_types"] == []


def test_a_write_with_no_geometry_column_exports_nothing_as_wkb():
    """``wkb_export_query`` leaves a geometry-less query alone (#1438)."""
    from geoparquet_io.core.write_strategies.base import wkb_export_query

    assert wkb_export_query(None, "SELECT 1 AS id", None) == "SELECT 1 AS id"


# -- the streaming strategy's two 1.1-geoarrow arms ---------------------------


def test_the_streaming_query_path_passes_a_native_column_through(tmp_path):
    """Asked for explicitly, streaming writes a native input as itself (#1438)."""
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli,
        [
            "extract",
            "geoparquet",
            str(_fixture("multipoint")),
            str(out),
            "--write-strategy",
            "streaming",
        ],
    )
    assert result.exit_code == 0, result.output
    _assert_native_output(out, "multipoint")


def test_the_streaming_table_path_still_converts_wkb_to_native(tmp_path):
    """The other arm: WKB in, the encoding the converter resolved out."""
    out = tmp_path / "out.parquet"
    # verbose: the strategy says which encoding it resolved, which is the one
    # line that distinguishes this arm from the pass-through one.
    gpio.read(DATA / "buildings_test.parquet").write(
        out, write_strategy="streaming", geoparquet_version="1.1-geoarrow", verbose=True
    )
    column = _geo_column(out)
    assert column["encoding"] in SPEC_TYPE
    assert not pa.types.is_binary(pq.read_schema(out).field("geometry").type)
