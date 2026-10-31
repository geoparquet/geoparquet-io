"""One bbox parser (#1367, concept 15): every --bbox and bbox= gives the same verdict.

Three parsers answered "what bbox did the user ask for" (``core/extract.parse_bbox``,
``format_writers._normalize_bbox``, ``preview._validated_bbox``) and three CLI
commands parsed the string inline. They disagreed: ``'nan,0,1,1'`` reached
DuckDB as the SQL token ``nan`` from ``gpio extract`` and was rejected by
``gpio convert``; ``gpio extract arcgis/wfs/carto`` sent swapped and NaN corners
to the service. ``core/bbox_parse.parse_bbox`` is the one parser now, and the
table below is the one verdict for every entry point.
"""

from __future__ import annotations

import math

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.bbox_parse import parse_bbox
from geoparquet_io.core.exceptions import InvalidParameterError

#: (value, parsed tuple or None for "rejected")
TEXT_CASES = [
    ("0,0,1,1", (0.0, 0.0, 1.0, 1.0)),
    (" -122.5 , 37.5 , -122.0 , 38.0 ", (-122.5, 37.5, -122.0, 38.0)),
    ("-180,-90,180,90", (-180.0, -90.0, 180.0, 90.0)),
    ("0,0,0,0", (0.0, 0.0, 0.0, 0.0)),
    ("1,0,0,1", (1.0, 0.0, 0.0, 1.0)),  # xmin > xmax: the antimeridian wrap
    ("170,-10,-170,10", (170.0, -10.0, -170.0, 10.0)),
    ("0,1,1,0", None),  # ymin > ymax
    ("500000,0,400000,1", None),  # xmin > xmax outside longitudes: swapped, not a wrap
    ("nan,0,1,1", None),
    ("0,0,inf,1", None),
    ("0,0,1", None),
    ("0,0,1,1,2", None),
    ("a,b,c,d", None),
    ("0;0;1;1", None),
]
SEQUENCE_CASES = [
    ([0, 0, 1, 1], (0.0, 0.0, 1.0, 1.0)),
    ((0.5, -1, 2, 3.25), (0.5, -1.0, 2.0, 3.25)),
    ([1, 0, 0, 1], (1.0, 0.0, 0.0, 1.0)),
    ([0, 1, 1, 0], None),
    ([math.nan, 0, 1, 1], None),
    ([0, 0, 1], None),
    (["a", 0, 1, 1], None),
    ([None, 0, 1, 1], None),
]
IDS = [repr(v) for v, _ in TEXT_CASES]


@pytest.mark.parametrize(("value", "expected"), TEXT_CASES + SEQUENCE_CASES)
def test_the_owner_verdict(value, expected):
    if expected is None:
        with pytest.raises(InvalidParameterError) as exc:
            parse_bbox(value)
        assert exc.value.param_name == "bbox"
    else:
        assert parse_bbox(value) == expected


def test_the_parameter_name_is_the_callers():
    with pytest.raises(InvalidParameterError) as exc:
        parse_bbox("0,0,1", param="query_bbox")
    assert exc.value.param_name == "query_bbox"


def _verdict(result) -> bool:
    """True when the command accepted the bbox."""
    if result.exit_code == 0:
        return True
    assert "bbox" in result.output.lower(), result.output
    assert "Traceback" not in result.output
    return False


@pytest.mark.parametrize(("value", "expected"), TEXT_CASES, ids=IDS)
def test_extract_agrees(buildings_test_file, tmp_path, value, expected):
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(cli, ["extract", buildings_test_file, str(out), "--bbox", value])
    assert _verdict(result) is (expected is not None), result.output


@pytest.mark.parametrize(("value", "expected"), TEXT_CASES, ids=IDS)
def test_convert_agrees(buildings_test_file, tmp_path, value, expected):
    out = tmp_path / "out.csv"
    result = CliRunner().invoke(
        cli, ["convert", "csv", buildings_test_file, str(out), "--bbox", value]
    )
    assert _verdict(result) is (expected is not None), result.output


SERVICES = {
    "arcgis": ["extract", "arcgis", "https://gpio.invalid/arcgis/rest/services/x/FeatureServer/0"],
    "wfs": ["extract", "wfs", "https://gpio.invalid/wfs", "layer"],
    "carto": ["extract", "carto", "https://gpio.invalid/carto", "table"],
}
REJECTED = [value for value, expected in TEXT_CASES if expected is None]


@pytest.mark.parametrize("service", sorted(SERVICES))
@pytest.mark.parametrize("value", REJECTED)
def test_the_service_extracts_refuse_what_the_owner_refuses(service, value, tmp_path, monkeypatch):
    """Refused before any request: name resolution is replaced by a tripwire."""
    import socket

    def no_network(*args, **kwargs):
        raise AssertionError("the bbox should have been refused before any request")

    monkeypatch.setattr(socket, "getaddrinfo", no_network)
    args = [*SERVICES[service], str(tmp_path / "out.parquet"), "--bbox", value]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code != 0
    assert "bbox" in result.output.lower(), result.output
    assert "should have been refused" not in str(result.exception or "")


@pytest.mark.parametrize(("value", "expected"), TEXT_CASES + SEQUENCE_CASES)
def test_preview_bbox_filter_agrees(buildings_test_file, value, expected):
    from geoparquet_io.api import ops

    if expected is None:
        with pytest.raises(InvalidParameterError):
            ops.preview_bbox_filter(buildings_test_file, value)
    else:
        ops.preview_bbox_filter(buildings_test_file, value)


# --- The antimeridian wrap (#1367 audit ruling) ---------------------------
#
# ``xmin > xmax`` is the antimeridian wrap (RFC 7946 5.2), the one reading
# ``validate`` and ``check_bbox_values`` already give it. Base refused such a
# bbox outright; now every SQL filter honours it through BBox.pieces, and a
# service that cannot send one says so with AntimeridianBBoxError.


def test_the_wrap_predicate_and_pieces():
    from geoparquet_io.core.bbox_parse import BBox

    plain = parse_bbox("0,0,1,1")
    assert isinstance(plain, BBox)
    assert not plain.crosses_antimeridian()
    assert plain.pieces() == (plain,)
    wrapped = parse_bbox("170,-10,-170,10")
    assert wrapped.crosses_antimeridian()
    assert wrapped.pieces() == (BBox(170.0, -10.0, 180.0, 10.0), BBox(-180.0, -10.0, -170.0, 10.0))


def test_non_finite_corners_are_still_refused_when_wrapped():
    with pytest.raises(InvalidParameterError):
        parse_bbox("170,-10,-inf,10")


@pytest.fixture(scope="module")
def dateline_points(tmp_path_factory):
    """Three points: two either side of the antimeridian, one at Greenwich."""
    import pyarrow as pa

    root = tmp_path_factory.mktemp("dateline")
    shapely = pytest.importorskip("shapely")
    pts = [shapely.Point(175, 0), shapely.Point(-175, 0), shapely.Point(0, 0)]
    from geoparquet_io.core.write_funnels import write_geoparquet_table

    plain = root / "plain.parquet"
    write_geoparquet_table(
        pa.table({"name": ["east", "west", "zero"], "geometry": shapely.to_wkb(pts).tolist()}),
        str(plain),
        geometry_column="geometry",
    )
    with_bbox = root / "bbox.parquet"
    result = CliRunner().invoke(cli, ["add", "bbox", str(plain), str(with_bbox)])
    assert result.exit_code == 0, result.output
    return {"plain": str(plain), "bbox": str(with_bbox)}


@pytest.mark.parametrize("variant", ["plain", "bbox"])
def test_extract_keeps_both_sides_of_a_wrapped_bbox(dateline_points, variant, tmp_path):
    import pyarrow.parquet as pq

    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli, ["extract", dateline_points[variant], str(out), "--bbox", "170,-10,-170,10"]
    )
    assert result.exit_code == 0, result.output
    assert sorted(pq.read_table(out).column("name").to_pylist()) == ["east", "west"]


def test_convert_keeps_both_sides_of_a_wrapped_bbox(dateline_points, tmp_path):
    out = tmp_path / "out.csv"
    result = CliRunner().invoke(
        cli, ["convert", "csv", dateline_points["plain"], str(out), "--bbox", "170,-10,-170,10"]
    )
    assert result.exit_code == 0, result.output
    text = out.read_text()
    assert "east" in text and "west" in text and "zero" not in text


def test_preview_counts_a_wrapped_bbox(dateline_points):
    from geoparquet_io.api import ops

    preview = ops.preview_bbox_filter(dateline_points["bbox"], "170,-10,-170,10")
    if preview.has_estimate:
        assert preview.est_rows >= 2


def test_preview_intersects_either_lobe():
    from geoparquet_io.core.preview import _intersects

    east = {"xmin": 175.0, "ymin": 0.0, "xmax": 176.0, "ymax": 1.0}
    zero = {"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}
    wrapped = parse_bbox("170,-10,-170,10")
    assert _intersects(east, wrapped)
    assert not _intersects(zero, wrapped)


def test_carto_sql_ors_the_two_lobes():
    from geoparquet_io.core.carto import _build_carto_count_query, _build_carto_query

    for sql in (
        _build_carto_query("t", bbox=parse_bbox("170,-10,-170,10")),
        _build_carto_count_query("t", bbox=parse_bbox("170,-10,-170,10")),
    ):
        assert "ST_MakeEnvelope(170.0, -10.0, 180.0, 10.0, 4326)" in sql
        assert "ST_MakeEnvelope(-180.0, -10.0, -170.0, 10.0, 4326)" in sql
        assert " OR " in sql


def test_bigquery_filters_or_the_two_lobes():
    from geoparquet_io.core.extract_bigquery import _build_bbox_filters

    for server in (True, False):
        bq, local = _build_bbox_filters("170,-10,-170,10", "geog", use_server_side=server)
        (condition,) = bq or local
        assert "170.0 -10.0, 180.0 -10.0" in condition
        assert "-180.0 -10.0, -170.0 -10.0" in condition
        assert " OR " in condition


@pytest.mark.parametrize("service", ["arcgis", "wfs"])
def test_a_service_that_cannot_send_a_wrap_says_so(service, tmp_path, monkeypatch):
    """ArcGIS envelopes and WFS BBOX have no wrapped form: one named error, before any request."""
    import socket

    def no_network(*args, **kwargs):
        raise AssertionError("the bbox should have been refused before any request")

    monkeypatch.setattr(socket, "getaddrinfo", no_network)
    args = [*SERVICES[service], str(tmp_path / "out.parquet"), "--bbox", "170,-10,-170,10"]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code != 0
    assert "antimeridian" in result.output.lower(), result.output


@pytest.mark.parametrize("entry", ["from_arcgis", "from_wfs"])
def test_the_api_raises_the_named_error(entry, monkeypatch):
    import socket

    from geoparquet_io.api import ops
    from geoparquet_io.core.bbox_parse import AntimeridianBBoxError

    monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: (_ for _ in ()).throw(OSError()))
    args = {
        "from_arcgis": ("https://gpio.invalid/arcgis/rest/services/x/FeatureServer/0",),
        "from_wfs": ("https://gpio.invalid/wfs", "layer"),
    }[entry]
    with pytest.raises(AntimeridianBBoxError):
        getattr(ops, entry)(*args, bbox=(170, -10, -170, 10))


@pytest.mark.parametrize("entry", ["from_arcgis", "from_wfs", "from_carto"])
def test_the_api_parses_with_the_owner(entry, monkeypatch):
    """The core entries parse, so bbox= on the API gets the CLI's verdict."""
    import socket

    from geoparquet_io.api import ops

    monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: (_ for _ in ()).throw(OSError()))

    args = {
        "from_arcgis": ("https://gpio.invalid/arcgis/rest/services/x/FeatureServer/0",),
        "from_wfs": ("https://gpio.invalid/wfs", "layer"),
        "from_carto": ("https://gpio.invalid/carto", "table"),
    }[entry]
    with pytest.raises(InvalidParameterError) as exc:
        getattr(ops, entry)(*args, bbox=(0, 0, float("nan"), 1))
    assert exc.value.param_name == "bbox"


# --- One wrap rule, three gates (#1367 audit pass 2) ------------------------
#
# xmin > xmax is the wrap only when (1) both x are longitudes in [-180, 180]
# and (2) the CRS is geographic or unknown. BBox.crosses_antimeridian is the
# whole rule; every reader of a wrapped bbox (parse_bbox, validate, check bbox,
# preview, merge_longitude_ranges, check stac) asks it.

WRAP_TABLE = [
    # (box, geographic, wraps)
    ((170, -10, -170, 10), True, True),
    ((170, -10, -170, 10), None, True),
    ((170, -10, -170, 10), False, False),
    ((0, 0, 1, 1), True, False),
    ((0, 0, 1, 1), None, False),
    ((0, 0, 1, 1), False, False),
    ((500000, 0, 400000, 1), True, False),
    ((500000, 0, 400000, 1), None, False),
    ((500000, 0, 400000, 1), False, False),
    ((190, 0, 170, 1), True, False),
    ((-170, 0, -190, 1), None, False),
]


@pytest.mark.parametrize(("box", "geographic", "wraps"), WRAP_TABLE)
def test_the_one_wrap_rule(box, geographic, wraps):
    from geoparquet_io.core.bbox_parse import BBox

    b = BBox(*map(float, box))
    assert b.crosses_antimeridian(geographic) is wraps
    assert len(b.pieces(geographic)) == (2 if wraps else 1)


@pytest.mark.parametrize(("box", "geographic", "wraps"), WRAP_TABLE)
def test_the_sql_form_agrees(box, geographic, wraps):
    import duckdb

    from geoparquet_io.core.bbox_parse import crosses_antimeridian_sql

    xmin, _, xmax, _ = box
    sql = crosses_antimeridian_sql("a", "b", geographic)
    got = duckdb.sql(f"SELECT {sql} FROM (SELECT {xmin}::DOUBLE a, {xmax}::DOUBLE b)").fetchall()
    assert got == [(wraps,)]


def test_parse_bbox_reads_the_predicate():
    """parse_bbox has no longitude rule of its own: refused exactly when it is reversed and no wrap."""
    for box, geographic, wraps in WRAP_TABLE:
        if geographic is not None:
            continue
        text = ",".join(map(str, box))
        if box[0] > box[2] and not wraps:
            with pytest.raises(InvalidParameterError):
                parse_bbox(text)
        else:
            assert parse_bbox(text) == tuple(map(float, box))


def test_stac_item_with_a_wrapped_bbox_is_valid():
    """RFC 7946 5.2 allows a wrapped bbox; base reported 'Invalid bbox ordering'."""
    from geoparquet_io.core.stac_check import _validate_geometry_bbox

    item = {"type": "Feature", "geometry": {"type": "Point"}, "bbox": [170, -10, -170, 10]}
    results = {"errors": [], "valid": True}
    _validate_geometry_bbox(item, results)
    assert results == {"errors": [], "valid": True}
    swapped = {**item, "bbox": [500000, -10, 400000, 10]}
    _validate_geometry_bbox(swapped, results)
    assert not results["valid"]


def test_merge_longitude_ranges_splits_with_the_owner():
    from geoparquet_io.core.crs_utils import merge_longitude_ranges

    assert merge_longitude_ranges([(170.0, -170.0), (-175.0, -160.0)]) == (170.0, -160.0)
    assert merge_longitude_ranges([(0.0, 1.0), (2.0, 3.0)]) == (0.0, 3.0)


def test_preview_splits_a_wrapped_row_group_with_the_owner():
    from geoparquet_io.core.preview import _intersects

    wrapped_group = {"xmin": 170.0, "ymin": 0.0, "xmax": -170.0, "ymax": 1.0}
    assert _intersects(wrapped_group, parse_bbox("-175,0,-172,1"))
    assert _intersects(wrapped_group, parse_bbox("172,0,175,1"))
    assert not _intersects(wrapped_group, parse_bbox("0,0,1,1"))
