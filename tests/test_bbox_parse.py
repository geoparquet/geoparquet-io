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
    ("1,0,0,1", None),  # xmin > xmax
    ("0,1,1,0", None),  # ymin > ymax
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
    ([1, 0, 0, 1], None),
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
