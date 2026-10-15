"""#1267: a ``crs`` that is present but empty is unknown, never lon/lat.

The GeoParquet spec (1.0, 1.1 and 2.0) allows a geometry column's ``crs`` to be
a PROJJSON object or null. An omitted key means OGC:CRS84 and ``null`` means
the CRS is unknown. ``""``, ``{}``, ``[]`` and ``0`` are not valid values, and
none of them names a CRS. Reading one as "absent" made gpio key a projected
file's metres as degrees with no warning.

The rule is one predicate, :func:`is_empty_crs_value`. An empty value in a
``geo`` block or in GeoArrow metadata is treated like ``null``. It warns once,
naming the value, and the CRS is unknown. The Parquet GEOMETRY logical type is
different: there an unset CRS is the OGC:CRS84 default, so it is not touched.
"""

from __future__ import annotations

import json
import logging

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely
from click.testing import CliRunner
from pyproj import CRS

from geoparquet_io.cli.main import cli
from geoparquet_io.core.crs_utils import (
    CRS_ABSENT,
    UNKNOWN_BY_EMPTY,
    UNKNOWN_BY_NULL,
    crs_string_from_geo_meta,
    crs_string_from_table,
    entry_crs_is_unknown,
    extract_crs_from_parquet,
    extract_crs_from_table,
    is_default_crs,
    is_empty_crs_value,
    transform_source_crs,
    unknown_crs_reason,
)
from geoparquet_io.core.geo_metadata import sanitize_geo_metadata

EMPTY_VALUES = ["", "   ", {}, [], 0, 0.0]
NOT_EMPTY_VALUES = [None, CRS_ABSENT, False, "EPSG:4326", {"id": {}}, [4326], 42]


def _empty_crs_warnings(records) -> list[str]:
    return [r.getMessage() for r in records if "empty crs" in r.getMessage().lower()]


def _projected_points(n: int = 5) -> pa.Array:
    points = [shapely.Point(500000 + i * 1000, 4000000 + i * 1000) for i in range(n)]
    return pa.array([shapely.to_wkb(p) for p in points], pa.binary())


def _geo_block(crs) -> dict:
    return {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": crs}},
    }


def _write_projected(path, crs) -> str:
    table = pa.table({"id": list(range(5)), "geometry": _projected_points()})
    table = table.replace_schema_metadata({"geo": json.dumps(_geo_block(crs))})
    pq.write_table(table, path)
    return str(path)


# --------------------------------------------------------------------------- #
# The predicate
# --------------------------------------------------------------------------- #


class TestIsEmptyCrsValue:
    @pytest.mark.parametrize("value", EMPTY_VALUES, ids=repr)
    def test_empty_values(self, value):
        assert is_empty_crs_value(value) is True

    @pytest.mark.parametrize("value", NOT_EMPTY_VALUES, ids=repr)
    def test_values_that_are_not_empty(self, value):
        """``null`` and absent have their own meanings, and a real value is a value."""
        assert is_empty_crs_value(value) is False

    @pytest.mark.parametrize("value", EMPTY_VALUES, ids=repr)
    def test_an_empty_value_is_not_the_default(self, value):
        assert is_default_crs(value) is False

    def test_none_and_absent_stay_default_for_the_write_side(self):
        assert is_default_crs(None) is True
        assert is_default_crs(CRS_ABSENT) is True


# --------------------------------------------------------------------------- #
# The sanitizer: an empty value becomes null, not absent
# --------------------------------------------------------------------------- #


class TestSanitizerKeepsEmptyAsUnknown:
    @pytest.mark.parametrize("value", ["", {}, [], 0], ids=repr)
    def test_empty_becomes_null_and_warns(self, value, caplog):
        with caplog.at_level(logging.WARNING):
            cleaned = sanitize_geo_metadata(_geo_block(value))
        entry = cleaned["columns"]["geometry"]
        assert "crs" in entry, "an empty crs must not turn into the absent default"
        assert entry["crs"] is None
        messages = _empty_crs_warnings(caplog.records)
        assert len(messages) == 1, messages
        assert repr(value) in messages[0] or json.dumps(value) in messages[0]

    def test_does_not_mutate_the_carried_block(self):
        block = _geo_block({})
        sanitize_geo_metadata(block)
        assert block["columns"]["geometry"]["crs"] == {}

    def test_a_real_crs_is_untouched(self):
        block = _geo_block({"id": {"authority": "EPSG", "code": 32633}})
        assert sanitize_geo_metadata(block) is block


# --------------------------------------------------------------------------- #
# Readers
# --------------------------------------------------------------------------- #


class TestTransformSourceCrs:
    @pytest.mark.parametrize("value", EMPTY_VALUES, ids=repr)
    def test_empty_warns_and_needs_no_transform(self, value, caplog):
        with caplog.at_level(logging.WARNING):
            assert transform_source_crs(value) is None
        assert len(_empty_crs_warnings(caplog.records)) == 1

    def test_warns_once_per_value(self, caplog):
        with caplog.at_level(logging.WARNING):
            transform_source_crs({})
            transform_source_crs({})
        assert len(_empty_crs_warnings(caplog.records)) == 1


class TestGeoArrowFieldEmptyCrs:
    """``0`` and ``[]`` in raw GeoArrow extension metadata on a Table."""

    @staticmethod
    def _table(crs) -> pa.Table:
        ext = json.dumps({"crs": crs}).encode()
        field = pa.field(
            "geometry",
            pa.binary(),
            metadata={b"ARROW:extension:name": b"geoarrow.wkb", b"ARROW:extension:metadata": ext},
        )
        return pa.Table.from_arrays([_projected_points()], schema=pa.schema([field]))

    @pytest.mark.parametrize("value", [0, [], {}, ""], ids=repr)
    def test_extract_crs_from_table_warns(self, value, caplog):
        with caplog.at_level(logging.WARNING):
            assert extract_crs_from_table(self._table(value), "geometry") is None
        assert len(_empty_crs_warnings(caplog.records)) == 1

    @pytest.mark.parametrize("value", [0, []], ids=repr)
    def test_crs_string_from_table_warns(self, value, caplog):
        with caplog.at_level(logging.WARNING):
            assert crs_string_from_table(self._table(value), "geometry") is None
        assert len(_empty_crs_warnings(caplog.records)) == 1


class TestGeoBlockEmptyCrs:
    def test_extract_crs_from_parquet_warns_for_empty_object(self, tmp_path, caplog):
        path = _write_projected(tmp_path / "obj.parquet", {})
        with caplog.at_level(logging.WARNING):
            assert extract_crs_from_parquet(path) is None
        assert _empty_crs_warnings(caplog.records)

    def test_crs_string_from_geo_meta_warns(self, caplog):
        with caplog.at_level(logging.WARNING):
            assert crs_string_from_geo_meta(_geo_block({}), "geometry") is None
        assert _empty_crs_warnings(caplog.records)


# --------------------------------------------------------------------------- #
# Commands: a projected file with an empty crs is never keyed in silence
# --------------------------------------------------------------------------- #


def _loud(result) -> bool:
    """The command either refused the file or told the user its CRS is empty."""
    return result.exit_code != 0 or "empty crs" in result.output.lower()


@pytest.mark.parametrize("value", ["", {}], ids=repr)
def test_add_h3_is_never_silent(value, tmp_path):
    from geoparquet_io.cli.main import cli

    src = _write_projected(tmp_path / "in.parquet", value)
    result = CliRunner().invoke(cli, ["add", "h3", src, str(tmp_path / "out.parquet")])
    assert _loud(result), result.output


@pytest.mark.parametrize("value", ["", {}], ids=repr)
def test_convert_geojson_is_never_silent(value, tmp_path):
    from geoparquet_io.cli.main import cli

    src = _write_projected(tmp_path / "in.parquet", value)
    result = CliRunner().invoke(cli, ["convert", "geojson", src, str(tmp_path / "out.geojson")])
    assert _loud(result), result.output


def test_add_h3_warns_for_empty_object(tmp_path):
    """``{}`` is readable by DuckDB, so it must run and warn, naming the value."""
    from geoparquet_io.cli.main import cli

    src = _write_projected(tmp_path / "in.parquet", {})
    result = CliRunner().invoke(cli, ["add", "h3", src, str(tmp_path / "out.parquet")])
    assert result.exit_code == 0, result.output
    assert "empty crs" in result.output.lower()
    assert "{}" in result.output


class TestQuadkeyStdin:
    """stdin ``add quadkey`` with an empty ``crs`` in the streamed geo metadata."""

    @pytest.mark.parametrize("value", ["", {}, [], 0], ids=repr)
    def test_streaming_source_crs_warns(self, value, caplog):
        from geoparquet_io.core.add.quadkey import _streaming_source_crs

        metadata = {b"geo": json.dumps(_geo_block(value)).encode()}
        with caplog.at_level(logging.WARNING):
            assert _streaming_source_crs("-", metadata, "geometry", False) is None
        assert _empty_crs_warnings(caplog.records)

    @pytest.mark.parametrize("value", ["", {}, [], 0], ids=repr)
    def test_validate_warns_on_raw_metadata(self, value, caplog):
        from geoparquet_io.core.add.quadkey import _validate_crs_from_geo_metadata

        with caplog.at_level(logging.WARNING):
            _validate_crs_from_geo_metadata(_geo_block(value), "geometry", False, "stream")
        assert _empty_crs_warnings(caplog.records)

    def test_is_geographic_crs_is_unknown_for_empty(self):
        from geoparquet_io.core.add.quadkey import _is_geographic_crs

        assert _is_geographic_crs({}) is None
        assert _is_geographic_crs("") is None


def test_native_type_empty_crs_stays_unset(tmp_path):
    """On the Parquet GEOMETRY type an unset CRS is OGC:CRS84, so it is not unknown."""
    import duckdb

    out = tmp_path / "native.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(
        f"COPY (SELECT ST_Point(1, 2) AS geometry) TO '{out}' "
        "(FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')"
    )
    con.close()
    assert extract_crs_from_parquet(str(out)) is None


def test_a_real_projected_crs_is_still_used(tmp_path):
    src = _write_projected(tmp_path / "utm.parquet", CRS.from_epsg(32633).to_json_dict())
    assert transform_source_crs(extract_crs_from_parquet(src)) == "EPSG:32633"


# --------------------------------------------------------------------------- #
# One rule for "is this entry's CRS unknown" (#1277 review)
# --------------------------------------------------------------------------- #

ALBERS = {"id": {"authority": "EPSG", "code": 5070}}


@pytest.mark.parametrize(
    ("col_meta", "type_crs", "expected"),
    [
        ({"crs": None}, None, True),
        ({"crs": {}}, None, True),
        ({"crs": ""}, None, True),
        ({"crs": ALBERS}, None, False),
        ({}, None, False),
        # No crs key: the Parquet type decides, and srid:0 is unknown.
        ({}, "srid:0", True),
        # A crs key decides, even over a srid:0 type.
        ({"crs": ALBERS}, "srid:0", False),
        (None, None, False),
        (None, "srid:0", True),
    ],
    ids=repr,
)
def test_entry_crs_is_unknown(col_meta, type_crs, expected):
    assert entry_crs_is_unknown(col_meta, type_crs) is expected


@pytest.mark.parametrize(
    ("crs", "reason"),
    [({}, UNKNOWN_BY_EMPTY), ("", UNKNOWN_BY_EMPTY), (None, UNKNOWN_BY_NULL)],
    ids=repr,
)
def test_reproject_names_an_empty_crs_as_empty(crs, reason, tmp_path):
    """Sanitizing reads ``{}`` as null, so the message must still say empty."""
    src = _write_projected(tmp_path / "in.parquet", crs)
    assert unknown_crs_reason(src) == reason
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(cli, ["convert", "reproject", src, str(out)])
    assert result.exit_code != 0
    assert f"Input has {reason} (CRS unknown)" in str(result.output) + str(result.exception)
