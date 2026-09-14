"""Unit tests for Z/M geometry_types handling (gpio #583).

The spec mandates dimension suffixes ("Point Z", "LineString ZM", ...) and
says geometry_types "is expected to be strictly correct". gpio must accept
suffixed names as valid and detect dimensionality when scanning data.
"""

import pytest

from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.validate import (
    CheckStatus,
    _check_geometry_types_list,
    _check_geometry_types_match_data,
    _check_native_geo_types_match,
)


class TestGeometryTypesListAcceptsSuffixes:
    @pytest.mark.parametrize(
        "types",
        [
            ["Point Z"],
            ["LineString M"],
            ["Polygon ZM"],
            ["MultiPolygon Z", "Polygon Z"],
            ["Point", "Point Z"],
        ],
    )
    def test_suffixed_types_valid(self, types):
        check = _check_geometry_types_list({"geometry_types": types}, "geometry")
        assert check.status == CheckStatus.PASSED, check.message

    @pytest.mark.parametrize(
        "types",
        [
            ["Point X"],
            ["LineString ZZ"],
            ["Sphere"],
            ["Point z"],  # lowercase suffix is not spec-valid
            ["PointZ"],  # missing the space
        ],
    )
    def test_invalid_types_still_rejected(self, types):
        check = _check_geometry_types_list({"geometry_types": types}, "geometry")
        assert check.status == CheckStatus.FAILED, check.message


@pytest.fixture
def zm_file(tmp_path):
    """Native 2.0 file containing XYZM linestrings."""
    out = tmp_path / "zm.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (ST_GeomFromText('LINESTRING ZM (0 0 100 0, 1 1 110 10)')),
            (ST_GeomFromText('LINESTRING ZM (5 5 50 0, 6 5 60 5)'))
          ) t(geometry)
        ) TO '{out.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
    """)
    con.close()
    return out


class TestDataScansAreDimensionAware:
    def test_match_data_passes_for_correct_zm_declaration(self, zm_file):
        con = get_duckdb_connection(load_spatial=True)
        try:
            check = _check_geometry_types_match_data(
                str(zm_file), "geometry", ["LineString ZM"], con, sample_size=100
            )
        finally:
            con.close()
        assert check.status == CheckStatus.PASSED, check.message

    def test_match_data_fails_when_dimension_undeclared(self, zm_file):
        """Declaring XY 'LineString' for ZM data is the corpus's zm_mismatch case."""
        con = get_duckdb_connection(load_spatial=True)
        try:
            check = _check_geometry_types_match_data(
                str(zm_file), "geometry", ["LineString"], con, sample_size=100
            )
        finally:
            con.close()
        assert check.status == CheckStatus.FAILED, check.message

    def test_native_geo_types_match_zm(self, zm_file):
        """Native stats declare linestring_zm; the data scan must agree."""
        con = get_duckdb_connection(load_spatial=True)
        try:
            check = _check_native_geo_types_match(str(zm_file), "geometry", 100, con)
        finally:
            con.close()
        assert check.status == CheckStatus.PASSED, check.message
