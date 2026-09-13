"""geometry_types uniqueness, undeclared native geometry columns, and geometry_types vs the
Parquet geospatial_types statistics (GeoParquet 2.0)."""

import json
from pathlib import Path

import geoarrow.pyarrow as ga
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.duckdb_metadata import get_schema_info
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.validate import (
    CheckStatus,
    _check_geometry_types_list,
    _check_geometry_types_match_stats,
    _check_native_columns_in_metadata,
    validate_geoparquet,
)

CORPUS = str(Path(__file__).parent / "data" / "geoparquet-testing" / "data")


def _wkb(*wkts):
    con = get_duckdb_connection(load_spatial=True)
    values = ", ".join(f"('{w}')" for w in wkts)
    rows = con.execute(
        f"SELECT ST_AsWKB(ST_GeomFromText(w)) FROM (VALUES {values}) t(w)"
    ).fetchall()
    con.close()
    return pa.array([bytes(r[0]) for r in rows], pa.binary())


def _write(path, columns, geo_columns):
    """Native GEOMETRY columns (pyarrow writes the logical type and the statistics)."""
    table = pa.table({name: ga.as_wkb(arr) for name, arr in columns.items()})
    geo = {"version": "2.0.0", "primary_column": "geometry", "columns": geo_columns}
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), path)
    return str(path)


def _col(*types):
    return {"encoding": "WKB", "geometry_types": list(types)}


class TestGeometryTypesUnique:
    @pytest.mark.parametrize("types", [["Point", "Point"], ["Polygon Z", "Polygon Z", "Polygon"]])
    def test_duplicates_fail(self, types):
        check = _check_geometry_types_list({"geometry_types": types}, "geometry")
        assert check.status == CheckStatus.FAILED
        assert "duplicate" in check.message

    def test_distinct_pass(self):
        check = _check_geometry_types_list({"geometry_types": ["Polygon", "Polygon Z"]}, "geometry")
        assert check.status == CheckStatus.PASSED


class TestNativeColumnsInMetadata:
    def test_undeclared_native_column_fails(self, tmp_path):
        path = _write(
            tmp_path / "f.parquet",
            {"geometry": _wkb("POINT (1 2)"), "centroid": _wkb("POINT (1 2)")},
            {"geometry": _col("Point")},
        )
        check = _check_native_columns_in_metadata(get_schema_info(path), {"geometry": {}})
        assert check.status == CheckStatus.FAILED
        assert "centroid" in check.message

    def test_all_declared_passes(self, tmp_path):
        path = _write(
            tmp_path / "f.parquet",
            {"geometry": _wkb("POINT (1 2)"), "centroid": _wkb("POINT (1 2)")},
            {"geometry": _col("Point"), "centroid": _col("Point")},
        )
        check = _check_native_columns_in_metadata(
            get_schema_info(path), {"geometry": {}, "centroid": {}}
        )
        assert check.status == CheckStatus.PASSED

    def test_reported_through_validate(self, tmp_path):
        path = _write(
            tmp_path / "f.parquet",
            {"geometry": _wkb("POINT (1 2)"), "centroid": _wkb("POINT (1 2)")},
            {"geometry": _col("Point")},
        )
        result = validate_geoparquet(path, validate_data=False)
        (check,) = [c for c in result.checks if c.name == "native_columns_in_metadata"]
        assert check.status == CheckStatus.FAILED


class TestGeometryTypesMatchStats:
    def test_undeclared_dimension_fails(self, tmp_path):
        path = _write(
            tmp_path / "f.parquet",
            {"geometry": _wkb("POINT (1 2)", "POINT Z (3 4 5)")},
            {"geometry": _col("Point")},
        )
        check = _check_geometry_types_match_stats(path, "geometry", ["Point"])
        assert check.status == CheckStatus.FAILED
        assert "Point Z" in check.message

    def test_declared_types_pass(self, tmp_path):
        path = _write(
            tmp_path / "f.parquet",
            {"geometry": _wkb("POINT (1 2)", "POINT Z (3 4 5)")},
            {"geometry": _col("Point", "Point Z")},
        )
        check = _check_geometry_types_match_stats(path, "geometry", ["Point", "Point Z"])
        assert check.status == CheckStatus.PASSED, check.message

    def test_empty_geometry_types_is_skipped(self, tmp_path):
        path = _write(
            tmp_path / "f.parquet", {"geometry": _wkb("POINT (1 2)")}, {"geometry": _col()}
        )
        check = _check_geometry_types_match_stats(path, "geometry", [])
        assert check.status == CheckStatus.SKIPPED

    @pytest.mark.corpus
    @pytest.mark.skipif(not Path(CORPUS).exists(), reason="run: git submodule update --init")
    def test_corpus_polygon_and_multipolygon_passes(self):
        path = f"{CORPUS}/geometry_types/polygon-and-multipolygon.parquet"
        check = _check_geometry_types_match_stats(path, "geometry", ["Polygon", "MultiPolygon"])
        assert check.status == CheckStatus.PASSED, check.message

    def test_reported_through_validate(self, tmp_path):
        path = _write(
            tmp_path / "f.parquet",
            {"geometry": _wkb("POINT (1 2)", "POINT Z (3 4 5)")},
            {"geometry": _col("Point")},
        )
        result = validate_geoparquet(path, validate_data=False)
        (check,) = [c for c in result.checks if c.name == "geometry_types_match_stats_geometry"]
        assert check.status == CheckStatus.FAILED


class TestGeometryTypesMatchStatsEdges:
    def test_wkb_integer_codes_are_mapped(self):
        from geoparquet_io.core.validate import _stats_geometry_type_name

        assert _stats_geometry_type_name(1001) == "Point Z"
        assert _stats_geometry_type_name("multipolygon_zm") == "MultiPolygon ZM"

    def test_remote_file_is_skipped(self):
        check = _check_geometry_types_match_stats("s3://bucket/file.parquet", "geometry", ["Point"])
        assert check.status == CheckStatus.SKIPPED

    def test_unreadable_file_is_skipped(self, tmp_path):
        """Unreadable stats skip, matching _check_native_geo_statistics on the same throw."""
        check = _check_geometry_types_match_stats(
            str(tmp_path / "missing.parquet"), "geometry", ["Point"]
        )
        assert check.status == CheckStatus.SKIPPED
        assert "could not check" in check.message
