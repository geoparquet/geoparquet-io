"""Writers must emit dimension-suffixed geometry_types for Z/M data (review todo 035).

The GeoParquet spec treats "LineString" and "LineString ZM" as distinct
geometry_types entries. The write-side SQL scan previously collapsed the
dimension, so gpio's own converted Z/M output failed gpio's own spec check.

The four write strategies must agree on that spelling. The in-memory strategy
computes the types from Arrow data rather than by SQL scan, and read only
geoarrow's base type code — so it alone declared "Polygon" for Polygon M data
(#892), a disagreement invisible until a reader compared the declaration
against the file's own statistics.
"""

import json

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import (
    get_duckdb_connection,
    split_zm_suffix,
    write_parquet_with_metadata,
)
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.validate import CheckStatus, validate_geoparquet
from tests.conftest import get_geo_metadata

WKT_BY_DIM = {
    "Z": ("LINESTRING Z (0 0 1, 1 1 2)", "LineString Z"),
    "M": ("LINESTRING M (0 0 5, 1 1 6)", "LineString M"),
    "ZM": ("LINESTRING ZM (0 0 1 5, 1 1 2 6)", "LineString ZM"),
}

#: Every dimension, XY included: the suffix-less case is the control.
POLYGON_BY_DIM = {
    "XY": ("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", "Polygon"),
    "Z": ("POLYGON Z ((0 0 1, 1 0 1, 1 1 1, 0 1 1, 0 0 1))", "Polygon Z"),
    "M": ("POLYGON M ((0 0 5, 1 0 5, 1 1 5, 0 1 5, 0 0 5))", "Polygon M"),
    "ZM": (
        "POLYGON ZM ((0 0 1 5, 1 0 1 5, 1 1 1 5, 0 1 1 5, 0 0 1 5))",
        "Polygon ZM",
    ),
}

WRITE_STRATEGIES = ["in-memory", "streaming", "disk-rewrite", "duckdb-kv"]


def _make_source(tmp_path, wkt):
    path = tmp_path / "src.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (1, ST_GeomFromText('{wkt}'))
          ) t(id, geometry)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
    """)
    con.close()
    return path


def test_split_zm_suffix():
    assert split_zm_suffix("Point") == ("Point", "")
    assert split_zm_suffix("Point Z") == ("Point", " Z")
    assert split_zm_suffix("MultiPolygon M") == ("MultiPolygon", " M")
    assert split_zm_suffix("LineString ZM") == ("LineString", " ZM")


def test_compute_geometry_types_via_sql_is_dimension_aware():
    """Both copies (common + geo_metadata) must return spec-suffixed names."""
    from geoparquet_io.core import common, geo_metadata

    con = get_duckdb_connection(load_spatial=True)
    try:
        query = (
            "SELECT ST_GeomFromText('LINESTRING ZM (0 0 1 5, 1 1 2 6)') AS geometry "
            "UNION ALL SELECT ST_GeomFromText('LINESTRING (2 2, 3 3)')"
        )
        expected = ["LineString", "LineString ZM"]
        assert common.compute_geometry_types_via_sql(con, query, "geometry") == expected
        assert geo_metadata.compute_geometry_types_via_sql(con, query, "geometry") == expected
    finally:
        con.close()


@pytest.mark.parametrize("dim", ["Z", "M", "ZM"])
@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_convert_emits_suffixed_geometry_types(tmp_path, dim, version):
    """Round-trip: converted Z/M output declares suffixed types and self-validates."""
    wkt, expected = WKT_BY_DIM[dim]
    src = _make_source(tmp_path, wkt)
    out = tmp_path / f"out_{dim}_{version.replace('.', '_')}.parquet"
    convert_to_geoparquet(str(src), str(out), skip_hilbert=True, geoparquet_version=version)

    geo = get_geo_metadata(str(out))
    col = geo["columns"][geo["primary_column"]]
    assert col["geometry_types"] == [expected], col["geometry_types"]

    result = validate_geoparquet(str(out), validate_data=True, sample_size=0)
    failures = [
        c for c in result.checks if "geometry_types" in c.name and c.status == CheckStatus.FAILED
    ]
    assert not failures, [f"{c.name}: {c.message}" for c in failures]


@pytest.mark.parametrize("dim", list(POLYGON_BY_DIM))
@pytest.mark.parametrize("strategy", WRITE_STRATEGIES)
def test_every_write_strategy_declares_the_same_suffix(tmp_path, strategy, dim):
    """All four strategies must spell the dimension the same way (#892)."""
    wkt, expected = POLYGON_BY_DIM[dim]
    out = tmp_path / f"{strategy}_{dim}.parquet"

    con = get_duckdb_connection(load_spatial=True)
    try:
        write_parquet_with_metadata(
            con,
            f"SELECT 1 AS id, ST_GeomFromText('{wkt}') AS geometry",
            str(out),
            geoparquet_version="1.1",
            write_strategy=strategy,
        )
    finally:
        con.close()

    geo = json.loads(pq.read_metadata(str(out)).metadata[b"geo"].decode("utf-8"))
    assert geo["columns"]["geometry"]["geometry_types"] == [expected]


def test_compute_geometry_types_from_arrow_is_dimension_aware():
    """The Arrow/geoarrow computation, on both copies, keeps the suffix (#892).

    geoarrow reports the base type and the dimensions in separate struct
    fields, so reading only ``geometry_type`` spelled every dimension "Polygon".
    """
    import pyarrow as pa
    import shapely
    from shapely import wkt as shapely_wkt

    from geoparquet_io.core import common, geo_metadata

    for _dim, (wkt, expected) in POLYGON_BY_DIM.items():
        table = pa.table(
            {
                "geometry": pa.array(
                    [shapely.to_wkb(shapely_wkt.loads(wkt), flavor="iso")],
                    type=pa.binary(),
                )
            }
        )
        assert common._compute_geometry_types(table, "geometry", False) == [expected]
        assert geo_metadata._compute_geometry_types(table, "geometry", False) == [expected]
