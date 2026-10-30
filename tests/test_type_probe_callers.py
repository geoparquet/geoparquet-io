"""Callers of the T1c type and schema owners whose answer this round changed or pinned.

Each test names the caller and the owner it now reads (#1367 concepts 10 and 17,
second T1c audit F1-F3, F7, F8 and the ``allow_crs`` aside).
"""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.duckdb_utils import (
    duckdb_type_is_geometry,
    duckdb_type_is_numeric,
    get_duckdb_connection,
    sql_path,
)


@pytest.fixture
def con():
    connection = get_duckdb_connection(load_spatial=True)
    yield connection
    connection.close()


# --- duckdb_type_is_numeric (F8) ---------------------------------------------------


@pytest.mark.parametrize(
    ("type_name", "expected"),
    [
        ("INTEGER", True),
        ("UHUGEINT", True),
        ("DOUBLE", True),
        ("FLOAT", True),
        ("REAL", True),
        ("DECIMAL(18,3)", True),
        ("NUMERIC(10, 2)", True),
        ("double", True),
        ("VARCHAR", False),
        ("BOOLEAN", False),
        ("TIMESTAMP", False),
        ("INTEGER[]", False),
        (None, False),
    ],
)
def test_duckdb_type_is_numeric(type_name, expected):
    assert duckdb_type_is_numeric(type_name) is expected


def test_convert_coordinate_columns_take_every_numeric_spelling():
    """Behaviour change: ``REAL``, ``NUMERIC(..)`` and a lower-case name now count
    as coordinate types (convert's own list lacked them); VARCHAR still does."""
    from geoparquet_io.core.convert import _coordinate_castable

    assert _coordinate_castable("REAL")
    assert _coordinate_castable("NUMERIC(10,2)")
    assert _coordinate_castable("double")
    assert _coordinate_castable("DECIMAL(9,6)")
    assert _coordinate_castable("VARCHAR")
    assert not _coordinate_castable("BOOLEAN")


def test_aggregate_nodata_accepts_the_same_numeric_types():
    """``--metric-nodata`` reads the same predicate: unchanged for aggregate's
    names (its list already had REAL and the DECIMAL/NUMERIC prefixes)."""
    from geoparquet_io.core.exceptions import InvalidParameterError
    from geoparquet_io.core.process.aggregate.common import _aggregated_column_expr

    for numeric in ("REAL", "DECIMAL(18,3)", "NUMERIC(4,1)", "BIGINT"):
        assert _aggregated_column_expr("h", [-1], {"h": numeric})
    with pytest.raises(InvalidParameterError, match="numeric metric columns"):
        _aggregated_column_expr("h", [-1], {"h": "VARCHAR"})


# --- duckdb_type_is_geometry(allow_crs=False): one rule at both Arrow-export callers --


def test_allow_crs_false_answers_only_for_a_bare_geometry():
    assert duckdb_type_is_geometry("GEOMETRY", allow_crs=False)
    assert not duckdb_type_is_geometry("GEOMETRY('EPSG:4326')", allow_crs=False)
    assert duckdb_type_is_geometry("GEOMETRY('EPSG:4326')")
    assert not duckdb_type_is_geometry("GEOGRAPHY", allow_crs=False)


def test_streaming_strategy_wraps_only_a_bare_geometry(con):
    """F2: the streaming strategy reads the type from DESCRIBE, not a TYPEOF row scan,
    and leaves a CRS-typed geometry to cross Arrow with its CRS."""
    from geoparquet_io.core.write_strategies.arrow_streaming import _check_geometry_type

    bare = "SELECT ST_Point(1, 2) AS geometry"
    with_crs = "SELECT ST_SetCRS(ST_Point(1, 2), 'EPSG:4326') AS geometry"
    empty = f"{bare} WHERE false"
    assert _check_geometry_type(con, bare, "geometry", False)[0] is True
    assert _check_geometry_type(con, with_crs, "geometry", False)[0] is False
    # No row to read: DESCRIBE still knows the type.
    assert _check_geometry_type(con, empty, "geometry", False)[0] is True


def test_read_partition_keeps_a_crs_typed_geometry_and_its_crs(tmp_path, con):
    import geoparquet_io as gpio

    part = tmp_path / "part"
    part.mkdir()
    con.execute(
        "COPY (SELECT 1 AS id, ST_SetCRS(ST_Point(1, 2), 'EPSG:4326') AS geometry) "
        f"TO {sql_path(str(part / 'a.parquet'))} (FORMAT PARQUET)"
    )
    table = gpio.read_partition(str(part))
    assert (table.crs or {}).get("id") == {"authority": "EPSG", "code": 4326}


# --- F1: geojson's geometry-column fallback reads DESCRIBE's types ------------------


def test_geojson_geometry_column_fallback_needs_no_row(con):
    """Behaviour change: the type fallback read ``typeof(col) LIMIT 1``, which
    finds nothing in an empty source; DESCRIBE types the column without a row."""
    from geoparquet_io.core.geojson_stream import _find_geometry_column

    con.execute("CREATE TABLE t AS SELECT 1 AS id, ST_Point(0, 0) AS footprint_pt WHERE false")
    assert _find_geometry_column(con, "t") == "footprint_pt"


# --- F3: WFS properties type from DESCRIBE ------------------------------------------


@pytest.mark.parametrize(
    ("features", "props_type", "can_unnest"),
    [
        ([{"type": "Feature", "geometry": None, "properties": {"a": 1}}], "STRUCT(a BIGINT)", True),
        ([{"type": "Feature", "geometry": None, "properties": {}}], "MAP(VARCHAR, JSON)", False),
        # Behaviour change: zero features read "NULL" from the row scan; DESCRIBE
        # reports the type DuckDB inferred. Neither can be unnested.
        ([], "JSON", False),
    ],
)
def test_wfs_properties_type(tmp_path, con, features, props_type, can_unnest):
    from geoparquet_io.core.wfs import _probe_properties_type

    path = tmp_path / "r.json"
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))
    assert _probe_properties_type(con, str(path), 1_000_000) == (props_type, can_unnest)


# --- F7: the preview's explicit column through the owner's strict mode --------------


def test_strict_native_stats_raise_on_a_missing_column(tmp_path, con):
    from geoparquet_io.core.duckdb_metadata import get_per_row_group_native_geo_stats
    from geoparquet_io.core.exceptions import InvalidParameterError

    path = tmp_path / "a.parquet"
    con.execute(
        f"COPY (SELECT ST_Point(1, 2) AS geometry) TO {sql_path(str(path))} "
        "(FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
    )
    assert get_per_row_group_native_geo_stats(str(path), "nope") == []
    with pytest.raises(InvalidParameterError, match="no column named 'nope'"):
        get_per_row_group_native_geo_stats(str(path), "nope", strict=True)
    rows = get_per_row_group_native_geo_stats(str(path), "geometry", strict=True)
    assert rows == get_per_row_group_native_geo_stats(str(path), "geometry")
    assert pq.ParquetFile(str(path)).metadata.num_row_groups == len(rows)
