"""``detect_geometry_columns`` answers for root columns, not nested fields (audit pass 6).

DuckDB (GEOPARQUET_VERSION 'V2') writes a struct child with a GEOMETRY logical
type; ``parquet_schema()`` lists it, so a flat walk reported the child ("g") as a
geometry column beside the real one. The listing's depth-first shape is what the
remote path (and any call that passes a connection) reads.
"""

import duckdb
import pytest

from geoparquet_io.core.duckdb_metadata import detect_geometry_columns


@pytest.fixture(scope="module")
def nested_geometry_file(tmp_path_factory):
    path = tmp_path_factory.mktemp("nested") / "nested.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    con.execute(
        "COPY (SELECT {'g': ST_Point(1, 2)} AS meta, ST_Point(1, 2) AS geometry) "
        f"TO '{path}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
    )
    return str(path)


def test_the_file_lists_a_nested_geometry_child(nested_geometry_file):
    from geoparquet_io.core.duckdb_metadata import get_schema_info

    con = duckdb.connect()
    rows = get_schema_info(nested_geometry_file, con)
    assert [r["name"] for r in rows if "Geometry" in (r.get("logical_type") or "")] == [
        "g",
        "geometry",
    ]


def test_parquet_schema_path_reports_root_geometry_columns_only(nested_geometry_file):
    # A supplied connection takes the parquet_schema() path, as a remote file does.
    assert detect_geometry_columns(nested_geometry_file, duckdb.connect()) == {
        "geometry": "Geometry"
    }
