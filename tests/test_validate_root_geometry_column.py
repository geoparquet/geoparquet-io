"""A nested field named like the geometry column must not stand in for it (audit of #1395).

``get_schema_info`` lists the schema depth first, so a struct's child called
``geometry`` can precede the real root column. The checks that read the geometry
column's physical type, repetition or logical type used to take the first entry
of that name; they now read the root one, as ``parquet_schema.root_schema_index``
does.
"""

from geoparquet_io.core.validate import (
    CheckStatus,
    _check_geometry_byte_array,
    _check_geometry_not_repeated,
    _check_native_geo_type_present,
)

#: A struct "meta" whose child is *also* called "geometry", before the root column.
LISTING = [
    {"name": "meta", "type": None, "num_children": 1, "repetition_type": "OPTIONAL"},
    {
        "name": "geometry",
        "type": "INT32",
        "num_children": 0,
        "repetition_type": "REPEATED",
        "logical_type": None,
    },
    {
        "name": "geometry",
        "type": "BYTE_ARRAY",
        "num_children": 0,
        "repetition_type": "OPTIONAL",
        "logical_type": "GeometryType(crs=null)",
    },
]


def test_repetition_is_the_root_columns():
    assert _check_geometry_not_repeated(LISTING, "geometry").status == CheckStatus.PASSED


def test_physical_type_is_the_root_columns():
    assert _check_geometry_byte_array(LISTING, "geometry").status == CheckStatus.PASSED


def test_logical_type_is_the_root_columns():
    assert _check_native_geo_type_present(LISTING, "geometry").status == CheckStatus.PASSED


# ---------------------------------------------------------------------------
# The `!=`-continue walks, on a real file: meta.geometry (an int) ahead of the
# root geometry column, written with a native GEOMETRY logical type (v2.0).
# ---------------------------------------------------------------------------

import duckdb  # noqa: E402
import pytest  # noqa: E402
from click.testing import CliRunner  # noqa: E402

from geoparquet_io.cli.main import cli  # noqa: E402
from geoparquet_io.core.duckdb_metadata import get_schema_info  # noqa: E402


def _written(root, name, select, version):
    raw, out = root / f"{name}_raw.parquet", root / f"{name}.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    con.execute(f"COPY ({select}) TO '{raw}' (FORMAT PARQUET)")
    result = CliRunner().invoke(
        cli, ["convert", "geoparquet", str(raw), str(out), "--geoparquet-version", version]
    )
    assert result.exit_code == 0, result.output
    return str(out)


DECOY = "SELECT {'geometry': 1} AS meta, ST_Point(1, 2) AS geometry"


@pytest.fixture(scope="module")
def decoy_file(tmp_path_factory):
    return _written(tmp_path_factory.mktemp("decoy"), "decoy", DECOY, "2.0")


def test_the_fixture_lists_the_decoy_before_the_root_column(decoy_file):
    names = [row["name"] for row in get_schema_info(decoy_file)]
    assert names.count("geometry") == 2 and names.index("geometry") < len(names) - 1


def test_schema_crs_is_the_root_columns(decoy_file):
    from geoparquet_io.core.validate import CRS_ABSENT, _schema_crs_for_consistency

    assert _schema_crs_for_consistency(get_schema_info(decoy_file), "geometry") is not CRS_ABSENT


def test_edges_facts_are_the_root_columns(decoy_file):
    from geoparquet_io.core.validate import _edges_schema_facts

    assert _edges_schema_facts(get_schema_info(decoy_file), "geometry") == (False, True, None)


def test_inspect_reads_the_root_columns_native_crs(tmp_path):
    """Without a geo block the native type is the only CRS there is."""
    from geoparquet_io.core.inspect_utils import extract_geo_info

    decoy = _written(tmp_path, "decoy_only", DECOY, "parquet-geo-only")
    plain = _written(
        tmp_path, "plain_only", "SELECT ST_Point(1, 2) AS geometry", "parquet-geo-only"
    )
    assert extract_geo_info(plain)["crs"] is not None
    assert extract_geo_info(decoy)["crs"] == extract_geo_info(plain)["crs"]


def test_geo_columns_info_is_the_root_columns(decoy_file):
    from geoparquet_io.core.metadata_utils import _build_geo_columns_info

    listing = get_schema_info(decoy_file)
    root = listing[-1]
    reordered = [root, *listing[:-1]]  # the root column first, the decoy after it
    info = _build_geo_columns_info(reordered, {"geometry": "Geometry"})
    assert info["geometry"]["crs"] is not None


# ---------------------------------------------------------------------------
# The set/dict-form walks over the whole listing (audit pass 5): a nested
# field is no column, whatever its logical type.
# ---------------------------------------------------------------------------

#: meta (struct) holding a GEOMETRY child "inner", then the root geometry column.
NESTED_GEOMETRY = [
    {"name": "meta", "type": None, "num_children": 1, "repetition_type": "OPTIONAL"},
    {
        "name": "inner",
        "type": "BYTE_ARRAY",
        "num_children": 0,
        "repetition_type": "OPTIONAL",
        "logical_type": "GeometryType(crs=null)",
    },
    {
        "name": "geometry",
        "type": "BYTE_ARRAY",
        "num_children": 0,
        "repetition_type": "OPTIONAL",
        "logical_type": "GeometryType(crs=null)",
    },
]


def test_native_columns_in_metadata_ignores_a_nested_geometry_child():
    from geoparquet_io.core.validate import _check_native_columns_in_metadata

    check = _check_native_columns_in_metadata(NESTED_GEOMETRY, {"geometry": {}})
    assert check.status == CheckStatus.PASSED, check.message


def test_native_columns_in_metadata_still_names_a_missing_root_column():
    from geoparquet_io.core.validate import _check_native_columns_in_metadata

    check = _check_native_columns_in_metadata(NESTED_GEOMETRY, {})
    assert check.status == CheckStatus.FAILED and "['geometry']" in check.message


def test_native_geography_edges_are_keyed_by_root_columns_only():
    from geoparquet_io.core.write_funnels import _native_geography_edges

    geography = 'GeographyType(crs=null, algorithm="spherical")'
    listing = [
        {"name": "meta", "type": None, "num_children": 1, "repetition_type": "OPTIONAL"},
        {"name": "inner", "type": "BYTE_ARRAY", "num_children": 0, "logical_type": geography},
        {"name": "geog", "type": "BYTE_ARRAY", "num_children": 0, "logical_type": geography},
    ]
    assert [name for name, _ in _native_geography_edges(listing)] == ["geog"]


def test_native_crs_of_a_file_is_the_root_columns_not_a_nested_childs():
    from geoparquet_io.core.extract import _native_crs_from_schema

    decoy = 'GeometryType(crs={"id": {"authority": "EPSG", "code": 3857}})'
    root = 'GeometryType(crs={"id": {"authority": "EPSG", "code": 4326}})'
    listing = [
        {"name": "meta", "type": None, "num_children": 1, "repetition_type": "OPTIONAL"},
        {"name": "geometry", "type": "BYTE_ARRAY", "num_children": 0, "logical_type": decoy},
        {"name": "geometry", "type": "BYTE_ARRAY", "num_children": 0, "logical_type": root},
    ]
    assert _native_crs_from_schema(listing, "geometry") == {
        "id": {"authority": "EPSG", "code": 4326}
    }
    assert _native_crs_from_schema(listing[:2], "geometry") is None
