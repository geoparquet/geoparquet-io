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
