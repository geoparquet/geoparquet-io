"""``inspect``'s geometry flag compares resolved names exactly (audit pass 5).

DuckDB renames a case twin on read (physical ``geom`` and ``GEOM`` are described
as ``geom`` and ``GEOM_1``), so folding a resolved primary column name against
the described names would mark the wrong column.
"""

from geoparquet_io.core.inspect import _build_columns_info

DESCRIBED = [{"name": "geom", "type": "BLOB"}, {"name": "GEOM_1", "type": "BLOB"}]


def test_primary_upper_flags_no_described_twin():
    assert [c["is_geometry"] for c in _build_columns_info(DESCRIBED, "GEOM")] == [False, False]


def test_primary_exact_spelling_is_flagged():
    assert [c["is_geometry"] for c in _build_columns_info(DESCRIBED, "geom")] == [True, False]
    assert [c["is_geometry"] for c in _build_columns_info(DESCRIBED, "GEOM_1")] == [False, True]


def test_no_primary_flags_nothing():
    assert [c["is_geometry"] for c in _build_columns_info(DESCRIBED, None)] == [False, False]
