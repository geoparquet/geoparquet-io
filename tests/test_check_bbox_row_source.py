"""``check bbox`` numbers rows the way DuckDB lets it (audit of #1395).

DuckDB refuses ``file_row_number=true`` on a file with a *root* column of that
name -- in any case, since its binder folds -- and accepts it beside a nested
field of that name. ``_row_source`` falls back to a window function only for the
first.
"""

import pytest

from geoparquet_io.core.check_bbox_values import _row_source

ROOT = {"name": "schema", "type": None, "num_children": 1, "file_name": "f.parquet"}


def _listing(*names):
    return [ROOT, *({"name": n, "type": "INT64", "num_children": 0} for n in names)]


@pytest.mark.parametrize("name", ["file_row_number", "FILE_ROW_NUMBER", "File_Row_Number"])
def test_a_root_column_of_that_name_in_any_case_forces_the_window_function(name):
    table, index = _row_source(_listing(name), "/tmp/f.parquet")
    assert "file_row_number=true" not in table
    assert index == "row_number() OVER () - 1"


def test_no_such_column_uses_the_reader_row_number():
    table, _ = _row_source(_listing("id"), "/tmp/f.parquet")
    assert "file_row_number=true" in table


def test_a_nested_field_of_that_name_does_not_force_it():
    listing = [
        ROOT,
        {"name": "s", "type": None, "num_children": 1, "repetition_type": "OPTIONAL"},
        {"name": "file_row_number", "type": "INT64", "num_children": 0},
    ]
    listing[0]["num_children"] = 1
    table, _ = _row_source(listing, "/tmp/f.parquet")
    assert "file_row_number=true" in table
