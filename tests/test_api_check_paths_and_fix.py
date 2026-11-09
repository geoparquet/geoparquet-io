"""Path-level ``gpio.check_*`` functions (#1090) and ``Table.fix`` (#1078).

The path functions are the one call path for a check: a ``Table`` read from a
file hands them its source, so ``gpio.check_x(path)`` and
``gpio.read(path).check_x()`` cannot drift. ``Table.fix`` repairs into a new
Table and never touches the file it was read from.
"""

from __future__ import annotations

import os
import shutil

import pytest

import geoparquet_io as gpio
from geoparquet_io.api import check as check_api

# Deterministic checks; check_spatial samples random pairs, so it is pinned by
# delegation below rather than by equal answers.
PAIRS = [
    ("check_all", "check"),
    ("check_compression", "check_compression"),
    ("check_row_groups", "check_row_groups"),
    ("check_bbox", "check_bbox"),
    ("check_bloom_filters", "check_bloom_filters"),
    ("check_optimization", "check_optimization"),
    ("check_spatial_pushdown", "check_spatial_pushdown"),
    ("validate", "validate"),
]


@pytest.mark.parametrize(("function", "method"), PAIRS)
def test_path_function_answers_as_the_table_method(unsorted_test_file, function, method):
    path = os.path.abspath(unsorted_test_file)

    by_path = getattr(gpio, function)(path)
    by_table = getattr(gpio.read(path), method)()

    assert by_path.to_dict() == by_table.to_dict()
    assert by_path.check_type == by_table.check_type
    assert by_path.source_path == path == by_table.source_path


def test_a_table_check_goes_through_the_path_function(monkeypatch, unsorted_test_file):
    seen = []
    real = check_api.check_spatial

    def spy(path, **kwargs):
        seen.append((path, kwargs))
        return real(path, **kwargs)

    monkeypatch.setattr(check_api, "check_spatial", spy)
    result = gpio.read(unsorted_test_file).check_spatial(sample_size=10, limit_rows=1000)

    assert seen == [(os.path.abspath(unsorted_test_file), {"sample_size": 10, "limit_rows": 1000})]
    assert result.source_path == os.path.abspath(unsorted_test_file)


@pytest.mark.parametrize(
    ("function", "command"), [("check_compression", "compression"), ("validate", "spec")]
)
@pytest.mark.parametrize("operand", ["dir", "glob"])
def test_a_directory_or_glob_is_refused(tmp_path, unsorted_test_file, function, command, operand):
    shutil.copy(unsorted_test_file, tmp_path / "a.parquet")
    path = str(tmp_path) if operand == "dir" else str(tmp_path / "*.parquet")

    with pytest.raises(gpio.InvalidParameterError, match=f"gpio check {command}"):
        getattr(gpio, function)(path)


def test_a_remote_url_goes_straight_to_core(monkeypatch):
    from geoparquet_io.core import check_parquet_structure

    url = "s3://bucket/data.parquet"
    seen = []
    monkeypatch.setattr(
        check_parquet_structure,
        "check_compression",
        lambda path, **_: seen.append(path) or {"passed": True},
    )

    result = gpio.check_compression(url)

    assert seen == [url]
    assert result.source_path == url
