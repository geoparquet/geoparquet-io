"""Path-level ``gpio.check_*`` functions (#1090) and ``Table.fix`` (#1078).

The path functions are the one call path for a check: a ``Table`` read from a
file hands them its source, so ``gpio.check_x(path)`` and
``gpio.read(path).check_x()`` cannot drift. ``Table.fix`` repairs into a new
Table and never touches the file it was read from.
"""

from __future__ import annotations

import json
import os
import shutil

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.api import check as check_api
from geoparquet_io.cli.commands import check as cli_check
from geoparquet_io.cli.main import cli

# Deterministic checks; check_spatial samples random pairs, so it is pinned by
# delegation below rather than by equal answers.
PAIRS = [
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


@pytest.fixture
def unsorted_misdeclared(tmp_path, unsorted_test_file):
    """unsorted.parquet (a spatial-order problem) declaring Polygon over its Points (a spec one)."""
    table = pq.read_table(unsorted_test_file)
    geo = json.loads(table.schema.metadata[b"geo"])
    geo["columns"][geo["primary_column"]]["geometry_types"] = ["Polygon"]
    path = tmp_path / "misdeclared.parquet"
    metadata = {**table.schema.metadata, b"geo": json.dumps(geo).encode()}
    pq.write_table(table.replace_schema_metadata(metadata), path, row_group_size=100)
    return str(path)


def test_check_all_runs_what_gpio_check_all_runs(monkeypatch, unsorted_misdeclared):
    categories = []
    real = cli_check.all_checks_result

    def spy(structure, spatial, spec):
        categories.extend([*structure, "spatial", "spec"])
        return real(structure, spatial, spec)

    monkeypatch.setattr(cli_check, "all_checks_result", spy)
    exit_code = CliRunner().invoke(cli, ["check", "all", unsorted_misdeclared]).exit_code

    result = gpio.check_all(unsorted_misdeclared)

    assert (exit_code, result.verdict()) == (1, "failure")
    assert list(result.to_dict()) == categories
    assert result.to_dict()["spatial"]["passed"] is False
    assert any(f.startswith("[spec] ") for f in result.failures())
    # Table.check() stays the structure checks only.
    structure = set(gpio.read(unsorted_misdeclared).check().to_dict())
    assert structure == set(categories) - {"spatial", "spec"}


def _raises(message):
    def check(path, **_):
        raise RuntimeError(message)

    return check


def test_check_all_reports_a_stage_that_could_not_run(monkeypatch, unsorted_test_file):
    monkeypatch.setattr(check_api, "validate", _raises("spec reader exploded"))
    result = gpio.check_all(unsorted_test_file)

    assert result.verdict() == "failure"
    assert "spatial" in result.to_dict()
    assert any("Spec validation could not run" in f for f in result.failures())

    # An interrupt is a stop, never a check that failed.
    monkeypatch.setattr(check_api, "check_spatial", _raises("Query interrupted"))
    with pytest.raises(RuntimeError, match="interrupted"):
        gpio.check_all(unsorted_test_file)


# --------------------------------------------------------------------------
# Table.fix
# --------------------------------------------------------------------------


@pytest.fixture
def unsorted_copy(tmp_path, unsorted_test_file):
    path = tmp_path / "in.parquet"
    shutil.copy(unsorted_test_file, path)
    return path


def test_fix_returns_a_fixed_table_and_leaves_the_input_alone(unsorted_copy):
    before = unsorted_copy.read_bytes()
    table = gpio.read(unsorted_copy)

    fixed = table.fix()

    assert unsorted_copy.read_bytes() == before
    assert fixed is not table
    assert fixed.source_path is None  # never a view of a scratch file
    assert fixed.num_rows == table.num_rows
    ids = fixed.to_arrow().column("id")
    assert ids != table.to_arrow().column("id")  # Hilbert-sorted


def test_fix_into_output_returns_a_view_of_the_repaired_file(tmp_path, unsorted_copy):
    out = tmp_path / "fixed.parquet"

    fixed = gpio.read(unsorted_copy).fix(output=out)

    assert fixed.source_path == os.path.abspath(out)
    assert gpio.check_compression(out).passed()
    assert gpio.check_row_groups(out).passed()


def test_fix_refuses_to_write_over_its_source(unsorted_copy):
    alias = os.path.join(os.path.dirname(unsorted_copy), ".", unsorted_copy.name)

    with pytest.raises(gpio.InvalidParameterError, match="source"):
        gpio.read(unsorted_copy).fix(output=alias)


def test_fix_refuses_an_unknown_check(unsorted_copy):
    with pytest.raises(gpio.InvalidParameterError, match="bloom_filters"):
        gpio.read(unsorted_copy).fix(checks=["compression", "bloom_filters"])


def test_fix_honours_the_checks_subset(tmp_path, unsorted_copy):
    out = tmp_path / "fixed.parquet"

    gpio.read(unsorted_copy).fix(checks=["compression"], output=out)

    source = pq.read_table(unsorted_copy)
    fixed = pq.read_table(out)
    assert pc.all(pc.equal(fixed.column("id"), source.column("id"))).as_py()  # not sorted
    assert gpio.check_compression(out).passed()
