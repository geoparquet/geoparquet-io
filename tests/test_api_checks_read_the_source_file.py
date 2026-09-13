"""``Table.check_*`` and ``validate()`` measure the file ``gpio.read()`` opened.

A Table with no file behind it (built in memory, read with ``columns=`` or
``filters=``, derived by an operation, or whose file changed since it was
read) answers prospectively: the verdict describes what ``write()`` would
produce, and says so. What the two answers *are* on the same bytes is pinned
against the CLI in ``tests/test_api_cli_behaviour_parity.py``.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1060,
https://github.com/geoparquet/geoparquet-io/issues/1061
"""

from __future__ import annotations

import os
import shutil

import pyarrow.parquet as pq
import pytest

import geoparquet_io as gpio
from geoparquet_io.api.check import CheckResult

CHECKS = [
    "check",
    "check_spatial",
    "check_spatial_pushdown",
    "check_compression",
    "check_bbox",
    "check_row_groups",
    "check_bloom_filters",
    "check_optimization",
    "validate",
]


def _no_temp_write(monkeypatch, why: str) -> None:
    monkeypatch.setattr(gpio.Table, "_with_temp_file", lambda *a, **k: pytest.fail(why))


@pytest.mark.parametrize("method", CHECKS)
def test_a_read_table_is_checked_on_its_file_without_a_temp_write(
    monkeypatch, unsorted_test_file, method
):
    _no_temp_write(monkeypatch, f"{method} re-wrote the rows instead of reading the file")

    result = getattr(gpio.read(unsorted_test_file), method)()

    assert result.source_path == os.path.abspath(unsorted_test_file)
    assert result.prospective is False
    assert "prospective" not in repr(result)


def test_a_file_check_reports_the_files_own_layout(unsorted_test_file):
    """The fact behind #1060: 15 SNAPPY row groups, not one ZSTD re-write."""
    table = gpio.read(unsorted_test_file)
    parquet = pq.ParquetFile(unsorted_test_file)
    geometry_index = parquet.schema_arrow.get_field_index("geometry")

    assert table.check_row_groups().to_dict()["stats"]["num_groups"] == parquet.num_row_groups
    assert (
        table.check_compression().to_dict()["current_compression"]
        == parquet.metadata.row_group(0).column(geometry_index).compression
    )
    assert table.validate().to_dict()["file_path"] == os.path.abspath(unsorted_test_file)


@pytest.mark.parametrize(
    "make",
    [
        lambda path: gpio.Table(pq.read_table(path)),
        lambda path: gpio.read(path, columns=["geometry"]),
        lambda path: gpio.read(path, filters=[("fsq_place_id", "!=", "")]),
        lambda path: gpio.read(path).head(10),
        lambda path: gpio.read(path).sort_hilbert(),
    ],
    ids=["in_memory", "columns_kwarg", "filters_kwarg", "head", "sort_hilbert"],
)
def test_a_table_with_no_file_behind_it_answers_prospectively(places_test_file, make):
    table = make(places_test_file)

    assert table.source_path is None
    result = table.check_row_groups()
    assert result.source_path is None
    assert result.prospective is True
    assert "prospective" in repr(result)
    assert result.to_dict()["stats"]["num_groups"] >= 1  # it still answers


def test_a_prospective_check_describes_what_write_would_produce(tmp_path, places_test_file):
    """The temp write uses ``write()``'s own defaults, so the answer is the file the caller would get."""
    derived = gpio.read(places_test_file).head(200)
    written = gpio.read(str(derived.write(tmp_path / "out.parquet")))

    prospective = derived.check_compression().to_dict()
    measured = written.check_compression().to_dict()
    assert prospective == measured
    assert derived.validate().to_dict()["detected_version"] == written.geoparquet_version


def test_a_source_that_changed_since_read_is_not_trusted(
    tmp_path, unsorted_test_file, places_test_file
):
    path = tmp_path / "a.parquet"
    shutil.copy(unsorted_test_file, path)
    table = gpio.read(path)
    shutil.copy(places_test_file, path)  # different bytes at the same path

    result = table.check_row_groups()

    assert result.prospective is True
    assert result.to_dict()["stats"]["total_rows"] == table.num_rows


def test_a_removed_source_falls_back_to_the_rows_in_memory(tmp_path, unsorted_test_file):
    path = tmp_path / "a.parquet"
    shutil.copy(unsorted_test_file, path)
    table = gpio.read(path)
    path.unlink()

    assert table.check_row_groups().prospective is True


def test_a_relative_path_survives_a_chdir(tmp_path, unsorted_test_file, monkeypatch):
    monkeypatch.chdir(os.path.dirname(unsorted_test_file))
    table = gpio.read(os.path.basename(unsorted_test_file))
    monkeypatch.chdir(tmp_path)

    assert table.check_row_groups().prospective is False


def test_a_remote_source_is_answered_prospectively_not_refetched(monkeypatch, unsorted_test_file):
    real_read_table = pq.read_table
    monkeypatch.setattr(
        pq, "read_table", lambda path, **kwargs: real_read_table(unsorted_test_file)
    )
    table = gpio.read("s3://bucket/x.parquet")

    assert table.source_path == "s3://bucket/x.parquet"
    assert table.check_row_groups().prospective is True


class TestGeoparquetVersionReportsWhatTheFileDeclares:
    def test_the_declared_string_verbatim(self, places_test_file, places_v11_file):
        assert gpio.read(places_test_file).geoparquet_version == "1.0.0"
        assert gpio.read(places_v11_file).geoparquet_version == "1.1.0"

    def test_metadata_and_info_agree(self, places_test_file):
        table = gpio.read(places_test_file)
        assert table.metadata()["geoparquet_version"] == "1.0.0"
        assert table.info(verbose=False)["geoparquet_version"] == "1.0.0"

    def test_a_plain_parquet_table_has_none(self):
        import pyarrow as pa

        assert gpio.Table(pa.table({"id": [1]})).geoparquet_version is None

    def test_write_still_upgrades_a_1_0_input_to_1_1(self, tmp_path, places_test_file):
        out = gpio.read(places_test_file).write(tmp_path / "out.parquet")
        assert gpio.read(str(out)).geoparquet_version.split(".")[:2] == ["1", "1"]


def test_a_result_built_without_a_source_is_prospective_and_its_dict_is_raw():
    results = {"passed": True, "issues": []}
    result = CheckResult(results, check_type="stac")

    assert result.prospective is True
    assert result.to_dict() is results


def test_validate_stac_names_the_file_it_read(tmp_path):
    item = tmp_path / "item.json"
    item.write_text("{}")
    result = gpio.validate_stac(item)

    assert result.source_path == str(item)
    assert result.prospective is False
