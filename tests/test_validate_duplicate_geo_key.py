"""``gpio check spec`` fails a footer that carries more than one ``geo`` entry (#1229).

DuckDB's ``COPY ... GEOPARQUET_VERSION 'V1'/'V2', KV_METADATA {geo: ...}`` writes
the supplied ``geo`` entry and then appends its own, which is how #1225 shipped
files with two entries that disagree. Readers resolve the duplicate differently,
and ``get_kv_metadata()`` returns a dict, so every check before this one saw a
single entry and the validator passed those files clean.

Fixtures come straight from that DuckDB behaviour rather than from gpio's
writers, which no longer produce the defect (#1227).
"""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_metadata import count_kv_key
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
from geoparquet_io.core.validate import CheckStatus, validate_geoparquet


def _geo_block(version: str) -> str:
    return json.dumps(
        {
            "version": version,
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
        }
    )


def _write(path, duckdb_version: str, kv_geo: str | None) -> str:
    """One point, written by DuckDB, optionally with a supplied `geo` entry."""
    kv = ""
    if kv_geo is not None:
        kv = ", KV_METADATA {geo: '" + kv_geo.replace("'", "''") + "'}"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            f"COPY (SELECT ST_Point(1, 2) AS geometry) TO {sql_path(str(path))} "
            f"(FORMAT PARQUET, GEOPARQUET_VERSION '{duckdb_version}'{kv})"
        )
    finally:
        con.close()
    return str(path)


@pytest.fixture(params=[("V1", "1.1.0"), ("V2", "2.0.0")], ids=["1.1", "2.0"])
def duplicated(request, tmp_path):
    duckdb_version, geo_version = request.param
    return _write(tmp_path / "dup.parquet", duckdb_version, _geo_block(geo_version))


@pytest.fixture(params=["V1", "V2"], ids=["1.1", "2.0"])
def single(request, tmp_path):
    return _write(tmp_path / "single.parquet", request.param, None)


def _check(result, name):
    matches = [c for c in result.checks if c.name == name]
    assert len(matches) == 1, [c.name for c in result.checks]
    return matches[0]


def test_count_kv_key_sees_every_entry(duplicated):
    assert count_kv_key(duplicated, "geo") == 2


def test_count_kv_key_on_a_single_entry_and_a_missing_key(single):
    assert count_kv_key(single, "geo") == 1
    assert count_kv_key(single, "no-such-key") == 0


@pytest.mark.parametrize("validate_data", [True, False], ids=["data", "metadata-only"])
def test_a_duplicated_geo_key_fails(duplicated, validate_data):
    result = validate_geoparquet(duplicated, validate_data=validate_data)

    check = _check(result, "geo_key_unique")
    assert check.status == CheckStatus.FAILED
    assert check.category == "core_metadata"
    assert "2" in check.message and '"geo"' in check.message
    assert not result.is_valid


def test_a_single_geo_key_passes(single):
    result = validate_geoparquet(single, validate_data=False)

    assert _check(result, "geo_key_unique").status == CheckStatus.PASSED


def test_the_cli_exits_1(duplicated):
    result = CliRunner().invoke(cli, ["check", "spec", duplicated])

    assert result.exit_code == 1, result.output
    assert '"geo"' in result.output


def test_a_duplicate_duckdb_cannot_parse_is_still_counted(tmp_path):
    """DuckDB refuses to read a footer whose `geo` value it cannot parse, so a
    local file is counted from its raw footer instead."""
    path = _write(tmp_path / "unparseable.parquet", "V2", json.dumps({"columns": "nope"}))

    result = validate_geoparquet(path, validate_data=False)

    assert _check(result, "geo_key_unique").status == CheckStatus.FAILED
    assert not result.is_valid


def test_an_empty_duplicate_on_a_native_file_still_fails(tmp_path):
    """An empty ``geo`` object reads as no GeoParquet metadata, so the file is
    checked as parquet-geo-only, and the duplicate must still fail there."""
    path = _write(tmp_path / "empty.parquet", "V2", "{}")

    result = validate_geoparquet(path, validate_data=False)

    assert _check(result, "geo_key_unique").status == CheckStatus.FAILED
    assert not result.is_valid


def test_a_duplicate_fails_when_parquet_geo_only_is_forced(duplicated):
    result = validate_geoparquet(duplicated, validate_data=False, target_version="parquet-geo-only")

    assert _check(result, "geo_key_unique").status == CheckStatus.FAILED
    assert not result.is_valid
