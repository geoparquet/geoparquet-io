"""Auto mode's "1.1-geoarrow" must survive paths that pass no ``geometry_info`` (#1176).

``resolve_geoparquet_version_from_file`` resolves a GeoArrow-native 1.1 input to
``1.1-geoarrow``, but the write funnel's already-native guard read that fact out
of ``geometry_info`` -- and only ``convert`` builds one. On sort, extract,
partition and the ``add`` family the guard therefore saw nothing, rerouted a
native input through arrow-streaming as if it were WKB, dropped
``--write-memory`` with a warning, and wrote a ``geo`` block declaring
``encoding: WKB`` over a still-nested GeoArrow column.

The guard now asks the query itself: a geometry column whose DuckDB type is not
``GEOMETRY``/``GEOGRAPHY``/``BLOB``/``VARCHAR`` is already native, whoever the
caller is.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.geo_metadata import GEOARROW_ENCODINGS
from geoparquet_io.core.validate import validate_geoparquet


def _geo_block(path):
    return json.loads(pq.read_schema(str(path)).metadata[b"geo"].decode("utf-8"))


def _assert_still_native(output):
    """The written file's block and schema both say GeoArrow, and it validates."""
    geo = _geo_block(output)
    primary = geo["primary_column"]
    encoding = geo["columns"][primary]["encoding"]
    assert encoding in GEOARROW_ENCODINGS, f"native input was relabelled as {encoding!r}"

    field = pq.read_schema(str(output)).field(primary)
    assert not pa.types.is_binary(field.type) and not pa.types.is_large_binary(field.type), (
        f"geometry was rewritten as {field.type} while the block claims {encoding}"
    )

    result = validate_geoparquet(str(output))
    failed = [check.message for check in result.checks if check.status.value == "failed"]
    assert result.is_valid, f"output failed spec validation: {failed}"


@pytest.fixture(params=["point", "polygon"])
def native_input(request, test_data_dir):
    """A GeoParquet 1.1 file whose geometry uses native GeoArrow encoding."""
    return str(test_data_dir / f"data-{request.param}-encoding_native.parquet")


class TestNativeEncodingSurvivesEveryWritePath:
    """No ``--geoparquet-version``: auto resolves 1.1-geoarrow, and the output
    must really be GeoArrow rather than WKB-labelled nested lists."""

    def test_extract_geoparquet(self, native_input, tmp_path):
        output = tmp_path / "extracted.parquet"
        result = CliRunner().invoke(cli, ["extract", "geoparquet", native_input, str(output)])
        assert result.exit_code == 0, result.output
        _assert_still_native(output)

    def test_sort_column(self, native_input, tmp_path):
        output = tmp_path / "sorted.parquet"
        result = CliRunner().invoke(cli, ["sort", "column", native_input, str(output), "col"])
        assert result.exit_code == 0, result.output
        _assert_still_native(output)

    def test_partition_string(self, native_input, tmp_path):
        output_dir = tmp_path / "partitions"
        result = CliRunner().invoke(
            cli,
            [
                "partition",
                "string",
                native_input,
                str(output_dir),
                "--column",
                "col",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0, result.output
        written = sorted(output_dir.rglob("*.parquet"))
        assert written, f"no partition files written: {result.output}"
        for part in written:
            _assert_still_native(part)


class TestWriteMemoryIsHonouredForANativeInput:
    """The limit was only dropped because the guard mistook native for WKB: a
    native input keeps the duckdb-kv strategy, so the limit stands (#663/#1176)."""

    def test_sort_column_keeps_the_limit(self, native_input, tmp_path):
        output = tmp_path / "sorted.parquet"
        result = CliRunner().invoke(
            cli,
            ["sort", "column", native_input, str(output), "col", "--write-memory", "512MB"],
        )
        assert result.exit_code == 0, result.output
        assert "--write-memory" not in result.output, (
            f"--write-memory was dropped for a native input: {result.output}"
        )
        _assert_still_native(output)
