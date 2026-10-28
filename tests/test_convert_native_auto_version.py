"""Auto mode must not relabel a GeoArrow-native input as WKB (#1176).

``resolve_geoparquet_version_from_file`` reads the input's *version* (1.1.0) and
answered "1.1". The nested-list geometry then passed through the conversion
query untouched while the output's ``geo`` block declared ``encoding: WKB`` --
a file whose metadata contradicts its own schema. ``1.1-geoarrow`` is the
version that describes a native 1.1 file, so that is what auto resolves to.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import resolve_geoparquet_version_from_file
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.validate import validate_geoparquet

NATIVE_ENCODINGS = (
    "point",
    "linestring",
    "polygon",
    "multipoint",
    "multilinestring",
    "multipolygon",
)


def _geo_block(path):
    return json.loads(pq.read_schema(str(path)).metadata[b"geo"].decode("utf-8"))


@pytest.fixture(params=["point", "linestring", "polygon", "multipolygon"])
def native_input(request, test_data_dir):
    """A GeoParquet 1.1 file whose geometry uses native GeoArrow encoding."""
    return str(test_data_dir / f"data-{request.param}-encoding_native.parquet"), request.param


class TestAutoVersionOfANativeInput:
    def test_resolves_to_1_1_geoarrow(self, native_input):
        path, _encoding = native_input
        assert resolve_geoparquet_version_from_file(path) == "1.1-geoarrow"

    def test_a_wkb_1_1_input_still_resolves_to_1_1(self, test_data_dir):
        assert (
            resolve_geoparquet_version_from_file(str(test_data_dir / "buildings_test.parquet"))
            == "1.1"
        )


class TestConvertPreservesNativeEncoding:
    def test_block_and_schema_agree(self, native_input, tmp_path):
        path, encoding = native_input
        output = tmp_path / "out.parquet"

        convert_to_geoparquet(path, str(output))

        geo = _geo_block(output)
        column = geo["columns"][geo["primary_column"]]
        assert column["encoding"] in NATIVE_ENCODINGS, (
            f"native input was relabelled as {column['encoding']!r}"
        )
        assert column["encoding"] == encoding
        assert geo["version"].startswith("1.1")

        # The schema really is still nested, not WKB.
        field = pq.read_schema(str(output)).field(geo["primary_column"])
        assert not pa.types.is_binary(field.type) and not pa.types.is_large_binary(field.type), (
            f"geometry was rewritten as {field.type} while the block claims {column['encoding']}"
        )

    def test_output_passes_validation(self, native_input, tmp_path):
        path, _encoding = native_input
        output = tmp_path / "out.parquet"

        convert_to_geoparquet(path, str(output))

        result = validate_geoparquet(str(output))
        failed = [check.message for check in result.checks if check.status.value == "failed"]
        assert result.is_valid, f"output failed spec validation: {failed}"

    def test_rows_survive(self, native_input, tmp_path):
        path, _encoding = native_input
        output = tmp_path / "out.parquet"

        convert_to_geoparquet(path, str(output))

        assert pq.read_table(str(output)).num_rows == pq.read_table(path).num_rows

    def test_an_explicit_version_still_wins(self, native_input, tmp_path):
        """``--geoparquet-version 1.1`` asked for WKB; auto mode is what changed."""
        path, _encoding = native_input
        output = tmp_path / "out.parquet"

        convert_to_geoparquet(path, str(output), geoparquet_version="1.1")

        geo = _geo_block(output)
        assert geo["columns"][geo["primary_column"]]["encoding"] == "WKB"
