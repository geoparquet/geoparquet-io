"""Tests for converting GeoParquet files with GeoArrow native geometry encoding.

GeoArrow native encoding stores geometries as nested Arrow structs/arrays
rather than WKB blobs. DuckDB reads these as STRUCT(x DOUBLE, y DOUBLE)[N]
types, which are incompatible with ST_XMin/ST_YMin/etc. without conversion.
"""

import os

import pytest

from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.exceptions import InvalidParameterError

GEOARROW_NATIVE_TYPES = [
    "point",
    "linestring",
    "polygon",
    "multipoint",
    "multilinestring",
    "multipolygon",
]


@pytest.fixture
def native_parquet(test_data_dir, request):
    """Return path to GeoArrow native-encoded parquet test file."""
    geom_type = request.param
    return str(test_data_dir / f"data-{geom_type}-encoding_native.parquet")


class TestConvertGeoArrowNative:
    """Converting GeoParquet files with GeoArrow native geometry encoding."""

    @pytest.mark.parametrize("geom_type", GEOARROW_NATIVE_TYPES)
    def test_convert_native_encoding_succeeds(self, geom_type, test_data_dir, tmp_path):
        """Converting a GeoArrow native-encoded file should not raise a type error."""
        input_file = str(test_data_dir / f"data-{geom_type}-encoding_native.parquet")
        output_file = str(tmp_path / f"output_{geom_type}.parquet")

        # This previously failed with:
        # "No function matches 'ST_XMin(STRUCT(x DOUBLE, y DOUBLE)[N])'"
        convert_to_geoparquet(input_file, output_file, verbose=False)

        assert os.path.exists(output_file)
        assert os.path.getsize(output_file) > 0

    @pytest.mark.parametrize("geom_type", GEOARROW_NATIVE_TYPES)
    def test_convert_native_encoding_with_version_2_is_refused(
        self, geom_type, test_data_dir, tmp_path
    ):
        """GeoParquet 2.0 is WKB-only, and nothing in gpio re-encodes a struct.

        This used to "succeed": the coordinate struct passed through and the
        geo key was dropped, leaving a file with no geometry identity at all --
        ``gpio check spec`` reads it as "No GeoParquet metadata or Parquet geo
        types found". The facade refuses the request instead (#1438).
        """
        input_file = str(test_data_dir / f"data-{geom_type}-encoding_native.parquet")
        output_file = str(tmp_path / f"output_{geom_type}_v2.parquet")

        with pytest.raises(InvalidParameterError, match="1.1-geoarrow"):
            convert_to_geoparquet(input_file, output_file, geoparquet_version="2.0", verbose=False)

        assert not os.path.exists(output_file)
