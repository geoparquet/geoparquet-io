"""Error and edge branches of --simplify-crs (#1197).

Dependency-light: pyproj and shapely are core dependencies, so these run on
every CI leg, including the ubuntu/3.11 coverage leg.
"""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.simplify import (
    _simplify_crs_transformers,
    simplify_file,
    simplify_table,
)

pytest.importorskip("coarsen", reason="simplify extra not installed")


def _sample_wkb():
    import shapely

    return shapely.to_wkb(shapely.box(0, 0, 1, 1))


class TestSimplifyCrsTransformerErrors:
    def test_unusable_carried_crs_is_a_clean_error(self):
        with pytest.raises(InvalidParameterError, match="input CRS unusable"):
            _simplify_crs_transformers("EPSG:32633", {"not": "projjson"}, _sample_wkb())

    def test_auto_utm_rejects_projected_input(self):
        from pyproj import CRS

        utm33 = CRS.from_epsg(32633).to_json_dict()
        with pytest.raises(InvalidParameterError, match="needs geographic"):
            _simplify_crs_transformers("auto-utm", utm33, _sample_wkb())

    def test_auto_utm_without_a_sample_transforms_nothing(self):
        forward, inverse = _simplify_crs_transformers("auto-utm", None, None)
        assert forward is None and inverse is None

    def test_all_null_column_with_auto_utm_passes_through(self):
        table = pa.table({"geometry": pa.array([None, None], type=pa.binary())})
        result = simplify_table(table, 10.0, simplify_crs="auto-utm")
        assert result.column("geometry").to_pylist() == [None, None]


class TestSimplifyCrsStreamingEdges:
    def test_zero_row_file_streams_with_simplify_crs(self, tmp_path):
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB"}},
        }
        table = pa.table({"geometry": pa.array([], type=pa.binary())}).replace_schema_metadata(
            {b"geo": json.dumps(geo).encode("utf-8")}
        )
        src = tmp_path / "empty.parquet"
        pq.write_table(table, str(src))
        out = tmp_path / "out.parquet"
        simplify_file(str(src), str(out), tolerance=10.0, simplify_crs="auto-utm")
        assert pq.ParquetFile(str(out)).metadata.num_rows == 0
