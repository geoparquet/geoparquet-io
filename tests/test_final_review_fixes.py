"""Regressions for the final #1367 correctness review."""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyproj import CRS
from shapely import Point, to_wkb

from geoparquet_io.api import ops
from geoparquet_io.api.table import Table
from geoparquet_io.core.crs_utils import crs_equal
from geoparquet_io.core.exceptions import PartitionError
from geoparquet_io.core.validate import CheckStatus, validate_geoparquet
from geoparquet_io.core.write_funnels import write_geoparquet_table


@pytest.mark.parametrize("strategy", ["in-memory", "streaming", "duckdb-kv", "disk-rewrite"])
@pytest.mark.parametrize("version", ["1.1", "2.0"])
@pytest.mark.parametrize("secondary", [None, CRS.from_epsg(3857).to_json_dict()])
@pytest.mark.parametrize("declaration", [{"orientation": "counterclockwise"}, {"epoch": 2020.0}])
def test_table_write_preserves_resolved_column_crs(
    tmp_path, strategy, version, secondary, declaration
):
    """``Table.write`` half of #1400; ``write_geoparquet_table`` is A4's
    ``{orientation,epoch}_{projected,unknown}_secondary`` shapes
    (tests/test_write_contract_two_geometry.py)."""
    columns = {
        "geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": None, **declaration},
        "other": {"encoding": "WKB", "geometry_types": ["Point"], "crs": secondary},
    }
    table = pa.table(
        {"geometry": [to_wkb(Point(1, 2))], "other": [to_wkb(Point(100, 200))]}
    ).replace_schema_metadata(
        {
            b"geo": json.dumps(
                {"version": "1.1.0", "primary_column": "geometry", "columns": columns}
            ).encode()
        }
    )
    out = tmp_path / "out.parquet"
    Table(table).write(out, write_strategy=strategy, geoparquet_version=version)
    result = json.loads(pq.read_metadata(out).metadata[b"geo"])["columns"]
    assert result["geometry"]["crs"] is None
    assert result["other"]["crs"] == secondary


@pytest.mark.parametrize("latitude", [float("nan"), float("inf"), float("-inf")])
def test_quadkey_nonfinite(latitude):
    table = pa.table({"geometry": [to_wkb(Point(0, latitude))]})
    if latitude != latitude:
        assert ops.add_quadkey(table, resolution=3)["quadkey"].to_pylist() == [None]
    else:
        with pytest.raises(Exception, match="non-finite.*infinite"):
            ops.add_quadkey(table, resolution=3)


@pytest.mark.parametrize("latitude", [float("nan"), float("inf"), float("-inf")])
@pytest.mark.parametrize("path", ["sort", "partition"])
def test_quadkey_nonfinite_sort_partition(tmp_path, latitude, path):
    table = pa.table({"geometry": [to_wkb(Point(0, latitude))]})

    def run():
        if path == "sort":
            return ops.sort_quadkey(table, resolution=3)
        ops.partition_by_quadkey(
            table,
            tmp_path / "partitions",
            resolution=3,
            partition_resolution=3,
            keep_quadkey_column=True,
        )
        files = list((tmp_path / "partitions").rglob("*.parquet"))
        return pq.read_table(files) if files else None

    if latitude != latitude and path == "partition":
        with pytest.raises(PartitionError, match="No non-NULL values"):
            run()
    elif latitude != latitude:
        result = run()
        if result is not None:
            assert result["quadkey"].to_pylist() == [None]
    else:
        with pytest.raises(Exception, match="non-finite.*infinite"):
            run()


def test_quadkey_null_geometry():
    table = pa.table({"geometry": pa.array([None], type=pa.binary())})
    assert ops.add_quadkey(table, resolution=3)["quadkey"].to_pylist() == [None]


def _wkb_field(name: str, crs: dict | None = None) -> pa.Field:
    """A plain ``binary`` field whose GeoArrow identity lives only in field metadata."""
    metadata = {b"ARROW:extension:name": b"geoarrow.wkb"}
    if crs is not None:
        metadata[b"ARROW:extension:metadata"] = json.dumps({"crs": crs}).encode()
    return pa.field(name, pa.binary(), metadata=metadata)


@pytest.mark.parametrize("strategy", ["in-memory", "streaming", "duckdb-kv", "disk-rewrite", None])
@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_table_write_keeps_a_field_metadata_only_secondary_crs(tmp_path, version, strategy):
    """A secondary whose CRS is only in raw ``ARROW:extension:metadata``: reading
    just ``field.type.crs`` wrote ``other`` with no crs, i.e. OGC:CRS84, on every
    strategy (only #1400's in-memory probe caught it, and only at 2.0)."""
    epsg_3857 = CRS.from_epsg(3857).to_json_dict()
    schema = pa.schema([_wkb_field("geometry"), _wkb_field("other", epsg_3857)])
    table = pa.table(
        {"geometry": [to_wkb(Point(1, 2))], "other": [to_wkb(Point(1e6, 2e6))]}, schema=schema
    )
    out = tmp_path / "out.parquet"
    write_geoparquet_table(table, str(out), geoparquet_version=version, write_strategy=strategy)

    other = json.loads(pq.read_schema(out).metadata[b"geo"])["columns"]["other"]
    assert crs_equal(other.get("crs"), epsg_3857), other
    result = validate_geoparquet(str(out))
    assert result.is_valid, [c.message for c in result.checks if c.status == CheckStatus.FAILED]
