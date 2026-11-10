"""The aggregate entry points must load their grid extension via the helper.

Both ran raw ``INSTALL a5 FROM community`` / ``LOAD a5`` and so skipped the
telemetry opt-out that issue #779 added, which is what made
``gpio process aggregate a5`` keep exiting 139 after ``gpio add a5`` was fixed.

These stay in the fast suite deliberately: the real path installs a community
extension over the network, so without a stub the two changed lines are covered
only by the slow lane and the diff-coverage gate fails.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core import index_ops
from geoparquet_io.core.process.aggregate import grid_common
from geoparquet_io.core.process.aggregate.by_a5 import A5_SCHEME


class _Loaded(Exception):
    """Raised by the stub so each test stops exactly at the extension load."""


@pytest.fixture
def record_extension_load(monkeypatch):
    """Replace the helper with a stub that records its arguments and stops."""
    seen = {}

    def fake_load(con, name, feature=None):
        seen["name"] = name
        seen["feature"] = feature
        raise _Loaded

    monkeypatch.setattr(index_ops, "load_community_extension", fake_load)
    return seen


def _empty_geometry_table():
    return pa.table({"geometry": pa.array([], type=pa.binary())})


def test_aggregate_grid_table_loads_through_the_helper(record_extension_load):
    with pytest.raises(_Loaded):
        grid_common.aggregate_grid_table(A5_SCHEME, _empty_geometry_table(), resolution=5)

    assert record_extension_load["name"] == "a5"
    assert record_extension_load["feature"] == "a5 aggregation"


def test_aggregate_grid_file_loads_through_the_helper(record_extension_load, tmp_path):
    src = tmp_path / "in.parquet"
    pq.write_table(_empty_geometry_table(), src)

    with pytest.raises(_Loaded):
        grid_common.aggregate_grid_file(
            A5_SCHEME,
            str(src),
            str(tmp_path / "out.parquet"),
            resolution=5,
        )

    assert record_extension_load["name"] == "a5"
    assert record_extension_load["feature"] == "a5 aggregation"
