"""Deterministic local inputs for the performance matrix."""

import hashlib
from urllib.request import Request, urlopen

from geoparquet_io.benchmarks.workloads import generate_workload, serve_workload


def test_workload_reproducible(tmp_path):
    def digest(path):
        files = sorted(path.rglob("*")) if path.is_dir() else [path]
        return [hashlib.sha256(file.read_bytes()).hexdigest() for file in files if file.is_file()]

    for name in ["W1", "W2", "W2-skew", "W3", "W4", "W5", "W6"]:
        first = generate_workload(name, tmp_path / name / "first")
        second = generate_workload(name, tmp_path / name / "second")
        assert digest(first) == digest(second), name
        assert digest(first), name


def test_http_ranges_and_wfs_pages(tmp_path):
    path = generate_workload("W4", tmp_path)
    with serve_workload(path) as url:
        with urlopen(Request(url, headers={"Range": "bytes=0-3"})) as response:
            assert response.status == 206
            assert response.read() == b"PAR1"
    path = generate_workload("W6", tmp_path)
    with serve_workload(path) as url:
        with urlopen(url + "?request=GetFeature&startIndex=1&count=2") as response:
            import json

            page = json.load(response)
            assert len(page["features"]) == 2
            assert page["features"][0]["id"] == "points.1"


def test_wide_workload_has_many_declared_geometry_columns(tmp_path):
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq

    path = generate_workload("W5", tmp_path, "small")
    schema = pq.read_schema(path)
    geometries = json.loads(schema.metadata[b"geo"])["columns"]
    assert len(geometries) > 1
    assert all(pa.types.is_binary(schema.field(name).type) for name in geometries)
