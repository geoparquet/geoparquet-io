"""A remote write goes to the configured S3 endpoint (HANDOFFS row 43).

``--s3-endpoint``/``--s3-no-ssl`` configured the reads (DuckDB's secret) but
not the upload: every remote output was handed to obstore without them, so a
MinIO or loopback destination was written to ``s3.<region>.amazonaws.com``
instead. One rule now: the upload's store is built from the active S3 config
unless the caller names an endpoint itself. The endpoint here is a loopback
server; nothing reaches the network.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from tests._fake_s3 import fake_s3

BUILDINGS = Path(__file__).parent / "data" / "buildings_test.parquet"


@pytest.fixture
def loopback(monkeypatch):
    for name, value in {
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(name, value)
    for name in ("AWS_PROFILE", "AWS_ENDPOINT_URL", "AWS_DEFAULT_REGION"):
        monkeypatch.delenv(name, raising=False)
    objects: dict[str, bytes] = {}
    with fake_s3(objects) as (endpoint, requests):
        yield endpoint, requests, objects


COMMANDS = {
    "convert_geoparquet": ["convert", "geoparquet", str(BUILDINGS)],
    "add_bbox": ["add", "bbox", str(BUILDINGS)],
    "extract": ["extract", str(BUILDINGS)],
}


@pytest.mark.parametrize("name", sorted(COMMANDS))
def test_a_remote_output_is_uploaded_to_the_configured_endpoint(name, loopback):
    endpoint, requests, objects = loopback
    args = [
        "--s3-endpoint",
        endpoint,
        "--s3-region",
        "us-east-1",
        "--s3-no-ssl",
        *COMMANDS[name],
        "s3://bkt/out.parquet",
    ]

    result = CliRunner().invoke(cli, args)

    assert result.exit_code == 0, f"{result.output[-800:]}\n{result.exception!r}"
    assert ("PUT", "/bkt/out.parquet") in requests, requests
    written = pa.BufferReader(objects["bkt/out.parquet"])
    assert pq.read_metadata(written).num_rows == pq.read_metadata(BUILDINGS).num_rows
