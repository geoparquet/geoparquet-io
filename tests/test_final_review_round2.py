"""Public-path regressions from the second final review of #1367."""

import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.exception_handler import cli_error_for
from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import RemoteAccessError
from geoparquet_io.core.file_utils import copy_file
from tests._defrag_fixtures import footer_geo, geo_block, point_wkb, write_with_geo


@pytest.mark.parametrize("version", ["1.1", "2.0"])
@pytest.mark.parametrize("strategy", ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"])
@pytest.mark.parametrize("only_empty", [False, True])
def test_empty_geometry_types_survive_every_write(tmp_path, version, strategy, only_empty):
    values = [struct.pack("<BII", 1, 3, 0), None]
    if not only_empty:
        values.append(point_wkb(1, 2))
    src = write_with_geo(
        tmp_path / "input.parquet",
        pa.table({"geometry": pa.array(values, pa.binary())}),
        geo_block(geometry_types=["Point", "Polygon"]),
    )
    out = tmp_path / "output.parquet"
    gpio.read(str(src)).write(str(out), write_strategy=strategy, geoparquet_version=version)
    meta = footer_geo(out)["columns"]["geometry"]
    assert set(meta["geometry_types"]) == ({"Polygon"} if only_empty else {"Point", "Polygon"})
    assert meta.get("bbox") == (None if only_empty else [1.0, 2.0, 1.0, 2.0])


def test_sort_csv_names_the_actual_format(tmp_path):
    src = tmp_path / "t.csv"
    src.write_text("x,y\n1,2\n")
    result = CliRunner().invoke(cli, ["sort", "hilbert", str(src), str(tmp_path / "out.parquet")])
    assert result.exit_code == 1
    assert "not a Parquet file; this command reads .parquet" in result.output
    assert "has a .parquet extension" not in result.output


@pytest.mark.parametrize("link", ["__cause__", "__context__"])
def test_not_parquet_context_does_not_claim_an_unrelated_failure(link):
    error = RuntimeError("later failure")
    setattr(error, link, duckdb.IOException("No magic bytes found at end of file"))
    assert cli_error_for(error, path="t.csv") is None


def test_extract_failed_overwrite_preserves_existing_output(tmp_path):
    src = write_with_geo(
        tmp_path / "input.parquet",
        pa.table({"geometry": [point_wkb(1, 2)]}),
        geo_block(),
    )
    out = tmp_path / "out.parquet"
    original = src.read_bytes()
    out.write_bytes(original)
    result = CliRunner().invoke(
        cli, ["extract", str(src), str(out), "--where", "nope > 1", "--overwrite"]
    )
    assert result.exit_code != 0
    assert out.read_bytes() == original


@pytest.mark.parametrize("column", ["geometry", "Geometry"])
@pytest.mark.parametrize("command", [["convert", "geoparquet"], ["add", "bbox"]])
def test_plain_binary_geometry_without_geo_key(tmp_path, column, command):
    src = tmp_path / "plain.parquet"
    pq.write_table(pa.table({column: pa.array([point_wkb(1, 2), None], pa.binary())}), src)
    out = tmp_path / "output.parquet"
    result = CliRunner().invoke(cli, [*command, str(src), str(out)])
    assert result.exit_code == 0, f"{result.output}\n{result.exception!r}"
    meta = footer_geo(out)["columns"][column]
    assert meta["geometry_types"] == ["Point"]
    assert meta["bbox"] == [1.0, 2.0, 1.0, 2.0]
    assert pq.read_table(out)["bbox"].to_pylist() == [
        {"xmin": 1.0, "ymin": 2.0, "xmax": 1.0, "ymax": 2.0},
        None,
    ]


def test_http_copy_retry_exhaustion_sanitizes_presigned_url(tmp_path):
    class Unavailable(BaseHTTPRequestHandler):
        attempts = 0

        def do_GET(self):
            type(self).attempts += 1
            self.send_response(503)
            self.send_header("Content-Length", "0")
            self.send_header("Retry-After", "0")
            self.end_headers()

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Unavailable)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    out = tmp_path / "old.parquet"
    out.write_bytes(b"keep me")
    query = "X-Amz-Credential=private-credential&X-Amz-Signature=private-signature"
    try:
        with pytest.raises(RemoteAccessError) as caught:
            copy_file(f"http://127.0.0.1:{server.server_port}/x.parquet?{query}", str(out))
        assert Unavailable.attempts == 3
        message = str(caught.value)
        assert "503" in message
        assert query not in message
        assert "private-credential" not in message
        assert "private-signature" not in message
        assert out.read_bytes() == b"keep me"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize("status, attempts", [(503, 3), (404, 1)])
def test_http_copy_error_boundary_without_socket(tmp_path, monkeypatch, status, attempts):
    import httpx

    from geoparquet_io.core import http_retry

    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(status)

    out = tmp_path / "existing.parquet"
    out.write_bytes(b"original")
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        monkeypatch.setattr(http_retry, "get_shared_http_client", lambda: client)
        with pytest.raises(RemoteAccessError) as caught:
            copy_file("https://example.com/x.parquet?X-Amz-Signature=secret-signature", str(out))
    assert len(requests) == attempts
    assert "secret-signature" not in str(caught.value)
    assert "X-Amz-Signature" not in str(caught.value)
    assert ("503" if status == 503 else "not found") in str(caught.value).lower()
    assert out.read_bytes() == b"original"
