"""An exception's text never carries a credential to the user (#1034).

``extract wfs`` / ``extract carto`` build error lines out of the exception
that was caught, and the exception text of httpx / OWSLib / DuckDB embeds the
full request URL -- query string, ``api_key=`` and all. Every message that
reaches the user passes through ``clean_error_text`` once, at the owner.
"""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from tests.http_transport import FakeTransport, error_reply, wfs_capabilities
from tests.wfs_documents import MOCK_CAPABILITIES_XML

SECRET = "SUPERSECRETKEY123"
KEYED_SERVICE = f"https://geo.example.org/geoserver/wfs?api_key={SECRET}"
TYPENAME = "test:cities"
CAPABILITIES = MOCK_CAPABILITIES_XML.encode("utf-8")
SCHEMA = {
    "geometry": "Point",
    "geometry_column": "the_geom",
    "properties": {"the_geom": "Point", "name": "string"},
}


def _wfs(out, *extra):
    return CliRunner().invoke(
        cli,
        [
            "extract",
            "wfs",
            KEYED_SERVICE,
            TYPENAME,
            str(out),
            "--wfs-version",
            "1.1.0",
            "--skip-hilbert",
            *extra,
        ],
    )


def test_wfs_http_error_does_not_echo_the_api_key(monkeypatch, tmp_path):
    http = FakeTransport.install(monkeypatch)
    wfs_capabilities(monkeypatch, CAPABILITIES, SCHEMA)
    http.respond(lambda r: True, error_reply(400, body=f"bad request api_key={SECRET}".encode()))

    result = _wfs(tmp_path / "out.parquet")

    assert result.exit_code != 0
    assert SECRET not in result.output, result.output
    assert "Traceback" not in result.output


def test_wfs_capabilities_failure_does_not_echo_the_api_key(monkeypatch, tmp_path):
    import requests

    def _factory(url, version="1.0.0", **kwargs):
        raise requests.exceptions.ConnectionError(
            f"HTTPSConnectionPool: Max retries exceeded with url: {url}&api_key={SECRET} "
            "(connection refused)"
        )

    monkeypatch.setattr("owslib.wfs.WebFeatureService", _factory)

    result = _wfs(tmp_path / "out.parquet")

    assert result.exit_code != 0
    assert SECRET not in result.output, result.output
    assert "Traceback" not in result.output


def test_extract_metadata_warning_does_not_echo_a_url_credential(monkeypatch, tmp_path):
    """The best-effort metadata warning in ``extract`` prints the caught text."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from geoparquet_io.core import extract as extract_module

    src = tmp_path / "in.parquet"
    pq.write_table(pa.table({"id": [1]}), src)

    def _boom(*_args, **_kwargs):
        raise OSError(f"GET https://host/x.parquet?X-Amz-Signature={SECRET} failed")

    monkeypatch.setattr(extract_module, "get_parquet_metadata", _boom)

    result = CliRunner().invoke(
        cli, ["extract", "geoparquet", str(src), str(tmp_path / "o.parquet")]
    )

    assert SECRET not in result.output, result.output


@pytest.mark.parametrize("flag", [None])
def test_carto_exhausted_retries_does_not_echo_the_api_key(monkeypatch, tmp_path, flag):
    import duckdb

    from geoparquet_io.core import carto as carto_module

    class _Conn:
        def execute(self, *_a, **_k):
            return self

        def sql(self, query):
            raise duckdb.IOException(f"IO Error: Could not open GDAL dataset at: {query}")

    monkeypatch.setattr(carto_module, "get_duckdb_connection", lambda *a, **k: _Conn())
    monkeypatch.setattr(
        carto_module,
        "_probe_request_status",
        lambda *_a, **_k: carto_module._CartoStatus(None, False),
    )
    monkeypatch.setattr("time.sleep", lambda *_a: None)

    result = CliRunner().invoke(
        cli,
        [
            "extract",
            "carto",
            "https://user.carto.com/api/v2/sql",
            "cities",
            str(tmp_path / "o.parquet"),
            "--api-key",
            SECRET,
            "--max-retries",
            "2",
        ],
    )

    assert result.exit_code != 0
    assert SECRET not in result.output, result.output
