"""``classify_failure``: exception type and structured fields -> failure kind.

The table is the contract WFS, Carto and ArcGIS share. Message text is never
read, which the last group pins directly: the same text on a different type
classifies differently, and a misleading message on the right type is ignored.
"""

from __future__ import annotations

import json
import socket
import xml.etree.ElementTree as ET

import duckdb
import httpx
import pytest
import requests

from geoparquet_io.core.exceptions import FailureClass, FailureKind, classify_failure


def _http_status_error(code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://h/x?api_key=SECRET")
    return httpx.HTTPStatusError(
        "boom", request=request, response=httpx.Response(code, request=request)
    )


def _requests_http_error(code: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = code
    return requests.HTTPError("boom", response=response)


def _with_status(code):
    exc = RuntimeError("boom")
    exc.status_code = code
    return exc


def _lxml_error():
    from lxml import etree

    try:
        etree.fromstring("<a><b></a>")
    except etree.XMLSyntaxError as exc:
        return exc
    raise AssertionError("lxml accepted broken xml")  # pragma: no cover


L, T, U, H, M, X = (
    FailureKind.LOCAL,
    FailureKind.TIMEOUT,
    FailureKind.UNREACHABLE,
    FailureKind.HTTP_STATUS,
    FailureKind.MALFORMED_RESPONSE,
    FailureKind.UNKNOWN,
)

TABLE = [
    (MemoryError(), FailureClass(L)),
    (duckdb.OutOfMemoryException("oom"), FailureClass(L)),
    (duckdb.InterruptException("stop"), FailureClass(L)),
    (duckdb.BinderException("bad"), FailureClass(L)),
    (duckdb.CatalogException("bad"), FailureClass(L)),
    (duckdb.ParserException("bad"), FailureClass(L)),
    (httpx.ReadTimeout("t"), FailureClass(T)),
    (httpx.ConnectTimeout("t"), FailureClass(T)),
    (requests.exceptions.ReadTimeout("t"), FailureClass(T)),
    (requests.exceptions.ConnectTimeout("t"), FailureClass(T)),
    (TimeoutError("t"), FailureClass(T)),
    (httpx.ConnectError("c"), FailureClass(U)),
    (httpx.RemoteProtocolError("p"), FailureClass(U)),
    (requests.exceptions.ConnectionError("c"), FailureClass(U)),
    (ConnectionRefusedError("c"), FailureClass(U)),
    (socket.gaierror("dns"), FailureClass(U)),
    (_http_status_error(404), FailureClass(H, 404)),
    (_http_status_error(503), FailureClass(H, 503)),
    (_requests_http_error(401), FailureClass(H, 401)),
    (_with_status(429), FailureClass(H, 429)),
    (_with_status(0), FailureClass(X)),
    (_with_status("404"), FailureClass(X)),
    (ET.ParseError("not well-formed"), FailureClass(M)),
    (json.JSONDecodeError("bad", "{", 0), FailureClass(M)),
    (UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad"), FailureClass(M)),
    (_lxml_error(), FailureClass(M)),
    (RuntimeError("anything"), FailureClass(X)),
    (ValueError("x"), FailureClass(X)),
]


@pytest.mark.parametrize(
    ("exc", "expected"), TABLE, ids=[f"{type(e).__name__}-{i}" for i, (e, _) in enumerate(TABLE)]
)
def test_failure_table(exc, expected):
    assert classify_failure(exc) == expected


def test_a_wrapped_library_error_classifies_through_its_cause():
    try:
        try:
            raise httpx.ConnectError("refused")
        except httpx.ConnectError as inner:
            raise RuntimeError("wrapper") from inner
    except RuntimeError as outer:
        assert classify_failure(outer) == FailureClass(U)


@pytest.mark.parametrize("text", ["connection timeout xml parse", "HTTP 404", ""])
def test_the_message_never_decides(text):
    assert classify_failure(RuntimeError(text)) == FailureClass(X)
    assert classify_failure(ValueError(text)) == FailureClass(X)


def test_a_misleading_message_on_the_right_type_is_ignored():
    assert classify_failure(httpx.ConnectError("XML parse error 404 timeout")) == FailureClass(U)


# ---------------------------------------------------------------------------
# One status-to-message mapping (WFS, ArcGIS and Carto used to carry three)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "needle"),
    [
        (401, "Authentication required for the thing"),
        (403, "Access denied to the thing"),
        (404, "The thing not found (404)"),
    ],
)
def test_one_mapping_for_the_fatal_statuses(status, needle):
    from geoparquet_io.core.exceptions import fatal_status_message

    message = fatal_status_message(status, subject="the thing", credentials="Use a key.")
    assert message.startswith(needle)
    if status != 404:
        assert message.endswith("Use a key.")


@pytest.mark.parametrize("status", [200, 400, 418, 429, 500, 503])
def test_other_statuses_have_no_fatal_message(status):
    from geoparquet_io.core.exceptions import fatal_status_message

    assert fatal_status_message(status, subject="x", credentials="y") is None


# ---------------------------------------------------------------------------
# Sanitizing happens once, at the owner
# ---------------------------------------------------------------------------


def test_a_gpio_error_arrives_sanitized():
    from geoparquet_io.core.exceptions import GeoParquetError
    from geoparquet_io.core.wfs import WFSError

    err = WFSError("failed: https://h/a/b/c/d/e?api_key=SECRET body: token=SECRET2 \x1b[31m")
    assert isinstance(err, GeoParquetError)
    assert "SECRET" not in str(err) and "SECRET2" not in str(err)
    assert "\x1b" not in str(err)
    # the destination a message names on purpose is not elided
    assert "https://h/a/b/c/d/e" in str(err)


@pytest.mark.parametrize(
    ("text", "kept"),
    [
        ("bad request api_key=SECRET", "bad request api_key=***"),
        ("Authorization: Bearer SECRET", "Authorization: ***"),
        ("X-Amz-Signature=SECRET&x=1", "X-Amz-Signature=***&x=1"),
        ("the key is missing", "the key is missing"),
        ("duplicate key: 5", "duplicate key: 5"),
    ],
)
def test_echoed_credential_pairs_are_redacted(text, kept):
    from geoparquet_io.core.exceptions import sanitize_error_message

    assert sanitize_error_message(text) == kept


def test_click_error_for_sanitizes_any_exception():
    from geoparquet_io.cli.exception_handler import click_error_for

    err = click_error_for(OSError("GET https://h/x?X-Amz-Signature=SECRET failed password=hunter2"))
    assert "SECRET" not in err.message and "hunter2" not in err.message


# ---------------------------------------------------------------------------
# DuckDB's text, read in one opt-in place with the user's own text cut out
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("HTTP Error: HTTP GET error on 'https://b/x' (HTTP 403)", FailureClass(H, 403)),
        ("IO Error: Could not read (HTTP 404)", FailureClass(H, 404)),
        ("Access Denied", FailureClass(H, 403)),
        ("Connection timed out", FailureClass(T)),
        ("Unable to connect to the server", FailureClass(U)),
        ("something else", FailureClass(X)),
    ],
)
def test_duckdb_io_text_is_read_in_one_place(message, expected):
    exc = duckdb.IOException(message)
    assert classify_failure(exc) == expected


def test_a_path_or_url_named_forbidden_cannot_decide_the_verdict():
    """#1020: the user's own text inside the message is cut out before it is read."""
    exc = duckdb.IOException(
        "IO Error: Could not open GDAL dataset at: https://h/forbidden/timeout?q=%27404%20not%20found%27"
    )
    assert classify_failure(exc) == FailureClass(X)


@pytest.mark.parametrize(
    "exc",
    [
        duckdb.InvalidInputException(
            "Invalid Input Error: No magic bytes found at end of file 'x'"
        ),
        duckdb.InvalidInputException("File 'x' too small to be a Parquet file"),
        __import__("pyarrow").lib.ArrowInvalid("Parquet magic bytes not found in footer."),
    ],
)
def test_one_not_parquet_predicate(exc):
    assert classify_failure(exc) == FailureClass(FailureKind.NOT_PARQUET)
