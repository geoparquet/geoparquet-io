"""The one HTTP retry policy (#1367, concept 14), pinned as a table.

Retried: transport failures and 408/429/5xx. Backoff: ``retry_delay * 2**attempt``.
``Retry-After`` raises the wait to the server's minimum; nothing waits longer than
``MAX_RETRY_AFTER``. Driven through ``make_request_with_retry`` over the offline
transport, which records the value handed to ``time.sleep``.
"""

from __future__ import annotations

import httpx
import pytest

from geoparquet_io.core.exceptions import RemoteAccessError
from geoparquet_io.core.http_retry import (
    MAX_RETRY_AFTER,
    RetryExhaustedError,
    RetryVerdict,
    is_retryable_status,
    make_request_with_retry,
    retry_wait,
    run_with_retry,
)
from tests.http_transport import (
    FakeTransport,
    connect_error,
    error_reply,
    json_reply,
    timeout_error,
)

URL = "https://host.example.com/svc"


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504, 599])
def test_a_transient_status_is_retried_then_succeeds(monkeypatch, status):
    http = FakeTransport.install(monkeypatch)
    http.respond(URL, error_reply(status), json_reply({"ok": True}))

    assert make_request_with_retry("GET", URL) == {"ok": True}
    assert len(http.requests) == 2
    assert http.sleeps == [1.0]


@pytest.mark.parametrize("status", [400, 401, 403, 404, 410, 422])
def test_any_other_status_fails_without_a_retry(monkeypatch, status):
    http = FakeTransport.install(monkeypatch)
    http.respond(URL, error_reply(status), json_reply({"ok": True}))

    with pytest.raises(RemoteAccessError):
        make_request_with_retry("GET", URL)
    assert len(http.requests) == 1
    assert http.sleeps == []


@pytest.mark.parametrize("failure", [timeout_error(), connect_error()])
def test_a_transport_failure_is_retried(monkeypatch, failure):
    http = FakeTransport.install(monkeypatch)
    http.respond(URL, failure, json_reply({"ok": True}))

    assert make_request_with_retry("GET", URL) == {"ok": True}
    assert http.sleeps == [1.0]


def test_the_backoff_doubles_per_attempt_and_the_last_attempt_does_not_sleep(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(URL, error_reply(503))

    with pytest.raises(RemoteAccessError, match="HTTP error 503"):
        make_request_with_retry("GET", URL, max_retries=5, retry_delay=0.5)
    assert len(http.requests) == 5
    assert http.sleeps == [0.5, 1.0, 2.0, 4.0]


@pytest.mark.parametrize(
    ("retry_after", "expected"),
    [
        ("30", [30.0]),  # beyond the 1s backoff: the server's minimum wins
        ("0", [1.0]),  # below the backoff: the backoff stays
        ("86400", [MAX_RETRY_AFTER]),  # capped
        ("soon", [1.0]),  # unreadable: ignored
    ],
)
def test_retry_after_is_the_servers_minimum_and_is_capped(monkeypatch, retry_after, expected):
    http = FakeTransport.install(monkeypatch)
    http.respond(URL, error_reply(429, retry_after=retry_after), json_reply({}))

    make_request_with_retry("GET", URL)
    assert http.sleeps == expected


def test_retry_after_is_compared_with_the_backoff_of_that_attempt(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(
        URL,
        error_reply(503, retry_after=3),
        error_reply(503, retry_after=1),
        error_reply(503, retry_after=1),
        json_reply({}),
    )

    make_request_with_retry("GET", URL, max_retries=4)
    assert http.sleeps == [3.0, 2.0, 4.0]


def test_a_dropped_connection_resets_the_pool_before_the_retry(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(URL, httpx.RemoteProtocolError("gone"), json_reply({}))

    make_request_with_retry("GET", URL)
    assert http.resets == 1


@pytest.mark.parametrize(
    ("attempt", "delay", "retry_after", "expected"),
    [
        (0, 1.0, None, 1.0),
        (1, 1.0, None, 2.0),
        (2, 1.5, None, 6.0),
        (0, 1.0, 5.0, 5.0),
        (3, 1.0, 5.0, 8.0),
        (10, 1.0, None, MAX_RETRY_AFTER),
    ],
)
def test_retry_wait_table(attempt, delay, retry_after, expected):
    assert retry_wait(attempt, delay, retry_after) == expected


@pytest.mark.parametrize(
    ("status", "expected"),
    [(200, False), (301, False), (404, False), (408, True), (429, True), (500, True), (503, True)],
)
def test_is_retryable_status(status, expected):
    assert is_retryable_status(status) is expected


def test_run_with_retry_reraises_what_classify_calls_fatal(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    calls = []

    def attempt():
        calls.append(1)
        raise ValueError("bad input")

    with pytest.raises(ValueError, match="bad input"):
        run_with_retry(attempt)
    assert calls == [1]
    assert http.sleeps == []


def test_run_with_retry_reports_exhaustion_with_the_last_failure(monkeypatch):
    http = FakeTransport.install(monkeypatch)

    def classify(exc):
        return RetryVerdict(True, description="flaky", detail=str(exc))

    def attempt():
        raise ValueError("again")

    with pytest.raises(RetryExhaustedError) as excinfo:
        run_with_retry(attempt, max_retries=2, classify=classify)
    assert excinfo.value.attempts == 2
    assert str(excinfo.value.last_exception) == "again"
    assert excinfo.value.verdict.detail == "again"
    assert http.sleeps == [1.0]
