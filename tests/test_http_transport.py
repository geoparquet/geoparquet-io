"""The offline transport itself: what ``tests/http_transport.py`` promises.

The extractor tests in ``test_arcgis_transport.py`` and ``test_wfs_transport.py``
lean on a handful of harness behaviours -- route order, the sticky last reply,
a loud failure for an unrouted request, the recorded query/form/timeout, and
instant recorded sleeps. Each is pinned here so a change to the harness cannot
quietly turn an extractor assertion vacuous.
"""

from __future__ import annotations

import threading
import time

import httpx
import pytest

from geoparquet_io.core import http_retry, wfs
from tests.http_transport import FakeTransport, error_reply, json_reply, reply, timeout_error

HOST = "https://fake.example.org"


def _client() -> httpx.Client:
    """The client the extractors get once the harness is installed."""
    return http_retry.get_shared_http_client()


def test_the_seam_hands_the_extractors_the_fake_client(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond("/ping", json_reply({"ok": True}))

    assert http_retry.get_shared_http_client(timeout=7.0).get(f"{HOST}/ping").json() == {"ok": True}
    assert wfs._get_shared_http_client_base(timeout=9.0).get(f"{HOST}/ping").json() == {"ok": True}
    assert http.client_timeouts == [7.0, 9.0]

    http_retry.reset_http_client()
    wfs._reset_http_client_base()
    assert http.resets == 2


def test_routes_are_tried_in_registration_order(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond("/specific", json_reply("specific"))
    http.respond(lambda request: True, json_reply("catch-all"))

    assert _client().get(f"{HOST}/specific").json() == "specific"
    assert _client().get(f"{HOST}/other").json() == "catch-all"


def test_replies_are_served_in_sequence_and_the_last_one_sticks(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond("/flaky", error_reply(503), json_reply({"n": 1}))

    statuses = [_client().get(f"{HOST}/flaky").status_code for _ in range(4)]

    assert statuses == [503, 200, 200, 200]
    assert len(http.requests) == 4


def test_an_unrouted_request_fails_loudly_naming_the_url(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond("/known", json_reply({}))

    with pytest.raises(AssertionError, match=r"unrouted request: GET .*/unknown"):
        _client().get(f"{HOST}/unknown")


def test_a_recorded_request_carries_the_merged_query_form_and_timeout(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond("/q", json_reply({}))

    _client().get(f"{HOST}/q?a=1&b=2", params={"a": "3"}, timeout=12.5)
    _client().post(f"{HOST}/q", data={"user": "u", "pass": "p w"})

    first, second = http.requests
    assert (first.method, first.path) == ("GET", "/q")
    assert first.params == {"a": "3"}  # httpx replaces the URL's query when params= is given
    assert first.timeout == 12.5
    assert second.method == "POST"
    assert second.form == {"user": "u", "pass": "p w"}
    assert http.last is second
    assert http.matching("/q") == [first, second]
    assert http.param_series("a") == ["3", None]


def test_a_reply_callable_sees_the_recorded_request(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond("/echo", lambda request: json_reply(request.params)(request))

    assert _client().get(f"{HOST}/echo?page=4").json() == {"page": "4"}


def test_an_exception_reply_is_raised_and_headers_pass_through(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond("/slow", timeout_error(), error_reply(429, retry_after=120))

    with pytest.raises(httpx.ReadTimeout):
        _client().get(f"{HOST}/slow")
    response = _client().get(f"{HOST}/slow")
    assert (response.status_code, response.headers["retry-after"]) == (429, "120")
    assert reply(204)(http.last).status_code == 204


def test_sleeps_are_recorded_by_value_and_take_no_time(monkeypatch):
    http = FakeTransport.install(monkeypatch)

    started = time.perf_counter()
    http_retry.time.sleep(30.0)
    wfs.time.sleep(2.5)

    assert http.sleeps == [30.0, 2.5]
    assert time.perf_counter() - started < 1.0
    assert isinstance(http_retry.time.time(), float)  # everything but sleep is real


def test_install_is_undone_with_the_monkeypatch():
    original_getter, original_time = http_retry.get_shared_http_client, http_retry.time
    with pytest.MonkeyPatch.context() as patch:
        FakeTransport.install(patch)
        assert http_retry.get_shared_http_client is not original_getter
    assert http_retry.get_shared_http_client is original_getter
    assert http_retry.time is original_time


def test_concurrent_requests_are_recorded_and_answered_in_one_order(monkeypatch):
    """The parallel ArcGIS path fetches from a thread pool.

    Recording a request and choosing its reply happen under one lock, so the
    n-th recorded request is the one that received the n-th reply -- which is
    what lets a test say "the first two requests get the failure" about a
    window fetched by several threads. The matcher pauses to widen the gap
    between the two steps; without the lock the pairing scrambles.
    """
    http = FakeTransport.install(monkeypatch)

    def slow_match(_request) -> bool:
        time.sleep(0.002)
        return True

    http.respond(slow_match, *(json_reply({"n": index}) for index in range(16)))
    gate = threading.Barrier(16)
    reply_for: dict[int, int] = {}

    def fetch(k: int) -> None:
        gate.wait()
        reply_for[k] = _client().get(f"{HOST}/page?i={k}").json()["n"]

    threads = [threading.Thread(target=fetch, args=(k,)) for k in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(reply_for.values()) == list(range(16))  # each reply served once
    assert all(http.requests[n].params["i"] == str(k) for k, n in reply_for.items())
