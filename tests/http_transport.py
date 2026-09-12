"""An offline HTTP transport for the remote extractors.

`gpio extract wfs` and `gpio extract arcgis` reach their servers through one
seam: ``geoparquet_io.core.http_retry.get_shared_http_client``. ArcGIS calls it
via ``make_request_with_retry``; WFS re-exports it as
``geoparquet_io.core.wfs._get_shared_http_client_base``. Swap what that seam
returns for an ``httpx.Client`` built on ``httpx.MockTransport`` and the whole
of both modules — paging, retries, content-type dispatch, the file that gets
written — runs with no network and no new dependency.

This module is the swap, plus the recording that makes a *request* assertable:

* every request issued is recorded (method, URL, merged query, headers, body),
  so a test asserts on **what was sent**, not on "the mock was called";
* responses are served by route, in sequence, so 429-then-200 is one line;
* failures are first-class — a status code, a ``Retry-After`` header, an
  ``httpx.TimeoutException``, a connection error;
* ``time.sleep`` is captured per-module, so backoff is asserted **by value**
  rather than inferred from a call count (and the suite stays fast).

Typical use::

    def test_count_probe_keeps_the_url_api_key(monkeypatch):
        http = FakeTransport.install(monkeypatch)
        http.respond("/wfs", xml_reply(b'<hits numberOfFeatures="7"/>'))

        assert _get_feature_count("https://host/wfs?apikey=k", "ns:layer") == 7
        assert http.last.params["apikey"] == "k"

Unrouted requests raise ``AssertionError`` naming the URL, so a test that
forgets a route fails loudly instead of reaching the internet.
"""

from __future__ import annotations

import re
import time as _real_time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx

__all__ = [
    "FakeTransport",
    "RecordedRequest",
    "bytes_reply",
    "connect_error",
    "error_reply",
    "geojson_reply",
    "html_reply",
    "json_reply",
    "protocol_error",
    "reply",
    "timeout_error",
    "wfs_capabilities",
    "xml_reply",
]

# Modules whose ``time`` name is replaced so their sleeps are recorded and
# instant. Both call ``time.sleep`` for backoff and ``time.time`` for timing.
_TIME_PATCHED_MODULES = (
    "geoparquet_io.core.http_retry",
    "geoparquet_io.core.wfs",
)

# The seams that hand out the shared client. Each entry is
# (module, getter attribute, reset attribute).
_CLIENT_SEAMS = (
    ("geoparquet_io.core.http_retry", "get_shared_http_client", "reset_http_client"),
    ("geoparquet_io.core.wfs", "_get_shared_http_client_base", "_reset_http_client_base"),
)


@dataclass(frozen=True)
class RecordedRequest:
    """One request as the transport actually saw it."""

    method: str
    url: str
    path: str
    multi_params: dict[str, list[str]]
    headers: dict[str, str]
    content: bytes
    timeout: float | None

    @property
    def params(self) -> dict[str, str]:
        """The query string as a single-valued dict (last value wins)."""
        return {key: values[-1] for key, values in self.multi_params.items()}

    @property
    def form(self) -> dict[str, str]:
        """A ``application/x-www-form-urlencoded`` body as a dict."""
        parsed = parse_qs(self.content.decode("utf-8"), keep_blank_values=True)
        return {key: values[-1] for key, values in parsed.items()}

    def __str__(self) -> str:  # pragma: no cover - debugging aid
        return f"{self.method} {self.url}"


# --------------------------------------------------------------------------
# Replies
# --------------------------------------------------------------------------

#: A reply is a callable ``(httpx.Request) -> httpx.Response``, or an exception
#: to raise, or a bare ``httpx.Response`` that is cloned for every call.
Reply = Callable[[httpx.Request], httpx.Response] | BaseException | httpx.Response


def reply(
    status: int = 200,
    *,
    content: bytes = b"",
    content_type: str | None = None,
    headers: dict[str, str] | None = None,
) -> Callable[[httpx.Request], httpx.Response]:
    """A reply factory. Every helper below is a thin wrapper over this one."""
    merged = dict(headers or {})
    if content_type is not None:
        merged.setdefault("content-type", content_type)

    def _build(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=content, headers=merged)

    return _build


def json_reply(payload: Any, status: int = 200, **kwargs: Any):
    """An ``application/json`` reply carrying ``payload``."""
    import json as _json

    return reply(
        status,
        content=_json.dumps(payload).encode("utf-8"),
        content_type="application/json",
        **kwargs,
    )


def geojson_reply(payload: Any, status: int = 200, **kwargs: Any):
    """A reply with the ``application/geo+json`` content type WFS dispatches on."""
    import json as _json

    return reply(
        status,
        content=_json.dumps(payload).encode("utf-8"),
        content_type="application/geo+json",
        **kwargs,
    )


def xml_reply(body: bytes, status: int = 200, **kwargs: Any):
    """A ``text/xml`` reply — WFS ``resultType=hits`` and GML both arrive this way."""
    return reply(status, content=body, content_type="text/xml", **kwargs)


def html_reply(body: bytes = b"<html>blocked</html>", status: int = 200, **kwargs: Any):
    """An HTML error/block page returned with a 200, as WAFs and proxies do."""
    return reply(status, content=body, content_type="text/html", **kwargs)


def bytes_reply(body: bytes, status: int = 200, content_type: str = "application/octet-stream"):
    """An opaque body with an explicit content type."""
    return reply(status, content=body, content_type=content_type)


def error_reply(
    status: int,
    *,
    retry_after: str | int | None = None,
    body: bytes = b"upstream error",
    content_type: str = "text/plain",
):
    """An error status, optionally carrying ``Retry-After``.

    ``retry_after`` is passed through verbatim, so a test can send the integer
    form (``"120"``) or the HTTP-date form
    (``"Wed, 21 Oct 2026 07:28:00 GMT"``) and assert what each one does.
    """
    headers = {} if retry_after is None else {"Retry-After": str(retry_after)}
    return reply(status, content=body, content_type=content_type, headers=headers)


def timeout_error(message: str = "read timed out") -> httpx.TimeoutException:
    """A transport timeout, as httpx raises it."""
    return httpx.ReadTimeout(message)


def connect_error(message: str = "connection refused") -> httpx.ConnectError:
    """A connection failure (an ``httpx.NetworkError`` subclass)."""
    return httpx.ConnectError(message)


def protocol_error(message: str = "server disconnected") -> httpx.RemoteProtocolError:
    """A mid-response disconnect — the error that resets the connection pool."""
    return httpx.RemoteProtocolError(message)


# --------------------------------------------------------------------------
# The transport
# --------------------------------------------------------------------------

Matcher = str | re.Pattern[str] | Callable[[RecordedRequest], bool]


@dataclass
class _Route:
    matcher: Matcher
    replies: list[Reply]
    calls: int = 0

    def matches(self, recorded: RecordedRequest) -> bool:
        if callable(self.matcher) and not isinstance(self.matcher, re.Pattern):
            return bool(self.matcher(recorded))
        if isinstance(self.matcher, re.Pattern):
            return bool(self.matcher.search(recorded.url))
        return self.matcher in recorded.url

    def next_reply(self) -> Reply:
        chosen = self.replies[min(self.calls, len(self.replies) - 1)]
        self.calls += 1
        return chosen


class _TimeShim:
    """``time`` with a recording, instant ``sleep``. Everything else is real."""

    def __init__(self, sink: list[float]) -> None:
        self._sink = sink

    def sleep(self, seconds: float) -> None:
        self._sink.append(seconds)

    def __getattr__(self, name: str) -> Any:
        return getattr(_real_time, name)


@dataclass
class FakeTransport:
    """Records every request the extractors issue and answers them by route."""

    routes: list[_Route] = field(default_factory=list)
    requests: list[RecordedRequest] = field(default_factory=list)
    sleeps: list[float] = field(default_factory=list)
    client_timeouts: list[float] = field(default_factory=list)
    resets: int = 0
    _client: httpx.Client | None = None

    # -- setup ------------------------------------------------------------
    @classmethod
    def install(cls, monkeypatch) -> FakeTransport:
        """Redirect the shared HTTP client and the retry sleeps at this test."""
        fake = cls()
        client = httpx.Client(transport=httpx.MockTransport(fake._handle), follow_redirects=True)

        def _get_client(timeout: float = 60.0, **_kwargs: Any) -> httpx.Client:
            fake.client_timeouts.append(timeout)
            return client

        def _reset() -> None:
            fake.resets += 1

        for module_name, getter, resetter in _CLIENT_SEAMS:
            monkeypatch.setattr(f"{module_name}.{getter}", _get_client)
            monkeypatch.setattr(f"{module_name}.{resetter}", _reset)

        shim = _TimeShim(fake.sleeps)
        for module_name in _TIME_PATCHED_MODULES:
            monkeypatch.setattr(f"{module_name}.time", shim)

        # Keep the client alive for the length of the test: only the closures
        # above hold it, and a closed client would refuse the next request.
        fake._client = client
        return fake

    def respond(self, matcher: Matcher, *replies: Reply) -> FakeTransport:
        """Answer requests matching ``matcher`` with ``replies``, in order.

        The last reply repeats once the sequence is exhausted, so a single
        reply serves every request and ``error_reply(503), json_reply(...)``
        expresses "fail once, then succeed". Routes are tried in the order
        they were registered.
        """
        if not replies:
            raise ValueError("respond() needs at least one reply")
        self.routes.append(_Route(matcher, list(replies)))
        return self

    # -- inspection -------------------------------------------------------
    @property
    def last(self) -> RecordedRequest:
        """The most recent request. Fails the test if nothing was issued."""
        assert self.requests, "no HTTP request was issued"
        return self.requests[-1]

    def matching(self, matcher: Matcher) -> list[RecordedRequest]:
        """Every recorded request matching ``matcher``."""
        probe = _Route(matcher, [reply()])
        return [recorded for recorded in self.requests if probe.matches(recorded)]

    def param_series(self, name: str) -> list[str | None]:
        """One query parameter's value across every request, in order.

        The oracle for paging: ``http.param_series("resultOffset")`` is the
        offsets the extractor actually walked.
        """
        return [recorded.params.get(name) for recorded in self.requests]

    # -- the transport itself ---------------------------------------------
    def _handle(self, request: httpx.Request) -> httpx.Response:
        recorded = _record(request)
        self.requests.append(recorded)

        for route in self.routes:
            if route.matches(recorded):
                return _resolve(route.next_reply(), request)

        raise AssertionError(
            f"unrouted request: {recorded}\n"
            f"registered routes: {[route.matcher for route in self.routes]}"
        )


def _record(request: httpx.Request) -> RecordedRequest:
    parts = urlsplit(str(request.url))
    timeouts = request.extensions.get("timeout") or {}
    return RecordedRequest(
        method=request.method,
        url=str(request.url),
        path=parts.path,
        multi_params=parse_qs(parts.query, keep_blank_values=True),
        headers={key.lower(): value for key, value in request.headers.items()},
        content=request.content,
        timeout=timeouts.get("read"),
    )


def _resolve(chosen: Reply, request: httpx.Request) -> httpx.Response:
    if isinstance(chosen, BaseException):
        raise chosen
    if isinstance(chosen, type) and issubclass(chosen, BaseException):
        raise chosen("injected failure")
    if isinstance(chosen, httpx.Response):
        return httpx.Response(chosen.status_code, content=chosen.content, headers=chosen.headers)
    return chosen(request)


# --------------------------------------------------------------------------
# WFS capabilities
# --------------------------------------------------------------------------


def wfs_capabilities(
    monkeypatch,
    xml: bytes | dict[str, bytes],
    schema: dict[str, Any] | Iterable[tuple[str, dict[str, Any]]] | None = None,
) -> list[str]:
    """Serve OWSLib a canned GetCapabilities document instead of a live one.

    OWSLib does its own HTTP (through ``requests``), so ``MockTransport`` never
    sees it — but ``WebFeatureService`` accepts ``xml=`` and then parses
    offline. This keeps the real OWSLib parsing that ``get_layer_info`` depends
    on (contents, ``crsOptions``, operation parameters) while removing the
    network.

    ``xml`` is one document, or a ``{version: document}`` mapping so a test can
    make version negotiation fail on 2.0.0 and succeed on 1.1.0 — a version
    with no entry raises, exactly as an unsupported server does.

    ``schema`` is the DescribeFeatureType answer ``wfs.get_schema()`` returns;
    ``None`` makes it raise, which is the degraded path both detectors already
    swallow.

    Returns the list of versions OWSLib was asked for, in order.
    """
    from owslib.wfs import WebFeatureService as _real

    documents = {"*": xml} if isinstance(xml, bytes) else dict(xml)
    asked: list[str] = []

    def _factory(url: str, version: str = "1.0.0", **kwargs: Any):
        asked.append(version)
        document = documents.get(version, documents.get("*"))
        if document is None:
            raise ConnectionError(f"no capabilities canned for WFS {version}")
        service = _real(url=url, version=version, xml=document, **kwargs)
        service.get_schema = _schema_reader(schema)  # type: ignore[method-assign]
        return service

    monkeypatch.setattr("owslib.wfs.WebFeatureService", _factory)
    return asked


def _schema_reader(schema):
    if schema is None:

        def _raise(_typename):
            raise ConnectionError("DescribeFeatureType unavailable")

        return _raise

    table = dict(schema) if not isinstance(schema, dict) else schema
    if "properties" in table or "geometry" in table:
        return lambda _typename: table
    return lambda typename: table.get(typename)
