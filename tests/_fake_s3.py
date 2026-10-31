"""A loopback S3 endpoint for tests that must read through DuckDB's httpfs.

Serves ``ListObjectsV2`` (what DuckDB's ``glob()`` asks), ``HEAD`` and ranged
``GET`` for a dict of ``{"bucket/key": bytes}``, path-style, over plain HTTP,
and stores a single-request ``PUT`` (what obstore sends for a small upload).
It stands in for the network and nothing else: gpio talks to it through its
real ``--s3-endpoint``/``--s3-no-ssl`` configuration.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from xml.sax.saxutils import escape

_LAST_MODIFIED = "Mon, 01 Jan 2024 00:00:00 GMT"


def _listing(bucket: str, prefix: str, keys: list[str], sizes: dict[str, int]) -> bytes:
    rows = "".join(
        f"<Contents><Key>{escape(k)}</Key><LastModified>2024-01-01T00:00:00.000Z"
        f'</LastModified><ETag>"x"</ETag><Size>{sizes[k]}</Size>'
        "<StorageClass>STANDARD</StorageClass></Contents>"
        for k in keys
    )
    return (
        "<?xml version='1.0' encoding='UTF-8'?>"
        "<ListBucketResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>"
        f"<Name>{bucket}</Name><Prefix>{escape(prefix)}</Prefix><KeyCount>{len(keys)}"
        "</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>"
        f"{rows}</ListBucketResult>"
    ).encode()


class _Handler(BaseHTTPRequestHandler):
    objects: dict[str, bytes]
    requests: list[tuple[str, str]]

    def log_message(self, *args) -> None:  # keep test output clean
        return

    def _send(self, status: int, body: bytes = b"", headers: dict | None = None) -> None:
        self.send_response(status)
        for name, value in {"Content-Length": str(len(body)), **(headers or {})}.items():
            self.send_header(name, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_HEAD(self) -> None:
        self.requests.append(("HEAD", self.path))
        data = self.objects.get(urlparse(self.path).path.lstrip("/"))
        if data is None:
            self._send(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Last-Modified", _LAST_MODIFIED)
        self.send_header("ETag", '"x"')
        self.end_headers()

    def do_PUT(self) -> None:
        self.requests.append(("PUT", self.path))
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        self.objects[urlparse(self.path).path.lstrip("/")] = body
        self._send(200, headers={"ETag": '"x"'})

    def do_GET(self) -> None:
        self.requests.append(("GET", self.path))
        url = urlparse(self.path)
        target = url.path.lstrip("/")
        query = parse_qs(url.query)
        if "list-type" in query or "/" not in target.rstrip("/"):
            bucket = target.rstrip("/")
            prefix = query.get("prefix", [""])[0]
            sizes = {
                key.split("/", 1)[1]: len(data)
                for key, data in self.objects.items()
                if key.startswith(bucket + "/")
            }
            keys = sorted(k for k in sizes if k.startswith(prefix))
            self._send(
                200, _listing(bucket, prefix, keys, sizes), {"Content-Type": "application/xml"}
            )
            return
        data = self.objects.get(target)
        if data is None:
            self._send(404)
            return
        span = self.headers.get("Range")
        if not span:
            self._send(200, data, {"Last-Modified": _LAST_MODIFIED})
            return
        start, _, end = span.split("=", 1)[1].partition("-")
        first, last = int(start), int(end) if end else len(data) - 1
        self._send(
            206,
            data[first : last + 1],
            {"Content-Range": f"bytes {first}-{last}/{len(data)}", "Last-Modified": _LAST_MODIFIED},
        )


@contextmanager
def fake_s3(objects: dict[str, bytes]) -> Iterator[tuple[str, list[tuple[str, str]]]]:
    """Serve *objects*; yield ``(endpoint, requests)`` with ``host:port`` and a request log.

    *objects* is served as given, so a ``PUT`` lands in the caller's dict.
    """
    requests: list[tuple[str, str]] = []
    handler = type("Handler", (_Handler,), {"objects": objects, "requests": requests})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"127.0.0.1:{server.server_address[1]}", requests
    finally:
        server.shutdown()
        server.server_close()
