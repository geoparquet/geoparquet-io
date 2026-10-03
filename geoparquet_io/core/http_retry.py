"""
Shared HTTP request utilities with retry logic.

This module provides reusable HTTP request functions with:
- Exponential backoff retry on transient errors
- Connection pooling via shared httpx client
- Gzip compression support
- Proper error classification (retryable vs. fatal)

Used by: arcgis.py (wfs.py shares only the pooled client)
"""

from __future__ import annotations

import json
import threading
import time
from typing import TYPE_CHECKING, Any, cast

from geoparquet_io.core.exceptions import BatchTooLargeError, RemoteAccessError
from geoparquet_io.core.logging_config import warn

if TYPE_CHECKING:
    import httpx

# Module-level HTTP client for connection pooling with thread safety
_shared_http_client: httpx.Client | None = None
_http_client_lock = threading.Lock()

# Default timeout and retry settings
DEFAULT_TIMEOUT = 60.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 1.0

# The longest a server's Retry-After is honoured for, so a hostile or confused
# header cannot park the client for a day. Carto's extractor has the same cap
# (carto.MAX_RETRY_AFTER, #1027).
MAX_RETRY_AFTER = 60.0  # seconds

# The HTTP statuses a server uses to refuse a page it could not produce: its
# own 500 (ArcGIS Server's "Error performing query operation" HTML page) and a
# proxy giving up on it (502, 504). Not 503 or 501, which say something specific
# (unavailable, unsupported) and carry Retry-After, nor 429. Only a request that
# asked for a page (``batch_size`` set) is refused in this sense (#1134).
PAGE_REFUSAL_STATUSES = frozenset({500, 502, 504})


def _json_error_envelope(response: httpx.Response) -> dict[str, Any] | None:
    """The ArcGIS ``{"error": {...}}`` body of an error response, if it has one."""
    try:
        body = response.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if isinstance(body, dict) and isinstance(body.get("error"), dict):
        return body
    return None


def get_shared_http_client(
    timeout: float = DEFAULT_TIMEOUT,
    http2: bool = False,
    max_connections: int = 50,
) -> httpx.Client:
    """
    Get or create a shared HTTP client for connection pooling.

    Thread-safe: uses a lock to prevent race conditions when multiple
    threads try to create or access the client simultaneously.

    Reuses TCP connections across requests, saving ~100-200ms per request
    on TLS handshakes.

    Args:
        timeout: Request timeout in seconds
        http2: Enable HTTP/2 (disabled by default for ArcGIS compatibility)
        max_connections: Maximum number of connections in pool

    Returns:
        Shared httpx.Client instance
    """
    global _shared_http_client
    import httpx

    with _http_client_lock:
        if _shared_http_client is None:
            _shared_http_client = httpx.Client(
                timeout=timeout,
                follow_redirects=True,
                http2=http2,
                limits=httpx.Limits(
                    max_connections=max_connections,
                    max_keepalive_connections=max_connections,
                ),
            )
        return _shared_http_client


def reset_http_client() -> None:
    """
    Reset the shared HTTP client (for connection errors or cleanup).

    Thread-safe: acquires lock before closing/clearing the client.
    Closes the existing client and allows a new one to be created.
    """
    global _shared_http_client

    with _http_client_lock:
        if _shared_http_client is not None:
            client_to_close = _shared_http_client
            _shared_http_client = None
            client_to_close.close()


def make_request_with_retry(
    method: str,
    url: str,
    params: dict | None = None,
    data: dict | None = None,
    headers: dict | None = None,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
    timeout: float = DEFAULT_TIMEOUT,
    parse_json: bool = True,
    batch_size: int | None = None,
) -> dict[str, Any] | bytes:
    """
    Make HTTP request with retry logic and proper error handling.

    Args:
        method: HTTP method ("GET" or "POST")
        url: Request URL
        params: Query parameters (for GET)
        data: Form data (for POST)
        headers: Additional headers (Accept-Encoding: gzip added automatically)
        max_retries: Number of retry attempts
        retry_delay: Base delay between retries (exponential backoff)
        timeout: Request timeout in seconds
        parse_json: If True, parse response as JSON and raise BatchTooLargeError
            on parse failure. If False, return raw bytes.
        batch_size: The page size of a paged request, or None for a request
            that asked for no page (layer info, count, token). Decides whether
            a refused response is the caller's batch-size ladder's business.

    Returns:
        Parsed JSON dict if parse_json=True, otherwise raw bytes

    Raises:
        RemoteAccessError: For fatal HTTP errors (401, 403, 404), or exhausted
            retries on a request that asked for no page
        BatchTooLargeError: On a paged request, when a 200 body is not JSON
            (an HTML error or block page) or a 500/502/504 without a JSON error
            envelope survived every retry. An HTTP 500 *with* an envelope is
            returned as-is for the caller to classify.
    """
    import httpx

    last_exception: Exception | None = None

    # Build headers with compression support
    request_headers = {"Accept-Encoding": "gzip, deflate"}
    if headers:
        request_headers.update(headers)

    for attempt in range(max_retries):
        try:
            client = get_shared_http_client(timeout=timeout)

            # Pass timeout per-request as well: the shared client caches the
            # timeout it was first created with, so a later call asking for a
            # longer timeout would otherwise be silently capped at that value.
            if method == "GET":
                response = client.get(url, params=params, headers=request_headers, timeout=timeout)
            else:
                response = client.post(url, data=data, headers=request_headers, timeout=timeout)

            response.raise_for_status()

            if parse_json:
                try:
                    return cast(dict[str, Any], response.json())
                except json.JSONDecodeError as e:
                    # Server returned non-JSON (likely HTML error page).
                    # Don't include response content in error - may contain
                    # sensitive tokens; the status code and Content-Type are
                    # safe and make a WAF/proxy block page distinguishable
                    # from a genuine payload-size truncation.
                    content_type = response.headers.get("content-type", "unknown")
                    detail = (
                        f"Server returned non-JSON response "
                        f"(Content-Type: {content_type}, HTTP {response.status_code}), "
                        f"likely an HTML error or block page"
                    )
                    if batch_size is None:
                        # No batch was requested (e.g. a feature-count or
                        # layer-info query), so reducing the batch size cannot
                        # help and BatchTooLargeError would be misleading.
                        raise RemoteAccessError(url, detail) from e
                    # This is NOT retryable with same params - batch is too large
                    raise BatchTooLargeError(
                        url=url,
                        batch_size=batch_size,
                        reason=detail,
                    ) from e
            else:
                return bytes(response.content)

        except httpx.RemoteProtocolError as e:
            # Server disconnected - reset connection pool and retry
            last_exception = e
            warn(f"HTTP protocol error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                reset_http_client()
                time.sleep(retry_delay * (attempt + 1))

        except httpx.TimeoutException as e:
            last_exception = e
            warn(f"HTTP timeout after {timeout}s (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))

        except httpx.NetworkError as e:
            last_exception = e
            warn(f"HTTP network error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))

        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            page_refused = batch_size is not None and status in PAGE_REFUSAL_STATUSES

            if page_refused and parse_json and status == 500:
                envelope = _json_error_envelope(e.response)
                if envelope is not None:
                    # Newer ArcGIS servers mirror their JSON error code into the
                    # HTTP status - always their own 500, never a proxy's 502/504,
                    # whose JSON bodies (if any) are the proxy's and keep the
                    # retries below. The envelope is the server's considered
                    # answer, and the caller's classifier (not this loop) decides
                    # whether it is a page-size problem or a specific, fatal error.
                    return envelope

            # Retry on rate limit or server errors
            if status == 429 or (500 <= status < 600):
                last_exception = e
                warn(f"HTTP {status} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    retry_after = e.response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        # Only the delay-seconds form is honoured; an
                        # HTTP-date (RFC 9110) falls through to the linear
                        # backoff below (#1052).
                        delay = min(float(retry_after), MAX_RETRY_AFTER)
                        if delay < float(retry_after):
                            warn(
                                f"Retry-After {retry_after}s exceeds "
                                f"{MAX_RETRY_AFTER}s; sleeping {delay}s instead"
                            )
                    else:
                        delay = retry_delay * (attempt + 1)
                    time.sleep(delay)
                    continue
                if page_refused and batch_size is not None:  # the latter narrows the type
                    # A 500/502/504 without a JSON body that survived every
                    # same-size retry on a *paged* request: ArcGIS Server answers
                    # a page it cannot serialize with HTTP 500 + an HTML error
                    # page on the GeoJSON path, and a proxy in front of it gives
                    # up with 502/504 (#1134). The page size is the one lever
                    # left, and it belongs to the caller's batch-size ladder.
                    raise BatchTooLargeError(
                        url=url,
                        batch_size=batch_size,
                        reason=f"HTTP {status} persisted after {max_retries} attempts",
                    ) from e

            # Fatal errors - don't retry
            if status == 401:
                raise RemoteAccessError(
                    url, "Authentication required. Use --token or --username/--password."
                ) from None
            if status == 403:
                raise RemoteAccessError(
                    url, "Access denied. Check your credentials and service permissions."
                ) from None
            if status == 404:
                raise RemoteAccessError(url, "Service not found (404). Check the URL.") from None

            # Not str(e): httpx spells out the full request URL there, and on
            # ArcGIS the token rides in the query string.
            reason_phrase = e.response.reason_phrase
            raise RemoteAccessError(url, f"HTTP error {status} {reason_phrase}".rstrip()) from e

        except BatchTooLargeError:
            # Don't retry BatchTooLargeError - caller needs to reduce batch size
            raise

    raise RemoteAccessError(url, f"Request failed after {max_retries} attempts: {last_exception}")
