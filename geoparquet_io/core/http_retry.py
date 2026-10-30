"""
Shared HTTP request utilities with retry logic.

This module is the one owner of gpio's remote-request retry policy (#1367).
Every extractor (ArcGIS, WFS, Carto) retries through it and keeps no loop,
status set or backoff of its own.

The policy (deterministic, no jitter, so tests can assert the exact sleeps):

* Retried: a transport failure (timeout, dropped connection, network error)
  and an HTTP 408, 429 or any 5xx. Not retried: every other status (401, 403,
  404 and the rest of the 4xx), which are the server's considered answer.
* Backoff: ``retry_delay * 2 ** attempt`` (1s, 2s, 4s with the default delay)
  between attempts; there is no sleep after the last attempt.
* ``Retry-After`` (seconds or HTTP-date): the server's minimum. The wait is
  ``max(backoff, Retry-After)``, because answering a 429 with more requests on
  our own schedule is the wrong direction.
* Maximum: no wait is longer than ``MAX_RETRY_AFTER`` (60s), whether it comes
  from the server's header or from the backoff.
* A dropped connection or network error resets the pooled client first.

Also provides connection pooling via a shared httpx client, gzip support and
error classification for ``make_request_with_retry``.

Callers whose attempt is not a plain request (a streamed WFS page, a DuckDB
read of a Carto URL) call ``run_with_retry`` with their own attempt function and,
where the status is not on an httpx exception, their own ``classify``.
"""

from __future__ import annotations

import json
import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Any, cast

from geoparquet_io.core.exceptions import (
    FATAL_STATUS_MESSAGES,
    BatchTooLargeError,
    FailureKind,
    RemoteAccessError,
    classify_failure,
    status_message,
)
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
#: The remedy a 401/403 names when the caller does not say which options carry a credential.
DEFAULT_CREDENTIALS_HINT = "Check your credentials."

# Statuses worth another attempt: request timeout, rate limit, and every 5xx.
RETRYABLE_STATUSES = frozenset({408, 429})


def is_retryable_status(status: int) -> bool:
    """Whether an HTTP status is transient: 408, 429 or any 5xx."""
    return status in RETRYABLE_STATUSES or 500 <= status < 600


# The HTTP statuses a server uses to refuse a page it could not produce: its
# own 500 (ArcGIS Server's "Error performing query operation" HTML page) and a
# proxy giving up on it (502, 504). Not 503 or 501, which say something specific
# (unavailable, unsupported) and carry Retry-After, nor 429. Only a request that
# asked for a page (``batch_size`` set) is refused in this sense (#1134).
PAGE_REFUSAL_STATUSES = frozenset({500, 502, 504})

# The longest a server's Retry-After is honoured for, so a hostile or confused
# header (or a WAF in front of the server) cannot park the client for a day.
MAX_RETRY_AFTER = 60.0  # seconds


def _retry_after_seconds(value: str, now: datetime | None) -> float | None:
    """Either Retry-After form (RFC 9110) as seconds from now, uncapped."""
    try:
        seconds = float(value)
    except ValueError:
        return _retry_after_date(value, now)
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return seconds


def _retry_after_date(value: str, now: datetime | None) -> float | None:
    """The HTTP-date form as seconds from ``now``, 0 for a date already past."""
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:  # the "-0000" zone; HTTP dates are always UTC
        when = when.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    return max(0.0, (when - current).total_seconds())


def parse_retry_after(value: str | None, *, now: datetime | None = None) -> float | None:
    """A server's ``Retry-After`` in seconds, capped at ``MAX_RETRY_AFTER``.

    Accepts delay-seconds (integer or decimal) and the HTTP-date form. Returns
    None when the header is absent or unreadable, so the caller keeps its own
    backoff. A value over the cap is clamped, with a warning.
    """
    text = (value or "").strip()
    if not text:
        return None
    seconds = _retry_after_seconds(text, now)
    if seconds is None:
        return None
    if seconds > MAX_RETRY_AFTER:
        warn(
            f"Server asked to wait {seconds:g}s (Retry-After: {text}), "
            f"capping the wait at {MAX_RETRY_AFTER:g}s"
        )
        return MAX_RETRY_AFTER
    return seconds


def response_retry_after(response: Any) -> float | None:
    """The capped ``Retry-After`` of an httpx response; the one place the header is read."""
    return parse_retry_after(response.headers.get("Retry-After"))


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


def retry_wait(attempt: int, retry_delay: float, retry_after: float | None = None) -> float:
    """Seconds to wait after failed attempt number ``attempt`` (0-based).

    ``retry_delay * 2 ** attempt``, raised to the server's ``Retry-After`` when it
    sent one, and never more than ``MAX_RETRY_AFTER``.
    """
    delay = float(retry_delay * (2**attempt))
    if retry_after is not None:
        delay = max(delay, retry_after)
    return min(delay, MAX_RETRY_AFTER)


@dataclass(frozen=True)
class RetryVerdict:
    """What a caller's ``classify`` decided about one failed attempt.

    Attributes:
        retryable: Whether another attempt is worth making.
        retry_after: The server's capped ``Retry-After`` in seconds, if it sent one.
        reset_client: Drop the pooled httpx client before the next attempt.
        description: Words for the retry warning, e.g. ``"HTTP 503"``.
        detail: Anything the caller wants back on exhaustion (its own status object).
    """

    retryable: bool
    retry_after: float | None = None
    reset_client: bool = False
    description: str = ""
    detail: Any = None


class RetryExhaustedError(Exception):
    """Every attempt failed with a retryable error.

    Attributes:
        attempts: How many attempts were made.
        last_exception: The failure of the last one (None when no attempt ran).
        verdict: The classification of that failure.
    """

    def __init__(
        self,
        attempts: int,
        last_exception: BaseException | None,
        verdict: RetryVerdict | None = None,
    ) -> None:
        super().__init__(f"Request failed after {attempts} attempts: {last_exception}")
        self.attempts = attempts
        self.last_exception = last_exception
        self.verdict = verdict


def classify_httpx_error(exc: BaseException) -> RetryVerdict:
    """The default ``classify``: transport failures and 408/429/5xx are retryable.

    The failure kind is ``exceptions.classify_failure``'s; this only turns it into
    a retry decision (and reads ``Retry-After`` off the response it came with).
    """
    verdict = classify_failure(exc)
    if verdict.kind is FailureKind.HTTP_STATUS and verdict.status is not None:
        response = getattr(exc, "response", None)
        return RetryVerdict(
            is_retryable_status(verdict.status),
            retry_after=response_retry_after(response) if response is not None else None,
            description=f"HTTP {verdict.status}",
        )
    if verdict.kind is FailureKind.TIMEOUT:
        return RetryVerdict(True, description="HTTP timeout")
    if verdict.kind is FailureKind.UNREACHABLE:
        return RetryVerdict(
            True, reset_client=True, description=f"HTTP network error: {type(exc).__name__}"
        )
    return RetryVerdict(False)


def run_with_retry(
    attempt_fn: Callable[[], Any],
    *,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
    classify: Callable[[BaseException], RetryVerdict] = classify_httpx_error,
) -> Any:
    """Call ``attempt_fn`` until it returns, under the module's retry policy.

    An exception from ``attempt_fn`` goes to ``classify``. A non-retryable
    verdict re-raises the original exception untouched (``classify`` may itself
    raise to translate it). A retryable one sleeps ``retry_wait(...)`` and tries
    again; when the last attempt fails too, ``RetryExhaustedError`` is raised
    with the last exception attached. Exceptions are the only signal, so an
    attempt that wants no retry returns or raises something ``classify`` rejects.
    """
    last_exception: Exception | None = None
    last_verdict: RetryVerdict | None = None

    for attempt in range(max_retries):
        try:
            return attempt_fn()
        except Exception as exc:  # noqa: BLE001 - classify decides what is transient
            verdict = classify(exc)
            if not verdict.retryable:
                raise
            last_exception, last_verdict = exc, verdict
            if verdict.reset_client:
                reset_http_client()
            if attempt >= max_retries - 1:
                warn(f"{verdict.description} (attempt {attempt + 1}/{max_retries})")
                break
            delay = retry_wait(attempt, retry_delay, verdict.retry_after)
            warn(
                f"{verdict.description} (attempt {attempt + 1}/{max_retries}), "
                f"retrying in {delay:.1f}s..."
            )
            time.sleep(delay)

    raise RetryExhaustedError(max_retries, last_exception, last_verdict) from last_exception


def status_error(
    url: str, response: Any, credentials: str, detail: str | None = None
) -> RemoteAccessError:
    """The user-facing error for an HTTP status that will not be retried further.

    The one place a response's status becomes gpio's sentence: 401/403/404 through
    ``exceptions.fatal_status_message``, anything else as ``HTTP error <n> <reason>``.
    ``detail`` is the server's own explanation (an error body), appended when given.
    """
    message = status_message(
        response.status_code,
        subject="the service",
        credentials=credentials,
        reason=getattr(response, "reason_phrase", ""),
    )
    if detail:
        message = f"{message}: {detail}"
    return RemoteAccessError(url, message)


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
    credentials: str = DEFAULT_CREDENTIALS_HINT,
) -> dict[str, Any] | bytes:
    """
    Make HTTP request with retry logic and proper error handling.

    Args:
        method: HTTP method ("GET" or "POST")
        url: Request URL
        params: Query parameters (for GET)
        data: Form data (for POST)
        headers: Additional headers (Accept-Encoding: gzip added automatically)
        max_retries: Number of attempts
        retry_delay: Base delay between retries (doubles per attempt)
        timeout: Request timeout in seconds
        parse_json: If True, parse response as JSON and raise BatchTooLargeError
            on parse failure. If False, return raw bytes.
        credentials: The remedy a 401/403 names; the caller knows which of its own
            options (a flag, an environment variable) carry a credential.
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

    # Build headers with compression support
    request_headers = {"Accept-Encoding": "gzip, deflate"}
    if headers:
        request_headers.update(headers)

    def attempt() -> dict[str, Any] | bytes:
        client = get_shared_http_client(timeout=timeout)

        # Pass timeout per-request as well: the shared client caches the
        # timeout it was first created with, so a later call asking for a
        # longer timeout would otherwise be silently capped at that value.
        if method == "GET":
            response = client.get(url, params=params, headers=request_headers, timeout=timeout)
        else:
            response = client.post(url, data=data, headers=request_headers, timeout=timeout)

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if _is_page_refusal(e.response.status_code, batch_size) and parse_json:
                if e.response.status_code == 500:
                    envelope = _json_error_envelope(e.response)
                    if envelope is not None:
                        # Newer ArcGIS servers mirror their JSON error code into
                        # the HTTP status - always their own 500, never a proxy's
                        # 502/504, whose JSON bodies (if any) are the proxy's and
                        # keep the retries. The envelope is the server's considered
                        # answer, and the caller's classifier (not this loop)
                        # decides whether it is a page-size problem or a specific,
                        # fatal error.
                        return envelope
            raise

        if not parse_json:
            return bytes(response.content)
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
            raise BatchTooLargeError(url=url, batch_size=batch_size, reason=detail) from e

    try:
        return cast(
            "dict[str, Any] | bytes",
            run_with_retry(attempt, max_retries=max_retries, retry_delay=retry_delay),
        )
    except (httpx.HTTPStatusError, RetryExhaustedError) as e:
        error, cause = final_request_error(
            url, e, max_retries=max_retries, credentials=credentials, batch_size=batch_size
        )
        raise error from cause


def final_request_error(
    url: str,
    failure: BaseException,
    *,
    max_retries: int,
    credentials: str,
    batch_size: int | None = None,
) -> tuple[RemoteAccessError | BatchTooLargeError, BaseException | None]:
    """The user-facing error (and its chained cause) for a ``run_with_retry`` failure.

    ``failure`` is what ``run_with_retry`` raised: an ``HTTPStatusError`` that was
    not retryable, or a ``RetryExhaustedError``. The one tail every caller shares;
    a caller that needs its own exception type re-wraps ``error.message``.
    """
    import httpx

    if isinstance(failure, httpx.HTTPStatusError):
        return status_error(url, failure.response, credentials), _cause(failure)
    last = failure.last_exception if isinstance(failure, RetryExhaustedError) else failure
    if isinstance(last, httpx.HTTPStatusError):
        status = last.response.status_code
        if _is_page_refusal(status, batch_size):
            # A 500/502/504 without a JSON body that survived every same-size
            # retry on a *paged* request: ArcGIS Server answers a page it cannot
            # serialize with HTTP 500 + an HTML error page on the GeoJSON path,
            # and a proxy in front of it gives up with 502/504 (#1134). The page
            # size is the one lever left, and it belongs to the caller's
            # batch-size ladder.
            return (
                BatchTooLargeError(
                    url=url,
                    batch_size=cast(int, batch_size),  # _is_page_refusal says it is set
                    reason=f"HTTP {status} persisted after {max_retries} attempts",
                ),
                last,
            )
        return status_error(url, last.response, credentials), _cause(last)
    return RemoteAccessError(url, f"Request failed after {max_retries} attempts: {last}"), last


def _is_page_refusal(status: int, batch_size: int | None) -> bool:
    return batch_size is not None and status in PAGE_REFUSAL_STATUSES


def _cause(exc: Any) -> Any:
    """The chained cause of a status error: none for the fixed fatal-status messages."""
    return None if exc.response.status_code in FATAL_STATUS_MESSAGES else exc
