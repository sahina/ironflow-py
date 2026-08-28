"""HTTP transport for the Ironflow Python SDK.

HAND-WRITTEN. Not generated — `cmd/sdk-gen` only emits `client.py` and
`models.py`; `client.py` subclasses `BaseClient` from this module. Keep real
logic here so ruff, mypy, and pytest can see it as ordinary Python.

Retry flow::

    attempt ──▶ send ──▶ 2xx ────────────────────────────────▶ return
                  │
                  ├─▶ HTTP error ──▶ classify ──┬─ not retryable ─▶ raise
                  │                             └─ retryable ──┐
                  ├─▶ network error ────────────────────────────┤
                  │                                             ▼
                  │                            attempts left? ──┴─ no ─▶ raise
                  │                                   │ yes
                  │                            method retryable? ─ no ─▶ raise
                  │                                   │ yes
                  │                            delay = Retry-After or backoff
                  │                                   │
                  │                            deadline would pass? ─ yes ─▶ raise
                  │                                   │ no
                  └───────────────── sleep ◀──────────┘

Rules that are easy to get wrong and are covered by tests:
  * Never sleep after the final attempt (wastes wall-clock, changes nothing).
  * Never retry a non-idempotent method by default. Retrying POST after a
    timeout can duplicate a committed write — on Ironflow that means a
    double-emitted event in an append-only stream. Callers who need it opt in
    per call and should pass their own Idempotency-Key header.

Everything below the transport treats the server as untrusted. It chooses the
response headers, the body, and the Location on a redirect, so each of those is
bounded here rather than taken at face value:
  * Retry-After is clamped to max_backoff, or one header decides how long the
    calling thread sleeps.
  * Cross-origin redirects are refused, or Authorization walks to another host
    (urllib keeps it; requests does not).
  * Bodies are size-capped, or the server chooses the client's memory use.
  * Every failure surfaces as IronflowError, including the ones that arrive as
    OverflowError or RecursionError from parsing hostile input.
"""

from __future__ import annotations

import contextlib
import json
import socket
import ssl
import time
from collections.abc import Mapping
from email.utils import parsedate_to_datetime
from typing import Any, TypedDict, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

__all__ = [
    "DEFAULT_SERVER_URL",
    "IDEMPOTENT_METHODS",
    "MAX_RESPONSE_BYTES",
    "BaseClient",
    "HealthResponse",
    "IronflowError",
    "ReadinessResponse",
    "ServerCapabilities",
]

DEFAULT_SERVER_URL = "http://localhost:9123"

#: Methods safe to retry automatically. Mirrors urllib3.Retry's default
#: allowed_methods: POST and PATCH are excluded because a retry after a
#: timeout can duplicate a committed write.
IDEMPOTENT_METHODS: frozenset[str] = frozenset(
    {"GET", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE"}
)

#: Status codes worth retrying. Everything else in 4xx is a client error that
#: will fail identically on retry.
_RETRYABLE_STATUS: frozenset[int] = frozenset({408, 429})

#: Ceiling on a single response body. Without it the server decides how much
#: memory the client allocates, and a trickled body never trips the socket
#: timeout (which measures inactivity, not total duration).
MAX_RESPONSE_BYTES = 32 * 1024 * 1024

#: Error bodies are echoed into the exception message, so they get a much
#: tighter cap — a 500 MB error body should not become a 500 MB log line.
_MAX_ERROR_BODY_BYTES = 8192

# Retry/backoff defaults mirror the Go SDK (sdk/go/ironflow/constants.go).
_DEFAULT_MAX_ATTEMPTS = 3
_DEFAULT_INITIAL_BACKOFF = 0.1
_DEFAULT_MAX_BACKOFF = 10.0
_DEFAULT_BACKOFF_FACTOR = 2.0


class HealthResponse(TypedDict, total=False):
    """Coarse liveness information returned by ``GET /health``."""

    status: str
    timestamp: str
    version: str
    warnings: list[str]


class ReadinessResponse(TypedDict, total=False):
    """Readiness state returned by ``GET /ready``."""

    status: str
    issues: dict[str, str]


class ServerCapabilities(TypedDict):
    """Transports and optional features exposed by the server."""

    transports: list[str]
    features: list[str]
    version: str
    auth_required: bool


class IronflowError(Exception):
    """Any failed Ironflow API call.

    Covers HTTP status errors *and* transport failures (DNS, refused
    connection, timeout, TLS) and malformed responses, so
    ``except IronflowError`` is sufficient — callers never need to catch
    ``urllib`` exceptions directly.
    """

    def __init__(
        self,
        message: str,
        status_code: int = 0,
        code: str = "",
        retryable: bool = False,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.retryable = retryable
        #: Seconds requested by the server's Retry-After header, if any.
        self.retry_after = retry_after


def _parse_retry_after(value: str | None) -> float | None:
    """Parse a Retry-After header.

    RFC 9110 allows two forms and hand-rolled parsers usually only handle the
    first: delta-seconds (``120``) and an HTTP-date
    (``Wed, 21 Oct 2026 07:28:00 GMT``). Returns None when absent or
    unparseable; never raises.
    """
    if not value:
        return None
    raw = value.strip()

    try:
        # int() accepts arbitrary precision; float() of a huge int raises
        # OverflowError, which would escape the IronflowError contract.
        return max(0.0, float(int(raw)))
    except (ValueError, OverflowError):
        pass

    try:
        when = parsedate_to_datetime(raw)
    except (TypeError, ValueError, IndexError):
        return None
    if when is None:
        return None

    try:
        delta = when.timestamp() - time.time()
    except (OverflowError, OSError, ValueError):
        return None
    return max(0.0, delta)


def _with_query(path: str, params: Mapping[str, Any] | None) -> str:
    """Append params to path as a query string.

    Shared by request() and _request() so the generated client and the escape
    hatch encode identically. Two rules matter:
      * None values are dropped, or an unset keyword argument on a generated
        method would arrive as the literal string "None".
      * Booleans render lowercase. Handlers compare `Query.Get("x") == "true"`,
        so Python's `str(True)` fails that comparison silently — the endpoint
        returns the unfiltered result instead of an error.
    """
    if not params:
        return path
    flat: dict[str, str] = {}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, bool):
            flat[key] = "true" if value else "false"
        else:
            flat[key] = str(value)
    if not flat:
        return path
    sep = "&" if "?" in path else "?"
    return f"{path}{sep}{urlencode(flat)}"


def _status_is_retryable(status: int) -> bool:
    return status >= 500 or status in _RETRYABLE_STATUS


def _origin(url: str) -> tuple[str, str, int | None]:
    parts = urlsplit(url)
    return (parts.scheme, parts.hostname or "", parts.port)


class _SafeRedirectHandler(HTTPRedirectHandler):
    """Redirect policy for an API client.

    urllib's stock handler copies every header except Content-Length and
    Content-Type onto the redirected request, so Authorization survives to a
    different host and across an https-to-http downgrade. requests strips it
    (Session.rebuild_auth); urllib does not. Refusing the redirect outright is
    stronger than stripping the header, because a followed cross-origin
    redirect also returns the other host's JSON to the caller as if Ironflow
    had sent it.

    Returning None makes urllib stop and surface the 3xx as an HTTPError,
    which _from_http_error turns into an IronflowError.
    """

    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> Request | None:
        if _origin(req.full_url) != _origin(newurl):
            return None
        if req.get_method() not in IDEMPOTENT_METHODS:
            # 301/302/303 re-issue the request as a bodyless GET. For a write
            # that means reporting success for something that never happened.
            return None
        return super().redirect_request(req, fp, code, msg, headers, newurl)


#: Module-level so connections and handler setup are not rebuilt per call.
_OPENER = build_opener(_SafeRedirectHandler)


class BaseClient:
    """Transport shared by every generated client method.

    ``timeout`` bounds socket inactivity on a single attempt, matching urllib
    semantics. ``total_timeout`` optionally bounds the whole call including
    retries and backoff: when set, each attempt's socket timeout is shrunk to
    whatever budget remains, so it is a real ceiling rather than advice. It
    stays best-effort in one respect — urllib gives us no hook to bound DNS
    resolution — and defaults to None to preserve prior behaviour.

    Response bodies are capped at ``MAX_RESPONSE_BYTES``; without a cap the
    server chooses how much memory the client allocates.
    """

    def __init__(
        self,
        server_url: str = DEFAULT_SERVER_URL,
        api_key: str | None = None,
        timeout: float = 30.0,
        total_timeout: float | None = None,
        max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
        retry_methods: frozenset[str] = IDEMPOTENT_METHODS,
        initial_backoff: float = _DEFAULT_INITIAL_BACKOFF,
        max_backoff: float = _DEFAULT_MAX_BACKOFF,
        backoff_factor: float = _DEFAULT_BACKOFF_FACTOR,
    ) -> None:
        self.server_url = server_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.total_timeout = total_timeout
        self.max_attempts = max(1, max_attempts)
        self.retry_methods = frozenset(m.upper() for m in retry_methods)
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_factor = backoff_factor

    # ── public ───────────────────────────────────────────────────────────

    def request(
        self,
        method: str,
        path: str,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        body: Any = None,
        retry: bool | None = None,
    ) -> Any:
        """Call any endpoint directly.

        The escape hatch for everything the generated methods cannot express.
        Headers declared in the route manifest and query parameters on typed
        routes have generated keyword arguments. Undeclared headers and query
        parameters on unannotated routes still need this method.

        Set ``retry=True`` to opt a non-idempotent call into retries — do that
        only alongside an ``Idempotency-Key`` header.
        """
        return self._send(method, _with_query(path, params), body, headers, retry)

    def health(self) -> HealthResponse:
        """Return the server's unauthenticated liveness response."""
        return cast("HealthResponse", self._request("GET", "/health"))

    def ready(self) -> ReadinessResponse:
        """Return readiness state, raising ``IronflowError`` when not ready."""
        return cast("ReadinessResponse", self._request("GET", "/ready"))

    def capabilities(self) -> ServerCapabilities:
        """Return the server's supported transports and optional features."""
        return cast(
            "ServerCapabilities", self._request("GET", "/api/v1/capabilities")
        )

    # ── internal ─────────────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        body: Any = None,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str | None] | None = None,
    ) -> Any:
        """Entry point used by every generated method in client.py."""
        present_headers = (
            {key: value for key, value in headers.items() if value is not None}
            if headers is not None
            else None
        )
        return self._send(
            method, _with_query(path, params), body, present_headers, None
        )

    def _send(
        self,
        method: str,
        path: str,
        body: Any,
        headers: Mapping[str, str] | None,
        retry_override: bool | None,
    ) -> Any:
        method = method.upper()
        if retry_override is None:
            may_retry = method in self.retry_methods
        else:
            may_retry = retry_override

        deadline = (
            time.monotonic() + self.total_timeout
            if self.total_timeout is not None
            else None
        )

        attempts = self.max_attempts if may_retry else 1
        delay = self.initial_backoff
        last: IronflowError

        for attempt in range(1, attempts + 1):
            try:
                return self._attempt(method, path, body, headers, deadline)
            except IronflowError as err:
                last = err

            # Do NOT sleep after the final attempt: nothing follows it, so the
            # wait is pure wasted wall-clock.
            is_final = attempt == attempts
            if is_final or not last.retryable:
                raise last

            # Clamp the server's Retry-After to our own ceiling. Unclamped, one
            # header decides how long the calling thread is suspended — a 429
            # carrying Retry-After: 86400 would sleep it for a day. The error
            # still carries .retry_after, so a caller who wants to honour a long
            # delay can, deliberately, at their own layer.
            wait = (
                min(last.retry_after, self.max_backoff)
                if last.retry_after is not None
                else delay
            )
            delay = min(delay * self.backoff_factor, self.max_backoff)

            if deadline is not None and time.monotonic() + wait >= deadline:
                raise last
            time.sleep(wait)

        raise last  # unreachable; satisfies type checkers

    def _attempt(
        self,
        method: str,
        path: str,
        body: Any,
        headers: Mapping[str, str] | None,
        deadline: float | None = None,
    ) -> Any:
        url = f"{self.server_url}{path}"
        data = json.dumps(body).encode("utf-8") if body is not None else None

        req = Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if self.api_key:
            req.add_header("Authorization", f"Bearer {self.api_key}")
        for key, value in (headers or {}).items():
            req.add_header(key, value)

        # Give this attempt only the budget that is left. Passing the full
        # self.timeout regardless is how a total_timeout of 0.5s could still
        # block for 30s.
        timeout = self.timeout
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise IronflowError(
                    f"total_timeout of {self.total_timeout}s elapsed before "
                    f"calling {path}",
                    retryable=False,
                )
            timeout = min(timeout, remaining)

        try:
            with _OPENER.open(req, timeout=timeout) as resp:
                raw = resp.read(MAX_RESPONSE_BYTES + 1)
                if len(raw) > MAX_RESPONSE_BYTES:
                    raise IronflowError(
                        f"response from {path} exceeds {MAX_RESPONSE_BYTES} "
                        f"bytes; refusing to buffer it"
                    )
        except HTTPError as e:
            raise self._from_http_error(e) from e
        except TimeoutError as e:
            raise IronflowError(
                f"request to {path} timed out after {timeout}s",
                retryable=True,
            ) from e
        except ssl.SSLError as e:
            # TLS failures are configuration problems; retrying repeats them.
            raise IronflowError(f"TLS error calling {path}: {e}") from e
        except URLError as e:
            reason = e.reason
            if isinstance(reason, socket.timeout):
                raise IronflowError(
                    f"request to {path} timed out after {timeout}s",
                    retryable=True,
                ) from e
            if isinstance(reason, ssl.SSLError):
                raise IronflowError(f"TLS error calling {path}: {reason}") from e
            raise IronflowError(
                f"cannot reach {self.server_url}: {reason}", retryable=True
            ) from e
        except OSError as e:
            raise IronflowError(
                f"cannot reach {self.server_url}: {e}", retryable=True
            ) from e

        if not raw:
            return None
        try:
            return json.loads(raw)
        except (ValueError, RecursionError) as e:
            # A 200 that is not JSON usually means a proxy or captive portal
            # answered instead of the server. Retrying rarely helps.
            # RecursionError comes from deeply nested JSON — a server-controlled
            # input, so it must surface as IronflowError like everything else.
            preview = raw[:200].decode("utf-8", errors="replace")
            raise IronflowError(f"expected JSON from {path}, got: {preview}") from e

    def _from_http_error(self, e: HTTPError) -> IronflowError:
        body_text = ""
        try:
            # Bounded: this text becomes the exception message, so an
            # unbounded read would put a server-sized string into the caller's
            # logs. Closed explicitly — an unread HTTPError holds its socket,
            # and the exception is retained as __cause__.
            body_text = e.read(_MAX_ERROR_BODY_BYTES).decode("utf-8", errors="replace")
        except (OSError, ValueError, AttributeError):
            # Body already consumed or the connection dropped. The status code
            # is the useful part; carry on without the body.
            body_text = ""
        finally:
            with contextlib.suppress(Exception):
                e.close()

        code = ""
        message = f"HTTP {e.code}"
        if 300 <= e.code < 400:
            message = (
                f"HTTP {e.code}: refused to follow a redirect from {e.url} "
                f"to {e.headers.get('Location', '?') if e.headers else '?'}. "
                f"Cross-origin redirects would leak the API key, and "
                f"redirected writes would silently become reads."
            )
        elif body_text:
            try:
                parsed = json.loads(body_text)
            except (ValueError, RecursionError):
                message = body_text
            else:
                if isinstance(parsed, dict):
                    code = str(parsed.get("code", "") or "")
                    message = str(parsed.get("message", message) or message)
                else:
                    message = body_text

        retry_after = _parse_retry_after(
            e.headers.get("Retry-After") if e.headers else None
        )
        return IronflowError(
            message,
            status_code=e.code,
            code=code,
            retryable=_status_is_retryable(e.code),
            retry_after=retry_after,
        )
