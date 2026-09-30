"""Runtime for the Ironflow ConnectRPC clients.

HAND-WRITTEN. Not generated — `make proto-python` emits `_client.py` (the
capability namespaces and both coordinators) and `v1.py` (the public types).
Keep real logic here so ruff, mypy --strict, and pytest see it as ordinary
Python. Same split as `client.py` / `_http.py` on the REST side.

What lives here and why (#1781, ADR 0062):

  * `IronflowRPCError` — the Ironflow error contract for Connect failures.
    Callers must not have to catch `connectrpc.errors.ConnectError`, which is
    a pre-1.0 upstream type.
  * `_AuthErrorInterceptor` — ONE interceptor doing three jobs. Auth has to be
    an interceptor because `ConnectClientSync.__init__` takes no `headers`
    argument; per-call headers would mean threading the key through all 88
    generated call sites. Error translation rides along because the same object
    already wraps every call, and because a server-stream interceptor returns
    the iterator, which is the only place a mid-iteration failure can be
    caught. Unary retry joins them because it is the only layer that both sees
    `ctx.method` and can re-invoke the send.
  * `_retry_waits` — the retry decision, in one place so the sync and async
    paths cannot drift. See its docstring for the whole policy.
  * `_Coordinator` / `_AsyncCoordinator` — transport ownership and the closed
    flag. The generated coordinators subclass these and add the namespaces.

Stream reconnection is scoped, not absent (#1848). `Subscribe` reconnects when
— and only when — the caller positioned it with `start_after_sequence`, which
is both the cursor to resume from and the opt-in. The other three exposed
streams are durable and server-positioned already, so a client-driven reconnect
on them would be wrong rather than merely unnecessary. See `_resumable`.
"""

from __future__ import annotations

import asyncio
import copy
import time
from typing import TYPE_CHECKING, Any, TypeVar

from connectrpc.errors import ConnectError
from connectrpc.method import IdempotencyLevel

from .._http import (
    _DEFAULT_BACKOFF_FACTOR,
    _DEFAULT_INITIAL_BACKOFF,
    _DEFAULT_MAX_ATTEMPTS,
    _DEFAULT_MAX_BACKOFF,
    DEFAULT_SERVER_URL,
    ErrorContext,
    ErrorHook,
    IronflowError,
    anotify_error,
    notify_error,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterator

    from connectrpc.request import RequestContext

__all__ = [
    "NO_TIMEOUT",
    "IronflowRPCError",
]

REQ = TypeVar("REQ")
RES = TypeVar("RES")


class _NoTimeout:
    """Type of the NO_TIMEOUT sentinel."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "ironflow.NO_TIMEOUT"


#: Pass as ``timeout=`` to run one call with no deadline, overriding a deadline
#: set on the client.
#:
#: ``timeout=None`` means "inherit the client's default", which leaves no way to
#: say "no deadline on THIS call" once the client has one. ``timeout=0`` would
#: read as "expire immediately" — the opposite of what it would mean here — so
#: the escape is a named object instead of a magic number.
NO_TIMEOUT = _NoTimeout()


class IronflowRPCError(IronflowError):
    """A failed ConnectRPC call.

    Subclasses `IronflowError`, so ``except IronflowError`` still catches every
    Ironflow failure regardless of which client raised it.

    The originating `connectrpc.errors.ConnectError` is attached as
    ``__cause__``. Reach through it only for something this type does not
    carry — depending on it directly re-couples your code to a pre-1.0 upstream
    interface, which is the coupling this class exists to remove.

    Inherited fields from `IronflowError` that do NOT mean here what they mean
    on an HTTP error:

    ``retryable`` is always ``False``, and stays that way now that the client
    DOES retry (#1809). It means "there is nothing left for this client to
    do": by the time you hold this error, either the method's contract barred
    a retry or the attempt budget ran out. It is deliberately not the
    "safe to repeat" flag — that lives on the method, not the error, and
    exposing it here would let ``if e.retryable: call_again()`` re-send a
    write. Read ``code`` and decide for yourself.

    ``status_code`` is always ``0``. Connect codes are not HTTP statuses.
    Synthesising one would invite ``if e.status_code == 503`` branches that are
    one protocol change away from wrong.

    ``retry_after`` is always ``None``. Connect carries no equivalent of the
    ``Retry-After`` header.
    """

    def __init__(self, message: str, code: str = "", details: tuple[Any, ...] = ()) -> None:
        super().__init__(message, status_code=0, code=code, retryable=False)
        #: Structured details the server attached, as generated protobuf
        #: messages. Empty for most errors. Their types are public through
        #: `ironflow.rpc.v1`.
        self.details = details

    @classmethod
    def _from_connect(cls, err: ConnectError) -> IronflowRPCError:
        # err.code is a str-valued enum, so `.value` IS the canonical Connect
        # code name ("unavailable", "not_found"). No mapping table to drift.
        code = err.code.value if err.code is not None else ""
        return cls(str(err), code=code, details=tuple(err.details or ()))


class _ClosedError(IronflowRPCError):
    """Raised on use after close. Private: callers catch IronflowRPCError."""


def _closed(client_name: str, method: str) -> _ClosedError:
    return _ClosedError(
        f"{client_name} is closed; {method}() cannot be called. "
        f"Create a new client, or use it as a context manager so the "
        f"lifetime is explicit.",
        code="unavailable",
    )


def _closed_stream(client_name: str) -> _ClosedError:
    """Raised when an already-created stream is iterated after close.

    Stream I/O is lazy: `rpc.pubsub.subscribe(...)` builds an iterator and sends
    nothing. Without this guard, `stream = rpc.pubsub.subscribe(...)` followed by
    `rpc.close()` and then iteration issues an authenticated request AFTER the
    client's context manager has exited — measured against the test server as
    zero requests before the close and one after.
    """
    return _ClosedError(
        f"{client_name} was closed before this stream was read. A stream is "
        f"lazy: creating it sends nothing, so it cannot outlive the client that "
        f"created it. Read it inside the client's lifetime.",
        code="unavailable",
    )


def _error_context(ctx: RequestContext[Any, Any]) -> ErrorContext:
    method = ctx.method
    return ErrorContext(method=method.name, endpoint=f"/{method.service_name}/{method.name}")


#: The only Connect code worth a second attempt.
#:
#: `unavailable` is what `_client_sync.py` raises for every transport failure —
#: refused connection, DNS, a socket dropped mid-response — and what the server
#: sends when it is shedding load. Nothing else qualifies:
#:
#:   * `deadline_exceeded` already consumed the caller's whole budget.
#:   * `resource_exhausted` is raised CLIENT-side for an oversized response
#:     (`_client_sync.py:343`), so retrying repeats a deterministic failure.
#:   * every other code is a decision the server will reach again identically.
_RETRYABLE_CODES: frozenset[str] = frozenset({"unavailable"})


def _retry_waits(
    ctx: RequestContext[Any, Any],
    is_closed: Callable[[], bool],
    max_attempts: int,
) -> Iterator[float]:
    """Yield the seconds to sleep before each retry; stop to give up.

    The whole retry policy, in one generator, so the sync and async paths share
    a single decision and can only differ in how they sleep. It is advanced
    lazily — one `next()` per failure — so every check below reads the state at
    the moment the decision is made, not at the moment the call started.

    Four independent reasons to stop:

    **The method's contract.** Only `NO_SIDE_EFFECTS` is retried, and this is
    the whole reason PR 1 of #1809 annotated the protos. `IDEMPOTENT` is
    deliberately NOT accepted: no Ironflow method uses it, and accepting it
    would put this predicate out of step with `scripts/check-proto-idempotency.sh`,
    which only guards `NO_SIDE_EFFECTS` — a gate and a policy disagreeing about
    which annotation is dangerous is how a write gets retried by accident.

    **The attempt budget.** `max_attempts` counts attempts, not retries, so it
    yields at most `max_attempts - 1` times. `max_attempts=1` disables retry.

    **The deadline.** `ctx.timeout_ms` is not the configured timeout — it is the
    time REMAINING against an absolute deadline fixed when the context was
    built. That is what makes `timeout=` still bound the whole call rather than
    each attempt, which is what `_timeout_ms` promises. Refusing to sleep past
    it also keeps `_send_request_unary` from handing pyqwest a negative timeout,
    whose failure would come back as `unavailable` and read as retryable.

    **The client closing.** A `close()` during a backoff should not be followed
    by another authenticated request.
    """
    if ctx.method.idempotency_level is not IdempotencyLevel.NO_SIDE_EFFECTS:
        return

    delay = _DEFAULT_INITIAL_BACKOFF
    for _ in range(max_attempts - 1):
        if is_closed():
            return
        wait = delay
        delay = min(delay * _DEFAULT_BACKOFF_FACTOR, _DEFAULT_MAX_BACKOFF)
        remaining_ms = ctx.timeout_ms
        if remaining_ms is not None and remaining_ms / 1000.0 <= wait:
            return
        yield wait


class _AuthErrorInterceptor:
    """Injects the bearer token and translates Connect failures.

    Implements the sync AND async interceptor protocols. They are separate
    `Protocol`s upstream (`intercept_unary_sync` vs `intercept_unary`), but the
    behaviour is identical and duplicating it into two classes would let one
    drift.

    The stream methods carry the four exposed server streams (`stream_events`,
    `wait_catchup_stream`, `subscribe`, `join_consumer_group`). A translation
    layer that silently stopped applying at the stream boundary would be worse
    than none, so they route through here too; `tests/test_rpc_streams.py`
    exercises both the creation-failure and mid-iteration paths.
    """

    def __init__(
        self,
        api_key: str | None,
        is_closed: Callable[[], bool] | None = None,
        client_name: str = "The client",
        max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
        on_error: ErrorHook | None = None,
    ) -> None:
        self._auth = f"Bearer {api_key}" if api_key else None
        #: Reads the owning coordinator's closed flag. The interceptor is the
        #: only layer that sees a stream begin, so the post-close guard has to
        #: live here rather than on the coordinator.
        self._is_closed = is_closed or (lambda: False)
        self._client_name = client_name
        self._max_attempts = max(1, max_attempts)
        self._on_error = on_error

    def _apply_auth(self, ctx: RequestContext[Any, Any]) -> None:
        if self._auth is not None:
            ctx.request_headers["authorization"] = self._auth

    def _report_sync(self, err: IronflowRPCError, ctx: RequestContext[Any, Any]) -> None:
        if not isinstance(err, _ClosedError):  # use after close is a programming error, not a failed call
            notify_error(self._on_error, err, _error_context(ctx))

    async def _report_async(self, err: IronflowRPCError, ctx: RequestContext[Any, Any]) -> None:
        if not isinstance(err, _ClosedError):
            await anotify_error(self._on_error, err, _error_context(ctx))

    # With no hook every wrapper below returns the old code path untouched, so "no hook, no behavior
    # change" holds by construction and the resume and aclose machinery never sees the extra layer.

    def intercept_unary_sync(
        self,
        call_next: Callable[[REQ, RequestContext[REQ, RES]], RES],
        request: REQ,
        ctx: RequestContext[REQ, RES],
    ) -> RES:
        if self._on_error is None:
            return self._unary_sync(call_next, request, ctx)
        try:
            return self._unary_sync(call_next, request, ctx)
        except IronflowRPCError as err:
            self._report_sync(err, ctx)
            raise

    def intercept_server_stream_sync(
        self,
        call_next: Callable[[REQ, RequestContext[REQ, RES]], Iterator[RES]],
        request: REQ,
        ctx: RequestContext[REQ, RES],
    ) -> Iterator[RES]:
        if self._on_error is None:
            return self._stream_sync(call_next, request, ctx)
        try:
            stream = self._stream_sync(call_next, request, ctx)  # creation can fail before any iteration
        except IronflowRPCError as err:
            self._report_sync(err, ctx)
            raise
        return _reporting_iter(stream, lambda err: self._report_sync(err, ctx))

    async def intercept_unary(
        self,
        call_next: Callable[[REQ, RequestContext[REQ, RES]], Any],
        request: REQ,
        ctx: RequestContext[REQ, RES],
    ) -> Any:
        if self._on_error is None:
            return await self._unary_async(call_next, request, ctx)
        try:
            return await self._unary_async(call_next, request, ctx)
        except IronflowRPCError as err:
            await self._report_async(err, ctx)
            raise

    def intercept_server_stream(
        self,
        call_next: Callable[[REQ, RequestContext[REQ, RES]], AsyncIterator[RES]],
        request: REQ,
        ctx: RequestContext[REQ, RES],
    ) -> AsyncIterator[RES]:
        # NOT async def: same reason as `_stream_async`.
        stream = self._stream_async(call_next, request, ctx)
        if self._on_error is None:
            return stream
        return _reporting_aiter(stream, lambda err: self._report_async(err, ctx))

    # ── sync ─────────────────────────────────────────────────────────────────

    def _unary_sync(
        self,
        call_next: Callable[[REQ, RequestContext[REQ, RES]], RES],
        request: REQ,
        ctx: RequestContext[REQ, RES],
    ) -> RES:
        self._apply_auth(ctx)
        waits = _retry_waits(ctx, self._is_closed, self._max_attempts)
        while True:
            try:
                return call_next(request, ctx)
            except ConnectError as err:
                translated = IronflowRPCError._from_connect(err)
                if translated.code not in _RETRYABLE_CODES:
                    raise translated from err
                wait = next(waits, None)
                if wait is None:
                    raise translated from err
            # Sleeping outside the `except` keeps the failed attempt's traceback
            # from being chained onto whatever the next one raises.
            time.sleep(wait)

    def _stream_sync(
        self,
        call_next: Callable[[REQ, RequestContext[REQ, RES]], Iterator[RES]],
        request: REQ,
        ctx: RequestContext[REQ, RES],
    ) -> Iterator[RES]:
        self._apply_auth(ctx)
        if _resumable(request):
            return _resuming_iter(
                call_next, request, ctx, self._is_closed,
                self._client_name, self._max_attempts,
            )
        try:
            stream = call_next(request, ctx)
        except ConnectError as err:
            raise IronflowRPCError._from_connect(err) from err
        return _translating_iter(stream, self._is_closed, self._client_name)

    # ── async ────────────────────────────────────────────────────────────────

    async def _unary_async(
        self,
        call_next: Callable[[REQ, RequestContext[REQ, RES]], Any],
        request: REQ,
        ctx: RequestContext[REQ, RES],
    ) -> Any:
        self._apply_auth(ctx)
        waits = _retry_waits(ctx, self._is_closed, self._max_attempts)
        while True:
            try:
                return await call_next(request, ctx)
            except ConnectError as err:
                translated = IronflowRPCError._from_connect(err)
                if translated.code not in _RETRYABLE_CODES:
                    raise translated from err
                wait = next(waits, None)
                if wait is None:
                    raise translated from err
            await asyncio.sleep(wait)

    def _stream_async(
        self,
        call_next: Callable[[REQ, RequestContext[REQ, RES]], AsyncIterator[RES]],
        request: REQ,
        ctx: RequestContext[REQ, RES],
    ) -> AsyncIterator[RES]:
        # NOT async def: the upstream protocol expects the AsyncIterator back
        # directly, not a coroutine resolving to one. Awaiting here would make
        # `async for` fail on a coroutine object.
        self._apply_auth(ctx)
        if _resumable(request):
            return _resuming_aiter(
                call_next, request, ctx, self._is_closed,
                self._client_name, self._max_attempts,
            )
        return _translating_aiter(
            call_next, request, ctx, self._is_closed, self._client_name
        )


def _resumable(request: Any) -> bool:
    """Did the caller position this stream?

    The gate for automatic reconnection, and it does two jobs at once.

    It scopes to `Subscribe`: of the four exposed server streams only
    `SubscribeRequest` has an `options` message, so
    `StreamProjectionEvents`, `WaitProjectionCatchupStream` and
    `JoinConsumerGroup` fall out here without being named. That is deliberate
    rather than lucky — the other three are already durable and
    server-positioned, so a client-driven reconnect on them would be wrong,
    not merely unnecessary (#1848).

    And it scopes to callers who opted in. Reconnecting a stream the caller
    never positioned would mean guessing where to resume from, which is the
    guessing the cursor exists to replace: `replay` is a count, and starting
    from "now" silently skips whatever arrived while the connection was down.
    """
    options = getattr(request, "options", None)
    if options is None:
        return False
    try:
        return bool(options.has_field("start_after_sequence"))
    except (AttributeError, ValueError, KeyError):
        return False


class _StreamResume:
    """Cursor and retry budget for one resumable subscription.

    Split out so the sync and async paths share one policy and differ only in
    how they sleep — the same reason `_retry_waits` exists for unary calls.
    """

    def __init__(self, request: Any, max_attempts: int) -> None:
        self._request = request
        self._cursor: int = request.options.start_after_sequence
        self._max_attempts = max_attempts
        self.attempts_left = max_attempts
        self.delay = _DEFAULT_INITIAL_BACKOFF

    def next_request(self) -> Any:
        """A fresh request anchored at the last event actually delivered.

        Deep-copied per attempt rather than mutated in place. The caller keeps
        a reference to the object they passed in, and a stream is lazy — it is
        read at send time, which for a reconnect is long after `subscribe()`
        returned — so mutating either one would be visible to them at a moment
        they have no reason to expect.
        """
        request = copy.deepcopy(self._request)
        request.options.start_after_sequence = self._cursor
        return request

    def progressed(self, event: Any) -> None:
        """Record a delivered event: advance the cursor, refill the budget.

        Refilling on progress is what makes a long-lived subscription viable.
        Without it a stream that reconnects normally over days dies on its
        third reconnect, having worked perfectly throughout.

        The accepted cost is a server that accepts, sends exactly one event and
        drops: that reconnects indefinitely, with the backoff reset each time.
        Judged the right trade — the client IS receiving events, so this is a
        slow subscription rather than a spin, and the alternative kills healthy
        long-lived streams to defend against a pathological server.

        Guarded against a zero sequence, which would rewind the cursor to the
        start of the stream and replay everything. That cannot happen for a
        positioned subscription — the server routes those to JetStream, which
        always reports a sequence — but a cursor is not the place to find out.
        """
        sequence = getattr(event, "sequence", 0)
        if sequence:
            self._cursor = sequence
        self.attempts_left = self._max_attempts
        self.delay = _DEFAULT_INITIAL_BACKOFF

    def wait(self, ctx: RequestContext[Any, Any], is_closed: Callable[[], bool]) -> float | None:
        """Seconds to sleep before reconnecting, or None to give up."""
        self.attempts_left -= 1
        if self.attempts_left <= 0 or is_closed():
            return None
        wait = self.delay
        self.delay = min(self.delay * _DEFAULT_BACKOFF_FACTOR, _DEFAULT_MAX_BACKOFF)
        # `timeout=` bounds the whole subscription, reconnects included. See
        # _retry_waits for why ctx.timeout_ms is the REMAINING budget.
        remaining_ms = ctx.timeout_ms
        if remaining_ms is not None and remaining_ms / 1000.0 <= wait:
            return None
        return wait


def _resuming_iter(
    call_next: Callable[[REQ, RequestContext[REQ, RES]], Iterator[RES]],
    request: REQ,
    ctx: RequestContext[REQ, RES],
    is_closed: Callable[[], bool],
    client_name: str,
    max_attempts: int,
) -> Iterator[RES]:
    """`_translating_iter` plus reconnection, for a positioned subscription.

    A clean end of stream returns; the server decided it was done. Only a
    transport failure reconnects, and only from the last event delivered.

    Delivery is at-least-once across a reconnect: the server sending a frame is
    not the client having processed it, so the frame in flight when the
    connection dropped can arrive twice. That is stated in the docs rather than
    papered over — deduplicating it would need an ack the protocol has no room
    for.
    """
    state = _StreamResume(request, max_attempts)
    while True:
        if is_closed():
            raise _closed_stream(client_name)
        try:
            for event in call_next(state.next_request(), ctx):
                state.progressed(event)
                yield event
            return
        except ConnectError as err:
            translated = IronflowRPCError._from_connect(err)
            if translated.code not in _RETRYABLE_CODES:
                raise translated from err
            wait = state.wait(ctx, is_closed)
            if wait is None:
                raise translated from err
        time.sleep(wait)


async def _resuming_aiter(
    call_next: Callable[[REQ, RequestContext[REQ, RES]], AsyncIterator[RES]],
    request: REQ,
    ctx: RequestContext[REQ, RES],
    is_closed: Callable[[], bool],
    client_name: str,
    max_attempts: int,
) -> AsyncIterator[RES]:
    """Async twin of `_resuming_iter`.

    The `finally` around each attempt is load-bearing for the same reason it is
    in `_translating_aiter`: `async for` does not close its iterator, so a
    dropped attempt would hold its HTTP response and server-side subscription
    until garbage collection — once per reconnect.
    """
    state = _StreamResume(request, max_attempts)
    while True:
        if is_closed():
            raise _closed_stream(client_name)
        stream = call_next(state.next_request(), ctx)
        try:
            async for event in stream:
                state.progressed(event)
                yield event
            return
        except ConnectError as err:
            translated = IronflowRPCError._from_connect(err)
            if translated.code not in _RETRYABLE_CODES:
                raise translated from err
            wait = state.wait(ctx, is_closed)
            if wait is None:
                raise translated from err
        finally:
            aclose = getattr(stream, "aclose", None)
            if aclose is not None:
                await aclose()
        await asyncio.sleep(wait)


def _translating_iter(
    stream: Iterator[RES],
    is_closed: Callable[[], bool],
    client_name: str,
) -> Iterator[RES]:
    """Re-raise a mid-iteration ConnectError as IronflowRPCError.

    A server stream can fail at any yield, not only at creation — the error
    arrives in the closing frame. Wrapping the creation call alone would let
    every such failure through untranslated.

    The `is_closed` guard runs on the first `next()`, which is BEFORE
    `yield from` pulls anything, so a stream created before `close()` sends no
    request. See `_closed_stream`.

    `yield from`, not an explicit loop: it propagates `.close()` to `stream`, so
    closing this wrapper closes the transport iterator underneath. The async
    twin has to do that by hand — `async for` does not.
    """
    if is_closed():
        raise _closed_stream(client_name)
    try:
        yield from stream
    except ConnectError as err:
        raise IronflowRPCError._from_connect(err) from err


async def _translating_aiter(
    call_next: Callable[[REQ, RequestContext[REQ, RES]], AsyncIterator[RES]],
    request: REQ,
    ctx: RequestContext[REQ, RES],
    is_closed: Callable[[], bool],
    client_name: str,
) -> AsyncIterator[RES]:
    """Async twin of `_translating_iter`.

    The `finally` is load-bearing and is NOT symmetric with the sync path.
    `async for` does not close its iterator when the body raises or the caller
    stops consuming, so without this the upstream generator stays suspended and
    its HTTP response, connection and server-side subscription are held until
    garbage collection. Measured before the fix: closing this wrapper left the
    upstream open, while the `yield from` in the sync twin closed it.
    """
    if is_closed():
        raise _closed_stream(client_name)
    stream = call_next(request, ctx)
    try:
        async for item in stream:
            yield item
    except ConnectError as err:
        raise IronflowRPCError._from_connect(err) from err
    finally:
        aclose = getattr(stream, "aclose", None)
        if aclose is not None:
            await aclose()


def _reporting_iter(stream: Iterator[RES], report: Callable[[IronflowRPCError], None]) -> Iterator[RES]:
    """Report a mid-iteration failure. `yield from` propagates `.close()` to the stream underneath."""
    try:
        yield from stream
    except IronflowRPCError as err:
        report(err)
        raise


async def _reporting_aiter(
    stream: AsyncIterator[RES], report: Callable[[IronflowRPCError], Awaitable[None]],
) -> AsyncIterator[RES]:
    """Async twin. The `finally` matters for the reason it does in `_translating_aiter`."""
    try:
        async for item in stream:
            yield item
    except IronflowRPCError as err:
        await report(err)
        raise
    finally:
        aclose = getattr(stream, "aclose", None)
        if aclose is not None:
            await aclose()


def _timeout_ms(
    timeout: float | _NoTimeout | None,
    default: float | None,
) -> int | None:
    """Resolve a per-call timeout to the milliseconds the generated layer wants.

    Three inputs, three meanings:
      None        inherit the client default (which may itself be None)
      NO_TIMEOUT  no deadline on this call, even if the client has one
      float       seconds

    Seconds, not milliseconds, because that is what every Python HTTP client
    means by `timeout` and what `IronflowClient` already takes. NOTE the two
    are not the same measurement: `IronflowClient.timeout` bounds socket
    inactivity per attempt (urllib semantics); this bounds the WHOLE call.
    """
    if isinstance(timeout, _NoTimeout):
        return None
    seconds = default if timeout is None else timeout
    if seconds is None:
        return None
    if seconds <= 0:
        raise ValueError(
            f"timeout must be positive, got {seconds!r}. "
            f"For no deadline pass ironflow.NO_TIMEOUT."
        )
    # Clamp to 1ms rather than truncating. `int(0.0005 * 1000)` is 0, and the
    # generated layer resolves the deadline as `timeout_ms or default` — so a
    # zero would be falsy, fall through to a default the service clients do not
    # set, and turn the TIGHTEST deadline a caller can ask for into NO deadline.
    # Failing open on a timeout is the one direction that must not happen
    # silently, so a sub-millisecond request becomes the smallest deadline the
    # protocol can express.
    return max(1, int(seconds * 1000))


class _Coordinator:
    """Transport ownership and lifecycle for the sync client.

    The generated `IronflowRPC` subclasses this and adds the namespaces. One
    `pyqwest` client is shared by every generated service client — that is what
    `http_client=` on the upstream constructor is for, and it is why the
    coordinator owns the lifetime rather than each namespace.

    ``max_attempts`` bounds how many times ONE unary call may be sent. It only
    ever applies to a method the protos annotate `NO_SIDE_EFFECTS`; pass 1 to
    turn retry off entirely. The backoff schedule itself is not configurable —
    the four constants are shared with `_http.py` so the two transports back off
    identically. See `_retry_waits`.

    ``on_error`` fires once per failed call, after retries; see `ErrorContext`.

    On what "close" actually does, because upstream is not what it looks like:
    `pyqwest.SyncClient` has NO close method and is NOT a context manager (the
    upstream docstring shows `with SyncClient() as http_client`, which raises
    TypeError — verified). `ConnectClientSync.close()` only flips its own
    `_closed` flag and never touches the HTTP client. So closing here marks
    every client unusable and lets the transport be released by refcount; there
    is no socket to shut down explicitly. The subclass supplies
    `_service_clients()` so the flag propagates to anything holding a namespace
    reference.
    """

    def _service_clients(self) -> tuple[Any, ...]:
        """Generated subclasses return their service clients. Base has none."""
        return ()

    _NAME = "IronflowRPC"

    def __init__(
        self,
        server_url: str = DEFAULT_SERVER_URL,
        api_key: str | None = None,
        timeout: float | None = None,
        read_max_bytes: int | None = None,
        max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
        on_error: ErrorHook | None = None,
    ) -> None:
        from pyqwest import SyncClient

        self._server_url = server_url.rstrip("/")
        self._default_timeout = timeout
        self._read_max_bytes = read_max_bytes
        # Set BEFORE the interceptor, which captures a reader for it.
        self._closed_flag = False
        self._interceptor = _AuthErrorInterceptor(
            api_key, lambda: self._closed_flag, self._NAME, max_attempts, on_error
        )
        self._http = SyncClient()

    def _client_kwargs(self) -> dict[str, Any]:
        return {
            "http_client": self._http,
            "interceptors": (self._interceptor,),
            "read_max_bytes": self._read_max_bytes,
        }

    def _check_open(self, method: str) -> None:
        if self._closed_flag:
            raise _closed(self._NAME, method)

    def close(self) -> None:
        """Mark the client and its service clients unusable. Idempotent."""
        if not self._closed_flag:
            self._closed_flag = True
            for client in self._service_clients():
                client.close()

    def __enter__(self) -> Any:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class _AsyncCoordinator:
    """Transport ownership and lifecycle for the async client.

    `aclose()`, not `close()` — asyncio convention, and it makes
    `await rpc.aclose()` read correctly. The generated service clients expose
    an awaitable `close()`; this layer is the one callers touch.

    See `_Coordinator` for why closing releases no socket.

    ``on_error`` fires once per failed call, after retries; see `ErrorContext`.
    """

    def _service_clients(self) -> tuple[Any, ...]:
        """Generated subclasses return their service clients. Base has none."""
        return ()

    _NAME = "AsyncIronflowRPC"

    def __init__(
        self,
        server_url: str = DEFAULT_SERVER_URL,
        api_key: str | None = None,
        timeout: float | None = None,
        read_max_bytes: int | None = None,
        max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
        on_error: ErrorHook | None = None,
    ) -> None:
        from pyqwest import Client

        self._server_url = server_url.rstrip("/")
        self._default_timeout = timeout
        self._read_max_bytes = read_max_bytes
        # Set BEFORE the interceptor, which captures a reader for it.
        self._closed_flag = False
        self._interceptor = _AuthErrorInterceptor(
            api_key, lambda: self._closed_flag, self._NAME, max_attempts, on_error
        )
        self._http = Client()

    def _client_kwargs(self) -> dict[str, Any]:
        return {
            "http_client": self._http,
            "interceptors": (self._interceptor,),
            "read_max_bytes": self._read_max_bytes,
        }

    def _check_open(self, method: str) -> None:
        if self._closed_flag:
            raise _closed(self._NAME, method)

    async def aclose(self) -> None:
        """Mark the client and its service clients unusable. Idempotent."""
        if not self._closed_flag:
            self._closed_flag = True
            for client in self._service_clients():
                await client.close()

    async def __aenter__(self) -> Any:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
