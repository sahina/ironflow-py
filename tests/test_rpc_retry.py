"""Automatic unary retry on the ConnectRPC clients (#1809).

Two layers, because they answer different questions.

The **behavioural** tests drive a real Connect server and read what it saw. A
retry is only real if a second request reaches the wire, and only safe if a
second request does NOT reach the wire for a write — neither is observable from
the client side, so the server's own count is the assertion. Every one is
parameterized over the sync and async clients: they are separate interceptor
protocol methods with separate loops, so a suite that exercised one would leave
the other's policy written but unproven.

The **policy** tests drive `_retry_waits` directly. Two of its four stop
conditions — the client closing, and the deadline running out mid-backoff — can
only be reached by winning a race against a sleeping thread. Asserting them
through the server would be a flaky test of a deterministic function.

On what "connection loss before / after acknowledgement" means here: the client
cannot distinguish them, which is exactly why retry is gated on the method's
idempotency rather than on the error. The stub raises at the top of a handler
for one and at the bottom for the other, and `Recorder.work_done` reads the
difference from the side that can see it.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
from connectrpc.code import Code
from connectrpc.method import IdempotencyLevel, MethodInfo

from ironflow import AsyncIronflowRPC, IronflowRPC, IronflowRPCError
from ironflow.rpc._runtime import _retry_waits

from .rpc_server import serve

API_KEY = "ifkey_test_abc123"

# `get_source` is annotated NO_SIDE_EFFECTS; `create_source` is not. They are
# handled by the same stub service, so the ONLY difference between the two
# scenarios below is the annotation PR 1 of #1809 put on the proto.
READ = "get_source"
WRITE = "create_source"


def _request(method: str) -> Any:
    from ironflow.rpc.v1 import CreateWebhookSourceRequest, GetWebhookSourceRequest

    if method == READ:
        return GetWebhookSourceRequest(id="whs_1")
    return CreateWebhookSourceRequest(name="w")


def call(client: Any, method: str) -> Any:
    """Invoke a webhooks method on either client, awaiting if needed."""
    result = getattr(client.webhooks, method)(_request(method))
    if asyncio.iscoroutine(result):
        return asyncio.get_event_loop().run_until_complete(result)
    return result


def close(client: Any) -> None:
    if isinstance(client, AsyncIronflowRPC):
        asyncio.get_event_loop().run_until_complete(client.aclose())
    else:
        client.close()


@pytest.fixture(params=[IronflowRPC, AsyncIronflowRPC], ids=["sync", "async"])
def client_cls(request: pytest.FixtureRequest) -> Any:
    return request.param


@pytest.fixture(autouse=True)
def loop() -> Any:
    """A fresh event loop per test. See test_rpc_client.py for why autouse."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


def _fake_ctx(level: IdempotencyLevel, timeout_ms: float | None = None) -> Any:
    """A RequestContext carrying just what `_retry_waits` reads.

    Built by hand rather than through the protocol: `create_request_context`
    would need a codec, a URL and a compression negotiation to hand back an
    object whose only two relevant fields are the method's idempotency level
    and the remaining deadline.
    """
    from connectrpc.request import Headers, RequestContext

    return RequestContext(
        method=MethodInfo(
            name="M",
            service_name="s.S",
            input=object,
            output=object,
            idempotency_level=level,
        ),
        http_method="POST",
        request_headers=Headers(),
        timeout_ms=timeout_ms,
    )


def _never_closed() -> bool:
    return False


# ── connection loss, before the server acted ─────────────────────────────────


def test_a_read_lost_before_the_server_acted_is_retried_and_succeeds(
    client_cls: Any,
) -> None:
    """The benign case: the request never landed, so the retry is the only
    execution and no duplicate is possible."""
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            srv.service.raise_code = Code.UNAVAILABLE
            result = call(client, READ)
        finally:
            close(client)

    assert result.id == "whs_1"
    assert len(srv.service.seen_headers) == 2, "the retry never reached the wire"
    assert srv.service.work_done == 1, "the first attempt should not have acted"
    # The interceptor applies auth once, before the loop, and the retry rebuilds
    # its HTTP headers from the same context. Asserted rather than reasoned
    # about: an interceptor that consumed the header would still pass every
    # count above, and the retry would arrive unauthenticated.
    assert [h.get("authorization") for h in srv.service.seen_headers] == [
        f"Bearer {API_KEY}",
        f"Bearer {API_KEY}",
    ]


# ── connection loss, after the server acted ──────────────────────────────────


def test_a_read_lost_after_the_server_acted_is_retried_and_re_executes(
    client_cls: Any,
) -> None:
    """The case the whole classification exists for.

    The server completed its work and the response was lost on the way back.
    The client sees the identical `unavailable` as the test above and retries,
    so the handler runs TWICE. That is harmless here only because the method is
    annotated NO_SIDE_EFFECTS. Run the same scenario against a write and the
    second execution would be a duplicate — which is why the next test asserts
    a write is never retried at all.
    """
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            srv.service.raise_after_work = Code.UNAVAILABLE
            result = call(client, READ)
        finally:
            close(client)

    assert result.id == "whs_1"
    assert len(srv.service.seen_headers) == 2
    assert srv.service.work_done == 2, (
        "the retry should have re-executed the handler; if this is 1 the "
        "second attempt never ran and the first assertion is passing for the "
        "wrong reason"
    )


# ── the failure persists across the backoff ──────────────────────────────────


def test_a_read_that_keeps_failing_stops_at_the_attempt_budget(
    client_cls: Any,
) -> None:
    """Loss that outlives every backoff window: the budget, not the server,
    ends it, and the error the caller sees is the last real one."""
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            srv.service.raise_code = Code.UNAVAILABLE
            srv.service.raise_times = 99
            with pytest.raises(IronflowRPCError) as excinfo:
                call(client, READ)
        finally:
            close(client)

    assert excinfo.value.code == "unavailable"
    assert excinfo.value.retryable is False, (
        "retryable means 'nothing left for this client to do', and the budget "
        "is spent"
    )
    assert len(srv.service.seen_headers) == 3, "max_attempts counts attempts, not retries"


def test_max_attempts_bounds_the_wire_calls(client_cls: Any) -> None:
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY, max_attempts=5)
        try:
            srv.service.raise_code = Code.UNAVAILABLE
            srv.service.raise_times = 99
            with pytest.raises(IronflowRPCError):
                call(client, READ)
        finally:
            close(client)

    assert len(srv.service.seen_headers) == 5


def test_max_attempts_of_one_disables_retry(client_cls: Any) -> None:
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY, max_attempts=1)
        try:
            srv.service.raise_code = Code.UNAVAILABLE
            with pytest.raises(IronflowRPCError):
                call(client, READ)
        finally:
            close(client)

    assert len(srv.service.seen_headers) == 1


# ── what must NOT be retried ─────────────────────────────────────────────────


def test_a_write_is_never_retried(client_cls: Any) -> None:
    """The test that proves the gate is wired to the right side.

    Same injected error, same service, same client — only the proto annotation
    differs. If this ever reads 2, an unannotated mutation is being duplicated
    on every transport blip.
    """
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            srv.service.raise_code = Code.UNAVAILABLE
            with pytest.raises(IronflowRPCError) as excinfo:
                call(client, WRITE)
        finally:
            close(client)

    assert excinfo.value.code == "unavailable"
    assert len(srv.service.seen_headers) == 1


@pytest.mark.parametrize(
    "code",
    [Code.NOT_FOUND, Code.PERMISSION_DENIED, Code.RESOURCE_EXHAUSTED, Code.INTERNAL],
)
def test_only_unavailable_is_retried(client_cls: Any, code: Code) -> None:
    """Every other code is a decision the server reaches again identically."""
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            srv.service.raise_code = code
            with pytest.raises(IronflowRPCError):
                call(client, READ)
        finally:
            close(client)

    assert len(srv.service.seen_headers) == 1


# ── the policy itself ────────────────────────────────────────────────────────


def test_only_no_side_effects_yields_any_wait() -> None:
    for level in IdempotencyLevel:
        waits = list(_retry_waits(_fake_ctx(level), _never_closed, 3))
        expected = 2 if level is IdempotencyLevel.NO_SIDE_EFFECTS else 0
        assert len(waits) == expected, f"{level} yielded {waits}"


def test_idempotent_is_deliberately_not_retried() -> None:
    """Not an oversight, and not free to change.

    `scripts/check-proto-idempotency.sh` only refuses NO_SIDE_EFFECTS on a
    mutating verb. Accepting IDEMPOTENT here would let
    `option idempotency_level = IDEMPOTENT` on a delete pass that gate and be
    auto-retried anyway. Widen both or neither.
    """
    ctx = _fake_ctx(IdempotencyLevel.IDEMPOTENT)
    assert list(_retry_waits(ctx, _never_closed, 3)) == []


def test_the_backoff_grows_and_is_capped() -> None:
    waits = list(_retry_waits(_fake_ctx(IdempotencyLevel.NO_SIDE_EFFECTS), _never_closed, 12))
    assert waits[:4] == [0.1, 0.2, 0.4, 0.8]
    assert waits == sorted(waits)
    assert max(waits) <= 10.0


def test_a_closed_client_stops_the_retries() -> None:
    """Reached in production by calling close() during a backoff. Driven
    directly here because winning that race is not a test, it is a coin flip."""
    ctx = _fake_ctx(IdempotencyLevel.NO_SIDE_EFFECTS)
    assert list(_retry_waits(ctx, lambda: True, 3)) == []


def test_a_retry_is_refused_when_the_deadline_would_pass_first() -> None:
    """`timeout=` bounds the WHOLE call, so a backoff that would outlast the
    remaining budget ends it instead. Without this the sleep runs, the deadline
    goes negative, and pyqwest's complaint comes back as `unavailable` — which
    this same policy would then read as retryable."""
    # 50ms left, first backoff is 100ms.
    ctx = _fake_ctx(IdempotencyLevel.NO_SIDE_EFFECTS, timeout_ms=50)
    assert list(_retry_waits(ctx, _never_closed, 3)) == []

    # 10s left is plenty for the first two.
    ctx = _fake_ctx(IdempotencyLevel.NO_SIDE_EFFECTS, timeout_ms=10_000)
    assert list(_retry_waits(ctx, _never_closed, 3)) == [0.1, 0.2]


def test_the_waits_are_computed_at_failure_time_not_up_front() -> None:
    """The generator is advanced once per failure, so a deadline that expires
    between attempts stops the next one. A precomputed list could not."""
    ctx = _fake_ctx(IdempotencyLevel.NO_SIDE_EFFECTS, timeout_ms=1_000)
    waits = _retry_waits(ctx, _never_closed, 8)
    assert next(waits) == 0.1
    # Burn the budget the way a slow attempt would.
    time.sleep(1.05)
    assert next(waits, None) is None
