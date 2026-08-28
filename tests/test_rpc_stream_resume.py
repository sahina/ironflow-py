"""Automatic reconnection for a positioned subscription (#1848).

`Subscribe` is the only exposed stream a client can reconnect, and it can only
do so when the caller named a position. The other three are durable and
server-positioned: re-issuing the call resumes them, and a client cursor would
move a position other readers share.

The assertions here are on `Recorder.seen_cursors` — what each reconnect asked
the server for — not on how many events arrived. A reconnect that resumes from
the wrong place still delivers events, and delivers them in the right order and
count; the cursor is the only thing that distinguishes a resume from a fresh
subscription that happens to look right.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError

from ironflow import AsyncIronflowRPC, IronflowRPC, IronflowRPCError
from ironflow.rpc._runtime import _resumable
from ironflow.rpc.v1 import (
    StreamProjectionEventsRequest,
    SubscribeOptions,
    SubscribeRequest,
)

from .rpc_server import serve

API_KEY = "ifkey_test_abc123"


@pytest.fixture(params=[IronflowRPC, AsyncIronflowRPC], ids=["sync", "async"])
def client_cls(request: pytest.FixtureRequest) -> Any:
    return request.param


@pytest.fixture(autouse=True)
def loop() -> Any:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


def drain(client: Any, request: Any, limit: int) -> list[int]:
    """Collect up to `limit` sequences from either client."""
    stream = client.pubsub.subscribe(request)
    if isinstance(client, AsyncIronflowRPC):

        async def run() -> list[int]:
            out: list[int] = []
            async for event in stream:
                out.append(event.sequence)
                if len(out) >= limit:
                    break
            return out

        return asyncio.get_event_loop().run_until_complete(run())

    out: list[int] = []
    for event in stream:
        out.append(event.sequence)
        if len(out) >= limit:
            break
    return out


def close(client: Any) -> None:
    if isinstance(client, AsyncIronflowRPC):
        asyncio.get_event_loop().run_until_complete(client.aclose())
    else:
        client.close()


def positioned(after: int) -> SubscribeRequest:
    return SubscribeRequest(
        pattern="orders.*",
        options=SubscribeOptions(start_after_sequence=after),
    )


# ── the gate ─────────────────────────────────────────────────────────────────


def test_only_a_positioned_subscribe_is_resumable() -> None:
    """The gate does two jobs: opt-in, and scoping to Subscribe.

    The three unpositioned forms must all read False, and so must the other
    exposed streams — whose requests carry no `options` at all, which is why
    the gate needs no list of method names to keep in step.
    """
    assert _resumable(positioned(7)) is True
    # Explicit 0 is a real position — "from the very beginning" — not unset.
    assert _resumable(positioned(0)) is True

    assert _resumable(SubscribeRequest(pattern="p")) is False
    assert _resumable(SubscribeRequest(pattern="p", options=SubscribeOptions())) is False
    assert _resumable(SubscribeRequest(pattern="p", options=SubscribeOptions(replay=5))) is False
    assert _resumable(StreamProjectionEventsRequest(name="p")) is False


# ── reconnection ─────────────────────────────────────────────────────────────


def test_a_positioned_stream_resumes_from_the_last_event_delivered(
    client_cls: Any,
) -> None:
    """The whole contract, in one assertion on what the server was asked for.

    The stub yields e0..e2 (sequences 1-3) then fails. The reconnect must ask
    for everything after 3 — the last event actually delivered — not repeat the
    caller's original 0, which would replay, and not skip ahead.
    """
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            srv.service.stream_events = 5  # must exceed fail_after, or the
            srv.service.fail_after = 3      # stream just ends cleanly
            srv.service.fail_after_times = 1
            srv.service.stream_code = Code.UNAVAILABLE
            seqs = drain(client, positioned(0), limit=5)
        finally:
            close(client)

    assert srv.service.seen_cursors == [0, 3], (
        "the reconnect asked the server for the wrong position; delivering the "
        "right number of events is not the same as resuming"
    )
    assert seqs == [1, 2, 3, 4, 5], f"gap or repeat across the reconnect: {seqs}"


def test_an_unpositioned_stream_is_not_reconnected(client_cls: Any) -> None:
    """The test that proves the gate is wired to the right side.

    Same injected failure, same stream, same client — only the cursor differs.
    Without a position there is nowhere honest to resume from, so the failure
    reaches the caller.
    """
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            srv.service.fail_after = 2
            srv.service.fail_after_times = 1
            srv.service.stream_code = Code.UNAVAILABLE
            with pytest.raises(IronflowRPCError) as excinfo:
                drain(client, SubscribeRequest(pattern="orders.*"), limit=5)
        finally:
            close(client)

    assert excinfo.value.code == "unavailable"
    assert len(srv.service.seen_cursors) == 1, "an unpositioned stream reconnected"


@pytest.mark.parametrize(
    "code", [Code.NOT_FOUND, Code.PERMISSION_DENIED, Code.INTERNAL]
)
def test_only_unavailable_reconnects(client_cls: Any, code: Code) -> None:
    """Every other code is a decision the server reaches again identically."""
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            srv.service.fail_after = 2
            srv.service.fail_after_times = 1
            srv.service.stream_code = code
            with pytest.raises(IronflowRPCError):
                drain(client, positioned(0), limit=5)
        finally:
            close(client)

    assert len(srv.service.seen_cursors) == 1


def test_a_clean_end_of_stream_is_not_a_reconnect(client_cls: Any) -> None:
    """The server deciding it is done must end the iteration, not restart it.

    Without this the loop is infinite: every finished stream looks like a
    reason to open another one.
    """
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            srv.service.stream_events = 2
            seqs = drain(client, positioned(0), limit=99)
        finally:
            close(client)

    assert seqs == [1, 2]
    assert len(srv.service.seen_cursors) == 1


def test_reconnects_stop_at_the_attempt_budget(client_cls: Any) -> None:
    """A failure that never yields an event cannot refill the budget, so the
    reconnects are bounded and the last real error reaches the caller."""
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY, max_attempts=3)
        try:
            # Fail before yielding anything, on every stream.
            srv.service.fail_after = 0
            srv.service.stream_code = Code.UNAVAILABLE
            with pytest.raises(IronflowRPCError) as excinfo:
                drain(client, positioned(0), limit=5)
        finally:
            close(client)

    assert excinfo.value.code == "unavailable"
    assert len(srv.service.seen_cursors) == 3, (
        "max_attempts counts attempts, not reconnects"
    )


def test_progress_refills_the_budget(client_cls: Any) -> None:
    """A long-lived subscription must not die of old age.

    max_attempts=2 allows one reconnect. Three consecutive failures that each
    deliver events still succeed, because each delivery refills the budget —
    without which a stream that reconnected normally over days would fail on
    its second reconnect having worked perfectly throughout.
    """
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY, max_attempts=2)
        try:
            srv.service.stream_events = 5
            srv.service.fail_after = 2
            srv.service.fail_after_times = 3
            srv.service.stream_code = Code.UNAVAILABLE
            seqs = drain(client, positioned(0), limit=7)
        finally:
            close(client)

    assert seqs == [1, 2, 3, 4, 5, 6, 7]
    assert srv.service.seen_cursors == [0, 2, 4, 6]


def test_the_callers_request_is_not_mutated(client_cls: Any) -> None:
    """A stream is lazy and reconnects long after subscribe() returned, so
    advancing the cursor in place would edit an object the caller still holds,
    at a moment they have no reason to expect it."""
    request = positioned(0)
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            srv.service.stream_events = 5
            srv.service.fail_after = 3
            srv.service.fail_after_times = 1
            srv.service.stream_code = Code.UNAVAILABLE
            drain(client, request, limit=5)
        finally:
            close(client)

    assert request.options.start_after_sequence == 0


# ── lifetime ─────────────────────────────────────────────────────────────────


def test_every_dropped_attempt_closes_its_upstream(client_cls: Any) -> None:
    """A reconnecting stream opens N transport iterators; all N must be released.

    tests/test_rpc_streams.py already asserts this for the non-resuming
    wrappers, and its docstring records why: `yield from` propagates `.close()`
    on the sync side while `async for` does NOT, so the async twin needs an
    explicit `finally`. Reconnection multiplies the stakes — a leak that used to
    cost one held HTTP response now costs one per reconnect, for the life of a
    long-running subscriber.

    `_resuming_iter` uses a plain `for` rather than `yield from`, because it has
    to inspect each event to advance the cursor. So it does not get the sync
    close for free the way `_translating_iter` does, and this is the assertion
    that says whether it needs it.
    """
    from ironflow.rpc._runtime import _resuming_aiter, _resuming_iter

    class Ctx:
        timeout_ms = None

    class Event:
        def __init__(self, sequence: int) -> None:
            self.sequence = sequence

    request = positioned(0)
    opened: list[bool] = []
    closed: list[bool] = []

    def make_sync(_req: Any, _ctx: Any) -> Any:
        first = len(opened) == 0
        opened.append(True)

        def gen() -> Any:
            try:
                yield Event(1)
                if first:
                    raise ConnectError(Code.UNAVAILABLE, "drop")
                yield Event(2)
            finally:
                closed.append(True)

        return gen()

    def make_async(_req: Any, _ctx: Any) -> Any:
        first = len(opened) == 0
        opened.append(True)

        async def gen() -> Any:
            try:
                yield Event(1)
                if first:
                    raise ConnectError(Code.UNAVAILABLE, "drop")
                yield Event(2)
            finally:
                closed.append(True)

        return gen()

    if client_cls is AsyncIronflowRPC:

        async def drive() -> None:
            stream = _resuming_aiter(make_async, request, Ctx(), lambda: False, "x", 3)
            seen = 0
            async for _ in stream:
                seen += 1
                if seen >= 2:
                    break
            await stream.aclose()

        asyncio.get_event_loop().run_until_complete(drive())
    else:
        stream = _resuming_iter(make_sync, request, Ctx(), lambda: False, "x", 3)
        for seen, _ in enumerate(stream, start=1):
            if seen >= 2:
                break
        stream.close()

    assert len(opened) == 2, f"expected one reconnect, got {len(opened)} attempts"
    assert len(closed) == len(opened), (
        f"opened {len(opened)} transport iterators but released {len(closed)}; "
        "an abandoned attempt holds its HTTP response, connection and "
        "server-side subscription until garbage collection — once per reconnect"
    )
