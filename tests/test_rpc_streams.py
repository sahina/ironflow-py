"""Server-stream behaviour for both clients (#1781).

All four exposed streams are server-streaming, so `PubSubService/Subscribe`
stands in for the shape. What differs between them is the message type, which
the surface test already checks against the ledger.

Everything here runs against the in-process Connect server rather than the real
binary, on purpose: a stub decides exactly when to fail, how many events to
yield, and when to stop. Against a live server the same assertions would depend
on a handler happening to be slow or happening to fail, which is where flaky
suites come from. The real-server suite covers what only it can — see
tests/integration/.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from connectrpc.code import Code

from ironflow import AsyncIronflowRPC, IronflowRPC, IronflowRPCError
from ironflow.rpc._runtime import NO_TIMEOUT
from ironflow.rpc.v1 import CreateWebhookSourceRequest, SubscribeRequest

from .rpc_server import serve

API_KEY = "ifkey_test_abc123"


def drain(client: Any, limit: int | None = None, **kwargs: Any) -> list[str]:
    """Consume rpc.pubsub.subscribe on either client, stopping after `limit`.

    A single helper so a sync and an async test assert on the same list rather
    than on two near-identical bodies that can drift.
    """
    stream = client.pubsub.subscribe(SubscribeRequest(), **kwargs)
    if isinstance(client, AsyncIronflowRPC):

        async def consume() -> list[str]:
            out: list[str] = []
            async for ev in stream:
                out.append(ev.event_id)
                if limit is not None and len(out) >= limit:
                    break
            return out

        return asyncio.get_event_loop().run_until_complete(consume())

    out: list[str] = []
    for ev in stream:
        out.append(ev.event_id)
        if limit is not None and len(out) >= limit:
            break
    return out


def close(client: Any) -> None:
    if isinstance(client, AsyncIronflowRPC):
        asyncio.get_event_loop().run_until_complete(client.aclose())
    else:
        client.close()


def unary(client: Any) -> Any:
    result = client.webhooks.create_source(
        CreateWebhookSourceRequest(name="x", event_prefix="x.")
    )
    if asyncio.iscoroutine(result):
        return asyncio.get_event_loop().run_until_complete(result)
    return result


@pytest.fixture(params=[IronflowRPC, AsyncIronflowRPC], ids=["sync", "async"])
def client_cls(request: pytest.FixtureRequest) -> Any:
    return request.param


@pytest.fixture(autouse=True)
def loop() -> Any:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


# ── iteration ────────────────────────────────────────────────────────────────


def test_stream_yields_every_event(client_cls: Any) -> None:
    with serve() as srv:
        srv.rec.stream_events = 3
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            assert drain(client) == ["e0", "e1", "e2"]
        finally:
            close(client)


def test_stream_carries_the_bearer_token(client_cls: Any) -> None:
    """Auth is applied by the interceptor's stream path, not only its unary one.

    Separate interceptor protocol methods handle the two, so unary coverage says
    nothing about this.
    """
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            drain(client)
        finally:
            close(client)
        assert srv.rec.seen_headers[0]["authorization"] == f"Bearer {API_KEY}"


def test_empty_stream_is_not_an_error(client_cls: Any) -> None:
    with serve() as srv:
        srv.rec.stream_events = 0
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            assert drain(client) == []
        finally:
            close(client)


# ── error translation ────────────────────────────────────────────────────────


def test_mid_stream_failure_translates_after_delivering_events(client_cls: Any) -> None:
    """The assertion PR 2 could not make.

    ADR 0062 recorded that mid-iteration translation was read off an interceptor
    signature rather than proven. This is the proof: two events arrive, THEN the
    failure surfaces as the Ironflow type with the right code.

    A failure here means `_translating_iter` stopped wrapping the iterator, and
    callers would be seeing raw connectrpc exceptions from inside a `for` loop.
    """
    with serve() as srv:
        srv.rec.stream_events = 5
        srv.rec.fail_after = 2
        srv.rec.stream_code = Code.RESOURCE_EXHAUSTED
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            with pytest.raises(IronflowRPCError) as exc:
                drain(client)
        finally:
            close(client)

    assert exc.value.code == "resource_exhausted"
    # The server's own count, not the client's: `drain` raises before it can
    # return a list, so the only evidence that events flowed BEFORE the failure
    # — rather than the stream dying at creation — is what the stub produced.
    assert srv.rec.yielded == 2, "the server should have produced two events before failing"


def test_failure_before_the_first_event_translates(client_cls: Any) -> None:
    """Stream CREATION failure, which takes the other branch of the interceptor."""
    with serve() as srv:
        srv.rec.raise_code = Code.PERMISSION_DENIED
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            with pytest.raises(IronflowRPCError) as exc:
                drain(client)
        finally:
            close(client)

    assert exc.value.code == "permission_denied"


# ── deadlines ────────────────────────────────────────────────────────────────


def test_client_default_timeout_applies_to_streams(client_cls: Any) -> None:
    """Deliberate, and documented on every stream method.

    A deadline bounds the whole subscription. Exempting streams would make one
    parameter mean two things depending on which method you called.
    """
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY, timeout=9.0)
        try:
            drain(client)
        finally:
            close(client)

    seen = srv.rec.seen_timeouts[0]
    assert seen is not None
    assert 8000 <= seen <= 9000


def test_no_timeout_clears_a_client_deadline_on_a_stream(client_cls: Any) -> None:
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY, timeout=9.0)
        try:
            drain(client, timeout=NO_TIMEOUT)
        finally:
            close(client)

    assert srv.rec.seen_timeouts[0] is None


# ── cancellation ─────────────────────────────────────────────────────────────


def test_breaking_out_early_raises_nothing(client_cls: Any) -> None:
    with serve() as srv:
        srv.rec.stream_events = 3
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            assert drain(client, limit=1) == ["e0"]
        finally:
            close(client)


def test_client_still_works_after_an_abandoned_stream(client_cls: Any) -> None:
    """The property that would actually bite someone.

    `IronflowRPC` shares one pyqwest client across all eight namespaces. If
    abandoning a subscription left that transport in a bad state, every later
    call on the same client would fail — and the failure would surface far from
    the subscription that caused it.

    Not an assertion to weaken if it goes red.
    """
    with serve() as srv:
        srv.rec.stream_events = 5
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            assert drain(client, limit=2) == ["e0", "e1"]
            assert unary(client).id == "whs_stub"
            assert drain(client, limit=1) == ["e0"]
        finally:
            close(client)


def test_explicit_close_on_the_iterator_raises_nothing(client_cls: Any) -> None:
    """The documented cancellation shape, exercised as documented.

    No cancellation API was added: a sync generator closes with `.close()` and
    an async one with `aclose()`, and wrapping an iterator in something that is
    almost an iterator misbehaves under comprehensions and `async for`.
    """
    with serve() as srv:
        srv.rec.stream_events = 5
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            stream = client.pubsub.subscribe(SubscribeRequest())
            if isinstance(client, AsyncIronflowRPC):

                async def take_then_close() -> None:
                    async for _ in stream:
                        break
                    await stream.aclose()

                asyncio.get_event_loop().run_until_complete(take_then_close())
            else:
                for _ in stream:
                    break
                stream.close()
            assert unary(client).id == "whs_stub"
        finally:
            close(client)


# ── lifecycle ────────────────────────────────────────────────────────────────


def test_closing_the_wrapper_closes_the_stream_underneath(client_cls: Any) -> None:
    """The leak the "raises nothing" tests could not see.

    `test_explicit_close_on_the_iterator_raises_nothing` passes whether or not
    the transport iterator is released, because it only asserts that nothing
    blows up. This asserts the release itself.

    The two paths get there differently and that asymmetry is the whole bug:
    the sync wrapper's `yield from` propagates `.close()` for free, while
    `async for` does NOT, so the async wrapper needs an explicit `finally`.
    Before the fix this assertion was True for sync and False for async.
    """
    closed = {"v": False}

    def sync_upstream() -> Any:
        try:
            for i in range(100):
                yield f"e{i}"
        finally:
            closed["v"] = True

    async def async_upstream(req: Any, ctx: Any) -> Any:
        try:
            for i in range(100):
                yield f"e{i}"
        finally:
            closed["v"] = True

    if client_cls is AsyncIronflowRPC:
        from ironflow.rpc._runtime import _translating_aiter

        async def drive() -> None:
            stream = _translating_aiter(async_upstream, None, None, lambda: False, "x")
            async for _ in stream:
                break
            await stream.aclose()

        asyncio.get_event_loop().run_until_complete(drive())
    else:
        from ironflow.rpc._runtime import _translating_iter

        stream = _translating_iter(sync_upstream(), lambda: False, "x")
        for _ in stream:
            break
        stream.close()

    assert closed["v"], (
        "closing the translating wrapper did not close the transport iterator "
        "underneath it — an abandoned subscription holds its HTTP response, "
        "connection and server-side subscription until garbage collection"
    )


def test_a_stream_created_before_close_does_not_run_after_it(client_cls: Any) -> None:
    """Stream I/O is lazy, so `_check_open` at call time is not enough.

    Creating a stream sends nothing. Without a guard inside the iterator, this
    sequence reaches the network AFTER the client's lifetime has ended —
    measured as zero requests before the close and one after.

    The server's own request count is the assertion, not an exception type:
    the point is that no request is sent, and only the server can testify to
    that.
    """
    with serve() as srv:
        srv.rec.stream_events = 3
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        stream = client.pubsub.subscribe(SubscribeRequest())
        close(client)

        assert srv.rec.seen_headers == [], "creating a stream should send nothing"

        with pytest.raises(IronflowRPCError) as exc:
            if isinstance(client, AsyncIronflowRPC):

                async def take() -> None:
                    async for _ in stream:
                        break

                asyncio.get_event_loop().run_until_complete(take())
            else:
                next(iter(stream))

        assert "closed" in str(exc.value)
        assert srv.rec.seen_headers == [], (
            "a stream created before close() still reached the server after it"
        )


def test_stream_on_a_closed_client_fails_immediately(client_cls: Any) -> None:
    """Eagerly, at the call — not lazily on first iteration.

    This is why the generated stream method is a plain `def` returning the
    iterator rather than an `async def` generator. Under the generator form the
    call would succeed and the failure would surface inside the caller's loop,
    at a line that says nothing about closing.
    """
    client = client_cls(server_url="http://x", api_key=API_KEY)
    close(client)

    with pytest.raises(IronflowRPCError) as exc:
        client.pubsub.subscribe(SubscribeRequest())

    assert "closed" in str(exc.value)
    assert "pubsub.subscribe" in str(exc.value)
