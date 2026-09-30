"""Client-level on_error hook on the ConnectRPC clients (#2412).

Every test is parameterized over the sync and async clients: they are separate
interceptor methods, and a suite that exercised one would leave the other unproven.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any

import pytest
from connectrpc.code import Code

from ironflow import AsyncIronflowRPC, IronflowRPC, IronflowRPCError
from ironflow._http import ErrorContext
from ironflow.rpc._runtime import _ClosedError
from ironflow.rpc.v1 import GetWebhookSourceRequest, SubscribeRequest

from .rpc_server import serve
from .test_rpc_stream_resume import drain as drain_from
from .test_rpc_stream_resume import positioned
from .test_rpc_streams import close, drain, unary

API_KEY = "ifkey_test_abc123"


@pytest.fixture(params=[IronflowRPC, AsyncIronflowRPC], ids=["sync", "async"])
def client_cls(request: pytest.FixtureRequest) -> Any:
    return request.param


@pytest.fixture(autouse=True)
def loop() -> Any:
    lp = asyncio.new_event_loop()
    asyncio.set_event_loop(lp)
    yield lp
    lp.close()


def read(client: Any) -> Any:
    """A NO_SIDE_EFFECTS unary call, the only kind the client retries."""
    result = client.webhooks.get_source(GetWebhookSourceRequest(id="whs_1"))
    if asyncio.iscoroutine(result):
        return asyncio.get_event_loop().run_until_complete(result)
    return result


def make(client_cls: Any, srv: Any, hook: Any) -> Any:
    return client_cls(server_url=srv.url, api_key=API_KEY, on_error=hook)


def test_a_failed_unary_call_fires_once_with_its_context(client_cls: Any) -> None:
    seen: list[tuple[Exception, ErrorContext]] = []
    with serve() as srv:
        srv.rec.raise_code = Code.NOT_FOUND
        client = make(client_cls, srv, lambda err, ctx: seen.append((err, ctx)))
        try:
            with pytest.raises(IronflowRPCError):
                unary(client)
        finally:
            close(client)
    assert len(seen) == 1
    err, ctx = seen[0]
    assert isinstance(err, IronflowRPCError) and err.code == "not_found"
    assert ctx.method and ctx.endpoint == f"/ironflow.v1.WebhookService/{ctx.method}" and ctx.status_code is None


def test_a_retry_that_succeeds_does_not_fire(client_cls: Any) -> None:
    seen: list[Any] = []
    with serve() as srv:
        srv.rec.raise_code = Code.UNAVAILABLE
        client = make(client_cls, srv, lambda err, ctx: seen.append(err))
        try:
            assert read(client).id == "whs_1"
        finally:
            close(client)
    assert seen == []


def test_exhausted_retries_fire_once(client_cls: Any) -> None:
    seen: list[Any] = []
    with serve() as srv:
        srv.rec.raise_code = Code.UNAVAILABLE
        srv.rec.raise_times = 99
        client = make(client_cls, srv, lambda err, ctx: seen.append(err))
        try:
            with pytest.raises(IronflowRPCError):
                read(client)
        finally:
            close(client)
        assert len(srv.rec.seen_headers) == 3  # it did retry
    assert len(seen) == 1


def test_a_mid_stream_failure_fires_once(client_cls: Any) -> None:
    seen: list[ErrorContext] = []
    with serve() as srv:
        srv.rec.stream_events = 5
        srv.rec.fail_after = 2
        srv.rec.stream_code = Code.RESOURCE_EXHAUSTED
        client = make(client_cls, srv, lambda err, ctx: seen.append(ctx))
        try:
            with pytest.raises(IronflowRPCError):
                drain(client)
        finally:
            close(client)
    assert len(seen) == 1 and seen[0].endpoint.endswith("/Subscribe")


def test_a_stream_creation_failure_fires_once(client_cls: Any) -> None:
    seen: list[ErrorContext] = []
    with serve() as srv:
        srv.rec.raise_code = Code.PERMISSION_DENIED
        client = make(client_cls, srv, lambda err, ctx: seen.append(ctx))
        try:
            with pytest.raises(IronflowRPCError):
                drain(client)
        finally:
            close(client)
    assert len(seen) == 1


def test_use_after_close_is_not_reported(client_cls: Any) -> None:
    seen: list[Any] = []
    with serve() as srv:
        client = make(client_cls, srv, lambda err, ctx: seen.append(err))
        stream = client.pubsub.subscribe(SubscribeRequest())
        close(client)
        with pytest.raises(IronflowRPCError) as exc:
            if isinstance(client, AsyncIronflowRPC):
                async def consume() -> None:
                    async for _ in stream:
                        pass

                asyncio.get_event_loop().run_until_complete(consume())
            else:
                for _ in stream:
                    pass
    # Without this, a server-side failure would also leave seen empty and the test would pass.
    assert isinstance(exc.value, _ClosedError)
    assert seen == []


def test_a_raising_hook_is_logged_and_never_masks_the_error(client_cls: Any, caplog: pytest.LogCaptureFixture) -> None:
    def hook(err: Exception, ctx: ErrorContext) -> None:
        raise RuntimeError("hook boom")

    with serve() as srv:
        srv.rec.raise_code = Code.NOT_FOUND
        client = make(client_cls, srv, hook)
        try:
            with caplog.at_level(logging.ERROR, logger="ironflow"), pytest.raises(IronflowRPCError) as e:
                unary(client)
        finally:
            close(client)
    assert e.value.code == "not_found" and "on_error hook raised" in caplog.text


def test_an_async_hook_is_awaited_on_the_async_client(client_cls: Any) -> None:
    if client_cls is not AsyncIronflowRPC:
        pytest.skip("async client only")
    seen: list[Any] = []

    async def hook(err: Exception, ctx: ErrorContext) -> None:
        await asyncio.sleep(0)
        seen.append(err)

    with serve() as srv:
        srv.rec.raise_code = Code.NOT_FOUND
        client = make(client_cls, srv, hook)
        try:
            with pytest.raises(IronflowRPCError):
                unary(client)
        finally:
            close(client)
    assert len(seen) == 1


def test_an_async_hook_on_the_sync_client_is_not_awaited_and_is_reported(
    client_cls: Any, caplog: pytest.LogCaptureFixture,
) -> None:
    if client_cls is not IronflowRPC:
        pytest.skip("sync client only")
    made: list[Any] = []

    async def body() -> None:
        raise AssertionError("must never run")

    def hook(err: Exception, ctx: ErrorContext) -> Any:
        made.append(body())
        return made[-1]

    with serve() as srv:
        srv.rec.raise_code = Code.NOT_FOUND
        client = make(client_cls, srv, hook)
        try:
            with caplog.at_level(logging.ERROR, logger="ironflow"), pytest.raises(IronflowRPCError):
                unary(client)
        finally:
            close(client)
    assert inspect.getcoroutinestate(made[0]) == inspect.CORO_CLOSED
    assert "synchronous" in caplog.text


def test_a_resumable_subscribe_fires_once_after_the_resume_budget_is_spent(client_cls: Any) -> None:
    seen: list[ErrorContext] = []
    with serve() as srv:
        srv.rec.fail_after = 0
        srv.rec.stream_code = Code.UNAVAILABLE
        client = client_cls(server_url=srv.url, api_key=API_KEY, max_attempts=3, on_error=lambda err, ctx: seen.append(ctx))
        try:
            with pytest.raises(IronflowRPCError):
                drain_from(client, positioned(0), limit=5)
        finally:
            close(client)
        assert len(srv.rec.seen_cursors) == 3  # it did reconnect
    assert len(seen) == 1 and seen[0].endpoint.endswith("/Subscribe")


def test_an_async_hook_runs_on_a_failed_stream_on_the_async_client(client_cls: Any) -> None:
    if client_cls is not AsyncIronflowRPC:
        pytest.skip("async client only")
    seen: list[Any] = []

    async def hook(err: Exception, ctx: ErrorContext) -> None:
        await asyncio.sleep(0)
        seen.append(ctx.endpoint)

    with serve() as srv:
        srv.rec.stream_events = 5
        srv.rec.fail_after = 2
        srv.rec.stream_code = Code.RESOURCE_EXHAUSTED
        client = make(client_cls, srv, hook)
        try:
            with pytest.raises(IronflowRPCError):
                drain(client)
        finally:
            close(client)
    assert len(seen) == 1 and seen[0].endswith("/Subscribe")
