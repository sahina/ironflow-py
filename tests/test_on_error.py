"""Client-level on_error hook on the REST client (#2412)."""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any

import pytest

from ironflow import IronflowClient, IronflowError
from ironflow._http import ErrorContext, anotify_error
from tests.harness import Response


def client(server: Any, hook: Any, **kwargs: Any) -> IronflowClient:
    kwargs.setdefault("initial_backoff", 0.01)
    kwargs.setdefault("max_backoff", 0.05)
    return IronflowClient(server_url=server.url, on_error=hook, **kwargs)


def test_fires_once_after_retries_are_exhausted(server: Any) -> None:
    server.script(*[Response(status=503) for _ in range(3)])
    seen: list[tuple[Exception, ErrorContext]] = []
    with pytest.raises(IronflowError):
        client(server, lambda err, ctx: seen.append((err, ctx))).events_list()
    assert len(server.requests) == 3 and len(seen) == 1
    err, ctx = seen[0]
    assert isinstance(err, IronflowError)
    assert ctx == ErrorContext("GET", server.requests[0]["path"].partition("?")[0], 503)


def test_does_not_fire_when_a_retry_succeeds(server: Any) -> None:
    server.script(Response(status=503), Response(status=200, body={"ok": True}))
    seen: list[Any] = []
    assert client(server, lambda err, ctx: seen.append(err)).events_list() == {"ok": True}
    assert seen == []


def test_a_non_retryable_status_fires_immediately_with_its_code(server: Any) -> None:
    server.script(Response(status=404))
    seen: list[ErrorContext] = []
    with pytest.raises(IronflowError):
        client(server, lambda err, ctx: seen.append(ctx)).events_list()
    assert len(server.requests) == 1 and [c.status_code for c in seen] == [404]


def test_a_connection_failure_has_no_status_code() -> None:
    seen: list[ErrorContext] = []
    c = IronflowClient(server_url="http://127.0.0.1:1", max_attempts=1, on_error=lambda err, ctx: seen.append(ctx))
    with pytest.raises(IronflowError):
        c.events_list()
    assert [x.status_code for x in seen] == [None]


def test_the_endpoint_never_carries_the_query_string(server: Any) -> None:
    server.script(Response(status=500))
    seen: list[ErrorContext] = []
    c = client(server, lambda err, ctx: seen.append(ctx), max_attempts=1)
    with pytest.raises(IronflowError):
        c.request("GET", "/api/v1/events", params={"token": "s3cret"})
    assert "?" not in seen[0].endpoint and "s3cret" not in seen[0].endpoint


def test_a_raising_hook_is_logged_and_never_masks_the_error(server: Any, caplog: pytest.LogCaptureFixture) -> None:
    server.script(Response(status=404))

    def hook(err: Exception, ctx: ErrorContext) -> None:
        raise RuntimeError("hook boom")

    with caplog.at_level(logging.ERROR, logger="ironflow"), pytest.raises(IronflowError) as e:
        client(server, hook).events_list()
    assert e.value.status_code == 404
    assert "on_error hook raised" in caplog.text


def test_an_async_hook_on_the_sync_client_is_not_awaited_and_is_reported(
    server: Any, caplog: pytest.LogCaptureFixture,
) -> None:
    server.script(Response(status=404))
    made: list[Any] = []

    async def body() -> None:
        raise AssertionError("must never run")

    def hook(err: Exception, ctx: ErrorContext) -> Any:
        made.append(body())  # what calling an `async def` hook does; kept so the test can inspect it
        return made[-1]

    with caplog.at_level(logging.ERROR, logger="ironflow"), pytest.raises(IronflowError):
        client(server, hook).events_list()
    # The "never awaited" warning comes from the coroutine's finalizer, which no test can assert on.
    # A closed coroutine cannot warn, so closing is the observable behavior.
    assert inspect.getcoroutinestate(made[0]) == inspect.CORO_CLOSED
    assert "synchronous" in caplog.text


def test_the_method_is_reported_upper_case(server: Any) -> None:
    server.script(Response(status=404))
    seen: list[ErrorContext] = []
    with pytest.raises(IronflowError):
        client(server, lambda err, ctx: seen.append(ctx)).request("get", "/api/v1/events")
    assert seen[0].method == "GET"


def test_anotify_error_logs_and_swallows_a_raising_async_hook(caplog: pytest.LogCaptureFixture) -> None:
    async def hook(err: Exception, ctx: ErrorContext) -> None:
        raise RuntimeError("hook boom")

    with caplog.at_level(logging.ERROR, logger="ironflow"):
        asyncio.run(anotify_error(hook, IronflowError("x"), ErrorContext("GET", "/p")))
    assert "on_error hook raised" in caplog.text


def test_anotify_error_accepts_a_sync_hook() -> None:
    seen: list[Exception] = []
    asyncio.run(anotify_error(lambda err, ctx: seen.append(err), IronflowError("x"), ErrorContext("GET", "/p")))
    assert len(seen) == 1
