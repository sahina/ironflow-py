from __future__ import annotations

import asyncio
import socket
import time

import pytest

from ironflow import IronflowError
from ironflow.worker._transport import Transport
from tests.worker.conftest import run
from tests.worker.fake_engine import FakeEngine


def test_sends_worker_headers(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    t = Transport(engine.url, "ifkey_x", "staging")
    reply = run(loop, t.request("POST", "/api/v1/workers/w1/register", {"function_ids": ["fn"]}))
    assert reply.ok and reply.body == {"status": "registered"}
    headers = {k.lower(): v for k, v in engine.calls("register")[0]["headers"].items()}
    assert headers["authorization"] == "Bearer ifkey_x"
    assert headers["x-ironflow-environment"] == "staging"
    assert headers["content-type"] == "application/json"
    assert engine.calls("register")[0]["body"] == {"function_ids": ["fn"]}


def test_no_auth_header_without_key(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    run(loop, Transport(engine.url, None, "default").request("POST", "/api/v1/workers/w1/register", {}))
    assert "authorization" not in {k.lower() for k in engine.calls("register")[0]["headers"]}


def test_status_is_returned_not_raised(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    engine.fail("poll", 409, {"error": "STALE_EXECUTION", "message": "x"})
    engine.registered.add("w1")
    reply = run(loop, Transport(engine.url, None, "default").request("GET", "/api/v1/workers/w1/jobs?available=1"))
    assert reply.status == 409 and not reply.ok and reply.error_code == "STALE_EXECUTION"


def test_204_has_no_body(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    engine.registered.add("w1")
    reply = run(loop, Transport(engine.url, None, "default").request("GET", "/api/v1/workers/w1/jobs?available=1"))
    assert reply.status == 204 and reply.body is None


def test_network_error_is_retryable_ironflow_error(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(IronflowError) as info:
        run(loop, Transport("http://127.0.0.1:1", None, "default").request("GET", "/health"))
    assert info.value.retryable is True and info.value.code == "NETWORK_ERROR"


def test_stalled_server_times_out(loop: asyncio.AbstractEventLoop) -> None:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    url = f"http://127.0.0.1:{listener.getsockname()[1]}"
    try:
        started = time.monotonic()
        with pytest.raises(IronflowError) as info:
            run(loop, Transport(url, None, "default", request_timeout=0.2).request("GET", "/x"))
        assert info.value.code == "NETWORK_ERROR" and time.monotonic() - started < 2
    finally:
        listener.close()


def test_per_call_headers_are_merged_over_the_defaults(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    t = Transport(engine.url, "ifkey_x", "staging")
    run(loop, t.request("POST", "/api/v1/workers/w1/register", {}, headers={"X-Ironflow-Run-ID": "run_9"}))
    headers = {k.lower(): v for k, v in engine.calls("register")[0]["headers"].items()}
    assert headers["x-ironflow-run-id"] == "run_9"
    assert headers["authorization"] == "Bearer ifkey_x" and headers["x-ironflow-environment"] == "staging"
