from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

from ironflow._http import IronflowError
from ironflow.worker import StreamingWorker, Worker
from tests.worker.conftest import run
from tests.worker.fake_engine import FakeEngine
from tests.worker.test_worker_loop import fn, run_until


@pytest.mark.parametrize("worker_class", [Worker, StreamingWorker])
@pytest.mark.parametrize("status", [400, 422, 503])
def test_terminal_registration_stops(
    loop: asyncio.AbstractEventLoop, engine: FakeEngine, worker_class: type[Worker], status: int,
) -> None:
    engine.fail("register_function", status, {"message": "invalid config", "retryable": False})
    # The old reconnect loop terminates on its second request, with the wrong error.
    engine.fail("register_function", 401)
    worker = worker_class(server_url=engine.url, functions=[fn], reconnect_delay=0.001)
    with pytest.raises(IronflowError, match="register function fn.*invalid config"):
        run(loop, worker.start())
    assert worker.state == "stopped"
    assert len(engine.calls("register_function")) == 1
    assert not engine.calls("register") and not engine.calls("poll")


@pytest.mark.parametrize("worker_class", [Worker, StreamingWorker])
@pytest.mark.parametrize("status", [429, 503, 400])
def test_retryable_registration_recovers(
    loop: asyncio.AbstractEventLoop, engine: FakeEngine, status: int,
    worker_class: type[Worker], monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine.fail("register_function", status, {"retryable": True} if status == 400 else {})
    worker = worker_class(server_url=engine.url, functions=[fn], reconnect_delay=0.001)
    stream_connected = False
    if isinstance(worker, StreamingWorker):
        async def connected(*_args: Any, **_kwargs: Any) -> AsyncIterator[Any]:
            nonlocal stream_connected
            stream_connected = True
            await asyncio.Event().wait()
            for response in ():
                yield response
        monkeypatch.setattr(worker._stream_client, "connect", connected)
    run(loop, run_until(worker, lambda: stream_connected or engine.calls("poll")))
    assert len(engine.calls("register_function")) == 2


def test_streaming_reregistration_stops_on_terminal_error(
    loop: asyncio.AbstractEventLoop, engine: FakeEngine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine.fail("register_function", 200, {})
    engine.fail("register_function", 400, {"message": "invalid config"})
    engine.fail("register_function", 401)
    worker = StreamingWorker(server_url=engine.url, functions=[fn], reconnect_delay=0.001)

    async def disconnected(*_args: Any, **_kwargs: Any) -> AsyncIterator[Any]:
        # An empty server stream drives the real reconnect and re-registration path.
        for response in ():
            yield response

    monkeypatch.setattr(worker._stream_client, "connect", disconnected)
    with pytest.raises(IronflowError, match="register function fn.*invalid config"):
        run(loop, worker.start())
    assert worker.state == "stopped"
    assert len(engine.calls("register_function")) == 2
