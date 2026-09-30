from __future__ import annotations

import asyncio
from typing import Any

import pytest

from ironflow.worker._function import function
from ironflow.worker._worker import Worker, WorkerAuthError
from tests.worker.conftest import run
from tests.worker.fake_engine import FakeEngine


@function(id="fn", triggers=[{"event": "e"}])
async def fn(ctx: Any) -> str:
    return "done"


def fast_worker(engine: FakeEngine, **kw: Any) -> Worker:
    options = {"server_url": engine.url, "worker_id": "w1", "heartbeat_interval": 0.02,
               "reconnect_delay": 0.01, "checkpoint_interval": 0.01, "drain_timeout": 1, "functions": [fn]}
    options.update(kw)
    w = Worker(**options)
    w._idle_poll = 0.01
    w._max_backoff = 0.05
    w._report_delays = (0.01, 0.01, 0.01, 0.01)
    return w


async def until(pred: Any, timeout: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not pred():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met")
        await asyncio.sleep(0.005)


async def run_until(worker: Worker, pred: Any) -> None:
    task = asyncio.ensure_future(worker.start())
    try:
        await until(pred)
    finally:
        await worker.stop()
        await task


def test_registers_function_then_worker(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    run(loop, run_until(fast_worker(engine, environment="staging"), lambda: engine.calls("poll")))
    routes = [r["route"] for r in engine.requests]
    assert routes.index("register_function") < routes.index("register") < routes.index("poll")
    rf = engine.calls("register_function")[0]
    assert rf["body"]["preferredMode"] == "EXECUTION_MODE_PULL"
    assert {k.lower(): v for k, v in rf["headers"].items()}["x-ironflow-environment"] == "staging"
    reg = engine.calls("register")[0]["body"]
    assert reg["function_ids"] == ["fn"] and reg["max_concurrent_jobs"] == 10
    assert reg["version"]["runtime"].startswith("python-")


def test_poll_advertises_free_slots(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    run(loop, run_until(fast_worker(engine, max_concurrent_jobs=3), lambda: engine.calls("poll")))
    assert engine.calls("poll")[0]["query"] == "available=3"


def test_heartbeat_always_sends_jobs_list(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    run(loop, run_until(fast_worker(engine), lambda: engine.calls("heartbeat")))
    assert engine.calls("heartbeat")[0]["body"]["jobs"] == []


def test_404_on_poll_registers_again(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    engine.fail("poll", 404, {"error": "worker not registered"})
    run(loop, run_until(fast_worker(engine), lambda: len(engine.calls("register")) >= 2))
    assert len(engine.calls("register_function")) >= 2


def test_heartbeat_stops_during_reregistration_then_restarts(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    engine.fail("heartbeat", 404, {"error": "worker not registered"})
    worker = fast_worker(engine, heartbeat_interval=0.05)
    register = worker._register
    second_started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def held_register() -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            second_started.set()
            await release.wait()
        await register()

    worker._register = held_register  # type: ignore[method-assign]

    async def scenario() -> None:
        task = asyncio.ensure_future(worker.start())
        try:
            await until(lambda: worker._heartbeat_task is not None)
            first = worker._heartbeat_task
            await asyncio.wait_for(second_started.wait(), 3)
            assert len(engine.calls("heartbeat")) == 1
            await asyncio.sleep(0.16)
            assert len(engine.calls("heartbeat")) == 1
            release.set()
            await until(lambda: len(engine.calls("register")) == 2 and len(engine.calls("heartbeat")) >= 2)
            assert first is not None and first.done()
            assert worker._heartbeat_task is not first
        finally:
            release.set()
            await worker.stop()
            await task

    run(loop, scenario())


def test_stop_during_registration_cannot_restart_heartbeat(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    worker = fast_worker(engine)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def register() -> None:
        entered.set()
        await release.wait()

    worker._register = register  # type: ignore[method-assign]

    async def scenario() -> None:
        task = asyncio.ensure_future(worker.start())
        stop_task: asyncio.Task[None] | None = None
        try:
            await entered.wait()
            stop_task = asyncio.ensure_future(worker.stop())
            await until(lambda: worker._draining is not None and worker._draining.is_set())
            release.set()
            await stop_task
            await asyncio.wait_for(task, 1)
            assert worker.state == "stopped"
            assert worker._heartbeat_task is None
        finally:
            release.set()
            if stop_task is not None and not stop_task.done():
                stop_task.cancel()
                await asyncio.gather(stop_task, return_exceptions=True)
            if worker._heartbeat_task is not None:
                worker._heartbeat_task.cancel()
                await asyncio.gather(worker._heartbeat_task, return_exceptions=True)
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    run(loop, scenario())


def test_401_stops_with_auth_error(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    engine.fail("register_function", 401, {"error": "unauthorized"})
    with pytest.raises(WorkerAuthError):
        run(loop, fast_worker(engine).start())


def test_5xx_backs_off_then_recovers(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    engine.fail("poll", 503, {"error": "busy"}, times=3)
    run(loop, run_until(fast_worker(engine), lambda: len(engine.calls("poll")) >= 5))


def test_bad_poll_shape_is_ignored(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    engine.fail("poll", 200, {"unexpected": []})
    run(loop, run_until(fast_worker(engine), lambda: len(engine.calls("poll")) >= 2))


def test_rejects_bad_config() -> None:
    with pytest.raises(ValueError):
        Worker(functions=[])
    with pytest.raises(ValueError):
        Worker(functions=[fn, fn])
    with pytest.raises(ValueError):
        Worker(functions=[fn], max_concurrent_jobs=0)
