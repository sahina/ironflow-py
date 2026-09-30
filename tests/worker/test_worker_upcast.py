from __future__ import annotations

import asyncio
from typing import Any

from ironflow import UpcasterRegistry
from tests.worker.conftest import run
from tests.worker.fake_engine import FakeEngine, make_job
from tests.worker.test_worker_jobs import terminal, worker_for
from tests.worker.test_worker_loop import run_until


def registry() -> UpcasterRegistry:
    r = UpcasterRegistry()
    r.register("e", 1, 2, lambda d: {"total": d["amount"]})
    return r


def test_v1_event_reaches_handler_as_v2(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> Any:
        return ctx.event.data

    engine.enqueue(make_job(event={"id": "ev", "name": "e", "data": {"amount": 7}, "version": 1,
                                   "timestamp": "2026-09-27T10:00:00Z"}))
    run(loop, run_until(worker_for(engine, h, upcasters=registry()), lambda: engine.calls("terminal")))
    assert terminal(engine)["output"] == {"total": 7}


def test_broken_chain_fails_job_non_retryable(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    r = UpcasterRegistry()
    r.register("e", 2, 3, lambda d: d)  # no v1→v2 link

    async def h(ctx: Any) -> Any:
        return "unreachable"

    engine.enqueue(make_job(event={"id": "ev", "name": "e", "data": {}, "version": 1,
                                   "timestamp": "2026-09-27T10:00:00Z"}))
    run(loop, run_until(worker_for(engine, h, upcasters=r), lambda: engine.calls("terminal")))
    body = terminal(engine)
    assert body["status"] == "failed"
    assert body["error"]["retryable"] is False and "chain broken at v1" in body["error"]["message"]


def test_no_registry_leaves_data_alone(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> Any:
        return ctx.event.data

    engine.enqueue(make_job(event={"id": "ev", "name": "e", "data": {"amount": 7}, "version": 1,
                                   "timestamp": "2026-09-27T10:00:00Z"}))
    run(loop, run_until(worker_for(engine, h), lambda: engine.calls("terminal")))
    assert terminal(engine)["output"] == {"amount": 7}


def test_version_zero_is_treated_as_v1(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> Any:
        return ctx.event.data

    engine.enqueue(make_job(event={"id": "ev", "name": "e", "data": {"amount": 7}, "version": 0,
                                   "timestamp": "2026-09-27T10:00:00Z"}))
    run(loop, run_until(worker_for(engine, h, upcasters=registry()), lambda: engine.calls("terminal")))
    assert terminal(engine)["output"] == {"total": 7}  # same as the streaming path (`version or 1`)
