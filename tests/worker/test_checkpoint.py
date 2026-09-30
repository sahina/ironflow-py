from __future__ import annotations

import asyncio
import logging
from typing import Any

from ironflow.worker._checkpoint import Checkpointer
from ironflow.worker._protocol import parse_job
from ironflow.worker._step import ExecutionContext
from ironflow.worker._transport import Transport
from tests.worker.conftest import run
from tests.worker.fake_engine import FakeEngine, make_job


def setup(engine: FakeEngine, interval: float = 0.01, base: int = 3) -> tuple[Checkpointer, ExecutionContext, list[int]]:
    engine.registered.add("w1")
    job = parse_job(make_job(step_sequence_base=base))
    ctx = ExecutionContext("run_1", [])
    stale: list[int] = []
    cp = Checkpointer(transport=Transport(engine.url, None, "default"), worker_id="w1", job=job, ctx=ctx,
                      interval=interval, on_stale=lambda: stale.append(1), logger=logging.getLogger("t"))
    ctx.on_step_recorded = cp.schedule
    return cp, ctx, stale


def add_steps(ctx: ExecutionContext, n: int, start: int = 0) -> None:
    for i in range(start, start + n):
        ctx.record({"id": f"s{i}", "name": f"s{i}", "type": "invoke", "status": "completed", "output": i,
                    "started_at": "x", "ended_at": "x", "duration_ms": 0})


async def until(pred: Any, timeout: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not pred():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met")
        await asyncio.sleep(0.005)


def test_flushes_after_step_with_offset(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    cp, ctx, _ = setup(engine)

    async def scenario() -> None:
        add_steps(ctx, 2)
        await until(lambda: engine.calls("progress"))
        steps, offset = await cp.finish()
        assert steps == [] and offset == 5

    run(loop, scenario())
    body = engine.calls("progress")[0]["body"]
    assert body["status"] == "progress" and body["step_offset"] == 3 and len(body["steps"]) == 2
    assert body["execution_seq"] == 1 and body["lease_token"] == "tok"


def test_batches_are_capped_at_500(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    cp, ctx, _ = setup(engine, base=0)

    async def scenario() -> None:
        add_steps(ctx, 1200)
        await until(lambda: sum(len(c["body"]["steps"]) for c in engine.calls("progress")) == 1200)
        assert await cp.finish() == ([], 1200)

    run(loop, scenario())
    sizes = [(c["body"]["step_offset"], len(c["body"]["steps"])) for c in engine.calls("progress")]
    assert sizes == [(0, 500), (500, 500), (1000, 200)]


def test_failed_flush_retries_without_new_steps(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    cp, ctx, _ = setup(engine)
    engine.fail("progress", 503, {"error": "busy"}, times=2)

    async def scenario() -> None:
        add_steps(ctx, 1)
        await until(lambda: len(engine.calls("progress")) == 3)
        steps, offset = await cp.finish()
        assert steps == [] and offset == 4

    run(loop, scenario())
    assert [c["body"]["step_offset"] for c in engine.calls("progress")] == [3, 3, 3]


def test_stale_calls_on_stale_and_stops(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    _, ctx, stale = setup(engine)
    engine.fail("progress", 409, {"error": "STALE_EXECUTION", "message": "x"})

    async def scenario() -> None:
        add_steps(ctx, 1)
        await until(lambda: stale == [1])
        add_steps(ctx, 1, start=1)
        await asyncio.sleep(0.05)

    run(loop, scenario())
    assert len(engine.calls("progress")) == 1


def test_other_4xx_disables_and_tail_goes_to_finish(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    cp, ctx, stale = setup(engine)
    engine.fail("progress", 409, {"error": "RUN_NOT_RUNNING", "message": "x"})

    async def scenario() -> None:
        add_steps(ctx, 1)
        await until(lambda: engine.calls("progress"))
        add_steps(ctx, 1, start=1)
        await asyncio.sleep(0.05)
        steps, offset = await cp.finish()
        assert [s["id"] for s in steps] == ["s0", "s1"] and offset == 3

    run(loop, scenario())
    assert stale == [] and len(engine.calls("progress")) == 1


def test_close_cancels_pending_retry(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    cp, ctx, _ = setup(engine)
    engine.fail("progress", 503, {"error": "busy"}, times=100)

    async def scenario() -> None:
        add_steps(ctx, 1)
        await until(lambda: engine.calls("progress"))
        await cp.close()
        await cp.close()
        sent = len(engine.calls("progress"))
        await asyncio.sleep(0.2)
        assert len(engine.calls("progress")) == sent

    run(loop, scenario())


def test_close_cancels_direct_inflight_flush(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    cp, ctx, stale = setup(engine)
    started = asyncio.Event()
    requests: list[int] = []

    async def blocked_request(method: str, path: str, body: Any) -> None:
        requests.append(1)
        started.set()
        await asyncio.Event().wait()

    async def scenario() -> None:
        cp._transport.request = blocked_request  # type: ignore[method-assign]
        add_steps(ctx, 1)
        direct = asyncio.create_task(cp.flush())
        await started.wait()
        await cp.close()
        assert direct.done() and direct.cancelled()
        await asyncio.sleep(0.05)
        assert requests == [1] and stale == []

    run(loop, scenario())


def test_retry_caps_large_failure_count(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    cp, ctx, _ = setup(engine, interval=0.001)
    cp.MAX_BACKOFF = 0.01
    cp._failures = 10_000
    engine.fail("progress", 503, {"error": "busy"})

    async def scenario() -> None:
        add_steps(ctx, 1)
        await until(lambda: len(engine.calls("progress")) == 2)
        assert await cp.finish() == ([], 4)

    run(loop, scenario())


def test_zero_interval_disables(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    cp, ctx, _ = setup(engine, interval=0)

    async def scenario() -> None:
        add_steps(ctx, 3)
        await asyncio.sleep(0.05)
        steps, _ = await cp.finish()
        assert len(steps) == 3

    run(loop, scenario())
    assert engine.calls("progress") == []
