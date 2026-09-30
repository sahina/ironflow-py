from __future__ import annotations

import asyncio

import pytest

from ironflow.worker._step import ExecutionContext, Step, _Yield
from tests.worker.conftest import run


def fresh() -> ExecutionContext:
    return ExecutionContext("run_1", [])


def test_branch_ids_are_positional_under_reverse_completion(loop: asyncio.AbstractEventLoop) -> None:
    ctx = fresh()

    def branch(i: int):  # type: ignore[no-untyped-def]
        async def body(step: Step) -> int:
            await asyncio.sleep((4 - i) / 1000)
            return await step.run("leaf", lambda: i)  # type: ignore[no-any-return]
        return body

    assert run(loop, Step(ctx).parallel("fan", [branch(i) for i in range(5)])) == list(range(5))
    assert sorted(s["id"] for s in ctx.executed) == [f"run_1:fan:{i}:leaf:0" for i in range(5)]


def test_nested_parallel_scopes(loop: asyncio.AbstractEventLoop) -> None:
    ctx = fresh()

    async def inner(step: Step) -> None:
        await step.run("x:y", lambda: 1)

    async def outer(step: Step) -> None:
        await step.parallel("inner", [inner])

    run(loop, Step(ctx).parallel("fan", [outer, outer]))
    assert sorted(s["id"] for s in ctx.executed) == [
        "run_1:fan:0:inner:0:x\\:y:0", "run_1:fan:1:inner:0:x\\:y:0",
    ]


def test_lowest_index_yield_wins_and_running_branches_finish(loop: asyncio.AbstractEventLoop) -> None:
    ctx = fresh()
    finished: list[int] = []

    async def worker(step: Step) -> None:
        await asyncio.sleep(0.02)
        await step.run("work", lambda: 1)
        finished.append(0)

    async def late_yield(step: Step) -> None:
        await asyncio.sleep(0.01)
        await step.sleep("s1", "1m")

    async def early_yield(step: Step) -> None:
        await step.sleep("s2", "1m")

    with pytest.raises(_Yield) as info:
        run(loop, Step(ctx).parallel("fan", [worker, late_yield, early_yield]))
    assert info.value.info["step_id"] == "run_1:fan:1:s1:0"
    assert finished == [0]


def test_yield_skips_branches_not_started(loop: asyncio.AbstractEventLoop) -> None:
    started: list[int] = []

    def branch(i: int):  # type: ignore[no-untyped-def]
        async def body(step: Step) -> None:
            started.append(i)
            if i == 0:
                await step.sleep("s", "1m")
        return body

    with pytest.raises(_Yield):
        run(loop, Step(fresh()).parallel("fan", [branch(i) for i in range(3)], concurrency=1))
    assert started == [0]


def test_fail_fast_raises_lowest_index_error(loop: asyncio.AbstractEventLoop) -> None:
    async def b0(step: Step) -> None:
        await asyncio.sleep(0.02)
        raise ValueError("zero")

    async def b1(step: Step) -> None:
        raise ValueError("one")

    with pytest.raises(ValueError, match="zero"):
        run(loop, Step(fresh()).parallel("fan", [b0, b1]))


def test_yield_beats_error(loop: asyncio.AbstractEventLoop) -> None:
    both_started = asyncio.Event()
    started = 0

    async def barrier() -> None:
        nonlocal started
        started += 1
        if started == 2:
            both_started.set()
        await both_started.wait()

    async def err(step: Step) -> None:
        await barrier()
        raise ValueError("x")

    async def y(step: Step) -> None:
        await barrier()
        await step.sleep("s", "1m")

    with pytest.raises(_Yield):
        run(loop, Step(fresh()).parallel("fan", [err, y]))


def test_collect_returns_values_and_exceptions(loop: asyncio.AbstractEventLoop) -> None:
    async def ok(step: Step) -> int:
        return 1

    async def bad(step: Step) -> int:
        raise ValueError("x")

    out = run(loop, Step(fresh()).parallel("fan", [ok, bad], on_error="collect"))
    assert out[0] == 1 and isinstance(out[1], ValueError)


def test_concurrency_limit(loop: asyncio.AbstractEventLoop) -> None:
    active = 0
    peak = 0

    async def body(step: Step) -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1

    run(loop, Step(fresh()).parallel("fan", [body] * 6, concurrency=2))
    assert peak == 2
