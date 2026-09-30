from __future__ import annotations

import asyncio
from typing import Any

from ironflow.worker._step import ExecutionContext, Step
from tests.worker.conftest import run


def test_map_ids_and_order(loop: asyncio.AbstractEventLoop) -> None:
    ctx = ExecutionContext("run_1", [])

    async def fn(item: int, step: Step, i: int) -> int:
        await asyncio.sleep((3 - i) * 0.01)
        return await step.run("leaf", lambda: item * 10)

    assert run(loop, Step(ctx).map("m", [1, 2, 3], fn)) == [10, 20, 30]
    assert sorted(s["id"] for s in ctx.executed) == ["run_1:m:0:leaf:0", "run_1:m:1:leaf:0", "run_1:m:2:leaf:0"]


def test_map_concurrency_limit(loop: asyncio.AbstractEventLoop) -> None:
    live = peak = 0

    async def fn(item: int, step: Step, i: int) -> int:
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        await asyncio.sleep(0.01)
        live -= 1
        return item

    run(loop, Step(ExecutionContext("run_1", [])).map("m", list(range(6)), fn, concurrency=2))
    assert peak == 2


def test_map_collect_mixes_values_and_errors(loop: asyncio.AbstractEventLoop) -> None:
    async def fn(item: int, step: Step, i: int) -> int:
        if item == 1:
            raise ValueError("bad")
        return item

    out = run(loop, Step(ExecutionContext("run_1", [])).map("m", [0, 1, 2], fn, on_error="collect"))
    assert out[0] == 0 and isinstance(out[1], ValueError) and out[2] == 2


def test_map_empty(loop: asyncio.AbstractEventLoop) -> None:
    async def fn(item: Any, step: Step, i: int) -> Any:
        ...

    assert run(loop, Step(ExecutionContext("run_1", [])).map("m", [], fn)) == []
