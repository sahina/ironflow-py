from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest

from ironflow import IronflowError
from ironflow.worker._step import (
    ExecutionContext,
    NonRetryableError,
    Step,
    StepError,
    StepTimeoutError,
    _Yield,
    escape_step_id_part,
)
from tests.worker.conftest import run


def ctx_with(completed: dict[str, Any] | None = None) -> ExecutionContext:
    steps = [{"step_id": k, "name": k, "output": v} for k, v in (completed or {}).items()]
    return ExecutionContext("run_1", steps)  # type: ignore[arg-type]


def test_failed_row_is_not_completed() -> None:
    ctx = ExecutionContext("run_1", [
        {"step_id": "a", "name": "a", "output": 1},
        {"step_id": "b", "name": "b", "output": None, "status": "failed", "error": {"cause": "x"}},
    ])
    assert ctx.is_completed("a") and not ctx.is_completed("b")
    assert ctx.failed_error("b") == {"cause": "x"} and ctx.failed_error("a") is None


@pytest.mark.parametrize(
    ("part", "escaped"),
    [("plain", "plain"), ("a:b", "a\\:b"), ("a\\b", "a\\\\b"), ("compensate:x:y", "compensate:x\\:y"),
     ("publish:t:1", "publish:t\\:1"), ("compensate", "compensate")],
)
def test_escape_step_id_part(part: str, escaped: str) -> None:
    assert escape_step_id_part(part) == escaped


def test_run_executes_and_records(loop: asyncio.AbstractEventLoop) -> None:
    ctx = ctx_with()
    recorded: list[int] = []
    ctx.on_step_recorded = lambda: recorded.append(1)

    async def body() -> int:
        return 42

    assert run(loop, Step(ctx).run("a", body)) == 42
    [step] = ctx.executed
    assert step["id"] == "run_1:a:0"
    assert step["status"] == "completed" and step["output"] == 42 and step["type"] == "invoke"
    assert recorded == [1]


def test_run_repeats_name_with_counter(loop: asyncio.AbstractEventLoop) -> None:
    ctx = ctx_with()
    step = Step(ctx)
    run(loop, step.run("a", lambda: 1))
    run(loop, step.run("a", lambda: 2))
    assert [s["id"] for s in ctx.executed] == ["run_1:a:0", "run_1:a:1"]


def test_run_memoized_does_not_call_body(loop: asyncio.AbstractEventLoop) -> None:
    ctx = ctx_with({"run_1:a:0": {"cached": True}})
    calls: list[int] = []
    out = run(loop, Step(ctx).run("a", lambda: calls.append(1)))
    assert out == {"cached": True}
    assert calls == [] and ctx.executed == []


def test_run_sync_callback_runs_in_thread(loop: asyncio.AbstractEventLoop) -> None:
    main = threading.get_ident()
    assert run(loop, Step(ctx_with()).run("a", lambda: threading.get_ident())) != main


def test_run_sync_callback_returning_coroutine_is_awaited(loop: asyncio.AbstractEventLoop) -> None:
    async def fetch() -> str:
        await asyncio.sleep(0)
        return "fetched"

    assert run(loop, Step(ctx_with()).run("a", lambda: fetch())) == "fetched"


def test_run_failure_records_and_raises_step_error(loop: asyncio.AbstractEventLoop) -> None:
    ctx = ctx_with()

    def boom() -> None:
        raise ValueError("bad input")

    with pytest.raises(StepError) as info:
        run(loop, Step(ctx).run("a", boom))
    assert info.value.retryable is True and info.value.step_id == "run_1:a:0"
    assert isinstance(info.value.__cause__, ValueError)
    assert ctx.executed[0]["status"] == "failed"
    assert ctx.executed[0]["error"] == {"message": "bad input", "retryable": True}


def test_run_non_retryable_cause_is_not_retryable(loop: asyncio.AbstractEventLoop) -> None:
    def boom() -> None:
        raise NonRetryableError("card declined")

    with pytest.raises(StepError) as info:
        run(loop, Step(ctx_with()).run("a", boom))
    assert info.value.retryable is False


def test_run_timeout_async(loop: asyncio.AbstractEventLoop) -> None:
    ctx = ctx_with()

    async def slow() -> None:
        await asyncio.sleep(5)

    started = time.monotonic()
    with pytest.raises(StepTimeoutError):
        run(loop, Step(ctx).run("a", slow, timeout=0.05))
    assert time.monotonic() - started < 1
    assert ctx.executed[0]["status"] == "failed"


def test_run_timeout_does_not_wait_for_cancellation_suppression(loop: asyncio.AbstractEventLoop) -> None:
    async def slow() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await asyncio.sleep(0.3)
            raise RuntimeError("late failure")

    started = time.monotonic()
    try:
        with pytest.raises(StepTimeoutError):
            run(loop, Step(ctx_with()).run("a", slow, timeout=0.01))
        assert time.monotonic() - started < 0.2
    finally:
        # Let the cancelled body finish before the fixture closes the loop.
        run(loop, asyncio.sleep(0.35))


def test_run_body_raising_timeout_error_is_a_step_failure(loop: asyncio.AbstractEventLoop) -> None:
    def boom() -> None:
        raise TimeoutError("upstream timed out")

    with pytest.raises(StepError):
        run(loop, Step(ctx_with()).run("a", boom, timeout=5))


@pytest.mark.parametrize("bad", [object(), float("nan"), {"a": [1, float("inf")]}, {"x": {"y": float("-inf")}}])
def test_run_rejects_non_json_output(loop: asyncio.AbstractEventLoop, bad: object) -> None:
    # NaN and infinities are not JSON; Go's decoder rejects them, so Python must too.
    ctx = ctx_with()
    with pytest.raises(NonRetryableError) as info:
        run(loop, Step(ctx).run("a", lambda: bad))
    assert info.value.code == "SERIALIZATION_ERROR"
    assert ctx.executed == []


def test_yield_is_not_caught_by_except_exception() -> None:
    assert not issubclass(_Yield, Exception)


def test_cancelled_error_is_not_recorded_as_failure(loop: asyncio.AbstractEventLoop) -> None:
    ctx = ctx_with()

    async def forever() -> None:
        await asyncio.sleep(10)

    async def scenario() -> None:
        task = asyncio.ensure_future(Step(ctx).run("a", forever))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    run(loop, scenario())
    assert ctx.executed == []


def test_rejects_empty_name(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(ValueError):
        run(loop, Step(ctx_with()).run("", lambda: 1))


def test_errors_are_ironflow_errors() -> None:
    assert issubclass(StepError, IronflowError) and issubclass(NonRetryableError, IronflowError)


def test_function_step_timeout_is_the_default(loop: asyncio.AbstractEventLoop) -> None:
    ctx = ExecutionContext("run_1", [], step_timeout=0.05)

    async def slow() -> None:
        await asyncio.sleep(5)

    with pytest.raises(StepTimeoutError):
        run(loop, Step(ctx).run("a", slow))


def test_step_timeout_overrides_function_default(loop: asyncio.AbstractEventLoop) -> None:
    ctx = ExecutionContext("run_1", [], step_timeout=0.01)

    async def body() -> int:
        await asyncio.sleep(0.05)
        return 1

    assert run(loop, Step(ctx).run("a", body, timeout=5)) == 1
