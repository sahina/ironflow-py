from __future__ import annotations

from typing import Any

from ironflow.worker._step import ExecutionContext, Step, run_compensations
from tests.worker.conftest import run


def test_compensations_run_in_reverse_and_continue_after_failure(loop) -> None:
    ctx = ExecutionContext("run_1", [])
    step, calls = Step(ctx), []
    step.compensate("a", lambda: calls.append("a"))

    def bad() -> None:
        calls.append("b")
        raise RuntimeError("undo b failed")

    step.compensate("b", bad)
    step.compensate("c", lambda: calls.append("c"))
    results = run(loop, run_compensations(ctx))
    assert calls == ["c", "b", "a"]
    assert [(r["id"], r["type"], r["status"], r.get("compensation_for")) for r in results] == [
        ("run_1:compensate:c:0", "compensate", "completed", "c"),
        ("run_1:compensate:b:0", "compensate", "failed", "b"),
        ("run_1:compensate:a:0", "compensate", "completed", "a"),
    ]
    assert results[1]["error"] == {"message": "undo b failed", "retryable": False}
    assert results[0]["name"] == "compensate:c"


def test_same_name_twice_gets_counter(loop) -> None:
    ctx = ExecutionContext("run_1", [])
    step, calls = Step(ctx), []
    step.compensate("a", lambda: calls.append(1))
    step.compensate("a", lambda: calls.append(2))
    ids = [r["id"] for r in run(loop, run_compensations(ctx))]
    assert calls == [2, 1] and ids == ["run_1:compensate:a:0", "run_1:compensate:a:1"]


def test_memoized_completed_is_skipped_failed_reruns(loop) -> None:
    ctx = ExecutionContext("run_1", [
        {"step_id": "run_1:compensate:a:0", "name": "compensate:a", "output": None},
        {"step_id": "run_1:compensate:b:0", "name": "compensate:b", "output": None, "status": "failed", "error": {}},
    ])
    step, calls = Step(ctx), []
    step.compensate("a", lambda: calls.append("a"))
    step.compensate("b", lambda: calls.append("b"))
    run(loop, run_compensations(ctx))
    assert calls == ["b"]


def test_branch_compensation_shares_registry(loop) -> None:
    ctx = ExecutionContext("run_1", [])
    calls: list[str] = []

    async def b(step: Step) -> None:
        step.compensate("x", lambda: calls.append("x"))

    run(loop, Step(ctx).parallel("fan", [b]))
    run(loop, run_compensations(ctx))
    assert calls == ["x"]


def test_compensation_that_yields_is_recorded_failed(loop) -> None:
    ctx = ExecutionContext("run_1", [])
    step = Step(ctx)

    async def sleeps() -> None:
        await step.sleep("nap", 1)

    step.compensate("a", sleeps)
    [r] = run(loop, run_compensations(ctx))
    assert r["status"] == "failed" and "cannot yield" in r["error"]["message"]


def test_async_compensation_is_awaited(loop) -> None:
    ctx = ExecutionContext("run_1", [])
    calls: list[str] = []

    async def undo() -> None:
        calls.append("a")

    Step(ctx).compensate("a", undo)
    run(loop, run_compensations(ctx))
    assert calls == ["a"]


def test_results_not_streamed_as_step_results(loop) -> None:
    ctx = ExecutionContext("run_1", [])
    streamed: list[Any] = []
    ctx.on_step_result = streamed.append
    Step(ctx).compensate("a", lambda: None)
    run(loop, run_compensations(ctx))
    assert streamed == [] and len(ctx.executed) == 1
