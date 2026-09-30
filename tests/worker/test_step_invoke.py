from __future__ import annotations

from typing import Any

import pytest

from ironflow.worker._step import (
    ExecutionContext,
    InvokeAsyncResult,
    InvokeError,
    Step,
    _Yield,
)
from tests.worker.conftest import run


def ctx_with(rows: list[dict[str, Any]] | None = None) -> ExecutionContext:
    return ExecutionContext("run_1", rows or [])  # type: ignore[arg-type]


def test_invoke_yields_invoke_function(loop) -> None:
    with pytest.raises(_Yield) as y:
        run(loop, Step(ctx_with()).invoke("child", {"n": 1}, timeout="2s"))
    assert y.value.info == {"step_id": "run_1:child:0", "type": "invoke_function",
                            "function_id": "child", "input": {"n": 1}, "invoke_timeout_ms": 2000}


def test_invoke_default_timeout_is_30s(loop) -> None:
    with pytest.raises(_Yield) as y:
        run(loop, Step(ctx_with()).invoke("child"))
    assert y.value.info["invoke_timeout_ms"] == 30000


def test_invoke_memo_completed_returns_output(loop) -> None:
    ctx = ctx_with([{"step_id": "run_1:child:0", "name": "child", "output": {"ok": 1}}])
    assert run(loop, Step(ctx).invoke("child")) == {"ok": 1}


def test_invoke_memo_failed_raises_and_never_yields(loop) -> None:
    ctx = ctx_with([{"step_id": "run_1:child:0", "name": "child", "output": None, "status": "failed",
                     "error": {"message": "invoked function 'child' failed", "function_id": "child",
                               "child_run_id": "r2", "cause": "boom", "retryable": False}}])
    with pytest.raises(InvokeError) as e:
        run(loop, Step(ctx).invoke("child"))
    assert e.value.child_run_id == "r2" and e.value.cause == "boom" and e.value.retryable is False
    assert "child" in str(e.value) and "boom" in str(e.value)


def test_invoke_error_falls_back_to_message(loop) -> None:
    ctx = ctx_with([{"step_id": "run_1:child:0", "name": "child", "output": None, "status": "failed",
                     "error": {"message": "target function 'child' not found", "retryable": False}}])
    with pytest.raises(InvokeError) as e:
        run(loop, Step(ctx).invoke("child"))
    assert e.value.cause == "target function 'child' not found" and e.value.function_id == "child"


def test_invoke_async_yields_and_memoizes_run_id(loop) -> None:
    with pytest.raises(_Yield) as y:
        run(loop, Step(ctx_with()).invoke_async("child", 1))
    assert y.value.info == {"step_id": "run_1:child:0", "type": "invoke_function_async",
                            "function_id": "child", "input": 1}
    ctx = ctx_with([{"step_id": "run_1:child:0", "name": "child", "output": {"run_id": "r9"}}])
    assert run(loop, Step(ctx).invoke_async("child")) == InvokeAsyncResult(run_id="r9")


def test_invoke_in_branch_uses_branch_scope(loop) -> None:
    async def b(step: Step) -> Any:
        return await step.invoke("child")
    with pytest.raises(_Yield) as y:
        run(loop, Step(ctx_with()).parallel("fan", [b]))
    assert y.value.info["step_id"] == "run_1:fan:0:child:0"


def test_invoke_resumes_failed_row_as_invoke_error(loop) -> None:
    ctx = ctx_with([{"step_id": "run_1:child:0", "name": "child", "output": None,
                     "status": "failed", "error": {"message": "invoke timed out"}}])
    with pytest.raises(InvokeError, match="invoke timed out"):
        run(loop, Step(ctx).invoke("child"))


def test_invoke_rejects_empty_function_id(loop) -> None:
    with pytest.raises(ValueError):
        run(loop, Step(ctx_with()).invoke(""))


def test_invoke_error_plain_string_is_not_json_quoted(loop) -> None:
    ctx = ctx_with([{"step_id": "run_1:child:0", "name": "child", "output": None, "status": "failed",
                     "error": "boom"}])
    with pytest.raises(InvokeError) as e:
        run(loop, Step(ctx).invoke("child"))
    assert e.value.cause == "boom"
