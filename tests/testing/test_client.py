from __future__ import annotations

import asyncio
import subprocess
import sys
from typing import Any

import pytest

from ironflow.testing import TestClient
from ironflow.worker import Context, SchemaValidationError, function


@function(id="order", triggers=[{"event": "order.created"}])
async def order(ctx: Context) -> Any:
    charge = await ctx.step.run("charge", lambda: {"ok": False})
    ctx.step.compensate("charge", lambda: None)
    paid = await ctx.step.wait_for_event("pay", event="payment.confirmed")
    await ctx.step.invoke("send-email", {"to": ctx.event.data["email"]})
    return {"charge": charge, "paid": paid.data, "run": ctx.run.function_id}


def test_happy_path() -> None:
    tc = TestClient([order])
    tc.mock_step("charge", lambda: {"ok": True})
    tc.mock_invoke("send-email", lambda data: None)
    tc.send_event("payment.confirmed", {"id": 9})
    run = asyncio.run(tc.emit("order.created", {"email": "a@b.c"}))
    assert run.status == "completed" and run.error is None
    assert run.output == {"charge": {"ok": True}, "paid": {"id": 9}, "run": "order"}
    assert run.step_output("charge") == {"ok": True}
    assert [s.type for s in run.steps] == ["run", "wait_for_event", "invoke"]
    assert run.compensations_ran == []


def test_success_skips_compensations() -> None:
    ran: list[str] = []

    @function(id="f", triggers=[{"event": "e"}])
    async def f(ctx: Context) -> str:
        ctx.step.compensate("a", lambda: ran.append("a"))
        return "ok"

    run = asyncio.run(TestClient([f]).emit("e", {}))
    assert run.status == "completed" and run.compensations_ran == [] and ran == []


def test_failure_runs_compensations_in_reverse() -> None:
    ran: list[str] = []

    def bad_undo() -> None:
        ran.append("b")
        raise RuntimeError("undo failed")

    @function(id="saga", triggers=[{"event": "go"}])
    async def saga(ctx: Context) -> None:
        ctx.step.compensate("a", lambda: ran.append("a"))
        ctx.step.compensate("b", bad_undo)
        ctx.step.compensate("c", lambda: ran.append("c"))
        raise ValueError("boom")

    run = asyncio.run(TestClient([saga]).emit("go", {}))
    assert run.status == "failed" and isinstance(run.error, ValueError)
    assert ran == ["c", "b", "a"] and run.compensations_ran == ["c", "b", "a"]
    comp = [s for s in run.steps if s.type == "compensate"]
    assert [s.name for s in comp] == ["compensate:c", "compensate:b", "compensate:a"]
    assert isinstance(comp[1].error, RuntimeError) and comp[0].error is None


def test_handler_error_before_steps() -> None:
    @function(id="f", triggers=[{"event": "e"}])
    async def f(ctx: Context) -> None:
        raise KeyError("x")

    run = asyncio.run(TestClient([f]).emit("e", {}))
    assert run.status == "failed" and isinstance(run.error, KeyError) and run.steps == []


def test_missing_invoke_mock_fails_run() -> None:
    tc = TestClient([order])
    tc.send_event("payment.confirmed", {})
    run = asyncio.run(tc.emit("order.created", {"email": "x"}))
    assert run.status == "failed" and isinstance(run.error, LookupError)


def test_no_matching_trigger() -> None:
    with pytest.raises(ValueError, match="no function has a trigger for 'nope'"):
        asyncio.run(TestClient([order]).emit("nope", {}))


def test_schema_failure_fails_run() -> None:
    def schema(data: Any) -> Any:
        raise ValueError("bad shape")

    @function(id="s", triggers=[{"event": "e"}], schema=schema)
    async def s(ctx: Context) -> None:
        return None

    run = asyncio.run(TestClient([s]).emit("e", {}))
    assert run.status == "failed" and isinstance(run.error, SchemaValidationError)


def test_schema_result_reaches_handler() -> None:
    @function(id="s", triggers=[{"event": "e"}], schema=lambda d: {"n": int(d["n"])})
    async def s(ctx: Context) -> Any:
        return ctx.event.data

    assert asyncio.run(TestClient([s]).emit("e", {"n": "3"})).output == {"n": 3}


def test_not_collected_by_pytest(tmp_path: Any) -> None:
    (tmp_path / "test_user.py").write_text(
        "from ironflow.testing import TestClient, TestRun, TestStep, TestStepRecord\n"
        "def test_ok():\n    assert TestClient\n")
    out = subprocess.run([sys.executable, "-m", "pytest", "-q", "-W", "error::pytest.PytestCollectionWarning",
                          str(tmp_path)], capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stdout + out.stderr
