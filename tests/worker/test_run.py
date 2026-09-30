# sdk/python/tests/worker/test_run.py
from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from ironflow import UpcasterRegistry
from ironflow.worker._function import function
from ironflow.worker._run import run_function
from ironflow.worker._step import ExecutionContext, NonRetryableError, RunInfo

EVENT = {"id": "ev", "name": "e", "data": {"amount": 7}, "version": 1, "timestamp": "2026-09-27T10:00:00Z"}
LOG = logging.LoggerAdapter(logging.getLogger("t"), {})


def call(handler: Any, *, raw_event: Any = EVENT, upcasters: Any = None,
         ctx: ExecutionContext | None = None, schema: Any = None) -> dict[str, Any]:
    fn = function(id="fn", triggers=[{"event": "e"}], schema=schema)(handler)
    return asyncio.run(run_function(
        fn, raw_event=raw_event, upcasters=upcasters, ctx=ctx or ExecutionContext("run_1", []),
        run=RunInfo(id="run_1", function_id="fn", attempt=1), secrets={"K": "v"}, logger=LOG))


def test_completed_carries_output_and_secrets() -> None:
    async def h(ctx: Any) -> Any:
        return {"d": ctx.event.data, "k": ctx.secrets["K"]}
    assert call(h) == {"status": "completed", "output": {"d": {"amount": 7}, "k": "v"}}


def test_yield_becomes_yielded() -> None:
    async def h(ctx: Any) -> None:
        await ctx.step.sleep("nap", "1h")
    out = call(h)
    assert out["status"] == "yielded" and out["yield"]["type"] == "sleep"


def test_non_json_output_is_serialization_error() -> None:
    async def h(ctx: Any) -> Any:
        return {1, 2}
    out = call(h)
    assert out["status"] == "failed"
    assert out["error"]["code"] == "SERIALIZATION_ERROR" and out["error"]["retryable"] is False


def test_non_retryable_runs_compensations() -> None:
    undone: list[str] = []

    async def h(ctx: Any) -> None:
        await ctx.step.run("a", lambda: 1)
        ctx.step.compensate("a", lambda: undone.append("a"))
        raise NonRetryableError("boom")
    ctx = ExecutionContext("run_1", [])
    out = call(h, ctx=ctx)
    assert out["error"] == {"message": "boom", "code": "NON_RETRYABLE", "retryable": False}
    assert undone == ["a"] and any(s["type"] == "compensate" for s in ctx.executed)


def test_plain_exception_is_retryable_error() -> None:
    async def h(ctx: Any) -> None:
        raise RuntimeError("x")
    assert call(h)["error"] == {"message": "x", "code": "ERROR", "retryable": True}


def test_upcast_applied() -> None:
    r = UpcasterRegistry()
    r.register("e", 1, 2, lambda d: {"total": d["amount"]})

    async def h(ctx: Any) -> Any:
        return ctx.event.data
    assert call(h, upcasters=r)["output"] == {"total": 7}


def test_broken_upcaster_chain_is_failed_outcome_not_raise() -> None:
    r = UpcasterRegistry()
    r.register("e", 1, 2, lambda d: {"total": d["amount"]})
    r.register("e", 3, 4, lambda d: d)  # gap: no 2 -> 3

    async def h(ctx: Any) -> Any:
        return 1
    out = call(h, upcasters=r)
    assert out["status"] == "failed" and out["error"]["retryable"] is False


def test_bad_event_timestamp_is_failed_outcome_not_raise() -> None:
    async def h(ctx: Any) -> Any:
        return 1
    out = call(h, raw_event={"id": "ev", "name": "e", "data": {}, "timestamp": "not-a-time"})
    assert out["status"] == "failed"


async def _metadata(ctx: Any) -> Any:
    return ctx.event.metadata


def test_event_metadata_reaches_the_handler() -> None:
    out = call(_metadata, raw_event={**EVENT, "metadata": {"traceId": "t-1"}})
    assert out == {"status": "completed", "output": {"traceId": "t-1"}}


# The engine serializes "no metadata" as JSON null; a hostile or older peer may send a non-object.
@pytest.mark.parametrize("raw", [
    pytest.param(EVENT, id="absent"),
    pytest.param({**EVENT, "metadata": None}, id="null"),
    pytest.param({**EVENT, "metadata": "x"}, id="string"),
    pytest.param({**EVENT, "metadata": [1]}, id="list"),
])
def test_event_metadata_absent_null_or_not_an_object_is_none(raw: dict[str, Any]) -> None:
    assert call(_metadata, raw_event=raw)["output"] is None


def test_event_metadata_survives_schema_validation() -> None:
    out = call(_metadata, raw_event={**EVENT, "metadata": {"k": 1}}, schema=lambda data: data)
    assert out["output"] == {"k": 1}
