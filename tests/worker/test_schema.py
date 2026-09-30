from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest

from ironflow.worker._function import function, validate_event
from ironflow.worker._step import Event, SchemaValidationError
from tests.worker.conftest import run
from tests.worker.fake_engine import FakeEngine, make_job
from tests.worker.test_worker_jobs import terminal
from tests.worker.test_worker_loop import fast_worker, run_until


def order(data: Any) -> dict[str, int]:
    if not isinstance(data, dict) or not isinstance(data.get("qty"), int):
        raise TypeError("qty must be an int")
    return {"qty": data["qty"], "parsed": 1}


async def async_order(data: Any) -> dict[str, int]:
    return order(data)


async def noop(ctx: Any) -> None: ...


def event(data: Any, source: str | None = None) -> Event:
    return Event(id="ev", name="order.placed", data=data, timestamp=datetime.now(timezone.utc), source=source)


@pytest.mark.parametrize("schema", [order, async_order])
def test_schema_result_replaces_event_data(loop: asyncio.AbstractEventLoop, schema: Any) -> None:
    fn = function(id="f", triggers=[{"event": "e"}], schema=schema)(noop)
    assert run(loop, validate_event(fn, event({"qty": 2}))).data == {"qty": 2, "parsed": 1}


def test_schema_failure_is_non_retryable(loop: asyncio.AbstractEventLoop) -> None:
    fn = function(id="f", triggers=[{"event": "e"}], schema=order)(noop)
    with pytest.raises(SchemaValidationError, match="qty must be an int") as exc:
        run(loop, validate_event(fn, event({"qty": "x"})))
    assert exc.value.retryable is False and exc.value.code == "VALIDATION_ERROR"


def test_cron_events_skip_the_schema(loop: asyncio.AbstractEventLoop) -> None:
    fn = function(id="f", triggers=[{"cron": "* * * * *"}], schema=order)(noop)
    ev = event({}, source="cron")
    assert run(loop, validate_event(fn, ev)) is ev


def test_redacted_payload_fails(loop: asyncio.AbstractEventLoop) -> None:
    fn = function(id="f", triggers=[{"event": "e"}], schema=lambda d: d)(noop)
    with pytest.raises(SchemaValidationError, match="redacted"):
        run(loop, validate_event(fn, event({"$redacted": True})))


def test_no_schema_passes_through(loop: asyncio.AbstractEventLoop) -> None:
    fn = function(id="f", triggers=[{"event": "e"}])(noop)
    ev = event({"$redacted": True})
    assert run(loop, validate_event(fn, ev)) is ev


def test_worker_applies_schema_before_handler(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> Any:
        return ctx.event.data

    w = fast_worker(engine)
    w._functions = {"fn": function(id="fn", triggers=[{"event": "e"}], schema=order)(h)}
    engine.enqueue(make_job(event={"id": "ev", "name": "e", "data": {"qty": 3}, "timestamp": "2026-09-24T10:00:00Z"}))
    run(loop, run_until(w, lambda: engine.calls("terminal")))
    assert terminal(engine)["output"] == {"qty": 3, "parsed": 1}


def test_worker_reports_schema_failure_without_retry(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    called: list[int] = []

    async def h(ctx: Any) -> None:
        called.append(1)

    w = fast_worker(engine)
    w._functions = {"fn": function(id="fn", triggers=[{"event": "e"}], schema=order)(h)}
    engine.enqueue(make_job(event={"id": "ev", "name": "e", "data": {}, "timestamp": "2026-09-24T10:00:00Z"}))
    run(loop, run_until(w, lambda: engine.calls("terminal")))
    body = terminal(engine)
    assert body["status"] == "failed" and called == []
    assert body["error"]["code"] == "VALIDATION_ERROR" and body["error"]["retryable"] is False
