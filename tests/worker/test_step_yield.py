from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import ironflow.worker._step as step_module
from ironflow.worker._duration import parse_timestamp
from ironflow.worker._step import Event, ExecutionContext, Step, _Yield
from tests.worker.conftest import run


def ctx_with(completed: dict[str, object] | None = None) -> ExecutionContext:
    return ExecutionContext(
        "run_1", [{"step_id": k, "name": k, "output": v} for k, v in (completed or {}).items()]
    )


def test_sleep_yields_with_until(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(_Yield) as info:
        run(loop, Step(ctx_with()).sleep("nap", "10m"))
    y = info.value.info
    assert y["step_id"] == "run_1:nap:0" and y["type"] == "sleep"
    assert set(y) == {"step_id", "type", "until"}
    delta = parse_timestamp(y["until"]) - datetime.now(timezone.utc)
    assert timedelta(minutes=9) < delta <= timedelta(minutes=10, milliseconds=1)


def test_sleep_yields_deadline_rounded_up(loop: asyncio.AbstractEventLoop, monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return datetime(2030, 1, 1, 0, 0, 0, 1, tzinfo=timezone.utc)

    monkeypatch.setattr(step_module, "datetime", FixedDatetime)
    with pytest.raises(_Yield) as info:
        run(loop, Step(ctx_with()).sleep("nap", 0.0001))
    assert info.value.info["until"] == "2030-01-01T00:00:00.001Z"


def test_sleep_memoized_returns(loop: asyncio.AbstractEventLoop) -> None:
    assert run(loop, Step(ctx_with({"run_1:nap:0": None})).sleep("nap", "10m")) is None


@pytest.mark.parametrize("bad", [0, -1, "0s"])
def test_sleep_rejects_non_positive(loop: asyncio.AbstractEventLoop, bad: object) -> None:
    with pytest.raises(ValueError):
        run(loop, Step(ctx_with()).sleep("nap", bad))  # type: ignore[arg-type]


def test_sleep_until_rejects_past_and_naive(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(ValueError):
        run(loop, Step(ctx_with()).sleep_until("t", datetime.now(timezone.utc) - timedelta(seconds=1)))
    with pytest.raises(ValueError):
        run(loop, Step(ctx_with()).sleep_until("t", datetime(2099, 1, 1, tzinfo=timezone.utc).replace(tzinfo=None)))


def test_sleep_until_yields_utc(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(_Yield) as info:
        run(loop, Step(ctx_with()).sleep_until("t", "2099-01-01T02:00:00+02:00"))
    assert info.value.info == {
        "step_id": "run_1:t:0", "type": "sleep", "until": "2099-01-01T00:00:00.000Z"
    }


def test_sleep_until_yields_deadline_rounded_up(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(_Yield) as info:
        run(loop, Step(ctx_with()).sleep_until("t", "2099-01-01T00:00:00.123001Z"))
    assert info.value.info["until"] == "2099-01-01T00:00:00.124Z"


def test_sleep_until_preserves_sub_microsecond_target(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(_Yield) as info:
        run(loop, Step(ctx_with()).sleep_until("t", "2099-01-01T00:00:00.123000001Z"))
    assert info.value.info["until"] == "2099-01-01T00:00:00.124Z"


def test_sleep_until_memoized_returns(loop: asyncio.AbstractEventLoop) -> None:
    assert run(loop, Step(ctx_with({"run_1:t:0": None})).sleep_until("t", "2000-01-01T00:00:00Z")) is None


def test_wait_for_event_yields_filter(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(_Yield) as info:
        run(loop, Step(ctx_with()).wait_for_event(
            "approval", event="order.approved", match="data.order_id", match_value="o-1",
            payload={"source": "checkout"}, timeout=3600,
        ))
    assert info.value.info == {
        "step_id": "run_1:approval:0", "type": "wait_for_event",
        "event_filter": {
            "event": "order.approved", "match": "data.order_id", "match_value": "o-1",
            "payload": {"source": "checkout"}, "timeout": "1h",
        },
    }


def test_wait_for_event_default_timeout(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(_Yield) as info:
        run(loop, Step(ctx_with()).wait_for_event("w", event="e"))
    assert info.value.info == {
        "step_id": "run_1:w:0", "type": "wait_for_event",
        "event_filter": {"event": "e", "timeout": "7d"},
    }


def test_wait_for_event_sub_millisecond_timeout(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(_Yield) as info:
        run(loop, Step(ctx_with()).wait_for_event("w", event="e", timeout=0.0001))
    assert info.value.info["event_filter"] == {"event": "e", "timeout": "1ms"}


def test_wait_for_event_memoized_returns_event(loop: asyncio.AbstractEventLoop) -> None:
    out = {"id": "ev_9", "name": "order.approved", "data": {"ok": True},
           "timestamp": "2026-09-24T10:00:00.123456789Z", "idempotencyKey": "k1"}
    ev = run(loop, Step(ctx_with({"run_1:approval:0": out})).wait_for_event("approval", event="order.approved"))
    assert isinstance(ev, Event)
    assert ev.id == "ev_9" and ev.data == {"ok": True} and ev.idempotency_key == "k1"
    assert ev.timestamp.tzinfo is not None


def test_wait_for_event_rejects_empty_event(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(ValueError):
        run(loop, Step(ctx_with()).wait_for_event("w", event="  "))


@pytest.mark.parametrize("bad", [0, -1, "0s"])
def test_wait_for_event_rejects_non_positive_timeout(loop: asyncio.AbstractEventLoop, bad: object) -> None:
    with pytest.raises(ValueError):
        run(loop, Step(ctx_with()).wait_for_event("w", event="e", timeout=bad))  # type: ignore[arg-type]
