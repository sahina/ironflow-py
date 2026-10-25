"""Retry policy over push: the run's budget, the retry delay on the wire, and when a failure compensates.

Converted from the coverage-audit repro test_cov_retry_sdk_python.py.
"""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from typing import Any

import pytest

from ironflow import IronflowError
from ironflow.serve._handler import handle
from ironflow.worker import NonRetryableError, RetryAfterError, function

EVENT = {"id": "ev", "name": "e", "data": {}, "version": 1, "timestamp": "2026-09-27T10:00:00Z"}


def post(fn: Any, **over: Any) -> dict[str, Any]:
    body = {"run_id": "run_1", "function_id": "fn", "attempt": 3, "max_attempts": 3,
            "on_exhausted": "fail", "event": EVENT, **over}
    body = {k: v for k, v in body.items() if v is not None}
    _, _, out = asyncio.run(handle([fn], method="POST", path="/", headers={}, body=json.dumps(body).encode(), signing_key=""))
    return json.loads(out)  # type: ignore[no-any-return]


def define(handler: Any) -> Any:
    return function(id="fn", triggers=[{"event": "e"}])(handler)


def test_handler_retry_delay_reaches_the_wire() -> None:
    async def h(ctx: Any) -> None:
        raise RetryAfterError("rate limited", 30)

    out = post(define(h), attempt=1)
    assert out["status"] == "failed"
    assert out["error"]["retryable"] is True
    assert out["error"]["retry_after_ms"] == 30_000


def test_retry_after_accepts_a_duration_string_and_rounds_up() -> None:
    async def h(ctx: Any) -> None:
        raise RetryAfterError("slow", "1500ms")

    assert post(define(h), attempt=1)["error"]["retry_after_ms"] == 1500

    async def tiny(ctx: Any) -> None:
        raise RetryAfterError("slow", 0.0004)

    assert post(define(tiny), attempt=1)["error"]["retry_after_ms"] == 1


def test_an_ironflow_error_with_a_server_retry_after_forwards_it() -> None:
    """A Retry-After the client read from a 429 survives the handler letting it propagate."""
    async def h(ctx: Any) -> None:
        raise IronflowError("rate limited", status_code=429, code="RATE_LIMITED", retryable=True, retry_after=2.5)

    assert post(define(h), attempt=1)["error"]["retry_after_ms"] == 2500


def test_retry_after_inside_a_step_body_survives_the_step_wrapper() -> None:
    async def h(ctx: Any) -> None:
        def call_api() -> None:
            raise RetryAfterError("429", "7s")

        await ctx.step.run("call-api", call_api)

    out = post(define(h), attempt=1)
    assert out["error"]["retryable"] is True
    assert out["error"]["retry_after_ms"] == 7000


@pytest.mark.parametrize("raise_", [
    lambda: RuntimeError("boom"),
    lambda: IronflowError("x", retryable=True),
    lambda: IronflowError("x", retryable=True, retry_after=0),
    lambda: IronflowError("x", retryable=False, retry_after=30),
    lambda: NonRetryableError("x"),
])
def test_no_delay_on_the_wire_when_none_is_asked_for(raise_: Any) -> None:
    async def h(ctx: Any) -> None:
        raise raise_()

    assert "retry_after_ms" not in post(define(h), attempt=1)["error"]


def test_retry_after_treats_a_negative_or_non_finite_delay_as_no_hint() -> None:
    # Matches Go and JS: a past reset time must not turn the retry into a crash.
    for bad in (-1, 0, float("nan"), float("inf"), timedelta(seconds=-5)):
        err = RetryAfterError("x", bad)
        assert err.retryable
        assert err.retry_after is None
    with pytest.raises(ValueError):
        RetryAfterError("x", "soon")


def test_run_info_exposes_the_retry_budget() -> None:
    seen: dict[str, Any] = {}

    async def h(ctx: Any) -> None:
        seen["run"] = ctx.run

    post(define(h))
    assert (seen["run"].attempt, seen["run"].max_attempts, seen["run"].on_exhausted) == (3, 3, "fail")


def test_run_info_defaults_when_the_engine_sends_neither() -> None:
    seen: dict[str, Any] = {}

    async def h(ctx: Any) -> None:
        seen["run"] = ctx.run

    post(define(h), max_attempts=None, on_exhausted=None)
    assert (seen["run"].max_attempts, seen["run"].on_exhausted) == (0, "")


def test_run_info_ignores_malformed_budget_fields() -> None:
    seen: dict[str, Any] = {}

    async def h(ctx: Any) -> None:
        seen["run"] = ctx.run

    post(define(h), max_attempts="3", on_exhausted="explode")
    assert (seen["run"].max_attempts, seen["run"].on_exhausted) == (0, "")


def run_compensating(raise_: Any, **over: Any) -> tuple[list[str], dict[str, Any]]:
    undone: list[str] = []

    async def h(ctx: Any) -> None:
        await ctx.step.run("charge", lambda: 1)
        ctx.step.compensate("charge", lambda: undone.append("charge"))
        raise raise_()

    return undone, post(define(h), **over)


@pytest.mark.parametrize(("on_exhausted", "attempt", "max_attempts", "compensates"), [
    ("fail", 3, 3, True),
    ("fail", 4, 3, True),
    ("fail", 2, 3, False),
    ("pause", 3, 3, False),
    ("", 3, 3, False),
    ("fail", 3, None, False),
])
def test_retryable_failure_compensates_only_when_it_exhausts_under_fail(
    on_exhausted: str, attempt: int, max_attempts: int | None, compensates: bool,
) -> None:
    undone, out = run_compensating(
        lambda: RuntimeError("temporary failure"),  # a plain exception is retryable
        on_exhausted=on_exhausted or None, attempt=attempt, max_attempts=max_attempts)
    assert out["error"]["retryable"] is True
    assert undone == (["charge"] if compensates else [])


@pytest.mark.parametrize("on_exhausted", ["pause", "fail", None])
def test_non_retryable_failure_always_compensates(on_exhausted: str | None) -> None:
    undone, _ = run_compensating(lambda: NonRetryableError("nope"), on_exhausted=on_exhausted, attempt=1)
    assert undone == ["charge"]


def test_retry_after_on_the_final_attempt_under_fail_compensates_and_still_reports_the_delay() -> None:
    undone, out = run_compensating(lambda: RetryAfterError("slow down", 5))
    assert undone == ["charge"]
    assert out["error"]["retry_after_ms"] == 5000
