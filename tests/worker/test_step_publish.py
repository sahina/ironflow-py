from __future__ import annotations

from typing import Any

import pytest

from ironflow.worker._step import (
    ExecutionContext,
    NonRetryableError,
    PublishResult,
    Step,
    StepError,
)
from tests.worker.conftest import run


def ctx_with(rows: list[dict[str, Any]] | None = None, publish: Any = None) -> ExecutionContext:
    ctx = ExecutionContext("run_1", rows or [])  # type: ignore[arg-type]
    if publish is not None:
        ctx.publish = publish
    return ctx


def recorder() -> tuple[list[tuple[str, Any, str | None]], Any]:
    sent: list[tuple[str, Any, str | None]] = []

    async def publish(topic: str, data: Any, key: str | None) -> dict[str, Any]:
        sent.append((topic, data, key))
        return {"eventId": f"evt_{len(sent)}", "sequence": 9}

    return sent, publish


def test_publish_sends_once_and_returns_the_result(loop) -> None:
    sent, publish = recorder()
    ctx = ctx_with(publish=publish)
    result = run(loop, Step(ctx).publish("orders", {"a": 1}, idempotency_key="k"))
    assert result == PublishResult(event_id="evt_1", sequence=9)
    assert sent == [("orders", {"a": 1}, "k")]
    step = ctx.executed[0]
    assert step["id"] == "run_1:publish:orders:0"  # the reserved publish: namespace, same id as Node and Go
    assert step["output"] == {"eventId": "evt_1", "sequence": 9}


def test_publish_replay_returns_the_memoized_result_and_never_sends(loop) -> None:
    sent, publish = recorder()
    rows = [{"step_id": "run_1:publish:orders:0", "name": "publish:orders",
             "output": {"eventId": "evt_old", "sequence": 3}}]
    result = run(loop, Step(ctx_with(rows, publish)).publish("orders", {"a": 1}))
    assert result == PublishResult(event_id="evt_old", sequence=3) and sent == []


def test_same_topic_twice_gets_distinct_step_ids(loop) -> None:
    _, publish = recorder()
    ctx = ctx_with(publish=publish)
    step = Step(ctx)
    run(loop, step.publish("orders", 1))
    run(loop, step.publish("orders", 2))
    assert [s["id"] for s in ctx.executed] == ["run_1:publish:orders:0", "run_1:publish:orders:1"]


def test_publish_inside_parallel_branches_uses_branch_scoped_ids(loop) -> None:
    _, publish = recorder()
    ctx = ctx_with(publish=publish)
    run(loop, Step(ctx).parallel("fan", [lambda s: s.publish("t", 1), lambda s: s.publish("t", 2)]))
    assert sorted(s["id"] for s in ctx.executed) == ["run_1:fan:0:publish:t:0", "run_1:fan:1:publish:t:0"]


def test_publish_without_a_configured_server_fails_and_is_not_retryable(loop) -> None:
    with pytest.raises(StepError) as e:
        run(loop, Step(ctx_with()).publish("orders"))
    assert e.value.retryable is False and "server URL not configured" in str(e.value)


def test_publish_rejects_data_that_is_not_json_before_sending(loop) -> None:
    sent, publish = recorder()
    with pytest.raises(NonRetryableError) as e:
        run(loop, Step(ctx_with(publish=publish)).publish("orders", {"x": float("nan")}))
    assert e.value.code == "SERIALIZATION_ERROR" and sent == []


def test_publish_needs_a_topic(loop) -> None:
    with pytest.raises(ValueError, match="topic"):
        run(loop, Step(ctx_with()).publish(""))


def test_a_memoized_publish_row_without_an_event_id_is_a_clear_error(loop) -> None:
    rows = [{"step_id": "run_1:publish:orders:0", "name": "publish:orders", "output": {"sequence": 3}}]
    with pytest.raises(NonRetryableError, match="eventId"):
        run(loop, Step(ctx_with(rows)).publish("orders"))


def test_publish_replay_needs_no_server_url(loop) -> None:
    # A bare context keeps the default publisher that raises; a completed row must never reach it.
    rows = [{"step_id": "run_1:publish:orders:0", "name": "publish:orders",
             "output": {"eventId": "evt_old", "sequence": 3}}]
    result = run(loop, Step(ctx_with(rows)).publish("orders", {"a": 1}))
    assert result == PublishResult(event_id="evt_old", sequence=3)
