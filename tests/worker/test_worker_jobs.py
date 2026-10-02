from __future__ import annotations

import asyncio
from typing import Any

import pytest

from ironflow._http import IronflowError
from ironflow.worker._function import function
from ironflow.worker._step import NonRetryableError
from ironflow.worker._worker import Worker, WorkerAuthError
from tests.worker.conftest import run
from tests.worker.fake_engine import FakeEngine, make_job
from tests.worker.test_worker_loop import fast_worker, run_until, until


def worker_for(engine: FakeEngine, handler: Any, **kw: Any) -> Worker:
    w = fast_worker(engine, **kw)
    fn = function(id="fn", triggers=[{"event": "e"}])(handler)
    w._functions = {"fn": fn}
    return w


def terminal(engine: FakeEngine) -> dict[str, Any]:
    return engine.calls("terminal")[0]["body"]


def test_completed_job_acks_then_reports(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> Any:
        a = await ctx.step.run("a", lambda: 1)
        return {"a": a, "secret": ctx.secrets["K"], "event": ctx.event.data}

    engine.enqueue(make_job(context={"secrets": {"K": "v"}}, event={
        "id": "ev", "name": "e", "data": {"x": 1}, "timestamp": "2026-09-24T10:00:00.123456789Z"}))
    run(loop, run_until(worker_for(engine, h), lambda: engine.calls("terminal")))
    routes = [r["route"] for r in engine.requests if r["route"] in ("ack", "terminal")]
    assert routes == ["ack", "terminal"]
    assert engine.calls("ack")[0]["body"] == {"run_id": "run_1", "execution_seq": 1, "lease_token": "tok"}
    body = terminal(engine)
    assert body["status"] == "completed" and body["output"] == {"a": 1, "secret": "v", "event": {"x": 1}}
    assert body["execution_seq"] == 1 and body["lease_token"] == "tok"
    assert sum(len(c["body"]["steps"]) for c in engine.calls("progress")) + len(body["steps"]) == 1


def test_yield_reports_unflushed_tail(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> None:
        for i in range(3):
            await ctx.step.run(f"s{i}", lambda i=i: i)
        await ctx.step.sleep("nap", "1h")

    engine.enqueue(make_job(step_sequence_base=4))
    run(loop, run_until(worker_for(engine, h, checkpoint_interval=0), lambda: engine.calls("terminal")))
    body = terminal(engine)
    assert body["status"] == "yielded" and body["yield"]["type"] == "sleep"
    assert [s["id"] for s in body["steps"]] == ["run_1:s0:0", "run_1:s1:0", "run_1:s2:0"]
    assert body["step_offset"] == 4


def test_memoized_steps_do_not_rerun(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    calls: list[int] = []

    async def h(ctx: Any) -> str:
        await ctx.step.run("a", lambda: calls.append(1))
        await ctx.step.sleep("nap", "1h")
        return "woke"

    engine.enqueue(make_job(completed_steps=[
        {"step_id": "run_1:a:0", "name": "run_1:a:0", "output": None},
        {"step_id": "run_1:nap:0", "name": "run_1:nap:0", "output": None}]))
    run(loop, run_until(worker_for(engine, h), lambda: engine.calls("terminal")))
    assert calls == [] and terminal(engine)["output"] == "woke"


def test_error_mapping(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def plain(ctx: Any) -> None:
        raise RuntimeError("boom")

    async def fatal(ctx: Any) -> None:
        raise NonRetryableError("no", code="CARD_DECLINED")

    async def bad_output(ctx: Any) -> Any:
        return {"score": [1.0, float("nan")]}  # nested NaN: the server's decoder rejects it

    expected = [
        (plain, {"message": "boom", "code": "ERROR", "retryable": True}),
        (fatal, {"message": "no", "code": "CARD_DECLINED", "retryable": False}),
        (bad_output, {"code": "SERIALIZATION_ERROR", "retryable": False}),
    ]
    for handler, err in expected:
        e = FakeEngine()
        try:
            e.enqueue(make_job())
            run(loop, run_until(worker_for(e, handler), lambda e=e: e.calls("terminal")))
            body = e.calls("terminal")[0]["body"]
            assert body["status"] == "failed"
            assert {k: body["error"][k] for k in err} == err
        finally:
            e.shutdown()


def test_non_retryable_failure_runs_compensations(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    undone: list[str] = []

    async def h(ctx: Any) -> None:
        await ctx.step.run("a", lambda: 1)
        ctx.step.compensate("a", lambda: undone.append("a"))
        raise NonRetryableError("stop")

    engine.enqueue(make_job())
    run(loop, run_until(worker_for(engine, h), lambda: engine.calls("terminal")))
    body = terminal(engine)
    assert body["status"] == "failed"
    compensations = [s for s in body["steps"] if s["type"] == "compensate"]
    assert len(compensations) == 1
    assert compensations[0]["compensation_for"] == "a"
    assert compensations[0]["status"] == "completed"
    assert undone == ["a"]


def test_retryable_failure_runs_no_compensations(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    undone: list[str] = []

    async def h(ctx: Any) -> None:
        await ctx.step.run("a", lambda: 1)
        ctx.step.compensate("a", lambda: undone.append("a"))
        raise RuntimeError("boom")

    engine.enqueue(make_job())
    run(loop, run_until(worker_for(engine, h), lambda: engine.calls("terminal")))
    body = terminal(engine)
    assert body["status"] == "failed"
    assert all(s["type"] != "compensate" for s in body["steps"])
    assert undone == []


def test_retryable_ironflow_error_runs_no_compensations(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    """A retryable IronflowError hits the ``if not exc.retryable`` gate itself,
    unlike a plain RuntimeError, which never reaches it."""
    undone: list[str] = []

    async def h(ctx: Any) -> None:
        await ctx.step.run("a", lambda: 1)
        ctx.step.compensate("a", lambda: undone.append("a"))
        raise IronflowError("transient", retryable=True)

    engine.enqueue(make_job())
    run(loop, run_until(worker_for(engine, h), lambda: engine.calls("terminal")))
    body = terminal(engine)
    assert body["status"] == "failed"
    assert all(s["type"] != "compensate" for s in body["steps"])
    assert undone == []


def test_unencodable_yield_fails_non_retryable(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> None:
        await ctx.step.wait_for_event("reply", event="reply", payload={"score": float("nan")})

    engine.enqueue(make_job())
    run(loop, run_until(worker_for(engine, h), lambda: engine.calls("terminal")))
    assert terminal(engine)["status"] == "failed"
    assert terminal(engine)["error"]["code"] == "SERIALIZATION_ERROR"
    assert terminal(engine)["error"]["retryable"] is False


def test_invalid_event_timestamp_reports_failure(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> None:
        raise AssertionError("handler should not run")

    engine.enqueue(make_job(event={"id": "ev", "name": "e", "data": {}, "timestamp": "invalid"}))
    run(loop, run_until(worker_for(engine, h), lambda: engine.calls("terminal")))
    assert terminal(engine)["status"] == "failed"


def test_unknown_function_fails_non_retryable(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> None: ...

    engine.enqueue(make_job(function_id="ghost"))
    run(loop, run_until(worker_for(engine, h), lambda: engine.calls("terminal")))
    assert terminal(engine)["error"]["code"] == "FUNCTION_NOT_FOUND"
    assert engine.calls("ack") == []


def test_failed_ack_drops_job(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    ran: list[int] = []

    async def h(ctx: Any) -> None:
        ran.append(1)

    engine.fail("ack", 409, {"error": "STALE_EXECUTION", "message": "x"})
    engine.enqueue(make_job())
    w = worker_for(engine, h)
    run(loop, run_until(w, lambda: engine.calls("ack") and not w._jobs))
    assert ran == [] and engine.calls("terminal") == []


def test_duplicate_assignment_does_not_replace_owned_job(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    gate = asyncio.Event()

    async def h(ctx: Any) -> None:
        await gate.wait()

    engine.enqueue(make_job(), make_job())
    w = worker_for(engine, h, max_concurrent_jobs=2)

    async def scenario() -> None:
        task = asyncio.ensure_future(w.start())
        try:
            await until(lambda: engine.calls("ack"))
            await asyncio.sleep(0.05)
            assert len(engine.calls("ack")) == 1
        finally:
            gate.set()
            await w.stop()
            await task

    run(loop, scenario())


def test_stale_report_is_not_retried(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> int:
        return 1

    engine.fail("terminal", 409, {"error": "STALE_EXECUTION", "message": "x"})
    engine.enqueue(make_job())
    w = worker_for(engine, h)
    run(loop, run_until(w, lambda: engine.calls("terminal") and not w._jobs))
    assert len(engine.calls("terminal")) == 1


def test_report_401_stops_worker_and_cancels_other_job(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    import pytest

    cancelled: list[int] = []
    finish = asyncio.Event()

    async def h(ctx: Any) -> int:
        if ctx.run.id == "run_1":
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                cancelled.append(1)
                raise
        await finish.wait()
        return 2

    engine.enqueue(make_job("run_1"), make_job("run_2"))
    engine.fail("terminal", 401, {"error": "unauthorized"})
    w = worker_for(engine, h, max_concurrent_jobs=2)

    async def scenario() -> None:
        task = asyncio.ensure_future(w.start())
        await until(lambda: len(engine.calls("ack")) == 2)
        finish.set()
        with pytest.raises(WorkerAuthError):
            await asyncio.wait_for(task, 5)

    run(loop, scenario())
    assert cancelled == [1] and w.state == "stopped"


def test_report_retries_5xx_with_heartbeat_listing_job(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> int:
        return 1

    engine.fail("terminal", 503, {"error": "busy"}, times=3)
    engine.enqueue(make_job())
    w = worker_for(engine, h)
    w._report_delays = (0.05, 0.05, 0.05, 0.05)
    run(loop, run_until(w, lambda: len(engine.calls("terminal")) == 4))
    listed = [c["body"]["jobs"] for c in engine.calls("heartbeat")]
    assert any(jobs and jobs[0]["job_id"] == "run_1" for jobs in listed)


def test_checkpoint_stale_abandons_running_job(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> None:
        await ctx.step.run("a", lambda: 1)
        await asyncio.sleep(10)

    engine.fail("progress", 409, {"error": "STALE_EXECUTION", "message": "x"})
    engine.enqueue(make_job())
    w = worker_for(engine, h)

    async def scenario() -> None:
        task = asyncio.ensure_future(w.start())
        await until(lambda: engine.calls("progress") and not w._jobs)
        job_calls = [r for r in engine.requests if "/jobs/run_1" in r["path"]]
        await asyncio.sleep(0.2)  # a leaked checkpoint timer or report would show up here
        assert [r for r in engine.requests if "/jobs/run_1" in r["path"]] == job_calls
        await w.stop()
        await task

    run(loop, scenario())
    assert engine.calls("terminal") == []


def test_heartbeat_401_with_all_slots_busy_stops_worker(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    import pytest

    from ironflow.worker._worker import WorkerAuthError

    cancelled: list[int] = []

    async def h(ctx: Any) -> None:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.append(1)
            raise

    engine.enqueue(make_job())
    w = worker_for(engine, h, max_concurrent_jobs=1)

    async def scenario() -> None:
        task = asyncio.ensure_future(w.start())
        await until(lambda: engine.calls("ack"))
        engine.fail("heartbeat", 401, {"error": "unauthorized"}, times=100)
        with pytest.raises(WorkerAuthError):
            await asyncio.wait_for(task, 5)

    run(loop, scenario())
    assert cancelled == [1] and engine.calls("terminal") == []
    assert w.state == "stopped"


def test_heartbeat_404_abandons_job_and_registers_again(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    cancelled: list[int] = []
    started: list[int] = []

    async def h(ctx: Any) -> None:
        started.append(1)
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.append(1)
            raise

    engine.enqueue(make_job())
    worker = worker_for(engine, h, max_concurrent_jobs=1)

    async def scenario() -> None:
        task = asyncio.ensure_future(worker.start())
        try:
            # Arm the 404 only once the handler runs: a heartbeat that lands
            # while the ack is in flight cancels the job before it starts, and
            # `cancelled` would never be set.
            await until(lambda: started)
            armed_at = len(engine.calls("heartbeat"))
            engine.fail("heartbeat", 404, {"error": "worker not registered"})
            await until(lambda: cancelled and len(engine.calls("register")) >= 2)
            # armed_at + 1 is the 404; the next one is after re-registration.
            await until(lambda: len(engine.calls("heartbeat")) >= armed_at + 2)
            assert engine.calls("heartbeat")[-1]["body"]["jobs"] == []
        finally:
            await worker.stop()
            await task

    run(loop, scenario())
    assert cancelled == [1] and engine.calls("terminal") == []


def test_broad_except_in_handler_does_not_swallow_yield(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> str:
        try:
            await ctx.step.sleep("nap", "1h")
        except Exception:  # noqa: BLE001 - verifies user catch-all cannot swallow worker yield
            return "swallowed"
        return "unreachable"

    engine.enqueue(make_job())
    run(loop, run_until(worker_for(engine, h), lambda: engine.calls("terminal")))
    assert terminal(engine)["status"] == "yielded"


def test_event_metadata_reaches_the_handler(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> Any:
        return ctx.event.metadata

    engine.enqueue(make_job(event={
        "id": "ev", "name": "e", "data": {}, "timestamp": "2026-09-24T10:00:00Z", "metadata": {"traceId": "t-1"}}))
    run(loop, run_until(worker_for(engine, h), lambda: engine.calls("terminal")))
    assert terminal(engine)["output"] == {"traceId": "t-1"}


def test_publish_step_posts_with_the_run_id(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> Any:
        return (await ctx.step.publish("orders", {"a": 1}, idempotency_key="k1")).event_id

    engine.enqueue(make_job())
    run(loop, run_until(worker_for(engine, h), lambda: engine.calls("terminal")))
    call = engine.calls("publish")[0]
    assert call["body"] == {"topic": "orders", "data": {"a": 1}, "idempotencyKey": "k1"}
    assert {k.lower(): v for k, v in call["headers"].items()}["x-ironflow-run-id"] == "run_1"
    assert terminal(engine)["output"] == "evt_pub_1"


@pytest.mark.parametrize("configured,env_var,want", [
    ("staging", "qa", "staging"),
    (None, "qa", "qa"),
    (None, None, "default"),
])
def test_run_info_carries_the_worker_environment(
    loop: asyncio.AbstractEventLoop, engine: FakeEngine, monkeypatch: pytest.MonkeyPatch,
    configured: str | None, env_var: str | None, want: str,
) -> None:
    """#2471: the run's outbound calls reuse the environment the worker polls with."""
    if env_var is None:
        monkeypatch.delenv("IRONFLOW_ENV", raising=False)
    else:
        monkeypatch.setenv("IRONFLOW_ENV", env_var)

    async def h(ctx: Any) -> Any:
        return ctx.run.environment

    engine.enqueue(make_job())
    run(loop, run_until(worker_for(engine, h, environment=configured), lambda: engine.calls("terminal")))
    assert terminal(engine)["output"] == want
