"""Retry policy on the pull workers: the job's budget, the retry delay, and compensation on exhaustion."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from protobuf.wkt import Struct, Timestamp

from ironflow._gen.types_pb import Event
from ironflow._gen.worker_pb import JobAssignment, JobFailed
from ironflow.worker import (
    NonRetryableError,
    RetryAfterError,
    StreamingWorker,
    function,
)
from ironflow.worker._protocol import parse_job
from ironflow.worker._streaming import _job_from_proto
from tests.worker.conftest import run
from tests.worker.fake_engine import FakeEngine, make_job
from tests.worker.test_worker_jobs import terminal, worker_for
from tests.worker.test_worker_loop import run_until


def compensating(raise_: Any, undone: list[str]) -> Any:
    async def h(ctx: Any) -> None:
        await ctx.step.run("charge", lambda: 1)
        ctx.step.compensate("charge", lambda: undone.append("charge"))
        raise raise_()

    return h


class TestPollingWorker:
    def test_run_info_reads_the_budget_from_the_job(self, loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
        seen: dict[str, Any] = {}

        async def h(ctx: Any) -> None:
            seen["run"] = ctx.run

        engine.enqueue(make_job(attempt=2, max_attempts=5, on_exhausted="pause"))
        run(loop, run_until(worker_for(engine, h), lambda: engine.calls("terminal")))
        assert (seen["run"].attempt, seen["run"].max_attempts, seen["run"].on_exhausted) == (2, 5, "pause")

    def test_run_info_defaults_for_an_older_engine(self, loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
        seen: dict[str, Any] = {}

        async def h(ctx: Any) -> None:
            seen["run"] = ctx.run

        engine.enqueue(make_job())
        run(loop, run_until(worker_for(engine, h), lambda: engine.calls("terminal")))
        assert (seen["run"].max_attempts, seen["run"].on_exhausted) == (0, "")

    def test_failed_job_carries_retry_after_ms(self, loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
        async def h(ctx: Any) -> None:
            raise RetryAfterError("429", "12s")

        engine.enqueue(make_job(max_attempts=9, on_exhausted="pause"))
        run(loop, run_until(worker_for(engine, h), lambda: engine.calls("terminal")))
        error = terminal(engine)["error"]
        assert error["retryable"] is True and error["retry_after_ms"] == 12_000

    def test_failed_job_without_a_delay_has_no_retry_after_ms(
        self, loop: asyncio.AbstractEventLoop, engine: FakeEngine,
    ) -> None:
        async def h(ctx: Any) -> None:
            raise RuntimeError("boom")

        engine.enqueue(make_job())
        run(loop, run_until(worker_for(engine, h), lambda: engine.calls("terminal")))
        assert "retry_after_ms" not in terminal(engine)["error"]

    @pytest.mark.parametrize(("on_exhausted", "compensates"), [("fail", True), ("pause", False), ("", False)])
    def test_retryable_failure_on_the_last_attempt(
        self, loop: asyncio.AbstractEventLoop, engine: FakeEngine, on_exhausted: str, compensates: bool,
    ) -> None:
        undone: list[str] = []
        job = make_job(attempt=3, max_attempts=3)
        if on_exhausted:
            job["on_exhausted"] = on_exhausted
        engine.enqueue(job)
        run(loop, run_until(worker_for(engine, compensating(lambda: RuntimeError("temporary"), undone)),
                            lambda: engine.calls("terminal")))
        body = terminal(engine)
        assert body["error"]["retryable"] is True
        assert undone == (["charge"] if compensates else [])
        reported = [s["type"] for c in engine.calls("progress") for s in c["body"]["steps"]] + [
            s["type"] for s in body["steps"]]
        assert ("compensate" in reported) is compensates

    def test_retryable_failure_before_the_last_attempt_does_not_compensate(
        self, loop: asyncio.AbstractEventLoop, engine: FakeEngine,
    ) -> None:
        undone: list[str] = []
        engine.enqueue(make_job(attempt=2, max_attempts=3, on_exhausted="fail"))
        run(loop, run_until(worker_for(engine, compensating(lambda: RuntimeError("temporary"), undone)),
                            lambda: engine.calls("terminal")))
        assert undone == []

    def test_non_retryable_failure_compensates_under_pause(
        self, loop: asyncio.AbstractEventLoop, engine: FakeEngine,
    ) -> None:
        undone: list[str] = []
        engine.enqueue(make_job(attempt=1, max_attempts=3, on_exhausted="pause"))
        run(loop, run_until(worker_for(engine, compensating(lambda: NonRetryableError("no"), undone)),
                            lambda: engine.calls("terminal")))
        assert undone == ["charge"]


def test_parse_job_rejects_a_malformed_budget() -> None:
    base = make_job()
    assert parse_job({**base, "max_attempts": 4, "on_exhausted": "fail"})["max_attempts"] == 4
    with pytest.raises(ValueError, match="max_attempts"):
        parse_job({**base, "max_attempts": "4"})
    with pytest.raises(ValueError, match="on_exhausted"):
        parse_job({**base, "on_exhausted": 1})


def _proto_job(**kw: Any) -> JobAssignment:
    return JobAssignment(
        job_id="job-1", run_id="run-1", function_id="fn", attempt=3, execution_seq=1, lease_token="l",
        event=Event(id="e1", name="e", data=Struct.from_python({}), timestamp=Timestamp(seconds=1_790_000_000)),
        **kw,
    )


def test_streaming_job_maps_the_budget_from_the_proto() -> None:
    job = _job_from_proto(_proto_job(max_attempts=5, on_exhausted="fail"))
    assert (job["max_attempts"], job["on_exhausted"]) == (5, "fail")
    old = _job_from_proto(_proto_job())
    assert "max_attempts" not in old and "on_exhausted" not in old


class TestStreamingWorker:
    @staticmethod
    def execute(loop: asyncio.AbstractEventLoop, handler: Any, **job: Any) -> list[Any]:
        fn = function(id="fn", triggers=[{"event": "e"}])(handler)

        async def scenario() -> list[Any]:
            worker = StreamingWorker(functions=[fn], worker_id="w")
            worker.state = "connected"
            worker._draining = asyncio.Event()
            await worker._handle_job_assignment(_proto_job(**job))
            deadline = asyncio.get_running_loop().time() + 1
            while worker._jobs and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0)
            return list(worker._outbox)

        return run(loop, scenario())

    def test_run_info_reads_the_budget(self, loop: asyncio.AbstractEventLoop) -> None:
        seen: dict[str, Any] = {}

        async def h(ctx: Any) -> None:
            seen["run"] = ctx.run

        self.execute(loop, h, max_attempts=5, on_exhausted="pause")
        assert (seen["run"].attempt, seen["run"].max_attempts, seen["run"].on_exhausted) == (3, 5, "pause")

    def test_failed_job_carries_retry_after_ms(self, loop: asyncio.AbstractEventLoop) -> None:
        async def h(ctx: Any) -> None:
            raise RetryAfterError("429", 12)

        failed = self.execute(loop, h, max_attempts=9, on_exhausted="pause")[-1].payload.value
        assert isinstance(failed, JobFailed)
        assert failed.error.retryable is True and failed.error.retry_after_ms == 12_000

    def test_failed_job_without_a_delay_sends_zero(self, loop: asyncio.AbstractEventLoop) -> None:
        async def h(ctx: Any) -> None:
            raise RuntimeError("boom")

        failed = self.execute(loop, h)[-1].payload.value
        assert failed.error.retry_after_ms == 0

    @pytest.mark.parametrize(("job", "compensates"), [
        ({"max_attempts": 3, "on_exhausted": "fail"}, True),
        ({"max_attempts": 3, "on_exhausted": "pause"}, False),
        ({}, False),
    ])
    def test_plain_exception_on_the_last_attempt(
        self, loop: asyncio.AbstractEventLoop, job: dict[str, Any], compensates: bool,
    ) -> None:
        undone: list[str] = []
        failed = self.execute(loop, compensating(lambda: RuntimeError("temporary"), undone), **job)[-1].payload.value
        assert isinstance(failed, JobFailed)
        assert failed.error.retryable is True
        assert undone == (["charge"] if compensates else [])
        assert [s.type for s in failed.steps] == (["compensate"] if compensates else [])

    def test_retry_after_error_on_the_last_attempt_under_fail_compensates(
        self, loop: asyncio.AbstractEventLoop,
    ) -> None:
        undone: list[str] = []
        failed = self.execute(
            loop, compensating(lambda: RetryAfterError("slow", 5), undone), max_attempts=3, on_exhausted="fail",
        )[-1].payload.value
        assert undone == ["charge"]
        assert failed.error.retry_after_ms == 5000
