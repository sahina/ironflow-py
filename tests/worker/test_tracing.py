from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry.trace import StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from protobuf.wkt import Timestamp

from ironflow._gen.types_pb import Event
from ironflow._gen.worker_pb import JobAssignment, JobContext
from ironflow.serve._handler import handle
from ironflow.worker import StreamingWorker, function
from ironflow.worker._step import NonRetryableError
from ironflow.worker._streaming import _job_from_proto
from ironflow.worker._worker import _ActiveJob
from tests.worker.fake_engine import FakeEngine, make_job
from tests.worker.test_worker_jobs import worker_for
from tests.worker.test_worker_loop import run_until

TRACE_ID = "1234567890abcdef1234567890abcdef"
PARENT_ID = "1234567890abcdef"
CARRIER = {"traceparent": f"00-{TRACE_ID}-{PARENT_ID}-01", "tracestate": "vendor=value"}


@pytest.fixture(scope="session")
def providers() -> Any:
    active: list[TracerProvider] = []
    yield active
    # pyqwest caches tracers beyond the lifetime of individual test clients.
    for provider in active:
        provider.shutdown()


@pytest.fixture()
def spans(monkeypatch: pytest.MonkeyPatch, providers: list[TracerProvider]) -> Any:
    exporter = InMemorySpanExporter()
    provider = TracerProvider(sampler=ALWAYS_ON)
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(trace, "_TRACER_PROVIDER", provider)
    providers.append(provider)
    yield exporter


async def execute(mode: str, handler: Any, carrier: Any, memo: bool = False) -> Any:
    fn = function(id="fn", triggers=[{"event": "e"}])(handler)
    completed = [{"step_id": "run_1:work:0", "name": "work", "output": 7}] if memo else []
    if mode == "push":
        return await handle([fn], method="POST", path="/", headers=carrier, body=json.dumps({
            "run_id": "run_1", "function_id": "fn", "event": {"id": "ev", "name": "e", "data": {}, "timestamp": "2026-10-31T00:00:00Z"},
            "steps": [{"id": s["step_id"], "output": s["output"]} for s in completed],
        }).encode())
    if mode == "stream":
        worker = StreamingWorker(functions=[fn])
        job = _job_from_proto(JobAssignment(
            job_id="j", run_id="run_1", function_id="fn", attempt=1,
            event=Event(id="ev", name="e", timestamp=Timestamp(seconds=1)), context=JobContext(**carrier),
            execution_seq=1, lease_token="token",
        ))
        job["completed_steps"] = completed
        await worker._execute(_ActiveJob(job=job, started_at=""))
        return list(worker._outbox)
    engine = FakeEngine()
    try:
        engine.enqueue(make_job(context=carrier, completed_steps=completed))
        await run_until(worker_for(engine, handler), lambda: engine.calls("terminal"))
        return engine.calls("terminal")
    finally:
        engine.shutdown()


@pytest.mark.parametrize("mode", ["push", "poll", "stream"])
@pytest.mark.parametrize("memo", [False, True])
def test_job_parent_and_only_executed_steps(mode: str, memo: bool, spans: Any) -> None:
    async def handler(ctx: Any) -> Any:
        return await ctx.step.run("work", lambda: 7)

    with trace.get_tracer("test").start_as_current_span("unrelated-request"):
        asyncio.run(execute(mode, handler, CARRIER, memo))
    finished = spans.get_finished_spans()
    process = next(s for s in finished if s.name == "process")
    assert process.context.trace_id == int(TRACE_ID, 16)
    assert process.parent.span_id == int(PARENT_ID, 16)
    assert process.context.trace_state.get("vendor") == "value"
    steps = [s for s in finished if s.name == "step.run"]
    assert len(steps) == (0 if memo else 1)
    if steps:
        assert steps[0].parent.span_id == process.context.span_id
    assert all(s.end_time is not None for s in finished)
    assert process.status.status_code == StatusCode.UNSET


@pytest.mark.parametrize("mode", ["push", "poll", "stream"])
@pytest.mark.parametrize("behavior", ["error", "step-error", "yield", "cancel", "serialize", "timeout", "step-cancel", "bad-yield"])
def test_execution_status_and_lifecycle(mode: str, behavior: str, spans: Any) -> None:
    async def handler(ctx: Any) -> Any:
        if behavior == "bad-yield":
            return await ctx.step.invoke("other", {"invalid": float("nan")})
        if behavior == "yield":
            await ctx.step.sleep("nap", "1h")
        if behavior == "cancel":
            raise asyncio.CancelledError()
        if behavior == "step-cancel":
            async def cancel() -> None:
                raise asyncio.CancelledError()
            return await ctx.step.run("cancel", cancel)
        if behavior == "serialize":
            return {1, 2}
        if behavior == "timeout":
            return await ctx.step.run("slow", lambda: asyncio.sleep(1), timeout=0.001)
        def fail() -> None:
            raise NonRetryableError("credential-must-not-appear")
        if behavior == "step-error":
            return await ctx.step.run("work", fail)
        fail()

    if behavior in ("cancel", "step-cancel") and mode == "poll":
        async def scenario() -> None:
            engine = FakeEngine()
            try:
                job = make_job(context=CARRIER)
                await worker_for(engine, handler)._execute(_ActiveJob(job=job, started_at=""))
            finally:
                engine.shutdown()
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(scenario())
    elif behavior in ("cancel", "step-cancel"):
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(execute(mode, handler, CARRIER))
    else:
        asyncio.run(execute(mode, handler, CARRIER))
    finished = spans.get_finished_spans()
    process = next(s for s in finished if s.name == "process")
    expected = StatusCode.UNSET if behavior in ("yield", "cancel", "step-cancel") else StatusCode.ERROR
    assert process.status.status_code == expected
    assert all(s.end_time is not None for s in finished)
    if behavior in ("step-error", "timeout"):
        assert next(s for s in finished if s.name == "step.run").status.status_code == StatusCode.ERROR
    assert "credential-must-not-appear" not in repr([(s.attributes, s.events, s.status) for s in finished])


@pytest.mark.parametrize("carrier", [{}, {"traceparent": "malformed"}, {"traceparent": 123},
                                    {"traceparent": "00-" + "0" * 32 + "-" + PARENT_ID + "-01"}])
def test_bad_or_absent_parent_starts_root(carrier: Any, spans: Any) -> None:
    async def handler(ctx: Any) -> int:
        return 1
    with trace.get_tracer("test").start_as_current_span("ambient"):
        asyncio.run(execute("poll", handler, carrier))
    process = next(s for s in spans.get_finished_spans() if s.name == "process")
    assert process.parent is None


@pytest.mark.parametrize("state", ["bad state", "a=" + "x" * 513])
def test_invalid_state_keeps_parent(state: str, spans: Any) -> None:
    async def handler(ctx: Any) -> int:
        return 1
    asyncio.run(execute("poll", handler, {**CARRIER, "tracestate": state}))
    process = next(s for s in spans.get_finished_spans() if s.name == "process")
    assert process.parent.span_id == int(PARENT_ID, 16)
    assert not process.context.trace_state


@pytest.mark.parametrize("mode", ["push", "poll", "stream"])
@pytest.mark.parametrize("suffix", ["\n", "-", "-extension"])
def test_version_zero_parent_has_exact_grammar(mode: str, suffix: str, spans: Any) -> None:
    async def handler(ctx: Any) -> int:
        return 1
    asyncio.run(execute(mode, handler, {**CARRIER, "traceparent": CARRIER["traceparent"] + suffix}))
    process = next(s for s in spans.get_finished_spans() if s.name == "process")
    assert process.parent is None


@pytest.mark.parametrize("mode", ["push", "poll", "stream"])
@pytest.mark.parametrize("state", [
    "bad-secret-token=secret-value!\nFORGED",
    "vendor=secret-value,vendor=duplicate",
    "bad-secret-token=",
    ",".join(f"v{i}=secret-value" for i in range(33)),
])
def test_invalid_state_is_discarded_without_logging(
    mode: str, state: str, spans: Any, caplog: pytest.LogCaptureFixture,
) -> None:
    async def handler(ctx: Any) -> int:
        return 1
    asyncio.run(execute(mode, handler, {**CARRIER, "tracestate": state}))
    process = next(s for s in spans.get_finished_spans() if s.name == "process")
    assert process.parent.span_id == int(PARENT_ID, 16)
    assert not process.context.trace_state
    assert not [r for r in caplog.records if r.name.startswith("opentelemetry")]


@pytest.mark.parametrize("mode", ["push", "poll", "stream"])
@pytest.mark.parametrize("state, expected", [
    (" ,\tvendor=value,\tother=two \t, ", "vendor=value,other=two"),
    ("a=" + "x" * 256 + ",b=" + "y" * 251, "a=" + "x" * 256 + ",b=" + "y" * 251),
    (",".join(f"v{i}=x" for i in range(32)), ",".join(f"v{i}=x" for i in range(32))),
])
def test_valid_state_survives_validation(mode: str, state: str, expected: str, spans: Any) -> None:
    async def handler(ctx: Any) -> int:
        return 1
    asyncio.run(execute(mode, handler, {**CARRIER, "tracestate": state}))
    process = next(s for s in spans.get_finished_spans() if s.name == "process")
    assert process.parent.span_id == int(PARENT_ID, 16)
    assert process.context.trace_state.to_header() == expected


def test_unsampled_context_propagates_without_recording(
    monkeypatch: pytest.MonkeyPatch, providers: list[TracerProvider],
) -> None:
    from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, ParentBased
    provider = TracerProvider(sampler=ParentBased(ALWAYS_OFF))
    providers.append(provider)
    monkeypatch.setattr(trace, "_TRACER_PROVIDER", provider)
    seen: list[dict[str, str]] = []
    async def handler(ctx: Any) -> Any:
        async def body() -> int:
            fields: dict[str, str] = {}
            TraceContextTextMapPropagator().inject(fields)
            seen.append(fields)
            assert not trace.get_current_span().is_recording()
            return 1
        return await ctx.step.run("work", body)
    asyncio.run(execute("stream", handler, {**CARRIER, "traceparent": CARRIER["traceparent"][:-2] + "00"}))
    assert seen[0]["traceparent"].split("-")[1] == TRACE_ID
    assert seen[0]["traceparent"].endswith("-00")
    assert seen[0]["tracestate"] == "vendor=value"


@pytest.mark.parametrize("mode", ["push", "poll", "stream"])
def test_each_execution_has_fresh_span_and_no_baggage(mode: str, spans: Any) -> None:
    from opentelemetry import baggage
    seen: list[Any] = []
    async def handler(ctx: Any) -> int:
        seen.append(baggage.get_baggage("credential"))
        return 1
    carrier = {**CARRIER, "baggage": "credential=secret"} if mode != "stream" else CARRIER
    from opentelemetry.context import attach, detach
    token = attach(baggage.set_baggage("credential", "ambient-secret"))
    try:
        asyncio.run(execute(mode, handler, carrier))
        asyncio.run(execute(mode, handler, carrier))
        assert baggage.get_baggage("credential") == "ambient-secret"
    finally:
        detach(token)
    process = [s for s in spans.get_finished_spans() if s.name == "process"]
    assert len(process) == 2
    assert process[0].context.span_id != process[1].context.span_id
    assert seen == [None, None]


def test_execution_without_opentelemetry_imports(monkeypatch: pytest.MonkeyPatch, spans: Any) -> None:
    import builtins
    original = builtins.__import__
    def without_otel(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.startswith("opentelemetry"):
            raise ImportError("tracing extra absent")
        return original(name, *args, **kwargs)
    async def handler(ctx: Any) -> int:
        return await ctx.step.run("work", lambda: 7)
    monkeypatch.setattr(builtins, "__import__", without_otel)
    response = asyncio.run(execute("push", handler, CARRIER))
    assert json.loads(response[2])["result"] == 7
    assert not spans.get_finished_spans()


def test_concurrent_jobs_keep_their_own_context(spans: Any) -> None:
    async def scenario() -> None:
        both_started = asyncio.Event()
        arrived = 0
        async def handler(ctx: Any) -> Any:
            nonlocal arrived
            arrived += 1
            if arrived == 2:
                both_started.set()
            await both_started.wait()
            return await ctx.step.run("work", lambda: 7)
        other = {**CARRIER, "traceparent": CARRIER["traceparent"].replace(TRACE_ID, "abcdef1234567890abcdef1234567890")}
        await asyncio.gather(execute("stream", handler, CARRIER), execute("stream", handler, other))
    async def bounded() -> None:
        await asyncio.wait_for(scenario(), 2)
    asyncio.run(bounded())
    finished = spans.get_finished_spans()
    processes = [s for s in finished if s.name == "process"]
    steps = [s for s in finished if s.name == "step.run"]
    assert len(processes) == len(steps) == 2
    assert len({s.context.trace_id for s in processes}) == 2
    for step in steps:
        parent = next(s for s in processes if s.context.span_id == step.parent.span_id)
        assert step.context.trace_id == parent.context.trace_id
