from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from protobuf.wkt import Struct, Timestamp, Value

from ironflow._gen.types_pb import Event
from ironflow._gen.worker_pb import (
    CompletedStep,
    InvokeFunctionAsyncYield,
    InvokeFunctionYield,
    JobAck,
    JobAssignment,
    JobCompleted,
    JobContext,
    JobFailed,
    StepCompleted,
    WorkerHeartbeat,
)
from ironflow.worker import StreamingWorker, function
from ironflow.worker._step import Event as WorkerEvent
from ironflow.worker._step import NonRetryableError
from ironflow.worker._streaming import _job_from_proto, _message
from tests.worker.conftest import run
from tests.worker.test_worker_loop import until


def test_job_assignment_maps_proto_event_steps_and_fence() -> None:
    job = JobAssignment(
        job_id="job-1",
        run_id="run-1",
        function_id="fn",
        event=Event(
            id="event-1",
            name="order.placed",
            data=Struct.from_python({"order_id": "o-1"}),
            timestamp=Timestamp(seconds=1_790_000_000),
            version=2,
            source="api",
            idempotency_key="idem-1",
        ),
        completed_steps=[
            CompletedStep(
                step_id="run-1:load:0",
                name="load",
                output=Struct.from_python({"count": 3}),
            ),
            CompletedStep(
                step_id="run-1:scalar:0",
                name="scalar",
                output_value=Value.from_python("ok"),
            ),
            CompletedStep(step_id="run-1:empty:0", name="empty"),
        ],
        attempt=2,
        context=JobContext(secrets={"token": "secret"}),
        execution_seq=7,
        lease_token="fence-7",
    )

    assert _job_from_proto(job) == {
        "job_id": "job-1",
        "run_id": "run-1",
        "function_id": "fn",
        "attempt": 2,
        "event": {
            "id": "event-1",
            "name": "order.placed",
            "data": {"order_id": "o-1"},
            "timestamp": "2026-09-21T14:13:20.000Z",
            "version": 2,
            "source": "api",
            "idempotency_key": "idem-1",
        },
        "completed_steps": [
            {"step_id": "run-1:load:0", "name": "load", "status": "completed", "error": None, "output": {"count": 3}},
            {"step_id": "run-1:scalar:0", "name": "scalar", "status": "completed", "error": None, "output": "ok"},
            {"step_id": "run-1:empty:0", "name": "empty", "status": "completed", "error": None, "output": None},
        ],
        "execution_seq": 7,
        "lease_token": "fence-7",
        "context": {"secrets": {"token": "secret"}},
    }


def test_streaming_worker_is_public() -> None:
    from ironflow.worker import Worker

    assert issubclass(StreamingWorker, Worker)


def test_outbox_keeps_unconfirmed_message_after_stream_closes(
    loop: asyncio.AbstractEventLoop,
) -> None:
    @function(id="outbox-fn", triggers=[{"event": "outbox.event"}])
    async def handler(_ctx: Any) -> None:
        return None

    async def scenario() -> None:
        worker = StreamingWorker(functions=[handler])
        stream = worker._request_messages()
        await anext(stream)  # register
        message = _message(("heartbeat", WorkerHeartbeat(worker_id=worker.worker_id)))
        assert worker._send(message)
        assert await anext(stream) is message
        await stream.aclose()  # disconnect before the generator can confirm it
        assert list(worker._outbox) == [message]

    run(loop, scenario())


def test_assignment_is_acked_and_durable_step_is_streamed(
    loop: asyncio.AbstractEventLoop,
) -> None:
    calls = 0

    @function(id="stream-fn", triggers=[{"event": "stream.started"}])
    async def handler(ctx: Any) -> str:
        nonlocal calls

        def work() -> str:
            nonlocal calls
            calls += 1
            return "done"

        return await ctx.step.run("work", work)

    async def scenario() -> list[Any]:
        worker = StreamingWorker(functions=[handler], worker_id="stream-worker")
        worker.state = "connected"
        worker._draining = asyncio.Event()
        job = JobAssignment(
            job_id="job-1",
            run_id="run-1",
            function_id="stream-fn",
            attempt=1,
            event=Event(
                id="event-1",
                name="stream.started",
                data=Struct.from_python({}),
                timestamp=Timestamp(seconds=1_790_000_000),
            ),
            execution_seq=9,
            lease_token="lease-9",
        )
        await worker._handle_job_assignment(job)
        deadline = asyncio.get_running_loop().time() + 1
        while worker._jobs and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0)
        return list(worker._outbox)

    messages = run(loop, scenario())
    assert [message.payload.field for message in messages] == [
        "job_ack",
        "step_started",
        "step_completed",
        "job_completed",
    ]
    ack = messages[0].payload.value
    assert isinstance(ack, JobAck)
    assert (ack.run_id, ack.execution_seq, ack.lease_token) == ("run-1", 9, "lease-9")
    step = messages[2].payload.value
    assert isinstance(step, StepCompleted)
    assert step.execution_seq == 9 and step.lease_token == "lease-9"
    assert step.output_value.to_python() == "done"
    result = messages[3].payload.value
    assert isinstance(result, JobCompleted)
    assert result.output_value.to_python() == "done"
    assert calls == 1


def test_schema_failure_fails_the_job_without_retry(loop: asyncio.AbstractEventLoop) -> None:
    def schema(data: Any) -> Any:
        raise TypeError("qty must be an int")

    @function(id="stream-fn", triggers=[{"event": "stream.started"}], schema=schema)
    async def handler(ctx: Any) -> None:
        raise AssertionError("the handler must not run")

    async def scenario() -> list[Any]:
        worker = StreamingWorker(functions=[handler], worker_id="stream-worker")
        worker.state = "connected"
        worker._draining = asyncio.Event()
        await worker._handle_job_assignment(JobAssignment(
            job_id="job-1", run_id="run-1", function_id="stream-fn", attempt=1,
            event=Event(id="event-1", name="stream.started", data=Struct.from_python({}),
                        timestamp=Timestamp(seconds=1_790_000_000)),
            execution_seq=9, lease_token="lease-9",
        ))
        deadline = asyncio.get_running_loop().time() + 1
        while worker._jobs and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0)
        return list(worker._outbox)

    messages = run(loop, scenario())
    assert [m.payload.field for m in messages] == ["job_ack", "job_failed"]
    failed = messages[1].payload.value
    assert isinstance(failed, JobFailed)
    assert failed.error.code == "VALIDATION_ERROR" and failed.error.retryable is False


def test_unencodable_invoke_input_fails_the_job(loop: asyncio.AbstractEventLoop) -> None:
    @function(id="stream-fn", triggers=[{"event": "stream.started"}])
    async def handler(ctx: Any) -> None:
        await ctx.step.invoke("child", float("nan"))

    async def scenario() -> list[Any]:
        worker = StreamingWorker(functions=[handler], worker_id="stream-worker")
        worker.state = "connected"
        worker._draining = asyncio.Event()
        await worker._handle_job_assignment(JobAssignment(
            job_id="job-1", run_id="run-1", function_id="stream-fn", attempt=1,
            event=Event(id="event-1", name="stream.started", data=Struct.from_python({}),
                        timestamp=Timestamp(seconds=1_790_000_000)),
            execution_seq=9, lease_token="lease-9",
        ))
        deadline = asyncio.get_running_loop().time() + 1
        while worker._jobs and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0)
        return list(worker._outbox)

    messages = run(loop, scenario())
    assert [m.payload.field for m in messages] == ["job_ack", "job_failed"]
    failed = messages[1].payload.value
    assert failed.error.code == "SERIALIZATION_ERROR" and failed.error.retryable is False


def test_non_retryable_job_failure_reports_compensations(loop: asyncio.AbstractEventLoop) -> None:
    @function(id="stream-fn", triggers=[{"event": "stream.started"}])
    async def handler(ctx: Any) -> None:
        await ctx.step.run("a", lambda: 1)
        ctx.step.compensate("a", lambda: None)
        raise NonRetryableError("stop")

    async def scenario() -> list[Any]:
        worker = StreamingWorker(functions=[handler], worker_id="stream-worker")
        worker.state = "connected"
        worker._draining = asyncio.Event()
        await worker._handle_job_assignment(JobAssignment(
            job_id="job-1", run_id="run-1", function_id="stream-fn", attempt=1,
            event=Event(id="event-1", name="stream.started", data=Struct.from_python({}),
                        timestamp=Timestamp(seconds=1_790_000_000)),
            execution_seq=9, lease_token="lease-9",
        ))
        deadline = asyncio.get_running_loop().time() + 1
        while worker._jobs and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0)
        return list(worker._outbox)

    messages = run(loop, scenario())
    fields = [m.payload.field for m in messages]
    assert fields.count("step_completed") == 1  # step "a" only, not the compensation
    assert fields[-1] == "job_failed"
    failed = messages[-1].payload.value
    assert isinstance(failed, JobFailed)
    assert len(failed.steps) == 1
    assert failed.steps[0].type == "compensate"
    assert failed.steps[0].compensation_for == "a"


def _proto_job(**event_kw: Any) -> JobAssignment:
    return JobAssignment(
        job_id="job-1", run_id="run-1", function_id="fn", attempt=1, execution_seq=1, lease_token="l",
        event=Event(id="e1", name="n", data=Struct.from_python({}),
                    timestamp=Timestamp(seconds=1_790_000_000), **event_kw),
    )


def test_job_assignment_carries_event_metadata() -> None:
    job = _job_from_proto(_proto_job(metadata=Struct.from_python({"traceId": "t-1"})))
    assert job["event"]["metadata"] == {"traceId": "t-1"}


def test_job_assignment_without_event_metadata_has_no_metadata_key() -> None:
    assert "metadata" not in _job_from_proto(_proto_job())["event"]


def test_publish_step_works_on_the_streaming_worker(loop: asyncio.AbstractEventLoop, engine: Any) -> None:
    @function(id="pub-fn", triggers=[{"event": "e"}])
    async def handler(ctx: Any) -> str:
        return (await ctx.step.publish("orders", {"a": 1})).event_id

    async def scenario() -> list[Any]:
        worker = StreamingWorker(functions=[handler], worker_id="w", server_url=engine.url)
        worker.state = "connected"
        worker._draining = asyncio.Event()
        await worker._handle_job_assignment(JobAssignment(
            job_id="job-1", run_id="run-1", function_id="pub-fn", attempt=1, execution_seq=1, lease_token="l",
            event=Event(id="e1", name="e", data=Struct.from_python({}), timestamp=Timestamp(seconds=1_790_000_000)),
        ))
        # The job includes a real HTTP publish; a tight deadline flakes on a loaded machine.
        await until(lambda: not worker._jobs, timeout=10)
        return list(worker._outbox)

    messages = run(loop, scenario())
    result = messages[-1].payload.value
    assert isinstance(result, JobCompleted) and result.output_value.to_python() == "evt_pub_1"
    assert engine.calls("publish")[0]["body"] == {"topic": "orders", "data": {"a": 1}}


def test_job_assignment_with_an_empty_metadata_struct_is_harmless() -> None:
    event = _job_from_proto(_proto_job(metadata=Struct.from_python({})))["event"]
    # The proto library reports an explicitly empty Struct as present, so it arrives as {}, not absent.
    assert event["metadata"] == {}
    assert WorkerEvent.from_wire(event).metadata == {}


def _event() -> Event:
    return Event(
        id="event-1", name="stream.started", data=Struct.from_python({}),
        timestamp=Timestamp(seconds=1_790_000_000),
    )


def test_failed_invoke_row_maps_status_and_error() -> None:
    job = _job_from_proto(JobAssignment(
        job_id="j", run_id="r", function_id="fn", event=_event(),
        completed_steps=[CompletedStep(
            step_id="r:child:0", name="child", status="failed",
            error_json=b'{"message": "invoke timed out"}',
        )],
    ))
    row = job["completed_steps"][0]
    assert row["status"] == "failed"
    assert row["error"] == {"message": "invoke timed out"}


def test_completed_row_has_completed_status() -> None:
    job = _job_from_proto(JobAssignment(
        job_id="j", run_id="r", function_id="fn", event=_event(),
        completed_steps=[CompletedStep(step_id="r:a:0", name="a", output=Struct.from_python({"ok": True}))],
    ))
    assert job["completed_steps"][0]["status"] == "completed"


@pytest.mark.parametrize(
    ("info", "case", "value_type"),
    [
        ({"step_id": "s", "type": "invoke_function", "function_id": "child",
          "input": [1, 2], "invoke_timeout_ms": 5000}, "invoke_function", InvokeFunctionYield),
        ({"step_id": "s", "type": "invoke_function_async", "function_id": "child",
          "input": None}, "invoke_function_async", InvokeFunctionAsyncYield),
    ],
)
def test_yield_message_maps_invoke(
    info: dict[str, Any], case: str, value_type: type[InvokeFunctionYield | InvokeFunctionAsyncYield]
) -> None:
    @function(id="yield-fn", triggers=[{"event": "yield.started"}])
    async def handler(_ctx: Any) -> None:
        return None

    worker = StreamingWorker(server_url="http://x", functions=[handler])
    job = {"job_id": "j", "execution_seq": 1, "lease_token": "t"}
    msg = worker._yield_message(job, info)  # type: ignore[arg-type]
    assert msg.yield_info.field == case
    value = msg.yield_info.value
    assert type(value) is value_type
    assert value.function_id == "child"
    if info["input"] is None:
        assert value.input_json == b""
    else:
        assert json.loads(value.input_json) == [1, 2]
        assert value.invoke_timeout_ms == 5000
