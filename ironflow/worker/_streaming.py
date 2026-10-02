"""ConnectRPC bidirectional worker transport."""

from __future__ import annotations

import asyncio
import json
import logging
import platform
import socket
import time
from collections import deque
from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, cast

import pyqwest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from protobuf import Oneof
from protobuf.wkt import Struct, Timestamp, Value
from typing_extensions import override

from .._gen.types_pb import Error, StepType
from .._gen.worker_connect import WorkerServiceClient
from .._gen.worker_pb import (
    ActiveJob,
    ExecutedStep,
    InvokeFunctionAsyncYield,
    InvokeFunctionYield,
    JobAck,
    JobCompleted,
    JobFailed,
    JobNack,
    JobNackReason,
    LeaseState,
    SleepYield,
    StepCompleted,
    StepFailed,
    StepStarted,
    StepYielded,
    WaitEventYield,
    WorkerHeartbeat,
    WorkerMessage,
    WorkerRegister,
    WorkerVersion,
)
from .._gen.worker_pb import (
    JobAssignment as ProtoJobAssignment,
)
from .._http import IronflowError
from ..upcaster import UpcasterRegistry
from ._duration import Duration, iso_utc, parse_timestamp, to_seconds
from ._function import validate_event
from ._protocol import JobAssignment, StepResult, YieldInfo
from ._publish import bind_publish
from ._step import (
    Context,
    ExecutionContext,
    RunInfo,
    Step,
    _Yield,
    encode_json,
    run_compensations,
)
from ._worker import Worker, WorkerAuthError, _ActiveJob, _sdk_version

if TYPE_CHECKING:
    from ..projection import Projection


def _job_from_proto(proto: ProtoJobAssignment) -> JobAssignment:
    event = proto.event
    if event is None or event.timestamp is None:
        raise ValueError("job assignment is missing its event timestamp")
    stamp = event.timestamp
    timestamp = datetime.fromtimestamp(
        stamp.seconds + stamp.nanos / 1_000_000_000, timezone.utc
    )
    if event.has_field("data_value"):
        assert event.data_value is not None
        data = event.data_value.to_python()
    elif event.has_field("data"):
        assert event.data is not None
        data = event.data.to_python()
    else:
        data = None
    job: dict[str, Any] = {
        "job_id": proto.job_id,
        "run_id": proto.run_id,
        "function_id": proto.function_id,
        "attempt": proto.attempt,
        "event": {
            "id": event.id,
            "name": event.name,
            "data": data,
            "timestamp": iso_utc(timestamp),
            "version": event.version or 1,
            "source": event.source or None,
            "idempotency_key": event.idempotency_key or None,
        },
        "completed_steps": [
            {
                "step_id": step.step_id,
                "name": step.name,
                "status": step.status or "completed",
                "error": json.loads(step.error_json) if step.error_json else None,
                "output": (
                    step.output_value.to_python()
                    if step.has_field("output_value") and step.output_value is not None
                    else step.output.to_python()
                    if step.has_field("output") and step.output is not None
                    else None
                ),
            }
            for step in proto.completed_steps
        ],
        "execution_seq": proto.execution_seq,
        "lease_token": proto.lease_token,
    }
    metadata = event.metadata if event.has_field("metadata") else None
    if metadata is not None:
        job["event"]["metadata"] = metadata.to_python()
    if (
        proto.has_field("context")
        and proto.context is not None
        and proto.context.secrets
    ):
        job["context"] = {"secrets": dict(proto.context.secrets)}
    return job  # type: ignore[return-value]


_OUTBOX_LIMIT = 256


def _payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return {"output": Struct.from_python(value)}
    return {"output_value": Value.from_python(value)}


def _message(payload: tuple[str, Any]) -> WorkerMessage:
    return WorkerMessage(payload=cast(Any, Oneof(*payload)))


def _input_json(value: Any) -> bytes:
    """Invoke input as JSON bytes; None means no input."""
    return b"" if value is None else json.dumps(value, allow_nan=False).encode()


class StreamingWorker(Worker):
    """Pull worker using WorkerService.Connect bidirectional streaming."""

    def __init__(
        self,
        *,
        functions: Sequence[Any],
        server_url: str | None = None,
        api_key: str | None = None,
        environment: str | None = None,
        worker_id: str | None = None,
        max_concurrent_jobs: int = 10,
        heartbeat_interval: Duration = 30,
        reconnect_delay: Duration = 5,
        checkpoint_interval: Duration = 1,
        drain_timeout: Duration = 60,
        labels: Mapping[str, str] | None = None,
        logger: logging.Logger | None = None,
        upcasters: UpcasterRegistry | None = None,
        projections: Sequence[Projection] = (),
    ) -> None:
        super().__init__(
            functions=functions,
            server_url=server_url,
            api_key=api_key,
            environment=environment,
            worker_id=worker_id,
            max_concurrent_jobs=max_concurrent_jobs,
            heartbeat_interval=heartbeat_interval,
            reconnect_delay=reconnect_delay,
            checkpoint_interval=checkpoint_interval,
            drain_timeout=drain_timeout,
            labels=labels,
            logger=logger,
            upcasters=upcasters,
            projections=projections,
        )
        self._stream_http = pyqwest.Client(
            transport=pyqwest.HTTPTransport(http_version=pyqwest.HTTPVersion.HTTP2),
        )
        self._stream_client = WorkerServiceClient(
            self._transport._base, http_client=self._stream_http
        )
        self._outbox: deque[WorkerMessage] = deque()
        self._outbox_ready = asyncio.Event()
        self._outbox_empty = asyncio.Event()
        self._outbox_empty.set()
        self._connection_stop = asyncio.Event()
        self._queue_overflow = asyncio.Event()
        self._registered = False

    @override
    async def _register(self) -> None:
        await self._register_functions()

    @override
    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            if self.state != "connected" or not self._registered:
                continue
            jobs = [active for active in self._jobs.values() if not active.abandoned]
            message = _message(
                (
                    "heartbeat",
                    WorkerHeartbeat(
                        worker_id=self.worker_id,
                        active_jobs=len(jobs),
                        jobs=[
                            ActiveJob(
                                job_id=active.job["job_id"],
                                started_at=Timestamp.from_datetime(
                                    parse_timestamp(active.started_at)
                                ),
                                run_id=active.job["run_id"],
                                execution_seq=active.job["execution_seq"],
                                lease_token=active.job["lease_token"],
                            )
                            for active in jobs
                        ],
                    ),
                )
            )
            self._send(message)

    @override
    async def _poll_loop(self) -> None:
        assert self._draining is not None
        first = True
        while not self._draining.is_set():
            self.state = "connecting"
            self._registered = False
            self._connection_stop.clear()
            self._queue_overflow.clear()
            try:
                if not first:
                    await self._register_functions()
                first = False
                if self._draining.is_set():
                    return
                await self._connect_once()
            except WorkerAuthError:
                raise
            except ConnectError as exc:
                if exc.code in (Code.UNAUTHENTICATED, Code.PERMISSION_DENIED):
                    raise WorkerAuthError(
                        f"stream authentication failed: {exc}"
                    ) from exc
                self._log.warning("stream disconnected: %s", exc)
            except IronflowError as exc:
                self._log.warning("stream connection failed: %s", exc)
            if not self._draining.is_set():
                await self._pause(self._reconnect_delay)

    async def _connect_once(self) -> None:
        stream = self._stream_client.connect(
            self._request_messages(), headers=self._transport.headers()
        )
        receiver = asyncio.create_task(self._receive(stream))
        stop_waiter = asyncio.create_task(self._connection_stop.wait())
        overflow_waiter = asyncio.create_task(self._queue_overflow.wait())
        try:
            done, _ = await asyncio.wait(
                (receiver, stop_waiter, overflow_waiter),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if receiver in done:
                await receiver
        finally:
            for task in (receiver, stop_waiter, overflow_waiter):
                if not task.done():
                    task.cancel()
            await asyncio.gather(
                receiver, stop_waiter, overflow_waiter, return_exceptions=True
            )
            await cast(Any, stream).aclose()
            self._registered = False
            if self.state != "draining" and self.state != "stopped":
                self.state = "connecting"

    async def _request_messages(self) -> AsyncIterator[WorkerMessage]:
        yield _message(
            (
                "register",
                WorkerRegister(
                    worker_id=self.worker_id,
                    hostname=socket.gethostname() or "unknown",
                    function_ids=list(self._functions),
                    max_concurrent_jobs=self._max,
                    labels=self._labels,
                    version=WorkerVersion(
                        sdk=_sdk_version(),
                        runtime=f"python-{platform.python_version()}",
                    ),
                ),
            )
        )
        while True:
            if not self._outbox:
                self._outbox_empty.set()
                self._outbox_ready.clear()
                await self._outbox_ready.wait()
                continue
            self._outbox_empty.clear()
            message = self._outbox[0]
            yield message
            if self._outbox and self._outbox[0] is message:
                self._outbox.popleft()
            if not self._outbox:
                self._outbox_empty.set()

    def _send(self, message: WorkerMessage) -> bool:
        if len(self._outbox) >= _OUTBOX_LIMIT:
            self._log.error("stream message queue is full; abandoning active jobs")
            self._queue_overflow.set()
            self._abandon_all("stream message queue is full")
            return False
        self._outbox.append(message)
        self._outbox_empty.clear()
        self._outbox_ready.set()
        return True

    def _abandon_all(self, reason: str) -> None:
        for active in list(self._jobs.values()):
            active.flush_on_cancel = False
            self._abandon(active, reason)

    async def _receive(self, stream: AsyncIterator[Any]) -> None:
        async for message in stream:
            await self._handle_engine_message(message)

    async def _handle_engine_message(self, message: Any) -> None:
        case = message.payload.field
        value = message.payload.value
        if case == "registered":
            interval = value.heartbeat_interval_ms
            if interval > 0:
                self._heartbeat_interval = interval / 1000
            self._registered = True
            self.state = "connected"
        elif case == "job":
            await self._handle_job_assignment(value)
        elif case == "cancel":
            active = self._jobs.get(value.job_id)
            if active is not None:
                self._abandon(active, f"server canceled job: {value.reason}")
        elif case == "shutdown":
            timeout = (
                value.drain_timeout_ms / 1000
                if value.drain_timeout_ms > 0
                else self._drain_timeout
            )
            task = asyncio.create_task(self.drain(timeout=timeout))
            self._background.add(task)
            task.add_done_callback(self._background.discard)
        elif case == "lease_refresh":
            statuses = {(s.run_id, s.execution_seq): s.state for s in value.segments}
            for active in list(self._jobs.values()):
                key = (active.job["run_id"], active.job["execution_seq"])
                if statuses.get(key) in (LeaseState.STALE, LeaseState.UNKNOWN):
                    self._abandon(active, "execution lease is stale")

    async def _handle_job_assignment(self, proto: ProtoJobAssignment) -> None:
        # A re-delivered job that already runs here is dropped, never nacked: a
        # nack would re-queue a job that is still running.
        if proto.job_id in self._jobs:
            self._log.debug("job %s is already active, ignoring it", proto.job_id)
            return
        # A job the worker cannot run gets a nack (#2456). A nack uses no run
        # attempt, and the engine re-queues the job at once.
        if self._draining is not None and self._draining.is_set():
            self._log.info("draining, nacking job %s", proto.job_id)
            self._nack(proto, JobNackReason.DRAINING)
            return
        if self.state != "connected" or self._draining is None:
            return
        job = _job_from_proto(proto)
        if len(self._jobs) >= self._max:
            self._log.warning("at capacity, nacking job %s", proto.job_id)
            self._nack(proto, JobNackReason.AT_CAPACITY)
            return
        ack = _message(
            (
                "job_ack",
                JobAck(
                    job_id=job["job_id"],
                    run_id=job["run_id"],
                    execution_seq=job["execution_seq"],
                    lease_token=job["lease_token"],
                ),
            )
        )
        if self._send(ack):
            self._start_job(job)

    def _nack(self, proto: ProtoJobAssignment, reason: JobNackReason) -> None:
        self._send(
            _message(
                (
                    "job_nack",
                    JobNack(
                        job_id=proto.job_id,
                        run_id=proto.run_id,
                        execution_seq=proto.execution_seq,
                        lease_token=proto.lease_token,
                        reason=reason,
                    ),
                )
            )
        )

    @override
    async def _execute(self, active: _ActiveJob) -> None:
        job = active.job
        fn = self._functions.get(job["function_id"])
        if fn is None:
            self._send(
                _message(
                    (
                        "job_failed",
                        JobFailed(
                            job_id=job["job_id"],
                            error=Error(
                                message=f"Function not found: {job['function_id']}",
                                code="FUNCTION_NOT_FOUND",
                                retryable=False,
                            ),
                            execution_seq=job["execution_seq"],
                            lease_token=job["lease_token"],
                        ),
                    )
                )
            )
            return
        ctx = ExecutionContext(job["run_id"], job["completed_steps"], fn.step_timeout)
        ctx.publish = bind_publish(self._transport, job["run_id"])

        def step_started(step_id: str, name: str) -> None:
            self._send(
                _message(
                    (
                        "step_started",
                        StepStarted(
                            job_id=job["job_id"],
                            step_id=step_id,
                            name=name,
                            step_type=StepType.INVOKE,
                            execution_seq=job["execution_seq"],
                            lease_token=job["lease_token"],
                        ),
                    )
                )
            )

        ctx.on_step_started = step_started

        def step_result(step: StepResult) -> None:
            self._send(self._step_message(job, step))

        ctx.on_step_result = step_result
        started = time.monotonic()
        outcome: WorkerMessage
        try:
            secrets = (job.get("context") or {}).get("secrets") or {}
            context = Context(
                event=await validate_event(fn, self._event_for(job)),
                step=Step(ctx),
                run=RunInfo(
                    id=job["run_id"],
                    function_id=job["function_id"],
                    attempt=job["attempt"],
                    environment=self._transport._environment,
                ),
                logger=logging.LoggerAdapter(
                    self._log,
                    {
                        "run_id": job["run_id"],
                        "function_id": job["function_id"],
                    },
                ),
                secrets=MappingProxyType(dict(secrets)),
            )
            output = await fn.handler(context)
            try:
                encode_json(output)
            except (TypeError, ValueError) as exc:
                outcome = _message(
                    (
                        "job_failed",
                        JobFailed(
                            job_id=job["job_id"],
                            error=Error(
                                message=f"output is not JSON-encodable: {exc}",
                                code="SERIALIZATION_ERROR",
                                retryable=False,
                            ),
                            duration_ms=int((time.monotonic() - started) * 1000),
                            execution_seq=job["execution_seq"],
                            lease_token=job["lease_token"],
                        ),
                    )
                )
            else:
                outcome = _message(
                    (
                        "job_completed",
                        JobCompleted(
                            job_id=job["job_id"],
                            **_payload(output),
                            duration_ms=int((time.monotonic() - started) * 1000),
                            execution_seq=job["execution_seq"],
                            lease_token=job["lease_token"],
                        ),
                    )
                )
        except _Yield as yielded:
            try:
                outcome = _message(("step_yielded", self._yield_message(job, yielded.info)))
            except (TypeError, ValueError) as exc:
                # An unsendable yield (e.g. NaN in the invoke input) fails the job
                # now instead of leaving it leased until lease expiry.
                outcome = _message(
                    (
                        "job_failed",
                        JobFailed(
                            job_id=job["job_id"],
                            error=Error(
                                message=f"yield is not JSON-encodable: {exc}",
                                code="SERIALIZATION_ERROR",
                                retryable=False,
                            ),
                            duration_ms=int((time.monotonic() - started) * 1000),
                            execution_seq=job["execution_seq"],
                            lease_token=job["lease_token"],
                        ),
                    )
                )
        except IronflowError as exc:
            comp = await run_compensations(ctx) if not exc.retryable else list[StepResult]()
            outcome = _message(
                (
                    "job_failed",
                    JobFailed(
                        job_id=job["job_id"],
                        error=Error(
                            message=str(exc),
                            code=exc.code or "ERROR",
                            retryable=exc.retryable,
                        ),
                        steps=[self._executed_step(s) for s in comp],
                        duration_ms=int((time.monotonic() - started) * 1000),
                        execution_seq=job["execution_seq"],
                        lease_token=job["lease_token"],
                    ),
                )
            )
        except Exception as exc:  # noqa: BLE001 - handler code may raise any ordinary exception
            outcome = _message(
                (
                    "job_failed",
                    JobFailed(
                        job_id=job["job_id"],
                        error=Error(message=str(exc), code="ERROR", retryable=True),
                        duration_ms=int((time.monotonic() - started) * 1000),
                        execution_seq=job["execution_seq"],
                        lease_token=job["lease_token"],
                    ),
                )
            )
        if not active.abandoned:
            self._send(outcome)

    def _executed_step(self, step: StepResult) -> ExecutedStep:
        error = step.get("error")
        return ExecutedStep(
            id=step["id"], name=step["name"], type=step["type"], status=step["status"],
            compensation_for=step.get("compensation_for", ""), duration_ms=step["duration_ms"],
            error=Error(message=error["message"], retryable=error["retryable"]) if error else None,
        )

    def _step_message(self, job: JobAssignment, step: StepResult) -> WorkerMessage:
        if step["status"] == "completed":
            completed = StepCompleted(
                job_id=job["job_id"],
                step_id=step["id"],
                **_payload(step["output"]),
                duration_ms=step["duration_ms"],
                execution_seq=job["execution_seq"],
                lease_token=job["lease_token"],
            )
            return _message(("step_completed", completed))
        error = step["error"]
        failed = StepFailed(
            job_id=job["job_id"],
            step_id=step["id"],
            error=Error(
                message=error["message"],
                code="STEP_FAILED",
                retryable=error["retryable"],
            ),
            duration_ms=step["duration_ms"],
            execution_seq=job["execution_seq"],
            lease_token=job["lease_token"],
        )
        return _message(("step_failed", failed))

    def _yield_message(self, job: JobAssignment, info: YieldInfo) -> StepYielded:
        if info["type"] == "sleep":
            yield_info: Any = Oneof(
                "sleep",
                SleepYield(
                    until=Timestamp.from_datetime(parse_timestamp(info["until"]))
                ),
            )
        elif info["type"] == "wait_for_event":
            event_filter = info["event_filter"]
            timeout = Timestamp.from_datetime(
                datetime.now(timezone.utc)
                + timedelta(seconds=to_seconds(event_filter["timeout"])),
            )
            payload = event_filter.get("payload")
            yield_info = Oneof(
                "wait_event",
                WaitEventYield(
                    event_name=event_filter["event"],
                    match_expression=event_filter.get("match", ""),
                    match_value=event_filter.get("match_value", ""),
                    timeout=timeout,
                    payload_json=json.dumps(payload, allow_nan=False).encode()
                    if payload is not None
                    else b"",
                ),
            )
        elif info["type"] == "invoke_function":
            yield_info = Oneof(
                "invoke_function",
                InvokeFunctionYield(
                    function_id=info["function_id"],
                    input_json=_input_json(info.get("input")),
                    invoke_timeout_ms=info["invoke_timeout_ms"],
                ),
            )
        else:
            yield_info = Oneof(
                "invoke_function_async",
                InvokeFunctionAsyncYield(
                    function_id=info["function_id"],
                    input_json=_input_json(info.get("input")),
                ),
            )
        return StepYielded(
            job_id=job["job_id"],
            step_id=info["step_id"],
            yield_info=yield_info,
            execution_seq=job["execution_seq"],
            lease_token=job["lease_token"],
        )

    @override
    async def _shutdown_now(self) -> None:
        if self.state == "draining" and self._fatal is None:
            try:
                await asyncio.wait_for(self._outbox_empty.wait(), self._cleanup_timeout)
            except asyncio.TimeoutError:
                self._log.warning("stream outbox did not drain before shutdown")
        self._connection_stop.set()
        await super()._shutdown_now()
