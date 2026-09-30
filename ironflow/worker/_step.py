"""Durable-step runtime. No I/O.

A handler runs again from the top on every execution. Each step call derives
a step ID; a completed ID returns its memoized output and does not run the
body. Pull mode has no resume context: a woken sleep and a matched
wait_for_event come back as completed steps.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, TypeVar

from .._http import IronflowError
from ._duration import (
    Duration,
    iso_utc,
    parse_timestamp,
    parse_when,
    to_seconds,
    to_wire,
)
from ._protocol import CompletedStep, EventFilter, StepResult, YieldInfo

_NAMESPACES = ("compensate:", "publish:")
T = TypeVar("T")
I = TypeVar("I")


def encode_json(value: Any) -> str:
    """Strict JSON: NaN and infinities raise ValueError (Go's decoder rejects them)."""
    return json.dumps(value, allow_nan=False)


def escape_step_id_part(part: str) -> str:
    """Escape one step-ID segment. Matches Node escapeStepIdPart and Go escapeStepIDPart."""
    for ns in _NAMESPACES:
        if part.startswith(ns):
            return ns + _escape_raw(part[len(ns):])
    return _escape_raw(part)


def _escape_raw(part: str) -> str:
    return part.replace("\\", "\\\\").replace(":", "\\:")


class _Yield(BaseException):
    """Ends the execution at a sleep or wait. BaseException, so ``except Exception`` in user code cannot swallow it."""

    def __init__(self, info: YieldInfo) -> None:
        super().__init__(info.get("step_id", ""))
        self.info = info


class NonRetryableError(IronflowError):
    """Raise from a handler or step to fail the run with no retry."""

    def __init__(self, message: str, code: str = "NON_RETRYABLE") -> None:
        super().__init__(message, code=code, retryable=False)


class StepError(IronflowError):
    """A ``step.run`` body raised. The cause is ``__cause__``."""

    def __init__(self, message: str, *, step_id: str, retryable: bool) -> None:
        super().__init__(message, code="STEP_FAILED", retryable=retryable)
        self.step_id = step_id


class StepTimeoutError(IronflowError):
    """A ``step.run`` body did not finish before its timeout."""

    def __init__(self, name: str, seconds: float) -> None:
        super().__init__(f"step {name!r} timed out after {seconds:g}s", code="STEP_TIMEOUT", retryable=True)


class SchemaValidationError(IronflowError):
    """The function's ``schema`` rejected the event data. The run fails with no retry."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="VALIDATION_ERROR", retryable=False)


class InvokeError(IronflowError):
    """A ``step.invoke`` child failed or could not start. Never retried."""

    def __init__(self, function_id: str, cause: str, child_run_id: str | None = None) -> None:
        where = f" (run {child_run_id})" if child_run_id else ""
        super().__init__(f"invoke {function_id!r} failed{where}: {cause}", code="INVOKE_FAILED", retryable=False)
        self.function_id = function_id
        self.child_run_id = child_run_id
        self.cause = cause


@dataclass(frozen=True)
class InvokeAsyncResult:
    run_id: str


@dataclass(frozen=True)
class PublishResult:
    event_id: str
    sequence: int


PublishFn = Callable[[str, Any, "str | None"], Awaitable[dict[str, Any]]]


def _check_publish(topic: str, data: Any) -> None:
    """Shared with the test kit, so a handler that fails in production fails there too."""
    if not isinstance(topic, str) or not topic:
        raise ValueError("step.publish: topic must be a non-empty string")
    try:
        encode_json(data)
    except (TypeError, ValueError) as exc:
        # Checked before the step body because a ValueError inside it would count as retryable.
        raise NonRetryableError(
            f"step.publish({topic!r}): data is not JSON-encodable: {exc}", code="SERIALIZATION_ERROR"
        ) from exc


async def _publish_unavailable(topic: str, data: Any, idempotency_key: str | None) -> dict[str, Any]:
    raise NonRetryableError("server URL not configured for publish step", code="UNSUPPORTED")


def _invoke_error(function_id: str, raw: Any) -> InvokeError:
    if isinstance(raw, str):
        return InvokeError(function_id, raw)
    info: dict[str, Any] = raw if isinstance(raw, dict) else {}
    cause = info.get("cause") or info.get("message") or json.dumps(raw)
    return InvokeError(info.get("function_id") or function_id, cause, info.get("child_run_id"))


@dataclass(frozen=True)
class Event:
    id: str
    name: str
    data: Any
    timestamp: datetime
    version: int = 1
    source: str | None = None
    idempotency_key: str | None = None
    metadata: dict[str, Any] | None = None

    @classmethod
    def from_wire(cls, raw: Mapping[str, Any]) -> Event:
        metadata = raw.get("metadata")
        return cls(
            id=raw["id"], name=raw["name"], data=raw.get("data"),
            timestamp=parse_timestamp(raw["timestamp"]), version=raw.get("version", 1),
            source=raw.get("source"), idempotency_key=raw.get("idempotencyKey", raw.get("idempotency_key")),
            metadata=metadata if isinstance(metadata, dict) else None,
        )


@dataclass(frozen=True)
class RunInfo:
    id: str
    function_id: str
    attempt: int


@dataclass(frozen=True)
class Context:
    event: Event
    step: Step
    run: RunInfo
    logger: logging.LoggerAdapter[logging.Logger]
    secrets: Mapping[str, str]


class ExecutionContext:
    """Per-execution state shared by the root Step and every branch Step."""

    def __init__(
        self, run_id: str, completed: Iterable[CompletedStep], step_timeout: Duration | None = None,
    ) -> None:
        self.run_id = run_id
        self.step_timeout = step_timeout
        rows = list(completed)
        self._completed: dict[str, Any] = {
            s["step_id"]: s.get("output") for s in rows if s.get("status", "completed") == "completed"
        }
        self._failed: dict[str, Any] = {s["step_id"]: s.get("error") or {} for s in rows if s.get("status") == "failed"}
        # A default that raises, not None: replaying a completed publish must still work with no server URL.
        self.publish: PublishFn = _publish_unavailable
        self.compensations: list[tuple[str, Callable[[], Any]]] = []
        self.executed: list[StepResult] = []
        self.on_step_recorded: Callable[[], None] = lambda: None
        self.on_step_started: Callable[[str, str], None] = lambda _step_id, _name: None
        self.on_step_result: Callable[[StepResult], None] = lambda _step: None

    def is_completed(self, step_id: str) -> bool:
        return step_id in self._completed

    def output(self, step_id: str) -> Any:
        return self._completed[step_id]

    def failed_error(self, step_id: str) -> Any:
        return self._failed.get(step_id)

    def record(self, step: StepResult) -> None:
        self.executed.append(step)
        self.on_step_result(step)
        self.on_step_recorded()


async def _invoke(fn: Callable[[], Any]) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn()
    result = await asyncio.to_thread(fn)
    # A sync callable can return an awaitable (``lambda: client.aget()``).
    # A coroutine object is not bound to a thread until it starts, so await it here.
    if inspect.isawaitable(result):
        return await result
    return result


async def _with_timeout(fn: Callable[[], Any], seconds: float, name: str) -> Any:
    task = asyncio.ensure_future(_invoke(fn))
    task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
    try:
        done, _ = await asyncio.wait({task}, timeout=seconds)
    except BaseException:
        task.cancel()
        raise
    if not done:
        task.cancel()
        # A thread-backed body or one that suppresses cancellation may keep running.
        raise StepTimeoutError(name, seconds)
    return task.result()


class Step:
    """The step client a handler uses as ``ctx.step``."""

    def __init__(self, ctx: ExecutionContext, scope: str | None = None) -> None:
        self._ctx = ctx
        self._scope = scope or ctx.run_id
        self._counters: dict[str, int] = {}

    def _next_id(self, name: str) -> str:
        if not isinstance(name, str) or not name:
            raise ValueError("a step name must be a non-empty string")
        index = self._counters.get(name, 0)
        self._counters[name] = index + 1
        return f"{self._scope}:{escape_step_id_part(name)}:{index}"

    async def parallel(
        self, name: str, branches: Sequence[Callable[[Step], Awaitable[T]]], *,
        concurrency: int | None = None, on_error: Literal["fail_fast", "collect"] = "fail_fast",
    ) -> list[Any]:
        """Run scoped branches concurrently and settle started work before yielding."""
        if not isinstance(name, str) or not name:
            raise ValueError("a parallel name must be a non-empty string")
        if on_error not in ("fail_fast", "collect"):
            raise ValueError("on_error must be 'fail_fast' or 'collect'")
        prefix = f"{self._scope}:{escape_step_id_part(name)}"
        scoped = [Step(self._ctx, scope=f"{prefix}:{i}") for i in range(len(branches))]
        gate = asyncio.Semaphore(concurrency if concurrency and concurrency > 0 else max(len(branches), 1))
        results: list[Any] = [None] * len(branches)
        errors: dict[int, Exception] = {}
        yields: dict[int, _Yield] = {}
        stop = False

        async def one(i: int) -> None:
            nonlocal stop
            async with gate:
                if stop:
                    return
                try:
                    results[i] = await branches[i](scoped[i])
                except _Yield as signal:
                    yields[i] = signal
                    stop = True
                except Exception as exc:  # noqa: BLE001 - collect arbitrary branch errors as values
                    errors[i] = exc
                    if on_error == "fail_fast":
                        stop = True

        await asyncio.gather(*(one(i) for i in range(len(branches))))
        if yields:
            raise yields[min(yields)]
        if errors and on_error == "fail_fast":
            raise errors[min(errors)]
        return [errors.get(i, results[i]) for i in range(len(branches))]

    async def map(
        self, name: str, items: Sequence[I], fn: Callable[[I, Step, int], Awaitable[T]], *,
        concurrency: int | None = None, on_error: Literal["fail_fast", "collect"] = "fail_fast",
    ) -> list[Any]:
        """Run ``fn`` for each item as a parallel branch. Branch ``i`` gets scope ``{name}:{i}``."""
        def branch(i: int, item: I) -> Callable[[Step], Awaitable[T]]:
            return lambda step: fn(item, step, i)
        return await self.parallel(name, [branch(i, x) for i, x in enumerate(items)],
                                   concurrency=concurrency, on_error=on_error)

    async def sleep(self, name: str, duration: Duration) -> None:
        """Pause the run. The worker is free while it sleeps."""
        step_id = self._next_id(name)
        if self._ctx.is_completed(step_id):
            return
        seconds = to_seconds(duration)
        if seconds <= 0:
            raise ValueError(f"step.sleep({name!r}): the duration must be positive")
        until = datetime.now(timezone.utc) + timedelta(microseconds=math.ceil(seconds * 1_000_000))
        until += timedelta(microseconds=-until.microsecond % 1000)
        raise _Yield({"step_id": step_id, "type": "sleep", "until": iso_utc(until)})

    async def sleep_until(self, name: str, when: datetime | str) -> None:
        """Pause the run until a timezone-aware time."""
        step_id = self._next_id(name)
        if self._ctx.is_completed(step_id):
            return
        wake = parse_when(when)
        if wake <= datetime.now(timezone.utc):
            raise ValueError(f"step.sleep_until({name!r}): the target time must be in the future, got {iso_utc(wake)}")
        wake += timedelta(microseconds=-wake.microsecond % 1000)
        raise _Yield({"step_id": step_id, "type": "sleep", "until": iso_utc(wake)})

    async def wait_for_event(
        self, name: str, *, event: str, match: str | None = None, match_value: str | None = None,
        payload: Any = None, timeout: Duration = "7d",
    ) -> Event:
        """Pause until a matching event arrives. On timeout the engine fails the run (no retry)."""
        step_id = self._next_id(name)
        if self._ctx.is_completed(step_id):
            return Event.from_wire(self._ctx.output(step_id))
        if not isinstance(event, str) or not event.strip():
            raise ValueError(f"step.wait_for_event({name!r}): event must be a non-empty string")
        if to_seconds(timeout) <= 0:
            raise ValueError(f"step.wait_for_event({name!r}): timeout must be positive")
        event_filter: EventFilter = {"event": event.strip(), "timeout": to_wire(timeout)}
        if match is not None:
            event_filter["match"] = match
        if match_value is not None:
            event_filter["match_value"] = match_value
        if payload is not None:
            event_filter["payload"] = payload
        raise _Yield({"step_id": step_id, "type": "wait_for_event", "event_filter": event_filter})

    async def run(self, name: str, fn: Callable[[], Any], *, timeout: Duration | None = None) -> Any:
        """Run ``fn`` once per run. ``fn`` may be async, sync, or sync returning an awaitable."""
        step_id = self._next_id(name)
        if self._ctx.is_completed(step_id):
            return self._ctx.output(step_id)

        self._ctx.on_step_started(step_id, name)
        started = datetime.now(timezone.utc)
        t0 = time.monotonic()
        if timeout is None:
            timeout = self._ctx.step_timeout
        try:
            if timeout is None:
                output = await _invoke(fn)
            else:
                output = await _with_timeout(fn, to_seconds(timeout), name)
        except StepTimeoutError as exc:
            self._record_failure(step_id, name, started, t0, str(exc), True)
            raise
        except Exception as exc:
            retryable = exc.retryable if isinstance(exc, IronflowError) else True
            self._record_failure(step_id, name, started, t0, str(exc), retryable)
            raise StepError(str(exc), step_id=step_id, retryable=retryable) from exc

        try:
            encode_json(output)
        except (TypeError, ValueError) as exc:
            raise NonRetryableError(
                f"step {name!r} output is not JSON-encodable: {exc}", code="SERIALIZATION_ERROR"
            ) from exc

        self._ctx.record({
            "id": step_id, "name": name, "type": "invoke", "status": "completed", "output": output,
            "started_at": iso_utc(started), "ended_at": iso_utc(datetime.now(timezone.utc)),
            "duration_ms": int((time.monotonic() - t0) * 1000),
        })
        return output

    def _invoke_step(self, function_id: str, method: str) -> str:
        if not isinstance(function_id, str) or not function_id:
            raise ValueError(f"step.{method}: function_id must be a non-empty string")
        step_id = self._next_id(function_id)
        failed = self._ctx.failed_error(step_id)
        if failed is not None:
            raise _invoke_error(function_id, failed)
        return step_id

    async def invoke(self, function_id: str, input: Any = None, *, timeout: Duration = 30) -> Any:
        """Run another function and wait for its output. Its failure raises InvokeError (no retry)."""
        step_id = self._invoke_step(function_id, "invoke")
        if self._ctx.is_completed(step_id):
            return self._ctx.output(step_id)
        ms = math.ceil(to_seconds(timeout) * 1000)
        if ms <= 0:
            raise ValueError("step.invoke: timeout must be positive")
        raise _Yield({"step_id": step_id, "type": "invoke_function", "function_id": function_id,
                      "input": input, "invoke_timeout_ms": ms})

    async def invoke_async(self, function_id: str, input: Any = None) -> InvokeAsyncResult:
        """Start another function and return its run ID at once."""
        step_id = self._invoke_step(function_id, "invoke_async")
        if self._ctx.is_completed(step_id):
            return InvokeAsyncResult(run_id=self._ctx.output(step_id)["run_id"])
        raise _Yield({"step_id": step_id, "type": "invoke_function_async",
                      "function_id": function_id, "input": input})

    async def publish(self, topic: str, data: Any = None, *, idempotency_key: str | None = None) -> PublishResult:
        """Publish to a pub/sub topic as a durable step. Memoized, so a retry does not publish twice.

        It does not trigger functions; use the events API for that.
        """
        _check_publish(topic, data)

        async def send() -> dict[str, Any]:
            return await self._ctx.publish(topic, data, idempotency_key)

        out = await self.run(f"publish:{topic}", send)
        event_id = out.get("eventId") if isinstance(out, dict) else None
        if not isinstance(event_id, str) or not event_id:
            # A memoized row from an older or hand-edited run; retrying would replay the same row.
            raise NonRetryableError(
                f"step.publish({topic!r}): the recorded step output has no eventId", code="PUBLISH_FAILED"
            )
        return PublishResult(event_id=event_id, sequence=int(out.get("sequence") or 0))

    def _record_failure(
        self, step_id: str, name: str, started: datetime, t0: float, message: str, retryable: bool
    ) -> None:
        self._ctx.record({
            "id": step_id, "name": name, "type": "invoke", "status": "failed",
            "error": {"message": message, "retryable": retryable},
            "started_at": iso_utc(started), "ended_at": iso_utc(datetime.now(timezone.utc)),
            "duration_ms": int((time.monotonic() - t0) * 1000),
        })

    def compensate(self, step_name: str, fn: Callable[[], Any]) -> None:
        """Register an undo for ``step_name``. Undos run newest first when the run fails with no retry."""
        if not isinstance(step_name, str) or not step_name:
            raise ValueError("a compensation step name must be a non-empty string")
        self._ctx.compensations.append((step_name, fn))


async def run_compensations(ctx: ExecutionContext) -> list[StepResult]:
    """Run registered compensations in reverse. A failure is recorded; the rest still run."""
    # A fresh root counter. Go reuses its live root counter instead, but the
    # IDs still match: handler step names never start with "compensate:".
    ids = Step(ctx)
    out: list[StepResult] = []
    for step_name, fn in reversed(ctx.compensations):
        name = f"compensate:{step_name}"
        step_id = ids._next_id(name)
        if ctx.is_completed(step_id):
            continue
        started, t0 = datetime.now(timezone.utc), time.monotonic()
        error: str | None = None
        try:
            await _invoke(fn)
        except _Yield:
            error = "compensations cannot yield (sleep, wait_for_event, invoke)"
        except Exception as exc:  # noqa: BLE001 - one failed undo must not stop the rest
            error = str(exc) or type(exc).__name__
        result: StepResult = {
            "id": step_id, "name": name, "type": "compensate", "status": "failed" if error else "completed",
            "compensation_for": step_name, "started_at": iso_utc(started),
            "ended_at": iso_utc(datetime.now(timezone.utc)), "duration_ms": int((time.monotonic() - t0) * 1000),
        }
        if error:
            result["error"] = {"message": error, "retryable": False}
        ctx.executed.append(result)
        ctx.on_step_recorded()
        out.append(result)
    return out
