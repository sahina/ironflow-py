"""In-process test kit for Ironflow functions. Mirrors Go ironflowtest and Node @ironflow/node/test.

Nothing talks to an engine. ``step.run`` returns a mock if one is set, otherwise it runs the real
body. ``step.invoke`` needs a mock. ``step.wait_for_event`` takes events queued with ``send_event``.
Sleeps return at once. Durable-step memoization is not simulated.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, TypeVar, cast

from ..worker import (
    Context,
    Event,
    Function,
    InvokeAsyncResult,
    PublishResult,
    RunInfo,
    Step,
)
from ..worker._duration import Duration
from ..worker._function import validate_event
from ..worker._step import _check_publish, _invoke

T = TypeVar("T")
I = TypeVar("I")

_ids = itertools.count(1)


@dataclass(frozen=True)
class TestStepRecord:
    """One step the handler ran, in call order."""

    __test__ = False
    name: str
    type: str
    output: Any = None
    error: BaseException | None = None


class TestStep:
    """Stand-in for ``ctx.step``. Same public methods as ``ironflow.worker.Step``."""

    __test__ = False

    def __init__(
        self, step_mocks: dict[str, Callable[[], Any]], invoke_mocks: dict[str, Callable[[Any], Any]],
        events: dict[str, list[Any]], records: list[TestStepRecord],
        compensations: list[tuple[str, Callable[[], Any]]],
    ) -> None:
        self._step_mocks = step_mocks
        self._invoke_mocks = invoke_mocks
        self._events = events
        self._records = records
        self._compensations = compensations

    def _child(self) -> TestStep:
        return TestStep(self._step_mocks, self._invoke_mocks, self._events, self._records, self._compensations)

    async def run(self, name: str, fn: Callable[[], Any], *, timeout: Duration | None = None) -> Any:
        body = self._step_mocks.get(name, fn)
        try:
            output = await _invoke(body)
        except Exception as exc:
            self._records.append(TestStepRecord(name, "run", error=exc))
            raise
        self._records.append(TestStepRecord(name, "run", output))
        return output

    async def invoke(self, function_id: str, input: Any = None, *, timeout: Duration = 30) -> Any:
        mock = self._invoke_mocks.get(function_id)
        if mock is None:
            raise LookupError(f"step.invoke: no mock for {function_id!r}; call mock_invoke({function_id!r}, fn)")
        output = await _invoke(lambda: mock(input))
        self._records.append(TestStepRecord(function_id, "invoke", output))
        return output

    async def invoke_async(self, function_id: str, input: Any = None) -> InvokeAsyncResult:
        mock = self._invoke_mocks.get(function_id)
        if mock is None:
            raise LookupError(
                f"step.invoke_async: no mock for {function_id!r}; call mock_invoke({function_id!r}, fn)")
        await _invoke(lambda: mock(input))
        result = InvokeAsyncResult(run_id=f"test-run-{next(_ids)}")
        self._records.append(TestStepRecord(function_id, "invoke_async", result.run_id))
        return result

    async def publish(self, topic: str, data: Any = None, *, idempotency_key: str | None = None) -> PublishResult:
        _check_publish(topic, data)
        result = PublishResult(event_id=f"test-evt-{next(_ids)}", sequence=next(_ids))
        # The record's output is what the handler published, so a test can assert on it.
        self._records.append(TestStepRecord(f"publish:{topic}", "publish", data))
        return result

    async def sleep(self, name: str, duration: Duration) -> None:
        self._records.append(TestStepRecord(name, "sleep"))

    async def sleep_until(self, name: str, when: datetime | str) -> None:
        self._records.append(TestStepRecord(name, "sleep"))

    async def wait_for_event(
        self, name: str, *, event: str, match: str | None = None, match_value: str | None = None,
        payload: Any = None, timeout: Duration = "7d",
    ) -> Event:
        queue = self._events.get(event)
        if not queue:
            raise LookupError(
                f"step.wait_for_event({name!r}): no {event!r} event queued; call send_event({event!r}, data)")
        received = Event(id=f"test-evt-{next(_ids)}", name=event, data=queue.pop(0),
                         timestamp=datetime.now(timezone.utc))
        self._records.append(TestStepRecord(name, "wait_for_event", received.data))
        return received

    async def parallel(
        self, name: str, branches: Sequence[Callable[[Any], Awaitable[T]]], *,
        concurrency: int | None = None, on_error: Literal["fail_fast", "collect"] = "fail_fast",
    ) -> list[Any]:
        gate = asyncio.Semaphore(concurrency if concurrency and concurrency > 0 else max(len(branches), 1))

        async def one(branch: Callable[[Any], Awaitable[T]]) -> Any:
            async with gate:
                return await branch(self._child())

        results = await asyncio.gather(*(one(b) for b in branches), return_exceptions=True)
        if on_error == "fail_fast":
            for r in results:
                if isinstance(r, Exception):
                    raise r
        return list(results)

    async def map(
        self, name: str, items: Sequence[I], fn: Callable[[I, Any, int], Awaitable[T]], *,
        concurrency: int | None = None, on_error: Literal["fail_fast", "collect"] = "fail_fast",
    ) -> list[Any]:
        def branch(i: int, item: I) -> Callable[[Any], Awaitable[T]]:
            return lambda step: fn(item, step, i)
        return await self.parallel(name, [branch(i, x) for i, x in enumerate(items)],
                                   concurrency=concurrency, on_error=on_error)

    def compensate(self, step_name: str, fn: Callable[[], Any]) -> None:
        if not isinstance(step_name, str) or not step_name:
            raise ValueError("a compensation step name must be a non-empty string")
        self._compensations.append((step_name, fn))


@dataclass
class TestRun:
    """The result of ``TestClient.emit``."""

    __test__ = False
    status: Literal["completed", "failed"]
    output: Any
    error: BaseException | None
    steps: list[TestStepRecord]
    compensations_ran: list[str]

    def step_output(self, name: str) -> Any:
        return next((s.output for s in self.steps if s.name == name), None)


class TestClient:
    """Run a function's handler in-process with mocked steps.

    ``emit`` runs the first function with a trigger for the event. It is async; call it with
    ``asyncio.run`` or from an async test.
    """

    __test__ = False

    def __init__(self, functions: Sequence[Function]) -> None:
        self._functions = list(functions)
        self._step_mocks: dict[str, Callable[[], Any]] = {}
        self._invoke_mocks: dict[str, Callable[[Any], Any]] = {}
        self._events: dict[str, list[Any]] = {}

    def mock_step(self, name: str, fn: Callable[[], Any]) -> None:
        self._step_mocks[name] = fn

    def mock_invoke(self, function_id: str, fn: Callable[[Any], Any]) -> None:
        self._invoke_mocks[function_id] = fn

    def send_event(self, name: str, data: Any) -> None:
        """Queue an event for ``step.wait_for_event``."""
        self._events.setdefault(name, []).append(data)

    async def emit(self, name: str, data: Any) -> TestRun:
        fn = next((f for f in self._functions if any(t.get("event") == name for t in f.triggers)), None)
        if fn is None:
            raise ValueError(f"no function has a trigger for {name!r}")
        records: list[TestStepRecord] = []
        compensations: list[tuple[str, Callable[[], Any]]] = []
        step = TestStep(self._step_mocks, self._invoke_mocks, self._events, records, compensations)
        n = next(_ids)
        try:
            event = await validate_event(fn, Event(
                id=f"test-evt-{n}", name=name, data=data, timestamp=datetime.now(timezone.utc)))
            ctx = Context(
                event=event, step=cast(Step, step), run=RunInfo(id=f"test-run-{n}", function_id=fn.id, attempt=1),
                logger=logging.LoggerAdapter(logging.getLogger("ironflow.testing"), {}), secrets={},
            )
            output = await fn.handler(ctx)
        except Exception as exc:  # noqa: BLE001 - any handler failure is a failed run
            ran: list[str] = []
            for step_name, undo in reversed(compensations):
                ran.append(step_name)
                try:
                    await _invoke(undo)
                    records.append(TestStepRecord(f"compensate:{step_name}", "compensate"))
                except Exception as undo_exc:  # noqa: BLE001 - one failed undo must not stop the rest
                    records.append(TestStepRecord(f"compensate:{step_name}", "compensate", error=undo_exc))
            return TestRun("failed", None, exc, records, ran)
        return TestRun("completed", output, None, records, [])


__all__ = ["TestClient", "TestRun", "TestStep", "TestStepRecord"]
