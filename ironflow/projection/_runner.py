"""Projection runner: register, stream (poll fallback), reduce or react, save or ack.

Imports connectrpc and the generated modules — keep it out of `ironflow.projection`'s
eager imports (see Global Constraints).
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import inspect
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from protobuf.wkt import Struct, Timestamp, Value

from .._gen.projection_connect import ProjectionServiceClient
from .._gen.projection_pb import (
    AckProjectionEventsRequest,
    GetProjectionRequest,
    PollProjectionEventsRequest,
    ProjectionEventKind,
    RegisterProjectionRequest,
    SaveProjectionStateRequest,
    StreamProjectionEventsRequest,
)
from .._gen.projection_pb import (
    ProjectionEvent as ProjectionEventProto,
)
from .._http import IronflowError
from ..worker._duration import iso_utc, parse_timestamp
from ._projection import (
    EventInfo,
    Projection,
    ProjectionContext,
    ProjectionEvent,
    ProjectionInfo,
)

GLOBAL = "__global__"


class ProjectionAuthError(IronflowError):
    """The engine rejected this runner's credentials. The runner stops; the worker does not."""


class _StreamingUnsupported(Exception):
    pass


def _ints(v: Any) -> Any:
    """Struct/Value carry every number as a double; give integral ones back as int.

    Otherwise state is `3` on a fresh run and `3.0` after any reload.
    """
    if isinstance(v, float) and v.is_integer():
        return int(v)
    if isinstance(v, dict):
        return {k: _ints(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_ints(x) for x in v]
    return v


def _to_python(msg: Any, struct_field: str, value_field: str) -> Any:
    if msg.has_field(value_field) and getattr(msg, value_field) is not None:
        return _ints(getattr(msg, value_field).to_python())
    if msg.has_field(struct_field) and getattr(msg, struct_field) is not None:
        return _ints(getattr(msg, struct_field).to_python())
    return None


def _event_from_proto(msg: ProjectionEventProto) -> ProjectionEvent:
    ts = ""
    if msg.has_field("timestamp") and msg.timestamp is not None:
        ts = iso_utc(datetime.fromtimestamp(msg.timestamp.seconds + msg.timestamp.nanos / 1e9, timezone.utc))
    meta = _ints(msg.metadata.to_python()) if msg.has_field("metadata") and msg.metadata is not None else {}
    return ProjectionEvent(
        id=msg.id,
        name=msg.name,
        data=_to_python(msg, "data", "data_value"),
        seq=int(msg.seq),
        timestamp=ts,
        source=msg.source,
        metadata=meta,
    )


def _state_fields(state: Any) -> dict[str, Any]:
    if isinstance(state, dict):
        return {"state": Struct.from_python(state)}
    return {"state_value": Value.from_python(state)}


async def _call(fn: Callable[..., Any], *args: Any) -> Any:
    result = fn(*args)
    return await result if inspect.isawaitable(result) else result


class ProjectionRunner:
    flush_interval = 0.1
    reconnect_base, reconnect_max, reconnect_after_end = 2.0, 30.0, 1.0
    poll_min, poll_max = 1.0, 10.0
    cleanup_timeout = 5.0
    rpc_timeout_ms = 30_000  # unary calls only; the event stream stays open by design
    _ESCALATE_AT = 3

    def __init__(
        self,
        projection: Projection,
        client: ProjectionServiceClient,
        headers: Callable[[], dict[str, str]],
        logger: logging.Logger,
    ) -> None:
        self._p = projection
        self._client = client
        self._headers = headers
        self._log = logger
        self._state: Any = self._initial()
        self._reader: asyncio.Future[None] | None = None
        self._timed: set[asyncio.Future[None]] = set()
        self._flush_failed = False
        self._stopping: asyncio.Event | None = None
        self._pending: list[ProjectionEvent] = []

    def _initial(self) -> Any:
        return self._p.initial_state() if self._p.initial_state is not None else dict[str, Any]()

    def _ctx(self, e: ProjectionEvent) -> ProjectionContext:
        return ProjectionContext(
            event=EventInfo(e.id, e.name, e.seq, e.timestamp),
            projection=ProjectionInfo(self._p.name, 1),
        )

    async def _load_state(self) -> bool:
        """Load the saved state. On failure keep the current state and return False."""
        try:
            resp = await self._client.get_projection(
                GetProjectionRequest(name=self._p.name), headers=self._headers(), timeout_ms=self.rpc_timeout_ms
            )
        except ConnectError as exc:
            if exc.code == Code.NOT_FOUND:  # first run: no saved row yet
                self._state = self._initial()
                return True
            self._auth(exc)
            self._log.warning("projection %s: could not load state (%s); keeping current state", self._p.name, exc)
            return False
        except Exception as exc:  # noqa: BLE001 - never wipe state: the next save would overwrite the server
            self._log.warning("projection %s: could not load state (%s); keeping current state", self._p.name, exc)
            return False
        state = _to_python(resp, "state", "state_value")
        self._state = state if state not in (None, {}) else self._initial()
        return True

    async def _flush(self, events: list[ProjectionEvent]) -> None:
        if not events:
            return
        if self._p.mode == "managed":
            await self._flush_managed(events)
        else:
            await self._flush_external(events)

    async def _flush_managed(self, events: list[ProjectionEvent]) -> None:
        groups: dict[str, list[ProjectionEvent]] = {}
        for e in events:
            groups.setdefault(str(e.metadata.get("__partition") or GLOBAL), []).append(e)
        try:
            for pk, batch in groups.items():
                state = self._state if pk == GLOBAL else self._initial()
                for e in batch:
                    state = await _call(self._p.handler, copy.deepcopy(state), e, self._ctx(e))
                    if state is None:
                        raise TypeError(
                            f"projection {self._p.name}: handler returned None for event {e.id}; "
                            "a managed handler must return the new state"
                        )
                last = batch[-1]
                last_event_time = None
                if last.timestamp:
                    dt = parse_timestamp(last.timestamp)
                    last_event_time = Timestamp(
                        seconds=int(dt.timestamp()), nanos=dt.microsecond * 1000
                    )
                req = SaveProjectionStateRequest(
                    name=self._p.name,
                    partition_key=pk,
                    last_event_id=last.id,
                    last_event_seq=last.seq,
                    last_event_time=last_event_time,
                    **_state_fields(state),
                )
                await self._client.save_projection_state(req, headers=self._headers(), timeout_ms=self.rpc_timeout_ms)
                if pk == GLOBAL:
                    self._state = state  # commit only after the save succeeds (spec decision 7)
        except Exception:
            with contextlib.suppress(ProjectionAuthError):  # the flush error below is what counts
                await self._load_state()
            raise

    async def _flush_external(self, events: list[ProjectionEvent]) -> None:
        for e in events:
            await _call(self._p.handler, e, self._ctx(e))
        last = events[-1]
        await self._client.ack_projection_events(
            AckProjectionEventsRequest(name=self._p.name, last_event_id=last.id, last_event_seq=last.seq),
            headers=self._headers(),
            timeout_ms=self.rpc_timeout_ms,
        )

    def _auth(self, exc: BaseException) -> None:
        if isinstance(exc, ConnectError) and exc.code in (Code.UNAUTHENTICATED, Code.PERMISSION_DENIED):
            status = 401 if exc.code == Code.UNAUTHENTICATED else 403
            raise ProjectionAuthError(
                f"projection {self._p.name}: unauthorized ({status}) — check IRONFLOW_API_KEY"
            ) from exc

    async def run(self) -> None:
        """Run until `stop()`. Raises `ProjectionAuthError` on 401/403."""
        self._stopping = asyncio.Event()
        self._pending = []
        self._lock = asyncio.Lock()
        if not await self._start():
            return
        self._log.info("projection runner started: %s", self._p.name)
        try:
            await self._stream_loop()
        except _StreamingUnsupported:
            self._log.info("projection %s: streaming unsupported, polling", self._p.name)
            await self._poll_loop()

    async def _start(self) -> bool:
        """Register (and load managed state), retrying with backoff. False when stopped first."""
        assert self._stopping is not None
        failures = 0
        while not self._stopping.is_set():
            try:
                await self._client.register_projection(
                    RegisterProjectionRequest(
                        name=self._p.name,
                        events=list(self._p.events),
                        partition_key=self._p.partition_key,
                        version=1,
                        mode=self._p.mode,
                    ),
                    headers=self._headers(),
                    timeout_ms=self.rpc_timeout_ms,
                )
                if self._p.mode != "managed" or await self._load_state():
                    return True
            except ProjectionAuthError:
                raise
            except ConnectError as exc:
                self._auth(exc)
                self._log.warning("projection %s: register failed: %s", self._p.name, exc)
            except Exception as exc:  # noqa: BLE001 - retried like a stream failure
                self._log.warning("projection %s: register failed: %s", self._p.name, exc)
            failures += 1
            await self._sleep(min(self.reconnect_base * 2 ** (failures - 1), self.reconnect_max))
        return False

    async def stop(self) -> None:
        if self._stopping is None:
            return
        self._stopping.set()
        try:
            # Bounded; the flush still hung past it is cancelled below.
            await asyncio.wait_for(self._stop_reader_then_drain(), self.cleanup_timeout)
        except Exception as exc:  # noqa: BLE001 - shutdown must not raise
            self._log.warning("projection %s: final flush abandoned: %s", self._p.name, exc)
        if self._reader is not None:
            self._reader.cancel()
        for t in list(self._timed):
            t.cancel()

    async def _stop_reader_then_drain(self) -> None:
        async with self._lock:  # a running flush (inline or timed) completes first
            if self._reader is not None:
                self._reader.cancel()
        await self._drain_pending()

    async def _drain_pending(self) -> None:
        async with self._lock:
            batch = self._pending
            self._pending = []
            if self._flush_failed:
                return  # flushing past a failed batch would skip it; the cursor redelivers both
            try:
                await self._flush(batch)
            except BaseException:
                self._flush_failed = True
                raise

    async def _stream_loop(self) -> None:
        assert self._stopping is not None
        failures = 0
        while not self._stopping.is_set():
            self._reader = asyncio.ensure_future(self._read_stream())
            try:
                await self._reader
                failures = 0
                delay = self.reconnect_after_end
                self._log.info("projection %s: stream ended, reconnecting", self._p.name)
            except asyncio.CancelledError:
                if self._stopping.is_set():
                    return
                raise
            except (_StreamingUnsupported, ProjectionAuthError):
                raise
            except Exception as exc:  # noqa: BLE001 - every other failure reconnects
                failures += 1
                level = logging.INFO if failures < self._ESCALATE_AT else logging.ERROR
                self._log.log(level, "projection %s: stream failed (%d): %s", self._p.name, failures, exc)
                delay = min(self.reconnect_base * 2 ** (failures - 1), self.reconnect_max)
            async with self._lock:  # waits out an in-flight flush from the old stream
                self._pending = []  # left over after a failed flush: redelivered from the saved cursor
                self._flush_failed = False
            await self._sleep(delay)

    async def _read_stream(self) -> None:
        req = StreamProjectionEventsRequest(
            name=self._p.name, batch_size=self._p.batch_size, accept_heartbeats=True
        )
        timer: asyncio.TimerHandle | None = None
        flush_error: list[BaseException] = []
        reader = asyncio.current_task()
        loop = asyncio.get_running_loop()
        stream = aiter(self._client.stream_projection_events(req, headers=self._headers()))
        try:
            while True:
                try:
                    msg = await anext(stream)
                except StopAsyncIteration:
                    break
                except ConnectError as exc:  # only errors from the stream itself are classified here
                    self._auth(exc)
                    if exc.code in (Code.UNIMPLEMENTED, Code.NOT_FOUND):
                        raise _StreamingUnsupported from exc
                    await self._drain_pending()  # frames already received are valid (as Node does)
                    raise
                if msg.kind == ProjectionEventKind.HEARTBEAT:
                    continue
                self._pending.append(_event_from_proto(msg))
                if timer is not None:
                    timer.cancel()
                    timer = None
                if len(self._pending) >= self._p.batch_size:
                    await self._drain_pending()
                else:
                    timer = loop.call_later(self.flush_interval, self._timed_flush, flush_error, reader)
            await self._drain_pending()
        except asyncio.CancelledError:
            # A failed timed flush cancels this reader: surface the flush error so
            # `_stream_loop` counts it as a stream failure and reconnects.
            if flush_error:
                self._auth(flush_error[0])
                raise flush_error[0] from None
            raise
        except ConnectError as exc:  # a failed save/ack: auth stops the runner, the rest reconnect
            self._auth(exc)
            raise
        finally:
            if timer is not None:
                timer.cancel()

    def _timed_flush(self, errors: list[BaseException], reader: asyncio.Task[Any] | None) -> None:
        async def go() -> None:
            try:
                await self._drain_pending()
            except Exception as exc:  # noqa: BLE001 - surfaced through the reader, which reconnects
                errors.append(exc)
                if reader is not None:
                    reader.cancel()

        task = asyncio.ensure_future(go())
        self._timed.add(task)
        task.add_done_callback(self._timed.discard)

    async def _poll_loop(self) -> None:
        assert self._stopping is not None
        backoff = self.poll_min
        while not self._stopping.is_set():
            try:
                resp = await self._client.poll_projection_events(
                    PollProjectionEventsRequest(name=self._p.name, batch_size=self._p.batch_size),
                    headers=self._headers(),
                    timeout_ms=self.rpc_timeout_ms,
                )
                events = [_event_from_proto(m) for m in resp.events]
                if events and self._p.mode == "managed":
                    current = _to_python(resp, "current_state", "current_state_value")
                    if current not in (None, {}):
                        self._state = current
                async with self._lock:
                    await self._flush(events)
                if events:
                    backoff = self.poll_min
                    continue
            except ConnectError as exc:
                self._auth(exc)
                self._log.error("projection %s poll error: %s", self._p.name, exc)
            except Exception as exc:  # noqa: BLE001 - keep polling
                self._log.error("projection %s poll error: %s", self._p.name, exc)
            await self._sleep(backoff)
            backoff = min(backoff * 2, self.poll_max)

    async def _sleep(self, seconds: float) -> None:
        assert self._stopping is not None
        try:
            await asyncio.wait_for(self._stopping.wait(), seconds)
        except asyncio.TimeoutError:
            pass
