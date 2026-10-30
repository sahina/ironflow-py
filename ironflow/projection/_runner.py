"""Projection runner: register, hold the lease, stream (poll fallback), reduce or react, save or ack.

Imports connectrpc and the generated modules — keep it out of `ironflow.projection`'s
eager imports (see Global Constraints).
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import inspect
import logging
import random
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from protobuf.wkt import Duration, Struct, Timestamp, Value

from .._gen.projection_connect import ProjectionServiceClient
from .._gen.projection_pb import (
    AckProjectionEventsRequest,
    AcquireProjectionLeaseRequest,
    GetProjectionRequest,
    PollProjectionEventsRequest,
    ProjectionEventKind,
    ProjectionLease,
    RegisterProjectionRequest,
    ReleaseProjectionLeaseRequest,
    RenewProjectionLeaseRequest,
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


class _LeaseLost(Exception):
    """The server fenced this runner's lease (ADR 0120 section 3). The runner goes back to standby."""


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
        entity_id=msg.entity_id or None,
        entity_type=msg.entity_type or None,
        entity_version=int(msg.entity_version) if msg.entity_id else None,
    )


def _seconds(d: Duration | None) -> float:
    return float(d.seconds + d.nanos / 1e9) if d is not None else 0.0


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
    # Lease timings (ADR 0120 section 7). The TTL comes from the server.
    standby_retry, renew_retry = 5.0, 1.0  # the standby retry gets ±20 % jitter
    lease_ttl = 30.0  # only when a grant has no ttl, as in the Go runner
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
        # A failed save may still have landed (only its response was lost), and the reload
        # after it failed too: load the state before the next stream (#2977).
        self._reload = False
        self._got_frame = False
        self._progressed = False  # a batch with events succeeded on this stream
        self._stopping: asyncio.Event | None = None
        self._pending: list[ProjectionEvent] = []
        # The held lease, or None in standby. `_lost` is set when Renew finds the
        # lease fenced; each grant gets a new event.
        self._lease: ProjectionLease | None = None
        self._ttl = 0.0
        self._lost: asyncio.Event | None = None
        self._renewer: asyncio.Future[None] | None = None

    def _initial(self) -> Any:
        return self._p.initial_state() if self._p.initial_state is not None else dict[str, Any]()

    def _ctx(self, e: ProjectionEvent) -> ProjectionContext:
        return ProjectionContext(
            event=EventInfo(e.id, e.name, e.seq, e.timestamp),
            projection=ProjectionInfo(self._p.name, 1),
        )

    async def _fetch_state(self, partition: str = "") -> Any:
        """Saved state of one partition, or initial_state() when it has none."""
        resp = await self._client.get_projection(
            GetProjectionRequest(name=self._p.name, partition=partition),
            headers=self._headers(),
            timeout_ms=self.rpc_timeout_ms,
        )
        state = _to_python(resp, "state", "state_value")
        return state if state not in (None, {}) else self._initial()

    async def _load_state(self) -> bool:
        """Load the saved state. On failure keep the current state and return False."""
        try:
            state = await self._fetch_state()
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
        self._state = state
        return True

    async def _flush(self, events: list[ProjectionEvent]) -> None:
        if not events:
            return
        if self._p.mode == "managed":
            await self._flush_managed(events)
        else:
            await self._flush_external(events)

    async def _flush_managed(self, events: list[ProjectionEvent]) -> None:
        self._check_lease()
        # Reduce and save in seq order, one run of consecutive same-partition events at a
        # time. Each save moves the server cursor to the run's last seq, so a failed save
        # leaves only unsaved events past the cursor.
        runs: list[tuple[str, list[ProjectionEvent]]] = []
        for e in sorted(events, key=lambda e: e.seq):
            pk = str(e.metadata.get("__partition") or GLOBAL)
            if runs and runs[-1][0] == pk:
                runs[-1][1].append(e)
            else:
                runs.append((pk, [e]))
        # Load every partition before the first save, so a failed load saves nothing and
        # needs no global reload.
        sem = asyncio.Semaphore(8)

        async def load(pk: str) -> Any:
            async with sem:
                return await self._fetch_state(pk)

        keys = list({pk: None for pk, _ in runs if pk != GLOBAL})
        loaded = await asyncio.gather(*(load(pk) for pk in keys), return_exceptions=True)
        for result in loaded:
            if isinstance(result, BaseException):
                raise result
        states = dict(zip(keys, loaded))
        # A {} state means "no state", as in the Go and Node runners (#2796).
        states[GLOBAL] = self._state if self._state not in (None, {}) else self._initial()
        try:
            for pk, batch in runs:
                state = states[pk]
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
                    lease=self._lease,
                    **_state_fields(state),
                )
                self._check_lease()  # Renew lost the lease: stop the batch before its next save
                await self._client.save_projection_state(req, headers=self._headers(), timeout_ms=self.rpc_timeout_ms)
                states[pk] = state
                if pk == GLOBAL:
                    self._state = state  # commit only after the save succeeds (spec decision 7)
        except Exception as exc:
            if self._is_lost(exc):
                raise  # the run loop drops the state; a reload here would read the next holder's
            self._reload = True
            with contextlib.suppress(ProjectionAuthError):  # the flush error below is what counts
                self._reload = not await self._load_state()
            raise

    async def _flush_external(self, events: list[ProjectionEvent]) -> None:
        for e in events:
            self._check_lease()  # external handlers have side effects: stop the batch at once
            await _call(self._p.handler, e, self._ctx(e))
        last = events[-1]
        self._check_lease()
        await self._client.ack_projection_events(
            AckProjectionEventsRequest(
                name=self._p.name, last_event_id=last.id, last_event_seq=last.seq, lease=self._lease
            ),
            headers=self._headers(),
            timeout_ms=self.rpc_timeout_ms,
        )

    def _auth(self, exc: BaseException) -> None:
        """Raise the error that stops the runner (401/403) or the grant (a lost lease)."""
        if isinstance(exc, ConnectError) and exc.code in (Code.UNAUTHENTICATED, Code.PERMISSION_DENIED):
            status = 401 if exc.code == Code.UNAUTHENTICATED else 403
            raise ProjectionAuthError(
                f"projection {self._p.name}: unauthorized ({status}) — check IRONFLOW_API_KEY"
            ) from exc
        if isinstance(exc, ConnectError) and self._is_lost(exc):
            if self._lost is not None:
                self._lost.set()  # any source, not only Renew: stop() then drains nothing
            raise _LeaseLost(str(exc)) from exc

    def _is_lost(self, exc: BaseException) -> bool:
        # Aborted is the one lease error.
        if isinstance(exc, _LeaseLost):
            return True
        return isinstance(exc, ConnectError) and exc.code == Code.ABORTED and self._lease is not None

    def _check_lease(self) -> None:
        if self._lost is not None and self._lost.is_set():
            raise _LeaseLost("projection lease lost")

    async def run(self) -> None:
        """Run until `stop()`. Raises `ProjectionAuthError` on 401/403, `ServerTooOldError` on a server without the lease."""
        self._stopping = asyncio.Event()
        self._lost = asyncio.Event()
        self._pending = []
        self._lock = asyncio.Lock()
        if not await self._start():
            return
        self._log.info("projection runner started: %s", self._p.name)
        polling = False
        try:
            while await self._acquire():
                # Renew from the grant on: a slow state load must not let the lease expire.
                self._renewer = asyncio.ensure_future(self._renew()) if self._lease is not None else None
                try:
                    if not await self._load_after_grant():
                        return
                    if not polling:
                        try:
                            await self._stream_loop()
                        except _StreamingUnsupported:
                            self._log.info("projection %s: streaming unsupported, polling", self._p.name)
                            polling = True
                    if polling:
                        await self._poll_loop()
                    return
                except _LeaseLost as exc:
                    lost = exc
                finally:
                    await self._stop_renew()
                if self._stopping.is_set():
                    return
                epoch = self._lease.epoch if self._lease is not None else 0
                self._log.info("projection %s: lease lost (epoch %d), standing by: %s", self._p.name, epoch, lost)
                await self._drop()
        finally:
            if not self._stopping.is_set():  # stop() releases itself, after its final save
                await self._release()

    async def _load_after_grant(self) -> bool:
        """Load managed state, retrying with backoff. False when stopped first.

        The state is read after the grant: a state read before the standby wait is stale.
        """
        assert self._stopping is not None
        failures = 0
        while self._p.mode == "managed" and not self._stopping.is_set() and not await self._load_state():
            self._check_lease()
            failures += 1
            await self._sleep(min(self.reconnect_base * 2 ** (failures - 1), self.reconnect_max))
        self._check_lease()
        return not self._stopping.is_set()

    async def _acquire(self) -> bool:
        """Call Acquire until the server grants the lease (ADR 0120). False when stopped first.

        Raises `ServerTooOldError` when the server is older than the lease RPCs.
        """
        assert self._stopping is not None
        standby = False
        while not self._stopping.is_set():
            try:
                resp = await self._client.acquire_projection_lease(
                    AcquireProjectionLeaseRequest(name=self._p.name),
                    headers=self._headers(),
                    timeout_ms=self.rpc_timeout_ms,
                )
            except ConnectError as exc:
                self._auth(exc)
                if exc.code == Code.UNIMPLEMENTED:
                    # Lazy: importing ironflow.rpc loads every service module.
                    from .._skew import MIN_SERVER_VERSION_PROJECTION_LEASE
                    from ..rpc._runtime import ServerTooOldError

                    raise ServerTooOldError("AcquireProjectionLease", MIN_SERVER_VERSION_PROJECTION_LEASE) from exc
                # A lost grant also lands here: the next tries get granted = false until that lease expires.
                self._log.warning("projection %s: lease acquire failed, retrying: %s", self._p.name, exc)
            except Exception as exc:  # noqa: BLE001 - retried like a standby answer
                self._log.warning("projection %s: lease acquire failed, retrying: %s", self._p.name, exc)
            else:
                if resp.granted and resp.lease is not None:
                    self._lease = resp.lease
                    if self._stopping.is_set():  # stop() released before this grant came back
                        await self._release()
                        return False
                    self._ttl = _seconds(resp.ttl) or self.lease_ttl
                    self._log.info("projection %s: lease granted (epoch %d)", self._p.name, resp.lease.epoch)
                    return True
                if not standby:
                    standby = True
                    self._log.info("projection %s: lease held by another runner, standing by", self._p.name)
            await self._sleep(self.standby_retry * random.uniform(0.8, 1.2))
        return False

    async def _renew(self) -> None:
        """Renew every TTL/3. On a transport error retry every `renew_retry` and keep processing.

        On Aborted, stop the transport. The runner keeps no expiry clock: only the server
        decides that the lease is lost (ADR 0120 section 3).
        """
        assert self._lease is not None and self._lost is not None
        lost, ttl = self._lost, self._ttl
        req = RenewProjectionLeaseRequest(name=self._p.name, lease=self._lease)
        wait = ttl / 3
        failed = 0
        while True:
            await asyncio.sleep(wait)
            try:
                # One Renew must not outlive the next one: a hung Renew would let the
                # lease expire with no failure to retry (ADR 0120 section 3).
                resp = await self._client.renew_projection_lease(
                    req, headers=self._headers(), timeout_ms=max(1, int(ttl * 1000 / 3))
                )
            except Exception as exc:  # noqa: BLE001 - only Aborted ends the grant
                if isinstance(exc, ConnectError) and exc.code == Code.ABORTED:
                    lost.set()
                    async with self._lock:  # a running flush stops at its next save or ack
                        if self._reader is not None:
                            self._reader.cancel()
                    return
                failed += 1
                if failed & (failed - 1) == 0:  # 1, 2, 4, 8 ...: the retry runs every second, as in Node
                    self._log.warning(
                        "projection %s: lease renew failed (%d), retrying: %s", self._p.name, failed, exc
                    )
                wait = self.renew_retry
                continue
            if failed:
                self._log.info("projection %s: lease renew recovered after %d failures", self._p.name, failed)
                failed = 0
            ttl = _seconds(resp.ttl) or ttl
            wait = ttl / 3

    async def _stop_renew(self) -> None:
        renewer, self._renewer = self._renewer, None
        if renewer is not None:
            renewer.cancel()
            await asyncio.gather(renewer, return_exceptions=True)

    async def _drop(self) -> None:
        """Forget the lost grant: its unsaved events, its state and its lease.

        The fenced runner did not move the cursor, so the next holder gets the events again.
        """
        async with self._lock:  # a timed flush that waits here then finds nothing to save
            self._pending = []
            self._flush_failed = False
            self._reload = False  # the next grant loads the state before it streams
            self._state = self._initial()
            self._lease = None
            self._lost = asyncio.Event()

    async def _release(self) -> None:
        """Free the lease, so a standby takes it on its next try instead of after the TTL."""
        await self._stop_renew()
        lease, self._lease = self._lease, None
        if lease is None:
            return
        with contextlib.suppress(Exception):  # the server never fails a Release; a lost one expires
            await self._client.release_projection_lease(
                ReleaseProjectionLeaseRequest(name=self._p.name, lease=lease),
                headers=self._headers(),
                timeout_ms=self.rpc_timeout_ms,
            )

    async def _start(self) -> bool:
        """Register, retrying with backoff. False when stopped first or on a config error."""
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
                return True
            except ProjectionAuthError:
                raise
            except ConnectError as exc:
                self._auth(exc)
                if exc.code == Code.INVALID_ARGUMENT:  # a config error does not fix itself
                    self._log.error("projection %s: register failed, stopping: %s", self._p.name, exc)
                    return False
                err: BaseException = exc
            except Exception as exc:  # noqa: BLE001 - retried like a stream failure
                err = exc
            failures += 1
            level = logging.WARNING if failures < self._ESCALATE_AT else logging.ERROR
            self._log.log(level, "projection %s: register failed (%d): %s", self._p.name, failures, err)
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
        # After the final save: a Release first would fence it. A lost Release expires.
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self._release(), self.cleanup_timeout)

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
            if self._lost is not None and self._lost.is_set():
                return  # the next holder gets these events again
            try:
                await self._flush(batch)
            except BaseException:
                self._flush_failed = True
                raise
            self._progressed = self._progressed or bool(batch)

    async def _stream_loop(self) -> None:
        assert self._stopping is not None
        failures = 0
        while not self._stopping.is_set():
            self._check_lease()
            # The stream replays from the server cursor, so the state must be the server's too.
            if self._reload:
                if not await self._load_state():
                    failures += 1
                    await self._sleep(min(self.reconnect_base * 2 ** (failures - 1), self.reconnect_max))
                    continue
                self._reload = False
            self._got_frame = False
            self._progressed = False
            self._reader = asyncio.ensure_future(self._read_stream())
            try:
                await self._await_reader(self._reader)
                failures = 0
                delay = self.reconnect_after_end
                self._log.info("projection %s: stream ended, reconnecting", self._p.name)
            except asyncio.CancelledError:
                if self._stopping.is_set():
                    return
                raise
            except (_StreamingUnsupported, ProjectionAuthError, _LeaseLost):
                raise  # a lost lease uses no reconnect ladder and no poll fallback
            except Exception as exc:  # noqa: BLE001 - every other failure reconnects
                # A stream that delivered a frame was healthy, so a transport error restarts the
                # backoff. A failed batch does not: an event that always fails must still escalate.
                # A batch that succeeded proves the processing works too.
                if self._progressed or (self._got_frame and not self._flush_failed):
                    failures = 0
                failures += 1
                level = logging.INFO if failures < self._ESCALATE_AT else logging.ERROR
                self._log.log(level, "projection %s: stream failed (%d): %s", self._p.name, failures, exc)
                delay = min(self.reconnect_base * 2 ** (failures - 1), self.reconnect_max)
            async with self._lock:  # waits out an in-flight flush from the old stream
                self._pending = []  # left over after a failed flush: redelivered from the saved cursor
                self._flush_failed = False
            await self._sleep(delay)

    async def _await_reader(self, reader: asyncio.Future[None]) -> None:
        """Wait for the reader without letting its cancel by stop() or Renew cancel this task.

        A cancel of this task cancels the reader too. A reader that Renew cancelled raises `_LeaseLost`.
        """
        try:
            await asyncio.wait({reader})
        except asyncio.CancelledError:
            reader.cancel()
            raise
        if reader.cancelled():
            self._check_lease()
            raise asyncio.CancelledError
        reader.result()

    async def _read_stream(self) -> None:
        req = StreamProjectionEventsRequest(
            name=self._p.name, batch_size=self._p.batch_size, accept_heartbeats=True, lease=self._lease
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
                    self._auth(exc)  # a lost lease raises here, so the frames received are dropped
                    if exc.code in (Code.UNIMPLEMENTED, Code.NOT_FOUND):
                        raise _StreamingUnsupported from exc
                    await self._drain_pending()  # frames already received are valid (as Node does)
                    raise
                self._got_frame = True
                if msg.kind == ProjectionEventKind.HEARTBEAT:
                    continue
                self._pending.append(_event_from_proto(msg))
                if len(self._pending) >= self._p.batch_size:
                    if timer is not None:
                        timer.cancel()
                        timer = None
                    await self._drain_pending()
                elif len(self._pending) == 1:  # first frame since the last drain; later frames keep this timer
                    timer = loop.call_later(self.flush_interval, self._timed_flush, flush_error, reader)
            await self._drain_pending()
        except asyncio.CancelledError:
            # A failed timed flush cancels this reader: surface the flush error so
            # `_stream_loop` counts it as a stream failure and reconnects, or stands by.
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
            self._check_lease()
            try:
                resp = await self._client.poll_projection_events(
                    PollProjectionEventsRequest(name=self._p.name, batch_size=self._p.batch_size, lease=self._lease),
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
            except _LeaseLost:
                raise
            except ConnectError as exc:
                self._auth(exc)
                self._log.error("projection %s poll error: %s", self._p.name, exc)
            except Exception as exc:  # noqa: BLE001 - keep polling
                self._log.error("projection %s poll error: %s", self._p.name, exc)
            await self._sleep(backoff)
            backoff = min(backoff * 2, self.poll_max)

    async def _sleep(self, seconds: float) -> None:
        """Pause. Stop and a lost lease end it early."""
        assert self._stopping is not None and self._lost is not None
        waits = [asyncio.ensure_future(self._stopping.wait()), asyncio.ensure_future(self._lost.wait())]
        try:
            await asyncio.wait(waits, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for w in waits:
                w.cancel()
