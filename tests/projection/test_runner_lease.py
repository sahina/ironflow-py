"""ADR 0120 lease client. The test names match the Go (#2798) and Node (#2800) runners."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from protobuf.wkt import Duration, Struct

from ironflow._gen.projection_pb import (
    AckProjectionEventsResponse,
    AcquireProjectionLeaseResponse,
    PollProjectionEventsResponse,
    ReleaseProjectionLeaseResponse,
    RenewProjectionLeaseResponse,
    SaveProjectionStateResponse,
)
from ironflow._gen.projection_pb import (
    ProjectionEvent as PE,
)
from ironflow.projection import create_projection
from ironflow.projection._runner import ProjectionRunner, _event_from_proto, _LeaseLost
from ironflow.rpc._runtime import ServerTooOldError
from tests.projection.fake_service import FakeProjectionService, granted
from tests.worker.conftest import run

TRANSPORTS = ["poll", "stream"]
GRANTED = "lease granted"
STANDBY = "lease held by another runner, standing by"
LOST = "lease lost"


def lost() -> ConnectError:
    return ConnectError(Code.ABORTED, "projection lease lost")


def event(id: str, seq: int, pk: str = "") -> PE:
    meta = Struct.from_python({"__partition": pk} if pk else {})
    return PE(id=id, name="inc", seq=seq, data=Struct.from_python({"n": 1}), metadata=meta)


def _epoch(req: Any) -> int:
    return int(req.lease.epoch) if req.lease is not None else 0


class LeaseFake(FakeProjectionService):
    """A server with the ADR 0120 rules: one holder, a fence on the epoch, and Aborted
    for a stale epoch on Poll, Stream, Save, Ack and Renew. Two runners can share it."""

    def __init__(self, *batches: list[PE]) -> None:
        super().__init__()
        self.epoch, self.held, self.ttl = 0, False, 0.3
        self.no_lease = False  # an older server: Acquire is Unimplemented
        self.batches = list(batches)  # each pull takes the next batch
        self.pulls: list[int] = []  # epoch of each Poll and Stream (0: no lease)
        self.saves: list[tuple[str, int, bool]] = []  # (partition, epoch, aborted) of each Save and Ack
        self.acquires = self.renews = 0
        self.renew_error: ConnectError | None = None  # Renew fails with this error
        self.renew_hang = False  # Renew answers only when the caller's timeout runs out
        self.fence_save: Callable[[str], bool] | None = None  # True: a rebuild start fences this save
        self.stream_fence = False  # the stream ends with Aborted when the epoch changes
        self.poll_only = False  # the stream is Unimplemented, so the runner polls

    def stale(self, req: Any) -> bool:
        return req.lease is not None and (not self.held or req.lease.epoch != self.epoch)

    def take_over(self) -> None:
        """Another runner takes the lease, as when the lease expires."""
        self.epoch, self.held = self.epoch + 1, True

    def free(self) -> None:
        """End the other runner's lease."""
        self.epoch, self.held = self.epoch + 1, False

    async def acquire_projection_lease(self, req: Any, **_: Any) -> Any:
        self.acquires += 1
        if self.no_lease:
            raise ConnectError(Code.UNIMPLEMENTED, "no such method")
        if self.held:
            return AcquireProjectionLeaseResponse(granted=False)
        self.take_over()
        return granted(self.epoch, self.ttl)

    async def renew_projection_lease(self, req: Any, **kw: Any) -> Any:
        self.renews += 1
        if self.renew_hang:
            await asyncio.sleep(kw["timeout_ms"] / 1000)
            raise ConnectError(Code.DEADLINE_EXCEEDED, "renew timed out")
        if self.renew_error is not None:
            raise self.renew_error
        if self.stale(req):
            raise lost()
        return RenewProjectionLeaseResponse(ttl=Duration(nanos=int(self.ttl * 1e9)))

    async def release_projection_lease(self, req: Any, **_: Any) -> Any:
        if not self.stale(req):
            self.free()
        return ReleaseProjectionLeaseResponse()

    async def poll_projection_events(self, req: Any, **_: Any) -> Any:
        self.pulls.append(_epoch(req))
        if self.stale(req):
            raise lost()
        return PollProjectionEventsResponse(events=self.batches.pop(0) if self.batches else [])

    async def stream_projection_events(self, req: Any, **_: Any) -> AsyncIterator[Any]:  # type: ignore[override]
        if self.poll_only:
            raise ConnectError(Code.UNIMPLEMENTED, "no stream")
        self.pulls.append(_epoch(req))
        if self.stale(req):
            raise lost()
        sent = False  # one batch per stream: the next one comes after a rewind, on the next open
        while True:
            if self.stream_fence and self.stale(req):
                raise lost()
            if not sent and self.batches:
                sent = True
                for e in self.batches.pop(0):
                    yield e
            await asyncio.sleep(0.005)

    def _fenced(self, req: Any, pk: str) -> bool:
        aborted = self.stale(req)
        if not aborted and self.fence_save is not None and self.fence_save(pk):
            self.free()  # a non-SQL rebuild start bumps the epoch and frees the lease
            aborted = True
        self.saves.append((pk, _epoch(req), aborted))
        return aborted

    async def save_projection_state(self, req: Any, **_: Any) -> Any:
        if self._fenced(req, req.partition_key):
            raise lost()
        return SaveProjectionStateResponse()

    async def ack_projection_events(self, req: Any, **_: Any) -> Any:
        if self._fenced(req, ""):
            raise lost()
        return AckProjectionEventsResponse()


class Runner:
    """One runner on the fake, with its own logger and its own task."""

    def __init__(
        self, svc: LeaseFake, transport: str, name: str, external: bool = False, handler: Any = None
    ) -> None:
        self.handled: list[str] = []

        def count(s: Any, e: Any, c: Any) -> Any:
            self.handled.append(e.id)
            return {"count": s.get("count", 0) + 1}

        def react(e: Any, c: Any) -> None:
            self.handled.append(e.id)

        kw: dict[str, Any] = {} if external else {"initial_state": dict}
        proj = create_projection(name="x", events=["inc"], handler=handler or (react if external else count), **kw)
        self.log = logging.getLogger(f"lease.{name}")
        r = ProjectionRunner(proj, svc, dict, self.log)
        r.flush_interval, r.reconnect_base, r.reconnect_max, r.reconnect_after_end = 0.01, 0.01, 0.02, 0.01
        r.poll_min, r.poll_max, r.cleanup_timeout = 0.01, 0.02, 0.5
        r.standby_retry, r.renew_retry = 0.05, 0.02
        svc.poll_only = transport == "poll"
        self.r = r
        self.task: asyncio.Future[None] | None = None
        self.svc = svc

    def start(self) -> Runner:
        self.task = asyncio.ensure_future(self.r.run())
        return self

    async def stop(self) -> None:
        await self.r.stop()
        assert self.task is not None
        await asyncio.wait_for(self.task, 2)


def logs(caplog: pytest.LogCaptureFixture, runner: Runner, text: str) -> int:
    return sum(1 for rec in caplog.records if rec.name == runner.log.name and text in rec.getMessage())


async def wait_for(what: str, cond: Callable[[], bool], timeout: float = 3.0) -> None:
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not cond():
        if loop.time() > end:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.005)


@pytest.fixture()
def caplog_info(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    caplog.set_level(logging.INFO, logger="lease")
    return caplog


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_two_runners_only_holder_reads(
    loop: asyncio.AbstractEventLoop, caplog_info: pytest.LogCaptureFixture, transport: str
) -> None:
    svc = LeaseFake([event("e1", 1)])

    async def go() -> None:
        holder = Runner(svc, transport, "holder", external=True).start()
        await wait_for("the first grant", lambda: logs(caplog_info, holder, GRANTED) == 1)
        standby = Runner(svc, transport, "standby", external=True).start()
        await wait_for("standby retries", lambda: svc.acquires >= 4)
        await wait_for("the holder's ack", lambda: len(svc.saves) == 1)
        await standby.stop()
        await holder.stop()
        assert all(e == 1 for e in svc.pulls), f"pull with another epoch than the holder's: {svc.pulls}"
        assert svc.saves[0] == ("", 1, False)
        assert logs(caplog_info, standby, STANDBY) == 1 and logs(caplog_info, standby, GRANTED) == 0
        assert standby.handled == [] and holder.handled == ["e1"]

    run(loop, go())


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_standby_takes_over_after_holder_stops(
    loop: asyncio.AbstractEventLoop, caplog_info: pytest.LogCaptureFixture, transport: str
) -> None:
    svc = LeaseFake([event("e1", 1)])

    async def go() -> None:
        holder = Runner(svc, transport, "holder").start()
        await wait_for("the first grant", lambda: logs(caplog_info, holder, GRANTED) == 1)
        standby = Runner(svc, transport, "standby").start()
        await wait_for("standby", lambda: logs(caplog_info, standby, STANDBY) == 1)

        await holder.stop()  # Release: epoch 2, the lease is free
        svc.batches = [[event("e2", 2)]]
        await wait_for("the takeover", lambda: logs(caplog_info, standby, GRANTED) == 1)
        await wait_for("a save from the new holder", lambda: any(e == 3 for _, e, _ in svc.saves))
        await standby.stop()
        assert 3 in svc.pulls, f"pulls = {svc.pulls}, want the new holder to read with epoch 3"

    run(loop, go())


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_fenced_save_stops_batch_no_retry(
    loop: asyncio.AbstractEventLoop, caplog_info: pytest.LogCaptureFixture, transport: str
) -> None:
    batch = [event("a1", 1, "a"), event("b1", 2, "b"), event("a2", 3, "a")]
    # The server rewinds after the fence, so the new holder reads the batch again.
    svc = LeaseFake(batch, batch)
    fenced: list[bool] = []

    def fence(pk: str) -> bool:
        if pk == "b" and not fenced:
            fenced.append(True)
            return True
        return False

    svc.fence_save = fence

    async def go() -> None:
        r = Runner(svc, transport, "r").start()
        await wait_for("the batch saved under the new lease", lambda: len(svc.saves) == 5)
        await r.stop()
        assert svc.saves == [
            ("a", 1, False), ("b", 1, True),  # stop at the fenced save; no retry with epoch 1
            ("a", 3, False), ("b", 3, False), ("a", 3, False),
        ]
        assert logs(caplog_info, r, LOST) == 1 and logs(caplog_info, r, GRANTED) == 2
        assert logs(caplog_info, r, "stream failed") == 0, "a lost lease must not use the reconnect ladder"

    run(loop, go())


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_expired_lease_goes_to_standby(
    loop: asyncio.AbstractEventLoop, caplog_info: pytest.LogCaptureFixture, transport: str
) -> None:
    # The stream does not end on its own: Renew finds the lost lease and the runner
    # drops the two pending frames.
    svc = LeaseFake([event("e1", 1), event("e2", 2)], [event("e3", 3)])

    async def go() -> None:
        r = Runner(svc, transport, "r")
        r.r.flush_interval = 3600
        r.start()
        await wait_for("the first grant", lambda: logs(caplog_info, r, GRANTED) == 1)
        if transport == "stream":
            await wait_for("the pending frames", lambda: len(r.r._pending) == 2)

        svc.take_over()  # the lease expired and another runner took it
        await wait_for("standby", lambda: logs(caplog_info, r, LOST) == 1 and logs(caplog_info, r, STANDBY) == 1)
        svc.free()
        await wait_for("a new grant", lambda: logs(caplog_info, r, GRANTED) == 2)
        epoch = svc.epoch
        await wait_for("a pull with the new lease", lambda: epoch in svc.pulls)
        if transport == "stream":
            assert r.handled == [] and svc.saves == [], "the frames of the lost lease must be dropped"
            assert r.r._pending == [] or r.r._pending[0].id == "e3"
        await r.stop()

    run(loop, go())


def test_stream_aborted_drops_pending_frames(
    loop: asyncio.AbstractEventLoop, caplog_info: pytest.LogCaptureFixture
) -> None:
    svc = LeaseFake([event("e1", 1), event("e2", 2)])
    svc.ttl = 30  # no Renew in this test: only the stream reports the lost lease
    svc.stream_fence = True

    async def go() -> None:
        r = Runner(svc, "stream", "r")
        r.r.flush_interval = 3600
        r.start()
        await wait_for("the pending frames", lambda: len(r.r._pending) == 2)
        svc.take_over()
        await wait_for("standby", lambda: logs(caplog_info, r, LOST) == 1 and logs(caplog_info, r, STANDBY) == 1)
        await r.stop()
        assert svc.saves == [] and r.handled == [], "the pending frames must be dropped"
        assert logs(caplog_info, r, "stream failed") == 0, "a lost lease must not use the reconnect ladder"
        assert svc.requests("poll_projection_events") == [] and svc.pulls == [1]

    run(loop, go())


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_old_server_stops(loop: asyncio.AbstractEventLoop, transport: str) -> None:
    svc = LeaseFake([event("e1", 1)])
    svc.no_lease = True

    async def go() -> None:
        r = Runner(svc, transport, "r").start()
        assert r.task is not None
        with pytest.raises(ServerTooOldError) as err:
            await asyncio.wait_for(r.task, 2)
        assert err.value.code == "SERVER_TOO_OLD"
        assert str(err.value) == "AcquireProjectionLease needs Ironflow server >= 0.44.0; upgrade the server"
        assert svc.pulls == [] and svc.saves == [], "no Poll, Stream, Save or Ack"
        assert svc.acquires == 1

    run(loop, go())


def test_renew_transport_error_keeps_processing(
    loop: asyncio.AbstractEventLoop, caplog_info: pytest.LogCaptureFixture
) -> None:
    svc = LeaseFake()
    svc.renew_error = ConnectError(Code.UNAVAILABLE, "renew 503")

    async def go() -> None:
        r = Runner(svc, "poll", "r").start()
        # The first Renew comes after TTL/3 (100 ms), then a retry every 20 ms.
        await wait_for("renew retries", lambda: svc.renews >= 5)
        svc.batches = [[event("e1", 1)]]
        await wait_for("a save while Renew fails", lambda: len(svc.saves) == 1)
        await r.stop()
        assert logs(caplog_info, r, LOST) == 0, "a transport error must not lose the lease"

    run(loop, go())


def test_stop_releases_after_the_final_save(loop: asyncio.AbstractEventLoop) -> None:
    svc = LeaseFake([event("e1", 1)])
    svc.ttl = 30

    async def go() -> None:
        r = Runner(svc, "stream", "r")
        r.r.flush_interval = 3600  # only stop() flushes the pending frame
        r.start()
        await wait_for("the pending frame", lambda: len(r.r._pending) == 1)
        await r.stop()
        assert svc.saves == [("__global__", 1, False)], "the final save must run before Release fences it"
        assert not svc.held

    run(loop, go())


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_lost_lease_stops_external_batch(
    loop: asyncio.AbstractEventLoop, caplog_info: pytest.LogCaptureFixture, transport: str
) -> None:
    svc = LeaseFake([event(f"e{i}", i) for i in range(1, 5)])

    async def go() -> None:
        handled: list[str] = []

        async def react(e: Any, c: Any) -> None:
            handled.append(e.id)
            if e.id == "e2":
                svc.take_over()
                await asyncio.sleep(0.3)  # Renew (every 0.1 s) finds the lease lost meanwhile

        r = Runner(svc, transport, "r", external=True, handler=react).start()
        await wait_for("standby", lambda: logs(caplog_info, r, LOST) == 1)
        await r.stop()
        assert handled == ["e1", "e2"], "no handler may run after the lease is lost"
        assert svc.saves == [], "the batch must not be acked"

    run(loop, go())


def test_stop_during_acquire_releases_the_grant(loop: asyncio.AbstractEventLoop) -> None:
    svc = LeaseFake()
    grant = svc.acquire_projection_lease

    async def slow_grant(req: Any, **kw: Any) -> Any:
        await asyncio.sleep(0.1)
        return await grant(req, **kw)

    svc.acquire_projection_lease = slow_grant  # type: ignore[method-assign]

    async def go() -> None:
        r = Runner(svc, "stream", "r").start()
        await asyncio.sleep(0.03)  # the Acquire is in flight
        await r.stop()
        assert svc.epoch == 2 and not svc.held, "the late grant must be released"
        assert svc.pulls == []

    run(loop, go())


def test_renews_while_the_state_load_fails(loop: asyncio.AbstractEventLoop) -> None:
    svc = LeaseFake([event("e1", 1)])
    svc.script["get_projection"] = [ConnectError(Code.UNAVAILABLE, "get 503")] * 10

    async def go() -> None:
        r = Runner(svc, "poll", "r")
        r.r.reconnect_base = r.r.reconnect_max = 0.05  # ten failed loads take about 0.5 s
        r.start()
        await wait_for("the first pull", lambda: len(svc.pulls) > 0)
        assert svc.renews >= 2, "Renew must run from the grant, not after the state load"
        await r.stop()

    run(loop, go())


def test_cancel_of_run_is_not_swallowed(loop: asyncio.AbstractEventLoop) -> None:
    svc = LeaseFake()
    svc.ttl = 30

    async def go() -> None:
        r = Runner(svc, "stream", "r").start()
        await wait_for("the stream", lambda: len(svc.pulls) == 1)
        assert r.task is not None
        r.task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(r.task, 1)
        assert not svc.held, "a cancelled runner releases its lease"
        await asyncio.sleep(0.01)
        assert r.r._reader is not None and r.r._reader.done(), "the reader must not outlive the runner"

    run(loop, go())


def test_hung_renew_is_bounded(loop: asyncio.AbstractEventLoop) -> None:
    svc = LeaseFake()
    svc.renew_hang = True

    async def go() -> None:
        r = Runner(svc, "poll", "r").start()
        # TTL 0.3 s: each Renew gets 0.1 s, then the 0.02 s retry sends the next one.
        await wait_for("renew retries past a hung renew", lambda: svc.renews >= 3, timeout=1.0)
        await r.stop()

    run(loop, go())


def test_stop_racing_a_lost_lease_drops_frames(loop: asyncio.AbstractEventLoop) -> None:
    """Aborted from any call, not only Renew, marks the lease lost, so the final
    drain of stop() saves nothing under it."""
    svc = LeaseFake()

    async def go() -> None:
        r = Runner(svc, "stream", "r")
        r.r._lost, r.r._lock, r.r._lease = asyncio.Event(), asyncio.Lock(), granted(1, 0.3).lease
        r.r._pending = [_event_from_proto(event("e1", 1))]
        with pytest.raises(_LeaseLost):
            r.r._auth(lost())  # as the stream reports it
        await r.r._drain_pending()
        assert svc.saves == [] and r.handled == []

    run(loop, go())
