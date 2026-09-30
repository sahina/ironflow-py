from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from protobuf.wkt import Struct

from ironflow._gen.projection_pb import (
    AckProjectionEventsResponse,
    GetProjectionResponse,
    PollProjectionEventsResponse,
    ProjectionEventKind,
    SaveProjectionStateResponse,
)
from ironflow._gen.projection_pb import (
    ProjectionEvent as PE,
)
from ironflow.projection import create_projection
from ironflow.projection._runner import ProjectionAuthError, ProjectionRunner
from tests.projection.fake_service import FakeProjectionService
from tests.worker.conftest import run


def frame(i: int) -> PE:
    return PE(id=f"e{i}", name="inc", seq=i, data=Struct.from_python({"n": 1}))


HEARTBEAT = PE(kind=ProjectionEventKind.HEARTBEAT)


def fast(r: ProjectionRunner) -> ProjectionRunner:
    r.flush_interval, r.reconnect_base, r.reconnect_max, r.reconnect_after_end = 0.01, 0.01, 0.02, 0.01
    r.poll_min, r.poll_max, r.cleanup_timeout = 0.01, 0.02, 0.2
    return r


def make(svc: FakeProjectionService, handler: Any = None, **kw: Any) -> ProjectionRunner:
    seen: list[str] = kw.pop("seen", [])
    proj = create_projection(name="x", events=["inc"], handler=handler or (lambda e, c: seen.append(e.id)), **kw)
    return fast(ProjectionRunner(proj, svc, lambda: {"Authorization": "Bearer k"}, logging.getLogger("t")))


async def run_until(r: ProjectionRunner, cond: Any, timeout: float = 2.0) -> None:
    task = asyncio.ensure_future(r.run())
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not cond() and loop.time() < end:
        await asyncio.sleep(0.005)
    await r.stop()
    await asyncio.wait_for(task, 1)
    assert cond(), "condition never became true"


def test_registers_then_streams_and_skips_heartbeats(loop: asyncio.AbstractEventLoop) -> None:
    svc, seen = FakeProjectionService(), []
    svc.streams = [[frame(1), HEARTBEAT, frame(2)]]
    run(loop, run_until(make(svc, seen=seen), lambda: len(seen) == 2))
    [reg] = svc.requests("register_projection")
    assert (reg.name, list(reg.events), reg.mode, reg.version) == ("x", ["inc"], "external", 1)
    [req] = svc.requests("stream_projection_events")
    assert req.accept_heartbeats and req.batch_size == 100
    assert seen == ["e1", "e2"]
    assert svc.requests("ack_projection_events")[-1].last_event_seq == 2


def test_flush_on_batch_size(loop: asyncio.AbstractEventLoop) -> None:
    svc, seen = FakeProjectionService(), []
    svc.streams = [[frame(i) for i in range(1, 5)]]
    r = make(svc, seen=seen, batch_size=2)
    r.flush_interval = 60  # only the size trigger can flush
    run(loop, run_until(r, lambda: len(svc.requests("ack_projection_events")) == 2))
    assert [a.last_event_seq for a in svc.requests("ack_projection_events")] == [2, 4]


@pytest.mark.parametrize("code", [Code.UNIMPLEMENTED, Code.NOT_FOUND])
def test_unsupported_stream_falls_back_to_poll(loop: asyncio.AbstractEventLoop, code: Code) -> None:
    svc, seen = FakeProjectionService(), []
    svc.streams = [ConnectError(code, "no stream")]
    svc.script["poll_projection_events"] = [PollProjectionEventsResponse(events=[frame(1)])]
    run(loop, run_until(make(svc, seen=seen), lambda: seen == ["e1"]))
    assert svc.requests("poll_projection_events")[0].batch_size == 100


def test_stream_error_reconnects(loop: asyncio.AbstractEventLoop) -> None:
    svc, seen = FakeProjectionService(), []
    svc.streams = [[frame(1), ConnectError(Code.UNAVAILABLE, "reset")], [frame(2)]]
    run(loop, run_until(make(svc, seen=seen), lambda: seen == ["e1", "e2"]))
    assert len(svc.requests("stream_projection_events")) >= 2


def test_flush_failure_reconnects_for_redelivery(
    loop: asyncio.AbstractEventLoop, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="t")
    svc, seen = FakeProjectionService(), []
    svc.script["ack_projection_events"] = [ConnectError(Code.UNAVAILABLE, "ack 503")]
    svc.streams = [[frame(1)], [frame(1)]]  # the engine redelivers after the reconnect
    run(loop, run_until(make(svc, seen=seen), lambda: len(svc.requests("ack_projection_events")) == 2))
    assert seen == ["e1", "e1"]  # external is at-least-once
    # the log names the real cause (the ack), not the reader cancel
    assert any("stream failed" in m and "ack 503" in m for m in caplog.messages)


@pytest.mark.parametrize("code", [Code.UNAUTHENTICATED, Code.PERMISSION_DENIED])
def test_auth_error_stops_runner(loop: asyncio.AbstractEventLoop, code: Code) -> None:
    svc = FakeProjectionService()
    svc.script["register_projection"] = [ConnectError(code, "nope")]
    with pytest.raises(ProjectionAuthError, match="projection x: unauthorized"):
        run(loop, make(svc).run())


def test_auth_error_on_state_load_stops_runner(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.script["get_projection"] = [ConnectError(Code.PERMISSION_DENIED, "nope")]
    r = make(svc, handler=lambda s, e, c: s, initial_state=dict)
    with pytest.raises(ProjectionAuthError, match=r"unauthorized \(403\)"):
        run(loop, asyncio.wait_for(r.run(), 1))


def test_managed_loads_state_before_streaming(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.streams = [[frame(1)]]
    r = make(svc, handler=lambda s, e, c: {"count": s["count"] + 1}, initial_state=lambda: {"count": 0})
    run(loop, run_until(r, lambda: r._state == {"count": 1}))
    methods = [m for m, _ in svc.calls]
    assert methods.index("get_projection") < methods.index("stream_projection_events")


def test_stop_flushes_pending(loop: asyncio.AbstractEventLoop) -> None:
    svc, seen = FakeProjectionService(), []
    svc.streams = [[frame(1)]]
    r = make(svc, seen=seen)
    r.flush_interval = 60  # nothing flushes on its own

    async def go() -> None:
        task = asyncio.ensure_future(r.run())
        while not svc.requests("stream_projection_events"):
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.02)
        await r.stop()
        await asyncio.wait_for(task, 1)

    run(loop, go())
    assert seen == ["e1"] and svc.requests("ack_projection_events")


def test_stop_is_bounded_when_save_hangs(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.streams = [[frame(1)]]

    async def hang(*_: Any, **__: Any) -> Any:
        await asyncio.sleep(3600)

    svc.save_projection_state = hang  # type: ignore[method-assign]
    r = make(svc, handler=lambda s, e, c: {"count": s["count"] + 1}, initial_state=lambda: {"count": 0})

    async def go() -> float:
        task = asyncio.ensure_future(r.run())
        await asyncio.sleep(0.1)
        t0 = asyncio.get_running_loop().time()
        await r.stop()
        await asyncio.wait_for(task, 1)
        return asyncio.get_running_loop().time() - t0

    assert run(loop, go()) < 1.0


def test_stop_waits_for_a_running_inline_flush(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.streams = [[frame(1)]]
    started: list[Any] = []
    saved: list[Any] = []

    async def slow_save(req: Any, **_: Any) -> Any:
        started.append(req)
        await asyncio.sleep(0.05)
        saved.append(req)
        return SaveProjectionStateResponse()

    svc.save_projection_state = slow_save  # type: ignore[method-assign]
    r = make(svc, handler=lambda s, e, c: {"count": s["count"] + 1}, initial_state=lambda: {"count": 0},
             batch_size=1)
    r.flush_interval = 60  # only the size trigger flushes, inline in the reader

    async def go() -> None:
        task = asyncio.ensure_future(r.run())
        while not started:
            await asyncio.sleep(0.002)
        await r.stop()  # the save is in flight
        await asyncio.wait_for(task, 1)

    run(loop, go())
    assert [s.last_event_seq for s in saved] == [1]  # the save completed, not cancelled
    assert r._state == {"count": 1}


def test_unary_rpcs_are_bounded_and_the_stream_is_not(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.streams = [[frame(1)]]
    r = make(svc, handler=lambda s, e, c: {"count": s["count"] + 1}, initial_state=lambda: {"count": 0})
    run(loop, run_until(r, lambda: svc.requests("save_projection_state")))
    by_method = dict(svc.kwargs)
    for m in ("register_projection", "get_projection", "save_projection_state"):
        assert by_method[m]["timeout_ms"] == 30_000, m
    assert by_method["stream_projection_events"].get("timeout_ms") is None


def test_poll_and_ack_are_bounded(loop: asyncio.AbstractEventLoop) -> None:
    svc, seen = FakeProjectionService(), []
    svc.streams = [ConnectError(Code.UNIMPLEMENTED, "no stream")]
    svc.script["poll_projection_events"] = [PollProjectionEventsResponse(events=[frame(1)])]
    run(loop, run_until(make(svc, seen=seen), lambda: svc.requests("ack_projection_events")))
    by_method = dict(svc.kwargs)
    assert by_method["poll_projection_events"]["timeout_ms"] == 30_000
    assert by_method["ack_projection_events"]["timeout_ms"] == 30_000


def test_startup_retries_a_failed_state_load(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.script["get_projection"] = [
        ConnectError(Code.UNAVAILABLE, "get 503"),
        GetProjectionResponse(state=Struct.from_python({"count": 41})),
    ]
    svc.streams = [[frame(1)]]
    r = make(svc, handler=lambda s, e, c: {"count": s["count"] + 1}, initial_state=lambda: {"count": 0})
    run(loop, run_until(r, lambda: r._state == {"count": 42}))
    assert len(svc.requests("register_projection")) == 2  # register + load are retried together
    [save] = svc.requests("save_projection_state")
    assert save.state.to_python() == {"count": 42}  # built on the server's state, not initial_state()


def test_startup_retries_a_failed_register(loop: asyncio.AbstractEventLoop) -> None:
    svc, seen = FakeProjectionService(), []
    svc.script["register_projection"] = [ConnectError(Code.UNAVAILABLE, "reg 503"), RuntimeError("reset")]
    svc.streams = [[frame(1)]]
    run(loop, run_until(make(svc, seen=seen), lambda: seen == ["e1"]))
    assert len(svc.requests("register_projection")) == 3


def test_stop_during_startup_retry_returns(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.script["register_projection"] = [ConnectError(Code.UNAVAILABLE, "down")] * 50
    r = make(svc)
    r.reconnect_base = r.reconnect_max = 60

    async def go() -> None:
        task = asyncio.ensure_future(r.run())
        while not svc.requests("register_projection"):
            await asyncio.sleep(0.002)
        await r.stop()
        await asyncio.wait_for(task, 1)

    run(loop, go())
    assert svc.requests("stream_projection_events") == []


def slow_failing_ack(svc: FakeProjectionService, delay: float) -> list[Any]:
    """The first ack sleeps `delay` then fails; later acks succeed. Returns the ack requests."""
    acks: list[Any] = []

    async def ack(req: Any, **_: Any) -> Any:
        acks.append(req)
        if len(acks) == 1:
            await asyncio.sleep(delay)
            raise ConnectError(Code.UNAVAILABLE, "ack 503")
        return AckProjectionEventsResponse()

    svc.ack_projection_events = ack  # type: ignore[method-assign]
    return acks


def test_timed_flush_failure_during_stream_error_drain_reconnects(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    acks = slow_failing_ack(svc, 0.03)
    # The timer fires at 0.01 and holds the lock in a slow ack; the stream resets at 0.02,
    # so the reader's error-path drain waits on that lock and is cancelled when the ack fails.
    svc.streams = [[frame(1), 0.02, ConnectError(Code.UNAVAILABLE, "reset")], [frame(1)]]
    run(loop, run_until(make(svc), lambda: len(acks) == 2))
    assert len(svc.requests("stream_projection_events")) >= 2
    assert acks[-1].last_event_seq == 1


def test_flush_not_found_is_not_streaming_unsupported(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.script["ack_projection_events"] = [ConnectError(Code.NOT_FOUND, "ack 404")]
    svc.streams = [[frame(1), frame(2)], [frame(1), frame(2)]]
    run(loop, run_until(make(svc, batch_size=2), lambda: len(svc.requests("ack_projection_events")) == 2))
    assert svc.requests("poll_projection_events") == []
    assert len(svc.requests("stream_projection_events")) >= 2


def test_nothing_after_a_failed_flush_is_saved_or_acked(loop: asyncio.AbstractEventLoop) -> None:
    svc, seen = FakeProjectionService(), []
    acks = slow_failing_ack(svc, 0.05)
    svc.streams = [[frame(1), 0.02, frame(2)]]
    r = make(svc, seen=seen)

    async def go() -> None:
        task = asyncio.ensure_future(r.run())
        while not (acks and r._pending):  # batch A is in its ack, B is pending
            await asyncio.sleep(0.002)
        await r.stop()
        await asyncio.wait_for(task, 1)

    run(loop, go())
    assert [a.last_event_seq for a in acks] == [1]
    assert seen == ["e1"]  # B never reached the handler: it is redelivered from the cursor
