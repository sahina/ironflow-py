"""The runner side of ADR 0164: the delivery position goes on every poll and
stream, rises only after a whole batch commits, stays where it was when a batch
fails, and an old server stops the runner before it applies anything."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError

from ironflow._gen.projection_pb import (
    AckProjectionEventsResponse,
    PollProjectionEventsResponse,
    ProjectionEventKind,
)
from ironflow._gen.projection_pb import (
    ProjectionEvent as PE,
)
from ironflow.projection._runner import ProjectionRefusedError
from ironflow.rpc._runtime import ServerTooOldError
from tests.projection.fake_service import FakeProjectionService
from tests.projection.test_runner_transport import frame, make, run_until
from tests.worker.conftest import run

UNAVAILABLE = ConnectError(Code.UNAVAILABLE, "reset")


def test_stream_position_rises_only_after_commit(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    mark = PE(kind=ProjectionEventKind.HEARTBEAT, seq=7)
    svc.streams = [[frame(1), mark, UNAVAILABLE], [frame(8)], []]
    svc.script["ack_projection_events"] = [AckProjectionEventsResponse(), UNAVAILABLE]
    r = make(svc, batch_size=1)
    run(loop, run_until(r, lambda: len(svc.requests("stream_projection_events")) >= 3))
    # Open 1 commits e1 and then the mark 7. Open 2 fails the ack of e8, so open 3
    # starts at 7 again.
    assert [q.position for q in svc.requests("stream_projection_events")][:3] == [0, 7, 7]
    assert all(q.has_field("position") for q in svc.requests("stream_projection_events")), "a 0 is sent, not left out"


def test_poll_position_rises_only_after_commit(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.streams = [ConnectError(Code.UNIMPLEMENTED, "no stream")]
    page = PollProjectionEventsResponse(events=[frame(41)], last_event_seq=45, position=40)
    svc.script["poll_projection_events"] = [
        PollProjectionEventsResponse(last_event_seq=40, position=0),  # other partitions only
        page,
        PollProjectionEventsResponse(events=[frame(41)], last_event_seq=45, position=40),
        PollProjectionEventsResponse(last_event_seq=45, position=45),
    ]
    svc.script["ack_projection_events"] = [UNAVAILABLE]
    r = make(svc)
    run(loop, run_until(r, lambda: len(svc.requests("poll_projection_events")) >= 4))
    assert [q.position for q in svc.requests("poll_projection_events")][:4] == [0, 40, 40, 45]
    assert all(q.has_field("position") for q in svc.requests("poll_projection_events"))


class _OldServer(FakeProjectionService):
    """A server older than ADR 0164: no position echo, no OPEN frame."""

    def __init__(self, poll: bool) -> None:
        super().__init__()
        self.poll = poll

    async def poll_projection_events(self, request: Any, **_: Any) -> Any:
        self.calls.append(("poll_projection_events", request))
        return PollProjectionEventsResponse(events=[frame(1)], last_event_seq=1)

    async def stream_projection_events(self, request: Any, **kw: Any) -> AsyncIterator[Any]:
        if self.poll:
            raise ConnectError(Code.UNIMPLEMENTED, "no stream")
        yield frame(1)
        await asyncio.sleep(3600)


@pytest.mark.parametrize("poll", [True, False])
def test_old_server_stops_the_runner(loop: asyncio.AbstractEventLoop, poll: bool) -> None:
    svc, seen = _OldServer(poll), []
    r = make(svc, seen=seen)

    async def go() -> None:
        with pytest.raises(ServerTooOldError, match="position"):
            await asyncio.wait_for(r.run(), 2)

    run(loop, go())
    assert seen == []
    assert svc.requests("ack_projection_events") == []


class _PreOpenEndServer(FakeProjectionService):
    async def stream_projection_events(self, request: Any, **kw: Any) -> AsyncIterator[Any]:
        self.calls.append(("stream_projection_events", request))
        return
        yield


def test_empty_stream_stops_the_runner(loop: asyncio.AbstractEventLoop) -> None:
    svc = _PreOpenEndServer()
    r = make(svc)

    async def go() -> None:
        with pytest.raises(ServerTooOldError, match="position"):
            await asyncio.wait_for(r.run(), 2)

    run(loop, go())
    assert len(svc.requests("stream_projection_events")) == 1


REFUSED = ConnectError(Code.FAILED_PRECONDITION, "projection is managed and its stream is behind its saved state")


@pytest.mark.parametrize("poll", [True, False])
def test_failed_precondition_stops_the_runner(loop: asyncio.AbstractEventLoop, poll: bool) -> None:
    svc = FakeProjectionService()
    svc.streams = [ConnectError(Code.UNIMPLEMENTED, "no stream") if poll else [REFUSED]]
    svc.script["poll_projection_events"] = [REFUSED]
    r = make(svc)

    async def go() -> None:
        with pytest.raises(ProjectionRefusedError, match="stream is behind"):
            await asyncio.wait_for(r.run(), 5)

    run(loop, go())
    method = "poll_projection_events" if poll else "stream_projection_events"
    assert len(svc.requests(method)) == 1


def test_refused_state_load_stops_the_runner(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.script["get_projection"] = [REFUSED]
    r = make(svc, handler=lambda s, e, c: s, initial_state=dict)

    async def go() -> None:
        with pytest.raises(ProjectionRefusedError, match="stream is behind"):
            await asyncio.wait_for(r.run(), 5)

    run(loop, go())
    assert len(svc.requests("get_projection")) == 1
