from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from protobuf.wkt import Struct, Value

from ironflow._gen.projection_pb import (
    GetProjectionResponse,
    PollProjectionEventsResponse,
    SaveProjectionStateResponse,
)
from ironflow._gen.projection_pb import ProjectionEvent as PE
from ironflow.projection import ProjectionEvent, create_projection
from ironflow.projection._runner import (
    ProjectionAuthError,
    ProjectionRunner,
    _event_from_proto,
)
from tests.projection.fake_service import FakeProjectionService
from tests.worker.conftest import run


def ev(i: int, partition: str = "") -> ProjectionEvent:
    meta = {"__partition": partition} if partition else {}
    return ProjectionEvent(id=f"e{i}", name="inc", data={"n": 1}, seq=i, timestamp="2026-09-27T10:00:00Z",
                           source="", metadata=meta)


def runner(proj: Any, svc: FakeProjectionService) -> ProjectionRunner:
    return ProjectionRunner(proj, svc, dict, logging.getLogger("t"))  # type: ignore[arg-type]


def counter(**kw: Any) -> Any:
    return create_projection(name="c", events=["inc"], initial_state=lambda: {"count": 0},
                             handler=lambda s, e, c: {"count": s["count"] + e.data["n"]}, **kw)


def test_managed_saves_each_partition(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    r = runner(counter(), svc)
    r._state = {"count": 10}
    run(loop, r._flush([ev(1), ev(2, "p1"), ev(3)]))
    saves = {s.partition_key: s for s in svc.requests("save_projection_state")}
    assert saves["__global__"].state.to_python() == {"count": 12}
    assert saves["__global__"].last_event_seq == 3 and saves["__global__"].last_event_id == "e3"
    assert saves["p1"].state.to_python() == {"count": 1}
    assert r._state == {"count": 12}


@pytest.mark.parametrize(("fail", "expected"), [
    (False, [("a", 1, 1), ("b", 2, 1), ("a", 3, 2)]),
    # The second save fails: the cursor stays at seq 1, so b@2 and a@3 are redelivered.
    (True, [("a", 1, 1), ("b", 2, 1)]),
])
def test_managed_saves_runs_in_seq_order(
    loop: asyncio.AbstractEventLoop, fail: bool, expected: list[tuple[str, int, int]],
) -> None:
    svc = FakeProjectionService()
    if fail:
        svc.script["save_projection_state"] = [SaveProjectionStateResponse(), ConnectError(Code.UNAVAILABLE, "down")]
    flush = runner(counter(), svc)._flush([ev(3, "a"), ev(1, "a"), ev(2, "b")])  # out of order on purpose
    if fail:
        with pytest.raises(ConnectError):
            run(loop, flush)
    else:
        run(loop, flush)
    assert [
        (s.partition_key, s.last_event_seq, s.state.to_python()["count"])
        for s in svc.requests("save_projection_state")
    ] == expected


def test_managed_partitions_resume_across_batches(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.script["get_projection"] = [
        GetProjectionResponse(state=Struct.from_python({"count": 40})),
        GetProjectionResponse(state=Struct.from_python({"count": 90})),
    ]
    run(loop, runner(counter(), svc)._flush([ev(1, "a"), ev(2, "a"), ev(3, "b")]))
    first = svc.requests("save_projection_state")
    assert [s.state.to_python() for s in first] == [{"count": 42}, {"count": 91}]
    svc.script["get_projection"] = [GetProjectionResponse(state=s.state) for s in first]
    run(loop, runner(counter(), svc)._flush([ev(4, "a"), ev(5, "b")]))
    assert [s.state.to_python() for s in svc.requests("save_projection_state")] == [
        {"count": 42}, {"count": 91}, {"count": 43}, {"count": 92},
    ]
    assert [(r.name, r.partition) for r in svc.requests("get_projection")] == [
        ("c", "a"), ("c", "b"), ("c", "a"), ("c", "b"),
    ]


@pytest.mark.parametrize(("response", "expected"), [
    (GetProjectionResponse(), {"initial": True}),
    (GetProjectionResponse(state=Struct.from_python({})), {"initial": True}),
    (GetProjectionResponse(state_value=Value.from_python([1, 2])), [1, 2]),
    (GetProjectionResponse(state_value=Value.from_python(0)), 0),
    (GetProjectionResponse(state_value=Value.from_python(False)), False),
])
def test_managed_partition_preserves_saved_state(
    loop: asyncio.AbstractEventLoop, response: GetProjectionResponse, expected: Any,
) -> None:
    svc = FakeProjectionService()
    svc.script["get_projection"] = [response]
    proj = create_projection(name="c", events=["inc"], initial_state=lambda: {"initial": True},
                             handler=lambda s, e, c: {"loaded": s})
    run(loop, runner(proj, svc)._flush([ev(1, "a")]))
    [saved] = svc.requests("save_projection_state")
    assert saved.state.to_python() == {"loaded": expected}


@pytest.mark.parametrize("code", [Code.NOT_FOUND, Code.UNAVAILABLE, Code.UNAUTHENTICATED, Code.PERMISSION_DENIED])
def test_managed_partition_load_failure_does_not_save(loop: asyncio.AbstractEventLoop, code: Code) -> None:
    svc = FakeProjectionService()
    error = ConnectError(code, "partition load failed")
    # Partition a loads fine, b fails: nothing may be saved, or the cursor skips b's events.
    svc.script["get_projection"] = [GetProjectionResponse(), error]
    handled: list[str] = []

    def h(s: Any, e: Any, c: Any) -> Any:
        handled.append(e.id)
        return s

    proj = create_projection(name="c", events=["inc"], initial_state=dict, handler=h)
    with pytest.raises(ConnectError):
        run(loop, runner(proj, svc)._flush([ev(1, "a"), ev(2, "b")]))
    assert handled == []
    assert svc.requests("save_projection_state") == []
    assert [r.partition for r in svc.requests("get_projection")] == ["a", "b"]


@pytest.mark.parametrize("code", [Code.UNAUTHENTICATED, Code.PERMISSION_DENIED])
@pytest.mark.parametrize("transport", ["poll", "stream"])
def test_partition_load_auth_error_stops_runner(
    loop: asyncio.AbstractEventLoop, code: Code, transport: str,
) -> None:
    svc = FakeProjectionService()
    svc.script["get_projection"] = [GetProjectionResponse(), ConnectError(code, "partition load denied")]
    event = PE(id="e1", name="inc", seq=1, data=Struct.from_python({"n": 1}),
               metadata=Struct.from_python({"__partition": "a"}))
    if transport == "poll":
        svc.streams = [ConnectError(Code.UNIMPLEMENTED, "poll only")]
        svc.script["poll_projection_events"] = [PollProjectionEventsResponse(events=[event])]
    else:
        svc.streams = [[event]]
    handled: list[str] = []

    def h(s: Any, e: Any, c: Any) -> Any:
        handled.append(e.id)
        return s

    proj = create_projection(name="c", events=["inc"], initial_state=dict, handler=h, batch_size=1)
    with pytest.raises(ProjectionAuthError):
        run(loop, asyncio.wait_for(runner(proj, svc).run(), 1))
    assert handled == []
    assert svc.requests("save_projection_state") == []


def test_state_is_copied_per_handler_call(loop: asyncio.AbstractEventLoop) -> None:
    seen: list[Any] = []

    def h(s: Any, e: Any, c: Any) -> Any:
        seen.append(s)
        s["items"].append(e.id)  # in-place mutation must not leak into earlier snapshots
        return s

    svc = FakeProjectionService()
    r = runner(create_projection(name="c", events=["inc"], initial_state=lambda: {"items": []}, handler=h), svc)
    r._state = {"items": []}
    run(loop, r._flush([ev(1), ev(2)]))
    assert seen[0] == {"items": ["e1"]} and seen[1] == {"items": ["e1", "e2"]}
    assert seen[0] is not seen[1]


def test_save_failure_keeps_committed_state_and_reloads(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.script["save_projection_state"] = [RuntimeError("503")]
    svc.script["get_projection"] = [GetProjectionResponse(state=Struct.from_python({"count": 99}))]
    r = runner(counter(), svc)
    r._state = {"count": 10}
    with pytest.raises(RuntimeError):
        run(loop, r._flush([ev(1)]))
    assert r._state == {"count": 99}  # reloaded, never the uncommitted 11


def test_handler_error_means_no_save(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()

    def h(s: Any, e: Any, c: Any) -> Any:
        raise ValueError("bad")

    r = runner(create_projection(name="c", events=["inc"], initial_state=dict, handler=h), svc)
    r._state = {}
    with pytest.raises(ValueError):
        run(loop, r._flush([ev(1)]))
    assert svc.requests("save_projection_state") == []


def test_managed_empty_global_state_means_initial_state(loop: asyncio.AbstractEventLoop) -> None:
    # A handler that returned {} leaves {} in memory; the next batch starts from initial_state().
    svc = FakeProjectionService()
    r = runner(counter(), svc)
    r._state = {}
    run(loop, r._flush([ev(1)]))
    [saved] = svc.requests("save_projection_state")
    assert saved.state.to_python() == {"count": 1}


def test_managed_handler_returning_none_fails(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    r = runner(create_projection(name="c", events=["inc"], initial_state=dict, handler=lambda s, e, c: None), svc)
    r._state = {}
    with pytest.raises(TypeError, match="returned None"):
        run(loop, r._flush([ev(1)]))
    assert svc.requests("save_projection_state") == []


def test_external_runs_handler_then_acks_last(loop: asyncio.AbstractEventLoop) -> None:
    seen: list[str] = []

    async def h(e: Any, c: Any) -> None:
        seen.append(f"{c.projection.name}:{e.id}:{c.event.seq}")

    svc = FakeProjectionService()
    run(loop, runner(create_projection(name="x", events=["inc"], handler=h), svc)._flush([ev(1), ev(2)]))
    assert seen == ["x:e1:1", "x:e2:2"]
    [ack] = svc.requests("ack_projection_events")
    assert (ack.last_event_id, ack.last_event_seq) == ("e2", 2)


def test_external_handler_error_means_no_ack(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()

    def h(e: Any, c: Any) -> None:
        raise RuntimeError("side effect failed")

    with pytest.raises(RuntimeError):
        run(loop, runner(create_projection(name="x", events=["inc"], handler=h), svc)._flush([ev(1)]))
    assert svc.requests("ack_projection_events") == []


def test_event_from_proto_reads_data_value_and_struct() -> None:
    from protobuf.wkt import Timestamp, Value
    obj = _event_from_proto(PE(id="a", name="n", seq=4, data=Struct.from_python({"k": 1}),
                               metadata=Struct.from_python({"__partition": "p"}),
                               timestamp=Timestamp(seconds=0)))
    lst = _event_from_proto(PE(id="b", name="n", seq=5, data_value=Value.from_python([1, 2])))
    assert obj.data == {"k": 1} and obj.metadata == {"__partition": "p"} and obj.seq == 4
    # iso_utc always renders millisecond precision, even for zero nanos.
    assert obj.timestamp == "1970-01-01T00:00:00.000Z"
    assert lst.data == [1, 2] and lst.metadata == {} and lst.timestamp == ""


def test_load_state_uses_server_state_when_present(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.script["get_projection"] = [GetProjectionResponse(state=Struct.from_python({"count": 5}))]
    r = runner(counter(), svc)
    run(loop, r._load_state())
    assert r._state == {"count": 5}
    svc.script["get_projection"] = [RuntimeError("down")]
    run(loop, r._load_state())
    assert r._state == {"count": 5}  # a failed load keeps the current state; it never wipes it


def test_load_state_gives_integral_numbers_back_as_int(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.script["get_projection"] = [GetProjectionResponse(state=Struct.from_python({"n": 3, "xs": [1, 2.5]}))]
    r = runner(counter(), svc)
    run(loop, r._load_state())
    assert type(r._state["n"]) is int and r._state["xs"] == [1, 2.5] and type(r._state["xs"][0]) is int


def test_load_state_not_found_means_initial_state(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.script["get_projection"] = [ConnectError(Code.NOT_FOUND, "no row")]
    r = runner(counter(), svc)
    r._state = {"count": 5}
    run(loop, r._load_state())
    assert r._state == {"count": 0}  # first run: no server row yet


def test_load_state_empty_object_means_initial_state(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.script["get_projection"] = [GetProjectionResponse(state=Struct.from_python({}))]
    r = runner(counter(), svc)
    r._state = {"count": 5}
    run(loop, r._load_state())
    assert r._state == {"count": 0}


def test_failed_reload_after_failed_save_keeps_committed_state(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.script["save_projection_state"] = [ConnectError(Code.UNAVAILABLE, "save 503")]
    svc.script["get_projection"] = [ConnectError(Code.UNAVAILABLE, "get 503")]
    r = runner(counter(), svc)
    r._state = {"count": 10}
    with pytest.raises(ConnectError):
        run(loop, r._flush([ev(1)]))
    assert r._state == {"count": 10}  # never initial_state(): the next save would overwrite the server
