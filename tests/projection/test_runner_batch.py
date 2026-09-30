from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from protobuf.wkt import Struct

from ironflow._gen.projection_pb import GetProjectionResponse
from ironflow._gen.projection_pb import ProjectionEvent as PE
from ironflow.projection import ProjectionEvent, create_projection
from ironflow.projection._runner import ProjectionRunner, _event_from_proto
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


def test_failed_reload_after_failed_save_keeps_committed_state(loop: asyncio.AbstractEventLoop) -> None:
    svc = FakeProjectionService()
    svc.script["save_projection_state"] = [ConnectError(Code.UNAVAILABLE, "save 503")]
    svc.script["get_projection"] = [ConnectError(Code.UNAVAILABLE, "get 503")]
    r = runner(counter(), svc)
    r._state = {"count": 10}
    with pytest.raises(ConnectError):
        run(loop, r._flush([ev(1)]))
    assert r._state == {"count": 10}  # never initial_state(): the next save would overwrite the server
