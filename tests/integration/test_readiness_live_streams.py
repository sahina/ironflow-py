"""The entity-streams group (#3003) through the Python RPC client.

The group needs no seed: this SDK creates and deletes its own stream, so no
other SDK's stream and not the shared seed entity can be reached. The
`readiness_seed` fixture only skips the module outside `make test-readiness-live`.
Tests run in file order; later ones read the stream the earlier ones built.
"""

from __future__ import annotations

import time
from typing import Any

import pytest
from protobuf.wkt import Struct

from ironflow import IronflowRPCError
from ironflow.rpc.v1 import (
    AppendEventRequest,
    CreateSnapshotRequest,
    DeleteStreamRequest,
    GetEntityHistoryRequest,
    GetSnapshotRequest,
    GetStreamInfoRequest,
    ListStreamsRequest,
    ReadStreamRequest,
)

pytestmark = pytest.mark.usefixtures("readiness_seed")

ENTITY_ID = f"readiness-streams-python-{time.time_ns()}"
ENTITY_TYPE = "readiness"


def _data(n: int) -> dict[str, Any]:
    # The non-ASCII value proves the SDK decodes event data as UTF-8.
    return {"n": n, "who": "José"}


def _append(rpc: Any, n: int, expected_version: int) -> Any:
    return rpc.streams.append_event(
        AppendEventRequest(
            entity_id=ENTITY_ID,
            entity_type=ENTITY_TYPE,
            event_name=f"readiness.step.{n}",
            data=Struct.from_python(_data(n)),
            expected_version=expected_version,
        )
    )


def _code(rpc_call: Any) -> str:
    with pytest.raises(IronflowRPCError) as exc:
        rpc_call()
    return str(exc.value.code)


def test_stream_append(rpc: Any) -> None:
    first = _append(rpc, 1, 0)
    assert first.entity_version == 1 and first.event_id
    second = _append(rpc, 2, 1)
    assert second.entity_version == 2 and second.event_id and second.event_id != first.event_id
    # Another writer got to version 2 first: the server answers aborted and writes nothing.
    assert _code(lambda: _append(rpc, 3, 1)) == "aborted"
    # 0 means "create only".
    assert _code(lambda: _append(rpc, 3, 0)) == "aborted"
    # "$" is reserved for platform events such as the delete tombstone.
    reserved = AppendEventRequest(entity_id=ENTITY_ID, entity_type=ENTITY_TYPE, event_name="$mine", expected_version=-1)
    assert _code(lambda: rpc.streams.append_event(reserved)) == "invalid_argument"
    assert rpc.streams.get_info(GetStreamInfoRequest(entity_id=ENTITY_ID)).version == 2


def test_stream_read(rpc: Any) -> None:
    page = rpc.streams.read_stream(ReadStreamRequest(entity_id=ENTITY_ID))
    assert page.total_count == 2
    assert [(e.name, e.entity_version, e.data.to_python()) for e in page.events] == [
        ("readiness.step.1", 1, _data(1)),
        ("readiness.step.2", 2, _data(2)),
    ]
    assert page.events[0].id and page.events[0].timestamp is not None
    last = rpc.streams.read_stream(ReadStreamRequest(entity_id=ENTITY_ID, direction="backward", limit=1))
    assert [e.entity_version for e in last.events] == [2]
    tail = rpc.streams.read_stream(ReadStreamRequest(entity_id=ENTITY_ID, from_version=2))
    assert [e.entity_version for e in tail.events] == [2]


def test_stream_info(rpc: Any) -> None:
    info = rpc.streams.get_info(GetStreamInfoRequest(entity_id=ENTITY_ID))
    assert (info.entity_id, info.entity_type, info.version, info.event_count) == (ENTITY_ID, ENTITY_TYPE, 2, 2)
    assert info.created_at is not None and info.updated_at is not None
    assert not info.deleted
    assert _code(lambda: rpc.streams.get_info(GetStreamInfoRequest(entity_id=ENTITY_ID + "-unknown"))) == "not_found"


def test_stream_list(rpc: Any) -> None:
    listed = [s for s in rpc.streams.list_streams(ListStreamsRequest()).streams if s.entity_id == ENTITY_ID]
    assert len(listed) == 1, listed
    assert (listed[0].entity_type, listed[0].version, listed[0].event_count) == (ENTITY_TYPE, 2, 2)
    assert listed[0].updated_at is not None


def test_stream_history(rpc: Any) -> None:
    history = rpc.streams.get_history(GetEntityHistoryRequest(entity_id=ENTITY_ID))
    assert [(e.event_name, e.entity_version, e.event_data.to_python()) for e in history.entries] == [
        ("readiness.step.1", 1, _data(1)),
        ("readiness.step.2", 2, _data(2)),
    ]
    assert history.entries[0].timestamp is not None
    one = rpc.streams.get_history(GetEntityHistoryRequest(entity_id=ENTITY_ID, limit=1))
    assert [e.entity_version for e in one.entries] == [1]
    # The window is inclusive, so each event's own time selects it.
    first, second = (e.timestamp for e in history.entries)
    upto = rpc.streams.get_history(GetEntityHistoryRequest(entity_id=ENTITY_ID, to_timestamp=first))
    assert [e.entity_version for e in upto.entries] == [1]
    since = rpc.streams.get_history(GetEntityHistoryRequest(entity_id=ENTITY_ID, from_timestamp=second))
    assert [e.entity_version for e in since.entries] == [2]


def test_snapshot_create_and_get(rpc: Any) -> None:
    def state(n: int) -> dict[str, Any]:
        return {"count": n, "who": "José"}

    for n in (1, 2):
        created = rpc.streams.create_snapshot(
            CreateSnapshotRequest(
                entity_id=ENTITY_ID, entity_type=ENTITY_TYPE, entity_version=n, state=Struct.from_python(state(n))
            )
        )
        assert created.snapshot_id
    latest = rpc.streams.get_snapshot(GetSnapshotRequest(entity_id=ENTITY_ID))
    assert latest.snapshot_id and latest.created_at is not None
    assert (latest.entity_id, latest.entity_type, latest.entity_version) == (ENTITY_ID, ENTITY_TYPE, 2)
    assert latest.state.to_python() == state(2)
    # The latest snapshot at or before the version asked for.
    earlier = rpc.streams.get_snapshot(GetSnapshotRequest(entity_id=ENTITY_ID, before_version=1))
    assert earlier.entity_version == 1 and earlier.state.to_python() == state(1)
    state_3 = Struct.from_python(state(3))
    past = CreateSnapshotRequest(entity_id=ENTITY_ID, entity_type=ENTITY_TYPE, entity_version=3, state=state_3)
    assert _code(lambda: rpc.streams.create_snapshot(past)) == "invalid_argument"
    again = CreateSnapshotRequest(
        entity_id=ENTITY_ID, entity_type=ENTITY_TYPE, entity_version=2, state=Struct.from_python(state(2))
    )
    assert _code(lambda: rpc.streams.create_snapshot(again)) == "already_exists"


def test_stream_delete(rpc: Any) -> None:
    assert rpc.streams.delete(DeleteStreamRequest(entity_id=ENTITY_ID)).entity_version == 3
    assert _code(lambda: _append(rpc, 3, -1)) == "failed_precondition"
    on_deleted = CreateSnapshotRequest(
        entity_id=ENTITY_ID, entity_type=ENTITY_TYPE, entity_version=2, state=Struct.from_python({})
    )
    assert _code(lambda: rpc.streams.create_snapshot(on_deleted)) == "failed_precondition"
    # The delete drops snapshots.
    assert _code(lambda: rpc.streams.get_snapshot(GetSnapshotRequest(entity_id=ENTITY_ID))) == "not_found"
    # Reads keep answering, and the tombstone is the last event.
    events = rpc.streams.read_stream(ReadStreamRequest(entity_id=ENTITY_ID)).events
    assert [(e.name, e.entity_version) for e in events] == [
        ("readiness.step.1", 1),
        ("readiness.step.2", 2),
        ("$stream.deleted", 3),
    ]
    info = rpc.streams.get_info(GetStreamInfoRequest(entity_id=ENTITY_ID))
    assert info.version == 3 and info.deleted
    assert ENTITY_ID not in [s.entity_id for s in rpc.streams.list_streams(ListStreamsRequest()).streams]
    shown = rpc.streams.list_streams(ListStreamsRequest(include_deleted=True)).streams
    assert [s.deleted for s in shown if s.entity_id == ENTITY_ID] == [True]
    assert _code(lambda: rpc.streams.delete(DeleteStreamRequest(entity_id=ENTITY_ID))) == "failed_precondition"
    # Purge removes the events below the tombstone; the tombstone stays.
    assert rpc.streams.delete(DeleteStreamRequest(entity_id=ENTITY_ID, purge=True)).entity_version == 3
    after = rpc.streams.read_stream(ReadStreamRequest(entity_id=ENTITY_ID)).events
    assert [e.name for e in after] == ["$stream.deleted"]
