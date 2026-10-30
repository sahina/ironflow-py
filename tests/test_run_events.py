"""Run frames and the version filter that keeps a watcher on the newest status (#3283)."""

from __future__ import annotations

from typing import Any

import pytest
from protobuf.wkt import struct_pb

from ironflow import RunEvent, RunVersionFilter
from ironflow.rpc.v1 import SubscriptionEvent


def frame(run_id: str, leaf: str, version: object = None, **extra: Any) -> SubscriptionEvent:
    data: dict[str, Any] = {"id": run_id, **extra}
    if version is not None:
        data["version"] = version
    return SubscriptionEvent(topic=f"system.run.{run_id}.{leaf}", data=struct_pb.Struct.from_python(data))


def test_drops_a_lower_version_arriving_late() -> None:
    keep = RunVersionFilter()
    seen = [e.topic for e in (frame("r1", "completed", 5), frame("r1", "updated", 3)) if keep.accept(e)]
    assert seen == ["system.run.r1.completed"]


def test_passes_in_order_frames_and_a_tie() -> None:
    keep = RunVersionFilter()
    # One write publishes both .failed and .updated, so a tie must pass.
    assert all(keep.accept(e) for e in (
        frame("r1", "created", 1), frame("r1", "updated", 2),
        frame("r1", "failed", 4), frame("r1", "updated", 4),
    ))


def test_ranks_each_run_on_its_own() -> None:
    keep = RunVersionFilter()
    assert keep.accept(frame("r1", "completed", 9))
    assert keep.accept(frame("r2", "updated", 2))


@pytest.mark.parametrize("version", [None, 0, -1, 1.5, "7", True])
def test_keeps_an_unversioned_frame_and_never_ranks_it(version: object) -> None:
    keep = RunVersionFilter()
    assert keep.accept(frame("r1", "completed", 5))
    assert keep.accept(frame("r1", "updated", version))
    assert not keep.accept(frame("r1", "updated", 4))  # the unversioned frame did not lower the bar


def test_passes_step_and_foreign_topics() -> None:
    keep = RunVersionFilter()
    keep.accept(frame("r1", "completed", 5))
    for topic in ("system.run.r1.step.s1.completed", "events:order.created", "system.function.f1.updated"):
        event = SubscriptionEvent(topic=topic, data=struct_pb.Struct.from_python({"version": 1}))
        assert keep.accept(event)


def test_deletion_and_independent_step_ordering() -> None:
    keep = RunVersionFilter(max_runs=2)
    assert keep.accept(frame("r1", "deleted"))
    assert keep.accept(frame("r1", "deleted"))
    for version in (None, 0, 1, 6):
        assert not keep.accept(frame("r1", "updated", version))
        assert not keep.accept(frame("r1", "step.s1.updated", version))
    assert keep.accept(frame("r2", "step.s1.updated", 5))
    assert not keep.accept(frame("r2", "step.s1.updated", 4))
    assert keep.accept(frame("r2", "step.s1.completed", 5))
    assert keep.accept(frame("r2", "step.s1.updated"))
    assert keep.accept(frame("r2", "step.s2.updated", 1))
    assert keep.accept(frame("r1", "updated", 1))  # the bounded cache evicted its tombstone


def test_forgets_the_least_recently_seen_run_past_max_runs() -> None:
    keep = RunVersionFilter(max_runs=2)
    keep.accept(frame("a", "updated", 5))
    keep.accept(frame("b", "updated", 5))
    keep.accept(frame("a", "updated", 6))  # refreshes a, so b is the oldest
    keep.accept(frame("c", "updated", 5))  # evicts b
    assert not keep.accept(frame("a", "updated", 4))
    assert keep.accept(frame("b", "updated", 1))


def test_run_event_from_a_subscription_frame() -> None:
    event = frame("r1", "updated", 3, status="running", attempt=1, maxAttempts=3, functionId="fn")
    run = RunEvent.from_data(event.data)
    assert (run.id, run.status, run.version, run.attempt, run.max_attempts, run.function_id) == (
        "r1", "running", 3, 1, 3, "fn")
    assert RunEvent.from_data({"id": "r1", "status": "deleted"}).version == 0


def test_a_watcher_ends_on_completed_when_running_arrives_late() -> None:
    keep = RunVersionFilter()
    statuses = [
        RunEvent.from_data(e.data).status
        for e in (
            frame("run-1", "completed", 5, status="completed"),  # node B's frame lands first
            frame("run-1", "updated", 4, status="running"),
        )
        if keep.accept(e)
    ]
    assert statuses == ["completed"]


def test_dotted_step_ordering() -> None:
    keep = RunVersionFilter()
    assert keep.accept(frame("r", "step.r:memory.get:0.completed", 5))
    assert not keep.accept(frame("r", "step.r:memory.get:0.updated", 4))
    assert keep.accept(frame("r", "step.r:memory.put:0.updated", 1))
    assert keep.accept(frame("r", "step.r:memory.get:0.updated", 5))


def test_dotted_step_after_deletion() -> None:
    keep = RunVersionFilter()
    keep.accept(frame("r", "deleted"))
    for version in (None, 0, 6):
        assert not keep.accept(frame("r", "step.r:memory.get:0.updated", version))
