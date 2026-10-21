"""Reads one real seeded run back through the Python helpers (#2942).

`make test-readiness-live` starts a server, runs tests/readiness-live/seed and
sets IRONFLOW_READINESS_SEED. Without it this module skips, so
`make test-python-integration` (same server variables, no seed) is unaffected.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any

import pytest
from protobuf.wkt import Duration, FieldMask, Struct, Timestamp

from ironflow import IronflowClient, IronflowRPCError, is_redacted
from ironflow.projection import create_projection
from ironflow.rpc.v1 import (
    CancelRebuildRequest,
    CancelRunRequest,
    ConsumerGroup,
    CreateConsumerGroupRequest,
    CreateSQLProjectionRequest,
    CreateWebhookSourceRequest,
    DeleteConsumerGroupRequest,
    DeleteRunRequest,
    DeleteRunsRequest,
    DeleteWebhookSourceRequest,
    DisableWebhookSignatureVerificationRequest,
    ExpireWebhookSecretPrevRequest,
    FunctionStatus,
    GetAuditTrailRequest,
    GetAuthAuditTrailRequest,
    GetConsumerGroupRequest,
    GetFunctionAtVersionRequest,
    GetFunctionRequest,
    GetPausedStateRequest,
    GetProjectionRequest,
    GetProjectionStatusRequest,
    GetRebuildJobRequest,
    GetRunRequest,
    GetRunStateAtRequest,
    GetRunStepsRequest,
    GetRunTimelineRequest,
    GetStepOutputAtRequest,
    ListConsumerGroupsRequest,
    ListProjectionsRequest,
    ListRunsRequest,
    ListWebhookDeliveriesRequest,
    PauseProjectionRequest,
    PauseRunRequest,
    QuerySQLProjectionRequest,
    RebuildProjectionRequest,
    RedactEventRequest,
    RedactRunRequest,
    RedactStepRequest,
    ResumeProjectionRequest,
    ResumeRunRequest,
    RotateWebhookIngestTokenRequest,
    RotateWebhookSecretRequest,
    RunStatus,
    StepType,
    TriggerBatchRequest,
    TriggerRequest,
    TriggerSyncRequest,
    UpdateConsumerGroupRequest,
    WaitForEventRequest,
    WaitItem,
    WaitProjectionCatchupBatchRequest,
    WaitProjectionCatchupRequest,
)
from ironflow.rpc.v1 import (
    TestUpcastRequest as UpcastRequest,  # aliased: pytest collects Test* names
)
from ironflow.worker import Worker

SEED_PATH = os.environ.get("IRONFLOW_READINESS_SEED")
STEP_EVENTS = ["run.created", "step.started", "step.completed", "run.status_changed"]
pytestmark = pytest.mark.skipif(not SEED_PATH, reason="IRONFLOW_READINESS_SEED not set; run `make test-readiness-live`")


@pytest.fixture
def seed() -> dict[str, Any]:
    assert SEED_PATH is not None
    with open(SEED_PATH, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def rest(server_url: str, api_key: str) -> IronflowClient:
    return IronflowClient(server_url, api_key=api_key)


def _ts(value: str) -> Timestamp:
    return Timestamp.from_datetime(datetime.fromisoformat(value.replace("Z", "+00:00")))


# #3051: these hold runs belong to the Python suite alone, so the tests below
# pause, cancel, redact and delete them. pytest runs them first, in file order.
def _hold(seed: dict[str, Any]) -> dict[str, Any]:
    return seed["holds"]["python"]


def _run_ids(runs: Any) -> list[str]:
    return sorted(r.id for r in runs)


def test_run_get(rpc: Any, seed: dict[str, Any]) -> None:
    run = rpc.runs.get(GetRunRequest(id=seed["runId"]))
    assert (run.id, run.function_id, run.status) == (seed["runId"], seed["functionId"], RunStatus.COMPLETED)
    assert run.input.to_python() == {} and run.output.to_python() == seed["stepOutput"]
    assert run.ended_at is not None


def test_run_list(rpc: Any, seed: dict[str, Any]) -> None:
    hold = _hold(seed)
    own = rpc.runs.list(ListRunsRequest(function_id=seed["functionId"]))
    assert _run_ids(own.runs) == [seed["runId"]] and own.total_count == 1
    both = sorted([hold["runId"], hold["spareRunId"]])
    waiting = rpc.runs.list(ListRunsRequest(function_id=hold["functionId"], status=RunStatus.WAITING))
    assert _run_ids(waiting.runs) == both and waiting.total_count == 2
    first = rpc.runs.list(ListRunsRequest(function_id=hold["functionId"], limit=1))
    assert len(first.runs) == 1 and first.next_cursor
    second = rpc.runs.list(ListRunsRequest(function_id=hold["functionId"], limit=1, cursor=first.next_cursor))
    assert _run_ids([*first.runs, *second.runs]) == both


def test_run_steps(rpc: Any, seed: dict[str, Any]) -> None:
    hold = _hold(seed)
    (step,) = rpc.runs.get_steps(GetRunStepsRequest(run_id=seed["runId"])).steps
    assert (step.step_id, step.run_id, step.stored_status) == (seed["stepId"], seed["runId"], "completed")
    assert step.output.to_python() == seed["stepOutput"]
    note, wait = rpc.runs.get_steps(GetRunStepsRequest(run_id=hold["runId"])).steps
    assert (note.id, note.stored_status, note.output.to_python()) == (hold["noteStepId"], "completed", {"sdk": "python"})
    assert (wait.step_type, wait.stored_status, wait.wait_event_name) == (
        StepType.WAIT_FOR_EVENT,
        "waiting",
        f"readiness.never.{hold['functionId']}",
    )
    assert wait.wait_timeout is not None


def test_run_pause(rpc: Any, seed: dict[str, Any]) -> None:
    run_id = _hold(seed)["runId"]
    # A waiting run has no next step boundary, so it is held at once.
    assert rpc.runs.pause(PauseRunRequest(run_id=run_id)).status == "paused"
    assert rpc.runs.get(GetRunRequest(id=run_id)).status == RunStatus.PAUSED


def test_run_paused_state(rpc: Any, seed: dict[str, Any]) -> None:
    hold = _hold(seed)
    state = rpc.runs.get_paused_state(GetPausedStateRequest(run_id=hold["runId"]))
    assert state.pause_reason == "injection"
    (step,) = state.steps
    assert (step.id, step.status, step.injected) == (hold["noteStepId"], "completed", False)
    assert step.completed_at is not None, step
    assert json.loads(step.output) == {"sdk": "python"}


def test_run_resume(rpc: Any, seed: dict[str, Any]) -> None:
    """The wait step is still open, so resume puts the run back on its wait."""
    run_id = _hold(seed)["runId"]
    run = rpc.runs.resume(ResumeRunRequest(run_id=run_id))
    assert (run.id, run.status) == (run_id, RunStatus.WAITING)


def test_run_cancel(rpc: Any, seed: dict[str, Any]) -> None:
    run_id = _hold(seed)["runId"]
    run = rpc.runs.cancel(CancelRunRequest(id=run_id, reason="readiness"))
    assert (run.id, run.status) == (run_id, RunStatus.CANCELLED)
    assert rpc.runs.get(GetRunRequest(id=run_id)).status == RunStatus.CANCELLED


def test_payload_redact(rpc: Any, rest: IronflowClient, seed: dict[str, Any]) -> None:
    hold = _hold(seed)
    rpc.runs.redact_step(RedactStepRequest(step_id=hold["noteStepId"]))
    rpc.runs.redact(RedactRunRequest(run_id=hold["runId"]))
    rpc.events.redact(RedactEventRequest(event_id=hold["eventId"]))
    note = rpc.runs.get_steps(GetRunStepsRequest(run_id=hold["runId"])).steps[0]
    assert is_redacted(note.output.to_python()), note.output
    run = rpc.runs.get(GetRunRequest(id=hold["runId"]))
    assert is_redacted(run.input.to_python()), run.input
    assert is_redacted(rest.events_get(hold["eventId"])["data"])


def test_run_delete(rpc: Any, seed: dict[str, Any]) -> None:
    hold = _hold(seed)
    rpc.runs.delete(DeleteRunRequest(id=hold["runId"]))
    with pytest.raises(IronflowRPCError) as exc:
        rpc.runs.get(GetRunRequest(id=hold["runId"]))
    assert exc.value.code == "not_found", exc.value.code
    rpc.runs.cancel(CancelRunRequest(id=hold["spareRunId"], reason="readiness"))
    assert rpc.runs.delete_many(DeleteRunsRequest(function_id=hold["functionId"])).deleted == 1
    assert rpc.runs.delete_many(DeleteRunsRequest(function_id=hold["functionId"])).deleted == 0


def test_event_emit_batch_and_event_names(rpc: Any, rest: IronflowClient) -> None:
    suffix = str(time.time_ns())
    a, b = f"pysdk.readiness.a.{suffix}", f"pysdk.readiness.b.{suffix}"
    resp = rpc.runs.trigger_batch(TriggerBatchRequest(events=[TriggerRequest(event=a), TriggerRequest(event=b)]))
    ids = [r.event_id for r in resp.results]
    assert len(ids) == 2 and ids[0] and ids[0] != ids[1], ids

    counts = {n["name"]: n["count"] for n in rest.events_list_names()["names"]}
    assert counts.get(a) == 1 and counts.get(b) == 1, (counts.get(a), counts.get(b))


def test_event_emit_list_and_get(rpc: Any, rest: IronflowClient) -> None:
    name = f"pysdk.readiness.emit.{time.time_ns()}"
    res = rpc.runs.trigger(TriggerRequest(event=name, data=Struct.from_python({"i": 7})))
    # No function listens on a unique name, so the event is stored with no runs.
    assert res.event_id and list(res.run_ids) == [], res

    page = rest.events_list(name=name, limit=5)
    assert page["count"] == 1 and page["limit"] == 5, page
    assert [e["id"] for e in page["events"]] == [res.event_id]

    got = rest.events_get(res.event_id)
    assert got["id"] == res.event_id and got["name"] == name and got["data"] == {"i": 7}, got


def test_event_emit_sync(rpc: Any, rest: IronflowClient) -> None:
    name = f"pysdk.readiness.sync.{time.time_ns()}"
    res = rpc.runs.trigger_sync(TriggerSyncRequest(event=name, timeout_ms=5000))
    assert res.event_id and list(res.results) == [], res
    assert rest.events_get(res.event_id)["name"] == name


def test_function_at_version(rpc: Any, seed: dict[str, Any]) -> None:
    entry = rpc.functions.get_at_version(
        GetFunctionAtVersionRequest(function_id=seed["functionId"], version=seed["pausedVersion"])
    ).entry
    assert entry.function_id == seed["functionId"]
    assert entry.entity_version == seed["pausedVersion"]
    assert entry.change_type == "status_change"
    assert entry.function_snapshot.status == FunctionStatus.PAUSED
    # The history read must differ from the current function.
    current = rpc.functions.get(GetFunctionRequest(id=seed["functionId"]))
    assert current.status == FunctionStatus.ACTIVE


def test_run_audit(rpc: Any, seed: dict[str, Any]) -> None:
    trail = rpc.audit.get_trail(GetAuditTrailRequest(run_id=seed["runId"]))
    assert [e.event_type for e in trail.events] == STEP_EVENTS
    assert trail.total_count == 4
    for e in trail.events:
        assert e.run_id == seed["runId"] and e.function_id == seed["functionId"]
        if e.event_type.startswith("step."):
            assert e.step_id == seed["stepId"]

    first = rpc.audit.get_trail(GetAuditTrailRequest(run_id=seed["runId"], limit=2))
    assert len(first.events) == 2 and first.total_count == 4 and first.next_cursor
    second = rpc.audit.get_trail(GetAuditTrailRequest(run_id=seed["runId"], limit=2, cursor=first.next_cursor))
    assert [e.event_type for e in [*first.events, *second.events]] == STEP_EVENTS


def test_run_streams(rest: IronflowClient, seed: dict[str, Any]) -> None:
    assert rest.runs_list_streams(seed["runId"])["entity_ids"] == [seed["entityId"]]


def test_tt_step_output_at(rpc: Any, seed: dict[str, Any]) -> None:
    with pytest.raises(IronflowRPCError) as exc:
        rpc.time_travel.get_step_output_at(
            GetStepOutputAtRequest(run_id=seed["runId"], step_id=seed["stepId"], timestamp=_ts(seed["beforeRun"]))
        )
    assert exc.value.code == "not_found", exc.value.code

    snap = rpc.time_travel.get_step_output_at(
        GetStepOutputAtRequest(run_id=seed["runId"], step_id=seed["stepId"], timestamp=_ts(seed["afterStep"]))
    )
    assert snap.status == "completed"
    assert json.loads(snap.output) == seed["stepOutput"]

    by_name = rpc.time_travel.get_step_output_at(
        GetStepOutputAtRequest(run_id=seed["runId"], step_id=seed["stepName"], timestamp=_ts(seed["afterStep"]))
    )
    assert by_name.step_id == seed["stepId"]


def test_tt_run_state_at(rpc: Any, seed: dict[str, Any]) -> None:
    state = rpc.time_travel.get_run_state_at(
        GetRunStateAtRequest(run_id=seed["runId"], timestamp=_ts(seed["afterStep"]))
    ).snapshot
    assert state.run_id == seed["runId"] and state.status == "completed"
    assert json.loads(state.input) == {}
    assert len(state.steps) == 1
    step = state.steps[0]
    assert step.step_id == seed["stepId"] and step.name == seed["stepName"]
    assert json.loads(step.output) == seed["stepOutput"]


def test_tt_run_timeline(rpc: Any, seed: dict[str, Any]) -> None:
    events = rpc.time_travel.get_run_timeline(GetRunTimelineRequest(run_id=seed["runId"])).events
    assert [e.event_type for e in events] == STEP_EVENTS
    for e in events:
        if e.event_type.startswith("step."):
            assert e.step_id == seed["stepId"] and e.step_name == seed["stepName"]


def test_proj_partitions(rest: IronflowClient, seed: dict[str, Any]) -> None:
    res = rest.projections_list_partitions(seed["partitionProjection"])
    assert sorted(res["partitions"]) == seed["partitions"]
    assert res["returned"] == len(seed["partitions"])


# #3003: this projection belongs to the Python suite alone, so the tests below
# may pause, rebuild and delete it. pytest runs them in file order.
def _own(seed: dict[str, Any]) -> dict[str, Any]:
    return seed["projections"]["python"]


def _status(rpc: Any, name: str) -> str:
    return rpc.projections.get_status(GetProjectionStatusRequest(name=name)).status


def test_proj_get_list_and_status(rpc: Any, seed: dict[str, Any]) -> None:
    name, seq = _own(seed)["name"], _own(seed)["seq"]
    got = rpc.projections.get(GetProjectionRequest(name=name))
    assert (got.name, got.mode, got.registry.status) == (name, "managed", "active")
    assert got.state.to_python() == {"count": 2}
    assert got.registry.last_event_seq >= seq
    listed = rpc.projections.list(ListProjectionsRequest(limit=1000)).projections
    assert [p.status for p in listed if p.name == name] == ["active"]
    status = rpc.projections.get_status(GetProjectionStatusRequest(name=name))
    assert (status.name, status.mode, status.status, status.last_event_seq) == (name, "managed", "active", got.registry.last_event_seq)


def test_proj_wait_for_event(rpc: Any, seed: dict[str, Any]) -> None:
    own = _own(seed)
    w = rpc.projections.wait_for_event(WaitForEventRequest(event_id=own["eventId"], projection=own["name"], timeout=Duration(seconds=5)))
    assert (w.caught_up, w.target_seq, w.behind_by_events, w.mode) == (True, own["seq"], 0, "managed")


def test_proj_catchup(rpc: Any, seed: dict[str, Any]) -> None:
    name, seq = _own(seed)["name"], _own(seed)["seq"]
    w = rpc.projections.wait_catchup(WaitProjectionCatchupRequest(name=name, min_seq=seq, timeout=Duration(seconds=5)))
    assert (w.caught_up, w.timed_out, w.target_seq) == (True, False, seq)
    assert w.current_seq >= seq
    far = seq + 1_000_000
    late = rpc.projections.wait_catchup(WaitProjectionCatchupRequest(name=name, min_seq=far, timeout=Duration(nanos=200_000_000)))
    assert (late.caught_up, late.timed_out, late.target_seq) == (False, True, far)
    stream = rpc.projections.wait_catchup_stream(WaitProjectionCatchupRequest(name=name, min_seq=seq, timeout=Duration(seconds=5)))
    last = None
    for frame in stream:
        last = frame
        if frame.caught_up or frame.timed_out or frame.error:
            break
    assert last is not None and (last.caught_up, last.error, last.target_seq) == (True, "", seq)


def test_proj_catchup_batch(rpc: Any, seed: dict[str, Any]) -> None:
    name, seq = _own(seed)["name"], _own(seed)["seq"]
    missing = f"readiness-missing-{time.time_ns()}"
    res = rpc.projections.wait_catchup_batch(
        WaitProjectionCatchupBatchRequest(items=[WaitItem(name=name, min_seq=seq), WaitItem(name=missing, min_seq=1)], timeout=Duration(seconds=5))
    ).results
    assert len(res) == 2
    assert res[0].result.caught_up and res[0].error == ""
    assert "not found" in res[1].error


def test_proj_pause_and_resume(rpc: Any, seed: dict[str, Any]) -> None:
    name = _own(seed)["name"]
    assert rpc.projections.pause(PauseProjectionRequest(name=name)).status == "paused"
    assert _status(rpc, name) == "paused"
    assert rpc.projections.resume(ResumeProjectionRequest(name=name)).status == "active"
    assert _status(rpc, name) == "active"


def test_proj_rebuild_status_and_cancel(rpc: Any, seed: dict[str, Any]) -> None:
    """No runner is connected, so the rebuild stays running until it is cancelled."""
    name = _own(seed)["name"]
    job = rpc.projections.rebuild(RebuildProjectionRequest(name=name)).job
    assert (job.projection_name, job.status) == (name, "running")
    assert job.job_id and job.total_events > 0
    assert _status(rpc, name) == "rebuilding"
    got = rpc.projections.get_rebuild_job(GetRebuildJobRequest(name=name)).job
    assert (got.job_id, got.status) == (job.job_id, "running")
    assert rpc.projections.cancel_rebuild(CancelRebuildRequest(name=name)).status == "cancelled"
    assert rpc.projections.get_rebuild_job(GetRebuildJobRequest(name=name)).job.status == "cancelled"
    assert _status(rpc, name) == "active"


def test_proj_delete_and_unregister(rpc: Any, rest: IronflowClient, seed: dict[str, Any]) -> None:
    """projections_delete is the SDK's unregister: the REST route runs the UnregisterProjection handler."""
    name = _own(seed)["name"]
    assert rest.projections_delete(name) == {"status": "deleted"}
    with pytest.raises(IronflowRPCError) as exc:
        rpc.projections.get(GetProjectionRequest(name=name))
    assert exc.value.code == "not_found", exc.value.code


def test_proj_sql_create_and_query(rpc: Any, rest: IronflowClient) -> None:
    name = f"readiness_sql_py_{time.time_ns()}"
    event = f"readiness.sql.{name}"
    created = rpc.projections.create_sql(
        CreateSQLProjectionRequest(
            name=name,
            table_sql=f"CREATE TABLE proj_{name} (k TEXT PRIMARY KEY, v TEXT)",
            events=[event],
            event_handlers={event: f"INSERT INTO proj_{name} (k, v) VALUES (:data.k, :data.v) ON CONFLICT (k) DO UPDATE SET v = excluded.v"},
        )
    )
    assert (created.name, created.status) == (name, "created")
    try:
        rpc.runs.trigger(TriggerRequest(event=event, data=Struct.from_python({"k": "a", "v": "1"})))
        deadline = time.monotonic() + 15
        res = rpc.projections.query_sql(QuerySQLProjectionRequest(name=name))
        while res.total_count == 0 and time.monotonic() < deadline:
            time.sleep(0.2)
            res = rpc.projections.query_sql(QuerySQLProjectionRequest(name=name))
        assert list(res.columns) == ["k", "v"]
        assert [list(r.values) for r in res.rows] == [["a", "1"]]
        assert [[v.to_python() for v in r.typed_values] for r in res.rows] == [["a", "1"]]
        assert res.total_count == 1
    finally:
        rest.projections_delete(name)


def test_schema_test_upcast(rpc: Any, seed: dict[str, Any]) -> None:
    data = {"orderId": "o1"}
    res = rpc.event_schemas.test_upcast(
        UpcastRequest(event_name=seed["schemaEvent"], from_version=1, to_version=3, data=Struct.from_python(data))
    )
    assert res.data.to_python() == data
    assert [(s.from_version, s.to_version, s.description) for s in res.steps_applied] == [
        (1, 2, "upcast to v2"),
        (2, 3, "no schema registered for version 3"),
    ]


def test_pubsub_cg_manage(rpc: Any) -> None:
    name = f"pysdk-cg-{time.time_ns()}"
    pattern = f"topic:pysdk.cg.{name}"
    rpc.pubsub.create_consumer_group(CreateConsumerGroupRequest(name=name, namespace="default", pattern=pattern, max_redeliveries=3))
    got = rpc.pubsub.get_consumer_group(GetConsumerGroupRequest(name=name, namespace="default"))
    assert (got.name, got.pattern, got.max_redeliveries) == (name, pattern, 3)
    groups = rpc.pubsub.list_consumer_groups(ListConsumerGroupsRequest(namespace="default")).groups
    assert name in [g.name for g in groups]
    updated = rpc.pubsub.update_consumer_group(
        UpdateConsumerGroupRequest(
            group=ConsumerGroup(name=name, namespace="default", max_redeliveries=7),
            update_mask=FieldMask(paths=["max_redeliveries"]),
        )
    )
    assert updated.max_redeliveries == 7
    assert rpc.pubsub.get_consumer_group(GetConsumerGroupRequest(name=name, namespace="default")).max_redeliveries == 7
    rpc.pubsub.delete_consumer_group(DeleteConsumerGroupRequest(name=name, namespace="default"))
    with pytest.raises(IronflowRPCError) as exc:
        rpc.pubsub.get_consumer_group(GetConsumerGroupRequest(name=name, namespace="default"))
    assert exc.value.code == "not_found", exc.value.code


def test_proj_runner_lease(rpc: Any, server_url: str, api_key: str) -> None:
    """Two runners of one external projection: one holds the lease past the 30s
    TTL (renew); stopping it hands over well before expiry (release). The
    standby retries every 5s with jitter."""
    name = f"pysdk-lease-{time.time_ns()}"
    event = f"pysdk.lease.{name}"
    seen: dict[str, list[int]] = {"a": [], "b": []}

    def worker(runner: str) -> Worker:
        proj = create_projection(
            name=name, events=[event], handler=lambda e, c: seen[runner].append(int(e.data["i"]))
        )
        return Worker(functions=[], projections=[proj], server_url=server_url, api_key=api_key)

    async def scenario() -> None:
        i = 0

        async def emit_until(cond: Any, within: float, every: float) -> bool:
            nonlocal i
            end = time.monotonic() + within
            while time.monotonic() < end:
                i += 1
                rpc.events.emit(TriggerRequest(event=event, data=Struct.from_python({"i": i})))
                if cond():
                    return True
                await asyncio.sleep(every)
            return cond()

        a, b = worker("a"), worker("b")
        task_a = asyncio.ensure_future(a.start())
        task_b: asyncio.Future[None] | None = None
        try:
            assert await emit_until(lambda: bool(seen["a"]), 30, 0.5), "runner a never handled an event"
            task_b = asyncio.ensure_future(b.start())
            await emit_until(lambda: False, 35, 2)  # past one TTL
            last = i
            end = time.monotonic() + 10
            while time.monotonic() < end and last not in seen["a"]:
                await asyncio.sleep(0.2)
            assert last in seen["a"] and seen["b"] == [], (last, seen)  # renew kept the lease with a

            await a.drain()
            await asyncio.wait_for(task_a, 10)
            after_stop = len(seen["a"])
            assert await emit_until(lambda: bool(seen["b"]), 15, 1), "standby b did not take over within 15s"
            assert len(seen["a"]) == after_stop
        finally:
            await a.stop()
            await b.stop()
            for task in (task_a, task_b):
                if task is not None:
                    await asyncio.wait_for(task, 10)

    asyncio.run(scenario())


def test_webhook_secret_ops_and_deliveries(rpc: Any, server_url: str) -> None:
    src = rpc.webhooks.create_source(
        CreateWebhookSourceRequest(
            name=f"pysdk-wh-{time.time_ns()}",
            event_prefix="pysdk.wh",
            verify_header="x-sig",
            verify_algorithm="hmac-sha256",
            verify_secret="s1",
        )
    )
    assert src.ingest_token

    def deliver(secret: str, token: str, ident: str) -> int:
        body = json.dumps({"type": "pysdk.wh", "id": ident, "data": {"k": 1}}).encode()
        headers = {"Content-Type": "application/json"}
        if secret:
            headers["x-sig"] = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        url = f"{server_url}/api/v1/webhooks/{src.id}?token={urllib.request.quote(token)}"
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=body, headers=headers, method="POST")) as resp:
                return int(resp.status)
        except urllib.error.HTTPError as err:
            return err.code

    try:
        t1 = src.ingest_token
        assert deliver("s1", t1, "d1") == 200

        rotated = rpc.webhooks.rotate_secret(RotateWebhookSecretRequest(id=src.id, verify_secret="s2"))
        assert rotated.verify_secret_prev_set
        assert deliver("s1", t1, "d2") == 200  # grace window
        assert deliver("s2", t1, "d3") == 200

        assert not rpc.webhooks.expire_secret_prev(ExpireWebhookSecretPrevRequest(id=src.id)).verify_secret_prev_set
        assert deliver("s1", t1, "d4") == 401
        assert deliver("s2", t1, "d5") == 200

        t2 = rpc.webhooks.rotate_ingest_token(RotateWebhookIngestTokenRequest(id=src.id)).ingest_token
        assert t2 and t2 != t1
        assert deliver("s2", t1, "d6") == 401
        assert deliver("s2", t2, "d7") == 200

        disabled = rpc.webhooks.disable_signature_verification(DisableWebhookSignatureVerificationRequest(id=src.id))
        assert not disabled.verify_secret_set
        assert deliver("", t2, "d8") == 200

        log = rpc.webhooks.list_deliveries(ListWebhookDeliveriesRequest(source_id=src.id))
        # Newest first; a bad ingest token is rejected before the body is read.
        assert log.total_count == 8
        assert [(d.external_id, d.status, d.error) for d in log.deliveries] == [
            ("d8", "accepted", ""),
            ("d7", "accepted", ""),
            ("", "rejected", "invalid ingest token"),
            ("d5", "accepted", ""),
            ("d4", "rejected", "signature mismatch"),
            ("d3", "accepted", ""),
            ("d2", "accepted", ""),
            ("d1", "accepted", ""),
        ]
    finally:
        rpc.webhooks.delete_source(DeleteWebhookSourceRequest(id=src.id))


def test_auth_audit(rpc: Any, rest: IronflowClient, server_url: str) -> None:
    """A viewer key is created, denied a write, then revoked; the auth trail shows all three."""
    key = rest.api_keys_create({"name": f"pysdk-viewer-{time.time_ns()}", "role_ids": ["role_viewer"]})
    req = urllib.request.Request(
        f"{server_url}/ironflow.v1.PubSubService/CreateConsumerGroup",
        data=json.dumps({"name": "denied", "pattern": "topic:denied"}).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {key['key']}"},
        method="POST",
    )
    with pytest.raises(urllib.error.HTTPError) as denied:
        urllib.request.urlopen(req)
    assert denied.value.code == 403
    rest.api_keys_delete(key["id"])

    trail = rpc.audit.get_auth_trail(GetAuthAuditTrailRequest(org_id="org_default", api_key_id=key["id"]))
    # Newest first.
    assert [e.event_type for e in trail.events] == ["authz.key.revoked", "authz.decision.denied", "authz.key.created"]
    assert trail.total_count == 3
    payload = trail.events[1].payload.to_python()
    assert payload["decision"] == "denied" and payload["path"] == "/ironflow.v1.PubSubService/CreateConsumerGroup"
    assert payload["api_key_id"] == key["id"]
