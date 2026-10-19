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
from protobuf.wkt import FieldMask, Struct, Timestamp

from ironflow import IronflowClient, IronflowRPCError
from ironflow.projection import create_projection
from ironflow.rpc.v1 import (
    ConsumerGroup,
    CreateConsumerGroupRequest,
    CreateWebhookSourceRequest,
    DeleteConsumerGroupRequest,
    DeleteWebhookSourceRequest,
    DisableWebhookSignatureVerificationRequest,
    ExpireWebhookSecretPrevRequest,
    FunctionStatus,
    GetAuditTrailRequest,
    GetAuthAuditTrailRequest,
    GetConsumerGroupRequest,
    GetFunctionAtVersionRequest,
    GetFunctionRequest,
    GetRunStateAtRequest,
    GetRunTimelineRequest,
    GetStepOutputAtRequest,
    ListConsumerGroupsRequest,
    ListWebhookDeliveriesRequest,
    RotateWebhookIngestTokenRequest,
    RotateWebhookSecretRequest,
    TriggerBatchRequest,
    TriggerRequest,
    TriggerSyncRequest,
    UpdateConsumerGroupRequest,
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
