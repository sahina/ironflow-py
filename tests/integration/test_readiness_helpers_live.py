"""Drives the #2948 rows through the Python SDK against a seeded server.

`make test-readiness-helpers-live` starts a server, runs
tests/readiness-live/helpers-seed and sets IRONFLOW_READINESS_HELPERS_SEED.
Without it this module skips.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

import pytest
from protobuf.wkt import Struct

from ironflow import IronflowClient
from ironflow.rpc.v1 import (
    ExecuteSQLRequest,
    ExecutionMode,
    ExecutionSnapshot,
    FunctionRoutingMode,
    GetDeploymentRequest,
    GetFunctionExecutionRoutingRequest,
    GetProjectionRevisionRequest,
    GetSchemaRevisionRequest,
    ListDeploymentsRequest,
    ListProjectionHistoryRequest,
    ListSchemaHistoryRequest,
    RegisterDeploymentRequest,
    Trigger,
    TriggerRequest,
)

SEED_PATH = os.environ.get("IRONFLOW_READINESS_HELPERS_SEED")
pytestmark = pytest.mark.skipif(
    not SEED_PATH, reason="IRONFLOW_READINESS_HELPERS_SEED not set; run `make test-readiness-helpers-live`"
)


@pytest.fixture
def seed() -> dict[str, Any]:
    assert SEED_PATH is not None
    with open(SEED_PATH, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def rest(server_url: str, api_key: str) -> IronflowClient:
    return IronflowClient(server_url, api_key=api_key)


@pytest.fixture
def platform(server_url: str, seed: dict[str, Any]) -> IronflowClient:
    return IronflowClient(server_url, api_key=seed["platformKey"])


def _time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def test_deployment_register_and_read(rpc: Any, seed: dict[str, Any]) -> None:
    fn = seed["debounceFunctionId"]
    req = RegisterDeploymentRequest(
        function_id=fn,
        deployment_id="pysdk-v1",
        artifact_digest="sha256:" + "3" * 64,
        snapshot=ExecutionSnapshot(
            triggers=[Trigger(event="orders.placed")],
            timeout_ms=30000,
            metadata=Struct.from_python({"who": "José"}),
        ),
        mode=ExecutionMode.PULL,
        snapshot_schema_version=1,
    )
    first = rpc.deployments.register(req)
    assert first.created is True
    d = first.deployment
    assert (d.deployment_id, d.mode, d.artifact_digest) == ("pysdk-v1", ExecutionMode.PULL, req.artifact_digest)
    assert d.snapshot.timeout_ms == 30000
    assert d.snapshot.metadata.to_python() == {"who": "José"}
    assert rpc.deployments.register(req).created is False

    got = rpc.deployments.get(GetDeploymentRequest(function_id=fn, deployment_id="pysdk-v1")).deployment
    assert got == d
    assert got.environment_id and got.created_by and got.created_at is not None

    listed = rpc.deployments.list(ListDeploymentsRequest(function_id=fn)).deployments
    assert "pysdk-v1" in [x.deployment_id for x in listed]

    routing = rpc.deployments.get_execution_routing(GetFunctionExecutionRoutingRequest(function_id=fn))
    # Registering never activates: the function stays on legacy routing.
    assert routing.function_id == fn
    assert routing.mode == FunctionRoutingMode.LEGACY
    assert routing.active_deployment_id == ""
    assert routing.updated_at is not None


def test_capacity_views(platform: IronflowClient, seed: dict[str, Any]) -> None:
    fn = seed["capacityFunctionId"]
    lanes = platform.capacity_list_lanes(function=fn)
    assert len(lanes) == 1, lanes
    assert (lanes[0]["limit"], lanes[0]["active_reserved"]) == (1, 1)
    lane = lanes[0]["lane_id"]
    assert lane

    queue = platform.capacity_list_queue(function=fn)
    assert len(queue) == 1, queue
    assert queue[0]["run_id"] == seed["queuedRunId"]
    assert queue[0]["lane_id"] == lane
    assert queue[0]["reservation_owner"] is None

    lease = next(x for x in platform.capacity_list_leases() if x["run_id"] == seed["leasedRunId"])
    assert lease["lane_id"] == lane
    assert _time(lease["expires_at"]) > _time(lease["activated_at"])

    buckets = platform.capacity_list_buckets(function=fn)
    assert [b["lane_id"] for b in buckets] == [lane]

    stat = next(s for s in platform.capacity_list_stats() if s["function_id"] == fn)
    assert (stat["active_leases"], stat["queued"], stat["lane_limit"]) == (1, 1, 1)

    sessions = platform.capacity_list_sessions()
    assert sessions, "the seed's streaming worker should hold a session"
    session = sessions[0]
    assert session["draining"] is False
    credits = [c for c in platform.capacity_list_credits() if c["session_id"] == session["session_id"]]
    assert credits
    assert {c["worker_id"] for c in credits} == {session["worker_id"]}


def test_debounce_manage(rpc: Any, rest: IronflowClient, seed: dict[str, Any]) -> None:
    fn, key = seed["debounceFunctionId"], "pysdk key/é"
    rpc.runs.trigger(TriggerRequest(event=fn, data=Struct.from_python({"k": key})))

    def find() -> Any:
        return next(
            (e for e in rest.debounce_list_entries() if e["function_id"] == fn and e["debounce_key"] == key),
            None,
        )

    entry = find()
    assert entry is not None
    assert entry["period_ms"] == 600000
    assert _time(entry["fires_at"]) > _time(entry["armed_at"])

    rest.debounce_delete_entries(entry["environment_id"], fn, key)
    assert find() is None


def test_sql_execute(rpc: Any) -> None:
    resp = rpc.query.execute_sql(ExecuteSQLRequest(query="SELECT 1 AS n, 'José' AS who"))
    assert list(resp.columns) == ["n", "who"]
    assert [list(r.values) for r in resp.rows] == [["1", "José"]]
    assert resp.total_rows == 1


def test_proj_history_and_revision(rpc: Any, seed: dict[str, Any]) -> None:
    name, events = seed["projection"], seed["projectionEvents"]
    first = rpc.projections.list_history(ListProjectionHistoryRequest(name=name, limit=1))
    assert len(first.entries) == 1 and first.has_more
    newest = first.entries[0]
    assert first.next_before_revision == newest.revision
    assert list(newest.previous_definition.events) == events[1]
    assert (newest.change_type, newest.change_reason) == ("updated", "change 2")

    second = rpc.projections.list_history(
        ListProjectionHistoryRequest(name=name, limit=1, before_revision=first.next_before_revision)
    )
    assert len(second.entries) == 1 and not second.has_more and second.next_before_revision == 0
    oldest = second.entries[0]
    assert list(oldest.previous_definition.events) == events[0]

    got = rpc.projections.get_revision(GetProjectionRevisionRequest(name=name, revision=oldest.revision)).entry
    assert got == oldest


def test_schema_history_and_revision(rpc: Any, seed: dict[str, Any]) -> None:
    event, version, docs = seed["schemaEvent"], seed["schemaVersion"], seed["schemaDocs"]
    page = rpc.event_schemas.list_history(ListSchemaHistoryRequest(event_name=event, version=version))
    assert not page.has_more
    assert [e.previous_definition.schema_json for e in page.entries] == [docs[1], docs[0]]
    oldest = page.entries[1]
    got = rpc.event_schemas.get_revision(
        GetSchemaRevisionRequest(event_name=event, version=version, revision=oldest.revision)
    ).entry
    assert got == oldest
    assert got.previous_definition.version == version
    assert got.change_type == "updated"

