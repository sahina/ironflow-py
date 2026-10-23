"""The operational-controls group (#3003) through the Python client.

Only Python has the circuit-breaker, flow-edge and outbox dead-letter calls
(the other SDKs leave them to the dashboard and the CLI, ADR 0092), and the
audit calls are checked in all four SDKs. `make test-readiness-live` seeds the
edges and the dead-letter rows (tests/readiness-live/seed/seed_operational_controls.go)
and sets IRONFLOW_READINESS_SEED; without it this module skips.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import queue
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from protobuf.wkt import Struct

from ironflow import IronflowClient, IronflowError, IronflowRPC
from ironflow.rpc.v1 import (
    DeleteFunctionRequest,
    GetRunRequest,
    RunStatus,
    SubscribeOptions,
    SubscribeRequest,
    TriggerRequest,
)
from ironflow.serve import register
from ironflow.worker import function

pytestmark = pytest.mark.usefixtures("readiness_seed")


@pytest.fixture
def rest(server_url: str, api_key: str) -> IronflowClient:
    return IronflowClient(server_url, api_key=api_key)


@pytest.fixture
def ops(readiness_seed: dict[str, Any]) -> dict[str, Any]:
    return readiness_seed["groups"]["operational-controls"]  # type: ignore[no-any-return]


def test_audit_env_wide(rest: IronflowClient, readiness_seed: dict[str, Any]) -> None:
    run_id, function_id = readiness_seed["runId"], readiness_seed["functionId"]
    # Newest first, the reverse of the per-run trail.
    want = ["run.status_changed", "step.completed", "step.started", "run.created"]

    by_run = rest.audit_list(run_id=run_id)
    assert [e["event_type"] for e in by_run["events"]] == want
    assert by_run["total_count"] == 4
    assert by_run["retention_days"] > 0
    for e in by_run["events"]:
        assert (e["run_id"], e["function_id"], e["environment_id"]) == (run_id, function_id, "env_default")
        assert e["id"] and e["created_at"]

    only = rest.audit_list(function_id=function_id, event_type="step.completed")
    assert [(e["event_type"], e["run_id"]) for e in only["events"]] == [("step.completed", run_id)]

    pages: list[str] = []
    cursor = ""
    for _ in range(2):
        page = rest.audit_list(run_id=run_id, limit=2, cursor=cursor or None)
        assert len(page["events"]) == 2 and page["total_count"] == 4
        pages += [e["event_type"] for e in page["events"]]
        cursor = page["next_cursor"]
    assert pages == want

    # The seed took beforeRun before it triggered the run, so the window decides.
    assert rest.audit_list(run_id=run_id, from_=readiness_seed["beforeRun"])["total_count"] == 4
    assert rest.audit_list(run_id=run_id, to=readiness_seed["beforeRun"])["events"] == []


def test_audit_settings_read(rest: IronflowClient) -> None:
    settings = rest.audit_list_settings()
    assert settings["capture_scope"]
    assert settings["delivery_guarantee"] and isinstance(settings["delivery_healthy"], bool)
    assert settings["retention_days"] >= 0 and settings["delivery_failure_count"] >= 0
    assert isinstance(settings["authz_successful_reads"], bool) and isinstance(settings["kv_capture"], bool)
    assert isinstance(settings["authz_excluded_actions"], list) and isinstance(settings["payload_redaction_fields"], list)
    assert settings["function_recording"] and isinstance(settings["changes_require_restart"], bool)


def test_flow_edges(rest: IronflowClient, ops: dict[str, Any]) -> None:
    """Real runs wrote these edges; each flow route caches its first answer for 30s, so read each once."""
    # The shared seed run adds edges of its own, so each list is checked for its members.
    assert {"function_id": ops["emitterId"], "event": ops["emittedEvent"]} in rest.flow_list_emit_edges()["edges"]
    assert {"event": ops["streamEvent"], "entity_type": ops["streamType"]} in rest.flow_list_stream_edges()["edges"]
    assert {"function_id": ops["emitterId"], "topic": ops["topic"]} in rest.flow_list_topic_edges()["edges"]
    assert {"parent_function_id": ops["parentId"], "child_function_id": ops["childId"]} in rest.flow_list_invoke_edges()["edges"]
    assert {"function_id": ops["waiterId"], "event_name": ops["waitEvent"]} in rest.flow_list_wait_edges()["edges"]

    hooks = rest.flow_list_webhooks()
    source = next(s for s in hooks["sources"] if s["id"] == ops["webhookId"])
    assert source["event_prefix"] == ops["webhookEvent"].removesuffix(".ping")
    assert source["delivery_counts"] == {"accepted": 1, "rejected": 0, "failed": 0}
    assert source["latest_delivery_status"] == "accepted"
    assert {"source_id": ops["webhookId"], "event": ops["webhookEvent"]} in hooks["edges"]


class _Watch:
    """A subscription to `events:<event>`, read by a thread; `replay` frames come first.

    The server tells the client nothing when the live transport is attached.
    So `live()` proves the attach by emitting probe events under the name until
    one arrives; only then is it safe to act and wait.
    """

    def __init__(self, server_url: str, api_key: str, event: str, replay: int = 0) -> None:
        self.event = event
        self.frames: queue.Queue[Any] = queue.Queue()

        def read() -> None:
            # Ends at the deadline, or when the client closes at the end of the test.
            with contextlib.suppress(Exception), IronflowRPC(server_url=server_url, api_key=api_key) as client:
                for frame in client.pubsub.subscribe(
                    SubscribeRequest(pattern=f"events:{event}", options=SubscribeOptions(replay=replay)), timeout=40.0
                ):
                    self.frames.put(frame)

        threading.Thread(target=read, daemon=True).start()

    def live(self, rpc: Any) -> None:
        deadline = time.monotonic() + 15
        probe = 0
        while time.monotonic() < deadline:
            rpc.events.emit(TriggerRequest(event=self.event, data=Struct.from_python({"probe": probe})))
            probe += 1
            if self.next(0.25) is not None:
                return
        pytest.fail(f"the subscription to {self.event} never went live")

    def next(self, timeout: float) -> Any:
        try:
            return self.frames.get(timeout=timeout)
        except queue.Empty:
            return None


def test_outbox_dlq(rest: IronflowClient, rpc: Any, server_url: str, api_key: str, ops: dict[str, Any]) -> None:
    requeue, discard = ops["dlqRequeue"], ops["dlqDiscard"]

    def listed() -> dict[str, Any]:
        return {i["event_id"]: i for i in rest.outbox_list_dead_letter(env_id="env_default", limit=200)["items"]}

    rows = listed()
    assert {requeue["eventId"], discard["eventId"]} <= set(rows)
    row = rows[requeue["eventId"]]
    assert (row["topic"], row["kind"], row["environment_id"], row["attempts"], row["last_error"]) == (
        f"events:{requeue['event']}", "events", "env_default", 1, "nats: no responders")
    assert json.loads(base64.b64decode(row["payload"]).decode("utf-8")) == requeue["data"]
    assert row["created_at"] and row["dead_at"]

    # Subscribe first and wait until the subscription is live: the server does
    # not say when the live transport is attached.
    watch = _Watch(server_url, api_key, requeue["event"])
    watch.live(rpc)

    # Discard drops the row and publishes nothing.
    result = rest.outbox_delete_dead_letter(discard["eventId"], env_id="env_default")
    assert (result["event_id"], result["result"]) == (discard["eventId"], "discarded")
    assert discard["eventId"] not in listed()

    # Requeue hands the event to the running outbox worker, which publishes it.
    result = rest.outbox_dead_letter_requeue(requeue["eventId"], env_id="env_default")
    assert (result["event_id"], result["result"]) == (requeue["eventId"], "requeued")
    assert requeue["eventId"] not in listed()
    # The seed left no outbox row for the worker to publish, so the only frame
    # with this event's ID is the requeue's. Probe frames from live() may still arrive.
    deadline = time.monotonic() + 15
    published = None
    while published is None and time.monotonic() < deadline:
        frame = watch.next(1.0)
        if frame is not None and frame.event_id == requeue["eventId"]:
            published = frame
    assert published is not None, "the requeued event was never published"
    assert published.data.to_python() == requeue["data"]
    # Nothing was ever published under the discarded name (a replay would show it).
    stale = _Watch(server_url, api_key, discard["event"], replay=1)
    assert stale.next(2.0) is None, f"the discarded event {discard['eventId']} was published"

    # Both rows are gone, so a second action finds nothing.
    for act in (
        lambda: rest.outbox_delete_dead_letter(discard["eventId"], env_id="env_default"),
        lambda: rest.outbox_dead_letter_requeue(requeue["eventId"], env_id="env_default"),
    ):
        with pytest.raises(IronflowError) as exc:
            act()
        assert exc.value.status_code == 404


def test_circuit_breaker_manage(rpc: Any, rest: IronflowClient, server_url: str, api_key: str) -> None:
    """Failures open the breaker, the list reports it, and a reset lets dispatch resume."""
    healthy = threading.Event()
    calls: list[bool] = []

    class Endpoint(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            self.rfile.read(int(self.headers.get("Content-Length") or 0))
            ok = healthy.is_set()
            calls.append(ok)
            body = json.dumps({"status": "completed", "result": {}} if ok else {}).encode()
            self.send_response(200 if ok else 500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: object) -> None:
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Endpoint)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{httpd.server_address[1]}/"
    fn_id, event = f"py-breaker-{time.time_ns()}", f"pysdk.breaker.{time.time_ns()}"

    async def handler(ctx: Any) -> Any:  # never runs: the stub answers for it
        return None

    # One attempt per run, so each failing run settles to failed at once.
    fn = function(id=fn_id, triggers=[{"event": event}], retry={"max_attempts": 1})(handler)

    def run_to_end(timeout: float) -> Any:
        run_ids = rpc.events.emit(TriggerRequest(event=event)).run_ids
        assert len(run_ids) == 1, run_ids
        deadline = time.monotonic() + timeout
        while True:
            run = rpc.runs.get(GetRunRequest(id=run_ids[0]))
            if run.status in (RunStatus.COMPLETED, RunStatus.FAILED) or time.monotonic() > deadline:
                return run
            time.sleep(0.1)

    def mine() -> list[dict[str, Any]]:
        return [dict(b) for b in rest.circuit_breakers_list() if b["function_id"] == fn_id]

    asyncio.run(register([fn], endpoint_url=url, server_url=server_url, api_key=api_key))
    try:
        # Failure counts once per run, after its retries, and the gate checks only at
        # admission: one run at a time keeps none in flight once the breaker opens.
        for _ in range(5):
            assert run_to_end(15).status == RunStatus.FAILED
        (breaker,) = mine()
        assert (breaker["state"], breaker["consecutive_fails"], breaker["endpoint"], breaker["environment_id"]) == (
            "open", 5, url, "env_default")
        assert breaker["key"] == f"{fn_id}|{url}"
        assert breaker["last_failure"]

        # The endpoint recovers, but the open breaker still holds the next run back.
        healthy.set()
        held = rpc.events.emit(TriggerRequest(event=event)).run_ids[0]
        time.sleep(1)
        assert calls == [False] * 5

        # The key is base64url without padding (docs/reference/api/rest-api.md).
        key = base64.urlsafe_b64encode(breaker["key"].encode()).rstrip(b"=").decode()
        reset = rest.circuit_breakers_reset(key)
        assert (reset["state"], reset["function_id"], reset["consecutive_fails"]) == ("closed", fn_id, 0)
        assert [(b["state"], b["consecutive_fails"]) for b in mine()] == [("closed", 0)]

        # Well inside the dispatcher's 30s sweep: the reset itself wakes dispatch.
        deadline = time.monotonic() + 10
        while rpc.runs.get(GetRunRequest(id=held)).status != RunStatus.COMPLETED:
            assert time.monotonic() < deadline, "the held run never completed after the reset"
            time.sleep(0.1)
    finally:
        rpc.functions.delete(DeleteFunctionRequest(id=fn_id))
        httpd.shutdown()
