"""A Python push function runs end to end against a real engine (#2396).

Proves what unit tests cannot: the engine signs, the handler verifies, the
push wire shapes match (including the wait_for_event yield), and both a sleep
and a matched wait resume through the memo path.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
import time
import uuid
from collections.abc import Iterator
from typing import Any

import uvicorn
from protobuf.wkt import Struct

from ironflow import IronflowRPC
from ironflow.rpc.v1 import (
    DeleteFunctionRequest,
    GetRunRequest,
    GetTopicStatsRequest,
    RunStatus,
    TriggerRequest,
)
from ironflow.serve import register, serve
from ironflow.worker import function

PUSH_BUDGET_S = 10.0  # engine default push timeout


@contextlib.contextmanager
def running(app: Any) -> Iterator[str]:
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning", lifespan="on"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        assert time.monotonic() < deadline, "uvicorn did not start"
        time.sleep(0.05)
    port = server.servers[0].sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(5)


def test_push_function_completes_through_a_sleep_and_a_wait(
    rpc: IronflowRPC, server_url: str, api_key: str, signing_key: str,
) -> None:
    fn_id = f"py-push-{uuid.uuid4().hex[:8]}"
    event_name = f"pysdk.push.{uuid.uuid4().hex[:8]}"

    paid_name = f"pysdk.paid.{uuid.uuid4().hex[:8]}"

    @function(id=fn_id, triggers=[{"event": event_name}])
    async def handler(ctx: Any) -> Any:
        a = await ctx.step.run("a", lambda: 1)
        await ctx.step.sleep("nap", 1)
        paid = await ctx.step.wait_for_event("paid", event=paid_name, timeout="30s")
        return {"a": a, "paid": paid.data}

    inner = serve([handler], signing_key=signing_key)
    durations: list[float] = []

    async def timed(scope: Any, receive: Any, send: Any) -> None:
        t0 = time.monotonic()
        await inner(scope, receive, send)
        if scope["type"] == "http":
            durations.append(time.monotonic() - t0)

    with running(timed) as base:
        asyncio.run(register([handler], endpoint_url=base + "/", server_url=server_url, api_key=api_key))
        try:
            run_ids = rpc.events.emit(TriggerRequest(event=event_name)).run_ids
            assert run_ids, "Emit returned no run ids; the push function did not match the trigger"
            run_id = run_ids[0]
            deadline = time.monotonic() + 30
            while True:
                run = rpc.runs.get(GetRunRequest(id=run_id))
                if run.status in (RunStatus.COMPLETED, RunStatus.FAILED) or time.monotonic() > deadline:
                    break
                # An event emitted before the wait is registered is not matched; keep sending
                # until the run finishes. No function triggers on paid_name, so extras are inert.
                rpc.events.emit(TriggerRequest(event=paid_name, data=Struct.from_python({"ok": True})))
                time.sleep(0.5)
            assert run.status == RunStatus.COMPLETED, run
            assert run.output.to_python() == {"a": 1, "paid": {"ok": True}}
            assert len(durations) >= 3, "expected pushes for start, post-sleep resume and post-wait resume"
            assert max(durations) < PUSH_BUDGET_S
        finally:
            rpc.functions.delete(DeleteFunctionRequest(id=fn_id))


def test_push_function_reads_event_metadata_and_publishes(
    rpc: IronflowRPC, server_url: str, api_key: str, signing_key: str,
) -> None:
    fn_id = f"py-meta-{uuid.uuid4().hex[:8]}"
    event_name = f"pysdk.meta.{uuid.uuid4().hex[:8]}"
    topic = f"pysdk.pub.{uuid.uuid4().hex[:8]}"  # not events:, system., entity: or public.: those are reserved

    @function(id=fn_id, triggers=[{"event": event_name}])
    async def handler(ctx: Any) -> Any:
        published = await ctx.step.publish(topic, {"n": 1})
        return {"meta": ctx.event.metadata, "event_id": published.event_id, "sequence": published.sequence}

    app = serve([handler], signing_key=signing_key, server_url=server_url, api_key=api_key)
    with running(app) as base:
        asyncio.run(register([handler], endpoint_url=base + "/", server_url=server_url, api_key=api_key))
        try:
            run_ids = rpc.events.emit(TriggerRequest(
                event=event_name, metadata=Struct.from_python({"traceId": "t-1"}))).run_ids
            assert run_ids, "Emit returned no run ids; the push function did not match the trigger"
            deadline = time.monotonic() + 30
            while True:
                run = rpc.runs.get(GetRunRequest(id=run_ids[0]))
                if run.status in (RunStatus.COMPLETED, RunStatus.FAILED) or time.monotonic() > deadline:
                    break
                time.sleep(0.5)
            assert run.status == RunStatus.COMPLETED, run
            out = run.output.to_python()
            assert out["meta"] == {"traceId": "t-1"}
            assert out["event_id"] and out["sequence"] >= 1
            # An event id in the reply is not proof the message reached the stream.
            assert rpc.pubsub.get_topic_stats(GetTopicStatsRequest(topic=topic)).message_count >= 1
        finally:
            rpc.functions.delete(DeleteFunctionRequest(id=fn_id))
