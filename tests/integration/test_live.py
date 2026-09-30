"""The RPC facade against a real Ironflow server (#1781).

Twelve scenarios, each here because only a live server can prove it:

  1. An ordinary unary RPC reaches a real handler and returns a real message.
  2. WebhookService round-trips — the service whose handler registers a
     non-default codec server-side.
  3. A server stream carries real events, via a genuine publish/subscribe round
     trip rather than a one-shot call.
  4. Both clients work against the same server.
  5. A bad API key is rejected. This is the reason the suite refuses `--dev`.
  6. A deadline ends a real subscription.
  7. Abandoning a real subscription leaves the client usable.
  8. A failure arriving AFTER real events translates during iteration.
  9. A projection-only worker registers and reduces real events (#2395).
 10. KV and config watch see real updates over WebSocket (#2397).
 11. Command dedup claims, finalizes and releases through real KV (#2412).
 12. A MANUAL consumer group redelivers an event after nak (#2412).

ON 6-8, WHICH THIS FILE PREVIOUSLY DECLARED OUT OF SCOPE
-------------------------------------------------------
They are here now, and the flake risk is real but avoidable — the trick is to
pick failures the server produces deterministically rather than ones that
depend on a handler happening to be slow.

A quiet topic NEVER yields, so a deadline on one fires every time. That single
fact makes 6 and 8 deterministic, and 7 rides on the publish/subscribe round
trip that scenario 3 already proves.

What is still only covered in-process: a server-generated APPLICATION error
mid-stream, such as `resource_exhausted`. `internal/server/connect/pubsub_handler.go`
has no reachable path to one — its fanout loop returns `nil` on `ctx.Done()`
and its only other mid-loop exit is a `stream.Send` transport failure. So the
mid-iteration failure available here is the deadline, which is a real error
frame arriving after real events over a real connection. `tests/test_rpc_streams.py`
covers the application-error variant against the stub, where the test decides
when to fail.
"""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from typing import Any

import pytest
from protobuf.wkt import Value

from ironflow import AsyncIronflowRPC, IronflowRPC, IronflowRPCError
from ironflow.projection import create_projection
from ironflow.rpc.v1 import (
    AckEventRequest,
    AckMode,
    AckType,
    CreateConsumerGroupRequest,
    CreateWebhookSourceRequest,
    DeleteWebhookSourceRequest,
    GetProjectionRequest,
    GetWebhookSourceRequest,
    JoinConsumerGroupRequest,
    ListTopicsRequest,
    PublishRequest,
    SubscribeRequest,
    TriggerRequest,
)
from ironflow.worker import Worker


def unique(prefix: str) -> str:
    """Names must not collide across reruns against a persistent database."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def publish_after(
    publish_rpc: IronflowRPC, topic: str, payload: str, delay: float = 1.0
) -> threading.Thread:
    """Publish to `topic` from a second thread, after `delay`.

    A subscription blocks the calling thread, so the publish that feeds it
    cannot run on that thread. The delay lets the subscription establish first;
    it is a property of the test's ordering, not a requirement of pub/sub, so
    every caller must still tolerate receiving nothing and say so clearly
    rather than hanging.

    """

    def run() -> None:
        time.sleep(delay)
        publish_rpc.pubsub.publish(
            PublishRequest(topic=topic, data_value=Value.from_python(payload))
        )

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


# ── 1 + 2. unary against real handlers, on the codec-configured service ──────


def test_webhook_source_round_trips(rpc: IronflowRPC) -> None:
    name = unique("py-sdk-test")
    created = rpc.webhooks.create_source(
        CreateWebhookSourceRequest(name=name, event_prefix="pysdk")
    )
    assert created.id
    assert created.name == name

    try:
        fetched = rpc.webhooks.get_source(GetWebhookSourceRequest(id=created.id))
        assert fetched.id == created.id
        assert fetched.name == name
    finally:
        rpc.webhooks.delete_source(DeleteWebhookSourceRequest(id=created.id))

    with pytest.raises(IronflowRPCError) as exc:
        rpc.webhooks.get_source(GetWebhookSourceRequest(id=created.id))
    assert exc.value.code in ("not_found", "invalid_argument"), exc.value.code


def test_list_topics_reaches_a_real_handler(rpc: IronflowRPC) -> None:
    """A capability the REST client does not deliver at all.

    Until PR 2 this was unreachable from Python, and
    docs/reference/api/python-sdk.md said so.
    """
    rpc.pubsub.list_topics(ListTopicsRequest())


# ── 3. a real server stream ─────────────────────────────────────────────────


def test_subscribe_receives_a_published_event(
    rpc: IronflowRPC, publish_rpc: IronflowRPC
) -> None:
    """A genuine round trip: subscribe, publish, assert the event arrives.

    Subscribing to a quiet topic yields nothing and blocks, so a one-shot call
    could not tell "the stream works" from "the stream is empty". The publish
    runs on a second thread because the subscription blocks this one — see
    `publish_after`, using a separate Connect client for publication.

    The deadline is what turns a missing event into a failure rather than a
    hang: the stream ends with deadline_exceeded and the assertion reports an
    empty list.
    """
    # The subscribe PATTERN needs a namespace prefix (internal/pubsub/pattern.go);
    # the publish takes the bare topic. Two spellings of the same thing, which
    # is the kind of asymmetry a live round trip catches and a stub cannot.
    topic = unique("pysdk.stream")
    payload = uuid.uuid4().hex

    received: list[Any] = []
    publisher = publish_after(publish_rpc, topic, payload)
    try:
        for event in rpc.pubsub.subscribe(
            SubscribeRequest(pattern=f"topic:{topic}"), timeout=15.0
        ):
            received.append(event)
            break
    except IronflowRPCError as err:
        if err.code != "deadline_exceeded":
            raise
    finally:
        publisher.join(timeout=5)

    assert received, (
        f"no event arrived on {topic} within the deadline. The stream opened, so "
        f"this is either a publish that did not land or a delivery path that is "
        f"broken — not a client-side framing problem."
    )


# ── 4. both clients ─────────────────────────────────────────────────────────


def test_async_client_against_the_same_server(async_rpc_factory: Any) -> None:
    async def scenario() -> str:
        async with async_rpc_factory() as rpc:
            name = unique("py-sdk-async")
            created = await rpc.webhooks.create_source(
                CreateWebhookSourceRequest(name=name, event_prefix="pysdk")
            )
            try:
                fetched = await rpc.webhooks.get_source(
                    GetWebhookSourceRequest(id=created.id)
                )
                return str(fetched.name)
            finally:
                await rpc.webhooks.delete_source(
                    DeleteWebhookSourceRequest(id=created.id)
                )

    assert asyncio.run(scenario()).startswith("py-sdk-async")


# ── 5. authentication ───────────────────────────────────────────────────────


def test_a_bad_api_key_is_rejected(server_url: str) -> None:
    """The test that forced this suite off `--dev`.

    Under `serve --dev` the server bypasses authentication entirely, so this
    would pass while proving nothing.
    """
    with (
        IronflowRPC(server_url=server_url, api_key="ifkey_not_a_real_key") as rpc,
        pytest.raises(IronflowRPCError) as exc,
    ):
        rpc.pubsub.list_topics(ListTopicsRequest())

    assert exc.value.code in ("unauthenticated", "permission_denied"), exc.value.code


def test_no_api_key_is_rejected(server_url: str) -> None:
    with (
        IronflowRPC(server_url=server_url) as rpc,
        pytest.raises(IronflowRPCError) as exc,
    ):
        rpc.pubsub.list_topics(ListTopicsRequest())

    assert exc.value.code in ("unauthenticated", "permission_denied"), exc.value.code


def test_a_valid_key_is_accepted(rpc: IronflowRPC) -> None:
    """The control for the two tests above.

    Without it, a server that rejected EVERY request would make them both pass.
    """
    rpc.pubsub.list_topics(ListTopicsRequest())


def test_async_client_reports_auth_failure_too(server_url: str) -> None:
    async def scenario() -> str:
        async with AsyncIronflowRPC(
            server_url=server_url, api_key="ifkey_not_a_real_key"
        ) as rpc:
            try:
                await rpc.pubsub.list_topics(ListTopicsRequest())
            except IronflowRPCError as err:
                return err.code
        return ""

    assert asyncio.run(scenario()) in ("unauthenticated", "permission_denied")


# ── 6. deadlines against a real stream ──────────────────────────────────────


def test_stream_deadline_ends_a_real_subscription(rpc: IronflowRPC) -> None:
    """A client deadline bounds the WHOLE subscription, not the gap between events.

    Deterministic because the topic is unique and nobody ever publishes to it:
    the stream opens, stays open, and the deadline is the only thing that can
    end it. No dependence on a handler being slow.

    The elapsed-time assertion is the part that matters. Without it this would
    pass against a client that gave up instantly, or one that ignored the
    deadline and was rescued by pytest timing out the run.
    """
    quiet = unique("pysdk.quiet")
    start = time.monotonic()

    with pytest.raises(IronflowRPCError) as exc:
        for _ in rpc.pubsub.subscribe(
            SubscribeRequest(pattern=f"topic:{quiet}"), timeout=4.0
        ):
            pytest.fail(f"a topic nobody published to yielded an event: {quiet}")

    elapsed = time.monotonic() - start
    assert exc.value.code == "deadline_exceeded", (
        f"expected the deadline to end the stream, got {exc.value.code}"
    )
    assert 2.0 <= elapsed <= 20.0, (
        f"the stream ended after {elapsed:.1f}s for a 4s deadline — either the "
        f"deadline never reached the wire, or the client gave up on its own"
    )


# ── 7. cancellation ─────────────────────────────────────────────────────────


def test_abandoning_a_real_stream_leaves_the_client_usable(
    rpc: IronflowRPC, publish_rpc: IronflowRPC
) -> None:
    """The property that would actually bite someone.

    `IronflowRPC` shares one pyqwest transport across all eight namespaces. If
    walking away from a subscription left that transport wedged, every later
    call on the same client would fail — far from the subscription that caused
    it. The stub suite asserts this too; only a real server proves it survives
    a real connection being abandoned mid-flight.

    Breaking out of the loop is the documented cancellation form, so that is
    what this exercises.
    """
    topic = unique("pysdk.cancel")
    payload = uuid.uuid4().hex
    publisher = publish_after(publish_rpc, topic, payload)

    received: list[Any] = []
    try:
        for event in rpc.pubsub.subscribe(
            SubscribeRequest(pattern=f"topic:{topic}"), timeout=15.0
        ):
            received.append(event)
            break  # walk away with the connection still open
    except IronflowRPCError as err:
        if err.code != "deadline_exceeded":
            raise
    finally:
        publisher.join(timeout=5)

    assert received, f"no event arrived on {topic}; cancellation was never exercised"

    # The assertion. Two different namespaces, so this is the shared transport
    # being reused rather than one service client that happened to survive.
    rpc.pubsub.list_topics(ListTopicsRequest())
    source = rpc.webhooks.create_source(
        CreateWebhookSourceRequest(name=unique("after-cancel"), event_prefix="pysdk")
    )
    assert source.id
    rpc.webhooks.delete_source(DeleteWebhookSourceRequest(id=source.id))


# ── 8. error translation DURING iteration ───────────────────────────────────


def test_failure_after_real_events_translates_during_iteration(
    rpc: IronflowRPC, publish_rpc: IronflowRPC
) -> None:
    """The mid-iteration path, over a real connection, after real events.

    Ordering is the whole point. A stream that fails at CREATION takes the
    other branch of the interceptor, and wrapping only that branch would let
    every mid-iteration failure through as a raw connectrpc exception — thrown
    from inside the caller's `for` loop, where nothing suggests catching
    `IronflowRPCError` would help.

    So this asserts BOTH that a real event arrived first AND that the later
    failure surfaced as the Ironflow type. If the event count were dropped, a
    stream that died at creation would satisfy the rest.

    The failure here is the deadline rather than a server-generated
    application error, because the fanout handler has no reachable path to one
    — see this module's docstring. `tests/test_rpc_streams.py` covers
    `resource_exhausted` mid-stream against the stub.
    """
    topic = unique("pysdk.midfail")
    payload = uuid.uuid4().hex
    publisher = publish_after(publish_rpc, topic, payload)

    received: list[Any] = []
    try:
        with pytest.raises(IronflowRPCError) as exc:
            # No break: keep consuming past the event until the deadline ends it.
            for event in rpc.pubsub.subscribe(
                SubscribeRequest(pattern=f"topic:{topic}"), timeout=6.0
            ):
                # PERF402 suppressed on the next line: `received = list(stream)`
                # is NOT equivalent here. This loop is expected to raise, and
                # the point is to know what arrived BEFORE it did — list() would
                # discard every item when the exception propagated, leaving
                # nothing to assert on.
                received.append(event)  # noqa: PERF402
    finally:
        publisher.join(timeout=5)

    assert received, (
        f"no event arrived on {topic} before the deadline, so the failure was not "
        f"MID-iteration and this test proved nothing about the translating path"
    )
    assert exc.value.code == "deadline_exceeded"
    assert isinstance(exc.value, IronflowRPCError), (
        "a connectrpc exception escaped the translating iterator"
    )


# ── 9. a projection-only worker against a real server (#2395) ────────────────


def test_projection_only_worker_reduces_real_events(
    server_url: str, api_key: str, rpc: IronflowRPC
) -> None:
    """A Worker with zero functions still registers and runs a managed projection.

    Three real events go in over the RPC facade; the projection runner streams
    (or polls) them off a real server and the reduced count comes back through
    GetProjection. This is the one property that only a live server proves:
    the worker's `_start_projection_runners` wiring against the real
    ProjectionService, not a stub.
    """
    event_name = unique("py.live.inc")
    projection_name = unique("py-live-counter")

    def handler(state: dict[str, int], event: Any, ctx: Any) -> dict[str, int]:
        return {"count": state.get("count", 0) + 1}

    counter = create_projection(
        name=projection_name, events=[event_name], handler=handler, initial_state=lambda: {"count": 0}
    )
    worker = Worker(functions=[], projections=[counter], server_url=server_url, api_key=api_key)

    async def scenario() -> int:
        task = asyncio.ensure_future(worker.start())
        try:
            # Emit only once the runner has registered: GetProjection is not_found until then.
            deadline = time.monotonic() + 30.0
            while True:
                try:
                    rpc.projections.get(GetProjectionRequest(name=projection_name))
                    break
                except IronflowRPCError as exc:
                    if exc.code != "not_found" or time.monotonic() > deadline:
                        raise
                    await asyncio.sleep(0.2)

            for _ in range(3):
                rpc.events.emit(TriggerRequest(event=event_name))

            deadline = time.monotonic() + 30.0
            count = 0
            while time.monotonic() < deadline:
                resp = rpc.projections.get(GetProjectionRequest(name=projection_name))
                state: Any = resp.state.to_python() if resp.has_field("state") and resp.state is not None else {}
                count = int(state.get("count", 0)) if isinstance(state, dict) else 0
                if count >= 3:
                    break
                await asyncio.sleep(0.5)
            return count
        finally:
            await worker.stop()
            await asyncio.wait_for(task, 10)

    assert asyncio.run(scenario()) == 3


def test_kv_and_config_watch_see_real_updates(server_url: str, api_key: str) -> None:
    from ironflow import IronflowClient

    client = IronflowClient(server_url, api_key=api_key)
    bucket, cfg = unique("watch-b"), unique("watch-c")
    client.kv_buckets({"name": bucket})

    async def first(gen: Any, write: Any) -> Any:
        task = asyncio.ensure_future(gen.__anext__())
        await asyncio.sleep(0.5)  # let the socket subscribe before writing
        await asyncio.to_thread(write)
        try:
            return await asyncio.wait_for(task, 10)
        finally:
            await gen.aclose()

    kv = asyncio.run(first(client.watch_kv(bucket), lambda: client.kv_update_buckets_keys(bucket, "k", {"v": 1})))
    assert (kv.key, kv.operation) == ("k", "put") and kv.value is not None

    cf = asyncio.run(first(client.watch_config(cfg), lambda: client.config_create(cfg, {"on": True})))
    assert cf.name == cfg and cf.data == {"on": True}


def test_command_dedup_claims_finalizes_and_releases(server_url: str, api_key: str) -> None:
    from ironflow import IronflowClient

    dedup = IronflowClient(server_url, api_key=api_key).command_dedup(unique("dedup"))
    assert dedup.try_claim("c1", {"status": "claimed"}) is None  # winner
    assert dedup.try_claim("c1", {"status": "claimed"}) == {"status": "claimed"}  # duplicate sees the claim
    dedup.finalize("c1", {"status": "done"})
    assert dedup.try_claim("c1", {"status": "claimed"}) == {"status": "done"}
    dedup.release("c1")
    dedup.release("c1")  # idempotent
    assert dedup.try_claim("c1", {"status": "claimed"}) is None  # released, so a retry wins


def test_manual_ack_group_redelivers_after_nak(rpc: IronflowRPC, publish_rpc: IronflowRPC) -> None:
    topic, group, consumer = unique("pysdk.ack"), unique("ack-group"), unique("consumer")
    # In a MANUAL group redeliver_delay_ms is also the JetStream AckWait, so an
    # unacked event returns after it anyway. 60s keeps that out of the 20s
    # window: the redelivery seen below can only come from the NAK.
    rpc.pubsub.create_consumer_group(CreateConsumerGroupRequest(
        name=group, pattern=f"topic:{topic}", ack_mode=AckMode.MANUAL,
        redeliver_delay_ms=60000, max_redeliveries=5,
    ))

    # The group consumer may not exist yet when the first publish lands, so
    # republish until the first event arrives.
    got_first = threading.Event()

    def publish_until_received() -> None:
        while not got_first.is_set():
            publish_rpc.pubsub.publish(
                PublishRequest(topic=topic, data_value=Value.from_python(uuid.uuid4().hex))
            )
            got_first.wait(0.3)

    publisher = threading.Thread(target=publish_until_received, daemon=True)
    publisher.start()

    target: str | None = None
    naked_at = 0.0
    redelivered_after = None
    try:
        for event in rpc.pubsub.join_consumer_group(
            JoinConsumerGroupRequest(group_name=group, consumer_id=consumer), timeout=20.0
        ):
            # The server mints the ack identity (#2436); consumer_id is only a label.
            assert event.subscription_id and event.subscription_id != consumer
            if target is None:
                target = event.event_id
                got_first.set()
                naked_at = time.monotonic()
                rpc.pubsub.ack_event(AckEventRequest(
                    group_name=group, consumer_id=event.subscription_id,
                    event_id=target, ack_type=AckType.NAK, redeliver_delay_ms=1000,
                ))
            elif event.event_id == target:  # extra published events are ignored
                redelivered_after = time.monotonic() - naked_at
                rpc.pubsub.ack_event(AckEventRequest(
                    group_name=group, consumer_id=event.subscription_id,
                    event_id=target, ack_type=AckType.ACK,
                ))
                break
    except IronflowRPCError as err:
        if err.code != "deadline_exceeded":
            raise
    finally:
        got_first.set()
        publisher.join(timeout=5)

    assert target is not None, "no event reached the group"
    assert redelivered_after is not None, "nak did not redeliver the event"
    assert redelivered_after >= 0.8, f"redelivered {redelivered_after:.2f}s after nak, before its 1s delay"
