"""The pubsub group (#3003) through the Python client.

Skips without IRONFLOW_READINESS_SEED; `make test-readiness-live` sets it.
test_live.py also covers subscribe under `make test-python-integration`.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import pytest
from protobuf.wkt import Value

from ironflow import IronflowRPCError
from ironflow.rpc.v1 import (
    GetTopicStatsRequest,
    ListTopicsRequest,
    PublishRequest,
    SubscribeOptions,
    SubscribeRequest,
)

pytestmark = pytest.mark.usefixtures("readiness_seed")


def test_pubsub_subscribe(rpc: Any) -> None:
    topic = f"pysdk.sub.{time.time_ns()}"
    published = [
        rpc.pubsub.publish(PublishRequest(topic=topic, data_value=Value.from_python({"n": n}))) for n in range(2)
    ]

    # The pattern needs the topic: namespace; publish takes the bare topic.
    seen: list[Any] = []
    try:
        for event in rpc.pubsub.subscribe(
            SubscribeRequest(pattern=f"topic:{topic}", options=SubscribeOptions(replay=2, include_metadata=True)),
            timeout=10.0,
        ):
            seen.append(event)
            if len(seen) == 2:
                break
    except IronflowRPCError as err:
        if err.code != "deadline_exceeded":
            raise

    assert [e.data.to_python() for e in seen] == [{"n": 0}, {"n": 1}]
    # The stream gives the ID and stream sequence that publish returned.
    assert [(e.event_id, e.sequence) for e in seen] == [(p.event_id, p.sequence) for p in published]
    assert {e.topic for e in seen} == {f"topic:{topic}"}
    # The delivery time, which is not the publish time: near now.
    now = datetime.now(timezone.utc)
    assert all(abs((e.metadata.timestamp.to_datetime() - now).total_seconds()) < 60 for e in seen)


def test_pubsub_topics_and_topic_stats(rpc: Any) -> None:
    topic = f"pysdk.topics.{time.time_ns()}"
    last = 0
    for n in range(3):
        last = rpc.pubsub.publish(PublishRequest(topic=topic, data_value=Value.from_python({"n": n}))).sequence

    # A subscription that resumes from a cursor owns a JetStream consumer on the
    # topic's subject; the three events it reads show that the consumer exists.
    stream = rpc.pubsub.subscribe(
        SubscribeRequest(pattern=f"topic:{topic}", options=SubscribeOptions(start_after_sequence=0)), timeout=15.0
    )
    try:
        assert [next(stream).data.to_python() for _ in range(3)] == [{"n": 0}, {"n": 1}, {"n": 2}]

        # Our consumer is one; the count is a floor because other suites may leave
        # consumers on the shared stream whose filter matches this topic.
        found = [t for t in rpc.pubsub.list_topics(ListTopicsRequest()).topics if t.name == topic]
        assert [(t.name, t.message_count) for t in found] == [(topic, 3)]
        assert found[0].consumer_count >= 1

        stats = rpc.pubsub.get_topic_stats(GetTopicStatsRequest(topic=topic))
        assert (stats.name, stats.message_count) == (topic, 3)
        assert stats.consumer_count >= 1 and stats.lag >= 0
        # Sequences are stream-wide, so other topics move them: the last one is a floor.
        assert stats.last_seq >= last
        assert 0 < stats.first_seq <= stats.last_seq
    finally:
        stream.close()

    never = rpc.pubsub.get_topic_stats(GetTopicStatsRequest(topic=f"{topic}.never"))
    assert (never.name, never.message_count) == (f"{topic}.never", 0)
