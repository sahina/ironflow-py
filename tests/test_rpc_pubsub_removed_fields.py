"""Pub/sub fields the engine never read are gone from the Python surface (#4114)."""

from typing import Any

import pytest

from ironflow.rpc.v1 import (
    AckEventRequest,
    ConsumerGroup,
    CreateConsumerGroupRequest,
    DeleteConsumerGroupRequest,
    EventMetadata,
    GetConsumerGroupRequest,
    JoinConsumerGroupRequest,
    ListConsumerGroupsRequest,
    SubscribeOptions,
)

REMOVED = [
    (CreateConsumerGroupRequest, "namespace"),
    (GetConsumerGroupRequest, "namespace"),
    (ListConsumerGroupsRequest, "namespace"),
    (DeleteConsumerGroupRequest, "namespace"),
    (JoinConsumerGroupRequest, "namespace"),
    (AckEventRequest, "namespace"),
    (ConsumerGroup, "namespace"),
    (SubscribeOptions, "namespace"),
    (SubscribeOptions, "ack_mode"),
    (SubscribeOptions, "backpressure"),
    (EventMetadata, "namespace"),
    (EventMetadata, "source"),
]


@pytest.mark.parametrize(("message", "field"), REMOVED)
def test_removed_field_cannot_be_set(message: Any, field: str) -> None:
    with pytest.raises(TypeError):
        message(**{field: "x"})
