"""The worker's REST error rule matches the SDK-wide rule (ADR 0110)."""

from __future__ import annotations

import pytest

from ironflow import IronflowError
from ironflow.worker._transport import Reply
from ironflow.worker._worker import Worker


@pytest.mark.parametrize(
    ("status", "body", "retryable"),
    [
        (408, {"error": "timeout"}, True),
        (429, {"error": "slow down"}, True),
        (503, {"error": "busy"}, True),
        (501, {"error": "no route"}, False),
        (503, {"error": "full", "retryable": False}, False),
        (409, {"error": "x", "retryable": True}, True),
    ],
)
def test_raise_for_follows_shared_rule(status: int, body: dict, retryable: bool) -> None:
    with pytest.raises(IronflowError) as exc:
        Worker._raise_for(object.__new__(Worker), Reply(status, body), "register worker")
    assert exc.value.retryable is retryable
