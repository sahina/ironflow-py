from __future__ import annotations

import asyncio
import socket
from typing import Any

import pytest

from ironflow import IronflowError
from ironflow.worker._publish import bind_publish
from ironflow.worker._transport import Transport
from tests.worker.conftest import run
from tests.worker.fake_engine import FakeEngine


def publisher(engine: FakeEngine) -> Any:
    return bind_publish(Transport(engine.url, "ifkey_x", "default"), "run_1")


def sent(engine: FakeEngine) -> dict[str, Any]:
    return engine.calls("publish")[0]


def test_object_data_is_sent_as_data_with_the_run_header(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    out = run(loop, publisher(engine)("orders", {"a": 1}, "k1"))
    assert out == {"eventId": "evt_pub_1", "sequence": 7}
    assert sent(engine)["body"] == {"topic": "orders", "data": {"a": 1}, "idempotencyKey": "k1"}
    headers = {k.lower(): v for k, v in sent(engine)["headers"].items()}
    assert headers["x-ironflow-run-id"] == "run_1" and headers["authorization"] == "Bearer ifkey_x"


@pytest.mark.parametrize("data,expected", [
    (None, {"data": {}}), ([1, 2], {"dataValue": [1, 2]}), ("hi", {"dataValue": "hi"}), (5, {"dataValue": 5}),
])
def test_non_object_data_uses_data_value(
    loop: asyncio.AbstractEventLoop, engine: FakeEngine, data: Any, expected: dict[str, Any],
) -> None:
    run(loop, publisher(engine)("t", data, None))
    assert sent(engine)["body"] == {"topic": "t", **expected}


def test_a_4xx_reply_is_not_retryable(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    engine.fail("publish", 400, {"code": "invalid_argument", "message": "bad topic"})
    with pytest.raises(IronflowError) as e:
        run(loop, publisher(engine)("t", {}, None))
    assert e.value.retryable is False and e.value.status_code == 400 and "bad topic" in str(e.value)


@pytest.mark.parametrize("status", [408, 429, 500, 503])
def test_transient_statuses_are_retryable(loop: asyncio.AbstractEventLoop, engine: FakeEngine, status: int) -> None:
    # A rate limit must not become a permanent failure: that would fail the run and fire its saga undos.
    engine.fail("publish", status, {"message": "try later"})
    with pytest.raises(IronflowError) as e:
        run(loop, publisher(engine)("t", {}, None))
    assert e.value.retryable is True and e.value.status_code == status


def test_a_501_reply_is_not_retryable(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    engine.fail("publish", 501, {"message": "not implemented"})
    with pytest.raises(IronflowError) as e:
        run(loop, publisher(engine)("t", {}, None))
    assert e.value.retryable is False and e.value.status_code == 501


def test_the_body_retryable_flag_wins_over_the_status(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    engine.fail("publish", 503, {"message": "gone", "retryable": False})
    with pytest.raises(IronflowError) as e:
        run(loop, publisher(engine)("t", {}, None))
    assert e.value.retryable is False


def test_a_success_reply_without_an_event_id_is_an_error(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    engine.fail("publish", 200, {})
    with pytest.raises(IronflowError, match="eventId"):
        run(loop, publisher(engine)("t", {}, None))


def test_an_error_body_without_a_message_reports_the_status_not_the_dict(
    loop: asyncio.AbstractEventLoop, engine: FakeEngine,
) -> None:
    engine.fail("publish", 400, {"code": "invalid_argument"})
    with pytest.raises(IronflowError) as e:
        run(loop, publisher(engine)("t", {}, None))
    assert str(e.value).endswith(": HTTP 400") and "{" not in str(e.value)


def test_a_401_reply_is_not_retryable(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    engine.fail("publish", 401, {"message": "bad key"})
    with pytest.raises(IronflowError) as e:
        run(loop, publisher(engine)("t", {}, None))
    assert e.value.retryable is False and e.value.status_code == 401


def test_a_success_reply_whose_body_is_not_an_object_is_an_error(
    loop: asyncio.AbstractEventLoop, engine: FakeEngine,
) -> None:
    engine.fail("publish", 200, ["nope"])
    with pytest.raises(IronflowError, match="eventId") as e:
        run(loop, publisher(engine)("t", {}, None))
    assert e.value.retryable is False


def test_a_network_error_is_retryable(loop: asyncio.AbstractEventLoop) -> None:
    with socket.socket() as sock:  # bind then close: the port is free, so nothing listens on it
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    publish = bind_publish(Transport(f"http://127.0.0.1:{port}", "ifkey_x", "default"), "run_1")
    with pytest.raises(IronflowError) as e:
        run(loop, publish("t", {}, None))
    assert e.value.retryable is True and e.value.code == "NETWORK_ERROR"
