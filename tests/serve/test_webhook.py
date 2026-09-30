from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

import ironflow.serve._webhook as webhook_module
from ironflow._http import IronflowError
from ironflow.serve._handler import handle
from ironflow.serve._webhook import Webhook, WebhookEvent, WebhookRequest
from tests.serve.fakes import FakeReply, FakeTransport


@pytest.fixture(autouse=True)
def fake(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeTransport.sent, FakeTransport.reply = [], FakeReply(200, {})
    monkeypatch.setattr(webhook_module, "Transport", FakeTransport)
    monkeypatch.delenv("IRONFLOW_SERVER_URL", raising=False)


def call(hooks: list[Webhook], hook_id: str, *, method: str = "POST", body: bytes = b"{}",
         **kw: Any) -> tuple[int, dict[str, Any]]:
    status, _, out = asyncio.run(handle([], method=method, path=f"/webhooks/{hook_id}",
                                        headers={"x-sig": "s"}, body=body, webhooks=hooks, **kw))
    return status, json.loads(out)


def stripe(**kw: Any) -> Webhook:
    return Webhook(id="stripe", transform=lambda b: WebhookEvent("stripe.paid", json.loads(b), "idem-1"), **kw)


def test_accepted_emits_object_as_data() -> None:
    status, body = call([stripe()], "stripe", body=b'{"a":1}', server_url="http://engine", api_key="k")
    assert status == 200 and body["status"] == "accepted"
    assert body["event"] == {"name": "stripe.paid", "data": {"a": 1}, "idempotency_key": "idem-1"}
    url, method, sent, key, _env = FakeTransport.sent[0]
    assert url == "http://engine/ironflow.v1.IronflowService/Emit" and method == "POST" and key == "k"
    assert sent == {"event": "stripe.paid", "data": {"a": 1}, "idempotencyKey": "idem-1"}


def test_non_object_data_goes_as_data_value() -> None:
    hook = Webhook(id="h", transform=lambda b: WebhookEvent("h.e", [1, 2]))
    call([hook], "h", server_url="http://engine")
    assert FakeTransport.sent[0][2] == {"event": "h.e", "dataValue": [1, 2]}


def test_async_verify_and_transform() -> None:
    seen: list[WebhookRequest] = []

    async def verify(r: WebhookRequest) -> None:
        seen.append(r)

    async def transform(b: bytes) -> WebhookEvent:
        return WebhookEvent("x")
    status, _ = call([Webhook(id="h", transform=transform, verify=verify)], "h")
    assert status == 200 and seen[0].headers["x-sig"] == "s" and seen[0].path == "/webhooks/h"


def test_no_server_url_means_no_emit_and_200() -> None:
    status, _ = call([stripe()], "stripe")
    assert status == 200 and FakeTransport.sent == []


def test_server_url_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IRONFLOW_SERVER_URL", "http://envserver")
    call([stripe()], "stripe")
    assert FakeTransport.sent[0][0].startswith("http://envserver/")


def test_unknown_webhook() -> None:
    status, body = call([stripe()], "nope")
    assert status == 404 and body["error"]["code"] == "WEBHOOK_NOT_FOUND"


def test_verify_failed() -> None:
    def bad(r: WebhookRequest) -> None:
        raise ValueError("bad sig")
    status, body = call([stripe(verify=bad)], "stripe")
    assert status == 401 and body["error"] == {"code": "VERIFY_FAILED", "message": "bad sig"}


def test_transform_failed() -> None:
    def bad(b: bytes) -> WebhookEvent:
        raise ValueError("bad body")
    status, body = call([Webhook(id="h", transform=bad)], "h")
    assert status == 400 and body["error"]["code"] == "TRANSFORM_FAILED"


def test_transform_returning_wrong_type_is_transform_failed() -> None:
    status, body = call([Webhook(id="h", transform=lambda b: {"name": "x"})], "h")  # type: ignore[arg-type,return-value]
    assert status == 400 and body["error"]["code"] == "TRANSFORM_FAILED"


def test_transform_returning_non_json_encodable_data_is_transform_failed() -> None:
    status, body = call([Webhook(id="h", transform=lambda b: WebhookEvent("h.e", {1, 2}))], "h")
    assert status == 400 and body["error"]["code"] == "TRANSFORM_FAILED"


def test_transform_returning_nan_data_is_transform_failed() -> None:
    status, body = call([Webhook(id="h", transform=lambda b: WebhookEvent("h.e", float("nan")))], "h")
    assert status == 400 and body["error"]["code"] == "TRANSFORM_FAILED"


def test_emit_network_error_and_rejection_are_502() -> None:
    FakeTransport.reply = IronflowError("down", code="NETWORK_ERROR", retryable=True)
    assert call([stripe()], "stripe", server_url="http://engine")[1]["error"]["code"] == "EMIT_FAILED"
    FakeTransport.reply = FakeReply(403, {"code": "permission_denied"})
    status, body = call([stripe()], "stripe", server_url="http://engine")
    assert status == 502 and body["error"]["code"] == "EMIT_FAILED"


def test_get_webhook_is_405() -> None:
    assert call([stripe()], "stripe", method="GET")[0] == 405


def test_signing_key_does_not_apply_to_webhooks() -> None:
    assert call([stripe()], "stripe", signing_key="k")[0] == 200


def test_emit_with_unusable_server_url_is_502_not_a_raise() -> None:
    FakeTransport.reply = ValueError("Invalid URL: invalid IPv6 address")  # what pyqwest raises for "http://[bad"
    status, body = call([stripe()], "stripe", server_url="http://[bad")
    assert status == 502 and body["error"]["code"] == "EMIT_FAILED"
