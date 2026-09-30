from __future__ import annotations

import asyncio
from typing import Any

import pytest

import ironflow.serve._register as register_module
from ironflow._http import IronflowError
from ironflow.serve import register
from ironflow.worker import function
from tests.serve.fakes import FakeReply, FakeTransport


@function(id="fn", triggers=[{"event": "e"}])
async def fn(ctx: Any) -> None: ...


@pytest.fixture(autouse=True)
def fake(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeTransport.sent, FakeTransport.reply = [], FakeReply(200, {"created": True})
    monkeypatch.setattr(register_module, "Transport", FakeTransport)


def test_register_posts_push_body() -> None:
    asyncio.run(register([fn], endpoint_url="https://app/ironflow", server_url="http://engine", api_key="k"))
    url, _method, body, key, _env = FakeTransport.sent[0]
    assert url == "http://engine/ironflow.v1.IronflowService/RegisterFunction" and key == "k"
    assert body["preferredMode"] == "EXECUTION_MODE_PUSH" and body["endpointUrl"] == "https://app/ironflow"


def test_register_raises_on_rejection() -> None:
    FakeTransport.reply = FakeReply(401, {"code": "unauthenticated"})
    with pytest.raises(IronflowError) as e:
        asyncio.run(register([fn], endpoint_url="https://app/ironflow", server_url="http://engine"))
    assert e.value.status_code == 401


def test_public_exports() -> None:
    import ironflow.serve as s
    assert set(s.__all__) == {"serve", "handle", "register", "Webhook", "WebhookEvent", "WebhookRequest",
                              "verify_signature", "SignatureError", "sign"}
