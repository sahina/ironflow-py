from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from ironflow.serve._asgi import serve
from ironflow.serve._webhook import Webhook, WebhookEvent
from ironflow.worker import function

EVENT = {"id": "ev", "name": "e", "data": {}, "version": 1, "timestamp": "2026-09-27T10:00:00Z"}


@function(id="fn", triggers=[{"event": "e"}])
async def fn(ctx: Any) -> str:
    return "ok"


def drive(app: Any, scope: dict[str, Any], messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sent: list[dict[str, Any]] = []
    inbox = list(messages)

    async def receive() -> dict[str, Any]:
        return inbox.pop(0)

    async def send(msg: dict[str, Any]) -> None:
        sent.append(msg)
    asyncio.run(app(scope, receive, send))
    return sent


def http(path: str = "/", method: str = "POST", root_path: str = "") -> dict[str, Any]:
    return {"type": "http", "method": method, "path": path, "root_path": root_path,
            "headers": [(b"Content-Type", b"application/json")]}


def body_chunks(data: bytes, n: int = 2) -> list[dict[str, Any]]:
    size = max(1, len(data) // n)
    parts = [data[i:i + size] for i in range(0, len(data), size)]
    return [{"type": "http.request", "body": p, "more_body": i < len(parts) - 1} for i, p in enumerate(parts)]


def test_chunked_body_completes_with_env_header() -> None:
    raw = json.dumps({"run_id": "r", "function_id": "fn", "event": EVENT}).encode()
    sent = drive(serve([fn], signing_key="", environment="staging"), http(), body_chunks(raw, 3))
    start, body = sent
    assert start["status"] == 200
    assert (b"x-ironflow-environment", b"staging") in start["headers"]
    assert json.loads(body["body"])["result"] == "ok"


def test_unset_environment_reaches_the_run_as_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """#2471: the "default" fallback is for the response header only, not for RunInfo."""
    monkeypatch.delenv("IRONFLOW_ENV", raising=False)

    @function(id="env-fn", triggers=[{"event": "e"}])
    async def env_fn(ctx: Any) -> Any:
        return ctx.run.environment

    raw = json.dumps({"run_id": "r", "function_id": "env-fn", "event": EVENT}).encode()
    start, body = drive(serve([env_fn], signing_key=""), http(), body_chunks(raw, 1))
    assert start["status"] == 200
    assert (b"x-ironflow-environment", b"default") in start["headers"]
    assert json.loads(body["body"])["result"] is None


def test_root_path_is_stripped_for_webhooks() -> None:
    hook = Webhook(id="h", transform=lambda b: WebhookEvent("h.e"))
    app = serve([fn], webhooks=[hook], signing_key="")
    sent = drive(app, http(path="/api/ironflow/webhooks/h", root_path="/api/ironflow"),
                 [{"type": "http.request", "body": b"{}", "more_body": False}])
    assert sent[0]["status"] == 200 and json.loads(sent[1]["body"])["status"] == "accepted"


def test_root_path_is_stripped_only_on_a_segment_boundary() -> None:
    # Old ASGI convention: path is already relative. A "/web" mount must not eat "/webhooks".
    hook = Webhook(id="h", transform=lambda b: WebhookEvent("h.e"))
    sent = drive(serve([fn], webhooks=[hook], signing_key=""), http(path="/webhooks/h", root_path="/web"),
                 [{"type": "http.request", "body": b"{}", "more_body": False}])
    assert sent[0]["status"] == 200 and json.loads(sent[1]["body"])["status"] == "accepted"


def test_non_latin1_environment_header_does_not_raise() -> None:
    raw = json.dumps({"run_id": "r", "function_id": "fn", "event": EVENT}).encode()
    sent = drive(serve([fn], signing_key="", environment="生产"), http(), body_chunks(raw, 1))
    assert sent[0]["status"] == 200


def test_lifespan_startup_and_shutdown() -> None:
    sent = drive(serve([fn]), {"type": "lifespan"},
                 [{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}])
    assert [m["type"] for m in sent] == ["lifespan.startup.complete", "lifespan.shutdown.complete"]


def test_disconnect_mid_body_sends_nothing() -> None:
    sent = drive(serve([fn], signing_key=""), http(),
                 [{"type": "http.request", "body": b"{", "more_body": True}, {"type": "http.disconnect"}])
    assert sent == []


def test_websocket_scope_rejected() -> None:
    with pytest.raises(RuntimeError, match="websocket"):
        drive(serve([fn]), {"type": "websocket"}, [])


def test_duplicate_ids_rejected_at_construction() -> None:
    with pytest.raises(ValueError):
        serve([fn, fn])
    hook = Webhook(id="h", transform=lambda b: WebhookEvent("x"))
    with pytest.raises(ValueError):
        serve([fn], webhooks=[hook, hook])
