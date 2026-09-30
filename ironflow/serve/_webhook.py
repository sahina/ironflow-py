"""POST /webhooks/{id}: verify, transform, emit. Mirrors Go serve.go handleWebhook."""

from __future__ import annotations

import inspect
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from ..worker._transport import Transport
from ._response import Response, error_response, json_response


@dataclass(frozen=True)
class WebhookEvent:
    name: str
    data: Any = None
    idempotency_key: str | None = None


@dataclass(frozen=True)
class WebhookRequest:
    body: bytes
    headers: Mapping[str, str]
    method: str
    path: str


@dataclass(frozen=True)
class Webhook:
    id: str
    transform: Callable[[bytes], WebhookEvent | Awaitable[WebhookEvent]]
    verify: Callable[[WebhookRequest], None | Awaitable[None]] | None = None


async def _settle(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


async def handle_webhook(
    hooks: Mapping[str, Webhook], hook_id: str, *, method: str, path: str, headers: Mapping[str, str],
    body: bytes, server_url: str | None, api_key: str | None, environment: str,
) -> Response:
    hook = hooks.get(hook_id)
    if hook is None:
        return error_response(404, "WEBHOOK_NOT_FOUND", f"webhook source not found: {hook_id}")
    if hook.verify is not None:
        try:
            await _settle(hook.verify(WebhookRequest(body=body, headers=headers, method=method, path=path)))
        except Exception as exc:  # noqa: BLE001 - any verify failure is a rejection
            return error_response(401, "VERIFY_FAILED", str(exc))
    try:
        event = await _settle(hook.transform(body))
        if not isinstance(event, WebhookEvent):
            raise TypeError(f"transform must return WebhookEvent, got {type(event).__name__}")
        json.dumps(event.data, allow_nan=False)  # fail fast: same check json_response applies later
    except Exception as exc:  # noqa: BLE001 - any transform failure is a bad request
        return error_response(400, "TRANSFORM_FAILED", str(exc))

    if server_url:
        emit: dict[str, Any] = {"event": event.name}
        emit["data" if isinstance(event.data, dict) else "dataValue"] = event.data
        if event.idempotency_key:
            emit["idempotencyKey"] = event.idempotency_key
        # ponytail: one HTTP client per webhook call; share one if webhook volume makes it matter.
        try:
            reply = await Transport(server_url, api_key, environment).request(
                "POST", "/ironflow.v1.IronflowService/Emit", emit)
        except Exception as exc:  # noqa: BLE001 - a bad server URL raises ValueError/RuntimeError, not IronflowError
            return error_response(502, "EMIT_FAILED", f"failed to emit event: {exc}")
        if not reply.ok:
            return error_response(502, "EMIT_FAILED", f"server rejected event: {reply.body}")

    out: dict[str, Any] = {"name": event.name, "data": event.data}
    if event.idempotency_key:
        out["idempotency_key"] = event.idempotency_key
    return json_response(200, {"status": "accepted", "event": out})
