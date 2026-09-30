"""step.publish transport: one Connect JSON call to PubSubService/Publish."""

from __future__ import annotations

from typing import Any

from .._http import IronflowError, _status_is_retryable
from ._step import PublishFn
from ._transport import Transport

PUBLISH_PATH = "/ironflow.v1.PubSubService/Publish"


def bind_publish(transport: Transport, run_id: str) -> PublishFn:
    async def publish(topic: str, data: Any, idempotency_key: str | None) -> dict[str, Any]:
        body: dict[str, Any] = {"topic": topic}
        # PublishRequest.data is a Struct; a list or scalar has to travel as data_value (#1963).
        if data is None:
            body["data"] = dict[str, Any]()
        elif isinstance(data, dict):
            body["data"] = data
        else:
            body["dataValue"] = data
        if idempotency_key:
            body["idempotencyKey"] = idempotency_key
        # The header lets the flow map learn function -> topic edges (#1706).
        reply = await transport.request("POST", PUBLISH_PATH, body, headers={"X-Ironflow-Run-ID": run_id})
        payload = reply.body if isinstance(reply.body, dict) else {}
        if not reply.ok:
            message = payload.get("message") or f"HTTP {reply.status}"
            raise IronflowError(
                f"publish to {topic!r} failed: {message}", status_code=reply.status,
                code=payload.get("code") or "PUBLISH_FAILED", retryable=_status_is_retryable(reply.status),
            )
        event_id = payload.get("eventId")
        if not isinstance(event_id, str) or not event_id:
            raise IronflowError(f"publish to {topic!r}: the reply carried no eventId", code="PUBLISH_FAILED")
        return {"eventId": event_id, "sequence": _sequence(payload.get("sequence"))}

    return publish


def _sequence(value: Any) -> int:
    # uint64 travels as a JSON string.
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
