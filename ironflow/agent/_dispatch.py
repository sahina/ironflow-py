"""Engine-to-app tool callback. Protocol matches Go agent/dispatch.go and Node agent/dispatch.ts."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from collections.abc import Mapping
from typing import Any

from ..worker._step import _invoke
from ._registry import lookup_local

DISPATCH_PATH = "/ironflow/agent-tools/dispatch"
_PREFIX = "sha256="
_REPLAY_S = 300
_FUTURE_S = 60
_MAX_BODY = 1 << 20
_log = logging.getLogger("ironflow.agent.dispatch")


def verify_hmac(raw_body: bytes, ts: int, received_hex: str, secret_hex: str) -> bool:
    try:
        secret, received = bytes.fromhex(secret_hex), bytes.fromhex(received_hex)
    except ValueError:
        return False
    if not received:
        return False
    expected = hmac.new(secret, f"{ts}.".encode() + raw_body, hashlib.sha256).digest()
    return hmac.compare_digest(received, expected)


def _err(status: int, code: str, message: str) -> tuple[int, dict[str, Any]]:
    return status, {"error": {"code": code, "message": message}}


async def handle_dispatch(
    headers: Mapping[str, str], body: bytes, *, now: float | None = None,
) -> tuple[int, dict[str, Any]]:
    if len(body) > _MAX_BODY:
        return _err(400, "INVALID_REQUEST", "failed to read body")
    sig, ts_raw = headers.get("x-ironflow-signature"), headers.get("x-ironflow-timestamp")
    if not sig or not ts_raw:
        return _err(401, "SIGNATURE_MISMATCH", "missing HMAC headers")
    if not sig.startswith(_PREFIX):
        return _err(401, "SIGNATURE_MISMATCH", "invalid signature format")
    try:
        ts = int(ts_raw)
    except ValueError:
        return _err(401, "TIMESTAMP_SKEW", "invalid timestamp")
    current = time.time() if now is None else now
    if current - ts > _REPLAY_S:
        return _err(401, "TIMESTAMP_SKEW", "request timestamp too old")
    if ts - current > _FUTURE_S:
        return _err(401, "TIMESTAMP_SKEW", "request timestamp too far in future")
    try:
        payload = json.loads(body)
    except (ValueError, RecursionError):
        return _err(400, "INVALID_REQUEST", "callback body is not valid JSON")
    if not isinstance(payload, dict):
        return _err(400, "INVALID_REQUEST", "callback body is not valid JSON")
    name = payload.get("qualified_name")
    if not isinstance(name, str) or not name:
        return _err(400, "INVALID_REQUEST", "qualified_name missing")
    entry = lookup_local(name)
    if entry is None:
        _log.warning("ironflow.agent.dispatch unknown_tool qualified_name=%r", name)
        return _err(401, "SIGNATURE_MISMATCH", "HMAC mismatch")
    if not verify_hmac(body, ts, sig[len(_PREFIX):], entry.hmac_secret):
        return _err(401, "SIGNATURE_MISMATCH", "HMAC mismatch")
    try:
        output = await _invoke(lambda: entry.defn.handler(payload.get("input")))
        json.dumps(output, allow_nan=False)  # fail here, not in the caller's json_response
    except Exception as exc:  # noqa: BLE001 - a tool failure is a response, not a 500
        return 200, {"error": {"code": "HANDLER_ERROR", "message": str(exc)}}
    return 200, {"output": output}
