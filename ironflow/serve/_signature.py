"""X-Ironflow-Signature, as the engine writes it (executor_transport.go signPayload)."""

from __future__ import annotations

import hashlib
import hmac
import time


class SignatureError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _mac(body: bytes, key: str, ts: int) -> str:
    return hmac.new(key.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()


def sign(body: bytes, key: str, ts: int) -> str:
    return f"t={ts},v1={_mac(body, key, ts)}"


def verify_signature(
    body: bytes, header: str | None, key: str, *, now: float | None = None, tolerance: float = 300.0,
) -> None:
    if not header:
        raise SignatureError("SIGNATURE_MISSING", "missing X-Ironflow-Signature header")
    parts = dict(p.split("=", 1) for p in header.split(",") if "=" in p)
    ts_raw, got = parts.get("t"), parts.get("v1")
    if not ts_raw or not got:
        raise SignatureError("SIGNATURE_INVALID", "malformed X-Ironflow-Signature header")
    try:
        ts = int(ts_raw)
    except ValueError:
        raise SignatureError("SIGNATURE_INVALID", "malformed signature timestamp") from None
    try:
        outside_tolerance = abs((time.time() if now is None else now) - ts) > tolerance
    except (ValueError, OverflowError):
        raise SignatureError("SIGNATURE_INVALID", "signature timestamp outside tolerance") from None
    if outside_tolerance:
        raise SignatureError("SIGNATURE_INVALID", "signature timestamp outside tolerance")
    if not hmac.compare_digest(_mac(body, key, ts).encode(), got.encode("utf-8", "surrogateescape")):
        raise SignatureError("SIGNATURE_INVALID", "signature mismatch")
