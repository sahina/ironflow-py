"""LLM result classification: refusal / max-tokens finish reasons become errors."""

from __future__ import annotations

from typing import Any

from ._errors import LLMMaxTokensError, LLMRefusalError

_REFUSAL = frozenset({"refusal", "safety", "content_filter"})
_MAX_TOKENS = frozenset({"max_tokens", "length"})


def classify(result: Any) -> None:
    reason = result.get("finish_reason") if isinstance(result, dict) else None
    if not reason:
        return
    details = {"finish_reason": reason, "metadata": result.get("metadata")}
    normalized = str(reason).lower()
    if normalized in _REFUSAL:
        raise LLMRefusalError(f"provider refused: {reason}", details)
    if normalized in _MAX_TOKENS:
        raise LLMMaxTokensError(f"provider hit max_tokens ({reason})", details)
