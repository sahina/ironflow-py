import pytest

from ironflow.agent import (
    DuplicateToolError,
    LLMError,
    LLMRefusalError,
    MaxTurnsExceededError,
    define_tool,
)
from ironflow.worker import NonRetryableError


def test_define_tool_defaults() -> None:
    t = define_tool(name="search", handler=lambda i: i)
    assert t.input_schema == {"type": "object"}
    assert t.idempotent == "by_call"
    assert t.timeout == "60s"
    assert t.scopes == ()


def test_define_tool_rejects_empty_name() -> None:
    with pytest.raises(ValueError):
        define_tool(name="", handler=lambda i: i)


def test_errors_are_non_retryable_with_codes() -> None:
    e = MaxTurnsExceededError(3)
    assert isinstance(e, NonRetryableError)
    assert e.code == "AGENT_MAX_TURNS_EXCEEDED" and e.retryable is False
    assert isinstance(LLMRefusalError("no"), LLMError)
    assert LLMRefusalError("no").code == "LLM_REFUSAL"
    assert "search" in str(DuplicateToolError("search"))
