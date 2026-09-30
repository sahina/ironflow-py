"""Agent errors. Codes match Go agent/errors.go and Node agent/errors.ts."""

from __future__ import annotations

from typing import Any

from ..worker import NonRetryableError


class AgentError(NonRetryableError):
    def __init__(self, message: str, code: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, code)
        self.details = details or {}


class MaxTurnsExceededError(AgentError):
    def __init__(self, max_turns: int) -> None:
        super().__init__(f"agent exceeded max_turns ({max_turns})", "AGENT_MAX_TURNS_EXCEEDED",
                         {"max_turns": max_turns})


class LLMError(AgentError):
    """Base for classified LLM errors raised by ``ctx.llm``."""


class LLMRefusalError(LLMError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, "LLM_REFUSAL", details)


class LLMInvalidJSONError(LLMError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, "LLM_INVALID_JSON", details)


class LLMMaxTokensError(LLMError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, "LLM_MAX_TOKENS", details)


class ToolValidationError(AgentError):
    def __init__(self, tool_name: str, cause: str) -> None:
        super().__init__(f"tool {tool_name!r} input validation failed: {cause}", "AGENT_TOOL_VALIDATION",
                         {"tool": tool_name})


class DuplicateToolError(AgentError):
    def __init__(self, tool_name: str) -> None:
        super().__init__(f"duplicate tool {tool_name!r} registered on the agent", "AGENT_DUPLICATE_TOOL",
                         {"tool": tool_name})


class ToolNotFoundError(AgentError):
    def __init__(self, tool_name: str) -> None:
        super().__init__(f"tool {tool_name!r} is not registered on this agent", "AGENT_TOOL_NOT_FOUND",
                         {"tool": tool_name})


class MemoryProjectionRequiredError(AgentError):
    def __init__(self, stream_id: str) -> None:
        super().__init__(f"memory for stream {stream_id!r} requires a projection name",
                         "AGENT_MEMORY_PROJECTION_REQUIRED", {"stream_id": stream_id})
