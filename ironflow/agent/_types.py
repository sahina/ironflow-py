"""Agent value types."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal, TypedDict

from ..worker import Duration

ToolIdempotency = Literal["by_call", "by_args"]


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    handler: Callable[[Any], Any]
    input_schema: dict[str, Any] = field(default_factory=lambda: {"type": "object"})
    description: str = ""
    idempotent: ToolIdempotency = "by_call"
    timeout: Duration = "60s"
    scopes: tuple[str, ...] = ()


def define_tool(
    *, name: str, handler: Callable[[Any], Any], input_schema: dict[str, Any] | None = None,
    description: str = "", idempotent: ToolIdempotency = "by_call", timeout: Duration = "60s",
    scopes: tuple[str, ...] = (),
) -> ToolDefinition:
    """Define a tool. ``input_schema`` is JSON Schema, passed to the model.

    With ``ironflow-py[validate]`` installed, args are validated against it before the handler
    runs; without it they are not. Either way ToolValidationError is raised for args that are
    not JSON-serialisable.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("tool name must be a non-empty string")
    return ToolDefinition(name=name, handler=handler, input_schema=input_schema or {"type": "object"},
                          description=description, idempotent=idempotent, timeout=timeout,
                          scopes=tuple(scopes))


class LLMToolCall(TypedDict):
    name: str
    input: Any


class LLMCompleteResult(TypedDict, total=False):
    content: Any
    tool_calls: list[LLMToolCall]
    finish_reason: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class ApproveResult:
    approved: bool
    approver: str | None = None
    payload: Any = None
    reason: str | None = None


@dataclass(frozen=True)
class SpawnResult:
    output: Any = None
    run_id: str | None = None


@dataclass(frozen=True)
class MemoryConfig:
    stream_id: str
    projection: str
    entity_type: str = "agent"
    backend: Any = None  # a MemoryBackend; None builds one from IRONFLOW_URL / IRONFLOW_API_KEY
