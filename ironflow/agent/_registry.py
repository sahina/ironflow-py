"""Process-local table of tools exposed with expose_mcp. Dispatch looks tools up here."""

from __future__ import annotations

from dataclasses import dataclass

from ._types import ToolDefinition


@dataclass(frozen=True)
class RegisteredTool:
    agent_name: str
    qualified_name: str
    hmac_secret: str
    defn: ToolDefinition


_tools: dict[str, RegisteredTool] = {}


def register_local(entry: RegisteredTool) -> None:
    _tools[entry.qualified_name] = entry


def unregister_local(agent_name: str) -> list[str]:
    gone = [q for q, e in _tools.items() if e.agent_name == agent_name]
    for q in gone:
        del _tools[q]
    return gone


def lookup_local(qualified_name: str) -> RegisteredTool | None:
    return _tools.get(qualified_name)


def clear_local() -> None:
    _tools.clear()
