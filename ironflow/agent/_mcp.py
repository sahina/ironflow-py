"""``expose_mcp``: register tools with the engine so MCP clients can dispatch to them."""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from typing import Any

from .._discovery import hydrate_env_from_discovery
from ._errors import AgentError, DuplicateToolError
from ._registry import RegisteredTool, register_local, unregister_local
from ._types import ToolDefinition


class ExposeMcpHandle:
    status = "active"

    def __init__(self, name: str, tool_names: Sequence[str], rpc: Any, owns_rpc: bool) -> None:
        self.name = name
        self.tool_names = tuple(tool_names)
        self.tool_count = len(self.tool_names)
        self._rpc, self._owns_rpc = rpc, owns_rpc
        self._done = False

    async def unregister(self) -> None:
        if self._done:
            return
        self._done = True
        unregister_local(self.name)
        from ..rpc.v1 import UnregisterToolRequest
        try:
            await self._rpc.agent_tools.unregister(UnregisterToolRequest(agent_name=self.name))
        except Exception as exc:
            raise AgentError(f"unregister failed: {exc}", "AGENT_MCP_UNREGISTER_FAILED") from exc
        finally:
            if self._owns_rpc:
                await self._rpc.aclose()


async def expose_mcp(
    *, name: str, callback_url: str, tools: Sequence[ToolDefinition], server_url: str | None = None,
    api_key: str | None = None, rpc: Any = None, environment: str | None = None,
) -> ExposeMcpHandle:
    """Register tools with the engine. MCP clients reach them through ``serve()``'s dispatch route.

    ``environment`` (else ``IRONFLOW_ENV``) scopes register and unregister. It applies only when
    the SDK builds the client; a passed ``rpc`` keeps its own.
    """
    if not tools:
        raise AgentError("expose_mcp() requires at least one tool", "AGENT_MCP_NO_TOOLS")
    if not callback_url:
        raise AgentError("expose_mcp() requires callback_url pointing at your serve() mount",
                         "AGENT_MCP_MISSING_CALLBACK_URL")
    seen: set[str] = set()
    for t in tools:
        if t.name in seen:
            raise DuplicateToolError(t.name)
        seen.add(t.name)
    owns_rpc = rpc is None
    if rpc is None:
        hydrate_env_from_discovery()
        url = server_url or os.environ.get("IRONFLOW_URL") or os.environ.get("IRONFLOW_SERVER_URL")
        if not url:
            raise AgentError("expose_mcp() requires server_url (or IRONFLOW_URL / IRONFLOW_SERVER_URL)",
                             "AGENT_MCP_MISSING_SERVER_URL")
        key = api_key or os.environ.get("IRONFLOW_API_KEY")
        if not key:
            raise AgentError("expose_mcp() requires api_key (or IRONFLOW_API_KEY) with agent:tools:register",
                             "AGENT_MCP_MISSING_API_KEY")
        from ..rpc import AsyncIronflowRPC
        rpc = AsyncIronflowRPC(server_url=url, api_key=key,
                               environment=environment or os.environ.get("IRONFLOW_ENV") or None)
    from ..rpc.v1 import RegisterToolRequest, ToolDef
    resp = await rpc.agent_tools.register(RegisterToolRequest(
        agent_name=name, callback_url=callback_url,
        tools=[ToolDef(name=t.name, description=t.description, input_schema_json=json.dumps(t.input_schema),
                       required_scopes=list(t.scopes), timeout_ms=0) for t in tools],
    ))
    if not resp.hmac_secret or not resp.registered_tool_names:
        if owns_rpc:
            await rpc.aclose()
        raise AgentError("RegisterTool response missing hmac_secret or registered_tool_names",
                         "AGENT_MCP_INVALID_RESPONSE")
    for t in tools:
        register_local(RegisteredTool(name, f"{name}.{t.name}", resp.hmac_secret, t))
    return ExposeMcpHandle(name, list(resp.registered_tool_names), rpc, owns_rpc)
