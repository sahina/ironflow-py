"""The agent-tools group (#3003) through the Python client.

Registration and invoke are in test_agent_tools.py. Skips without
IRONFLOW_READINESS_SEED; `make test-readiness-live` sets it.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import pytest
from protobuf.wkt import Struct

from ironflow import IronflowRPCError
from ironflow.agent import DISPATCH_PATH, define_tool, expose_mcp
from ironflow.rpc.v1 import InvokeToolRequest, ListToolsRequest

pytestmark = pytest.mark.usefixtures("readiness_seed")


def test_agent_tool_list_and_unregister(rpc: Any, server_url: str, api_key: str) -> None:
    name = f"pysdk{time.time_ns()}"
    schema = {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}
    lookup = define_tool(name="lookup", description="Look up an order", handler=lambda i: {"got": i}, input_schema=schema)
    refund = define_tool(name="refund", description="Refund an order", handler=lambda i: {"got": i}, input_schema=schema)

    # The registry is shared with the other SDKs' tools, so look only at this agent's.
    def mine() -> list[Any]:
        return [t for t in rpc.agent_tools.list(ListToolsRequest()).tools if t.qualified_name.startswith(f"{name}.")]

    async def scenario() -> None:
        # The server checks a callback URL's scheme and, for an IP literal, its range.
        # A hostname passes, and nothing here is dispatched.
        handle = await expose_mcp(
            name=name, callback_url="https://tools.example.test/api/ironflow" + DISPATCH_PATH,
            tools=[lookup, refund], server_url=server_url, api_key=api_key,
        )
        try:
            tools = mine()
            assert sorted(t.qualified_name for t in tools) == [f"{name}.lookup", f"{name}.refund"]
            found = next(t for t in tools if t.qualified_name == f"{name}.lookup")
            assert found.description == "Look up an order"
            assert json.loads(found.input_schema_json) == schema
            assert list(found.required_scopes) == []
        finally:
            await handle.unregister()

    asyncio.run(scenario())

    assert mine() == []
    with pytest.raises(IronflowRPCError) as exc:
        rpc.agent_tools.invoke(InvokeToolRequest(tool_name=f"{name}.lookup", input=Struct.from_python({"id": "o-1"})))
    assert exc.value.code == "not_found"
