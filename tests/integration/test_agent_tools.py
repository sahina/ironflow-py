"""expose_mcp registers a Python tool; the engine calls it back through serve() (#2406)."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from protobuf.wkt import Struct

from ironflow import IronflowRPC
from ironflow.agent import DISPATCH_PATH, define_tool, expose_mcp
from ironflow.rpc.v1 import InvokeToolRequest, ListToolsRequest
from ironflow.serve import serve

from .test_push_serve import running


def test_exposed_tool_round_trips_through_dispatch(rpc: IronflowRPC, server_url: str, api_key: str) -> None:
    agent_name = f"pyit{uuid.uuid4().hex[:8]}"
    echo = define_tool(name="echo", handler=lambda i: {"got": i}, input_schema={"type": "object"})

    async def scenario(app_url: str) -> None:
        # One event loop for both calls: expose_mcp builds the AsyncIronflowRPC that
        # handle.unregister() later closes, and a client cannot cross loops.
        handle = await expose_mcp(name=agent_name, callback_url=app_url + DISPATCH_PATH, tools=[echo],
                                  server_url=server_url, api_key=api_key)
        try:
            names: list[str] = []
            cursor = ""
            while True:
                page = rpc.agent_tools.list(ListToolsRequest(cursor=cursor))
                names += [t.qualified_name for t in page.tools]
                cursor = page.next_cursor
                if not cursor:
                    break
            assert f"{agent_name}.echo" in names

            out = rpc.agent_tools.invoke(InvokeToolRequest(tool_name=f"{agent_name}.echo",
                                                           input=Struct.from_python({"x": 1})))
            assert not out.has_field("error"), out
            assert out.output is not None
            got: Any = out.output.to_python()
            assert got == {"got": {"x": 1.0}}  # Struct numbers are floats
        finally:
            await handle.unregister()

    with running(serve([])) as app_url:
        asyncio.run(scenario(app_url))
