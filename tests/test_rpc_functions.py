"""Function facades retain configuration and asynchronous invocation results."""

import asyncio
from typing import Any

import pytest
from protobuf.wkt import Value

from ironflow import AsyncIronflowRPC, IronflowRPC, IronflowRPCError
from ironflow.rpc.v1 import (
    GetFunctionRequest,
    InvokeFunctionRequest,
    ListFunctionsRequest,
)

from .rpc_server import serve


@pytest.mark.parametrize("client_cls", [IronflowRPC, AsyncIronflowRPC])
def test_functions(client_cls: Any) -> None:
    async def exercise() -> None:
        with serve() as server:
            client = client_cls(server_url=server.url)

            async def resolve(value: Any) -> Any:
                return await value if asyncio.iscoroutine(value) else value

            try:
                listed = await resolve(
                    client.functions.list(
                        ListFunctionsRequest(name="Process", mode="pull", offset=2)
                    )
                )
                assert listed.total_count == 3
                assert listed.functions[0].concurrency.limit == 5
                fn = await resolve(client.functions.get(GetFunctionRequest(id="fn-1")))
                assert fn.name == "Process"
                assert fn.debounce.max_wait_ms == 9007199254740993
                invoked = await resolve(
                    client.functions.invoke(
                        InvokeFunctionRequest(
                            function_id="fn-1",
                            data_value=Value.from_python(["input", 42]),
                            idempotency_key="same",
                        )
                    )
                )
                assert invoked.run_id == "run-1"
                assert invoked.event_id == "event-1"
                with pytest.raises(IronflowRPCError) as exc:
                    await resolve(
                        client.functions.get(GetFunctionRequest(id="missing"))
                    )
                assert exc.value.retryable is False
            finally:
                if isinstance(client, AsyncIronflowRPC):
                    await client.aclose()
                else:
                    client.close()

    asyncio.run(exercise())
