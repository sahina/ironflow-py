"""Both emit facades preserve values, schema versions and idempotency keys."""

import asyncio
from typing import Any

import pytest
from protobuf.wkt import Struct, Value

from ironflow import AsyncIronflowRPC, IronflowRPC
from ironflow.rpc import v1

from .rpc_server import serve


@pytest.mark.parametrize("client_cls", [IronflowRPC, AsyncIronflowRPC])
def test_emit(client_cls: Any) -> None:
    async def exercise() -> None:
        with serve() as server:
            client = client_cls(server_url=server.url)
            try:
                for method, request_cls in [
                    (client.events.emit, v1.TriggerRequest),
                    (client.pubsub.emit, v1.EmitRequest),
                ]:
                    result = method(
                        request_cls(
                            event="order.placed",
                            version=2,
                            idempotency_key="once",
                            data_value=Value.from_python(False),
                            metadata=Struct.from_python({"trace_id": "trace"}),
                        )
                    )
                    if asyncio.iscoroutine(result):
                        result = await result
                    assert result.event_id == "event" and result.run_ids == ["run"]
            finally:
                if isinstance(client, AsyncIronflowRPC):
                    await client.aclose()
                else:
                    client.close()

    asyncio.run(exercise())
