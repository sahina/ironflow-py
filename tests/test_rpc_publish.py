"""Publishing through the generated Connect service."""

import asyncio
from typing import Any

import pytest
from protobuf.wkt import Struct

from ironflow import AsyncIronflowRPC, IronflowRPC
from ironflow.rpc.v1 import PublishRequest

from .rpc_server import serve


@pytest.mark.parametrize("client_cls", [IronflowRPC, AsyncIronflowRPC])
def test_publish(client_cls: Any) -> None:
    async def exercise() -> None:
        with serve() as srv:
            client = client_cls(server_url=srv.url)
            try:
                result = client.pubsub.publish(
                    PublishRequest(
                        topic="orders",
                        data=Struct.from_python({"id": 1}),
                        idempotency_key="key",
                    )
                )
                if asyncio.iscoroutine(result):
                    result = await result
                assert result.event_id == "evt_1"
                assert result.sequence == 9007199254740993
            finally:
                if isinstance(client, AsyncIronflowRPC):
                    await client.aclose()
                else:
                    client.close()

    asyncio.run(exercise())
