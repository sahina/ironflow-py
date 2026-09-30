"""Acknowledging a consumer-group event through the generated Connect service (#2412)."""

import asyncio
from typing import Any

import pytest

from ironflow import AsyncIronflowRPC, IronflowRPC
from ironflow.rpc.v1 import AckEventRequest, AckType

from .rpc_server import serve


@pytest.mark.parametrize("client_cls", [IronflowRPC, AsyncIronflowRPC])
def test_ack_event(client_cls: Any) -> None:
    async def exercise() -> None:
        with serve() as srv:
            client = client_cls(server_url=srv.url)
            try:
                result = client.pubsub.ack_event(
                    AckEventRequest(
                        group_name="workers",
                        consumer_id="consumer-1",
                        event_id="evt_1",
                        ack_type=AckType.NAK,
                        redeliver_delay_ms=250,
                    )
                )
                if asyncio.iscoroutine(result):
                    result = await result
                assert result is not None
            finally:
                if isinstance(client, AsyncIronflowRPC):
                    await client.aclose()
                else:
                    client.close()

    asyncio.run(exercise())
