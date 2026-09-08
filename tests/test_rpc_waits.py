"""Projection waits through the generated Connect server and Python facade."""

import asyncio
from typing import Any

import pytest
from protobuf.wkt import Duration

from ironflow import AsyncIronflowRPC, IronflowRPC
from ironflow.rpc.v1 import (
    WaitForEventRequest,
    WaitItem,
    WaitProjectionCatchupBatchRequest,
)

from .rpc_server import serve


@pytest.mark.parametrize("client_cls", [IronflowRPC, AsyncIronflowRPC])
def test_projection_waits(client_cls: Any) -> None:
    async def exercise() -> None:
        with serve() as srv:
            client = client_cls(server_url=srv.url)
            try:
                result = client.projections.wait_for_event(
                    WaitForEventRequest(
                        event_id="evt_1",
                        projection="orders",
                        timeout=Duration(seconds=5),
                    )
                )
                if asyncio.iscoroutine(result):
                    result = await result
                assert result.caught_up is True
                assert result.current_seq == 7
                assert result.mode == "managed"
                batch = client.projections.wait_catchup_batch(
                    WaitProjectionCatchupBatchRequest(
                        items=[
                            WaitItem(name="orders", min_seq=9007199254740993),
                            WaitItem(name="missing", min_seq=1),
                        ],
                        timeout=Duration(seconds=5),
                    )
                )
                if asyncio.iscoroutine(batch):
                    batch = await batch
                assert batch.results[0].result.current_seq == 9007199254740993
                assert batch.results[1].error == "projection not found"
            finally:
                if isinstance(client, AsyncIronflowRPC):
                    await client.aclose()
                else:
                    client.close()

    asyncio.run(exercise())
