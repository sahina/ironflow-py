"""Projection RPCs preserve typed fields through the generated protocol."""

import asyncio
from typing import Any

import pytest

from ironflow import AsyncIronflowRPC, IronflowRPC
from ironflow.rpc import v1

from .rpc_server import serve


@pytest.mark.parametrize("client_cls", [IronflowRPC, AsyncIronflowRPC])
def test_projections(client_cls: Any) -> None:
    async def exercise() -> None:
        with serve() as server:
            client = client_cls(server_url=server.url)

            async def resolve(value: Any) -> Any:
                return await value if asyncio.iscoroutine(value) else value

            try:
                projection = await resolve(
                    client.projections.get(v1.GetProjectionRequest(name="orders"))
                )
                assert projection.state_value.to_python() is False
                assert projection.registry.version_full == 9007199254740993
                listed = await resolve(
                    client.projections.list(v1.ListProjectionsRequest(offset=1000000))
                )
                assert listed.projections[0].description == "Order view"
                status = await resolve(
                    client.projections.get_status(
                        v1.GetProjectionStatusRequest(name="orders")
                    )
                )
                assert status.last_event_seq == 9007199254740993
                for method, req in [
                    (
                        client.projections.rebuild,
                        v1.RebuildProjectionRequest(name="orders"),
                    ),
                    (
                        client.projections.get_rebuild_job,
                        v1.GetRebuildJobRequest(name="orders"),
                    ),
                ]:
                    result = await resolve(method(req))
                    assert result.job.events_processed == 9007199254740993
                for method, req in [
                    (
                        client.projections.pause,
                        v1.PauseProjectionRequest(name="orders"),
                    ),
                    (
                        client.projections.resume,
                        v1.ResumeProjectionRequest(name="orders"),
                    ),
                    (
                        client.projections.cancel_rebuild,
                        v1.CancelRebuildRequest(name="orders"),
                    ),
                ]:
                    assert (await resolve(method(req))).status == "ok"
                waited = await resolve(
                    client.projections.wait_catchup(
                        v1.WaitProjectionCatchupRequest(
                            name="orders", min_seq=9007199254740993
                        )
                    )
                )
                assert waited.caught_up and waited.current_seq == 9007199254740993
            finally:
                if isinstance(client, AsyncIronflowRPC):
                    await client.aclose()
                else:
                    client.close()

    asyncio.run(exercise())
