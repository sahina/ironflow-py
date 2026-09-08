"""Entity-stream facade round trips through generated Connect handlers."""

import asyncio
from typing import Any

import pytest
from protobuf.wkt import Timestamp, Value

from ironflow import AsyncIronflowRPC, IronflowRPC, IronflowRPCError
from ironflow.rpc.v1 import (
    AppendEventRequest,
    CreateSnapshotRequest,
    GetEntityHistoryRequest,
    GetSnapshotRequest,
    GetStreamInfoRequest,
    ListStreamsRequest,
    ReadStreamRequest,
)

from .rpc_server import serve


@pytest.mark.parametrize("client_cls", [IronflowRPC, AsyncIronflowRPC])
def test_streams(client_cls: Any) -> None:
    async def exercise() -> None:
        with serve() as server:
            client = client_cls(server_url=server.url)

            async def resolve(value: Any) -> Any:
                return await value if asyncio.iscoroutine(value) else value

            try:
                version = 9007199254740993
                streams = client.streams
                appended = await resolve(
                    streams.append_event(
                        AppendEventRequest(
                            entity_id="order-1", expected_version=version
                        )
                    )
                )
                assert appended.entity_version == version + 1
                read = await resolve(
                    streams.read_stream(
                        ReadStreamRequest(entity_id="order-1", from_version=version)
                    )
                )
                assert read.events[0].entity_version == version
                info = await resolve(
                    streams.get_info(GetStreamInfoRequest(entity_id="order-1"))
                )
                assert info.version == version
                listed = await resolve(
                    streams.list_streams(ListStreamsRequest(search="order"))
                )
                assert listed.streams[0].version == version
                history = await resolve(
                    streams.get_history(
                        GetEntityHistoryRequest(
                            entity_id="order-1", from_timestamp=Timestamp(seconds=123)
                        )
                    )
                )
                assert history.entries[0].entity_version == version
                created = await resolve(
                    streams.create_snapshot(
                        CreateSnapshotRequest(
                            entity_id="order-1",
                            entity_version=version,
                            state_value=Value.from_python("state"),
                        )
                    )
                )
                assert created.snapshot_id == "snapshot-1"
                snapshot = await resolve(
                    streams.get_snapshot(
                        GetSnapshotRequest(entity_id="order-1", before_version=version)
                    )
                )
                assert snapshot.entity_version == version
                with pytest.raises(IronflowRPCError) as exc:
                    await resolve(
                        streams.get_info(GetStreamInfoRequest(entity_id="missing"))
                    )
                assert exc.value.retryable is False
            finally:
                if isinstance(client, AsyncIronflowRPC):
                    await client.aclose()
                else:
                    client.close()

    asyncio.run(exercise())
