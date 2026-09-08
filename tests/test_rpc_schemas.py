"""Schema lifecycle through the generated Connect handlers."""

import asyncio
from typing import Any

import pytest

from ironflow import AsyncIronflowRPC, IronflowRPC, IronflowRPCError
from ironflow.rpc.v1 import (
    DeleteSchemaRequest,
    GetSchemaRequest,
    ListSchemasRequest,
    RegisterSchemaRequest,
)

from .rpc_server import serve


@pytest.mark.parametrize("client_cls", [IronflowRPC, AsyncIronflowRPC])
def test_schema_lifecycle(client_cls: Any) -> None:
    async def exercise() -> None:
        with serve() as srv:
            client = client_cls(server_url=srv.url)

            async def resolve(result: Any) -> Any:
                return await result if asyncio.iscoroutine(result) else result

            try:
                result = await resolve(
                    client.event_schemas.register(
                        RegisterSchemaRequest(
                            event_name="order.placed",
                            version=2,
                            schema_json='{"type":"object"}',
                        )
                    )
                )
                assert result.status == "created"
                for version in [0, 2]:
                    schema = await resolve(
                        client.event_schemas.get(
                            GetSchemaRequest(event_name="order.placed", version=version)
                        )
                    )
                    assert schema.version == 2
                    assert schema.schema_json == '{"type":"object"}'
                    assert schema.environment_id == "env_default"
                result = await resolve(
                    client.event_schemas.list(
                        ListSchemasRequest(event_name="order.placed", limit=1)
                    )
                )
                assert result.total_count == 1
                assert result.schemas[0].schema_json == '{"type":"object"}'
                await resolve(
                    client.event_schemas.delete(
                        DeleteSchemaRequest(event_name="order.placed", version=2)
                    )
                )
                with pytest.raises(IronflowRPCError) as exc:
                    await resolve(
                        client.event_schemas.get(
                            GetSchemaRequest(event_name="order.placed")
                        )
                    )
                assert exc.value.retryable is False
            finally:
                if isinstance(client, AsyncIronflowRPC):
                    await client.aclose()
                else:
                    client.close()

    asyncio.run(exercise())
