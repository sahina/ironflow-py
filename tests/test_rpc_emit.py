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


@pytest.mark.parametrize("client_cls", [IronflowRPC, AsyncIronflowRPC])
def test_events_redact(client_cls: Any) -> None:
    """Redaction keeps the event row and replaces only its data, so the call
    answers Empty rather than a mutated event."""

    async def exercise() -> None:
        with serve() as server:
            client = client_cls(server_url=server.url)
            try:
                result = client.events.redact(v1.RedactEventRequest(event_id="ev"))
                if asyncio.iscoroutine(result):
                    await result
            finally:
                if isinstance(client, AsyncIronflowRPC):
                    await client.aclose()
                else:
                    client.close()

    asyncio.run(exercise())


def test_is_redacted() -> None:
    """The predicate spec §7 names, so a reducer can tell a placeholder from
    real state without knowing the placeholder's shape."""
    from ironflow import is_redacted

    assert is_redacted({"$redacted": True, "sha256": "ab", "redactedAt": "x"})
    assert not is_redacted({"email": "a@b.c"})
    assert not is_redacted({"$redacted": False})
    assert not is_redacted({"$redacted": "yes"})
    assert not is_redacted(None)
    assert not is_redacted([1, 2])
    assert not is_redacted("$redacted")
