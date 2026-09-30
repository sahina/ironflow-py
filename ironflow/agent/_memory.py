"""``ctx.memory``: an agent's entity-stream memory, read through a projection."""

from __future__ import annotations

import os
from typing import Any, Protocol

from ..worker import Step
from ._errors import AgentError
from ._types import MemoryConfig

WAIT_TIMEOUT_S = 5


class MemoryBackend(Protocol):
    async def append_event(self, stream_id: str, *, name: str, data: dict[str, Any], entity_type: str,
                           idempotency_key: str, metadata: dict[str, Any] | None = None) -> str: ...
    async def get_projection(self, name: str) -> Any: ...
    async def wait_for_event(self, event_id: str, projection: str, timeout_s: int) -> None: ...


class Memory:
    """``ctx.memory``: an agent's entity-stream memory, read through a projection."""

    def __init__(self, step: Step, config: MemoryConfig, run_id: str, backend: MemoryBackend | None) -> None:
        self._step, self._config, self._run_id, self._backend = step, config, run_id, backend
        self._cached = False
        self._value: Any = None
        self._appends = 0

    def _require_backend(self) -> MemoryBackend:
        if self._backend is None:
            raise AgentError("agent memory needs a backend: set IRONFLOW_URL (or IRONFLOW_SERVER_URL) and "
                             "IRONFLOW_API_KEY, or pass MemoryConfig(backend=...)", "AGENT_MEMORY_NO_BACKEND")
        return self._backend

    async def get(self, *, bypass_cache: bool = False) -> Any:
        if self._cached and not bypass_cache:
            return self._value
        backend = self._require_backend()
        self._value = await self._step.run("memory.get", lambda: backend.get_projection(self._config.projection))
        self._cached = True
        return self._value

    async def append(self, event_name: str, data: dict[str, Any], *, metadata: dict[str, Any] | None = None) -> None:
        if not isinstance(data, dict):
            raise AgentError("memory.append() requires data to be a dict; wrap lists and scalars so the "
                             "projection reducer sees a stable shape", "AGENT_MEMORY_INVALID_DATA",
                             {"received_type": type(data).__name__})
        backend = self._require_backend()
        cfg = self._config
        key = f"{self._run_id}:memory.append:{self._appends}"
        self._appends += 1
        event_id = await self._step.run("memory.append", lambda: backend.append_event(
            cfg.stream_id, name=event_name, data=data, entity_type=cfg.entity_type, idempotency_key=key,
            metadata=metadata))
        if event_id:
            await self._step.run("memory.append.wait",
                                 lambda: backend.wait_for_event(event_id, cfg.projection, WAIT_TIMEOUT_S))
        self._cached, self._value = False, None


class _RPCBackend:
    """Default backend over ConnectRPC. The REST client has none of these three calls."""

    def __init__(self, url: str, api_key: str | None) -> None:
        self._url, self._api_key = url, api_key

    def _rpc(self) -> Any:
        # ponytail: one client per call, so append/get/wait each pay a connection setup
        # (two per append). Cache one client per backend instance if that latency matters.
        from ..rpc import AsyncIronflowRPC  # lazy: keep `import ironflow.agent` cheap
        return AsyncIronflowRPC(server_url=self._url, api_key=self._api_key)

    async def append_event(self, stream_id: str, *, name: str, data: dict[str, Any], entity_type: str,
                           idempotency_key: str, metadata: dict[str, Any] | None = None) -> str:
        from protobuf.wkt import Struct

        from ..rpc.v1 import AppendEventRequest
        async with self._rpc() as rpc:
            resp = await rpc.streams.append_event(AppendEventRequest(
                entity_id=stream_id, entity_type=entity_type, event_name=name, data=Struct.from_python(data),
                idempotency_key=idempotency_key,
                metadata=Struct.from_python(metadata) if metadata is not None else None))
        return str(resp.event_id)

    async def get_projection(self, name: str) -> Any:
        from ..projection._runner import _to_python
        from ..rpc.v1 import GetProjectionRequest
        async with self._rpc() as rpc:
            resp = await rpc.projections.get(GetProjectionRequest(name=name))
        return _to_python(resp, "state", "state_value")

    async def wait_for_event(self, event_id: str, projection: str, timeout_s: int) -> None:
        from protobuf.wkt import Duration

        from ..rpc.v1 import WaitForEventRequest
        async with self._rpc() as rpc:
            await rpc.projections.wait_for_event(WaitForEventRequest(
                event_id=event_id, projection=projection, timeout=Duration(seconds=timeout_s)))


def rpc_backend() -> MemoryBackend | None:
    url = os.environ.get("IRONFLOW_URL") or os.environ.get("IRONFLOW_SERVER_URL")
    return _RPCBackend(url, os.environ.get("IRONFLOW_API_KEY")) if url else None
