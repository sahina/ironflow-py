"""A duck-typed stand-in for the generated ProjectionServiceClient."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from protobuf.wkt import Duration

from ironflow._gen.projection_pb import (
    AckProjectionEventsResponse,
    AcquireProjectionLeaseResponse,
    GetProjectionResponse,
    PollProjectionEventsResponse,
    ProjectionLease,
    RegisterProjectionResponse,
    ReleaseProjectionLeaseResponse,
    RenewProjectionLeaseResponse,
    SaveProjectionStateResponse,
)


def granted(epoch: int = 1, ttl: float = 3600.0) -> AcquireProjectionLeaseResponse:
    """A grant with a TTL so long that no Renew runs in a test that does not ask for one."""
    return AcquireProjectionLeaseResponse(
        granted=True,
        lease=ProjectionLease(token=f"sdk:t{epoch}", epoch=epoch),
        ttl=Duration(seconds=int(ttl), nanos=int(ttl % 1 * 1e9)),
    )


DEFAULTS: dict[str, Any] = {
    "register_projection": RegisterProjectionResponse,
    "get_projection": GetProjectionResponse,
    "poll_projection_events": PollProjectionEventsResponse,
    "save_projection_state": SaveProjectionStateResponse,
    "ack_projection_events": AckProjectionEventsResponse,
    "acquire_projection_lease": granted,
    "renew_projection_lease": RenewProjectionLeaseResponse,
    "release_projection_lease": ReleaseProjectionLeaseResponse,
}


class FakeProjectionService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.kwargs: list[tuple[str, dict[str, Any]]] = []  # per-call keyword arguments (headers, timeout_ms)
        self.script: dict[str, list[Any]] = {}
        self.streams: list[list[Any]] = []  # each entry: frames, exceptions or float pauses for one stream call

    def _next(self, method: str, request: Any = None) -> Any:
        self.calls.append((method, request))
        queue = self.script.get(method) or []
        item = queue.pop(0) if queue else DEFAULTS[method]()
        if isinstance(item, BaseException):
            raise item
        return item

    def __getattr__(self, method: str) -> Any:
        if method not in DEFAULTS:
            raise AttributeError(method)

        async def call(request: Any, **kw: Any) -> Any:
            self.kwargs.append((method, kw))
            return self._next(method, request)

        return call

    async def stream_projection_events(self, request: Any, **kw: Any) -> AsyncIterator[Any]:
        self.kwargs.append(("stream_projection_events", kw))
        self.calls.append(("stream_projection_events", request))
        frames = self.streams.pop(0) if self.streams else []
        if isinstance(frames, BaseException):
            raise frames
        for f in frames:
            if isinstance(f, BaseException):
                raise f
            if isinstance(f, float):  # a pause between frames, in seconds
                await asyncio.sleep(f)
                continue
            yield f
        await asyncio.sleep(3600)  # an idle open stream; the test cancels the runner

    def requests(self, method: str) -> list[Any]:
        return [r for m, r in self.calls if m == method]
