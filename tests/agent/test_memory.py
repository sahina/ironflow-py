import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from ironflow.agent import (
    AgentError,
    MemoryConfig,
    MemoryProjectionRequiredError,
    agent,
)
from ironflow.agent._memory import _RPCBackend
from ironflow.testing import TestClient


class FakeBackend:
    def __init__(self) -> None:
        self.appended: list[tuple[str, dict[str, Any]]] = []
        self.metadata: list[Any] = []
        self.waited: list[str] = []
        self.state: Any = {"notes": 0}

    async def append_event(self, stream_id, *, name, data, entity_type, idempotency_key, metadata=None):
        self.appended.append((idempotency_key, {"stream": stream_id, "name": name, "type": entity_type, **data}))
        self.metadata.append(metadata)
        return f"evt-{len(self.appended)}"

    async def get_projection(self, name):
        return self.state

    async def wait_for_event(self, event_id, projection, timeout_s):
        self.waited.append(event_id)


def make(backend, body):
    @agent(id="a", triggers=[{"event": "go"}], memory=MemoryConfig("s1", "notes", backend=backend))
    async def a(ctx):
        return await body(ctx)
    return asyncio.run(TestClient([a]).emit("go", {}))


def test_append_then_get() -> None:
    b = FakeBackend()

    async def body(ctx):
        await ctx.memory.append("note.added", {"text": "hi"})
        b.state = {"notes": 1}
        return await ctx.memory.get()

    r = make(b, body)
    assert r.output == {"notes": 1}
    assert [s.name for s in r.steps] == ["memory.append", "memory.append.wait", "memory.get"]
    key, row = b.appended[0]
    assert key.endswith(":memory.append:0") and row["type"] == "agent" and row["stream"] == "s1"
    assert b.waited == ["evt-1"]


def test_append_passes_metadata() -> None:
    b = FakeBackend()

    async def body(ctx):
        await ctx.memory.append("note.added", {"text": "hi"}, metadata={"trace": "abc"})

    make(b, body)
    assert b.metadata == [{"trace": "abc"}]


def test_get_is_cached_until_append() -> None:
    b = FakeBackend()

    async def body(ctx):
        await ctx.memory.get()
        await ctx.memory.get()
        await ctx.memory.get(bypass_cache=True)
    assert [s.name for s in make(b, body).steps] == ["memory.get", "memory.get"]


@pytest.mark.parametrize("bad", [[1], "x", None])
def test_append_rejects_non_dict(bad) -> None:
    b = FakeBackend()

    async def body(ctx):
        await ctx.memory.append("e", bad)
    r = make(b, body)
    assert isinstance(r.error, AgentError) and r.error.code == "AGENT_MEMORY_INVALID_DATA"
    assert b.appended == []


def test_projection_required() -> None:
    with pytest.raises(MemoryProjectionRequiredError):
        agent(id="a", memory=MemoryConfig("s1", ""))


def test_no_memory_configured() -> None:
    @agent(id="a", triggers=[{"event": "go"}])
    async def a(ctx):
        return ctx.memory
    assert asyncio.run(TestClient([a]).emit("go", {})).output is None


def test_no_backend(monkeypatch) -> None:
    monkeypatch.delenv("IRONFLOW_URL", raising=False)
    monkeypatch.delenv("IRONFLOW_SERVER_URL", raising=False)

    async def body(ctx):
        await ctx.memory.get()
    r = make(None, body)
    assert r.error.code == "AGENT_MEMORY_NO_BACKEND"


class FakeRPC:
    def __init__(self) -> None:
        self.reqs: list[Any] = []

        async def append_event(req):
            self.reqs.append(req)
            return SimpleNamespace(event_id="e1")

        async def get(req):
            self.reqs.append(req)
            from protobuf.wkt import Struct
            return SimpleNamespace(state=Struct.from_python({"n": 2.0}), has_field=lambda f: f == "state")

        async def wait_for_event(req):
            self.reqs.append(req)

        self.streams = SimpleNamespace(append_event=append_event)
        self.projections = SimpleNamespace(get=get, wait_for_event=wait_for_event)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None


def test_rpc_backend_builds_requests(monkeypatch) -> None:
    fake = FakeRPC()
    monkeypatch.setattr(_RPCBackend, "_rpc", lambda self: fake)
    b = _RPCBackend("http://engine", "k")
    assert asyncio.run(b.append_event("s1", name="n", data={"a": 1}, entity_type="agent",
                                      idempotency_key="r:memory.append:0")) == "e1"
    assert asyncio.run(b.get_projection("notes")) == {"n": 2}
    asyncio.run(b.wait_for_event("e1", "notes", 5))
    append, get, wait = fake.reqs
    assert (append.entity_id, append.event_name, append.idempotency_key) == ("s1", "n", "r:memory.append:0")
    assert not append.has_field("metadata")
    assert get.name == "notes"
    assert (wait.event_id, wait.projection, wait.timeout.seconds) == ("e1", "notes", 5)


def test_rpc_backend_forwards_metadata(monkeypatch) -> None:
    fake = FakeRPC()
    monkeypatch.setattr(_RPCBackend, "_rpc", lambda self: fake)
    b = _RPCBackend("http://engine", "k")
    asyncio.run(b.append_event("s1", name="n", data={"a": 1}, entity_type="agent",
                               idempotency_key="k", metadata={"trace": "abc"}))
    (append,) = fake.reqs
    assert append.metadata.to_python() == {"trace": "abc"}
