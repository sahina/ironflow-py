import asyncio
import json
from types import SimpleNamespace

import pytest

from ironflow.agent import (
    AgentError,
    DuplicateToolError,
    _registry,
    define_tool,
    expose_mcp,
)

echo = define_tool(name="echo", handler=lambda i: i, input_schema={"type": "object"}, description="d",
                   scopes=("read",))


class FakeTools:
    def __init__(self, fail_unregister: bool = False) -> None:
        self.requests: list = []
        self.unregistered: list[str] = []
        self.fail = fail_unregister

    async def register(self, req):
        self.requests.append(req)
        return SimpleNamespace(hmac_secret="ab" * 32, registered_tool_names=[f"{req.agent_name}.echo"])

    async def unregister(self, req):
        if self.fail:
            raise RuntimeError("down")
        self.unregistered.append(req.agent_name)


def fake(**kw):
    return SimpleNamespace(agent_tools=FakeTools(**kw))


@pytest.fixture(autouse=True)
def clean():
    _registry.clear_local()
    yield
    _registry.clear_local()


def test_register_and_unregister() -> None:
    rpc = fake()
    h = asyncio.run(expose_mcp(name="demo", callback_url="http://app/x", tools=[echo], rpc=rpc))
    assert h.tool_names == ("demo.echo",) and h.status == "active" and h.tool_count == 1
    req = rpc.agent_tools.requests[0]
    assert req.agent_name == "demo" and req.callback_url == "http://app/x"
    assert json.loads(req.tools[0].input_schema_json) == {"type": "object"}
    assert list(req.tools[0].required_scopes) == ["read"]
    assert _registry.lookup_local("demo.echo").hmac_secret == "ab" * 32
    asyncio.run(h.unregister())
    asyncio.run(h.unregister())  # idempotent
    assert rpc.agent_tools.unregistered == ["demo"]
    assert _registry.lookup_local("demo.echo") is None


@pytest.mark.parametrize("kwargs,code", [
    ({"tools": []}, "AGENT_MCP_NO_TOOLS"),
    ({"callback_url": ""}, "AGENT_MCP_MISSING_CALLBACK_URL"),
])
def test_validation(kwargs, code) -> None:
    args = {"name": "demo", "callback_url": "http://app/x", "tools": [echo], "rpc": fake(), **kwargs}
    with pytest.raises(AgentError) as e:
        asyncio.run(expose_mcp(**args))
    assert e.value.code == code


def test_missing_server_url(monkeypatch) -> None:
    for k in ("IRONFLOW_URL", "IRONFLOW_SERVER_URL"):
        monkeypatch.delenv(k, raising=False)
    with pytest.raises(AgentError) as e:
        asyncio.run(expose_mcp(name="d", callback_url="http://x", tools=[echo]))
    assert e.value.code == "AGENT_MCP_MISSING_SERVER_URL"


def test_missing_api_key(monkeypatch) -> None:
    monkeypatch.setenv("IRONFLOW_URL", "http://engine")
    monkeypatch.delenv("IRONFLOW_API_KEY", raising=False)
    with pytest.raises(AgentError) as e:
        asyncio.run(expose_mcp(name="d", callback_url="http://x", tools=[echo]))
    assert e.value.code == "AGENT_MCP_MISSING_API_KEY"


def test_duplicate() -> None:
    with pytest.raises(DuplicateToolError):
        asyncio.run(expose_mcp(name="d", callback_url="http://x", tools=[echo, echo], rpc=fake()))


def test_invalid_response() -> None:
    rpc = fake()

    async def empty(req):
        return SimpleNamespace(hmac_secret="", registered_tool_names=[])
    rpc.agent_tools.register = empty
    with pytest.raises(AgentError) as e:
        asyncio.run(expose_mcp(name="d", callback_url="http://x", tools=[echo], rpc=rpc))
    assert e.value.code == "AGENT_MCP_INVALID_RESPONSE"


def test_unregister_failure() -> None:
    h = asyncio.run(expose_mcp(name="d", callback_url="http://x", tools=[echo], rpc=fake(fail_unregister=True)))
    with pytest.raises(AgentError) as e:
        asyncio.run(h.unregister())
    assert e.value.code == "AGENT_MCP_UNREGISTER_FAILED"


@pytest.mark.parametrize("configured,env_var,want", [
    ("staging", "qa", "staging"),
    (None, "qa", "qa"),
    (None, None, None),
])
def test_environment_reaches_the_built_client(monkeypatch, configured, env_var, want) -> None:
    """#2471: register and unregister share one client, so one assertion covers both."""
    import ironflow.rpc as rpc_mod

    built: dict = {}
    client = fake()

    async def aclose() -> None:
        return None

    client.aclose = aclose

    def make(**kw):
        built.update(kw)
        return client

    monkeypatch.setattr(rpc_mod, "AsyncIronflowRPC", make)
    monkeypatch.setenv("IRONFLOW_URL", "http://engine")
    monkeypatch.setenv("IRONFLOW_API_KEY", "ifkey_x")
    if env_var is None:
        monkeypatch.delenv("IRONFLOW_ENV", raising=False)
    else:
        monkeypatch.setenv("IRONFLOW_ENV", env_var)

    h = asyncio.run(expose_mcp(name="demo", callback_url="http://app/x", tools=[echo], environment=configured))
    asyncio.run(h.unregister())
    assert built["environment"] == want
    assert client.agent_tools.unregistered == ["demo"]
