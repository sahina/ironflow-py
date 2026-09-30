import pytest

from ironflow.agent import DuplicateToolError, agent, define_tool
from ironflow.worker import Function

t = define_tool(name="t", handler=lambda i: i)


def test_agent_returns_function() -> None:
    @agent(id="x", triggers=[{"event": "go"}], tools=[t])
    async def x(ctx):
        return None
    assert isinstance(x, Function) and x.id == "x"


def test_duplicate_tool_rejected_at_definition() -> None:
    with pytest.raises(DuplicateToolError):
        agent(id="x", tools=[t, t])


def test_handler_must_be_async() -> None:
    with pytest.raises(TypeError):
        agent(id="x")(lambda ctx: None)


def test_code_hash_follows_agent_body() -> None:
    @agent(id="x")
    async def one(ctx):
        return 1

    @agent(id="y")
    async def two(ctx):
        return 2

    assert one.code_hash != two.code_hash
