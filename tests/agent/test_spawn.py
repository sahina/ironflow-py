import asyncio

from ironflow.agent import SpawnResult, agent
from ironflow.testing import TestClient


def test_spawn_wait_invokes() -> None:
    @agent(id="a", triggers=[{"event": "go"}])
    async def a(ctx):
        return await ctx.spawn("child", function_id="child-fn", input={"n": 1})

    tc = TestClient([a])
    tc.mock_invoke("child-fn", lambda inp: inp["n"] + 1)
    assert asyncio.run(tc.emit("go", {})).output == SpawnResult(output=2)


def test_spawn_no_wait_returns_run_id() -> None:
    @agent(id="a", triggers=[{"event": "go"}])
    async def a(ctx):
        return await ctx.spawn("child", function_id="child-fn", wait=False)

    tc = TestClient([a])
    tc.mock_invoke("child-fn", lambda inp: None)
    r = asyncio.run(tc.emit("go", {}))
    assert r.status == "completed" and r.output.output is None
    assert r.output.run_id.startswith("test-run-")
