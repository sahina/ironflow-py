import asyncio
from types import SimpleNamespace
from typing import Any

from ironflow.agent import AgentContext, ApproveResult, agent
from ironflow.testing import TestClient


def test_approve_waits_on_named_event() -> None:
    @agent(id="a", triggers=[{"event": "go"}])
    async def a(ctx):
        return await ctx.approve("ship", payload={"diff": 1}, ttl="1h")

    tc = TestClient([a])
    tc.send_event("agent.approve.ship", {"runId": "x", "approved": True, "approver": "sam", "reason": "ok"})
    r = asyncio.run(tc.emit("go", {}))
    assert r.output == ApproveResult(approved=True, approver="sam", payload=None, reason="ok")
    assert [s.name for s in r.steps] == ["approve.ship"]


def test_approve_missing_fields_default_false() -> None:
    @agent(id="a", triggers=[{"event": "go"}])
    async def a(ctx):
        return await ctx.approve("ship")

    tc = TestClient([a])
    tc.send_event("agent.approve.ship", {})
    assert asyncio.run(tc.emit("go", {})).output.approved is False


def test_approve_filters_on_run_id() -> None:
    seen: dict[str, Any] = {}

    class Stub:
        async def wait_for_event(self, name, **kw):
            seen.update(kw, name=name)
            return SimpleNamespace(data={"approved": True})

    inner = SimpleNamespace(event=None, step=Stub(), run=SimpleNamespace(id="run-9"), logger=None, secrets={})
    ctx = AgentContext(inner, tools={}, max_turns=1)  # type: ignore[arg-type]
    asyncio.run(ctx.approve("ship", ttl="2h"))
    assert seen == {"name": "approve.ship", "event": "agent.approve.ship", "match": "data.runId",
                    "match_value": "run-9", "payload": None, "timeout": "2h"}
