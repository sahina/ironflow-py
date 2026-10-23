import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from ironflow.agent import AgentContext, ApproveResult, agent
from ironflow.testing import TestClient


def test_approve_waits_on_named_event() -> None:
    @agent(id="a", triggers=[{"event": "go"}])
    async def a(ctx):
        return await ctx.approve("ship.approved", payload={"diff": 1}, ttl="1h")

    tc = TestClient([a])
    tc.send_event("ship.approved", {"runId": "x", "approved": True, "approver": "sam", "reason": "ok"})
    r = asyncio.run(tc.emit("go", {}))
    assert r.output == ApproveResult(approved=True, approver="sam", payload=None, reason="ok")
    assert [s.name for s in r.steps] == ["approve.ship.approved"]


def test_approve_missing_fields_default_false() -> None:
    @agent(id="a", triggers=[{"event": "go"}])
    async def a(ctx):
        return await ctx.approve("ship.approved")

    tc = TestClient([a])
    tc.send_event("ship.approved", {})
    assert asyncio.run(tc.emit("go", {})).output.approved is False


def test_approve_filters_on_run_id() -> None:
    seen: dict[str, Any] = {}

    class Stub:
        async def wait_for_event(self, name, **kw):
            seen.update(kw, name=name)
            return SimpleNamespace(data={"approved": True}, emitted_by="")

    inner = SimpleNamespace(event=None, step=Stub(), run=SimpleNamespace(id="run-9"), logger=None, secrets={})
    ctx = AgentContext(inner, tools={}, max_turns=1)  # type: ignore[arg-type]
    asyncio.run(ctx.approve("ship.approved", ttl="2h"))
    assert seen == {"name": "approve.ship.approved", "event": "ship.approved", "match": "data.runId",
                    "match_value": "run-9", "payload": None, "timeout": "2h"}


def test_approve_returns_engine_set_emitted_by_beside_approver() -> None:
    class Stub:
        async def wait_for_event(self, name, **kw):
            return SimpleNamespace(data={"approved": True, "approver": "alice"}, emitted_by="apikey:k1")

    inner = SimpleNamespace(event=None, step=Stub(), run=SimpleNamespace(id="run-9"), logger=None, secrets={})
    ctx = AgentContext(inner, tools={}, max_turns=1)  # type: ignore[arg-type]
    r = asyncio.run(ctx.approve("ship.approved"))
    assert (r.emitted_by, r.approver) == ("apikey:k1", "alice")


@pytest.mark.parametrize("event", ["", "  "])
def test_approve_rejects_empty_event_name(event: str) -> None:
    inner = SimpleNamespace(event=None, step=SimpleNamespace(), run=SimpleNamespace(id="run-9"), logger=None, secrets={})
    ctx = AgentContext(inner, tools={}, max_turns=1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="non-empty"):
        asyncio.run(ctx.approve(event))
