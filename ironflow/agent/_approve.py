"""``ctx.approve``: wait for a human-approval event named ``agent.approve.<name>``."""

from __future__ import annotations

from typing import Any

from ..worker import Duration, Step
from ._types import ApproveResult

APPROVE_EVENT_PREFIX = "agent.approve."


async def approve(step: Step, run_id: str, name: str, *, payload: Any = None, ttl: Duration = "7d") -> ApproveResult:
    ev = await step.wait_for_event(f"approve.{name}", event=APPROVE_EVENT_PREFIX + name, match="data.runId",
                                   match_value=run_id, payload=payload, timeout=ttl)
    data = ev.data if isinstance(ev.data, dict) else {}
    return ApproveResult(approved=bool(data.get("approved")), approver=data.get("approver"),
                         payload=data.get("payload"), reason=data.get("reason"))
