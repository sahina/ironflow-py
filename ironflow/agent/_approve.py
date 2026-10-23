"""``ctx.approve``: wait for a human-approval event whose name the caller chooses."""

from __future__ import annotations

from typing import Any

from ..worker import Duration, Step
from ._types import ApproveResult


async def approve(step: Step, run_id: str, event: str, *, payload: Any = None, ttl: Duration = "7d") -> ApproveResult:
    """Sugar over ``wait_for_event``: the engine has no approval concept.

    The approver emits ``event`` with ``{"runId": ..., "approved": ...}`` through any client.
    The SDK escapes the name in the step ID, so any event name is a valid step name.
    """
    if not isinstance(event, str) or not event.strip():
        raise ValueError("approve(): event must be a non-empty string")
    ev = await step.wait_for_event(f"approve.{event}", event=event, match="data.runId",
                                   match_value=run_id, payload=payload, timeout=ttl)
    data = ev.data if isinstance(ev.data, dict) else {}
    return ApproveResult(approved=bool(data.get("approved")), approver=data.get("approver"),
                         payload=data.get("payload"), reason=data.get("reason"), emitted_by=ev.emitted_by)
