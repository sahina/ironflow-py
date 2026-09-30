"""``ctx.spawn``: invoke a child function, waiting for its output or firing it and moving on."""

from __future__ import annotations

from typing import Any

from ..worker import Duration, Step
from ._types import SpawnResult


async def spawn(step: Step, name: str, *, function_id: str, input: Any = None, wait: bool = True,
                timeout: Duration = 30) -> SpawnResult:
    # ``name`` labels the child for the caller, as in Node. invoke/invoke_async are durable steps already.
    if wait:
        return SpawnResult(output=await step.invoke(function_id, input, timeout=timeout))
    return SpawnResult(run_id=(await step.invoke_async(function_id, input)).run_id)
