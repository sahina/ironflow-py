"""The ``@agent`` decorator: a ``Function`` whose handler receives an ``AgentContext``."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from ..worker import Context, Function, function
from ..worker._function import _code_hash
from ._context import AgentContext
from ._errors import DuplicateToolError, MemoryProjectionRequiredError
from ._memory import Memory, rpc_backend
from ._types import MemoryConfig, ToolDefinition

AgentHandler = Callable[[AgentContext], Awaitable[Any]]


def agent(
    *, id: str, tools: Sequence[ToolDefinition] = (), memory: MemoryConfig | None = None,
    max_turns: int = 20, **function_kwargs: Any,
) -> Callable[[AgentHandler], Function]:
    """Define an agent. Returns a normal Function; register it with a worker or ``serve()``.

    ``function_kwargs`` go to ``ironflow.worker.function`` (triggers, retry, ...).
    """
    registry: dict[str, ToolDefinition] = {}
    for t in tools:
        if t.name in registry:
            raise DuplicateToolError(t.name)
        registry[t.name] = t

    if memory is not None and not memory.projection:
        raise MemoryProjectionRequiredError(memory.stream_id)

    def decorate(handler: AgentHandler) -> Function:
        if not inspect.iscoroutinefunction(handler):
            raise TypeError(f"agent {id!r}: the handler must be an 'async def' function")

        async def run(ctx: Context) -> Any:
            mem = Memory(ctx.step, memory, ctx.run.id, memory.backend or rpc_backend(ctx.run.environment)) if memory else None
            return await handler(AgentContext(ctx, tools=registry, max_turns=max_turns, memory=mem))

        fn = function(id=id, **function_kwargs)(run)
        fn.code_hash = _code_hash(handler)  # the wrapper is shared; hash the user's body
        return fn

    return decorate
