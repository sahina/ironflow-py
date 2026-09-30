"""``AgentContext``: the plain function context plus agent helpers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from ..worker import Context, Duration
from . import _approve, _spawn, _tool
from ._errors import MaxTurnsExceededError, ToolNotFoundError
from ._llm import classify
from ._types import ApproveResult, LLMCompleteResult, SpawnResult, ToolDefinition

LLMCall = Callable[[], LLMCompleteResult | Awaitable[LLMCompleteResult]]


class AgentContext:
    """The agent handler's ``ctx``: the plain function context plus agent helpers."""

    def __init__(
        self, inner: Context, *, tools: dict[str, ToolDefinition], max_turns: int, memory: Any = None,
    ) -> None:
        self.event = inner.event
        self.step = inner.step
        self.run = inner.run
        self.logger = inner.logger
        self.secrets = inner.secrets
        self.memory = memory
        self._tools = tools
        self._max_turns = max_turns
        self._turn = 0
        self._by_args: dict[str, Any] = {}

    @property
    def turn(self) -> int:
        """LLM turns used so far in this execution."""
        return self._turn

    async def tool(self, defn: ToolDefinition, args: Any) -> Any:
        return await _tool.run_tool(self.step, defn, args, self._by_args)

    async def tool_by_name(self, name: str, args: Any) -> Any:
        defn = self._tools.get(name)
        if defn is None:
            raise ToolNotFoundError(name)
        return await self.tool(defn, args)

    async def llm(self, *, call: LLMCall, messages: Any = None, tools: Any = None) -> LLMCompleteResult:
        """Run one LLM turn as step ``llm.turn``. ``messages``/``tools`` are for the caller's ``call`` only."""
        self._turn += 1
        if self._turn > self._max_turns:
            raise MaxTurnsExceededError(self._max_turns)
        result = await self.step.run("llm.turn", call)
        classify(result)
        return result  # type: ignore[no-any-return]

    async def approve(self, name: str, *, payload: Any = None, ttl: Duration = "7d") -> ApproveResult:
        return await _approve.approve(self.step, self.run.id, name, payload=payload, ttl=ttl)

    async def spawn(self, name: str, *, function_id: str, input: Any = None, wait: bool = True,
                    timeout: Duration = 30) -> SpawnResult:
        return await _spawn.spawn(self.step, name, function_id=function_id, input=input, wait=wait,
                                  timeout=timeout)
