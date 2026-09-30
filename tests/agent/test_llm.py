import asyncio
import logging
from datetime import datetime, timezone

from ironflow.agent import (
    LLMMaxTokensError,
    LLMRefusalError,
    MaxTurnsExceededError,
    agent,
)
from ironflow.testing import TestClient
from ironflow.worker import Context, Event, RunInfo, Step
from ironflow.worker._step import ExecutionContext


def emit(fn):
    return asyncio.run(TestClient([fn]).emit("go", {}))


def test_llm_turn_step_and_counter() -> None:
    @agent(id="a", triggers=[{"event": "go"}])
    async def a(ctx):
        r = await ctx.llm(call=lambda: {"content": "hi", "finish_reason": "stop"})
        return [r["content"], ctx.turn]

    r = emit(a)
    assert r.output == ["hi", 1]
    assert [s.name for s in r.steps] == ["llm.turn"]


def test_max_turns() -> None:
    @agent(id="a", triggers=[{"event": "go"}], max_turns=2)
    async def a(ctx):
        for _ in range(3):
            await ctx.llm(call=lambda: {"content": "x"})

    assert isinstance(emit(a).error, MaxTurnsExceededError)


def test_classification() -> None:
    for reason, err in [("refusal", LLMRefusalError), ("content_filter", LLMRefusalError),
                        ("safety", LLMRefusalError), ("LENGTH", LLMMaxTokensError),
                        ("max_tokens", LLMMaxTokensError)]:
        @agent(id="a", triggers=[{"event": "go"}])
        async def a(ctx, reason=reason):
            await ctx.llm(call=lambda: {"finish_reason": reason})
        assert isinstance(emit(a).error, err), reason


def test_classification_ignores_non_string_finish_reason() -> None:
    @agent(id="a", triggers=[{"event": "go"}])
    async def a(ctx):
        return await ctx.llm(call=lambda: {"finish_reason": 1})

    r = emit(a)
    assert r.error is None
    assert r.output == {"finish_reason": 1}


def test_llm_replay_skips_provider() -> None:
    provider_calls: list[int] = []

    @agent(id="a", triggers=[{"event": "go"}])
    async def a(ctx):
        return await ctx.llm(call=lambda: provider_calls.append(1) or {"content": "live"})

    ec = ExecutionContext("run-1", [{"step_id": "run-1:llm.turn:0", "status": "completed",
                                     "output": {"content": "cached"}}])
    ctx = Context(event=Event(id="e", name="go", data={}, timestamp=datetime.now(timezone.utc)),
                  step=Step(ec), run=RunInfo(id="run-1", function_id="a", attempt=2),
                  logger=logging.LoggerAdapter(logging.getLogger("t"), {}), secrets={})
    assert asyncio.run(a.handler(ctx)) == {"content": "cached"}
    assert provider_calls == []
