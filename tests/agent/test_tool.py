import asyncio
import sys

from ironflow.agent import ToolNotFoundError, ToolValidationError, agent, define_tool
from ironflow.testing import TestClient

calls: list[object] = []
search = define_tool(name="search", handler=lambda i: {"hits": i["q"]})
dedupe = define_tool(name="dedupe", handler=lambda i: calls.append(i) or "ok", idempotent="by_args")


def emit(fn):
    return asyncio.run(TestClient([fn]).emit("go", {}))


def test_tool_runs_as_named_step() -> None:
    @agent(id="a", triggers=[{"event": "go"}], tools=[search])
    async def a(ctx):
        return await ctx.tool(search, {"q": "x"})

    r = emit(a)
    assert r.status == "completed" and r.output == {"hits": "x"}
    assert [s.name for s in r.steps] == ["tool.search"]


def test_async_handler_supported() -> None:
    async def h(i):
        return i["n"] * 2
    t = define_tool(name="dbl", handler=h)

    @agent(id="a", triggers=[{"event": "go"}], tools=[t])
    async def a(ctx):
        return await ctx.tool(t, {"n": 21})

    assert emit(a).output == 42


def test_by_args_key_order_stable() -> None:
    calls.clear()

    @agent(id="a", triggers=[{"event": "go"}], tools=[dedupe])
    async def a(ctx):
        await ctx.tool(dedupe, {"a": 1, "b": 2})
        await ctx.tool(dedupe, {"b": 2, "a": 1})

    r = emit(a)
    assert len(calls) == 1
    assert len(r.steps) == 1 and r.steps[0].name.startswith("tool.dedupe.")
    assert len(r.steps[0].name.rsplit(".", 1)[1]) == 16


def test_tool_by_name_and_not_found() -> None:
    @agent(id="a", triggers=[{"event": "go"}], tools=[search])
    async def a(ctx):
        assert await ctx.tool_by_name("search", {"q": "y"}) == {"hits": "y"}
        await ctx.tool_by_name("nope", {})

    r = emit(a)
    assert r.status == "failed" and isinstance(r.error, ToolNotFoundError)


def test_by_args_unserialisable_args() -> None:
    @agent(id="a", triggers=[{"event": "go"}], tools=[dedupe])
    async def a(ctx):
        await ctx.tool(dedupe, {"x": object()})

    assert isinstance(emit(a).error, ToolValidationError)


typed = define_tool(
    name="typed", handler=lambda i: calls.append(i) or i["n"],
    input_schema={"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]},
)


def test_invalid_args_rejected_before_handler() -> None:
    calls.clear()

    @agent(id="a", triggers=[{"event": "go"}], tools=[typed])
    async def a(ctx):
        await ctx.tool(typed, {"n": "x"})

    r = emit(a)
    assert isinstance(r.error, ToolValidationError) and "n: 'x' is not of type" in str(r.error)
    assert calls == [] and r.steps == []


def test_valid_args_pass_schema() -> None:
    @agent(id="a", triggers=[{"event": "go"}], tools=[typed])
    async def a(ctx):
        return await ctx.tool(typed, {"n": 3})

    assert emit(a).output == 3


def test_bad_schema_is_validation_error() -> None:
    bad = define_tool(name="bad", handler=lambda i: i, input_schema={"type": "nope"})

    @agent(id="a", triggers=[{"event": "go"}], tools=[bad])
    async def a(ctx):
        await ctx.tool(bad, {})

    assert "input_schema is invalid" in str(emit(a).error)


def test_validation_skipped_without_jsonschema(monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "jsonschema", None)

    @agent(id="a", triggers=[{"event": "go"}], tools=[typed])
    async def a(ctx):
        return await ctx.tool(typed, {"n": "x"})

    assert emit(a).output == "x"
