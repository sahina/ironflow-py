from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import pytest

from ironflow import UpcasterRegistry
from ironflow.serve._handler import handle, index_functions
from ironflow.serve._signature import sign
from ironflow.worker import NonRetryableError, function
from tests.worker.fake_engine import FakeEngine

EVENT = {"id": "ev", "name": "e", "data": {"x": 1}, "version": 1, "timestamp": "2026-09-27T10:00:00Z"}


def req(**over: Any) -> dict[str, Any]:
    return {"run_id": "run_1", "function_id": "fn", "attempt": 1, "event": EVENT, **over}


def fn_of(handler: Any) -> Any:
    return function(id="fn", triggers=[{"event": "e"}])(handler)


def call(fns: list[Any], body: Any, *, method: str = "POST", headers: dict[str, str] | None = None,
         raw: bytes | None = None, **kw: Any) -> tuple[int, dict[str, Any]]:
    kw.setdefault("signing_key", "")
    data = raw if raw is not None else json.dumps(body).encode()
    status, _, out = asyncio.run(handle(fns, method=method, path="/", headers=headers or {}, body=data, **kw))
    return status, json.loads(out)


async def ok(ctx: Any) -> Any:
    a = await ctx.step.run("a", lambda: 1)
    return {"a": a, "k": ctx.secrets.get("K")}


def test_completed_uses_result_and_lists_executed_steps() -> None:
    status, body = call([fn_of(ok)], req(secrets={"K": "v"}))
    assert status == 200
    assert body["status"] == "completed" and body["result"] == {"a": 1, "k": "v"}
    assert "output" not in body
    assert [s["id"] for s in body["steps"]] == ["run_1:a:0"]


def test_memoized_step_not_rerun_and_not_listed() -> None:
    calls: list[int] = []

    async def h(ctx: Any) -> Any:
        return await ctx.step.run("a", lambda: calls.append(1) or 2)
    _status, body = call([fn_of(h)], req(steps=[
        {"id": "run_1:a:0", "name": "a", "status": "completed", "output": 5}]))
    assert body == {"status": "completed", "result": 5, "steps": []} and calls == []


def test_completed_wait_step_returns_event_and_resume_is_ignored() -> None:
    async def h(ctx: Any) -> Any:
        ev = await ctx.step.wait_for_event("w", event="order.paid", timeout="1h")
        return ev.data
    payload = {"id": "ev2", "name": "order.paid", "data": {"ok": True}, "timestamp": "2026-09-27T10:01:00Z"}
    status, body = call([fn_of(h)], req(
        steps=[{"id": "run_1:w:0", "name": "w", "status": "completed", "output": payload}],
        resume={"step_id": "run_1:w:0", "type": "wait_event", "data": {"ignored": True}}))
    assert status == 200 and body["result"] == {"ok": True}


def test_memoized_step_error_reaches_invoke_error() -> None:
    # Defensive: no production caller sends a memoized step with "error" today (see
    # _memo's comment in _handler.py); this proves the remap still behaves if one ever does.
    async def h(ctx: Any) -> Any:
        return await ctx.step.invoke("child", {"a": 1})
    status, body = call([fn_of(h)], req(steps=[
        {"id": "run_1:child:0", "name": "child", "status": "completed",
         "error": "child exploded"}]))
    assert status == 200 and body["status"] == "failed"
    assert "child exploded" in body["error"]["message"]


@pytest.mark.parametrize(("kind", "wire"), [
    ("sleep", "sleep"), ("wait_for_event", "wait_for_event"), ("invoke", "invoke_function")])
def test_yields(kind: str, wire: str) -> None:
    async def h(ctx: Any) -> None:
        if kind == "sleep":
            await ctx.step.sleep("s", "1h")
        elif kind == "wait_for_event":
            await ctx.step.wait_for_event("w", event="x", timeout="1h")
        else:
            await ctx.step.invoke("other", {"a": 1})
    status, body = call([fn_of(h)], req())
    assert status == 200 and body["status"] == "yielded" and body["yield"]["type"] == wire


def test_failed_retryable_and_non_retryable() -> None:
    async def boom(ctx: Any) -> None:
        raise RuntimeError("x")

    async def fatal(ctx: Any) -> None:
        raise NonRetryableError("no")
    assert call([fn_of(boom)], req())[1]["error"] == {"message": "x", "code": "ERROR", "retryable": True}
    status, body = call([fn_of(fatal)], req())
    assert status == 200 and body["status"] == "failed" and body["error"]["retryable"] is False


def test_bad_event_timestamp_is_200_failed() -> None:
    status, body = call([fn_of(ok)], req(event={"id": "ev", "name": "e", "data": {}}))
    assert status == 200 and body["status"] == "failed"


def test_unencodable_yield_is_200_serialization_error_with_steps() -> None:
    async def h(ctx: Any) -> Any:
        await ctx.step.run("a", lambda: 1)
        return await ctx.step.invoke("other", {1, 2})  # a set: nothing validates yield input
    status, body = call([fn_of(h)], req())
    assert status == 200 and body["status"] == "failed"
    assert body["error"]["code"] == "SERIALIZATION_ERROR" and body["error"]["retryable"] is False
    assert [s["id"] for s in body["steps"]] == ["run_1:a:0"]


def test_method_not_allowed() -> None:
    assert call([fn_of(ok)], req(), method="GET")[0] == 405


@pytest.mark.parametrize("raw", [b"not json", b"{"])
def test_invalid_json(raw: bytes) -> None:
    status, body = call([fn_of(ok)], None, raw=raw)
    assert status == 400 and body["error"]["code"] == "INVALID_JSON"


def test_deeply_nested_json_is_invalid_json_not_500() -> None:
    raw = b"[" * 200000
    status, body = call([fn_of(ok)], None, raw=raw)
    assert status == 400 and body["error"]["code"] == "INVALID_JSON"


@pytest.mark.parametrize("payload", [
    [], "x", None,
    {"function_id": "fn", "event": EVENT},
    {"run_id": "r", "event": EVENT},
    {"run_id": "r", "function_id": "fn"},
    req(attempt="x"), req(attempt=None), req(attempt=True),
    req(steps="x"), req(steps=[1]), req(steps=[{"name": "a"}]), req(steps=[{"id": 3}]),
    req(secrets=["a"]),
])
def test_validation_error(payload: Any) -> None:
    status, body = call([fn_of(ok)], payload)
    assert status == 400 and body["error"]["code"] == "VALIDATION_ERROR"


def test_function_not_found() -> None:
    status, body = call([fn_of(ok)], req(function_id="nope"))
    assert status == 404 and body["error"]["code"] == "FUNCTION_NOT_FOUND"


def test_signature_required_when_key_set() -> None:
    raw = json.dumps(req()).encode()
    now = int(time.time())
    # Mixed-case header name: handle() lower-cases header names itself.
    assert call([fn_of(ok)], None, raw=raw, signing_key="k",
                headers={"X-Ironflow-Signature": sign(raw, "k", now)})[0] == 200
    status, body = call([fn_of(ok)], None, raw=raw, signing_key="k")
    assert status == 401 and body["error"]["code"] == "SIGNATURE_MISSING"
    status, body = call([fn_of(ok)], None, raw=raw, signing_key="k",
                        headers={"x-ironflow-signature": sign(raw, "bad", now)})
    assert status == 401 and body["error"]["code"] == "SIGNATURE_INVALID"


def test_signing_key_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IRONFLOW_SIGNING_KEY", "k")
    status, _ = call([fn_of(ok)], req(), signing_key=None)
    assert status == 401


def test_upcasters_applied() -> None:
    r = UpcasterRegistry()
    r.register("e", 1, 2, lambda d: {"y": d["x"]})

    async def h(ctx: Any) -> Any:
        return ctx.event.data
    assert call([fn_of(h)], req(), upcasters=r)[1]["result"] == {"y": 1}


def test_duplicate_function_ids_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        index_functions([fn_of(ok), fn_of(ok)])


def test_event_metadata_reaches_the_handler() -> None:
    async def h(ctx: Any) -> Any:
        return ctx.event.metadata

    status, body = call([fn_of(h)], req(event={**EVENT, "metadata": {"k": "v"}}))
    assert status == 200 and body["result"] == {"k": "v"}


def test_publish_step_posts_to_the_configured_server() -> None:
    engine = FakeEngine()
    try:
        async def h(ctx: Any) -> Any:
            return (await ctx.step.publish("orders", {"a": 1})).event_id

        status, body = call([fn_of(h)], req(), server_url=engine.url, api_key="ifkey_x", environment="staging")
        published = engine.calls("publish")
    finally:
        engine.shutdown()
    assert status == 200 and body["result"] == "evt_pub_1"
    assert published[0]["body"] == {"topic": "orders", "data": {"a": 1}}
    headers = {k.lower(): v for k, v in published[0]["headers"].items()}
    assert headers["x-ironflow-run-id"] == "run_1" and headers["x-ironflow-environment"] == "staging"


def test_publish_step_without_a_server_url_fails_without_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IRONFLOW_SERVER_URL", raising=False)

    async def h(ctx: Any) -> Any:
        await ctx.step.publish("orders", {})

    status, body = call([fn_of(h)], req())
    assert status == 200 and body["status"] == "failed"
    assert body["error"]["retryable"] is False and "server URL" in body["error"]["message"]


@pytest.mark.parametrize("configured,env_var,want", [
    ("staging", "qa", "staging"),
    (None, "qa", "qa"),
    (None, None, None),
])
def test_run_info_environment_has_no_default_fallback(
    monkeypatch: pytest.MonkeyPatch, configured: str | None, env_var: str | None, want: str | None
) -> None:
    """#2471: push RunInfo.environment is explicit only; "default" would 403 a scoped key."""
    if env_var is None:
        monkeypatch.delenv("IRONFLOW_ENV", raising=False)
    else:
        monkeypatch.setenv("IRONFLOW_ENV", env_var)

    async def h(ctx: Any) -> Any:
        return ctx.run.environment

    status, body = call([fn_of(h)], req(), environment=configured)
    assert status == 200 and body["result"] == want


def test_agent_memory_backend_gets_the_run_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """#2471: agent() hands ctx.run.environment to the default memory backend."""
    import ironflow.agent._agent as agent_mod
    from ironflow.agent import MemoryConfig, agent

    seen: list[str | None] = []
    monkeypatch.setattr(agent_mod, "rpc_backend", lambda environment=None: seen.append(environment))

    @agent(id="fn", triggers=[{"event": "e"}], memory=MemoryConfig("s1", "notes"))
    async def a(ctx: Any) -> Any:
        return "ok"

    status, body = call([a], req(), environment="staging")
    assert status == 200 and body["result"] == "ok"
    assert seen == ["staging"]
