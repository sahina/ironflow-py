# sdk/python/ironflow/serve/_handler.py
"""Push-mode request handling. Framework-free: bytes in, (status, headers, bytes) out.

A function outcome is always HTTP 200; the engine treats any 4xx/5xx as a
transport error, retries it and counts it against the circuit breaker. The
``resume`` field is ignored on purpose: a woken sleep or matched wait arrives
as a completed step.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from .._discovery import hydrate_env_from_discovery
from ..worker._function import Function
from ..worker._protocol import CompletedStep
from ..worker._publish import bind_publish
from ..worker._run import run_function
from ..worker._step import ExecutionContext, PublishFn, RunInfo
from ..worker._transport import Transport
from ._response import Response, env, error_response, json_response
from ._signature import SignatureError, verify_signature
from ._webhook import Webhook, handle_webhook

if TYPE_CHECKING:
    from ..upcaster import UpcasterRegistry

_log = logging.getLogger("ironflow.serve")


def index_functions(functions: Iterable[Function]) -> dict[str, Function]:
    out: dict[str, Function] = {}
    for fn in functions:
        if fn.id in out:
            raise ValueError(f"duplicate function id: {fn.id!r}")
        out[fn.id] = fn
    return out


def _valid(req: Any) -> bool:
    if not (isinstance(req, dict) and isinstance(req.get("run_id"), str) and req["run_id"]
            and isinstance(req.get("function_id"), str) and isinstance(req.get("event"), dict)):
        return False
    attempt = req.get("attempt", 1)
    steps = req.get("steps") or []
    return (isinstance(attempt, int) and not isinstance(attempt, bool)
            and isinstance(steps, list)
            and all(isinstance(s, dict) and isinstance(s.get("id"), str) for s in steps)
            and isinstance(req.get("secrets") or {}, dict))


def _memo(steps: list[dict[str, Any]]) -> list[CompletedStep]:
    out: list[CompletedStep] = []
    for s in steps:
        # Defensive: the current engine push path (PushExecutor.Execute, executor.go:178-187)
        # builds CompletedStep only from steps with StepStatusCompleted and never sets
        # "error" on the wire. Nothing production-side sends a failed memoized step today,
        # but if one ever arrives, remap it rather than mis-report it as completed.
        row: CompletedStep = {"step_id": s["id"], "name": s.get("name", ""), "output": s.get("output"),
                              "status": "failed" if s.get("error") else s.get("status", "completed")}
        if s.get("error"):
            row["error"] = s["error"]  # engine sends a string; _invoke_error accepts one
        out.append(row)
    return out


def _publisher(server_url: str, api_key: str | None, environment: str, run_id: str) -> PublishFn:
    """Builds the Transport per publish, lazily: most executions never publish (same per-call pattern as _webhook.py)."""
    async def publish(topic: str, data: Any, idempotency_key: str | None) -> dict[str, Any]:
        return await bind_publish(Transport(server_url, api_key, environment), run_id)(topic, data, idempotency_key)

    return publish


async def handle(
    functions: Sequence[Function], *, method: str, path: str, headers: Mapping[str, str], body: bytes,
    signing_key: str | None = None, upcasters: UpcasterRegistry | None = None,
    webhooks: Sequence[Webhook] = (), server_url: str | None = None, api_key: str | None = None,
    environment: str | None = None,
) -> Response:
    hydrate_env_from_discovery()
    if method != "POST":
        return error_response(405, "METHOD_NOT_ALLOWED", "only POST is allowed")
    headers = {k.lower(): v for k, v in headers.items()}
    if path == "/ironflow/agent-tools/dispatch":  # agent._dispatch.DISPATCH_PATH, imported lazily
        from ..agent._dispatch import handle_dispatch
        status, payload = await handle_dispatch(headers, body)
        return json_response(status, payload)
    if path.startswith("/webhooks/") and path[len("/webhooks/"):]:
        return await handle_webhook(
            {w.id: w for w in webhooks}, path[len("/webhooks/"):], method=method, path=path,
            headers=headers, body=body, server_url=env(server_url, "IRONFLOW_SERVER_URL"),
            api_key=env(api_key, "IRONFLOW_API_KEY"),
            environment=env(environment, "IRONFLOW_ENV") or "default",
        )
    # ponytail: re-indexed per request; O(functions), trivial next to running a handler.
    fns = index_functions(functions)
    key = env(signing_key, "IRONFLOW_SIGNING_KEY")
    if key:
        try:
            verify_signature(body, headers.get("x-ironflow-signature"), key)
        except SignatureError as exc:
            return error_response(401, exc.code, str(exc))
    try:
        req = json.loads(body)
    except (ValueError, RecursionError):
        return error_response(400, "INVALID_JSON", "request body is not valid JSON")
    if not _valid(req):
        return error_response(400, "VALIDATION_ERROR",
                              "run_id, function_id and event are required; attempt, steps and secrets must be well-formed")
    fn = fns.get(req["function_id"])
    if fn is None:
        return error_response(404, "FUNCTION_NOT_FOUND", f"function not found: {req['function_id']}")

    ctx = ExecutionContext(req["run_id"], _memo(req.get("steps") or []), fn.step_timeout)
    server = env(server_url, "IRONFLOW_SERVER_URL")
    if server:
        ctx.publish = _publisher(
            server, env(api_key, "IRONFLOW_API_KEY"), env(environment, "IRONFLOW_ENV") or "default", req["run_id"])
    outcome = await run_function(
        fn, raw_event=req["event"], upcasters=upcasters, ctx=ctx,
        run=RunInfo(id=req["run_id"], function_id=fn.id, attempt=req.get("attempt", 1),
                    environment=env(environment, "IRONFLOW_ENV") or None),
        secrets=req.get("secrets") or {},
        logger=logging.LoggerAdapter(_log, {"run_id": req["run_id"], "function_id": fn.id}),
    )
    if outcome["status"] == "completed":
        outcome = {"status": "completed", "result": outcome["output"]}
    try:
        return json_response(200, {**outcome, "steps": ctx.executed})
    except (TypeError, ValueError) as exc:
        # Reachable through yield info, which nothing validates. Steps are always
        # encodable (step.run checks outputs), so keep them, as pull _report does.
        return json_response(200, {"status": "failed", "steps": ctx.executed, "error": {
            "message": f"result is not JSON-encodable: {exc}", "code": "SERIALIZATION_ERROR",
            "retryable": False}})
