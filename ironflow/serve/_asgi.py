"""ASGI adapter for push serve. FastAPI/Starlette mount it; uvicorn runs it directly."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any

from .._discovery import hydrate_env_from_discovery
from ..worker._function import Function
from ._handler import handle, index_functions
from ._response import env
from ._webhook import Webhook

if TYPE_CHECKING:
    from ..upcaster import UpcasterRegistry

Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]
ASGIApp = Callable[[dict[str, Any], Receive, Send], Awaitable[None]]


def serve(
    functions: Sequence[Function], *, signing_key: str | None = None,
    upcasters: UpcasterRegistry | None = None, webhooks: Sequence[Webhook] = (),
    server_url: str | None = None, api_key: str | None = None, environment: str | None = None,
) -> ASGIApp:
    hydrate_env_from_discovery()
    fns = list(index_functions(functions).values())
    hooks = list(webhooks)
    if len({h.id for h in hooks}) != len(hooks):
        raise ValueError("duplicate webhook id")
    # Resolve once. "" (not None) passed on means "explicitly unset" to handle().
    key = env(signing_key, "IRONFLOW_SIGNING_KEY") or ""
    server = env(server_url, "IRONFLOW_SERVER_URL") or ""
    token = env(api_key, "IRONFLOW_API_KEY")
    # handle() gets the value without the "default" fallback: RunInfo.environment
    # must stay None when nothing is set, or agent memory sends "default" and a
    # key scoped to another environment gets 403 (#2471).
    run_env = env(environment, "IRONFLOW_ENV") or ""
    environ = run_env or "default"

    async def app(scope: dict[str, Any], receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            while True:
                msg = await receive()
                if msg["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif msg["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        if scope["type"] != "http":
            raise RuntimeError(f"unsupported ASGI scope type: {scope['type']}")
        chunks: list[bytes] = []
        while True:
            msg = await receive()
            if msg["type"] == "http.disconnect":
                return
            chunks.append(msg.get("body", b""))
            if not msg.get("more_body", False):
                break
        path, root = scope["path"], scope.get("root_path") or ""
        if root and (path == root or path.startswith(root + "/")):
            path = path[len(root):] or "/"
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}
        status, out, body = await handle(
            fns, method=scope["method"], path=path, headers=headers, body=b"".join(chunks),
            signing_key=key, upcasters=upcasters, webhooks=hooks, server_url=server,
            api_key=token, environment=run_env,
        )
        out["x-ironflow-environment"] = environ
        await send({"type": "http.response.start", "status": status,
                    "headers": [(k.encode("latin-1"), v.encode()) for k, v in out.items()]})
        await send({"type": "http.response.body", "body": body})

    return app
