# sdk/python/ironflow/serve/_register.py
from __future__ import annotations

from collections.abc import Sequence

from .._discovery import hydrate_env_from_discovery
from .._http import DEFAULT_SERVER_URL, IronflowError
from ..worker._function import Function, registration_body
from ..worker._transport import Transport
from ._handler import index_functions
from ._response import env


async def register(
    functions: Sequence[Function], *, endpoint_url: str, server_url: str | None = None,
    api_key: str | None = None, environment: str | None = None,
) -> None:
    """Register ``functions`` as push functions served at ``endpoint_url``. Call once per deploy."""
    hydrate_env_from_discovery()
    transport = Transport(
        env(server_url, "IRONFLOW_SERVER_URL") or DEFAULT_SERVER_URL,
        env(api_key, "IRONFLOW_API_KEY"),
        env(environment, "IRONFLOW_ENV") or "default",
    )
    for fn in index_functions(functions).values():
        reply = await transport.request("POST", "/ironflow.v1.IronflowService/RegisterFunction",
                                        registration_body(fn, mode="push", endpoint_url=endpoint_url))
        if not reply.ok:
            # Known limit: Reply.error_code reads a string, Connect errors are {code, message}.
            raise IronflowError(f"register function {fn.id} failed: {reply.status} {reply.body}",
                                status_code=reply.status, code=reply.error_code,
                                retryable=reply.status >= 500)
