"""Typed ConnectRPC clients for Ironflow.

    from ironflow import IronflowRPC
    from ironflow.rpc.v1 import CreateWebhookSourceRequest

    with IronflowRPC(server_url="http://localhost:9123", api_key="ifkey_...") as rpc:
        source = rpc.webhooks.create_source(
            CreateWebhookSourceRequest(name="Stripe", event_prefix="stripe.")
        )

Generated request and response types live in `ironflow.rpc.v1`. The private
`ironflow._gen` package is not part of the public API.
"""

from __future__ import annotations

from ._client import AsyncIronflowRPC, IronflowRPC
from ._runtime import NO_TIMEOUT, IronflowRPCError

__all__ = [
    "NO_TIMEOUT",
    "AsyncIronflowRPC",
    "IronflowRPC",
    "IronflowRPCError",
]
