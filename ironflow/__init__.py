"""Ironflow Python SDK.

HAND-WRITTEN. `cmd/sdk-gen` emits `client.py` and `models.py`;
`make proto-python` emits `rpc/v1.py` and `rpc/_client.py`.

Client-only: this package ships no worker runtime. There is no `step.run`, no
`step.sleep`, and no push or pull mode. For durable step execution use the Go
or JavaScript SDK.

Two clients, different protocols, both first-class:

  `IronflowClient` — REST. Events, runs, projections, entity streams, KV,
  config, schemas. Retries idempotent methods.

  `IronflowRPC` / `AsyncIronflowRPC` — ConnectRPC. The capabilities REST does
  not deliver: webhook management, agent tools, time travel, pub/sub consumer
  groups, function versioning. Retries only the unary methods the protos
  annotate `NO_SIDE_EFFECTS`; reconnects a subscription you positioned with
  `start_after_sequence`, and no other stream (#1848).

Neither is a superset of the other. `IronflowClient.request()` remains the
escape hatch for anything neither wraps.
"""

from typing import TYPE_CHECKING, Any

from ._http import (
    IDEMPOTENT_METHODS,
    BaseClient,
    HealthResponse,
    IronflowError,
    ReadinessResponse,
    ServerCapabilities,
)
from .client import IronflowClient

if TYPE_CHECKING:  # pragma: no cover - import-time cost is the point
    from .rpc import NO_TIMEOUT, AsyncIronflowRPC, IronflowRPC, IronflowRPCError

__all__ = [
    "IDEMPOTENT_METHODS",
    "NO_TIMEOUT",
    "AsyncIronflowRPC",
    "BaseClient",
    "HealthResponse",
    "IronflowClient",
    "IronflowError",
    "IronflowRPC",
    "IronflowRPCError",
    "ReadinessResponse",
    "ServerCapabilities",
]

#: The ConnectRPC names, resolved on first access instead of at import.
#:
#: Importing `.rpc` eagerly pulls in connectrpc and seven generated protobuf
#: modules. Measured with `-X importtime`: it took `import ironflow` from ~45ms
#: to ~132ms, of which `ironflow.rpc._client` alone was ~91ms — paid by every
#: REST-only caller who never constructs an RPC client, on every CLI invocation
#: and every cold start.
#:
#: PEP 562 keeps the names on `ironflow` where the contract puts them, so
#: `from ironflow import IronflowRPC`, `hasattr`, and the `__all__` surface test
#: all behave exactly as before — the cost simply moves to first use.
_RPC_NAMES = frozenset(
    {"NO_TIMEOUT", "AsyncIronflowRPC", "IronflowRPC", "IronflowRPCError"}
)


def __getattr__(name: str) -> Any:
    if name in _RPC_NAMES:
        from . import rpc

        value = getattr(rpc, name)
        # Cache on the module so the indirection is paid once, not per access.
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """Keep tab-completion and `dir(ironflow)` listing the lazy names too."""
    return sorted(set(globals()) | _RPC_NAMES)
