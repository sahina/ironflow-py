"""The RPC clients expose exactly what the ledger promises (#1781).

Asserted against `sdk/python/rpc-capabilities.yaml` directly rather than a
snapshot file. A snapshot would be a second copy of the ledger that has to be
regenerated in lockstep — the duplication ADR 0062 rejected for the ledger
itself. The acceptance criterion IS "expose exactly the promised capability
methods", so this states it once, where it is already written down.

Types still get a snapshot (tests/rpc_api_snapshot.json): those come from the
protos and have no ledger row.
"""

from __future__ import annotations

import re
from pathlib import Path

import ironflow
from ironflow import AsyncIronflowRPC, IronflowRPC

LEDGER = Path(__file__).parents[1] / "rpc-capabilities.yaml"

#: Inherited from ConnectClientSync by every generated service client. The
#: facade wraps rather than subclasses precisely so these stay off the public
#: API — a subclass could not remove them.
ESCAPE_HATCHES = frozenset(
    {
        "execute_unary",
        "execute_server_stream",
        "execute_client_stream",
        "execute_bidi_stream",
    }
)

#: Both classifications are exposed as of PR 3. This stayed {"rpc"} through
#: PR 2 so "exactly N" held at every commit rather than being red in between.
EXPOSED_NOW = frozenset({"rpc", "stream"})


def ledger_rows() -> list[tuple[str, str, str]]:
    """(classification, capability, method) for every classified row."""
    rows: list[tuple[str, str, str]] = []
    cur: dict[str, str] = {}
    for line in LEDGER.read_text().splitlines():
        if re.match(r"^  - path: ironflow\.v1\.\w+/\w+$", line):
            if {"classification", "capability", "method"} <= cur.keys():
                rows.append((cur["classification"], cur["capability"], cur["method"]))
            cur = {}
            continue
        m = re.match(r"^    (classification|capability|method): (\S+)$", line)
        if m:
            cur[m.group(1)] = m.group(2)
    if {"classification", "capability", "method"} <= cur.keys():
        rows.append((cur["classification"], cur["capability"], cur["method"]))
    return rows


def expected_surface() -> dict[str, set[str]]:
    return {
        cap: {m for c, ca, m in ledger_rows() if ca == cap and c in EXPOSED_NOW}
        for _, cap, _ in ledger_rows()
    }


def actual_surface(client: object) -> dict[str, set[str]]:
    return {
        cap: {a for a in dir(getattr(client, cap)) if not a.startswith("_")}
        for cap in expected_surface()
    }


def test_ledger_is_readable() -> None:
    """Guards the parser, not the client.

    Every other test here compares two things this file computes. If the parser
    silently returned nothing they would all pass while checking nothing —
    the failure mode ADR 0059 exists to prevent, reproduced in a test file.
    """
    rows = ledger_rows()
    assert len(rows) == 46, f"expected 46 classified rows, parsed {len(rows)}"
    assert sum(1 for c, _, _ in rows if c == "rpc") == 42
    assert sum(1 for c, _, _ in rows if c == "stream") == 4


def test_sync_client_exposes_exactly_the_ledger() -> None:
    assert actual_surface(IronflowRPC(server_url="http://x")) == expected_surface()


def test_async_client_exposes_exactly_the_ledger() -> None:
    assert actual_surface(AsyncIronflowRPC(server_url="http://x")) == expected_surface()


def test_no_escape_hatches_on_any_namespace() -> None:
    """The four `execute_*` methods must not reach the public API.

    Covered by the equality tests above, but asserted by name so a failure says
    what leaked instead of printing two 40-element set diffs.
    """
    for client in (IronflowRPC(server_url="http://x"), AsyncIronflowRPC(server_url="http://x")):
        for cap in expected_surface():
            leaked = ESCAPE_HATCHES & {
                a for a in dir(getattr(client, cap)) if not a.startswith("_")
            }
            assert not leaked, f"{type(client).__name__}.{cap} leaked {sorted(leaked)}"


def test_subscribe_bidirectional_is_not_exposed() -> None:
    """Served, but the handler answers Unimplemented.

    Its absence is a promise in the ADR, and it is generated onto
    PubSubServiceClientSync, so this asserts the wrapper is what removes it.
    """
    from ironflow._gen.pubsub_connect import PubSubServiceClientSync

    assert hasattr(PubSubServiceClientSync, "subscribe_bidirectional"), (
        "the generated client no longer has subscribe_bidirectional — this test "
        "is asserting the absence of something that no longer exists anywhere."
    )
    for client in (IronflowRPC(server_url="http://x"), AsyncIronflowRPC(server_url="http://x")):
        assert not hasattr(client.pubsub, "subscribe_bidirectional")


def test_transport_only_rpcs_are_not_exposed() -> None:
    """The projection-runner transports are plumbing the SDK drives itself.

    They sit in RPC_SKIP, so they have no ledger row at all — meaning nothing
    in the equality tests above would notice them appearing.
    """
    from ironflow._gen.projection_connect import ProjectionServiceClientSync

    transport = {
        "poll_projection_events",
        "ack_projection_events",
        "save_projection_state",
        "report_rebuild_progress",
    }
    assert transport <= {a for a in dir(ProjectionServiceClientSync) if not a.startswith("_")}

    client = IronflowRPC(server_url="http://x")
    exposed = {a for a in dir(client.projections) if not a.startswith("_")}
    assert not (transport & exposed)


def test_module_exports() -> None:
    """`ironflow.__all__` is public surface and nothing else gated it.

    The install smoke in `make test-python` proves the named symbols import; it
    cannot notice a new one appearing. PR 2 is the commit that turns this list
    from four stable names into one that grows.
    """
    assert sorted(ironflow.__all__) == [
        "AsyncIronflowRPC",
        "BaseClient",
        "HealthResponse",
        "IDEMPOTENT_METHODS",
        "IronflowClient",
        "IronflowError",
        "IronflowRPC",
        "IronflowRPCError",
        "NO_TIMEOUT",
        "ReadinessResponse",
        "ServerCapabilities",
    ]
    missing = [n for n in ironflow.__all__ if not hasattr(ironflow, n)]
    assert not missing, f"__all__ names symbols that are not bound: {missing}"
