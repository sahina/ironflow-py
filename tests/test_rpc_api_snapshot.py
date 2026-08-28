"""Public type snapshot for `ironflow.rpc.v1`.

SEPARATE from tests/api_snapshot.json on purpose. That snapshot guards a
specific hazard — deduplicateMethods renames a client method when route ORDER
changes, silently — and its failure message tells you to go check for that.
These names carry no such hazard: they come from protobuf message declarations,
not from a positional naming fallback. Folding them into one file would make
that message wrong for most of its contents.

What this file guards instead: `ironflow.rpc.v1` is a published namespace, so
adding or removing a message type in a .proto changes the Python SDK's public
API. That should be visible in a diff, not discovered by a user.

If you added a message to a proto and this test goes red, that is working as
intended. Regenerate to accept:

    cd sdk/python && python -c "import json;import ironflow.rpc.v1 as m;\\
    print(json.dumps(sorted(m.__all__),indent=2))" > tests/rpc_api_snapshot.json

If a name DISAPPEARED, look harder before regenerating. The export set is
module-level and driven by rpc-capabilities.yaml, so a service losing its last
`rpc` or `stream` row drops every one of its types at once — which is a
breaking change, not a tidy-up.
"""

from __future__ import annotations

import json
from pathlib import Path

import ironflow.rpc.v1 as rpc_v1

SNAPSHOT = Path(__file__).parent / "rpc_api_snapshot.json"


def _current() -> list[str]:
    return sorted(rpc_v1.__all__)


def test_public_types_match_snapshot() -> None:
    expected = json.loads(SNAPSHOT.read_text())
    actual = _current()

    added = sorted(set(actual) - set(expected))
    removed = sorted(set(expected) - set(actual))

    assert not removed, (
        f"Public types disappeared: {removed}. A proto removed a message, or a "
        f"service lost its last rpc/stream row in rpc-capabilities.yaml and took "
        f"its whole module with it. Both are breaking; confirm before "
        f"regenerating tests/rpc_api_snapshot.json."
    )
    assert not added, (
        f"New public types: {added}. Regenerate tests/rpc_api_snapshot.json to accept."
    )


def test_all_names_are_importable() -> None:
    """__all__ must not name something the module does not actually bind.

    The generator builds __all__ from a regex over generated source. A parse
    that drifts would produce a namespace whose `from ironflow.rpc.v1 import X`
    fails for a name the snapshot happily records.
    """
    missing = [n for n in rpc_v1.__all__ if not hasattr(rpc_v1, n)]
    assert not missing, f"__all__ names types that are not bound: {missing}"


def test_transport_only_services_do_not_leak() -> None:
    """Types from transport-only services stay out.

    WorkerService is transport plumbing (RPC_SKIP). Exporting its types would
    advertise a surface the public client cannot use. The sampled name comes
    from worker_pb; if the proto renames it this test is checking nothing, so it
    also asserts that the module still declares it.
    """
    banned = {"WorkerHeartbeat"}
    leaked = banned & set(rpc_v1.__all__)
    assert not leaked, f"types from excluded services leaked: {sorted(leaked)}"

    from ironflow._gen import worker_pb

    assert hasattr(worker_pb, "WorkerHeartbeat"), (
        "worker_pb no longer declares WorkerHeartbeat — same problem."
    )
