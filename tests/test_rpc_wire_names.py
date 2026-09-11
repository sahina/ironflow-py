"""Every exposed RPC calls the procedure the ledger names (#1828).

The chain a call travels is ledger row -> facade wrapper -> generated client
method -> `MethodInfo`, and only the last link decides what goes on the wire.
Three checks already cover the earlier links: `test_rpc_surface` asserts the
facade exposes exactly the ledger's methods, the type snapshot pins request and
response annotations, and `make proto-python-verify` regenerates and diffs.

None of them read `MethodInfo`. A plugin regression emitting `def trigger(...)`
bound to `MethodInfo(name="Emit")` passes all three: `Trigger` and `Emit` share
`TriggerRequest -> TriggerResponse`, so it type-checks, and the verify step
diffs generation against generation from the same pinned plugin, so both sides
reproduce the wrong name identically.

This walks all 44 exposed rows on both clients and compares the wire path to
the ledger. It resolves function objects and reads each one's own source rather
than pairing `def` with the next `MethodInfo(` in the file — that naive form
mispairs helper methods such as `path` and reports mismatches that are not real.

Scope: wire NAMES only. A correct name bound to the wrong `input`/`output` is
the type snapshot's job, and is why the example above type-checks.
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

from ironflow import AsyncIronflowRPC, IronflowRPC

LEDGER = Path(__file__).parents[1] / "rpc-capabilities.yaml"

#: Same set `test_rpc_surface` exposes. A row outside it has no facade method
#: to resolve, so it is not walkable here.
EXPOSED = frozenset({"rpc", "stream"})

_CALL = re.compile(r"self\._client\.(\w+)\(")
_METHOD_INFO = re.compile(r'MethodInfo\(\s*name="(\w+)",\s*service_name="([\w.]+)",')


def ledger_rows() -> list[tuple[str, str, str]]:
    """(capability, method, path) for every exposed row."""
    rows: list[tuple[str, str, str]] = []
    cur: dict[str, str] = {}

    def flush() -> None:
        if (
            cur.get("classification") in EXPOSED
            and {"capability", "method"} <= cur.keys()
        ):
            rows.append((cur["capability"], cur["method"], cur["path"]))

    for line in LEDGER.read_text().splitlines():
        m = re.match(r"^  - path: (ironflow\.v1\.\w+/\w+)$", line)
        if m:
            flush()
            cur = {"path": m.group(1)}
            continue
        m = re.match(r"^    (classification|capability|method): (\S+)$", line)
        if m:
            cur[m.group(1)] = m.group(2)
    flush()
    return rows


def generated_call(facade_src: str) -> str:
    """The generated method a facade wrapper delegates to.

    Exactly one, or the wrapper has a shape this walk does not model and the
    row would otherwise go unchecked. Measured at 88 of 88 when written.
    """
    calls = _CALL.findall(facade_src)
    assert len(calls) == 1, (
        f"expected exactly one self._client.X( call, found {len(calls)}: {calls}. "
        "A wrapper with a different shape is a row this test stops covering."
    )
    return calls[0]


def wire_path(generated_src: str) -> str:
    """The `service/Method` path a generated method puts on the wire."""
    found = _METHOD_INFO.findall(generated_src)
    assert len(found) == 1, (
        f"expected exactly one MethodInfo(...) construction, found {len(found)}"
    )
    name, service = found[0]
    return f"{service}/{name}"


def resolve(client: object, capability: str, method: str) -> str:
    """Walk one ledger row to the path it actually calls."""
    namespace = getattr(client, capability)
    facade = getattr(type(namespace), method)
    call = generated_call(inspect.getsource(facade))
    generated = getattr(type(namespace._client), call)
    return wire_path(inspect.getsource(generated))


def test_ledger_is_readable() -> None:
    """Guards the parser.

    Every walk below is driven by this list. A parser that silently returned
    nothing would leave them all green while checking nothing.
    """
    rows = ledger_rows()
    assert len(rows) == 90, f"expected 90 exposed rows, parsed {len(rows)}"
    assert len({p for _, _, p in rows}) == 90, "ledger paths are not unique"


@pytest.mark.parametrize(
    "client_factory", [IronflowRPC, AsyncIronflowRPC], ids=["sync", "async"]
)
def test_every_exposed_row_calls_the_procedure_the_ledger_names(
    client_factory: type,
) -> None:
    client = client_factory(server_url="http://x")
    wrong = [
        (cap, method, path, actual)
        for cap, method, path in ledger_rows()
        if (actual := resolve(client, cap, method)) != path
    ]
    assert not wrong, "\n".join(
        f"rpc.{c}.{m} -> {a}, ledger says {p}" for c, m, p, a in wrong
    )


def test_no_two_rows_resolve_to_the_same_procedure() -> None:
    """Two capabilities cannot share one wire path.

    The walk above compares each row to the ledger independently, so a wrapper
    wired to another row's generated method fails there — but only because the
    ledger paths happen to be unique. This states the collision directly, and
    catches the case where a duplicated wrapper body sends two capability
    methods to the same procedure.
    """
    for factory in (IronflowRPC, AsyncIronflowRPC):
        client = factory(server_url="http://x")
        resolved = [resolve(client, cap, m) for cap, m, _ in ledger_rows()]
        duplicated = {p for p in resolved if resolved.count(p) > 1}
        assert not duplicated, (
            f"{factory.__name__} rows collide on {sorted(duplicated)}"
        )


def test_sync_and_async_bind_to_their_own_generated_clients() -> None:
    """A namespace wired to the other client's class would still type-check.

    Nothing else pins this: both generated classes carry the same method names
    and the same MethodInfo, so the walk above passes either way.
    """
    sync, asyn = (
        IronflowRPC(server_url="http://x"),
        AsyncIronflowRPC(server_url="http://x"),
    )
    for cap, _, _ in ledger_rows():
        sync_cls = type(getattr(sync, cap)._client).__name__
        async_cls = type(getattr(asyn, cap)._client).__name__
        assert sync_cls.endswith("ClientSync"), f"rpc.{cap} sync uses {sync_cls}"
        assert not async_cls.endswith("ClientSync"), f"rpc.{cap} async uses {async_cls}"


def test_the_walk_detects_a_wrong_wire_name() -> None:
    """Proves the extraction can fail, using the extraction itself.

    A separate hand-rolled check here would test different code than the walk
    runs, which is how a harness ends up asserting nothing.
    """
    facade = "        return self._client.trigger(\n            request,\n        )\n"
    generated = (
        "    def trigger(\n"
        "        self,\n"
        "    ) -> TriggerResponse:\n"
        "        return self.execute_unary(\n"
        "            method=MethodInfo(\n"
        '                name="Emit",\n'
        '                service_name="ironflow.v1.PubSubService",\n'
        "            ),\n"
        "        )\n"
    )
    assert generated_call(facade) == "trigger"
    assert wire_path(generated) == "ironflow.v1.PubSubService/Emit"
    assert wire_path(generated) != "ironflow.v1.PubSubService/Trigger"


def test_a_multi_call_wrapper_is_rejected_not_skipped() -> None:
    """The shape guard fails loudly. A wrapper it cannot read must not pass."""
    with pytest.raises(AssertionError, match="exactly one self._client"):
        generated_call("self._client.a()\nself._client.b()\n")
    with pytest.raises(AssertionError, match="exactly one self._client"):
        generated_call("return None\n")
    with pytest.raises(AssertionError, match="exactly one MethodInfo"):
        wire_path("no method info here\n")
