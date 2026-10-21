"""The event-schemas group (#3003) through the Python RPC client.

Every test uses an event of its own, so the shared seed schema (versions 1 and 2,
read by schema-test-upcast) stays as the seed left it. The `readiness_seed`
fixture only skips the module outside `make test-readiness-live`. Tests run in
file order; later ones read the schema the earlier ones registered.
"""

from __future__ import annotations

import json
import time
from typing import Any

import pytest

from ironflow import IronflowRPCError
from ironflow.rpc.v1 import (
    CheckEnforcementRequest,
    DeleteSchemaRequest,
    GetSchemaRequest,
    ListSchemasRequest,
    RegisterSchemaRequest,
)

pytestmark = pytest.mark.usefixtures("readiness_seed")

NAME = f"readiness.schemas.python.{time.time_ns()}"


def _doc(field: str) -> dict[str, Any]:
    return {"type": "object", "properties": {field: {"type": "string"}}, "required": [field]}


def _register(rpc: Any, version: int, field: str) -> str:
    res = rpc.event_schemas.register(
        RegisterSchemaRequest(event_name=NAME, version=version, schema_json=json.dumps(_doc(field)))
    )
    return str(res.status)


def _code(rpc_call: Any) -> str:
    with pytest.raises(IronflowRPCError) as exc:
        rpc_call()
    return str(exc.value.code)


def test_schema_register(rpc: Any) -> None:
    assert _register(rpc, 1, "orderId") == "created"
    assert _register(rpc, 2, "orderRef") == "created"
    # The same name and version again replaces the document.
    assert _register(rpc, 1, "orderId") == "updated"
    # A document that does not compile is refused at registration.
    bad = RegisterSchemaRequest(event_name=NAME, version=3, schema_json='{"type": "no-such-type"}')
    assert _code(lambda: rpc.event_schemas.register(bad)) == "invalid_argument"


def test_schema_read(rpc: Any) -> None:
    latest = rpc.event_schemas.get(GetSchemaRequest(event_name=NAME))
    assert (latest.event_name, latest.version) == (NAME, 2)
    assert json.loads(latest.schema_json) == _doc("orderRef") and latest.created_at is not None
    first = rpc.event_schemas.get(GetSchemaRequest(event_name=NAME, version=1))
    assert first.version == 1 and json.loads(first.schema_json) == _doc("orderId")
    listed = rpc.event_schemas.list(ListSchemasRequest(event_name=NAME))
    assert sorted(s.version for s in listed.schemas) == [1, 2] and listed.total_count == 2


def test_schema_check_enforcement(rpc: Any) -> None:
    """Only Python wraps this RPC; Go, Node and the browser leave it to the CLI."""
    report = rpc.event_schemas.check_enforcement(CheckEnforcementRequest(event_name=NAME))
    assert report.mode in ("off", "warn", "reject")  # IRONFLOW_EVENT_SCHEMA_ENFORCEMENT may be set
    assert report.total_schemas == 2
    assert report.traffic_window_hours == 24  # 0 asks for the server default
    assert sorted((s.event_name, s.version, s.asserts) for s in report.schemas) == [(NAME, 1, True), (NAME, 2, True)]
    for s in report.schemas:
        assert s.schema_hash and not s.compile_error and not s.traffic and not s.traffic_truncated

    # A schema that accepts everything is registered fine and enforces nothing; this report is how that shows.
    open_name = NAME + ".open"
    rpc.event_schemas.register(RegisterSchemaRequest(event_name=open_name, version=1, schema_json="{}"))
    try:
        wide = rpc.event_schemas.check_enforcement(CheckEnforcementRequest(event_name=open_name, traffic_window_hours=72))
        assert wide.traffic_window_hours == 72 and wide.total_schemas == 1
        assert [(s.event_name, s.version, s.asserts) for s in wide.schemas] == [(open_name, 1, False)]
    finally:
        rpc.event_schemas.delete(DeleteSchemaRequest(event_name=open_name, version=1))


def test_schema_delete(rpc: Any) -> None:
    rpc.event_schemas.delete(DeleteSchemaRequest(event_name=NAME, version=2))
    assert rpc.event_schemas.get(GetSchemaRequest(event_name=NAME)).version == 1
    assert _code(lambda: rpc.event_schemas.get(GetSchemaRequest(event_name=NAME, version=2))) == "not_found"
    assert _code(lambda: rpc.event_schemas.delete(DeleteSchemaRequest(event_name=NAME, version=2))) == "not_found"
    assert [s.version for s in rpc.event_schemas.list(ListSchemasRequest(event_name=NAME)).schemas] == [1]
    rpc.event_schemas.delete(DeleteSchemaRequest(event_name=NAME, version=1))
    assert _code(lambda: rpc.event_schemas.get(GetSchemaRequest(event_name=NAME))) == "not_found"
