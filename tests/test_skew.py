"""Version skew (ADR 0125): the SDK names itself on every call, and a newer SDK
on an older server fails with the minimum server version."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError

import ironflow
from ironflow import IronflowClient, IronflowRPC, IronflowRPCError
from ironflow._skew import (
    MIN_SERVER_VERSION_SUBSCRIBE_BIDIRECTIONAL,
    SDK_HEADER,
    sdk_header_value,
)
from ironflow.rpc._runtime import ServerTooOldError
from ironflow.rpc.v1 import CreateWebhookSourceRequest
from tests.harness import Response

from .rpc_server import serve

BIDI = "/ironflow.v1.PubSubService/SubscribeBidirectional"


def test_header_value_names_language_and_version() -> None:
    assert re.fullmatch(r"python/\S+", sdk_header_value())


def test_rest_call_sends_the_header(server) -> None:
    server.script(Response(body={"events": []}))
    IronflowClient(server_url=server.url).request("GET", "/api/v1/events")
    headers = {k.lower(): v for k, v in server.requests[0]["headers"].items()}
    assert headers[SDK_HEADER.lower()] == sdk_header_value()


def test_a_caller_header_wins(server) -> None:
    server.script(Response(body={}))
    IronflowClient(server_url=server.url).request("GET", "/api/v1/events", headers={SDK_HEADER: "custom/1"})
    headers = {k.lower(): v for k, v in server.requests[0]["headers"].items()}
    assert headers[SDK_HEADER.lower()] == "custom/1"


def test_rpc_call_sends_the_header() -> None:
    with serve() as srv:
        client = IronflowRPC(server_url=srv.url)
        try:
            client.webhooks.create_source(CreateWebhookSourceRequest(name="Stripe", event_prefix="stripe."))
        finally:
            client.close()
        assert srv.service.seen_headers[0][SDK_HEADER.lower()] == sdk_header_value()


def test_unimplemented_names_the_minimum_server() -> None:
    err = IronflowRPCError._from_connect(ConnectError(Code.UNIMPLEMENTED, "no such method"), procedure=BIDI)
    assert isinstance(err, ServerTooOldError)
    assert err.code == "SERVER_TOO_OLD"
    assert err.retryable is False
    assert MIN_SERVER_VERSION_SUBSCRIBE_BIDIRECTIONAL == "0.48.0" and "0.48.0" in str(err)


def test_other_codes_and_other_procedures_pass_through() -> None:
    other = IronflowRPCError._from_connect(ConnectError(Code.UNAVAILABLE, "down"), procedure=BIDI)
    assert type(other) is IronflowRPCError and other.code == "unavailable"
    plain = IronflowRPCError._from_connect(ConnectError(Code.UNIMPLEMENTED, "x"), procedure="/ironflow.v1.Other/Method")
    assert type(plain) is IronflowRPCError


def test_translating_iter_names_the_minimum_server_on_first_read() -> None:
    from ironflow.rpc._runtime import _translating_iter

    def boom() -> Any:
        raise ConnectError(Code.UNIMPLEMENTED, "no such method")
        yield  # pragma: no cover

    with pytest.raises(ServerTooOldError, match="0.48.0"):
        list(_translating_iter(boom(), lambda: False, "IronflowRPC", BIDI))


def test_rpc_facade_accepts_an_enum_value_a_newer_server_added() -> None:
    from ironflow.rpc.v1 import GetRunRequest

    with serve() as srv:
        client = IronflowRPC(server_url=srv.url)
        try:
            run = client.runs.get(GetRunRequest(id="future"))
        finally:
            client.close()
    assert run.id == "future" and int(run.status) == 99


def test_every_header_site_sends_the_sdk_header() -> None:
    """A new place that builds request headers must call into _skew."""
    root = Path(ironflow.__file__).parent
    sites = [p for p in root.rglob("*.py") if "_gen" not in p.parts and re.search(r"[Aa]uthorization", p.read_text())]
    missing = [str(p.relative_to(root)) for p in sites if "_skew" not in p.read_text() and "sdk_header" not in p.read_text()]
    assert missing == [], missing


def test_unknown_enum_values_load(server) -> None:
    server.script(
        Response(body={"id": "r1", "status": "RUN_STATUS_ZZ_FUTURE"}),
        Response(body={"runs": [{"id": "r1", "status": "RUN_STATUS_ZZ_FUTURE"}, {"id": "r2", "status": "RUN_STATUS_COMPLETED"}]}),
    )
    c = IronflowClient(server_url=server.url)
    assert c.request("POST", "/ironflow.v1.IronflowService/GetRun", body={"id": "r1"})["status"] == "RUN_STATUS_ZZ_FUTURE"
    runs = c.request("POST", "/ironflow.v1.IronflowService/ListRuns", body={})["runs"]
    assert [r["id"] for r in runs] == ["r1", "r2"]
