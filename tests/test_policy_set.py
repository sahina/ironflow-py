import pytest

from ironflow import (
    IronflowClient,
    IronflowError,
    PreconditionFailedError,
    PreconditionRequiredError,
)
from tests.harness import Response


def client(server):
    return IronflowClient(server_url=server.url, initial_backoff=0.01, max_backoff=0.05)


def test_k01_put_sends_if_match_and_body(server):
    server.script(Response(200, {"version": 2, "modules": {}, "mode": "enforce", "enforcement_active": True}))
    out = client(server).policy_set_update({"modules": {"a.rego": "package p\n"}, "mode": "enforce"}, if_match="1")
    req = server.requests[-1]
    assert req["path"] == "/api/v1/policy-set"
    assert {k.lower(): v for k, v in req["headers"].items()}["if-match"] == "1"
    assert out["enforcement_active"] is True


@pytest.mark.parametrize("status,exc", [(412, PreconditionFailedError), (428, PreconditionRequiredError)])
def test_k02_typed_errors(server, status, exc):
    server.script(Response(status, {"code": "policy_set_x", "error": "m"}))
    with pytest.raises(exc) as ei:
        client(server).policy_set_update({"modules": {}}, if_match="0")
    assert ei.value.status_code == status


def test_not_propagated_keeps_its_code_and_the_committed_put_is_not_resent(server):
    server.script(
        Response(503, {"code": "policy_set_not_propagated", "error": "saved as version 3", "retryable": False}),
        Response(412, {"code": "policy_set_version_conflict", "error": "changed"}),
    )
    with pytest.raises(IronflowError) as ei:
        client(server).policy_set_update({"modules": {}}, if_match="2")
    assert ei.value.code == "policy_set_not_propagated"
    assert ei.value.retryable is False
    assert len(server.requests) == 1


def test_a_rate_limited_conditional_write_is_still_retried(server):
    server.script(Response(429, {"code": "policy_set_rate_limited", "error": "slow"}), Response(200, {"version": 3, "modules": {}, "mode": "monitor"}))
    assert client(server).policy_set_update({"modules": {}}, if_match="2")["version"] == 3
    assert len(server.requests) == 2
