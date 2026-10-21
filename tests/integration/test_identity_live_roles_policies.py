"""role-crud and policy-set (#3003) on a tenant of its own.

The sets deny one action in monitor mode, so they change nothing the other
tests do. `make test-identity-live` sets IRONFLOW_TEST_PLATFORM_TOKEN; without
it this module skips.
"""

from __future__ import annotations

import os
import time
from typing import Any

import pytest

from ironflow import IronflowClient, IronflowError

pytestmark = pytest.mark.skipif(
    not os.environ.get("IRONFLOW_TEST_PLATFORM_TOKEN"),
    reason="IRONFLOW_TEST_PLATFORM_TOKEN not set; run `make test-identity-live`",
)

SUFFIX = str(time.time_ns())
DENY_DELETE = 'package ironflow.authz\n\ndeny contains "no deletes" if input.request.action == "runs:delete"\n'
DENY_CANCEL = 'package ironflow.authz\n\ndeny contains "no cancels" if input.request.action == "runs:cancel"\n'


def error_of(call: Any) -> IronflowError:
    with pytest.raises(IronflowError) as exc:
        call()
    return exc.value


@pytest.fixture(scope="module")
def tenant() -> dict[str, Any]:
    operator = IronflowClient(os.environ["IRONFLOW_TEST_SERVER"], api_key=os.environ["IRONFLOW_TEST_API_KEY"])
    return dict(operator.tenants_provision({"org_name": f"Py roles {SUFFIX}"}))


@pytest.fixture(scope="module")
def admin(tenant: dict[str, Any]) -> IronflowClient:
    return IronflowClient(os.environ["IRONFLOW_TEST_SERVER"], api_key=tenant["api_key"]["key"])


def test_role_crud(admin: IronflowClient, tenant: dict[str, Any]) -> None:
    name = f"deployer-{SUFFIX}"
    created = admin.roles_create({"name": name})
    assert created["name"] == name and created["org_id"] == tenant["org"]["id"] and created["id"]
    assert not created.get("is_default") and created["created_at"]
    assert error_of(lambda: admin.roles_create({"name": name})).status_code == 409
    assert error_of(lambda: admin.roles_create({"name": "admin"})).status_code == 400

    got = admin.roles_get(created["id"])
    assert (got["id"], got["name"], got["org_id"]) == (created["id"], name, tenant["org"]["id"])
    roles = admin.roles_list()
    assert sorted(r["name"] for r in roles) == sorted(["admin", "developer", name, "viewer"])

    renamed = f"releaser-{SUFFIX}"
    updated = admin.roles_patch(created["id"], {"name": renamed})
    assert updated["id"] == created["id"] and updated["name"] == renamed
    assert admin.roles_get(created["id"])["name"] == renamed
    # Built-in roles are fixed.
    builtin = next(r["id"] for r in roles if r["name"] == "admin")
    assert error_of(lambda: admin.roles_patch(builtin, {"name": "root"})).status_code == 400
    assert error_of(lambda: admin.roles_delete(builtin)).status_code == 400

    admin.roles_delete(created["id"])
    assert error_of(lambda: admin.roles_get(created["id"])).status_code == 404
    assert error_of(lambda: admin.roles_delete(created["id"])).status_code == 404


def test_policy_set(admin: IronflowClient) -> None:
    v1 = {"modules": {"authz.rego": DENY_DELETE}, "mode": "monitor"}
    v2 = {"modules": {"authz.rego": DENY_DELETE, "more.rego": DENY_CANCEL}, "mode": "monitor"}
    delete_input = {"request": {"action": "runs:delete"}, "subject": {}}
    cancel_input = {"request": {"action": "runs:cancel"}, "subject": {}}

    empty = admin.policy_set_list()
    assert empty["version"] == 0 and empty["modules"] == {}
    assert isinstance(empty["enforcement_active"], bool)

    # validate and dry-run store nothing.
    assert admin.policy_set_validate(v1)["valid"] is True
    bad = error_of(lambda: admin.policy_set_validate({"modules": {"bad.rego": "package ironflow.authz\n\ndeny contains {\n"}}))
    assert bad.status_code == 400 and bad.code == "policy_set_compile_error"
    deny = admin.policy_set_dry_run({"set": v1, "input": delete_input})
    assert deny["decision"] == "deny" and "no deletes" in deny["reasons"]
    assert admin.policy_set_dry_run({"set": v1, "input": cancel_input})["decision"] == "allow"
    assert admin.policy_set_list()["version"] == 0

    put1 = admin.policy_set_update(v1, if_match="0")
    assert (put1["version"], put1["mode"], put1["modules"]) == (1, "monitor", v1["modules"])
    assert error_of(lambda: admin.policy_set_update(v2, if_match="0")).status_code == 412
    put2 = admin.policy_set_update(v2, if_match="1")
    assert put2["version"] == 2 and put2["modules"] == v2["modules"]
    current = admin.policy_set_list()
    assert current["version"] == 2 and current["modules"] == v2["modules"]

    versions = admin.policy_set_list_versions()["versions"]
    assert sorted(v["version"] for v in versions) == [1, 2]
    assert all(v["mode"] == "monitor" and v["created_at"] for v in versions)
    one = admin.policy_set_get_versions("1")
    assert one["version"] == 1 and one["modules"] == v1["modules"]
    assert error_of(lambda: admin.policy_set_get_versions("99")).status_code == 404

    # Rollback stores version 1's files as version 3 and keeps the mode.
    rolled = admin.policy_set_versions_rollback("1", if_match="2")
    assert (rolled["version"], rolled["mode"], rolled["modules"]) == (3, "monitor", v1["modules"])
    assert error_of(lambda: admin.policy_set_versions_rollback("1", if_match="2")).status_code == 412
    # Without a candidate set, dry-run evaluates the active one.
    active = admin.policy_set_dry_run({"input": cancel_input})
    assert active["decision"] == "allow" and active["version"] == 3


def test_policy_set_writes() -> None:
    """The two write answers a normal call cannot reach: no If-Match is 428, an unchanged PUT adds no version.

    Each org may write 5 times in a burst (a write earns back one token every 6
    seconds, and the budget is checked before If-Match), and test_policy_set
    spends its own, so this runs on a second tenant.
    """
    server = os.environ["IRONFLOW_TEST_SERVER"]
    operator = IronflowClient(server, api_key=os.environ["IRONFLOW_TEST_API_KEY"])
    second = operator.tenants_provision({"org_name": f"Py policy writes {SUFFIX}"})
    client = IronflowClient(server, api_key=second["api_key"]["key"])
    body = {"modules": {"authz.rego": DENY_DELETE}, "mode": "monitor"}

    assert client.policy_set_update(body, if_match="0")["version"] == 1
    assert client.policy_set_update(body, if_match="1")["version"] == 1
    assert len(client.policy_set_list_versions()["versions"]) == 1

    # The generated method requires If-Match, so send the bare request.
    missing = error_of(lambda: client.request("PUT", "/api/v1/policy-set", body=body))
    assert missing.status_code == 428 and missing.code == "policy_set_precondition_required"
