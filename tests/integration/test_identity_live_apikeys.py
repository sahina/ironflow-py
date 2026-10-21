"""apikey-crud and apikey-rotate (#3003) on a tenant of its own.

`make test-identity-live` sets IRONFLOW_TEST_PLATFORM_TOKEN; without it this module skips.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from ironflow import IronflowClient, IronflowError

pytestmark = pytest.mark.skipif(
    not os.environ.get("IRONFLOW_TEST_PLATFORM_TOKEN"),
    reason="IRONFLOW_TEST_PLATFORM_TOKEN not set; run `make test-identity-live`",
)

SUFFIX = str(time.time_ns())
NAME = f"py-ci-{SUFFIX}"
STATE: dict[str, Any] = {}


def _instant(stamp: str) -> datetime:
    return datetime.fromisoformat(stamp.replace("Z", "+00:00"))


def status_of(call: Any) -> int:
    with pytest.raises(IronflowError) as exc:
        call()
    return int(exc.value.status_code)


@pytest.fixture(scope="module")
def tenant() -> dict[str, Any]:
    operator = IronflowClient(os.environ["IRONFLOW_TEST_SERVER"], api_key=os.environ["IRONFLOW_TEST_API_KEY"])
    return dict(operator.tenants_provision({"org_name": f"Py apikeys {SUFFIX}"}))


def as_key(key: str) -> IronflowClient:
    return IronflowClient(os.environ["IRONFLOW_TEST_SERVER"], api_key=key)


def test_apikey_crud(tenant: dict[str, Any]) -> None:
    admin = as_key(tenant["api_key"]["key"])
    org_id = tenant["org"]["id"]
    viewer = next(r["id"] for r in admin.roles_list() if r["name"] == "viewer")

    created = admin.api_keys_create({"name": NAME, "role_ids": [viewer], "expires_in": "720h"})
    assert created["key"].startswith("ifkey_") and created["key"].startswith(created["key_prefix"])
    assert created["name"] == NAME and created["role_ids"] == [viewer] and created["id"]
    days = (_instant(created["expires_at"]) - datetime.now(timezone.utc)).days
    assert 29 <= days <= 30

    got = admin.api_keys_get(created["id"])
    assert (got["id"], got["name"], got["key_prefix"], got["role_ids"]) == (created["id"], NAME, created["key_prefix"], [viewer])
    assert created["id"] in [k["id"] for k in admin.api_keys_list()]
    # The viewer role is what the key holds: it reads, but cannot create keys.
    assert as_key(created["key"]).orgs_get(org_id)["id"] == org_id
    assert status_of(lambda: as_key(created["key"]).api_keys_create({"name": f"denied-{SUFFIX}"})) == 403
    assert status_of(lambda: admin.api_keys_get("ak_does_not_exist")) == 404
    assert status_of(lambda: admin.api_keys_create({"name": ""})) == 400
    STATE["created"] = created


def test_apikey_rotate(tenant: dict[str, Any]) -> None:
    admin = as_key(tenant["api_key"]["key"])
    org_id, created = tenant["org"]["id"], STATE["created"]

    rotated = admin.api_keys_rotate(created["id"])
    assert rotated["id"] != created["id"] and rotated["key"] != created["key"]
    assert rotated["key"].startswith("ifkey_") and rotated["key"].startswith(rotated["key_prefix"])
    # Rotation keeps the name, the roles and the expiry.
    assert (rotated["name"], rotated["role_ids"]) == (NAME, created["role_ids"])
    # Compare the instants, not the strings: the two backends format the stored stamp differently.
    assert abs(_instant(rotated["expires_at"]) - _instant(created["expires_at"])) < timedelta(seconds=1)

    assert status_of(lambda: admin.api_keys_get(created["id"])) == 404
    assert status_of(lambda: as_key(created["key"]).orgs_get(org_id)) == 401
    assert as_key(rotated["key"]).orgs_get(org_id)["id"] == org_id
    assert status_of(lambda: admin.api_keys_rotate("ak_does_not_exist")) == 404
    STATE["rotated"] = rotated


def test_apikey_environment_and_tool_scopes(tenant: dict[str, Any]) -> None:
    """A key bound to a second environment with tool scopes: both show on read and list, rotation keeps them."""
    admin = as_key(tenant["api_key"]["key"])
    org_id = tenant["org"]["id"]
    staging = admin.environments_create({"name": "staging", "project_id": admin.projects_list()[0]["id"]})
    scopes = ["billing-tools", "reporting-tools"]

    def bound(key: dict[str, Any]) -> bool:
        return bool(key["environment_id"] == staging["id"] and key["tool_scopes"] == scopes)

    def org_in(key: str, env: str) -> Any:
        """The client has no environment setting, so name the environment in a header."""
        return as_key(key).request("GET", f"/api/v1/orgs/{org_id}", headers={"X-Ironflow-Environment": env})

    scoped = admin.api_keys_create({"name": f"scoped-{SUFFIX}", "env_id": staging["id"], "tool_scopes": scopes})
    assert bound(scoped) and bound(admin.api_keys_get(scoped["id"]))
    assert any(k["id"] == scoped["id"] and bound(k) for k in admin.api_keys_list())
    assert org_in(scoped["key"], staging["id"])["id"] == org_id
    assert status_of(lambda: org_in(scoped["key"], tenant["environment"]["id"])) == 403

    rotated = admin.api_keys_rotate(scoped["id"])
    assert rotated["id"] != scoped["id"] and bound(rotated) and bound(admin.api_keys_get(rotated["id"]))
    assert org_in(rotated["key"], staging["id"])["id"] == org_id
    assert status_of(lambda: org_in(rotated["key"], tenant["environment"]["id"])) == 403
    admin.api_keys_delete(rotated["id"])


def test_apikey_delete(tenant: dict[str, Any]) -> None:
    admin = as_key(tenant["api_key"]["key"])
    rotated = STATE["rotated"]

    admin.api_keys_delete(rotated["id"])
    assert status_of(lambda: admin.api_keys_get(rotated["id"])) == 404
    assert status_of(lambda: as_key(rotated["key"]).orgs_get(tenant["org"]["id"])) == 401
