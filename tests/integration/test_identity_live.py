"""The identity and tenancy rows against a real engine (#2945).

`make test-identity-live` starts a server, creates a platform admin and sets
IRONFLOW_TEST_PLATFORM_TOKEN; without it this module skips.
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request

import pytest

from ironflow import IronflowClient, IronflowError, IronflowRPC
from ironflow.rpc.v1 import RotateAPIKeyRequest

PLATFORM_TOKEN = os.environ.get("IRONFLOW_TEST_PLATFORM_TOKEN")
pytestmark = pytest.mark.skipif(not PLATFORM_TOKEN, reason="IRONFLOW_TEST_PLATFORM_TOKEN not set; run `make test-identity-live`")

SUFFIX = str(time.time_ns())
ORG_NAME = f"Py Zürich {SUFFIX}"


def login(server_url: str, email: str, password: str) -> tuple[int, str]:
    req = urllib.request.Request(
        f"{server_url}/api/v1/auth/login",
        data=json.dumps({"email": email, "password": password}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.load(resp)["token"]
    except urllib.error.HTTPError as e:
        return e.code, ""


@pytest.fixture(scope="module")
def tenant() -> dict:
    """tenant-provision: the bootstrap org's admin provisions a tenant, once for the module."""
    server_url, api_key = os.environ["IRONFLOW_TEST_SERVER"], os.environ["IRONFLOW_TEST_API_KEY"]
    result = IronflowClient(server_url, api_key=api_key).tenants_provision({"org_name": ORG_NAME})
    assert result["org"]["name"] == ORG_NAME
    assert result["environment"]["name"] == "production"
    assert result["api_key"]["key"].startswith("ifkey_")
    assert result["api_key"]["roles"] == ["admin"]
    return dict(result)


def test_tenant_provision(server_url: str, tenant: dict) -> None:
    own = IronflowClient(server_url, api_key=tenant["api_key"]["key"])
    assert own.orgs_get(tenant["org"]["id"])["name"] == ORG_NAME
    for call in (lambda: own.tenants_provision({"org_name": f"Escalate {SUFFIX}"}), own.tenants_list):
        with pytest.raises(IronflowError) as err:
            call()
        assert err.value.status_code == 403


def test_tenant_list(server_url: str, api_key: str, tenant: dict) -> None:
    rows = [t for t in IronflowClient(server_url, api_key=api_key).tenants_list() if t["id"] == tenant["org"]["id"]]
    assert len(rows) == 1
    row = rows[0]
    assert (row["name"], row["env_count"], row["key_count"]) == (ORG_NAME, 1, 1)
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", row["created_at"])


def test_env_rotate_apikey(server_url: str, tenant: dict) -> None:
    old_key = tenant["api_key"]["key"]
    env_id = tenant["environment"]["id"]
    with IronflowRPC(server_url=server_url, api_key=old_key, timeout=30.0) as rpc:
        rotated = rpc.environments.rotate_api_key(RotateAPIKeyRequest(id=env_id))
    assert (rotated.id, rotated.name) == (env_id, "production")
    assert rotated.api_key.startswith("ifkey_") and rotated.api_key != old_key
    with pytest.raises(IronflowError) as err:
        IronflowClient(server_url, api_key=old_key).orgs_get(tenant["org"]["id"])
    assert err.value.status_code == 401
    assert IronflowClient(server_url, api_key=rotated.api_key).orgs_get(tenant["org"]["id"])["id"] == tenant["org"]["id"]
    tenant["api_key"]["key"] = rotated.api_key


def test_user_crud(server_url: str, tenant: dict) -> None:
    client = IronflowClient(server_url, api_key=tenant["api_key"]["key"])
    email = f"py-user-{SUFFIX}@example.com"
    user = client.users_create({"email": email, "name": "Zoë Py", "password": "first-pass-123", "roles": ["viewer"]})
    assert (user["org_id"], user["email"], user["name"], user["roles"]) == (tenant["org"]["id"], email, "Zoë Py", ["viewer"])
    assert user["id"] in [u["id"] for u in client.users_list()]
    updated = client.users_patch(user["id"], {"name": "Zoë Renamed", "roles": ["developer"]})
    assert (updated["name"], updated["roles"]) == ("Zoë Renamed", ["developer"])
    fetched = client.users_get(user["id"])
    assert (fetched["name"], fetched["roles"]) == ("Zoë Renamed", ["developer"])
    client.users_delete(user["id"])
    with pytest.raises(IronflowError) as err:
        client.users_get(user["id"])
    assert err.value.status_code == 404


def test_user_password(server_url: str, api_key: str) -> None:
    # In the bootstrap org: a session defaults to env_default, and the Python
    # REST client cannot name another environment.
    email = f"py-pw-{SUFFIX}@example.com"
    user = IronflowClient(server_url, api_key=api_key).users_create({"email": email, "password": "old-pass-123", "roles": ["developer"]})
    status, session = login(server_url, email, "old-pass-123")
    assert status == 200
    me = IronflowClient(server_url, api_key=session)
    with pytest.raises(IronflowError) as err:
        me.users_patch_password(user["id"], {"current_password": "wrong", "new_password": "x-pass-789"})
    assert err.value.status_code == 401
    me.users_patch_password(user["id"], {"current_password": "old-pass-123", "new_password": "new-pass-456"})
    assert login(server_url, email, "old-pass-123")[0] == 401
    assert login(server_url, email, "new-pass-456")[0] == 200


def test_org_crud(server_url: str, tenant: dict) -> None:
    created = IronflowClient(server_url, api_key=PLATFORM_TOKEN).orgs_create_orgs({"name": f"Py Plattform Ünïcode {SUFFIX}"})
    assert created["id"].startswith("org_")
    assert created["name"] == f"Py Plattform Ünïcode {SUFFIX}"

    own = IronflowClient(server_url, api_key=tenant["api_key"]["key"])
    with pytest.raises(IronflowError) as err:
        own.orgs_create({"name": "Escalate"})
    assert err.value.status_code == 403
    org_id = tenant["org"]["id"]
    assert [o["id"] for o in own.orgs_list()] == [org_id]
    renamed = f"Py Zürich renamed {SUFFIX}"
    assert own.orgs_patch(org_id, {"name": renamed})["name"] == renamed
    assert own.orgs_get(org_id)["name"] == renamed
    own.orgs_delete(org_id)
    # Deleting the org deletes its keys.
    with pytest.raises(IronflowError) as err:
        own.orgs_get(org_id)
    assert err.value.status_code == 401
