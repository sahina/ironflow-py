"""Platform, API-key roles, cluster ops and the OpenAPI route against a real engine (#2947).

`make test-platform-live` starts a fresh server and sets IRONFLOW_PLATFORM_JWT_FILE
to the secret file the server keeps next to its database. Without it this module
skips: it bootstraps the one platform admin a server allows and rotates the JWT
secret, so it must never share the server `make test-python-integration` uses.

The tests run in file order: bootstrap first, the JWT rotation last.
"""

from __future__ import annotations

import base64
import json
import os
import secrets
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pytest

from ironflow import IronflowClient, IronflowError

JWT_FILE = os.environ.get("IRONFLOW_PLATFORM_JWT_FILE")
pytestmark = pytest.mark.skipif(not JWT_FILE, reason="IRONFLOW_PLATFORM_JWT_FILE not set; run `make test-platform-live`")

REPO_SPEC = Path(__file__).resolve().parents[4] / "api" / "openapi.json"
OP_EMAIL = "operator@example.com"
OP_PASSWORD = "correct-horse-battery-staple"
STATE: dict[str, Any] = {}


def unique(prefix: str) -> str:
    return f"{prefix}{time.time_ns()}"


def raw(server_url: str, method: str, path: str, token: str | None = None, body: Any = None) -> tuple[int, Any, bytes]:
    """Status, headers and bytes, for what the generated client hides (cookies, raw spec, routes it has no method for)."""
    req = urllib.request.Request(server_url + path, method=method, data=None if body is None else json.dumps(body).encode())
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.headers, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.headers, e.read()


def status_of(call: Any) -> int:
    with pytest.raises(IronflowError) as exc:
        call()
    return exc.value.status_code


def login(server_url: str, password: str = OP_PASSWORD) -> str:
    resp = IronflowClient(server_url).platform_auth_login({"email": OP_EMAIL, "password": password})
    return resp["token"]


@pytest.fixture
def admin(server_url: str, api_key: str) -> IronflowClient:
    return IronflowClient(server_url, api_key=api_key)


def own_key_id(admin: IronflowClient) -> str:
    assert admin.api_key
    return next(k["id"] for k in admin.api_keys_list() if admin.api_key.startswith(k["key_prefix"]))


def role_id(admin: IronflowClient, name: str) -> str:
    return next(r["id"] for r in admin.roles_list() if r["name"] == name)


def test_platform_auth(server_url: str, admin: IronflowClient) -> None:
    created = admin.platform_bootstrap({"email": OP_EMAIL, "password": OP_PASSWORD, "name": "Operator"})
    assert created["email"] == OP_EMAIL and created["name"] == "Operator" and created["id"]
    STATE["operator_id"] = created["id"]

    assert status_of(lambda: admin.platform_bootstrap({"email": "b@example.com", "password": OP_PASSWORD, "name": "B"})) == 409

    resp = IronflowClient(server_url).platform_auth_login({"email": OP_EMAIL, "password": OP_PASSWORD})
    assert resp["user"]["email"] == OP_EMAIL and "platform_admin" in resp["user"]["roles"]
    platform = IronflowClient(server_url, api_key=resp["token"])
    assert created["id"] in [u["id"] for u in platform.platform_users_list()]

    status, headers, _ = raw(server_url, "POST", "/api/v1/platform/auth/login", body={"email": OP_EMAIL, "password": OP_PASSWORD})
    assert status == 200 and "HttpOnly" in headers.get("Set-Cookie", "")

    assert status_of(lambda: IronflowClient(server_url).platform_auth_login({"email": OP_EMAIL, "password": "wrong-password"})) == 401
    assert status_of(lambda: IronflowClient(server_url).platform_auth_login({"email": "nobody@example.com", "password": OP_PASSWORD})) == 401


def test_platform_admin(server_url: str) -> None:
    p = IronflowClient(server_url, api_key=login(server_url))

    # The server prefixes platform role names with "platform_".
    role = p.platform_roles_create({"name": unique("auditors-")})
    assert role["name"].startswith("platform_auditors-")
    assert p.platform_roles_get(role["id"])["name"] == role["name"]
    renamed = "platform_" + unique("reviewers-")
    assert p.platform_roles_update(role["id"], {"name": renamed[len("platform_") :]})["name"] == renamed
    assert p.platform_roles_get(role["id"])["name"] == renamed
    assert role["id"] in [r["id"] for r in p.platform_roles_list()]

    email = unique("user-") + "@example.com"
    user = p.platform_users_create({"email": email, "name": "Uma", "password": OP_PASSWORD, "role_ids": [role["id"]]})
    assert p.platform_users_get(user["id"])["email"] == email
    updated = p.platform_users_update(user["id"], {"name": "Uma Two", "is_active": False})
    assert updated["name"] == "Uma Two" and updated["is_active"] is False
    got = p.platform_users_get(user["id"])
    assert got["name"] == "Uma Two" and got["is_active"] is False
    assert user["id"] in [u["id"] for u in p.platform_users_list()]
    p.platform_users_delete(user["id"])
    assert status_of(lambda: p.platform_users_get(user["id"])) == 404

    p.platform_roles_delete(role["id"])
    assert status_of(lambda: p.platform_roles_get(role["id"])) == 404

    tenant = p.platform_tenants_create({"name": unique("acme-")})
    assert p.platform_tenants_get(tenant["id"])["name"] == tenant["name"]
    assert tenant["id"] in [t["id"] for t in p.platform_tenants_list()]
    p.platform_tenants_delete(tenant["id"])
    assert status_of(lambda: p.platform_tenants_get(tenant["id"])) == 404

    org = p.orgs_create_orgs({"name": unique("globex-")})
    assert org["id"] and p.platform_tenants_get(org["id"])["name"] == org["name"]

    # Lifecycle audit rows are written asynchronously.
    want = {("platform.user.created", user["id"]), ("platform.user.updated", user["id"]), ("platform.user.deleted", user["id"])}
    deadline = time.monotonic() + 10
    while True:
        events = p.platform_audit_list(limit=100)["events"]
        seen = {(e["event_type"], (e["payload"] or {}).get("user_id")) for e in events}
        if want <= seen or time.monotonic() > deadline:
            break
        time.sleep(0.2)
    assert want <= seen, f"missing audit rows: {want - seen}"
    assert all(e["scope"] == "platform" for e in events)


def test_apikey_roles(server_url: str, admin: IronflowClient) -> None:
    viewer, admin_role = role_id(admin, "viewer"), role_id(admin, "admin")
    key = admin.api_keys_create({"name": unique("ci-"), "role_ids": [viewer]})
    as_key = IronflowClient(server_url, api_key=key["key"])
    assert status_of(lambda: as_key.api_keys_create({"name": unique("denied-")})) == 403

    replaced = admin.api_keys_update_roles(key["id"], {"role_ids": [admin_role]})
    assert replaced["role_ids"] == [admin_role]
    assert admin.api_keys_get(key["id"])["role_ids"] == [admin_role]
    assert as_key.api_keys_create({"name": unique("allowed-")})["id"]

    assert status_of(lambda: admin.api_keys_update_roles(key["id"], {"role_ids": ["role_does_not_exist"]})) == 400
    assert status_of(lambda: admin.api_keys_update_roles(key["id"], {"role_ids": []})) == 400
    assert status_of(lambda: admin.api_keys_update_roles("key_does_not_exist", {"role_ids": [viewer]})) == 404
    own_id = own_key_id(admin)
    assert status_of(lambda: admin.api_keys_update_roles(own_id, {"role_ids": [viewer]})) == 403
    assert admin.api_keys_get(key["id"])["role_ids"] == [admin_role]


def test_platform_policy_set(server_url: str, admin: IronflowClient) -> None:
    """platform-policy-set (#3003): the platform org's own set, and the break-glass for a tenant's."""
    deny_delete = 'package ironflow.authz\n\ndeny contains "no deletes" if input.request.action == "runs:delete"\n'
    deny_cancel = 'package ironflow.authz\n\ndeny contains "no cancels" if input.request.action == "runs:cancel"\n'
    v1 = {"modules": {"platform.rego": deny_delete}, "mode": "monitor"}
    v2 = {"modules": {"platform.rego": deny_delete, "more.rego": deny_cancel}, "mode": "monitor"}
    delete_input = {"request": {"action": "runs:delete"}, "subject": {}}
    cancel_input = {"request": {"action": "runs:cancel"}, "subject": {}}
    p = IronflowClient(server_url, api_key=login(server_url))

    empty = p.platform_policy_set_list()
    assert empty["version"] == 0 and empty["modules"] == {}

    # validate and dry-run store nothing.
    assert p.platform_policy_set_create_validate(v1)["valid"] is True
    assert status_of(lambda: p.platform_policy_set_create_validate({"modules": {"bad.rego": "package ironflow.authz\n\ndeny contains {\n"}})) == 400
    deny = p.platform_policy_set_create_dry_run({"set": v1, "input": delete_input})
    assert deny["decision"] == "deny" and "no deletes" in deny["reasons"]
    assert p.platform_policy_set_create_dry_run({"set": v1, "input": cancel_input})["decision"] == "allow"
    assert p.platform_policy_set_list()["version"] == 0

    tenant_version = admin.policy_set_list()["version"]
    put1 = p.platform_policy_set_update(v1, if_match="0")
    assert (put1["version"], put1["mode"], put1["modules"]) == (1, "monitor", v1["modules"])
    assert status_of(lambda: p.platform_policy_set_update(v2, if_match="0")) == 412
    put2 = p.platform_policy_set_update(v2, if_match="1")
    assert put2["version"] == 2 and put2["modules"] == v2["modules"]
    assert p.platform_policy_set_list()["modules"] == v2["modules"]

    versions = p.platform_policy_set_list_versions()["versions"]
    assert sorted(v["version"] for v in versions) == [1, 2]
    one = p.platform_policy_set_get("1")
    assert one["version"] == 1 and one["modules"] == v1["modules"]
    assert status_of(lambda: p.platform_policy_set_get("99")) == 404
    rolled = p.platform_policy_set_create_rollback("1", if_match="2")
    assert (rolled["version"], rolled["mode"], rolled["modules"]) == (3, "monitor", v1["modules"])
    active = p.platform_policy_set_create_dry_run({"input": cancel_input})
    assert active["decision"] == "allow" and active["version"] == 3
    # The platform set is not the tenant's.
    assert admin.policy_set_list()["version"] == tenant_version

    # Break-glass: an organization's enforce-mode set goes to monitor, as a new version.
    org = "org_default"
    enforced = admin.policy_set_update({"modules": {"tenant.rego": deny_delete}, "mode": "enforce"}, if_match=str(tenant_version))
    assert enforced["mode"] == "enforce"
    switched = p.platform_policy_set_create(org, {"reason": "platform-policy-set live test"})
    assert switched["mode"] == "monitor" and switched["version"] == enforced["version"] + 1
    assert switched["modules"] == enforced["modules"]
    assert admin.policy_set_list()["mode"] == "monitor"
    assert status_of(lambda: p.platform_policy_set_create(org, {})) == 400
    assert status_of(lambda: p.platform_policy_set_create("org_platform", {"reason": "x"})) == 400
    assert status_of(lambda: p.platform_policy_set_create("org_does_not_exist", {"reason": "x"})) == 404
    # A tenant admin key has no platform policy access.
    assert status_of(admin.platform_policy_set_list) == 403

    # The row also names the ifplatform_ key, not only the platform login token.
    platform_key = admin.api_keys_create({"name": unique("policy-"), "platform": True, "role_ids": ["role_platform_admin"]})
    assert platform_key["key"].startswith("ifplatform_")
    by_key = IronflowClient(server_url, api_key=platform_key["key"])
    assert by_key.platform_policy_set_list()["version"] == 3
    assert by_key.platform_policy_set_get("1")["modules"] == v1["modules"]
    dry = by_key.platform_policy_set_create_dry_run({"input": delete_input})
    assert dry["decision"] == "deny" and dry["version"] == 3
    assert by_key.platform_policy_set_create_validate(v1)["valid"] is True
    admin.api_keys_delete(platform_key["id"])
    assert status_of(by_key.platform_policy_set_list) == 401


def test_openapi_spec(server_url: str, api_key: str) -> None:
    status, headers, body = raw(server_url, "GET", "/api/v1/openapi.json", token=api_key)
    assert status == 200 and headers.get("Content-Type", "").startswith("application/json")
    assert body == REPO_SPEC.read_bytes()
    assert json.loads(body)["openapi"] == "3.1.0"
    assert raw(server_url, "GET", "/api/v1/openapi.json")[0] == 401


def test_cluster_health(server_url: str, api_key: str, admin: IronflowClient) -> None:
    status, _, body = raw(server_url, "GET", "/api/v1/cluster/health", token=api_key)
    assert status == 200
    h = json.loads(body)
    assert h["components"]["database"] == "healthy" and h["components"]["nats"] == "healthy"
    assert h["version"] and h["timestamp"]
    assert "disk" in h and "nats" in h, "the default-org admin sees the node-wide fields"

    viewer_key = admin.api_keys_create({"name": unique("hv-"), "role_ids": [role_id(admin, "viewer")]})["key"]
    status, _, body = raw(server_url, "GET", "/api/v1/cluster/health", token=viewer_key)
    v = json.loads(body)
    assert status == 200 and v["components"]["database"] == "healthy"
    assert "disk" not in v and "nats" not in v, "a viewer must not see node-wide fields"


@pytest.mark.parametrize("kind", ["engine_api_key", "founder_api_key"])
def test_cluster_rotate_api_key(server_url: str, admin: IronflowClient, kind: str) -> None:
    target = admin.api_keys_create({"name": unique("push-"), "role_ids": [role_id(admin, "admin")]})
    new_value = "ifkey_" + secrets.token_hex(24)
    idem = unique("rot-")
    resp = admin.cluster_rotate_token({"kind": kind, "new_value": new_value, "remote_key_id": target["id"]}, idempotency_key=idem)
    assert resp["status"] == "rotated" and resp["rotated_at"].endswith("Z")

    assert status_of(lambda: IronflowClient(server_url, api_key=target["key"]).api_keys_list()) == 401
    assert target["id"] in [k["id"] for k in IronflowClient(server_url, api_key=new_value).api_keys_list()]
    assert admin.api_keys_get(target["id"])["key_prefix"] == new_value[: len(target["key_prefix"])]

    # A replay returns the first response and changes nothing.
    other = "ifkey_" + secrets.token_hex(24)
    again = admin.cluster_rotate_token({"kind": kind, "new_value": other, "remote_key_id": target["id"]}, idempotency_key=idem)
    assert again == resp
    assert status_of(lambda: IronflowClient(server_url, api_key=other).api_keys_list()) == 401
    IronflowClient(server_url, api_key=new_value).api_keys_list()


def test_cluster_rotate_errors(admin: IronflowClient) -> None:
    own_id = own_key_id(admin)
    cases = [
        ({"kind": "bogus", "new_value": "x"}, 400),
        ({"kind": "engine_api_key", "new_value": ""}, 400),
        ({"kind": "engine_api_key", "new_value": "ifkey_x"}, 400),
        ({"kind": "engine_api_key", "new_value": "ifkey_x", "remote_key_id": own_id}, 400),
        ({"kind": "engine_api_key", "new_value": "ifkey_x", "remote_key_id": "key_does_not_exist"}, 404),
        ({"kind": "dashboard_jwt_secret", "new_value": "not base64!"}, 400),
        ({"kind": "dashboard_jwt_secret", "new_value": base64.b64encode(b"short").decode()}, 400),
    ]
    for body, want in cases:
        assert status_of(lambda b=body: admin.cluster_rotate_token(b)) == want, body


def test_cluster_rotate_jwt_secret(server_url: str, admin: IronflowClient) -> None:
    old_token = login(server_url)
    IronflowClient(server_url, api_key=old_token).platform_users_list()

    secret = secrets.token_bytes(32)
    resp = admin.cluster_rotate_token({"kind": "dashboard_jwt_secret", "new_value": base64.b64encode(secret).decode()})
    assert resp["status"] == "rotated"

    assert status_of(lambda: IronflowClient(server_url, api_key=old_token).platform_users_list()) == 401
    fresh = IronflowClient(server_url, api_key=login(server_url))
    assert STATE["operator_id"] in [u["id"] for u in fresh.platform_users_list()]

    assert JWT_FILE is not None
    assert Path(JWT_FILE).read_text().strip() == secret.hex(), "the rotated secret must survive a restart"
