"""secret-crud (#3003) on a tenant of its own, so the key is bound to an environment other than env_default.

The REST docs say a secret's value is write-only, so no response carries it.
Python names the environment itself, so this sends the tenant's real environment ID.
`make test-identity-live` sets IRONFLOW_TEST_PLATFORM_TOKEN; without it this module skips.
"""

from __future__ import annotations

import os
import time

import pytest

from ironflow import IronflowClient, IronflowError

pytestmark = pytest.mark.skipif(
    not os.environ.get("IRONFLOW_TEST_PLATFORM_TOKEN"),
    reason="IRONFLOW_TEST_PLATFORM_TOKEN not set; run `make test-identity-live`",
)

SUFFIX = str(time.time_ns())


def test_secret_crud() -> None:
    server = os.environ["IRONFLOW_TEST_SERVER"]
    operator = IronflowClient(server, api_key=os.environ["IRONFLOW_TEST_API_KEY"])
    tenant = operator.tenants_provision({"org_name": f"Py secrets {SUFFIX}"})
    client = IronflowClient(server, api_key=tenant["api_key"]["key"])
    env = tenant["environment"]["id"]
    name = f"PY_KEY_{SUFFIX}"

    created = client.secrets_create({"name": name, "value": "v1-sensitive", "description": "billing"}, x_ironflow_environment_id=env)
    assert (created["name"], created["description"]) == (name, "billing")
    assert created["created_at"] and created["updated_at"] and created["revision"] > 0
    assert "value" not in created
    got = client.secrets_get(name, x_ironflow_environment_id=env)
    assert (got["name"], got["description"], got["revision"]) == (name, "billing", created["revision"])
    assert "value" not in got

    updated = client.secrets_update(name, {"value": "v2-sensitive", "description": "rotated"}, x_ironflow_environment_id=env)
    assert updated["description"] == "rotated" and updated["revision"] > created["revision"]
    assert "value" not in updated

    renamed = f"PY_RENAMED_{SUFFIX}"
    patched = client.secrets_patch(name, {"name": renamed, "description": "moved"}, x_ironflow_environment_id=env)
    assert (patched["name"], patched["description"]) == (renamed, "moved")
    with pytest.raises(IronflowError) as old:
        client.secrets_get(name, x_ironflow_environment_id=env)
    assert old.value.status_code == 404

    listed = client.secrets_list(x_ironflow_environment_id=env)
    assert [s["name"] for s in listed] == [renamed]
    assert listed[0]["description"] == "moved" and "value" not in listed[0]

    client.secrets_delete(renamed, x_ironflow_environment_id=env)
    with pytest.raises(IronflowError) as gone:
        client.secrets_get(renamed, x_ironflow_environment_id=env)
    assert gone.value.status_code == 404
    assert client.secrets_list(x_ironflow_environment_id=env) == []
