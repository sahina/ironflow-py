"""project-crud and env-crud (#3003) on a tenant of its own.

`make test-identity-live` sets IRONFLOW_TEST_PLATFORM_TOKEN; without it this module skips.
"""

from __future__ import annotations

import os
import time
from typing import Any

import pytest

from ironflow import IronflowClient, IronflowError, IronflowRPC, IronflowRPCError
from ironflow.rpc.v1 import GetEnvironmentRequest

pytestmark = pytest.mark.skipif(
    not os.environ.get("IRONFLOW_TEST_PLATFORM_TOKEN"),
    reason="IRONFLOW_TEST_PLATFORM_TOKEN not set; run `make test-identity-live`",
)

SUFFIX = str(time.time_ns())
NAME = f"svc-{SUFFIX}"
STATE: dict[str, Any] = {}


def status_of(call: Any) -> int:
    with pytest.raises(IronflowError) as exc:
        call()
    return int(exc.value.status_code)


@pytest.fixture(scope="module")
def tenant() -> dict[str, Any]:
    operator = IronflowClient(os.environ["IRONFLOW_TEST_SERVER"], api_key=os.environ["IRONFLOW_TEST_API_KEY"])
    return dict(operator.tenants_provision({"org_name": f"Py projects {SUFFIX}"}))


@pytest.fixture(scope="module")
def client(tenant: dict[str, Any]) -> IronflowClient:
    return IronflowClient(os.environ["IRONFLOW_TEST_SERVER"], api_key=tenant["api_key"]["key"])


def test_project_crud(client: IronflowClient, tenant: dict[str, Any]) -> None:
    project = client.projects_create({"name": NAME, "description": "created"})
    assert project["name"] == NAME and project["description"] == "created" and project["org_id"] == tenant["org"]["id"]
    assert project["id"] and project["created_at"]
    assert sorted(p["name"] for p in client.projects_list()) == sorted(["default", NAME])
    assert next(p for p in client.projects_list() if p["id"] == project["id"])["description"] == "created"

    renamed = f"renamed-{SUFFIX}"
    updated = client.projects_update(project["id"], {"name": renamed, "description": "updated"})
    assert updated["id"] == project["id"] and updated["name"] == renamed and updated["description"] == "updated"
    listed = next(p for p in client.projects_list() if p["id"] == project["id"])
    assert listed["name"] == renamed and listed["description"] == "updated"
    # Omitting the description keeps it; an empty string clears it.
    assert client.projects_update(project["id"], {"name": renamed})["description"] == "updated"
    assert client.projects_update(project["id"], {"description": ""})["description"] == ""
    assert next(p for p in client.projects_list() if p["id"] == project["id"])["description"] == ""
    assert status_of(lambda: client.projects_update("proj_does_not_exist", {"name": "x"})) == 404
    assert status_of(lambda: client.projects_create({"name": ""})) == 400
    STATE["project"] = project


def test_env_crud(client: IronflowClient, tenant: dict[str, Any]) -> None:
    project = STATE["project"]
    env = client.environments_create({"name": "staging", "project_id": project["id"]})
    assert env["name"] == "staging" and env["project_id"] == project["id"] and env["id"]
    assert status_of(lambda: client.environments_create({"name": "Not Valid!", "project_id": project["id"]})) == 400
    envs = client.environments_list()
    assert next(e for e in envs if e["id"] == env["id"])["project_id"] == project["id"]
    assert tenant["environment"]["id"] in [e["id"] for e in envs]

    updated = client.environments_update(env["id"], {"name": "qa"})
    assert (updated["id"], updated["name"], updated["project_id"]) == (env["id"], "qa", project["id"])

    # The Connect service sees the same environment.
    server_url = os.environ["IRONFLOW_TEST_SERVER"]
    with IronflowRPC(server_url=server_url, api_key=tenant["api_key"]["key"]) as rpc:
        got = rpc.environments.get(GetEnvironmentRequest(id=env["id"]))
    assert got.id == env["id"] and got.name == "qa"

    client.environments_delete(env["id"])
    assert env["id"] not in [e["id"] for e in client.environments_list()]
    assert status_of(lambda: client.environments_delete(env["id"])) == 404


def test_env_boundaries(client: IronflowClient, tenant: dict[str, Any]) -> None:
    """The default environment stays, and another org gets a 404, the same as for one that does not exist.

    Of the Connect EnvironmentService, the Python SDK wraps only GetEnvironment and RotateAPIKey.
    """
    server = os.environ["IRONFLOW_TEST_SERVER"]
    with pytest.raises(IronflowError) as default:
        client.environments_delete("env_default")
    assert default.value.status_code == 403 and "cannot delete default environment" in str(default.value)

    env = client.environments_create({"name": "owned", "project_id": STATE["project"]["id"]})
    operator = IronflowClient(server, api_key=os.environ["IRONFLOW_TEST_API_KEY"])
    other = operator.tenants_provision({"org_name": f"Py projects other {SUFFIX}"})
    stranger = IronflowClient(server, api_key=other["api_key"]["key"])
    assert status_of(lambda: stranger.environments_update(env["id"], {"name": "stolen"})) == 404
    assert status_of(lambda: stranger.environments_delete(env["id"])) == 404
    with IronflowRPC(server_url=server, api_key=other["api_key"]["key"]) as rpc, pytest.raises(IronflowRPCError) as miss:
        rpc.environments.get(GetEnvironmentRequest(id=env["id"]))
    assert "not found" in str(miss.value)
    with IronflowRPC(server_url=server, api_key=tenant["api_key"]["key"]) as rpc:
        assert rpc.environments.get(GetEnvironmentRequest(id=env["id"])).id == env["id"]
    client.environments_delete(env["id"])


def test_project_delete(client: IronflowClient) -> None:
    project = STATE["project"]
    child = client.environments_create({"name": "child", "project_id": project["id"]})
    client.projects_delete(project["id"])
    assert project["id"] not in [p["id"] for p in client.projects_list()]
    assert child["id"] not in [e["id"] for e in client.environments_list()]
    assert status_of(lambda: client.projects_delete(project["id"])) == 404
