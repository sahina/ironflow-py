"""K01 against a real server: the generated policy-set methods reach the real routes."""

from __future__ import annotations

from ironflow import IronflowClient

_DENY_DELETE = 'package ironflow.authz\n\ndeny contains "x" if input.request.action == "runs:delete"\n'


def test_policy_set_k01_live(server_url: str, api_key: str) -> None:
    client = IronflowClient(server_url, api_key=api_key)
    assert client.policy_set_list()["version"] >= 0

    assert client.policy_set_validate({"modules": {"a.rego": "package ironflow.authz\n"}})["valid"] is True

    dry = client.policy_set_dry_run(
        {
            "set": {"modules": {"a.rego": _DENY_DELETE}},
            "input": {"request": {"action": "runs:delete"}, "subject": {}},
        }
    )
    assert dry["decision"] == "deny"
