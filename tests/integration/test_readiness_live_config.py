"""The config group (#3003) through the Python client.

The writes are set-up for the reads; config-write is proven by
test_watch_live.py. Skips without IRONFLOW_READINESS_SEED;
`make test-readiness-live` sets it.
"""

from __future__ import annotations

import time

import pytest

from ironflow import IronflowClient, IronflowError

pytestmark = pytest.mark.usefixtures("readiness_seed")


@pytest.fixture
def client(server_url: str, api_key: str) -> IronflowClient:
    return IronflowClient(server_url, api_key=api_key)


def test_config_read(client: IronflowClient) -> None:
    name = f"pycfg{time.time_ns()}"
    # The non-ASCII value proves the data survives as UTF-8.
    data = {"a": 1, "s": "José", "nested": {"k": [1, 2]}}
    revision = client.config_create(name, data)["revision"]
    try:
        got = client.config_get(name)
        assert (got["name"], got["data"], got["revision"]) == (name, data, revision)
        assert got["updatedAt"]

        # List gives the name, revision and time, not the data.
        def find() -> dict[str, object] | None:
            return next((dict(e) for e in client.config_list()["configs"] if e["name"] == name), None)

        entry = find()
        assert entry is not None
        assert (entry["name"], entry["revision"]) == (name, revision)
        assert entry["updatedAt"]
        assert "data" not in entry

        patched = client.config_patch(name, {"b": True})["revision"]
        assert patched > revision
        got = client.config_get(name)
        assert (got["data"], got["revision"]) == ({**data, "b": True}, patched)
        latest = find()
        assert latest is not None and latest["revision"] == patched
    finally:
        client.config_delete(name)
    with pytest.raises(IronflowError) as exc:
        client.config_get(name)
    assert exc.value.status_code == 404
    assert find() is None
