"""The kv group (#3003) through the Python client.

Each test makes its own bucket. Watch is in test_watch_live.py. Skips without
IRONFLOW_READINESS_SEED; `make test-readiness-live` sets it.
"""

from __future__ import annotations

import base64
import json
import time
from typing import Any

import pytest

from ironflow import IronflowClient, IronflowError, PreconditionFailedError

pytestmark = pytest.mark.usefixtures("readiness_seed")


@pytest.fixture
def client(server_url: str, api_key: str) -> IronflowClient:
    return IronflowClient(server_url, api_key=api_key)


def _value(entry: dict[str, Any]) -> Any:
    return json.loads(base64.b64decode(entry["value"]).decode("utf-8"))


def test_kv_buckets(client: IronflowClient) -> None:
    name = f"pykvb{time.time_ns()}"
    created = client.kv_buckets({"name": name, "description": "readiness", "ttl_seconds": 3600, "history": 3})
    try:
        assert {k: created[k] for k in ("name", "description", "ttl_seconds", "history", "values")} == {
            "name": name, "description": "readiness", "ttl_seconds": 3600, "history": 3, "values": 0}
        assert created["created_at"]

        client.kv_update_buckets_keys(name, "k", {"v": 1})
        info = client.kv_get_buckets(name)
        assert {k: info[k] for k in ("name", "description", "ttl_seconds", "history", "values")} == {
            "name": name, "description": "readiness", "ttl_seconds": 3600, "history": 3, "values": 1}
        assert info["bytes"] > 0
        assert info["created_at"] == created["created_at"]

        listed = [b for b in client.kv_list_buckets()["buckets"] if b["name"] == name]
        assert [b["values"] for b in listed] == [1]
    finally:
        client.kv_delete_buckets(name)
    with pytest.raises(IronflowError) as exc:
        client.kv_get_buckets(name)
    assert exc.value.status_code == 404
    assert name not in [b["name"] for b in client.kv_list_buckets()["buckets"]]


def test_kv_keys(client: IronflowClient) -> None:
    name = f"pykvk{time.time_ns()}"
    client.kv_buckets({"name": name})
    try:
        # The non-ASCII value proves the bytes survive the base64 round trip.
        revision = client.kv_update_buckets_keys(name, "user.a", {"who": "José"})["revision"]
        assert revision > 0
        entry = client.kv_get_buckets_keys(name, "user.a")
        assert (entry["key"], entry["revision"], entry["operation"]) == ("user.a", revision, "put")
        assert _value(entry) == {"who": "José"}

        # Create refuses an existing key; update is a compare-and-swap on the revision.
        with pytest.raises(PreconditionFailedError):
            client.kv_update_buckets_keys(name, "user.a", 1, if_none_match="*")
        nxt = client.kv_update_buckets_keys(name, "user.a", 2, if_match=str(revision))["revision"]
        assert nxt > revision
        with pytest.raises(PreconditionFailedError):
            client.kv_update_buckets_keys(name, "user.a", 3, if_match=str(revision))
        latest = client.kv_get_buckets_keys(name, "user.a")
        assert (_value(latest), latest["revision"]) == (2, nxt)

        client.kv_update_buckets_keys(name, "user.b", {}, if_none_match="*")
        client.kv_update_buckets_keys(name, "other.c", {}, if_none_match="*")
        assert sorted(client.kv_list_buckets_keys(name)["keys"]) == ["other.c", "user.a", "user.b"]
        assert sorted(client.kv_list_buckets_keys(name, filter="user.*")["keys"]) == ["user.a", "user.b"]

        # Delete leaves a tombstone: the key reads as missing and drops out of the list.
        client.kv_delete_buckets_keys(name, "user.a")
        with pytest.raises(IronflowError) as exc:
            client.kv_get_buckets_keys(name, "user.a")
        assert exc.value.status_code == 404
        client.kv_delete_buckets_keys(name, "user.b", purge=True)
        assert client.kv_list_buckets_keys(name)["keys"] == ["other.c"]
    finally:
        client.kv_delete_buckets(name)
