"""KV and config watch against a real engine (#2949).

`make test-watch-live` (and `make test-python-integration`) start a server and set
IRONFLOW_TEST_SERVER; without it conftest skips this module.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timedelta, timezone
from typing import Any, TypeVar

import pytest

from ironflow import IronflowClient, IronflowError

IDLE_GAP = 1.5  # outlasts any read deadline a watcher might poll with
T = TypeVar("T")


@pytest.fixture
def client(server_url: str, api_key: str) -> IronflowClient:
    return IronflowClient(server_url, api_key=api_key)


def unique(prefix: str) -> str:
    return f"{prefix}{time.time_ns()}"


async def take(gen: AsyncIterator[T], steps: list[Callable[[], Any] | None]) -> list[T]:
    """Run each write in a thread, then read one event; None reads without writing."""
    out: list[T] = []
    try:
        for step in steps:
            if step is not None:
                await asyncio.to_thread(step)
            out.append(await asyncio.wait_for(gen.__anext__(), 10))
    finally:
        await gen.aclose()  # type: ignore[attr-defined]
    return out


def test_kv_watch_replay_put_delete(client: IronflowClient) -> None:
    bucket = unique("pywatch")
    client.kv_buckets({"name": bucket})
    pre = client.kv_update_buckets_keys(bucket, "pre", {"v": 0})

    async def scenario() -> list[Any]:
        gen = client.watch_kv(bucket)
        replay = await asyncio.wait_for(gen.__anext__(), 10)
        await asyncio.sleep(IDLE_GAP)
        rest = await take(gen, [
            lambda: client.kv_update_buckets_keys(bucket, "user.a", {"v": 1, "s": "é"}),
            lambda: client.kv_delete_buckets_keys(bucket, "user.a"),
        ])
        return [replay, *rest]

    replay, put, delete = asyncio.run(scenario())
    assert (replay.key, replay.operation, replay.revision, replay.bucket) == ("pre", "put", pre["revision"], bucket)
    assert replay.value is not None and json.loads(replay.value) == {"v": 0}
    assert (put.key, put.operation) == ("user.a", "put")
    assert put.value is not None and json.loads(put.value) == {"v": 1, "s": "é"}
    assert (delete.key, delete.operation, delete.value) == ("user.a", "delete", None)
    assert delete.revision > put.revision


def test_kv_watch_key_filter(client: IronflowClient) -> None:
    bucket = unique("pywatchf")
    client.kv_buckets({"name": bucket})

    async def scenario() -> list[Any]:
        gen = client.watch_kv(bucket, key="user.*")
        await asyncio.sleep(0.5)  # let the socket subscribe before writing
        await asyncio.to_thread(client.kv_update_buckets_keys, bucket, "other.x", 1)
        return await take(gen, [lambda: client.kv_update_buckets_keys(bucket, "user.b", 2)])

    (got,) = asyncio.run(scenario())
    assert (got.key, got.value) == ("user.b", b"2")


def test_kv_watch_missing_bucket_raises(client: IronflowClient) -> None:
    async def scenario() -> None:
        async for _ in client.watch_kv(unique("pynosuch")):
            pass

    with pytest.raises(IronflowError, match="bucket not found"):
        asyncio.run(scenario())


def test_watch_bad_key_fails_handshake(server_url: str) -> None:
    async def scenario() -> None:
        async for _ in IronflowClient(server_url, api_key="ifkey_bogus").watch_config("any"):
            pass

    with pytest.raises(IronflowError) as exc:
        asyncio.run(scenario())
    assert exc.value.status_code == 401


def test_config_watch_replay_set_patch_delete(client: IronflowClient) -> None:
    name = unique("pywatchcfg")
    pre = client.config_create(name, {"a": 1})

    async def scenario() -> list[Any]:
        gen = client.watch_config(name)
        replay = await asyncio.wait_for(gen.__anext__(), 10)
        await asyncio.sleep(IDLE_GAP)
        rest = await take(gen, [
            lambda: client.config_create(name, {"a": 2, "s": "é"}),
            lambda: client.config_patch(name, {"b": True}),
            lambda: client.config_delete(name),
        ])
        return [replay, *rest]

    replay, set_, patch, delete = asyncio.run(scenario())
    assert (replay.name, replay.data, replay.revision) == (name, {"a": 1}, pre["revision"])
    updated = datetime.fromisoformat(replay.updated_at.replace("Z", "+00:00"))
    assert updated.utcoffset() == timedelta(0)
    assert datetime.now(timezone.utc) - updated < timedelta(minutes=1)
    assert set_.data == {"a": 2, "s": "é"}
    assert patch.data == {"a": 2, "s": "é", "b": True}
    assert delete.data is None and delete.revision > patch.revision
