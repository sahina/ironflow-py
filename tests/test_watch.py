from __future__ import annotations

import asyncio
import base64
import builtins
import json
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from websockets.asyncio.server import ServerConnection, serve

from ironflow import ConfigWatchEvent, IronflowClient, IronflowError, KVWatchEvent
from ironflow._watch import _ws_url

Handler = Callable[[ServerConnection], Awaitable[None]]


async def _collect(handler: Handler, run: Callable[[str], Awaitable[Any]], **kw: Any) -> Any:
    async with serve(handler, "127.0.0.1", 0, **kw) as server:
        port = server.sockets[0].getsockname()[1]
        return await run(f"http://127.0.0.1:{port}")


def _frames(*frames: Any, seen: dict[str, Any] | None = None) -> Handler:
    async def handler(ws: ServerConnection) -> None:
        if seen is not None:
            seen["path"] = ws.request.path
            seen["auth"] = ws.request.headers.get("Authorization")
        for f in frames:
            await ws.send(f if isinstance(f, (str, bytes)) else json.dumps(f))
    return handler


def test_url_building() -> None:
    assert _ws_url("http://h:9123", "/api/v1/x") == "ws://h:9123/api/v1/x"
    assert _ws_url("https://h/pre", "/api/v1/x") == "wss://h/pre/api/v1/x"


def test_config_events_and_auth() -> None:
    seen: dict[str, Any] = {}
    frame = {"type": "config_update", "name": "a/b", "data": {"x": 1}, "revision": 3, "updatedAt": "t"}

    async def run(url: str) -> list[ConfigWatchEvent]:
        client = IronflowClient(url, api_key="k1")
        return [ev async for ev in client.watch_config("a/b")]

    out = asyncio.run(_collect(_frames({"type": "future_thing"}, frame, seen=seen), run))
    assert out == [ConfigWatchEvent(name="a/b", data={"x": 1}, revision=3, updated_at="t")]
    assert seen == {"path": "/api/v1/config/a%2Fb/watch", "auth": "Bearer k1"}


def test_kv_events_decode_base64_and_query() -> None:
    seen: dict[str, Any] = {}
    put = {"type": "kv_update", "key": "user.1", "value": base64.b64encode(b"hi").decode(),
           "revision": 1, "operation": "put", "bucket": "s b"}
    empty = {"type": "kv_update", "key": "user.2", "value": "", "revision": 2, "operation": "put", "bucket": "s b"}
    delete = {"type": "kv_update", "key": "user.1", "revision": 3, "operation": "delete", "bucket": "s b"}

    async def run(url: str) -> list[KVWatchEvent]:
        return [ev async for ev in IronflowClient(url).watch_kv("s b", key="user.*")]

    out = asyncio.run(_collect(_frames(put, empty, delete, seen=seen), run))
    assert out == [KVWatchEvent("user.1", b"hi", 1, "put", "s b"), KVWatchEvent("user.2", b"", 2, "put", "s b"),
                   KVWatchEvent("user.1", None, 3, "delete", "s b")]
    assert seen == {"path": "/api/v1/kv/buckets/s%20b/watch?key=user.%2A", "auth": None}


def test_error_frame_raises() -> None:
    async def run(url: str) -> None:
        async for _ in IronflowClient(url).watch_kv("nope"):
            pass

    with pytest.raises(IronflowError, match="bucket not found"):
        asyncio.run(_collect(_frames({"type": "error", "message": "bucket not found"}), run))


def test_bad_frame_raises_ironflow_error() -> None:
    async def run(url: str) -> None:
        async for _ in IronflowClient(url).watch_config("c"):
            pass

    with pytest.raises(IronflowError, match="watch frame"):
        asyncio.run(_collect(_frames("not json"), run))
    with pytest.raises(IronflowError, match="watch frame"):
        asyncio.run(_collect(_frames(b"\x00"), run))


def test_handshake_rejected_raises_with_status() -> None:
    async def run(url: str) -> None:
        async for _ in IronflowClient(url, api_key="bad").watch_config("c"):
            pass

    with pytest.raises(IronflowError) as info:
        asyncio.run(_collect(_frames(), run, process_request=lambda conn, req: conn.respond(401, "nope\n")))
    assert info.value.status_code == 401


def test_abnormal_close_raises() -> None:
    async def handler(ws: ServerConnection) -> None:
        await ws.close(code=1011, reason="boom")

    async def run(url: str) -> None:
        async for _ in IronflowClient(url).watch_config("c"):
            pass

    with pytest.raises(IronflowError, match="closed"):
        asyncio.run(_collect(handler, run))


def test_connect_refused_raises() -> None:
    async def run() -> None:
        async for _ in IronflowClient("http://127.0.0.1:1").watch_config("c"):
            pass

    with pytest.raises(IronflowError):
        asyncio.run(run())


def test_break_closes_socket() -> None:
    closed = asyncio.Event()

    async def handler(ws: ServerConnection) -> None:
        await ws.send(json.dumps({"type": "config_update", "name": "c", "data": None, "revision": 1,
                                  "updatedAt": "t"}))
        await ws.wait_closed()
        closed.set()

    async def run(url: str) -> bool:
        gen = IronflowClient(url).watch_config("c")
        async for _ in gen:
            break
        await gen.aclose()
        await asyncio.wait_for(closed.wait(), 2)
        return True

    assert asyncio.run(_collect(handler, run))


def test_missing_extra_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    real = builtins.__import__

    def fake(name: str, *a: Any, **kw: Any) -> Any:
        if name.startswith("websockets"):
            raise ImportError(name)
        return real(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", fake)

    async def run() -> None:
        async for _ in IronflowClient("http://x").watch_config("c"):
            pass

    with pytest.raises(ImportError, match=r"pip install 'ironflow\[watch\]'"):
        asyncio.run(run())


def test_handshake_timeout_raises_ironflow_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import websockets.asyncio.client as wsc

    async def slow(*a: Any, **kw: Any) -> Any:
        raise asyncio.TimeoutError  # on 3.10 this is not an OSError

    monkeypatch.setattr(wsc, "connect", slow)

    async def run() -> None:
        async for _ in IronflowClient("http://x").watch_config("c"):
            pass

    with pytest.raises(IronflowError, match="watch connection failed"):
        asyncio.run(run())


def test_aclose_closes_socket_immediately() -> None:
    closed = asyncio.Event()

    async def handler(ws: ServerConnection) -> None:
        await ws.send(json.dumps({"type": "kv_update", "key": "k", "revision": 1, "operation": "delete",
                                  "bucket": "b"}))
        await ws.wait_closed()
        closed.set()

    async def run(url: str) -> bool:
        gen = IronflowClient(url).watch_kv("b")
        await gen.__anext__()
        await gen.aclose()
        await asyncio.sleep(0)
        await asyncio.wait_for(closed.wait(), 0.5)
        return True

    assert asyncio.run(_collect(handler, run))
