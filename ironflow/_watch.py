"""KV and config watch over the engine's WebSocket routes (#2397). Needs the 'watch' extra."""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import aclosing
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlencode

if TYPE_CHECKING:
    from websockets.asyncio.client import ClientConnection

_HINT = "KV/config watch needs the 'watch' extra: pip install 'ironflow[watch]'"


@dataclass(frozen=True)
class ConfigWatchEvent:
    name: str
    data: dict[str, Any] | None
    revision: int
    updated_at: str


@dataclass(frozen=True)
class KVWatchEvent:
    key: str
    value: bytes | None
    revision: int
    operation: str  # "put" or "delete"
    bucket: str


def _ws_url(server_url: str, path: str) -> str:
    if server_url.startswith("https://"):
        return "wss://" + server_url[len("https://"):] + path
    if server_url.startswith("http://"):
        return "ws://" + server_url[len("http://"):] + path
    return server_url + path


def _decode(raw: str | bytes) -> dict[str, Any] | None:
    if not isinstance(raw, str):
        return None
    try:
        frame = json.loads(raw)
    except ValueError:
        return None
    return frame if isinstance(frame, dict) else None


class _WatchMixin:
    server_url: str
    api_key: str | None

    async def _frames(self, path: str) -> AsyncGenerator[dict[str, Any], None]:
        from ._http import IronflowError
        try:
            from websockets.asyncio.client import connect
            from websockets.exceptions import (
                ConnectionClosedError,
                InvalidStatus,
                WebSocketException,
            )
        except ImportError as exc:
            raise ImportError(_HINT) from exc

        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else None
        try:
            ws: ClientConnection = await connect(_ws_url(self.server_url, path), additional_headers=headers)
        except InvalidStatus as exc:
            status = exc.response.status_code
            raise IronflowError(f"watch handshake rejected: HTTP {status}", status_code=status) from exc
        except (OSError, asyncio.TimeoutError, WebSocketException) as exc:
            raise IronflowError(f"watch connection failed: {exc}") from exc
        try:
            async for raw in ws:
                frame = _decode(raw)
                if frame is None:
                    raise IronflowError("malformed watch frame: expected a JSON object text frame")
                if frame.get("type") == "error":
                    raise IronflowError(str(frame.get("message") or "watch error"))
                yield frame
        except ConnectionClosedError as exc:
            raise IronflowError(f"watch connection closed: {exc}") from exc
        finally:
            await ws.close()

    async def watch_config(self, name: str) -> AsyncIterator[ConfigWatchEvent]:
        """GET /api/v1/config/{name}/watch

        Yield each update to one config until the server closes. Leaving the loop closes the socket.
        Call ``aclose()`` on the iterator (or use ``contextlib.aclosing``) to close the socket at once after ``break``.
        """
        async with aclosing(self._frames(f"/api/v1/config/{quote(name, safe='')}/watch")) as frames:
            async for f in frames:
                if f.get("type") == "config_update":
                    yield ConfigWatchEvent(name=f["name"], data=f.get("data"), revision=f["revision"],
                                           updated_at=f.get("updatedAt", ""))

    async def watch_kv(self, bucket: str, *, key: str | None = None) -> AsyncIterator[KVWatchEvent]:
        """GET /api/v1/kv/buckets/{bucket}/watch

        Yield each put and delete in a bucket, optionally filtered by a key pattern such as ``user.*``.
        Call ``aclose()`` on the iterator (or use ``contextlib.aclosing``) to close the socket at once after ``break``.
        """
        path = f"/api/v1/kv/buckets/{quote(bucket, safe='')}/watch"
        if key:
            path += "?" + urlencode({"key": key}, quote_via=quote, safe="")
        async with aclosing(self._frames(path)) as frames:
            async for f in frames:
                if f.get("type") == "kv_update":
                    value = f.get("value")
                    yield KVWatchEvent(key=f["key"], value=None if value is None else base64.b64decode(value),
                                       revision=f["revision"], operation=f["operation"], bucket=f["bucket"])
