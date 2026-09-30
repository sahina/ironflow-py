"""Bounded async HTTP requests for the pull worker."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pyqwest

from .._http import IronflowError

_NETWORK_ERRORS: tuple[type[BaseException], ...] = (
    OSError, pyqwest.ReadError, pyqwest.WriteError, pyqwest.RemoteProtocolError,
)


@dataclass(frozen=True)
class Reply:
    status: int
    body: Any

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300

    @property
    def error_code(self) -> str:
        if isinstance(self.body, dict):
            code = self.body.get("error")
            return code if isinstance(code, str) else ""
        return ""


class Transport:
    def __init__(
        self, server_url: str, api_key: str | None, environment: str,
        client: pyqwest.Client | None = None, request_timeout: float = 30.0,
    ) -> None:
        self._base = server_url.rstrip("/")
        self._api_key = api_key
        self._environment = environment
        self._client = client or pyqwest.Client()
        self._timeout = request_timeout

    def headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json", "X-Ironflow-Environment": self._environment}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        return headers

    async def request(
        self, method: str, path: str, body: Any = None, headers: Mapping[str, str] | None = None,
    ) -> Reply:
        content = None if body is None else json.dumps(body, allow_nan=False).encode()
        try:
            # Not asyncio.wait_for: before Python 3.12 it returns the result and
            # swallows a cancel that lands as the request completes, so a
            # cancelled heartbeat kept looping and shutdown awaited it forever.
            call = asyncio.ensure_future(
                self._client.execute(method, self._base + path, {**self.headers(), **(headers or {})}, content))
            try:
                done, _ = await asyncio.wait({call}, timeout=self._timeout)
            except asyncio.CancelledError:
                call.cancel()
                raise
            if not done:
                call.cancel()
                raise asyncio.TimeoutError
            response = call.result()
        except asyncio.TimeoutError as exc:
            raise IronflowError(
                f"{method} {path}: no response within {self._timeout:g}s",
                code="NETWORK_ERROR", retryable=True,
            ) from exc
        except _NETWORK_ERRORS as exc:
            raise IronflowError(
                f"{method} {path}: {exc}", code="NETWORK_ERROR", retryable=True,
            ) from exc
        raw = response.content
        if not raw:
            return Reply(response.status, None)
        try:
            return Reply(response.status, json.loads(raw))
        except ValueError:
            return Reply(response.status, raw.decode(errors="replace"))
