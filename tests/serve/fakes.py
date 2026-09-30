"""Stand-in for worker._transport.Transport, shared by the webhook and register tests."""

from __future__ import annotations

from typing import Any, ClassVar


class FakeReply:
    def __init__(self, status: int, body: Any = None) -> None:
        self.status, self.body = status, body

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300

    @property
    def error_code(self) -> str:
        return ""


class FakeTransport:
    sent: ClassVar[list[tuple[str, str, Any, str | None, str]]] = []
    reply: ClassVar[Any] = FakeReply(200, {})

    def __init__(self, server_url: str, api_key: str | None, environment: str) -> None:
        self.url, self.key, self.env = server_url, api_key, environment

    async def request(self, method: str, path: str, body: Any = None) -> Any:
        FakeTransport.sent.append((self.url + path, method, body, self.key, self.env))
        if isinstance(FakeTransport.reply, Exception):
            raise FakeTransport.reply
        return FakeTransport.reply
