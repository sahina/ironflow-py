from __future__ import annotations

from typing import Any

import pytest

import ironflow._http as http
from ironflow.worker._function import function
from ironflow.worker._worker import Worker


@function(id="fn", triggers=[{"event": "e"}])
async def fn(ctx: Any) -> str:
    return "ok"


@pytest.mark.parametrize(("value", "want"), [
    (None, None),
    ("  ", None),
    (" http://engine:9912 ", "http://engine:9912"),
])
def test_configured_server_url(monkeypatch: pytest.MonkeyPatch, value: str | None, want: str | None) -> None:
    if value is None:
        monkeypatch.delenv("IRONFLOW_SERVER_URL", raising=False)
    else:
        monkeypatch.setenv("IRONFLOW_SERVER_URL", value)
    assert http.configured_server_url() == want


def test_worker_ignores_blank_server_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IRONFLOW_NO_DISCOVERY", "1")
    monkeypatch.setenv("IRONFLOW_SERVER_URL", "  ")
    assert Worker(functions=[fn])._transport._base == http.DEFAULT_SERVER_URL
