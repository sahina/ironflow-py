"""The local engine stand-in must not report a client hang-up as a server fault."""

from __future__ import annotations

import socket
import time
from typing import Any
from urllib.parse import urlparse

from tests.worker.fake_engine import FakeEngine


def test_client_hangup_during_response_stays_silent(capsys: Any) -> None:
    engine = FakeEngine()
    engine.stall("poll", 0.2)
    try:
        parsed = urlparse(engine.url)
        assert parsed.hostname is not None and parsed.port is not None
        conn = socket.create_connection((parsed.hostname, parsed.port))
        conn.sendall(
            b"GET /api/v1/workers/w/jobs?available=1 HTTP/1.1\r\n"
            b"Host: localhost\r\nConnection: close\r\n\r\n"
        )
        conn.close()
        time.sleep(0.5)
    finally:
        engine.shutdown()
    assert capsys.readouterr().err == ""
