"""What a hostile or merely misconfigured server can do to a client.

Every test here is a regression test for a defect found by review, and each
one failed before the corresponding fix. The server picks the redirect target,
the Retry-After value, and the body size, so all three are attacker-controlled
in the threat model where any hop between client and Ironflow is compromised —
a reverse proxy, a load balancer, a captive portal, a stale DNS record.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

from ironflow import IronflowClient, IronflowError
from ironflow._http import MAX_RESPONSE_BYTES
from tests.harness import Response


class _QuietServer(HTTPServer):
    """Client disconnects are the point of several tests, not an error."""

    def handle_error(self, *args: Any) -> None: ...


def _serve(handler: type[BaseHTTPRequestHandler]) -> HTTPServer:
    srv = _QuietServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, args=(0.01,), daemon=True).start()
    return srv


class TestRedirects:
    """urllib's stock handler keeps Authorization across origins; requests does not."""

    def test_cross_origin_redirect_does_not_leak_the_api_key(self) -> None:
        captured: dict[str, Any] = {}

        class Evil(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                captured["auth"] = self.headers.get("Authorization")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"pwned": true}')

            def log_message(self, *a: Any) -> None: ...

        evil = _serve(Evil)

        class Redirector(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.send_response(302)
                self.send_header("Location", f"http://127.0.0.1:{evil.server_port}/x")
                self.end_headers()

            def log_message(self, *a: Any) -> None: ...

        good = _serve(Redirector)
        try:
            c = IronflowClient(
                server_url=f"http://127.0.0.1:{good.server_port}",
                api_key="ifkey_SUPER_SECRET",
                max_attempts=1,
            )
            with pytest.raises(IronflowError) as exc:
                c.events_list()

            assert captured.get("auth") is None, (
                f"API key leaked to another origin: {captured['auth']}"
            )
            assert exc.value.status_code == 302
            assert "redirect" in str(exc.value).lower()
        finally:
            good.shutdown()
            good.server_close()
            evil.shutdown()
            evil.server_close()

    def test_same_origin_redirect_is_still_followed(self) -> None:
        """Trailing-slash and path moves must keep working."""

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == "/api/v1/events":
                    self.send_response(302)
                    self.send_header("Location", "/moved")
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"ok": true}')

            def log_message(self, *a: Any) -> None: ...

        srv = _serve(Handler)
        try:
            c = IronflowClient(server_url=f"http://127.0.0.1:{srv.server_port}")
            assert c.events_list() == {"ok": True}
        finally:
            srv.shutdown()
            srv.server_close()

    def test_redirected_write_is_refused_not_downgraded(self) -> None:
        """301/302/303 re-issue a POST as a bodyless GET.

        Following that reports success for an event that was never emitted.
        """
        seen: list[str] = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                seen.append("POST")
                self.send_response(303)
                self.send_header("Location", "/after")
                self.end_headers()

            def do_GET(self) -> None:
                seen.append("GET")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"ok": true}')

            def log_message(self, *a: Any) -> None: ...

        srv = _serve(Handler)
        try:
            c = IronflowClient(
                server_url=f"http://127.0.0.1:{srv.server_port}", max_attempts=1
            )
            with pytest.raises(IronflowError) as exc:
                c.request("POST", "/api/v1/projects", body={"name":"x"})
            assert exc.value.status_code == 303
            assert seen == ["POST"], f"write was silently downgraded: {seen}"
        finally:
            srv.shutdown()
            srv.server_close()


class TestRetryAfterIsClamped:
    def test_huge_delta_seconds_cannot_pin_the_thread(
        self, server, monkeypatch
    ) -> None:
        slept: list[float] = []
        monkeypatch.setattr("ironflow._http.time.sleep", lambda s: slept.append(s))

        # 10 years. Before the clamp this slept for exactly that.
        server.script(
            Response(status=429, headers={"Retry-After": "315360000"}),
            Response(status=200, body={"ok": 1}),
        )
        c = IronflowClient(server_url=server.url)
        assert c.events_list() == {"ok": 1}

        assert slept and slept[0] <= c.max_backoff, (
            f"Retry-After bypassed max_backoff={c.max_backoff}: slept {slept}"
        )

    def test_error_still_carries_the_servers_request(self, server) -> None:
        """Clamping the sleep must not hide what the server asked for."""
        server.script(*[Response(status=429, headers={"Retry-After": "3600"})] * 3)
        c = IronflowClient(server_url=server.url, max_attempts=1)
        with pytest.raises(IronflowError) as exc:
            c.events_list()
        assert exc.value.retry_after == 3600.0

    @pytest.mark.parametrize("value", ["9" * 401, "not-a-date", "", "-5"])
    def test_hostile_retry_after_never_escapes_as_another_exception(
        self, server, value: str
    ) -> None:
        """The module contract is that IronflowError is sufficient.

        '9'*401 parses as an int fine and then overflows float().
        """
        server.script(
            Response(status=503, headers={"Retry-After": value}),
            Response(status=200, body={"ok": 1}),
        )
        c = IronflowClient(server_url=server.url)
        c.initial_backoff = 0.01
        c.max_backoff = 0.02
        assert c.events_list() == {"ok": 1}


class TestResponseSizeIsBounded:
    def test_oversized_body_is_refused(self) -> None:
        class Flood(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                chunk = b"x" * (1024 * 1024)
                for _ in range((MAX_RESPONSE_BYTES // len(chunk)) + 2):
                    try:
                        self.wfile.write(chunk)
                    except (BrokenPipeError, ConnectionResetError):
                        return

            def log_message(self, *a: Any) -> None: ...

        srv = _serve(Flood)
        try:
            c = IronflowClient(
                server_url=f"http://127.0.0.1:{srv.server_port}", max_attempts=1
            )
            with pytest.raises(IronflowError) as exc:
                c.events_list()
            assert "exceeds" in str(exc.value)
        finally:
            srv.shutdown()
            srv.server_close()

    def test_huge_error_body_does_not_become_the_message(self, server) -> None:
        server.script(Response(status=500, raw=b"E" * (2 * 1024 * 1024)))
        c = IronflowClient(server_url=server.url, max_attempts=1)
        with pytest.raises(IronflowError) as exc:
            c.events_list()
        assert len(str(exc.value)) < 64 * 1024, (
            f"error body became a {len(str(exc.value))}-byte exception message"
        )

    def test_deeply_nested_json_surfaces_as_ironflow_error(self, server) -> None:
        """RecursionError is a server-controlled input, not a bug in the caller."""
        server.script(Response(status=200, raw=b"[" * 200_000 + b"]" * 200_000))
        c = IronflowClient(server_url=server.url, max_attempts=1)
        with pytest.raises(IronflowError):
            c.events_list()


class TestTotalTimeoutBoundsTheAttempt:
    def test_slow_attempt_cannot_overshoot_the_deadline(self) -> None:
        """The deadline used to gate only the backoff sleep.

        A total_timeout of 0.5s with the default timeout of 30s could still
        block for 30s inside a single attempt.
        """

        class Slow(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                import time as _t

                _t.sleep(5)

            def log_message(self, *a: Any) -> None: ...

        srv = _serve(Slow)
        try:
            import time as _t

            c = IronflowClient(
                server_url=f"http://127.0.0.1:{srv.server_port}",
                timeout=30.0,
                total_timeout=0.5,
                max_attempts=1,
            )
            started = _t.monotonic()
            with pytest.raises(IronflowError):
                c.events_list()
            elapsed = _t.monotonic() - started
            assert elapsed < 3.0, (
                f"total_timeout=0.5 with timeout=30 took {elapsed:.2f}s"
            )
        finally:
            srv.shutdown()
            srv.server_close()

    def test_exhausted_budget_refuses_before_dialling(self, server) -> None:
        c = IronflowClient(server_url=server.url, total_timeout=-1.0, max_attempts=1)
        with pytest.raises(IronflowError) as exc:
            c.events_list()
        assert "total_timeout" in str(exc.value)
        assert len(server.requests) == 0, "should not have dialled at all"


def test_json_body_still_round_trips(server) -> None:
    """Guard against the hardening breaking the ordinary path."""
    server.script(Response(status=200, body={"runs": [{"id": "r1"}]}))
    c = IronflowClient(server_url=server.url)
    assert c.events_list() == {"runs": [{"id": "r1"}]}
    assert json.loads(json.dumps({"ok": 1})) == {"ok": 1}
