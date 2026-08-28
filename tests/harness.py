"""Scripted mock server used by the test suite.

The mock server takes a *queue* of scripted responses so a test can say
"fail once, then succeed" — which is what retry behaviour actually needs.
The pre-existing tests use their own single-response handler; this is
additive and does not disturb them.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Any


class Response:
    """One scripted reply."""

    def __init__(
        self,
        status: int = 200,
        body: Any = None,
        headers: dict[str, str] | None = None,
        raw: bytes | None = None,
    ) -> None:
        self.status = status
        self.body = body if body is not None else {}
        self.headers = headers or {}
        #: When set, sent verbatim instead of JSON — used to simulate a proxy
        #: returning HTML with a 200.
        self.raw = raw


class ScriptedServer:
    """HTTP server that replays a queue of Responses and records requests."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.responses: list[Response] = []
        self._default = Response()
        harness = self

        class Handler(BaseHTTPRequestHandler):
            def _handle(self) -> None:
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length) if length else b""
                harness.requests.append(
                    {
                        "method": self.command,
                        "path": self.path,
                        "headers": dict(self.headers),
                        "body": json.loads(raw) if raw else None,
                    }
                )

                if harness.responses:
                    resp = harness.responses.pop(0)
                else:
                    resp = harness._default

                payload = (
                    resp.raw
                    if resp.raw is not None
                    else json.dumps(resp.body).encode()
                )
                self.send_response(resp.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                for key, value in resp.headers.items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(payload)

            do_GET = _handle
            do_POST = _handle
            do_PUT = _handle
            do_PATCH = _handle
            do_DELETE = _handle

            def log_message(self, fmt: str, *args: Any) -> None:
                pass

        class Server(HTTPServer):
            # Tests deliberately hang up mid-body (oversized responses, bounded
            # error reads). The resulting broken pipe is the assertion working,
            # not a failure, so keep it out of the test log.
            def handle_error(self, *args: Any) -> None:
                pass

        self._server = Server(("127.0.0.1", 0), Handler)
        self.port = self._server.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def script(self, *responses: Response) -> None:
        self.responses = list(responses)

    def shutdown(self) -> None:
        self._server.shutdown()
        # Without this every test leaks a bound listening socket for the life
        # of the pytest process.
        self._server.server_close()
