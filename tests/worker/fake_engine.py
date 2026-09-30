"""Stateful local HTTP stand-in for pull-worker endpoints."""

from __future__ import annotations

import json
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

_ROUTES = [
    ("POST", re.compile(r"^/ironflow\.v1\.IronflowService/RegisterFunction$"), "register_function"),
    ("POST", re.compile(r"^/ironflow\.v1\.PubSubService/Publish$"), "publish"),
    ("POST", re.compile(r"^/api/v1/workers/([^/]+)/register$"), "register"),
    ("POST", re.compile(r"^/api/v1/workers/([^/]+)/heartbeat$"), "heartbeat"),
    ("GET", re.compile(r"^/api/v1/workers/([^/]+)/jobs$"), "poll"),
    ("PUT", re.compile(r"^/api/v1/workers/([^/]+)/jobs/([^/]+)/ack$"), "ack"),
    ("PUT", re.compile(r"^/api/v1/workers/([^/]+)/jobs/([^/]+)$"), "update"),
]


def make_job(job_id: str = "run_1", **overrides: Any) -> dict[str, Any]:
    job: dict[str, Any] = {
        "job_id": job_id, "run_id": job_id, "function_id": "fn", "attempt": 1,
        "event": {"id": "ev_1", "name": "e", "version": 1, "data": {}, "timestamp": "2026-09-24T10:00:00Z"},
        "completed_steps": [], "step_sequence_base": 0, "execution_seq": 1, "lease_token": "tok",
    }
    job.update(overrides)
    return job


class FakeEngine:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.requests: list[dict[str, Any]] = []
        self.registered: set[str] = set()
        self.queue: list[dict[str, Any]] = []
        self._faults: dict[str, list[tuple[int, Any]]] = {}
        self._stalls: dict[str, float] = {}
        engine = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:
                pass

            def _serve(self, method: str) -> None:
                path, _, query = self.path.partition("?")
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                body = json.loads(raw) if raw else None
                status, reply = engine.handle(method, path, query, dict(self.headers), body)
                data = b"" if reply is None else json.dumps(reply).encode()
                self.send_response(status)
                if data:
                    self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self) -> None:
                self._serve("GET")

            def do_POST(self) -> None:
                self._serve("POST")

            def do_PUT(self) -> None:
                self._serve("PUT")

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def fail(self, route: str, status: int, body: Any = None, times: int = 1) -> None:
        with self.lock:
            self._faults.setdefault(route, []).extend([(status, body)] * times)

    def stall(self, route: str, seconds: float) -> None:
        with self.lock:
            self._stalls[route] = seconds

    def enqueue(self, *jobs: dict[str, Any]) -> None:
        with self.lock:
            self.queue.extend(jobs)

    def calls(self, route: str) -> list[dict[str, Any]]:
        with self.lock:
            return [r for r in self.requests if r["route"] == route]

    def handle(self, method: str, path: str, query: str, headers: dict[str, str], body: Any) -> tuple[int, Any]:
        for route_method, pattern, route in _ROUTES:
            match = pattern.match(path)
            if route_method == method and match:
                break
        else:
            return 404, {"error": "no route"}
        if route == "update" and isinstance(body, dict):
            route = "progress" if body.get("status") == "progress" else "terminal"
        with self.lock:
            stall = self._stalls.get(route, 0.0)
        if stall:
            time.sleep(stall)
        with self.lock:
            self.requests.append({"route": route, "path": path, "query": query, "headers": headers, "body": body})
            faults = self._faults.get(route)
            if faults:
                return faults.pop(0)
            worker = match.group(1) if match.groups() else None
            if route == "register":
                self.registered.add(worker or "")
                return 200, {"status": "registered"}
            if route == "register_function":
                return 200, {}
            if route == "publish":
                return 200, {"eventId": "evt_pub_1", "sequence": "7"}
            if route in ("heartbeat", "poll", "progress", "terminal") and worker not in self.registered:
                return 404, {"error": "worker not registered"}
            if route == "poll":
                available = int(dict(p.split("=") for p in query.split("&") if p).get("available", "1"))
                jobs, self.queue = self.queue[:available], self.queue[available:]
                return (200, {"jobs": jobs}) if jobs else (204, None)
            return 200, {"status": "ok"}

    def shutdown(self) -> None:
        self._server.shutdown()
        self._server.server_close()
