"""Tests for the generated Ironflow Python client."""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Any, ClassVar
from urllib.parse import parse_qs, urlparse

import pytest

from ironflow import IronflowClient, models
from ironflow.client import IronflowError


class MockHandler(BaseHTTPRequestHandler):
    """Mock HTTP handler that records requests and returns configurable responses."""

    # Deliberately class-level: the fixture resets these between tests. See
    # tests/harness.py for the newer scripted server used by the other suites.
    requests: ClassVar[list[dict[str, Any]]] = []
    response_body: ClassVar[Any] = {}
    response_status: ClassVar[int] = 200

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def do_PUT(self) -> None:
        self._handle()

    def do_PATCH(self) -> None:
        self._handle()

    def do_DELETE(self) -> None:
        self._handle()

    def _handle(self) -> None:
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length else b""

        MockHandler.requests.append(
            {
                "method": self.command,
                "path": self.path,
                "headers": dict(self.headers),
                "body": json.loads(body) if body else None,
            }
        )

        self.send_response(MockHandler.response_status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if MockHandler.response_status == 204:
            pass  # No Content: no body at all, like an idle job poll
        elif MockHandler.response_status < 400:
            self.wfile.write(json.dumps(MockHandler.response_body).encode())
        else:
            self.wfile.write(
                json.dumps({"code": "ERROR", "message": "test error"}).encode()
            )

    def log_message(self, format: str, *args: Any) -> None:
        pass  # suppress server logs


@pytest.fixture()
def mock_server():
    """Start a mock HTTP server and return an IronflowClient connected to it."""
    MockHandler.requests = []
    MockHandler.response_body = {}
    MockHandler.response_status = 200

    server = HTTPServer(("127.0.0.1", 0), MockHandler)
    port = server.server_address[1]
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()

    client = IronflowClient(
        server_url=f"http://127.0.0.1:{port}",
        api_key="test_key",
    )

    yield client

    server.shutdown()


class TestClientInit:
    def test_default_url(self) -> None:
        client = IronflowClient()
        assert client.server_url == "http://localhost:9123"

    def test_custom_url(self) -> None:
        client = IronflowClient(server_url="http://example.com:8080/")
        assert client.server_url == "http://example.com:8080"  # trailing slash stripped

    def test_api_key(self) -> None:
        client = IronflowClient(api_key="ifkey_test")
        assert client.api_key == "ifkey_test"


class TestRequestMechanics:
    def test_get_sends_correct_method(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {"runs": []}
        mock_server.runs_list()
        assert len(MockHandler.requests) == 1
        assert MockHandler.requests[0]["method"] == "GET"
        assert MockHandler.requests[0]["path"] == "/api/v1/runs"

    def test_post_sends_body(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {"run_ids": ["r1"]}
        mock_server.events_create(body={"event": "test", "data": {}})
        assert MockHandler.requests[0]["method"] == "POST"
        assert MockHandler.requests[0]["body"]["event"] == "test"

    def test_auth_header(self, mock_server: IronflowClient) -> None:
        mock_server.runs_list()
        assert "Bearer test_key" in MockHandler.requests[0]["headers"].get(
            "Authorization", ""
        )

    def test_path_params_escaped(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {"id": "r1"}
        mock_server.runs_get("run/with/slashes")
        assert "/api/v1/runs/run%2Fwith%2Fslashes" == MockHandler.requests[0]["path"]

    def test_error_response(self, mock_server: IronflowClient) -> None:
        MockHandler.response_status = 404
        with pytest.raises(IronflowError) as exc_info:
            mock_server.runs_get("nonexistent")
        assert exc_info.value.status_code == 404
        assert exc_info.value.code == "ERROR"


class TestEndpointCoverage:
    """Verify key endpoints are callable."""

    def test_events_create(self, mock_server: IronflowClient) -> None:
        mock_server.events_create(body={"event": "test"})
        assert MockHandler.requests[0]["path"] == "/api/v1/events"

    def test_events_list(self, mock_server: IronflowClient) -> None:
        mock_server.events_list()
        assert MockHandler.requests[0]["path"] == "/api/v1/events"

    def test_runs_list(self, mock_server: IronflowClient) -> None:
        mock_server.runs_list()
        assert MockHandler.requests[0]["path"] == "/api/v1/runs"

    def test_runs_get(self, mock_server: IronflowClient) -> None:
        mock_server.runs_get("r1")
        assert MockHandler.requests[0]["path"] == "/api/v1/runs/r1"

    def test_runs_cancel(self, mock_server: IronflowClient) -> None:
        mock_server.runs_cancel("r1", body={"reason": "test"})
        assert MockHandler.requests[0]["path"] == "/api/v1/runs/r1/cancel"

    def test_projections_list(self, mock_server: IronflowClient) -> None:
        mock_server.projections_list()
        assert MockHandler.requests[0]["path"] == "/api/v1/projections"

    def test_projections_get(self, mock_server: IronflowClient) -> None:
        mock_server.projections_get("my-proj")
        assert MockHandler.requests[0]["path"] == "/api/v1/projections/my-proj"

    def test_projections_rebuild(self, mock_server: IronflowClient) -> None:
        mock_server.projections_rebuild("my-proj")
        assert MockHandler.requests[0]["path"] == "/api/v1/projections/my-proj/rebuild"

    def test_kv_list_buckets(self, mock_server: IronflowClient) -> None:
        mock_server.kv_list_buckets()
        assert MockHandler.requests[0]["path"] == "/api/v1/kv/buckets"

    def test_kv_get_buckets_keys(self, mock_server: IronflowClient) -> None:
        mock_server.kv_get_buckets_keys("my-bucket", "my-key")
        assert (
            MockHandler.requests[0]["path"]
            == "/api/v1/kv/buckets/my-bucket/keys/my-key"
        )

    def test_api_keys_create(self, mock_server: IronflowClient) -> None:
        mock_server.api_keys_create(body={"name": "test"})
        assert MockHandler.requests[0]["path"] == "/api/v1/apikeys"

    def test_secrets_list(self, mock_server: IronflowClient) -> None:
        mock_server.secrets_list(x_ironflow_environment="default")
        assert MockHandler.requests[0]["path"] == "/api/v1/secrets"
        assert MockHandler.requests[0]["headers"]["X-Ironflow-Environment"] == "default"

    def test_config_get(self, mock_server: IronflowClient) -> None:
        mock_server.config_get("my-config")
        assert MockHandler.requests[0]["path"] == "/api/v1/config/my-config"


class TestAnnotatedGroupRoundTrips:
    """One canned-response round trip per schema-annotated group.

    These pin the wire (path + query) and that the declared fields survive the
    trip. They are NOT schema validation: models are TypedDicts behind a
    `cast`, so nothing is checked at runtime.
    """

    def test_runs_list(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {
            "runs": [{"id": "run_1", "function_id": "fn_1", "status": "completed"}],
            "count": 1,
            "total_count": 1,
        }
        result = mock_server.runs_list(status="completed", limit=1)
        assert MockHandler.requests[0]["path"].startswith("/api/v1/runs?")
        query = parse_qs(urlparse(MockHandler.requests[0]["path"]).query)
        assert query["status"] == ["completed"]
        assert query["limit"] == ["1"]
        assert result["total_count"] == 1
        assert result["runs"][0]["id"] == "run_1"

    def test_events_list(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {
            "events": [{"id": "evt_1", "name": "user.created", "source": "api"}],
            "count": 1,
            "limit": 20,
            "has_next": False,
            "has_prev": False,
        }
        result = mock_server.events_list(name="user.created")
        query = parse_qs(urlparse(MockHandler.requests[0]["path"]).query)
        assert query["name"] == ["user.created"]
        assert result["events"][0]["name"] == "user.created"
        assert result["has_next"] is False

    def test_streams_list_events(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {
            "events": [{"id": "evt_1", "name": "order.placed"}],
            "total_count": 1,
        }
        result = mock_server.streams_list_events("order-42", from_version=3)
        assert MockHandler.requests[0]["path"].startswith(
            "/api/v1/streams/order-42/events?"
        )
        query = parse_qs(urlparse(MockHandler.requests[0]["path"]).query)
        assert query["from_version"] == ["3"]
        assert result["total_count"] == 1
        assert result["events"][0]["name"] == "order.placed"

    def test_projections_list(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {
            "projections": [
                {"name": "order_totals", "mode": "managed", "status": "active"}
            ],
            "count": 1,
        }
        result = mock_server.projections_list(status="active", limit=1)
        assert MockHandler.requests[0]["path"].startswith("/api/v1/projections?")
        query = parse_qs(urlparse(MockHandler.requests[0]["path"]).query)
        assert query["status"] == ["active"]
        assert query["limit"] == ["1"]
        assert result["count"] == 1
        assert result["projections"][0]["name"] == "order_totals"

    def test_workers_list_jobs(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {
            "jobs": [{"job_id": "run_1", "run_id": "run_1", "function_id": "fn_1"}]
        }
        result = mock_server.workers_list_jobs("worker-7", available=2)
        assert MockHandler.requests[0]["path"].startswith(
            "/api/v1/workers/worker-7/jobs?"
        )
        query = parse_qs(urlparse(MockHandler.requests[0]["path"]).query)
        assert query["available"] == ["2"]
        assert result is not None
        assert result["jobs"][0]["function_id"] == "fn_1"

    def test_workers_list_jobs_idle_poll_returns_none(
        self, mock_server: IronflowClient
    ) -> None:
        """204 on an idle poll decodes to None, and the return type says so.

        This is the only route annotated RespMayBe204, so it is the only place
        a typed method may return None. If the annotation is dropped,
        workers_list_jobs() goes back to promising a JobBatchResponse it does
        not always deliver.
        """
        MockHandler.response_status = 204
        assert mock_server.workers_list_jobs("worker-7") is None

    def test_kv_list_buckets_keys(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {"keys": ["user:1", "user:2"], "count": 2}
        result = mock_server.kv_list_buckets_keys("sessions", filter="user:")
        assert MockHandler.requests[0]["path"].startswith(
            "/api/v1/kv/buckets/sessions/keys?"
        )
        query = parse_qs(urlparse(MockHandler.requests[0]["path"]).query)
        assert query["filter"] == ["user:"]
        assert result["count"] == 2
        assert result["keys"] == ["user:1", "user:2"]

    def test_roles_list_policies(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {
            "policies": [
                {"id": "pol_1", "name": "read-runs", "effect": "allow"},
            ]
        }
        result = mock_server.roles_list_policies("role_ops")
        assert MockHandler.requests[0]["path"] == "/api/v1/roles/role_ops/policies"
        assert result["policies"][0]["id"] == "pol_1"
        assert result["policies"][0]["effect"] == "allow"

    def test_users_get(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {
            "id": "usr_1",
            "org_id": "org_default",
            "email": "a@example.com",
            "name": "Ada",
            "roles": ["admin"],
        }
        result = mock_server.users_get("usr_1")
        assert MockHandler.requests[0]["path"] == "/api/v1/users/usr_1"
        assert result["email"] == "a@example.com"
        assert result["roles"] == ["admin"]

    def test_orgs_list(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = [{"id": "org_1", "name": "Acme"}]
        result = mock_server.orgs_list()
        assert MockHandler.requests[0]["path"] == "/api/v1/orgs"
        assert result[0]["id"] == "org_1"
        assert result[0]["name"] == "Acme"

    def test_secrets_get(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {
            "name": "STRIPE_KEY",
            "description": "billing",
            "revision": 3,
        }
        result = mock_server.secrets_get("STRIPE_KEY", x_ironflow_environment="default")
        assert MockHandler.requests[0]["path"] == "/api/v1/secrets/STRIPE_KEY"
        assert MockHandler.requests[0]["headers"]["X-Ironflow-Environment"] == "default"
        assert result["name"] == "STRIPE_KEY"
        assert result["revision"] == 3

    def test_schemas_list(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {
            "schemas": [
                {"event_name": "order.placed", "version": 2, "schema_json": "{}"}
            ],
            "total_count": 1,
            "count": 1,
        }
        result = mock_server.schemas_list(event_name="order.placed", limit=1)
        assert MockHandler.requests[0]["path"].startswith("/api/v1/events/schemas?")
        query = parse_qs(urlparse(MockHandler.requests[0]["path"]).query)
        assert query["event_name"] == ["order.placed"]
        assert query["limit"] == ["1"]
        assert result["total_count"] == 1
        assert result["schemas"][0]["version"] == 2

    def test_policies_list(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = [
            {"id": "pol_1", "name": "deny-prod", "effect": "deny", "role_count": 2}
        ]
        result = mock_server.policies_list()
        assert MockHandler.requests[0]["path"] == "/api/v1/policies"
        assert result[0]["id"] == "pol_1"
        assert result[0]["role_count"] == 2

    def test_config_get(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {
            "name": "billing",
            "data": {"retries": 3},
            "revision": 7,
            "updatedAt": "2026-08-15T00:00:00Z",
        }
        result = mock_server.config_get("billing")
        assert MockHandler.requests[0]["path"] == "/api/v1/config/billing"
        assert result["revision"] == 7
        assert result["data"]["retries"] == 3

    def test_projects_list(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = [
            {"id": "proj_acme_web", "org_id": "org_acme", "name": "web"}
        ]
        result = mock_server.projects_list()
        assert MockHandler.requests[0]["path"] == "/api/v1/projects"
        assert result[0]["id"] == "proj_acme_web"
        assert result[0]["name"] == "web"

    def test_environments_list(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = [
            {
                "id": "env_acme_prod",
                "project_id": "proj_acme_default",
                "name": "prod",
                "color": "#ff0000",
                "created_at": "2026-08-15T00:00:00Z",
                "updated_at": "2026-08-15T00:00:00Z",
            }
        ]
        result = mock_server.environments_list(project_id="proj_acme_default")
        assert MockHandler.requests[0]["path"].startswith("/api/v1/environments?")
        query = parse_qs(urlparse(MockHandler.requests[0]["path"]).query)
        assert query["project_id"] == ["proj_acme_default"]
        assert result[0]["id"] == "env_acme_prod"
        assert result[0]["name"] == "prod"

    def test_environments_delete(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {"status": "deleted"}
        result = mock_server.environments_delete("env_acme_prod")
        assert MockHandler.requests[0]["method"] == "DELETE"
        assert MockHandler.requests[0]["path"] == "/api/v1/environments/env_acme_prod"
        assert result["status"] == "deleted"

    def test_functions_list(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {
            "functions": [{"id": "fn_1", "slug": "send-email", "status": "active"}],
            "count": 1,
            "total_count": 1,
        }
        result = mock_server.functions_list(status="active", limit=1, offset=0)
        assert MockHandler.requests[0]["path"].startswith("/api/v1/functions?")
        query = parse_qs(urlparse(MockHandler.requests[0]["path"]).query)
        assert query["status"] == ["active"]
        assert query["limit"] == ["1"]
        assert query["offset"] == ["0"]
        assert result["total_count"] == 1
        assert result["functions"][0]["slug"] == "send-email"

    def test_functions_invoke(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {"run_id": "run_1", "event_id": "evt_1"}
        result = mock_server.functions_invoke("fn_1", {"data": {"to": "a@b.com"}})
        assert MockHandler.requests[0]["method"] == "POST"
        assert MockHandler.requests[0]["path"] == "/api/v1/functions/fn_1/invoke"
        assert MockHandler.requests[0]["body"] == {"data": {"to": "a@b.com"}}
        assert result["run_id"] == "run_1"

    def test_audit_list(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {
            "events": [{"id": "aud_1", "run_id": "run_1", "event_type": "run.created"}],
            "total_count": 1,
            "next_cursor": "",
        }
        result = mock_server.audit_list(run_id="run_1", limit=1, from_="2026-08-01")
        assert MockHandler.requests[0]["path"].startswith("/api/v1/audit?")
        query = parse_qs(urlparse(MockHandler.requests[0]["path"]).query)
        assert query["run_id"] == ["run_1"]
        assert query["limit"] == ["1"]
        # `from` is a Python keyword, so the generator names the kwarg `from_`
        # while the wire parameter stays `from`.
        assert query["from"] == ["2026-08-01"]
        assert result["events"][0]["id"] == "aud_1"

    def test_publish(self, mock_server: IronflowClient) -> None:
        # Pins the method NAME, not just the payload. The generator builds a
        # method suffix by locating the group inside the path, and "pubsub"
        # does not appear in "/api/v1/publish" — which is the shape of the
        # #1551 bug, where an absent group collapsed a name and the collision
        # was resolved by silently renaming the incumbent. This name predates
        # the schema sweep and must not move under anyone's feet.
        MockHandler.response_body = {"event_id": "evt_1", "sequence": 42}
        result = mock_server.pub_sub_create(
            {"topic": "orders", "data": {"id": 1}, "idempotency_key": "k1"}
        )
        assert MockHandler.requests[0]["method"] == "POST"
        assert MockHandler.requests[0]["path"] == "/api/v1/publish"
        assert MockHandler.requests[0]["body"]["topic"] == "orders"
        assert result["sequence"] == 42

    # ── groups added by the 25-route registration sweep ──────────────────

    def test_capacity_list_lanes(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = [
            {"lane_id": "lane_1", "environment_id": "env_default", "max_concurrent": 4}
        ]
        result = mock_server.capacity_list_lanes(env="env_default", limit=1)
        assert MockHandler.requests[0]["path"].startswith("/api/v1/capacity/lanes?")
        query = parse_qs(urlparse(MockHandler.requests[0]["path"]).query)
        assert query["env"] == ["env_default"]
        assert query["limit"] == ["1"]
        assert result[0]["lane_id"] == "lane_1"

    def test_capacity_list_stats_takes_no_query(
        self, mock_server: IronflowClient
    ) -> None:
        # stats is the one capacity route with no filter — handleCapacityStats
        # calls store.CapacityStats with no argument. A Query annotation here
        # would generate kwargs the server ignores.
        MockHandler.response_body = [{"environment_id": "env_default", "running": 2}]
        mock_server.capacity_list_stats()
        assert MockHandler.requests[0]["path"] == "/api/v1/capacity/stats"

    def test_circuit_breakers_list_and_reset(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = [
            {"key": "fn_1|https://x", "function_id": "fn_1", "state": "open"}
        ]
        listed = mock_server.circuit_breakers_list()
        assert MockHandler.requests[0]["path"] == "/api/v1/circuit-breakers"
        assert listed[0]["state"] == "open"

        MockHandler.response_body = {
            "key": "fn_1|https://x",
            "function_id": "fn_1",
            "state": "closed",
            "consecutive_fails": 0,
        }
        # The path segment is the base64url-encoded "fnID|endpoint" composite;
        # the generator quotes it with safe='' so padding-free base64url with a
        # "/" or "=" cannot break out of the segment.
        reset = mock_server.circuit_breakers_reset("Zm5fMXxodHRwczovL3g")
        assert (
            MockHandler.requests[1]["path"]
            == "/api/v1/circuit-breakers/Zm5fMXxodHRwczovL3g/reset"
        )
        assert reset["state"] == "closed"

    def test_outbox_dead_letter_round_trip(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {
            "items": [
                {
                    "id": "dlq_1",
                    "event_id": "evt_1",
                    "topic": "orders",
                    "kind": "event",
                    "environment_id": "env_default",
                    "attempts": 5,
                    "last_error": "connection refused",
                }
            ],
            "limit": 50,
            "offset": 0,
        }
        listed = mock_server.outbox_list_dead_letter(env="env_default", limit=50)
        query = parse_qs(urlparse(MockHandler.requests[0]["path"]).query)
        assert query["env"] == ["env_default"]
        assert listed["items"][0]["attempts"] == 5

        # requeue and discard answer 200 with a body, NOT 204 — both write
        # {"event_id","result"}. A 204 annotation would have typed these None.
        MockHandler.response_body = {"event_id": "evt_1", "result": "requeued"}
        requeued = mock_server.outbox_dead_letter_requeue("evt_1", env="env_default")
        assert MockHandler.requests[1]["method"] == "POST"
        assert MockHandler.requests[1]["path"].startswith(
            "/api/v1/outbox/dead-letter/evt_1/requeue?"
        )
        assert requeued["result"] == "requeued"

        MockHandler.response_body = {"event_id": "evt_1", "result": "discarded"}
        discarded = mock_server.outbox_delete_dead_letter("evt_1", env="env_default")
        assert MockHandler.requests[2]["method"] == "DELETE"
        assert discarded["result"] == "discarded"

    def test_projections_catchup_round_trip(self, mock_server: IronflowClient) -> None:
        # The catch-up routes block server-side but are ordinary
        # request/response JSON — registered with addT, not addStreaming.
        # Keys are lowerCamelCase: waitResponseToMap names them explicitly and
        # does not use the proto's snake_case JSON tags.
        MockHandler.response_body = {
            "caughtUp": True,
            "timedOut": False,
            "currentSeq": 42,
            "targetSeq": 42,
            "behindByEvents": 0,
            "rebuilding": False,
            "mode": "managed",
        }
        result = mock_server.projections_list_catchup(
            "order_totals", min_seq=42, timeout=5000
        )
        assert MockHandler.requests[0]["path"].startswith(
            "/api/v1/projections/order_totals/catchup?"
        )
        query = parse_qs(urlparse(MockHandler.requests[0]["path"]).query)
        assert query["minSeq"] == ["42"]
        assert query["timeout"] == ["5000"]
        assert result["caughtUp"] is True
        assert result["behindByEvents"] == 0

    def test_projections_wait_for_event(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {
            "caughtUp": True,
            "timedOut": False,
            "currentSeq": 7,
            "targetSeq": 7,
            "behindByEvents": 0,
            "rebuilding": False,
            "mode": "managed",
        }
        result = mock_server.projections_wait_for_event(
            {"eventId": "evt_1", "projection": "order_totals", "timeoutMs": 5000}
        )
        assert MockHandler.requests[0]["method"] == "POST"
        assert MockHandler.requests[0]["path"] == "/api/v1/projections/wait-for-event"
        assert MockHandler.requests[0]["body"]["eventId"] == "evt_1"
        assert result["mode"] == "managed"

    def test_projections_catchup_batch(self, mock_server: IronflowClient) -> None:
        # minSeq goes on the wire as a STRING. The handler decodes with
        # UseNumber and ParseUints it, so a string survives values above 2^53
        # that a JSON number would round.
        MockHandler.response_body = {
            "results": [
                {
                    "result": {
                        "caughtUp": True,
                        "timedOut": False,
                        "currentSeq": 9007199254740993,
                        "targetSeq": 9007199254740993,
                        "behindByEvents": 0,
                        "rebuilding": False,
                        "mode": "managed",
                    }
                },
                {"error": "projection not found"},
            ]
        }
        result = mock_server.projections_catchup_batch(
            {
                "items": [
                    {"name": "order_totals", "minSeq": "9007199254740993"},
                    {"name": "missing", "minSeq": "1"},
                ],
                "timeoutMs": 5000,
            }
        )
        assert MockHandler.requests[0]["path"] == "/api/v1/projections/catchup/batch"
        assert MockHandler.requests[0]["body"]["items"][0]["minSeq"] == (
            "9007199254740993"
        )
        # Each result carries error or result, never both.
        assert result["results"][0]["result"]["caughtUp"] is True
        assert result["results"][1]["error"] == "projection not found"

    def test_policies_versions_and_dry_run(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = [
            {"policy_id": "pol_1", "version": 2, "name": "deny-prod"},
            {"policy_id": "pol_1", "version": 1, "name": "deny-prod"},
        ]
        versions = mock_server.policies_list_versions("pol_1")
        assert MockHandler.requests[0]["path"] == "/api/v1/policies/pol_1/versions"
        assert versions[0]["version"] == 2

        # dry-run always answers 200: a compile or eval failure is reported in
        # the body, not as an HTTP error, so the client never raises for it.
        MockHandler.response_body = {
            "matched": False,
            "compile_error": "undeclared reference to 'nope'",
            "condition_source": "nope == 1",
        }
        dry = mock_server.policies_dry_run({"condition": "nope == 1"})
        assert MockHandler.requests[1]["path"] == "/api/v1/policies/dry-run"
        assert dry["matched"] is False
        assert "undeclared" in dry["compile_error"]

    def test_policy_templates_install(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {
            "template_id": "tmpl_baseline",
            "version": "1.0.0",
            "policies": [{"id": "pol_1", "name": "deny-prod"}],
            "policies_count": 1,
        }
        result = mock_server.policy_templates_install("tmpl_baseline")
        assert MockHandler.requests[0]["method"] == "POST"
        assert (
            MockHandler.requests[0]["path"]
            == "/api/v1/policy-templates/tmpl_baseline/install"
        )
        assert result["policies_count"] == 1

    def test_runs_list_streams(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {"entity_ids": ["order-42", "order-43"]}
        result = mock_server.runs_list_streams("run_1")
        assert MockHandler.requests[0]["path"] == "/api/v1/runs/run_1/streams"
        assert result["entity_ids"] == ["order-42", "order-43"]

    def test_debounce_list_entries(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = [
            {
                "environment_id": "env_default",
                "function_id": "fn_1",
                "debounce_key": "user:1",
                "event_id": "evt_1",
                "function_version": 1,
                "period_ms": 500,
                "armed_at": "2026-08-16T00:00:00.000Z",
                "fires_at": "2026-08-16T00:00:00.500Z",
            }
        ]
        result = mock_server.debounce_list_entries()
        assert MockHandler.requests[0]["path"] == "/api/v1/debounce/entries"
        assert result[0]["period_ms"] == 500

    # ── groups added by the 22 platform-route registration ───────────────

    def test_platform_users_round_trip(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = [
            {
                "id": "puser_1",
                "email": "admin@example.com",
                "name": "Ada",
                "is_active": True,
            }
        ]
        listed = mock_server.platform_users_list()
        assert MockHandler.requests[0]["path"] == "/api/v1/platform/users"
        assert listed[0]["id"] == "puser_1"

        # 201 Created. The generated method does not read the status, so this
        # pins the body shape; the 201 itself lives in api/openapi.json.
        MockHandler.response_status = 201
        MockHandler.response_body = {
            "id": "puser_2",
            "email": "ops@example.com",
            "name": "Grace",
            "is_active": True,
        }
        created = mock_server.platform_users_create(
            {
                "email": "ops@example.com",
                "password": "hunter2",
                "name": "Grace",
                "role_ids": ["role_platform_operator"],
            }
        )
        assert MockHandler.requests[1]["method"] == "POST"
        assert MockHandler.requests[1]["body"]["role_ids"] == ["role_platform_operator"]
        assert created["id"] == "puser_2"

        # PUT, not PATCH — the platform surface spells update differently from
        # the tenant one (PATCH /api/v1/users/{id}).
        MockHandler.response_status = 200
        MockHandler.response_body = {
            "id": "puser_2",
            "email": "ops@example.com",
            "name": "Grace H",
            "is_active": False,
        }
        updated = mock_server.platform_users_update("puser_2", {"is_active": False})
        assert MockHandler.requests[2]["method"] == "PUT"
        assert MockHandler.requests[2]["path"] == "/api/v1/platform/users/puser_2"
        assert updated["is_active"] is False

        # 204, no body: the handler ends in a bare WriteHeader.
        MockHandler.response_status = 204
        assert mock_server.platform_users_delete("puser_2") is None
        assert MockHandler.requests[3]["method"] == "DELETE"

    def test_platform_roles_round_trip(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = [
            {
                "id": "role_platform_admin",
                "org_id": "org_platform",
                "name": "platform_admin",
            }
        ]
        listed = mock_server.platform_roles_list()
        assert MockHandler.requests[0]["path"] == "/api/v1/platform/roles"
        assert listed[0]["name"] == "platform_admin"

        MockHandler.response_status = 201
        MockHandler.response_body = {
            "id": "prole_1",
            "org_id": "org_platform",
            "name": "platform_auditor",
        }
        created = mock_server.platform_roles_create(
            {"name": "auditor", "policy_ids": ["ppol_1"]}
        )
        # The handler force-prefixes "platform_" onto the name it is given.
        assert MockHandler.requests[1]["body"]["name"] == "auditor"
        assert created["name"] == "platform_auditor"

        MockHandler.response_status = 204
        assert mock_server.platform_roles_delete("prole_1") is None

    def test_platform_policies_round_trip(self, mock_server: IronflowClient) -> None:
        MockHandler.response_status = 201
        MockHandler.response_body = {
            "id": "ppol_1",
            "org_id": "org_platform",
            "name": "deny-tenant-delete",
            "effect": "deny",
            "actions": "tenants:delete",
            "resources": "*",
        }
        created = mock_server.platform_policies_create(
            {
                "name": "deny-tenant-delete",
                "effect": "deny",
                "actions": "tenants:delete",
                "resources": "*",
            }
        )
        assert MockHandler.requests[0]["path"] == "/api/v1/platform/policies"
        assert created["effect"] == "deny"

        MockHandler.response_status = 200
        MockHandler.response_body = dict(created, actions="tenants:*")
        updated = mock_server.platform_policies_update(
            "ppol_1", {"actions": "tenants:*"}
        )
        assert MockHandler.requests[1]["method"] == "PUT"
        assert updated["actions"] == "tenants:*"

    def test_platform_tenants_round_trip(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = [
            {
                "id": "org_acme",
                "name": "Acme",
                "created_at": "2026-08-16T00:00:00Z",
                "updated_at": "2026-08-16T00:00:00Z",
            }
        ]
        listed = mock_server.platform_tenants_list()
        assert MockHandler.requests[0]["path"] == "/api/v1/platform/tenants"
        assert listed[0]["id"] == "org_acme"

        # The platform provision body is just a name — unlike
        # POST /api/v1/tenants/provision, which also takes env_name and mints
        # an API key.
        MockHandler.response_status = 201
        MockHandler.response_body = {
            "id": "org_new",
            "name": "Initech",
            "created_at": "2026-08-16T00:00:00Z",
            "updated_at": "2026-08-16T00:00:00Z",
        }
        created = mock_server.platform_tenants_create({"name": "Initech"})
        assert MockHandler.requests[1]["body"] == {"name": "Initech"}
        assert created["name"] == "Initech"

        MockHandler.response_status = 204
        assert mock_server.platform_tenants_delete("org_new") is None

    def test_platform_audit_list(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = {
            "events": [{"id": "aud_1", "event_type": "platform.role.blocked"}],
            "total": 1,
        }
        result = mock_server.platform_audit_list(
            event_type="platform.role.blocked",
            from_="2026-08-01",
            limit=1,
            platform_key_id="ifplatform_1",
            impersonated_org_id="org_acme",
        )
        assert MockHandler.requests[0]["path"].startswith("/api/v1/platform/audit?")
        query = parse_qs(urlparse(MockHandler.requests[0]["path"]).query)
        assert query["event_type"] == ["platform.role.blocked"]
        assert query["platform_key_id"] == ["ifplatform_1"]
        assert query["impersonated_org_id"] == ["org_acme"]
        assert query["limit"] == ["1"]
        # `from` is a Python keyword, so the kwarg is from_ and the wire key
        # stays `from` — same as audit_list above.
        assert query["from"] == ["2026-08-01"]
        # {events,total}, NOT the {events,total_count,next_cursor} that
        # GET /api/v1/audit returns. Two different shapes, two schemas.
        assert result["total"] == 1

    def test_platform_bootstrap_and_login(self, mock_server: IronflowClient) -> None:
        # Bootstrap answers 201 with three keys, not the created user row.
        MockHandler.response_status = 201
        MockHandler.response_body = {
            "id": "puser_1",
            "email": "admin@example.com",
            "name": "Ada",
        }
        created = mock_server.platform_bootstrap(
            {"email": "admin@example.com", "password": "hunter2", "name": "Ada"}
        )
        assert MockHandler.requests[0]["path"] == "/api/v1/platform/bootstrap"
        assert created == {
            "id": "puser_1",
            "email": "admin@example.com",
            "name": "Ada",
        }

        # Login answers 200 — not 201, despite being a POST.
        MockHandler.response_status = 200
        MockHandler.response_body = {
            "token": "eyJhbGciOiJIUzI1NiJ9.fake.sig",
            "user": {
                "id": "puser_1",
                "email": "admin@example.com",
                "name": "Ada",
                "roles": ["platform_admin"],
            },
        }
        result = mock_server.platform_auth_login(
            {"email": "admin@example.com", "password": "hunter2"}
        )
        assert MockHandler.requests[1]["path"] == "/api/v1/platform/auth/login"
        assert result["user"]["roles"] == ["platform_admin"]


class TestCapacityLeaseNeverExposesRawToken:
    """Lease rows must carry the token HASH only, never the raw token.

    Same reasoning as TestSecretsNeverExposeValue: this asserts on the
    *generated* TypedDict, not on a mock body. Client methods `cast` the parsed
    JSON, which is a runtime no-op, so `"lease_token" not in result` would only
    re-read the dict this test itself authored. Adding a raw-token field to
    store.ConcurrencyLease and regenerating must turn this red.
    """

    def test_lease_type_exposes_hash_not_token(self) -> None:
        keys = (
            models.ConcurrencyLease.__required_keys__
            | models.ConcurrencyLease.__optional_keys__
        )
        assert "lease_token_hash" in keys, f"lease hash missing: {keys}"
        assert "lease_token" not in keys, f"ConcurrencyLease leaks a raw token: {keys}"


class TestExportIsNotGenerated:
    """GET /api/v1/export must NOT appear on the generated client.

    It streams NDJSON — Content-Type: application/x-ndjson, WriteHeader(200),
    then one JSON envelope per line with a 30s heartbeat, never WriteJSON. A
    generated request/response method would try to json.loads the whole stream.
    It is registered with addStreaming for exactly this reason; if someone
    "fixes" that to addT, a method appears here and this turns red.
    """

    def test_no_export_method(self) -> None:
        assert not [m for m in dir(IronflowClient) if "export" in m]


class TestSecretsNeverExposeValue:
    """The secrets endpoints must return metadata only.

    This asserts on the *generated* TypedDict, not on a mock body. A canned
    response cannot test this: client methods `cast` the parsed JSON, which is
    a runtime no-op, so `"value" not in result` would only re-read the dict the
    test itself authored. Adding a Value field to secretInfoResponse in
    secrets_handler.go and regenerating must turn this red.
    """

    def test_secret_response_type_never_exposes_value(self) -> None:
        keys = (
            models.SecretInfoResponse.__required_keys__
            | models.SecretInfoResponse.__optional_keys__
        )
        assert "value" not in keys, f"SecretInfoResponse leaks a value field: {keys}"


class TestPlatformLoginDeliberatelyReturnsAToken:
    """POST /api/v1/platform/auth/login puts a live admin JWT in the body.

    Same technique as TestSecretsNeverExposeValue, inverted: it asserts on the
    *generated* TypedDict, because a canned mock body would only re-read the
    dict this test authored. The handler both sets the HttpOnly
    `ironflow_dashboard_token` cookie AND writes the token, since a non-browser
    caller has no cookie jar. That is deliberate, so the schema declares it —
    an annotation that hid the field would leave callers with no way to
    authenticate. If someone stops returning it, this turns red and forces the
    decision to be made explicitly rather than by omission.
    """

    def test_login_response_type_declares_the_token(self) -> None:
        keys = (
            models.PlatformLoginResponse.__required_keys__
            | models.PlatformLoginResponse.__optional_keys__
        )
        assert "token" in keys, f"PlatformLoginResponse hides its token: {keys}"
        assert "user" in keys, f"PlatformLoginResponse lost its user block: {keys}"

    def test_platform_user_row_never_exposes_the_password_hash(self) -> None:
        """store.PlatformUser tags PasswordHash json:"-"; keep it that way.

        GET/POST/PUT /api/v1/platform/users all write the raw store row.
        Dropping that tag would put every admin's argon2 hash in the manifest,
        the OpenAPI spec and the generated client.
        """
        keys = (
            models.PlatformUser.__required_keys__
            | models.PlatformUser.__optional_keys__
        )
        assert "password_hash" not in keys, f"PlatformUser leaks a hash: {keys}"
        assert "PasswordHash" not in keys, f"PlatformUser leaks a hash: {keys}"


class TestTypedQueryParams:
    """Query parameters generated from the manifest's schemas.

    The Go generator test pins the emitted signature; this pins the wire.
    """

    def test_platform_true_is_lowercase(self, mock_server: IronflowClient) -> None:
        """handleListAPIKeys compares `== "true"`.

        Python's str(True) is "True", which fails that comparison silently:
        the endpoint returns every API key instead of only the platform ones.
        """
        MockHandler.response_body = []
        mock_server.api_keys_list(platform=True)
        query = parse_qs(urlparse(MockHandler.requests[0]["path"]).query)
        assert query["platform"] == ["true"]

    def test_platform_false_is_sent(self, mock_server: IronflowClient) -> None:
        MockHandler.response_body = []
        mock_server.api_keys_list(platform=False)
        query = parse_qs(urlparse(MockHandler.requests[0]["path"]).query)
        assert query["platform"] == ["false"]

    def test_omitted_param_leaves_path_clean(self, mock_server: IronflowClient) -> None:
        """The default None must not reach the wire as `?platform=None`.

        The generated method passes the dict unconditionally, so this rests
        entirely on _with_query dropping None values.
        """
        MockHandler.response_body = []
        mock_server.api_keys_list()
        assert MockHandler.requests[0]["path"] == "/api/v1/apikeys"


class TestMethodCount:
    """Sanity check that we have the expected number of methods."""

    def test_has_at_least_100_methods(self) -> None:
        client = IronflowClient()
        methods = [m for m in dir(client) if not m.startswith("_")]
        assert len(methods) >= 100, f"Expected 100+ methods, got {len(methods)}"
