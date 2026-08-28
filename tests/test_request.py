"""Generated header arguments and the request() escape hatch.

Headers declared in the route manifest are generated as keyword arguments.
The escape hatch remains available for undeclared headers and for query
parameters on routes without schema annotations.
"""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

from ironflow import IronflowClient
from tests.harness import Response


def client(server) -> IronflowClient:
    return IronflowClient(server_url=server.url)


class TestQueryParams:
    def test_params_are_encoded(self, server) -> None:
        server.script(Response(body={"runs": []}))
        client(server).request(
            "GET", "/api/v1/runs", params={"limit": 50, "status": "failed"}
        )

        query = parse_qs(urlparse(server.requests[0]["path"]).query)
        assert query["limit"] == ["50"]
        assert query["status"] == ["failed"]

    def test_params_none_leaves_path_clean(self, server) -> None:
        server.script(Response(body={}))
        client(server).request("GET", "/api/v1/runs")
        assert server.requests[0]["path"] == "/api/v1/runs"

    def test_empty_params_leaves_path_clean(self, server) -> None:
        server.script(Response(body={}))
        client(server).request("GET", "/api/v1/runs", params={})
        assert server.requests[0]["path"] == "/api/v1/runs"

    def test_none_valued_params_are_dropped(self, server) -> None:
        server.script(Response(body={}))
        client(server).request(
            "GET", "/api/v1/runs", params={"limit": 10, "cursor": None}
        )
        query = parse_qs(urlparse(server.requests[0]["path"]).query)
        assert "cursor" not in query
        assert query["limit"] == ["10"]

    def test_booleans_are_lowercased(self, server) -> None:
        """Go and JS send `true`, not Python's `True`."""
        server.script(Response(body={}))
        client(server).request(
            "GET", "/api/v1/runs", params={"active": True, "done": False}
        )
        query = parse_qs(urlparse(server.requests[0]["path"]).query)
        assert query["active"] == ["true"]
        assert query["done"] == ["false"]

    def test_params_append_to_existing_query(self, server) -> None:
        server.script(Response(body={}))
        client(server).request("GET", "/api/v1/runs?env=prod", params={"limit": 5})
        query = parse_qs(urlparse(server.requests[0]["path"]).query)
        assert query["env"] == ["prod"]
        assert query["limit"] == ["5"]

    def test_values_are_url_escaped(self, server) -> None:
        server.script(Response(body={}))
        client(server).request("GET", "/api/v1/runs", params={"q": "a b&c=d"})
        query = parse_qs(urlparse(server.requests[0]["path"]).query)
        assert query["q"] == ["a b&c=d"]


class TestHeaders:
    def test_generated_conditional_header_is_sent_and_none_is_omitted(
        self, server
    ) -> None:
        server.script(Response(body={"revision": 4}))
        client(server).kv_update_buckets_keys(
            "profiles", "alice", body={"name": "Alice"}, if_match="3"
        )

        sent = server.requests[0]["headers"]
        assert sent.get("If-Match") == "3"
        assert "If-None-Match" not in sent

    def test_generated_required_environment_header_is_sent(self, server) -> None:
        server.script(Response(body={"name": "api-token"}))
        client(server).secrets_get("api-token", x_ironflow_environment="staging")

        assert server.requests[0]["headers"].get("X-Ironflow-Environment") == "staging"

    def test_custom_headers_sent(self, server) -> None:
        server.script(Response(body={}))
        client(server).request(
            "GET",
            "/api/v1/secrets",
            headers={"X-Ironflow-Environment": "staging", "If-Match": 'W/"3"'},
        )
        sent = server.requests[0]["headers"]
        assert sent.get("X-Ironflow-Environment") == "staging"
        assert sent.get("If-Match") == 'W/"3"'

    def test_auth_header_still_applied(self, server) -> None:
        server.script(Response(body={}))
        c = IronflowClient(server_url=server.url, api_key="ifkey_test")
        c.request("GET", "/api/v1/runs", headers={"X-Custom": "1"})
        sent = server.requests[0]["headers"]
        assert sent.get("Authorization") == "Bearer ifkey_test"
        assert sent.get("X-Custom") == "1"

    def test_content_type_always_json(self, server) -> None:
        server.script(Response(body={}))
        client(server).request("POST", "/api/v1/events", body={"name": "x"})
        assert server.requests[0]["headers"].get("Content-Type") == "application/json"


class TestBody:
    def test_body_is_json_encoded(self, server) -> None:
        server.script(Response(body={"ok": 1}))
        client(server).request("POST", "/api/v1/events", body={"name": "order.placed"})
        assert server.requests[0]["body"] == {"name": "order.placed"}

    def test_no_body_sends_nothing(self, server) -> None:
        server.script(Response(body={}))
        client(server).request("GET", "/api/v1/runs")
        assert server.requests[0]["body"] is None


class TestSystemWrappers:
    def test_health(self, server) -> None:
        server.script(
            Response(
                body={
                    "status": "healthy",
                    "timestamp": "2026-08-28T00:00:00Z",
                    "version": "0.31.0",
                }
            )
        )
        result = client(server).health()
        assert result["status"] == "healthy"
        assert server.requests[0]["path"] == "/health"

    def test_ready(self, server) -> None:
        server.script(Response(body={"status": "ready"}))
        result = client(server).ready()
        assert result["status"] == "ready"
        assert server.requests[0]["path"] == "/ready"

    def test_capabilities(self, server) -> None:
        server.script(
            Response(
                body={
                    "transports": ["websocket"],
                    "features": ["replay"],
                    "version": "0.31.0",
                    "auth_required": True,
                }
            )
        )
        result = client(server).capabilities()
        assert result["auth_required"] is True
        assert result["features"] == ["replay"]
        assert server.requests[0]["path"] == "/api/v1/capabilities"
