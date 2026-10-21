"""health-ready and capabilities-discovery (#3003) through the Python REST client.

`health`, `ready` and `capabilities` read the public routes GET /health, GET /ready
and GET /api/v1/capabilities.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ironflow import IronflowClient


def test_capabilities_discovery(server_url: str, readiness_seed: dict[str, Any]) -> None:
    client = IronflowClient(server_url)  # the route is public: no key
    caps = client.capabilities()
    assert caps["version"]
    assert "websocket" in caps["transports"]
    assert {"replay", "wildcard-patterns"} <= set(caps["features"])
    assert caps["auth_required"] is True


def test_health_ready(server_url: str, readiness_seed: dict[str, Any]) -> None:
    client = IronflowClient(server_url)
    health = client.health()
    assert health["status"] == "healthy" and health["version"] == client.capabilities()["version"]
    age = datetime.now(timezone.utc) - datetime.fromisoformat(health["timestamp"].replace("Z", "+00:00"))
    assert abs(age.total_seconds()) < 60
    assert client.ready()["status"] == "ready"
