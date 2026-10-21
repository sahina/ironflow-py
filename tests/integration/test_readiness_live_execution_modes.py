"""The execution-modes group (#3003) through the Python SDK.

exec-push-serve is proven by test_push_serve.py in this directory. A Python
worker polls over REST, so the workers list is read through the REST client.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from ironflow import IronflowClient


def test_exec_workers_list(readiness_seed: dict[str, Any], server_url: str, api_key: str) -> None:
    rest = IronflowClient(server_url, api_key=api_key)
    worker_id = f"readiness-worker-python-{uuid.uuid4().hex[:8]}"
    function_id = f"readiness-worker-fn-{worker_id}"
    rest.workers_register(worker_id, {
        "hostname": "readiness-host",
        "function_ids": [function_id],
        "max_concurrent_jobs": 3,
        "labels": {"pool": "readiness"},
        "version": {"sdk": "python", "runtime": "pytest"},
    })

    listed = rest.workers_list()
    assert listed["count"] == len(listed["workers"])
    worker = next(w for w in listed["workers"] if w["id"] == worker_id)
    assert worker["hostname"] == "readiness-host"
    assert worker["function_ids"] == [function_id]
    assert worker["max_concurrent"] == 3
    assert worker["labels"] == {"pool": "readiness"}
    assert worker["version"] == {"sdk": "python", "runtime": "pytest"}
    assert (worker["active_jobs"], worker["transport"]) == (0, "rest-polling")
    for stamp in (worker["registered_at"], worker["last_heartbeat"]):
        datetime.fromisoformat(stamp.replace("Z", "+00:00"))
