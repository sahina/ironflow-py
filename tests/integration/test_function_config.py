"""A real server accepts every function-config field the Python worker sends (#2394).

The unit tests prove the body parses as a RegisterFunctionRequest. Only a live
server proves the engine keeps each field: registration goes through the
worker's own HTTP path, and GetFunction reads the stored definition back.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

from ironflow import IronflowRPC
from ironflow.rpc.v1 import DeleteFunctionRequest, GetFunctionRequest
from ironflow.worker import Worker, function


def test_worker_registration_persists_every_config_field(
    rpc: IronflowRPC, server_url: str, api_key: str
) -> None:
    fn_id = f"py-cfg-{uuid.uuid4().hex[:8]}"

    @function(
        id=fn_id, triggers=[{"event": "pysdk.cfg"}], description="config parity",
        debounce={"period": "2s", "key": "data.customerId", "max_wait": "10s"},
        cancel_on=[{"event": "pysdk.cancel", "match": "data.orderId"}],
        actor_key="data.customerId", secrets=["PY_SDK_TEST"], recording=True,
        recording_profile="steps", metadata={"team": "sdk"},
    )
    async def handler(ctx: Any) -> None: ...

    worker = Worker(functions=[handler], server_url=server_url, api_key=api_key)
    asyncio.run(worker._register_functions())
    try:
        got = rpc.functions.get(GetFunctionRequest(id=fn_id))
        assert got.description == "config parity"
        assert got.debounce is not None
        assert (got.debounce.period_ms, got.debounce.key, got.debounce.max_wait_ms) == (
            2000, "data.customerId", 10000)
        assert [(c.event, c.match) for c in got.cancel_on] == [("pysdk.cancel", "data.orderId")]
        assert got.actor_key == "data.customerId"
        assert got.recording is True
        assert got.recording_profile == "steps"
        metadata = json.loads(got.to_json())["metadata"]
        assert metadata["team"] == "sdk" and metadata["__ironflow_code_hash"] == handler.code_hash
    finally:
        rpc.functions.delete(DeleteFunctionRequest(id=fn_id))
