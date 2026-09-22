"""Run readers and cancellation use the generated protocol and preserve payloads."""

import asyncio
from typing import Any

import pytest

from ironflow import AsyncIronflowRPC, IronflowRPC, IronflowRPCError
from ironflow.rpc.v1 import (
    CancelRunRequest,
    DeleteRunRequest,
    DeleteRunsRequest,
    GetAuditTrailRequest,
    GetRunRequest,
    GetRunStepsRequest,
    ListRunsRequest,
    RedactRunRequest,
    RedactStepRequest,
    RunStatus,
)

from .rpc_server import serve


@pytest.mark.parametrize("client_cls", [IronflowRPC, AsyncIronflowRPC])
def test_runs(client_cls: Any) -> None:
    async def exercise() -> None:
        with serve() as server:
            client = client_cls(server_url=server.url)

            async def resolve(value: Any) -> Any:
                return await value if asyncio.iscoroutine(value) else value

            try:
                listed = await resolve(
                    client.runs.list(ListRunsRequest(offset=2, search="run"))
                )
                assert listed.total_count == 3
                run = await resolve(client.runs.get(GetRunRequest(id="run")))
                assert run.event_name == "order.created"
                assert run.input_value.to_python() is False
                steps = await resolve(
                    client.runs.get_steps(GetRunStepsRequest(run_id="run"))
                )
                assert steps.steps[0].duration_ms_full == 9007199254740993
                assert steps.steps[0].compensation_for == "charge"
                assert steps.steps[0].output_value.to_python() == "output"
                cancelled = await resolve(
                    client.runs.cancel(CancelRunRequest(id="run", reason="requested"))
                )
                assert cancelled.status == RunStatus.CANCELLED
                await resolve(client.runs.delete(DeleteRunRequest(id="run")))
                deleted = await resolve(
                    client.runs.delete_many(DeleteRunsRequest(function_id="fn"))
                )
                assert deleted.deleted == 3
                # Redaction keeps the row and replaces only the payload, so
                # these answer Empty rather than a mutated Run.
                await resolve(client.runs.redact(RedactRunRequest(run_id="run")))
                await resolve(
                    client.runs.redact_step(RedactStepRequest(step_id="step"))
                )
                audit = await resolve(
                    client.audit.get_trail(
                        GetAuditTrailRequest(run_id="run", event_type="run.created")
                    )
                )
                assert audit.events[0].metadata_value.to_python() == {"attempt": 2}
                assert audit.events[0].payload_value.to_python() == ["payload"]
                with pytest.raises(IronflowRPCError) as exc:
                    await resolve(client.runs.get(GetRunRequest(id="missing")))
                assert exc.value.retryable is False
            finally:
                if isinstance(client, AsyncIronflowRPC):
                    await client.aclose()
                else:
                    client.close()

    asyncio.run(exercise())
