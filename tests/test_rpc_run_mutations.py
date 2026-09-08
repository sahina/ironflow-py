"""Resume and patch responses through the generated service."""

import asyncio
from typing import Any

import pytest
from protobuf.wkt import Struct

from ironflow import AsyncIronflowRPC, IronflowRPC, IronflowRPCError
from ironflow.rpc.v1 import PatchStepRequest, ResumeRunRequest, RunStatus, StepStatus

from .rpc_server import serve


@pytest.mark.parametrize("client_cls", [IronflowRPC, AsyncIronflowRPC])
def test_run_mutations(client_cls: Any) -> None:
    async def exercise() -> None:
        with serve() as srv:
            client = client_cls(server_url=srv.url)

            async def resolve(result: Any) -> Any:
                return await result if asyncio.iscoroutine(result) else result

            try:
                run = await resolve(
                    client.runs.resume(ResumeRunRequest(run_id="run", from_step="step"))
                )
                assert run.id == "run"
                assert run.status == RunStatus.RUNNING
                assert run.resume_from_step == "step"
                assert run.parent_run_id == "parent"
                output = Struct.from_python({"result": "fixed"})
                step = await resolve(
                    client.runs.patch_step(
                        PatchStepRequest(step_id="step", output=output, reason="fix")
                    )
                )
                assert step.id == "step"
                assert step.status == StepStatus.COMPLETED
                assert step.output.to_python() == {"result": "fixed"}
                assert step.patched_by == "fix"
                with pytest.raises(IronflowRPCError) as exc:
                    await resolve(
                        client.runs.patch_step(PatchStepRequest(step_id="missing"))
                    )
                assert exc.value.retryable is False
            finally:
                if isinstance(client, AsyncIronflowRPC):
                    await client.aclose()
                else:
                    client.close()

    asyncio.run(exercise())
