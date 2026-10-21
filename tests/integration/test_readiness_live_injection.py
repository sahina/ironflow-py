"""The scoped-injection group (#3003) through the Python RPC client.

Each SDK has its own seeded function with a completed run to patch, and a run
held in a one-hour sleep behind two failed steps. Following an injected value
through later steps after a resume is left to the engine tests, because the
held run cannot get past its sleep here.
"""

from __future__ import annotations

import json
import time
from typing import Any

import pytest
from protobuf.wkt import Struct

from ironflow import IronflowRPCError
from ironflow.rpc.v1 import (
    GetAuditTrailRequest,
    GetPausedStateRequest,
    GetRunStepsRequest,
    InjectStepOutputRequest,
    PatchStepRequest,
    PauseRunRequest,
    StepStatus,
)


def _runs(readiness_seed: dict[str, Any]) -> dict[str, Any]:
    return readiness_seed["groups"]["injection"]["python"]


def _step(rpc: Any, run_id: str, step_id: str) -> Any:
    return next(s for s in rpc.runs.get_steps(GetRunStepsRequest(run_id=run_id)).steps if s.id == step_id)


def test_step_patch(rpc: Any, readiness_seed: dict[str, Any]) -> None:
    r = _runs(readiness_seed)
    patched = {"n": 43, "who": "patched"}
    step = rpc.runs.patch_step(PatchStepRequest(step_id=r["doneStepId"], output=Struct.from_python(patched), reason="readiness patch"))
    assert step.id == r["doneStepId"] and step.output.to_python() == patched
    read = _step(rpc, r["doneRunId"], r["doneStepId"])
    assert read.output.to_python() == patched
    assert read.original_output.to_python() == readiness_seed["stepOutput"]
    assert read.patched_at is not None

    # A failed step becomes completed and loses its error.
    before = _step(rpc, r["holdRunId"], r["patchStepId"])
    assert before.status == StepStatus.FAILED and before.error is not None
    rpc.runs.patch_step(PatchStepRequest(step_id=r["patchStepId"], output=Struct.from_python(patched), reason="readiness recover"))
    recovered = _step(rpc, r["holdRunId"], r["patchStepId"])
    assert recovered.status == StepStatus.COMPLETED and recovered.error is None
    assert recovered.output.to_python() == patched and recovered.patched_at is not None

    # Only completed and failed steps can be patched.
    with pytest.raises(IronflowRPCError) as exc:
        rpc.runs.patch_step(PatchStepRequest(step_id=r["sleepStepId"], output=Struct.from_python(patched)))
    assert exc.value.code == "failed_precondition", exc.value.code
    with pytest.raises(IronflowRPCError) as exc:
        rpc.runs.patch_step(PatchStepRequest(step_id="no-such-step", output=Struct.from_python(patched)))
    assert exc.value.code == "not_found", exc.value.code


def test_step_inject_output(rpc: Any, readiness_seed: dict[str, Any]) -> None:
    r = _runs(readiness_seed)
    # A run that is not paused for injection refuses it.
    with pytest.raises(IronflowRPCError) as exc:
        rpc.runs.inject_step_output(InjectStepOutputRequest(run_id=r["doneRunId"], step_id=r["doneStepId"], new_output=b"{}"))
    assert exc.value.code == "failed_precondition", exc.value.code
    assert rpc.runs.pause(PauseRunRequest(run_id=r["holdRunId"])).status == "paused"
    state = rpc.runs.get_paused_state(GetPausedStateRequest(run_id=r["holdRunId"]))
    failed = next(s for s in state.steps if s.id == r["injectStepId"])
    assert failed.status == "failed" and failed.error
    steps = rpc.runs.get_steps(GetRunStepsRequest(run_id=r["holdRunId"])).steps
    write = next(s for s in steps if s.status == StepStatus.COMPLETED and s.patched_at is None and s.original_output is None)

    # A completed step: the call returns the output it replaced.
    injected = {"n": 7, "who": "José injected"}
    res = rpc.runs.inject_step_output(InjectStepOutputRequest(
        run_id=r["holdRunId"], step_id=write.id, new_output=json.dumps(injected).encode(), reason="readiness inject",
    ))
    assert res.step_id == write.id
    assert json.loads(res.previous_output) == readiness_seed["stepOutput"]
    after = _step(rpc, r["holdRunId"], write.id)
    assert after.output.to_python() == injected
    assert after.original_output.to_python() == readiness_seed["stepOutput"]

    # A failed step becomes completed and loses its error.
    rpc.runs.inject_step_output(InjectStepOutputRequest(
        run_id=r["holdRunId"], step_id=r["injectStepId"], new_output=json.dumps(injected).encode(), reason="readiness recover",
    ))
    recovered = _step(rpc, r["holdRunId"], r["injectStepId"])
    assert recovered.status == StepStatus.COMPLETED and recovered.error is None
    assert recovered.output.to_python() == injected
    state = rpc.runs.get_paused_state(GetPausedStateRequest(run_id=r["holdRunId"]))
    done = next(s for s in state.steps if s.id == r["injectStepId"])
    assert done.injected and done.status == "completed" and json.loads(done.output) == injected

    # The function records, so each injection is a step.injected audit event.
    deadline = time.monotonic() + 10
    while True:
        trail = rpc.audit.get_trail(GetAuditTrailRequest(run_id=r["holdRunId"], event_type="step.injected"))
        if len(trail.events) == 2 or time.monotonic() > deadline:
            break
        time.sleep(0.2)
    assert len(trail.events) == 2, trail.events
