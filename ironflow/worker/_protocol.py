"""Wire shapes for the REST pull-worker protocol. No I/O.

Field names match ``internal/server/worker_rest.go`` exactly.
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict, cast

STALE_EXECUTION = "STALE_EXECUTION"
MAX_CHECKPOINT_STEPS = 500


class _CompletedStepReq(TypedDict):
    step_id: str
    name: str
    output: Any


class CompletedStep(_CompletedStepReq, total=False):
    status: Literal["completed", "failed"]
    error: Any


class _JobEventReq(TypedDict):
    id: str
    name: str
    data: Any
    timestamp: str


class JobEvent(_JobEventReq, total=False):
    version: int
    source: str
    idempotency_key: str
    metadata: dict[str, Any] | None


class JobContext(TypedDict, total=False):
    secrets: dict[str, str]


class _JobAssignmentReq(TypedDict):
    job_id: str
    run_id: str
    function_id: str
    attempt: int
    event: JobEvent
    completed_steps: list[CompletedStep]
    execution_seq: int
    lease_token: str


class JobAssignment(_JobAssignmentReq, total=False):
    step_sequence_base: int
    context: JobContext


class StepError(TypedDict):
    message: str
    retryable: bool


class _StepResultReq(TypedDict):
    id: str
    name: str
    type: Literal["invoke", "compensate"]
    status: Literal["completed", "failed"]
    started_at: str
    ended_at: str
    duration_ms: int


class StepResult(_StepResultReq, total=False):
    output: Any
    error: StepError
    compensation_for: str


class JobError(TypedDict):
    message: str
    code: str
    retryable: bool


class EventFilter(TypedDict, total=False):
    event: str
    payload: Any
    match: str
    match_value: str
    timeout: str


class YieldInfo(TypedDict, total=False):
    step_id: str
    type: Literal["sleep", "wait_for_event", "invoke_function", "invoke_function_async"]
    until: str
    event_filter: EventFilter
    function_id: str
    input: Any
    invoke_timeout_ms: int


_REQUIRED: tuple[tuple[str, type], ...] = (
    ("job_id", str), ("run_id", str), ("function_id", str), ("attempt", int),
    ("event", dict), ("completed_steps", list), ("execution_seq", int), ("lease_token", str),
)
_EVENT_REQUIRED: tuple[tuple[str, type], ...] = (("id", str), ("name", str), ("timestamp", str))


def _check(obj: dict[str, Any], key: str, typ: type, where: str) -> None:
    value = obj.get(key)
    if not isinstance(value, typ) or (typ is int and isinstance(value, bool)):
        raise ValueError(f"{where} field {key!r} is missing or not {typ.__name__}")


def parse_jobs(body: object) -> list[object]:
    """Return the raw assignments from a poll reply (current batched shape only)."""
    if not isinstance(body, dict) or not isinstance(body.get("jobs"), list):
        raise TypeError('poll reply is not {"jobs": [...]}')
    return cast("list[object]", body["jobs"])


def parse_job(raw: object) -> JobAssignment:
    """Validate one job assignment. Raise ValueError when it is malformed."""
    if not isinstance(raw, dict):
        raise ValueError("job assignment is not an object")  # noqa: TRY004 — malformed wire shape contract.
    for key, typ in _REQUIRED:
        _check(raw, key, typ, "job assignment")
    event = raw["event"]
    for key, typ in _EVENT_REQUIRED:
        _check(event, key, typ, "job event")
    if "data" not in event:
        raise ValueError("job event field 'data' is missing")
    for key in ("version",):
        if key in event:
            _check(event, key, int, "job event")
    for key in ("source", "idempotency_key"):
        if key in event:
            _check(event, key, str, "job event")

    for step in raw["completed_steps"]:
        if not isinstance(step, dict):
            raise ValueError("job assignment field 'completed_steps' has a non-object entry")  # noqa: TRY004
        for key, typ in (("step_id", str), ("name", str)):
            _check(step, key, typ, "job assignment completed_steps")
        if "output" not in step:
            raise ValueError("completed step field 'output' is missing")
        if "status" in step:
            _check(step, "status", str, "job assignment completed_steps")

    if "step_sequence_base" in raw:
        _check(raw, "step_sequence_base", int, "job assignment")
    if "context" in raw:
        context = raw["context"]
        if not isinstance(context, dict):
            raise ValueError("job assignment field 'context' is not an object")
        if "secrets" in context:
            secrets = context["secrets"]
            if not isinstance(secrets, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in secrets.items()):
                raise ValueError("job context field 'secrets' must map strings to strings")
    return cast(JobAssignment, raw)


def fence(job: JobAssignment) -> dict[str, object]:
    """Return the fence fields every ack, checkpoint and report must echo."""
    return {"execution_seq": job["execution_seq"], "lease_token": job["lease_token"]}
