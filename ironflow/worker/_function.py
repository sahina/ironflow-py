"""The ``@function`` decorator and the RegisterFunction request body."""

from __future__ import annotations

import dataclasses
import hashlib
import inspect
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Literal, TypedDict

from ._duration import Duration, to_seconds
from ._step import Event, SchemaValidationError

Handler = Callable[[Any], Awaitable[Any]]
Schema = Callable[[Any], Any]
RecordingProfile = Literal["all", "run_lifecycle", "steps"]

CODE_HASH_META_KEY = "__ironflow_code_hash"
REDACTED_MARKER_KEY = "$redacted"


class Trigger(TypedDict, total=False):
    event: str
    expression: str
    cron: str


class RetryConfig(TypedDict, total=False):
    max_attempts: int
    initial_delay: Duration
    backoff_factor: float
    max_delay: Duration


class _ConcurrencyReq(TypedDict):
    limit: int


class ConcurrencyConfig(_ConcurrencyReq, total=False):
    key: str


class _DebounceReq(TypedDict):
    period: Duration


class DebounceConfig(_DebounceReq, total=False):
    """Collapse rapid events into one run after ``period`` of quiet. ``period`` is at least 1s."""

    key: str
    max_wait: Duration


class CancelOnSpec(TypedDict):
    """Cancel the run when ``event`` arrives and its ``match`` path equals the run's."""

    event: str
    match: str


_TRIGGER_KEYS = frozenset({"event", "expression", "cron"})


class Function:
    """A function definition. The decorator returns one; it does not wrap the coroutine."""

    def __init__(
        self, *, id: str, handler: Handler, triggers: list[Trigger], name: str | None,
        retry: RetryConfig | None, timeout: Duration | None, concurrency: ConcurrencyConfig | None,
        description: str | None = None, step_timeout: Duration | None = None,
        debounce: DebounceConfig | None = None, cancel_on: list[CancelOnSpec] | None = None,
        actor_key: str | None = None, secrets: list[str] | None = None, recording: bool | None = None,
        recording_profile: RecordingProfile | None = None, recording_retention: str | None = None,
        metadata: dict[str, Any] | None = None, schema: Schema | None = None,
    ) -> None:
        self.id = id
        self.handler = handler
        self.triggers = triggers
        self.name = name
        self.retry = retry
        self.timeout = timeout
        self.concurrency = concurrency
        self.description = description
        self.step_timeout = step_timeout
        self.debounce = debounce
        self.cancel_on = cancel_on
        self.actor_key = actor_key
        self.secrets = secrets
        self.recording = recording
        self.recording_profile = recording_profile
        self.recording_retention = recording_retention
        self.metadata = metadata
        self.schema = schema
        self.code_hash = _code_hash(handler)

    def __repr__(self) -> str:
        return f"Function(id={self.id!r})"


def _code_hash(handler: Handler) -> str:
    try:
        source = inspect.getsource(handler)
    except (OSError, TypeError):
        source = getattr(handler, "__qualname__", repr(handler))
    return hashlib.sha256(source.encode()).hexdigest()[:16]


def _validate_triggers(triggers: Sequence[Trigger]) -> list[Trigger]:
    out: list[Trigger] = []
    for trigger in triggers:
        keys = set(trigger)
        if not keys or not keys <= _TRIGGER_KEYS:
            raise ValueError(f"invalid trigger {dict(trigger)!r}: allowed keys are event, expression, cron")
        if ("event" in keys) == ("cron" in keys):
            raise ValueError(f"invalid trigger {dict(trigger)!r}: set exactly one of 'event' or 'cron'")
        if not (trigger.get("event") or trigger.get("cron")):
            raise ValueError(f"invalid trigger {dict(trigger)!r}: event or cron must be non-empty")
        out.append(trigger.copy())
    return out


def _validate_config(
    step_timeout: Duration | None, debounce: DebounceConfig | None,
    cancel_on: Sequence[CancelOnSpec] | None, recording_profile: str | None,
) -> None:
    """Mirror Go validateFunctionConfig for the fields the server does not reject clearly."""
    if step_timeout is not None:
        to_seconds(step_timeout)  # rejects a negative or non-finite value at definition time
    if debounce is not None:
        period = to_seconds(debounce["period"])
        if period < 1:
            raise ValueError(f"debounce period must be >= 1s (got {period:g}s); the scheduler tick floor is 1s")
        max_wait = to_seconds(debounce.get("max_wait", 0))
        if 0 < max_wait < period:  # 0 means no cap, as in Go
            raise ValueError("debounce max_wait must be >= period")
    seen: set[tuple[str, str]] = set()
    for i, spec in enumerate(cancel_on or ()):
        if not spec.get("event") or not spec.get("match"):
            raise ValueError(f"cancel_on[{i}]: event and match must be non-empty")
        key = (spec["event"], spec["match"])
        if key in seen:
            raise ValueError(f"cancel_on[{i}]: duplicate spec {dict(spec)!r}")
        seen.add(key)
    if recording_profile is not None and recording_profile not in ("all", "run_lifecycle", "steps"):
        raise ValueError(f"recording_profile must be 'all', 'run_lifecycle' or 'steps' (got {recording_profile!r})")


def function(
    *, id: str, triggers: Sequence[Trigger] = (), name: str | None = None, description: str | None = None,
    retry: RetryConfig | None = None, timeout: Duration | None = None, step_timeout: Duration | None = None,
    concurrency: ConcurrencyConfig | None = None, debounce: DebounceConfig | None = None,
    cancel_on: Sequence[CancelOnSpec] | None = None, actor_key: str | None = None,
    secrets: Sequence[str] | None = None, recording: bool | None = None,
    recording_profile: RecordingProfile | None = None, recording_retention: str | None = None,
    metadata: dict[str, Any] | None = None, schema: Schema | None = None,
) -> Callable[[Handler], Function]:
    """Define a pull-mode function. The handler must be ``async def handler(ctx)``.

    ``step_timeout`` is the default for every ``step.run`` without its own ``timeout``.
    ``schema`` is a callable such as ``Model.model_validate``: it receives the event
    data and returns the value the handler sees as ``ctx.event.data``. If it raises,
    the run fails with no retry. Cron events skip it.
    No triggers: the function runs only through ``step.invoke``.
    """
    if not isinstance(id, str) or not id:
        raise ValueError("function id must be a non-empty string")
    checked = _validate_triggers(triggers)
    _validate_config(step_timeout, debounce, cancel_on, recording_profile)

    def decorate(handler: Handler) -> Function:
        if not inspect.iscoroutinefunction(handler):
            raise TypeError(f"function {id!r}: the handler must be an 'async def' function")
        return Function(
            id=id, handler=handler, triggers=checked, name=name, retry=retry, timeout=timeout,
            concurrency=concurrency, description=description,
            # 0 means no default step timeout, as in Go.
            step_timeout=step_timeout if step_timeout is not None and to_seconds(step_timeout) > 0 else None,
            debounce=debounce, cancel_on=[s.copy() for s in cancel_on] if cancel_on else None,
            actor_key=actor_key, secrets=list(secrets) if secrets else None, recording=recording,
            recording_profile=recording_profile, recording_retention=recording_retention,
            metadata=metadata, schema=schema,
        )

    return decorate


async def validate_event(fn: Function, event: Event) -> Event:
    """Apply ``fn.schema`` to the event data. Matches Node validateEventData."""
    if fn.schema is None or event.source == "cron":
        return event
    if isinstance(event.data, dict) and event.data.get(REDACTED_MARKER_KEY) is True:
        raise SchemaValidationError(
            f"event {event.name!r} for function {fn.id!r} was redacted: its payload was irreversibly "
            "replaced with a placeholder and cannot satisfy the declared schema")
    try:
        data = fn.schema(event.data)
        if inspect.isawaitable(data):
            data = await data
    except Exception as exc:
        raise SchemaValidationError(
            f"validation failed in event {event.name!r} for function {fn.id!r}: {exc}") from exc
    return dataclasses.replace(event, data=data)


def _ms(value: Duration) -> int:
    return round(to_seconds(value) * 1000)


def registration_body(
    fn: Function, *, mode: Literal["pull", "push"] = "pull", endpoint_url: str | None = None,
) -> dict[str, Any]:
    """Return the Connect JSON body for ``IronflowService/RegisterFunction``."""
    if mode == "push" and not endpoint_url:
        raise ValueError(f"push registration of {fn.id!r} needs endpoint_url")
    body: dict[str, Any] = {
        "id": fn.id,
        "name": fn.name or fn.id,
        "triggers": [dict(t) for t in fn.triggers],
        "preferredMode": "EXECUTION_MODE_PUSH" if mode == "push" else "EXECUTION_MODE_PULL",
        # The reserved code-hash key wins over user metadata, as in Node.
        "metadata": {**(fn.metadata or {}), CODE_HASH_META_KEY: fn.code_hash},
    }
    if endpoint_url and mode == "push":
        body["endpointUrl"] = endpoint_url
    if fn.description:
        body["description"] = fn.description
    if fn.retry:
        retry: dict[str, Any] = {}
        if "max_attempts" in fn.retry:
            retry["maxAttempts"] = fn.retry["max_attempts"]
        if "initial_delay" in fn.retry:
            retry["initialDelayMs"] = _ms(fn.retry["initial_delay"])
        if "backoff_factor" in fn.retry:
            retry["backoffFactor"] = fn.retry["backoff_factor"]
        if "max_delay" in fn.retry:
            retry["maxDelayMs"] = _ms(fn.retry["max_delay"])
        body["retry"] = retry
    if fn.timeout is not None:
        body["timeoutMs"] = _ms(fn.timeout)
    if fn.concurrency:
        concurrency: dict[str, Any] = {"limit": fn.concurrency["limit"]}
        if fn.concurrency.get("key"):
            concurrency["key"] = fn.concurrency["key"]
        body["concurrency"] = concurrency
    if fn.debounce:
        debounce: dict[str, Any] = {"periodMs": _ms(fn.debounce["period"]), "key": fn.debounce.get("key", "")}
        if _ms(fn.debounce.get("max_wait", 0)) > 0:
            debounce["maxWaitMs"] = _ms(fn.debounce["max_wait"])
        body["debounce"] = debounce
    if fn.cancel_on:
        body["cancelOn"] = [dict(s) for s in fn.cancel_on]
    if fn.actor_key:
        body["actorKey"] = fn.actor_key
    if fn.secrets:
        body["secrets"] = fn.secrets
    if fn.recording is not None:
        body["recording"] = fn.recording
    if fn.recording_profile is not None:
        body["recordingProfile"] = fn.recording_profile
    if fn.recording_retention is not None:
        body["recordingRetention"] = fn.recording_retention
    return body
