# sdk/python/ironflow/worker/_run.py
"""One function execution, shared by the pull worker and push serve.

Every failure the handler or the event can cause becomes a ``failed``
outcome. Nothing but cancellation escapes: in push mode an exception would
become an HTTP 500, which the engine retries and counts as a transport error.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from .._http import IronflowError
from ..upcaster import UpcasterChainError
from ._function import Function, validate_event
from ._step import (
    Context,
    Event,
    ExecutionContext,
    NonRetryableError,
    RunInfo,
    Step,
    _Yield,
    encode_json,
    run_compensations,
)

if TYPE_CHECKING:
    from ..upcaster import UpcasterRegistry


def is_terminal(retryable: bool, run: RunInfo) -> bool:
    """Whether a failure ends the run, so its compensations must run now.

    A non-retryable error is always terminal. A retryable one is terminal only on the last attempt
    under ``on_exhausted == "fail"``; under ``"pause"`` the run is held for resume and keeps its
    steps. An empty policy or budget (an engine that predates them) keeps the old rule.
    """
    if not retryable:
        return True
    return run.on_exhausted == "fail" and run.max_attempts > 0 and run.attempt >= run.max_attempts


def retry_after_ms(exc: IronflowError) -> int:
    """The delay a retryable error asks for, in whole milliseconds, or 0 for none."""
    seconds = exc.retry_after
    if not exc.retryable or seconds is None or not math.isfinite(seconds) or seconds <= 0:
        return 0
    return math.ceil(seconds * 1000)


def upcast_event(raw: Mapping[str, Any], upcasters: UpcasterRegistry | None) -> Event:
    if upcasters is not None:
        try:
            data = upcasters.upcast_to_latest(raw["name"], raw.get("data"), raw.get("version") or 1)
        except UpcasterChainError as exc:
            # Go swallows this and hands the handler old-schema data; we fail loudly (spec §Upcasters).
            raise NonRetryableError(str(exc)) from exc
        raw = {**raw, "data": data}
    return Event.from_wire(raw)


async def run_function(
    fn: Function, *, raw_event: Mapping[str, Any], upcasters: UpcasterRegistry | None,
    ctx: ExecutionContext, run: RunInfo, secrets: Mapping[str, str],
    logger: logging.LoggerAdapter[logging.Logger],
) -> dict[str, Any]:
    try:
        context = Context(
            event=await validate_event(fn, upcast_event(raw_event, upcasters)), step=Step(ctx),
            run=run, logger=logger, secrets=MappingProxyType(dict(secrets)),
        )
        output = await fn.handler(context)
        try:
            encode_json(output)
        except (TypeError, ValueError) as exc:
            return {"status": "failed", "error": {"message": f"output is not JSON-encodable: {exc}",
                                                  "code": "SERIALIZATION_ERROR", "retryable": False}}
        return {"status": "completed", "output": output}
    except _Yield as signal:
        return {"status": "yielded", "yield": signal.info}
    except IronflowError as exc:
        if is_terminal(exc.retryable, run):
            await run_compensations(ctx)
        error: dict[str, Any] = {"message": str(exc), "code": exc.code or "ERROR", "retryable": exc.retryable}
        if delay := retry_after_ms(exc):
            error["retry_after_ms"] = delay
        return {"status": "failed", "error": error}
    except Exception as exc:  # noqa: BLE001 - handlers may raise any ordinary exception
        if is_terminal(True, run):
            await run_compensations(ctx)
        return {"status": "failed", "error": {"message": str(exc), "code": "ERROR", "retryable": True}}
