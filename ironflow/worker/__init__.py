"""Pull-mode worker runtime with durable steps (Tier 1).

Handlers are ``async def``. Step bodies may be sync or async; a sync body runs
in a thread, and a thread cannot be stopped by a timeout or a drain. Step
bodies must be idempotent: a step that ran after the last persisted
checkpoint can run again after a crash. ``await Worker(...).start()`` does
not return until the worker stops.
"""

from typing import Any

from ._duration import Duration
from ._function import (
    CancelOnSpec,
    DebounceConfig,
    Function,
    RecordingProfile,
    function,
)
from ._step import (
    Context,
    Event,
    InvokeAsyncResult,
    InvokeError,
    NonRetryableError,
    PublishResult,
    RunInfo,
    SchemaValidationError,
    Step,
    StepError,
    StepTimeoutError,
)
from ._worker import Worker, WorkerAuthError

__all__ = [
    "CancelOnSpec", "Context", "DebounceConfig", "Duration", "Event", "Function", "InvokeAsyncResult",
    "InvokeError", "NonRetryableError", "PublishResult", "RecordingProfile", "RunInfo",
    "SchemaValidationError", "Step", "StepError", "StepTimeoutError", "StreamingWorker", "Worker", "WorkerAuthError", "function",
]


def __getattr__(name: str) -> Any:
    if name == "StreamingWorker":
        from ._streaming import StreamingWorker

        return StreamingWorker
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
