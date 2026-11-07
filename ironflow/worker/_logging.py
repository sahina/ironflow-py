"""Application-owned callback logging and task-local durable step labels."""
from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping, MutableMapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from typing_extensions import override


@dataclass
class _StepScope:
    execution: object
    step_id: str
    active: bool = True
    parent: _StepScope | None = None

    def is_active(self) -> bool:
        scope: _StepScope | None = self
        while scope is not None:
            if not scope.active:
                return False
            scope = scope.parent
        return True


_step: ContextVar[_StepScope | None] = ContextVar("ironflow_log_step", default=None)


@contextmanager
def step_scope(execution: object, step_id: str) -> Iterator[None]:
    parent = _step.get()
    scope = _StepScope(execution, step_id, parent=parent if parent is not None
                       and parent.execution is execution else None)
    token = _step.set(scope)
    try:
        yield
    finally:
        # Child tasks and sync bodies in threads can outlive the callback deadline.
        scope.active = False
        _step.reset(token)


class CorrelationFormatter(logging.Formatter):
    """Text formatter for callback records, also safe for transport diagnostics."""

    def __init__(self) -> None:
        super().__init__(
            "%(levelname)s %(message)s run_id=%(run_id)s function_id=%(function_id)s "
            "step_id=%(step_id)s attempt=%(attempt)s trace_id=%(trace_id)s",
            defaults={"run_id": "", "function_id": "", "step_id": "", "attempt": 0, "trace_id": ""},
        )


def _trace_id() -> str:
    try:
        from opentelemetry import trace
    except ImportError:
        return ""
    context = trace.get_current_span().get_span_context()
    return f"{context.trace_id:032x}" if context.is_valid else ""


if TYPE_CHECKING:
    _Adapter = logging.LoggerAdapter[logging.Logger]
else:
    _Adapter = logging.LoggerAdapter


class CallbackLogger(_Adapter):
    def __init__(
        self, logger: logging.Logger, *, execution: object,
        run_id: str, function_id: str, attempt: int, extra: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(logger, extra or {})
        self._execution = execution
        self._run_id = run_id
        self._function_id = function_id
        self._attempt = attempt

    @override
    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> tuple[Any, dict[str, Any]]:
        scope = _step.get()
        kwargs["extra"] = {
            **(self.extra or {}), **(kwargs.get("extra") or {}),
            "run_id": self._run_id, "function_id": self._function_id,
            "attempt": self._attempt, "trace_id": _trace_id(),
            "step_id": scope.step_id if scope is not None and scope.is_active()
            and scope.execution is self._execution else "",
        }
        return msg, dict(kwargs)

    @override
    def log(self, level: int, msg: Any, *args: Any, **kwargs: Any) -> None:
        if not self.isEnabledFor(level):
            return
        msg, kwargs = self.process(msg, kwargs)
        kwargs["stacklevel"] = kwargs.get("stacklevel", 1) + 1
        if self.logger.hasHandlers():
            self.logger.log(level, msg, *args, **kwargs)
            return
        # Keep the fallback local to this call: transport diagnostics and the
        # application's logger hierarchy must not acquire an SDK handler.
        fallback = logging.Logger(self.logger.name, self.logger.getEffectiveLevel())  # noqa: LOG001 - unregistered local fallback
        fallback.filters = self.logger.filters
        handler = logging.StreamHandler()
        handler.setFormatter(CorrelationFormatter())
        fallback.addHandler(handler)
        fallback.log(level, msg, *args, **kwargs)
