from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from opentelemetry.trace import Span

_STATE_MEMBER = re.compile(
    r"([a-z][_0-9a-z*/-]{0,255}|[a-z0-9][_0-9a-z*/-]{0,240}@[a-z][_0-9a-z*/-]{0,13})="
    r"([\x20-\x2b\x2d-\x3c\x3e-\x7e]{0,255}[\x21-\x2b\x2d-\x3c\x3e-\x7e])"
)


def _trace_state(value: object) -> str | None:
    if not isinstance(value, str) or len(value) > 512:
        return None
    members = [member.strip(" \t") for member in value.split(",") if member.strip(" \t")]
    if len(members) > 32:
        return None
    keys: set[str] = set()
    for member in members:
        match = _STATE_MEMBER.fullmatch(member)
        if match is None or match[1] in keys:
            return None
        keys.add(match[1])
    return ",".join(members) or None


def fail_span(span: Span | None) -> None:
    if span is not None:
        from opentelemetry.trace import StatusCode

        span.set_status(StatusCode.ERROR)


@contextmanager
def execution_span(name: str, *, carrier: Mapping[str, object] | None = None) -> Iterator[Span | None]:
    try:
        from opentelemetry import trace
        from opentelemetry.context import Context, attach, detach
        from opentelemetry.trace.propagation.tracecontext import (
            TraceContextTextMapPropagator,
        )
    except ImportError:
        yield None
        return

    parent = None
    if name == "process":
        # Poll and server request spans are transport context, never job ancestry.
        fields: dict[str, str] = {}
        value = (carrier or {}).get("traceparent")
        if (isinstance(value, str) and len(value) <= 512
                and all(" " <= char <= "~" for char in value)
                and (not value.startswith("00-") or len(value) == 55)):
            fields["traceparent"] = value
        state = _trace_state((carrier or {}).get("tracestate"))
        if state is not None:
            fields["tracestate"] = state
        parent = TraceContextTextMapPropagator().extract(fields, context=Context())
    token = attach(parent) if parent is not None else None
    try:
        with trace.get_tracer("ironflow").start_as_current_span(
            name, context=parent, kind=trace.SpanKind.CONSUMER if name == "process" else trace.SpanKind.INTERNAL,
            record_exception=False, set_status_on_exception=False,
        ) as span:
            try:
                yield span
            except Exception as exc:
                fail_span(span)
                # Exception messages and stack traces can contain credentials or payloads.
                span.add_event("exception", {"exception.type": type(exc).__name__})
                raise
    finally:
        if token is not None:
            detach(token)
