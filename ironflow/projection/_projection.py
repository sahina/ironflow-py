"""Projection definitions: stateful event handlers with automatic retry and batching.

Semantics match the Node SDK (sdk/js/core/src/projection.ts): a projection is either
managed (stateful, with initial_state) or external (stateless, side-effect only).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class ProjectionEvent:
    """An event consumed by a projection."""

    id: str
    name: str
    data: Any
    seq: int
    timestamp: str
    source: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class EventInfo:
    """Event metadata passed to projection handlers."""

    id: str
    name: str
    seq: int
    timestamp: str


@dataclass(frozen=True)
class ProjectionInfo:
    """Projection metadata passed to handlers."""

    name: str
    version: int


@dataclass(frozen=True)
class ProjectionContext:
    """Context passed to projection handlers."""

    event: EventInfo
    projection: ProjectionInfo


@dataclass(frozen=True)
class Projection:
    """A registered projection definition."""

    name: str
    events: tuple[str, ...]
    handler: Callable[..., Any]
    mode: Literal["managed", "external"]
    initial_state: Callable[[], Any] | None
    partition_key: str
    max_retries: int
    batch_size: int


def create_projection(
    *,
    name: str,
    events: Sequence[str],
    handler: Callable[..., Any],
    initial_state: Callable[[], Any] | None = None,
    mode: Literal["managed", "external"] | None = None,
    partition_key: str | None = None,
    max_retries: int = 3,
    batch_size: int = 100,
) -> Projection:
    """Create a projection definition.

    Args:
        name: Projection name (required, non-empty).
        events: List of event names to subscribe to (required, non-empty).
        handler: Event handler function. For managed projections, signature is
                 (state, event: ProjectionEvent, context: ProjectionContext) -> new_state.
                 For external projections, signature is (event: ProjectionEvent, context: ProjectionContext) -> None.
        initial_state: State factory for managed projections. If provided, mode defaults to "managed".
        mode: "managed" (stateful) or "external" (stateless). Defaults to "managed" if initial_state is provided,
              otherwise "external".
        partition_key: Optional partition key for grouping event processing (default: "").
        max_retries: Maximum retry attempts on handler failure (default: 3).
        batch_size: Maximum batch size for event processing (default: 100, must be >= 1).

    Returns:
        A Projection instance.

    Raises:
        ValueError: If name is empty, events is empty, mode is invalid, or batch_size < 1.
    """
    if not name:
        raise ValueError("projection name is required")
    if not events:
        raise ValueError(f"projection {name!r} must subscribe to at least one event")
    resolved = mode or ("managed" if initial_state is not None else "external")
    if resolved not in ("managed", "external"):
        raise ValueError(f"projection {name!r}: mode must be 'managed' or 'external', got {resolved!r}")
    if batch_size < 1:
        raise ValueError(f"projection {name!r}: batch_size must be at least 1")
    return Projection(
        name=name,
        events=tuple(events),
        handler=handler,
        mode=resolved,
        initial_state=initial_state,
        partition_key=partition_key or "",
        max_retries=max_retries,
        batch_size=batch_size,
    )
