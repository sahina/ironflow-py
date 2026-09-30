"""Projections: stateful event handlers with automatic retry and batching."""

from __future__ import annotations

from ._projection import (
    EventInfo,
    Projection,
    ProjectionContext,
    ProjectionEvent,
    ProjectionInfo,
    create_projection,
)

__all__ = [
    "EventInfo",
    "Projection",
    "ProjectionContext",
    "ProjectionEvent",
    "ProjectionInfo",
    "create_projection",
]
