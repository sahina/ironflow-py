"""Run lifecycle frames (`system.run.{run_id}.{event}`) and the version filter that keeps a watcher on the newest status (#3283)."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

_DEFAULT_MAX_RUNS = 10_000


@dataclass(frozen=True)
class RunEvent:
    """Payload of a `system.run.{run_id}.{event}` frame; step frames have a different shape.

    `version` is the run row's version as written by the transition the frame announces.
    It rises with every write to the run. 0 means unknown: the `deleted` frame (the row is
    gone) and a frame from a server that predates the field carry none.
    """

    id: str
    status: str
    version: int = 0
    function_id: str = ""
    event_id: str = ""
    execution_mode: str = ""
    attempt: int = 0
    max_attempts: int = 0
    created_at: str = ""
    updated_at: str = ""
    started_at: str | None = None
    completed_at: str | None = None
    output: Any = None
    error: Any = None
    timestamp: str = ""

    @classmethod
    def from_data(cls, data: Any) -> RunEvent:
        """Build from an event's `data`: a mapping or the generated `Struct` a subscription yields."""
        data = _as_mapping(data)
        return cls(
            id=str(data.get("id", "")),
            status=str(data.get("status", "")),
            version=_version(data.get("version")),
            function_id=str(data.get("functionId", "")),
            event_id=str(data.get("eventId", "")),
            execution_mode=str(data.get("executionMode", "")),
            attempt=int(data.get("attempt", 0) or 0),
            max_attempts=int(data.get("maxAttempts", 0) or 0),
            created_at=str(data.get("createdAt", "")),
            updated_at=str(data.get("updatedAt", "")),
            started_at=data.get("startedAt"),
            completed_at=data.get("completedAt"),
            output=data.get("output"),
            error=data.get("error"),
            timestamp=str(data.get("timestamp", "")),
        )


class _Frame(Protocol):
    topic: str
    data: Any


def _as_mapping(data: Any) -> Mapping[str, Any]:
    if hasattr(data, "to_python"):
        data = data.to_python()
    return data if isinstance(data, Mapping) else {}


def _version(value: object) -> int:
    """A usable version is a whole number >= 1; anything else is unknown (0)."""
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value if value >= 1 else 0
    # A Struct decodes every JSON number as a float.
    if isinstance(value, float) and value.is_integer() and value >= 1:
        return int(value)
    return 0


class RunVersionFilter:
    """Drops older run and step frames, and late frames for remembered deleted runs.

    Frames for one run can arrive out of order: a pull worker may poll one node and
    complete on another, and the `completed` frame can land before the `running` one. A
    watcher that shows the status of the last frame it got would then show `running` after
    `completed`.

    `accept` passes without ranking: any frame outside `system.run.`, a frame
    with no usable version (unless its run was deleted), and a frame whose version ties the highest seen
    (one write publishes both `.failed` and `.updated`). A `deleted` frame remembers a tombstone that blocks late run and step frames.

    Run and step identities share one cache capped at `max_runs`. Steps rank independently.
    Eviction forgets ordering and deletion history. One filter
    belongs to one subscription. It is not thread-safe.

        keep = RunVersionFilter()
        for event in rpc.pubsub.subscribe(SubscribeRequest(pattern="system.run.>")):
            if keep.accept(event):
                render(RunEvent.from_data(event.data))
    """

    def __init__(self, max_runs: int = _DEFAULT_MAX_RUNS) -> None:
        self._max_runs = max_runs
        # Insertion order doubles as recency: the first key is the least recently seen.
        self._highest: OrderedDict[str, int] = OrderedDict()

    def accept(self, event: _Frame) -> bool:
        parts = event.topic.split(".")
        is_step = len(parts) >= 6 and parts[3] == "step"
        if (not is_step and len(parts) != 4) or parts[0] != "system" or parts[1] != "run":
            return True
        run_id = parts[2]
        deleted = not is_step and parts[3] == "deleted"
        if not deleted and self._highest.get(run_id) == -1:
            return False
        key = ".".join(parts[2:-1]) if is_step else run_id
        version = -1 if deleted else _version(_as_mapping(event.data).get("version"))
        if version == 0:
            return True
        seen = self._highest.get(key)
        if not deleted and seen is not None and version < seen:
            return False
        self._highest[key] = version
        self._highest.move_to_end(key)
        if len(self._highest) > self._max_runs:
            self._highest.popitem(last=False)
        return True
