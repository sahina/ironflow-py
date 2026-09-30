"""Duration and time parsing for the worker runtime.

A bare number is seconds (the Python convention), not milliseconds as in the
Node SDK. A string uses Node's grammar: ``30s``, ``5m``, ``2h``, ``7d``, ``500ms``.
"""

from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone

Duration = timedelta | int | float | str

_GRAMMAR = re.compile(r"^(\d+(?:\.\d+)?)(ms|s|m|h|d)$")
_UNIT_SECONDS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}
_WIRE_UNITS = (("d", 86_400_000), ("h", 3_600_000), ("m", 60_000), ("s", 1_000))
_FRACTION = re.compile(r"\.(\d+)")


def to_seconds(value: Duration) -> float:
    """Return the duration in seconds."""
    if isinstance(value, bool):
        raise TypeError("a duration must not be a bool")
    if isinstance(value, timedelta):
        seconds = value.total_seconds()
    elif isinstance(value, (int, float)):
        seconds = float(value)
    elif isinstance(value, str):
        match = _GRAMMAR.fullmatch(value)
        if match is None:
            raise ValueError(
                f'invalid duration "{value}": use "30s", "5m", "2h", "7d", "500ms", or a number of seconds'
            )
        seconds = float(match.group(1)) * _UNIT_SECONDS[match.group(2)]
    else:
        raise TypeError(f"unsupported duration type: {type(value).__name__}")
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError("a duration must be finite and non-negative")
    return seconds


def to_wire(value: Duration) -> str:
    """Return a validated duration string for the server."""
    if isinstance(value, str):
        to_seconds(value)
        return value
    seconds = to_seconds(value)
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError("a duration must be finite and non-negative")
    ms = math.ceil(seconds * 1000)
    for unit, size in _WIRE_UNITS:
        if ms >= size and ms % size == 0:
            return f"{ms // size}{unit}"
    return f"{ms}ms"


def _parse(value: str) -> datetime:
    normalized = value.strip().replace("Z", "+00:00").replace("z", "+00:00")
    normalized = _FRACTION.sub(lambda match: "." + match.group(1)[:6].ljust(6, "0"), normalized, count=1)
    try:
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"invalid ISO-8601 time: {value!r}") from exc


def parse_timestamp(value: str) -> datetime:
    """Parse a server RFC 3339 timestamp as aware UTC; absent offsets mean UTC."""
    dt = _parse(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_when(when: datetime | str) -> datetime:
    """Parse a sleep target that must include a timezone."""
    dt = _parse(when) if isinstance(when, str) else when
    if isinstance(when, str):
        fraction = _FRACTION.search(when)
        if fraction is not None and any(digit != "0" for digit in fraction.group(1)[6:]):
            dt += timedelta(microseconds=1)
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError("sleep_until needs a timezone-aware datetime or an ISO-8601 string with an offset")
    return dt.astimezone(timezone.utc)


def iso_utc(dt: datetime) -> str:
    """Format a datetime as ISO-8601 UTC with milliseconds and a ``Z`` suffix."""
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
