from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ironflow.worker._duration import (
    iso_utc,
    parse_timestamp,
    parse_when,
    to_seconds,
    to_wire,
)


@pytest.mark.parametrize(
    ("value", "seconds"),
    [("30s", 30.0), ("5m", 300.0), ("2h", 7200.0), ("7d", 604800.0), ("500ms", 0.5),
     ("1.5s", 1.5), (10, 10.0), (0.25, 0.25), (timedelta(minutes=2), 120.0)],
)
def test_to_seconds(value: object, seconds: float) -> None:
    assert to_seconds(value) == seconds  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", ["10", "5x", "-1s", "", " 5s"])
def test_to_seconds_rejects_bad_strings(bad: str) -> None:
    with pytest.raises(ValueError):
        to_seconds(bad)


def test_to_seconds_rejects_bool() -> None:
    with pytest.raises(TypeError):
        to_seconds(True)


@pytest.mark.parametrize("value", [-1, -0.5, timedelta(seconds=-1), float("nan"), float("inf")])
def test_to_seconds_rejects_negative_or_non_finite(value: float | timedelta) -> None:
    with pytest.raises(ValueError):
        to_seconds(value)


@pytest.mark.parametrize(
    ("value", "wire"),
    [("1d", "1d"), (86400, "1d"), (3600, "1h"), (90, "90s"), (120, "2m"), (0.5, "500ms"), (timedelta(hours=36), "36h")],
)
def test_to_wire_uses_largest_whole_unit(value: object, wire: str) -> None:
    assert to_wire(value) == wire  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf")])
def test_to_wire_rejects_negative_or_non_finite(value: float) -> None:
    with pytest.raises(ValueError):
        to_wire(value)


@pytest.mark.parametrize(("value", "wire"), [(0.0001, "1ms"), (0.0011, "2ms")])
def test_to_wire_rounds_positive_numeric_duration_up(value: float, wire: str) -> None:
    assert to_wire(value) == wire


@pytest.mark.parametrize(
    "raw",
    ["2026-09-24T10:00:00Z", "2026-09-24T10:00:00.123456789Z", "2026-09-24T10:00:00.1Z", "2026-09-24T12:00:00+02:00"],
)
def test_parse_timestamp_accepts_go_rfc3339(raw: str) -> None:
    dt = parse_timestamp(raw)
    assert dt.tzinfo is not None
    assert dt.astimezone(timezone.utc).hour == 10


def test_parse_when_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        parse_when(datetime(2030, 1, 1))  # noqa: DTZ001


def test_parse_when_rejects_string_without_offset() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        parse_when("2030-01-01T00:00:00")


def test_iso_utc() -> None:
    dt = datetime(2026, 9, 24, 12, 0, 0, 123456, tzinfo=timezone(timedelta(hours=2)))
    assert iso_utc(dt) == "2026-09-24T10:00:00.123Z"
