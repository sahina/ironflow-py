"""Retry behaviour.

Covers the gaps the eng review found: retry's success path had no test at all,
only its classification did.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest

from ironflow import IronflowClient
from ironflow._http import IronflowError
from tests.harness import Response


def client(server, **kwargs) -> IronflowClient:
    # Keep the suite fast; the retry logic is identical at any scale. Explicit
    # kwargs win — tests that exercise Retry-After parsing need a max_backoff
    # above the value under test, or the clamp masks what they are asserting.
    kwargs.setdefault("initial_backoff", 0.01)
    kwargs.setdefault("max_backoff", 0.05)
    return IronflowClient(server_url=server.url, **kwargs)


class TestRetrySucceeds:
    def test_succeeds_on_second_attempt(self, server) -> None:
        """The core retry path. A transient 503 must be invisible to the caller."""
        server.script(
            Response(status=503),
            Response(status=200, body={"runs": ["r1"]}),
        )
        result = client(server).runs_list()
        assert result == {"runs": ["r1"]}
        assert len(server.requests) == 2

    def test_succeeds_on_final_allowed_attempt(self, server) -> None:
        server.script(
            Response(status=500),
            Response(status=500),
            Response(status=200, body={"ok": True}),
        )
        assert client(server).runs_list() == {"ok": True}
        assert len(server.requests) == 3


class TestRetryExhausted:
    def test_raises_after_max_attempts(self, server) -> None:
        server.script(*[Response(status=503) for _ in range(5)])
        c = client(server)
        with pytest.raises(IronflowError) as exc:
            c.runs_list()
        assert exc.value.retryable is True
        assert exc.value.status_code == 503
        # Default max_attempts is 3 — not 5.
        assert len(server.requests) == 3

    def test_no_sleep_after_final_attempt(self, server, monkeypatch) -> None:
        """A wait after the last attempt is pure wasted wall-clock.

        Counts sleeps rather than measuring elapsed time: a wall-clock
        assertion here measured the harness and the backoff ceiling, not the
        invariant under test.
        """
        slept = []
        monkeypatch.setattr("ironflow._http.time.sleep", lambda s: slept.append(s))

        server.script(*[Response(status=503) for _ in range(3)])
        c = client(server, max_attempts=3)

        with pytest.raises(IronflowError):
            c.runs_list()

        assert len(server.requests) == 3, "expected 3 attempts"
        assert len(slept) == 2, f"3 attempts must sleep exactly twice, slept {slept}"

    def test_single_attempt_never_sleeps(self, server, monkeypatch) -> None:
        slept = []
        monkeypatch.setattr("ironflow._http.time.sleep", lambda s: slept.append(s))
        server.script(Response(status=503))
        c = client(server, max_attempts=1)
        with pytest.raises(IronflowError):
            c.runs_list()
        assert slept == []


class TestNonRetryable:
    def test_404_not_retried(self, server) -> None:
        server.script(
            Response(status=404, body={"code": "NOT_FOUND", "message": "nope"})
        )
        with pytest.raises(IronflowError) as exc:
            client(server).runs_get("missing")
        assert exc.value.retryable is False
        assert exc.value.status_code == 404
        assert exc.value.code == "NOT_FOUND"
        assert len(server.requests) == 1

    @pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 422])
    def test_client_errors_not_retryable(self, server, status: int) -> None:
        server.script(Response(status=status))
        with pytest.raises(IronflowError) as exc:
            client(server).runs_list()
        assert exc.value.retryable is False
        assert len(server.requests) == 1

    @pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
    def test_retryable_statuses(self, server, status: int) -> None:
        server.script(Response(status=status), Response(status=200, body={"ok": 1}))
        assert client(server).runs_list() == {"ok": 1}
        assert len(server.requests) == 2


class TestWriteMethodsNotRetried:
    """Retrying a write after a timeout can duplicate a committed operation.

    On Ironflow that means a double-emitted event in an append-only stream.
    """

    def test_post_not_retried_by_default(self, server) -> None:
        server.script(Response(status=503), Response(status=200, body={"ok": 1}))
        with pytest.raises(IronflowError):
            client(server).events_create(body={"name": "x"})
        assert len(server.requests) == 1, "POST must not retry by default"

    def test_patch_not_retried_by_default(self, server) -> None:
        server.script(Response(status=503), Response(status=200, body={"ok": 1}))
        with pytest.raises(IronflowError):
            client(server).steps_patch(body={})
        assert len(server.requests) == 1

    def test_put_is_retried(self, server) -> None:
        """PUT is idempotent by definition, so it retries."""
        server.script(Response(status=503), Response(status=200, body={"ok": 1}))
        c = client(server)
        assert c.request("PUT", "/api/v1/projects/p1", body={}) == {"ok": 1}
        assert len(server.requests) == 2

    def test_post_retry_opt_in(self, server) -> None:
        server.script(Response(status=503), Response(status=200, body={"ok": 1}))
        c = client(server)
        result = c.request(
            "POST",
            "/api/v1/events",
            headers={"Idempotency-Key": "abc123"},
            body={"name": "x"},
            retry=True,
        )
        assert result == {"ok": 1}
        assert len(server.requests) == 2
        assert server.requests[0]["headers"].get("Idempotency-Key") == "abc123"


class TestRetryAfter:
    """RFC 9110 allows two formats. Hand-rolled parsers usually miss the second.

    These tests cover PARSING. The wait derived from a parsed value is clamped
    to max_backoff so a server cannot pin the calling thread — that clamp is
    covered in test_hostile_server.py. Each test here therefore raises
    max_backoff above the value under test, so the clamp does not mask whether
    the parse worked.
    """

    def test_delta_seconds(self, server, monkeypatch) -> None:
        slept = []
        monkeypatch.setattr("ironflow._http.time.sleep", lambda s: slept.append(s))

        server.script(
            Response(status=429, headers={"Retry-After": "1"}),
            Response(status=200, body={"ok": 1}),
        )
        c = client(server, max_backoff=5.0)
        assert c.runs_list() == {"ok": 1}

        assert slept == [1.0], f"Retry-After: 1 should request 1s, got {slept}"

    def test_http_date(self, server, monkeypatch) -> None:
        """The format hand-rolled parsers usually miss.

        HTTP-date has one-second granularity, so asserting on elapsed time is
        inherently fuzzy (now+2s truncates to somewhere in 1.0-2.0s). Capture
        the requested delay directly instead.
        """
        slept = []
        monkeypatch.setattr("ironflow._http.time.sleep", lambda s: slept.append(s))

        when = datetime.now(timezone.utc) + timedelta(seconds=30)
        server.script(
            Response(status=503, headers={"Retry-After": format_datetime(when)}),
            Response(status=200, body={"ok": 1}),
        )
        assert client(server, max_backoff=60.0).runs_list() == {"ok": 1}

        assert len(slept) == 1
        # ~30s minus sub-second truncation, and emphatically not the 0.01s backoff.
        assert 28 < slept[0] <= 30, f"HTTP-date parsed wrong: slept {slept[0]}"

    def test_garbage_retry_after_falls_back_to_backoff(self, server) -> None:
        server.script(
            Response(status=503, headers={"Retry-After": "not-a-date"}),
            Response(status=200, body={"ok": 1}),
        )
        # Must not raise, must still retry.
        assert client(server).runs_list() == {"ok": 1}


class TestTotalTimeout:
    def test_deadline_stops_retrying(self, server) -> None:
        server.script(
            *[Response(status=503, headers={"Retry-After": "5"}) for _ in range(3)]
        )
        c = client(server, total_timeout=0.5)
        started = time.monotonic()
        with pytest.raises(IronflowError):
            c.runs_list()
        elapsed = time.monotonic() - started
        # Retry-After asks for 5s; the deadline must refuse rather than obey.
        assert elapsed < 1.0, f"total_timeout ignored; took {elapsed:.2f}s"

    def test_none_by_default(self, server) -> None:
        assert client(server).total_timeout is None
