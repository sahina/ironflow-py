"""Public API snapshot.

Why this exists: deduplicateMethods (cmd/sdk-gen/main.go) falls back to
appending a positional index when it cannot derive a meaningful disambiguator.
That index depends on route ORDER, so adding an unrelated route can silently
RENAME an existing public method — a breaking change with no signal.

This test turns that silence into a CI failure. It does not make the naming
scheme stable; replacing the positional fallback is tracked in TODOS.md. Until
then, an intentional API change means regenerating the snapshot deliberately:

    python -c "import json;from ironflow import IronflowClient as C;\\
    print(json.dumps(sorted(n for n in dir(C) if not n.startswith('_')),indent=2))" \\
      > tests/api_snapshot.json
"""

from __future__ import annotations

import json
from pathlib import Path

from ironflow import IronflowClient

SNAPSHOT = Path(__file__).parent / "api_snapshot.json"


def _current() -> list[str]:
    return sorted(n for n in dir(IronflowClient) if not n.startswith("_"))


def test_public_api_matches_snapshot() -> None:
    expected = json.loads(SNAPSHOT.read_text())
    actual = _current()

    added = sorted(set(actual) - set(expected))
    removed = sorted(set(expected) - set(actual))

    assert not removed, (
        f"Public methods disappeared: {removed}. If intentional, regenerate "
        f"tests/api_snapshot.json — but check first whether deduplicateMethods "
        f"renamed them, which is a silent breaking change."
    )
    assert not added, (
        f"New public methods: {added}. Regenerate tests/api_snapshot.json to accept."
    )


def test_streaming_endpoints_are_absent() -> None:
    """WebSocket and watch endpoints call streams as plain JSON HTTP.

    They are filtered out server-side via RouteEntry.Streaming. If one comes
    back, the filter regressed and the method is broken by construction.
    """
    banned = {"websocket_list", "config_list_watch", "kv_list_buckets_watch"}
    present = banned & set(_current())
    assert not present, f"streaming endpoints leaked into the client: {sorted(present)}"


def test_escape_hatch_is_public() -> None:
    """request() remains available for undeclared params and headers."""
    assert "request" in _current()


def test_no_method_ends_in_a_bare_digit() -> None:
    """Positional-index disambiguators mean the name is order-dependent."""
    suspicious = [n for n in _current() if n and n[-1].isdigit()]
    assert not suspicious, (
        f"Methods named by positional index: {suspicious}. These rename "
        f"themselves when routes are added — see TODOS.md."
    )
