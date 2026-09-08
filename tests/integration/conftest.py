"""Fixtures for the real-server suite (#1781).

These tests need a running Ironflow binary, so they are skipped unless
`IRONFLOW_TEST_SERVER` is set. `make test-python-integration` builds the binary,
starts a server, and sets it along with `IRONFLOW_TEST_API_KEY`.

The skip is loud on purpose — a reason naming the variable reads as an
instruction, where a silently absent directory reads as "no such tests".

WHY NOT `--dev`
---------------
`sdk-gen-check`, the closest precedent, starts its server with `--dev`. This
suite must not. `--dev` takes the bypass branch in internal/server/auth.go and
synthesises a request context without checking any key, so the
authentication-failure case — an acceptance criterion of #1781 — would pass
against a server that never authenticates anything. `serve --bootstrap-key-file`
writes a real admin `ifkey_`, so running for real costs one flag.

WHAT BELONGS HERE, AND WHAT DOES NOT
------------------------------------
Only what a live server can prove: real handlers, real auth, real Connect
framing over the wire.

That now includes timeout, cancellation and mid-iteration error translation,
which an earlier version of this file ruled out as inherently racy. They are
not, provided the failure is one the server produces deterministically: a
unique topic nobody publishes to never yields, so a deadline on it fires every
time. Four consecutive runs against a fresh server agreed to within 20ms.

What stays in tests/test_rpc_streams.py against the stub is the case a live
server cannot be made to produce: a server-generated APPLICATION error
mid-stream. internal/server/connect/pubsub_handler.go has no reachable path to
one — its fanout loop returns nil on ctx.Done() and its only other mid-loop
exit is a stream.Send transport failure.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from ironflow import AsyncIronflowRPC, IronflowRPC

SERVER_URL = os.environ.get("IRONFLOW_TEST_SERVER")
API_KEY = os.environ.get("IRONFLOW_TEST_API_KEY")

_SKIP_REASON = (
    "no live server: set IRONFLOW_TEST_SERVER (and IRONFLOW_TEST_API_KEY), "
    "or run `make test-python-integration`, which starts one"
)


_HERE = Path(__file__).parent


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Skip the tests under THIS directory when no server is configured.

    Two things this got wrong on the way in, both of which looked fine:

    1. A module-level `pytestmark` in a conftest applies only to tests defined
       in that same file, so it skipped nothing and every test here failed on a
       None server_url during `make test-python`.
    2. This hook receives the WHOLE session's item list, not just the items
       under the conftest that defines it — the first version skipped all 219
       tests in the SDK. Hence the path filter.

    A skip rather than `collect_ignore`, so the normal run PRINTS a line naming
    the variable. Ignored files look like tests that do not exist.
    """
    if SERVER_URL:
        return
    skip = pytest.mark.skip(reason=_SKIP_REASON)
    for item in items:
        if _HERE in Path(str(item.path)).parents:
            item.add_marker(skip)


@pytest.fixture
def server_url() -> str:
    assert SERVER_URL is not None
    return SERVER_URL


@pytest.fixture
def api_key() -> str:
    if not API_KEY:
        pytest.fail(
            "IRONFLOW_TEST_SERVER is set but IRONFLOW_TEST_API_KEY is not. The "
            "suite runs against a NON-dev server, so an absent key would turn "
            "every test into an authentication failure and the auth test into a "
            "false pass."
        )
    return API_KEY


@pytest.fixture
def rpc(server_url: str, api_key: str) -> Any:
    with IronflowRPC(server_url=server_url, api_key=api_key, timeout=30.0) as client:
        yield client


@pytest.fixture
def publish_rpc(server_url: str, api_key: str) -> Any:
    """A separate client can publish while the subscriber blocks."""
    with IronflowRPC(server_url=server_url, api_key=api_key) as client:
        yield client


@pytest.fixture
def async_rpc_factory(server_url: str, api_key: str) -> Any:
    def make() -> AsyncIronflowRPC:
        return AsyncIronflowRPC(server_url=server_url, api_key=api_key, timeout=30.0)

    return make
