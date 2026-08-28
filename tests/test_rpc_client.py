"""Behaviour of IronflowRPC and AsyncIronflowRPC (#1781).

Every behavioural test is parameterized over both clients. The sync and async
halves are separate generated method trees driven by separate interceptor
protocol methods, so a suite that exercised one would leave the other's error
path written but unproven — which is how the two silently diverge.

Tests run against a real Connect server, in-process. See tests/rpc_server.py
for why that is worth the ~40 lines over a mock.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from connectrpc.code import Code

from ironflow import AsyncIronflowRPC, IronflowError, IronflowRPC, IronflowRPCError
from ironflow.rpc._runtime import NO_TIMEOUT
from ironflow.rpc.v1 import CreateWebhookSourceRequest, GetWebhookSourceRequest

from .rpc_server import serve

API_KEY = "ifkey_test_abc123"


def call(client: Any, request: Any, **kwargs: Any) -> Any:
    """Invoke rpc.webhooks.create_source on either client, awaiting if needed.

    Keeps each test a single assertion about behaviour instead of two
    near-identical bodies that can drift apart.
    """
    result = client.webhooks.create_source(request, **kwargs)
    if asyncio.iscoroutine(result):
        return asyncio.get_event_loop().run_until_complete(result)
    return result


def close(client: Any) -> None:
    if isinstance(client, AsyncIronflowRPC):
        asyncio.get_event_loop().run_until_complete(client.aclose())
    else:
        client.close()


@pytest.fixture(params=[IronflowRPC, AsyncIronflowRPC], ids=["sync", "async"])
def client_cls(request: pytest.FixtureRequest) -> Any:
    return request.param


@pytest.fixture(autouse=True)
def loop() -> Any:
    """A fresh event loop per test, for EVERY test, not only the ones that
    obviously need one.

    autouse because `close()` below drives `aclose()` through the loop, so any
    test that constructs an AsyncIronflowRPC touches asyncio at teardown even
    if its body never awaits. Without autouse those tests inherit whatever loop
    the previous test closed and die with "Event loop is closed" — six of them
    did.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


# ── auth ─────────────────────────────────────────────────────────────────────


def test_bearer_token_reaches_the_server(client_cls: Any) -> None:
    """The whole reason auth is an interceptor rather than a per-call header.

    A mocked transport cannot fail this test, which is the point of running a
    real server: this asserts on what the SERVER received.
    """
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            call(client, CreateWebhookSourceRequest(name="Stripe", event_prefix="stripe."))
        finally:
            close(client)

        assert srv.service.seen_headers, "the server was never reached"
        assert srv.service.seen_headers[0]["authorization"] == f"Bearer {API_KEY}"


def test_read_max_bytes_bounds_the_response(client_cls: Any) -> None:
    """`read_max_bytes` is public API, so something has to prove it reaches the wire.

    It was threaded from the constructor through `_client_kwargs()` into all
    seven generated service clients with no test and no documentation — a
    parameter callers are committed to but nothing exercised.

    The stub echoes `request.name` back, so an oversized request produces an
    oversized response without the server needing to know about this test.

    The overrun also has to arrive as the Ironflow type, not as a raw
    connectrpc exception — the limit is enforced while reading the response, a
    different code path from the error frames the other tests cover.
    """
    big = "x" * 50_000
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY, read_max_bytes=1024)
        try:
            with pytest.raises(IronflowRPCError) as exc:
                call(client, CreateWebhookSourceRequest(name=big, event_prefix="x."))
        finally:
            close(client)
    assert srv.service.seen_headers, "the server was never reached"
    assert exc.value.code == "resource_exhausted"
    assert "1024" in str(exc.value)


def test_no_read_max_bytes_accepts_the_same_response(client_cls: Any) -> None:
    """The control for the test above.

    Without it, a `read_max_bytes` that silently did nothing would still pass
    if the oversized response happened to fail for some unrelated reason.
    """
    big = "x" * 50_000
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            got = call(client, CreateWebhookSourceRequest(name=big, event_prefix="x."))
        finally:
            close(client)
    assert got.name == big


def test_no_api_key_sends_no_authorization_header(client_cls: Any) -> None:
    """An absent key must not become the literal string "Bearer None"."""
    with serve() as srv:
        client = client_cls(server_url=srv.url)
        try:
            call(client, CreateWebhookSourceRequest(name="x", event_prefix="x."))
        finally:
            close(client)

        assert "authorization" not in srv.service.seen_headers[0]


def test_one_transport_is_shared_across_namespaces(client_cls: Any) -> None:
    """Every service client must receive the same pyqwest client.

    Otherwise each namespace opens its own connection pool and `close()`
    releases one of eight.
    """
    client = client_cls(server_url="http://x", api_key=API_KEY)
    try:
        transports = {id(c._http_client) for c in client._service_clients()}
        assert len(transports) == 1
        assert transports == {id(client._http)}
    finally:
        close(client)


# ── error translation ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (Code.NOT_FOUND, "not_found"),
        (Code.PERMISSION_DENIED, "permission_denied"),
        (Code.UNAVAILABLE, "unavailable"),
        (Code.UNAUTHENTICATED, "unauthenticated"),
        (Code.INVALID_ARGUMENT, "invalid_argument"),
    ],
)
def test_connect_error_becomes_ironflow_error(
    client_cls: Any, code: Code, expected: str
) -> None:
    """A real Connect error frame, not a locally constructed exception."""
    with serve() as srv:
        srv.service.raise_code = code
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            with pytest.raises(IronflowRPCError) as exc:
                call(client, CreateWebhookSourceRequest(name="x", event_prefix="x."))
        finally:
            close(client)

    assert exc.value.code == expected


def test_ironflow_rpc_error_is_an_ironflow_error(client_cls: Any) -> None:
    """`except IronflowError` must still catch everything this SDK raises."""
    with serve() as srv:
        srv.service.raise_code = Code.NOT_FOUND
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            with pytest.raises(IronflowError):
                call(client, CreateWebhookSourceRequest(name="x", event_prefix="x."))
        finally:
            close(client)


def test_original_connect_error_is_the_cause(client_cls: Any) -> None:
    """The upstream error stays reachable without being the public type."""
    from connectrpc.errors import ConnectError

    with serve() as srv:
        srv.service.raise_code = Code.INTERNAL
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            with pytest.raises(IronflowRPCError) as exc:
                call(client, CreateWebhookSourceRequest(name="x", event_prefix="x."))
        finally:
            close(client)

    assert isinstance(exc.value.__cause__, ConnectError)


def test_inherited_fields_do_not_lie(client_cls: Any) -> None:
    """retryable/status_code/retry_after carry the documented values.

    `retryable is False` even for UNAVAILABLE is deliberate, and stayed that way
    when #1809 added retries. It means "nothing left for this client to do" —
    here, that CreateWebhookSource is not annotated NO_SIDE_EFFECTS, so it was
    never retried. It is not a claim that the call is safe to repeat; see
    tests/test_rpc_retry.py for the behaviour this asserts the error side of.
    """
    with serve() as srv:
        srv.service.raise_code = Code.UNAVAILABLE
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            with pytest.raises(IronflowRPCError) as exc:
                call(client, CreateWebhookSourceRequest(name="x", event_prefix="x."))
        finally:
            close(client)

    assert exc.value.retryable is False
    assert exc.value.status_code == 0
    assert exc.value.retry_after is None


# ── deadlines ────────────────────────────────────────────────────────────────


def test_client_default_timeout_reaches_the_server(client_cls: Any) -> None:
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY, timeout=7.5)
        try:
            call(client, CreateWebhookSourceRequest(name="x", event_prefix="x."))
        finally:
            close(client)

    # Seconds in, milliseconds on the wire. The server sees a deadline slightly
    # below 7500 because the header carries remaining time, so assert a band.
    seen = srv.service.seen_timeouts[0]
    assert seen is not None
    assert 7000 <= seen <= 7500


def test_per_call_timeout_overrides_the_client_default(client_cls: Any) -> None:
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY, timeout=30.0)
        try:
            call(client, CreateWebhookSourceRequest(name="x", event_prefix="x."), timeout=2.0)
        finally:
            close(client)

    seen = srv.service.seen_timeouts[0]
    assert seen is not None
    assert 1500 <= seen <= 2000


def test_no_timeout_sentinel_clears_a_client_deadline(client_cls: Any) -> None:
    """The hole NO_TIMEOUT exists to fill.

    `timeout=None` means "inherit", so without a sentinel a caller whose client
    sets a deadline has no way to run one call without one.
    """
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY, timeout=30.0)
        try:
            call(
                client,
                CreateWebhookSourceRequest(name="x", event_prefix="x."),
                timeout=NO_TIMEOUT,
            )
        finally:
            close(client)

    assert srv.service.seen_timeouts[0] is None


def test_no_client_default_means_no_deadline(client_cls: Any) -> None:
    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            call(client, CreateWebhookSourceRequest(name="x", event_prefix="x."))
        finally:
            close(client)

    assert srv.service.seen_timeouts[0] is None


@pytest.mark.parametrize("bad", [0, -1, -0.5])
def test_non_positive_timeout_is_rejected(client_cls: Any, bad: float) -> None:
    """`timeout=0` reads as "expire immediately" and must not silently mean
    "no deadline" — the error names the sentinel instead."""
    client = client_cls(server_url="http://x", api_key=API_KEY)
    try:
        with pytest.raises(ValueError, match="NO_TIMEOUT"):
            call(client, CreateWebhookSourceRequest(name="x", event_prefix="x."), timeout=bad)
    finally:
        close(client)


@pytest.mark.parametrize("tiny", [0.0005, 0.0009, 0.000001])
def test_sub_millisecond_timeout_never_becomes_no_timeout(tiny: float) -> None:
    """The one direction a deadline must never fail in.

    `int(0.0005 * 1000)` is 0, and the generated layer resolves the deadline as
    `timeout_ms or default` — so a truncated zero is falsy, falls through to a
    default the service clients do not set, and the TIGHTEST deadline a caller
    can ask for becomes NO deadline at all.

    `test_non_positive_timeout_is_rejected` above cannot catch this: it only
    covers values at or below zero, and these are positive and legal.

    Asserted at the resolution boundary rather than over the wire, on purpose.
    A round trip cannot settle this: the deadline is 1ms either way, so whether
    an in-process server beats it is a race — `1e-06` passed and failed across
    runs while the resolved deadline was identical. The resolution IS the bug,
    so that is what this pins.

    Its sibling `test_non_positive_timeout_is_rejected` covers values at or
    below zero. These are positive and legal, which is why they slipped past it.
    """
    from ironflow.rpc._runtime import _timeout_ms

    resolved = _timeout_ms(tiny, None)
    assert resolved is not None, f"timeout={tiny} resolved to no deadline at all"
    assert resolved >= 1, (
        f"timeout={tiny} resolved to {resolved}ms; the generated layer reads "
        f"`timeout_ms or default`, so 0 is falsy and means NO deadline"
    )


# ── lifecycle ────────────────────────────────────────────────────────────────


def test_use_after_close_raises_the_ironflow_error(client_cls: Any) -> None:
    """Not a pyqwest or connectrpc exception leaking through the contract."""
    client = client_cls(server_url="http://x", api_key=API_KEY)
    close(client)

    with pytest.raises(IronflowRPCError) as exc:
        call(client, CreateWebhookSourceRequest(name="x", event_prefix="x."))

    assert "closed" in str(exc.value)
    assert "webhooks.create_source" in str(exc.value)


def test_close_is_idempotent(client_cls: Any) -> None:
    client = client_cls(server_url="http://x", api_key=API_KEY)
    close(client)
    close(client)


def test_context_manager_closes(loop: Any) -> None:
    with serve() as srv:
        with IronflowRPC(server_url=srv.url, api_key=API_KEY) as rpc:
            rpc.webhooks.create_source(
                CreateWebhookSourceRequest(name="x", event_prefix="x.")
            )
        with pytest.raises(IronflowRPCError):
            rpc.webhooks.get_source(GetWebhookSourceRequest(id="whs_1"))


def test_async_context_manager_closes(loop: Any) -> None:
    async def scenario(url: str) -> None:
        async with AsyncIronflowRPC(server_url=url, api_key=API_KEY) as rpc:
            await rpc.webhooks.create_source(
                CreateWebhookSourceRequest(name="x", event_prefix="x.")
            )
        with pytest.raises(IronflowRPCError):
            await rpc.webhooks.get_source(GetWebhookSourceRequest(id="whs_1"))

    with serve() as srv:
        loop.run_until_complete(scenario(srv.url))


# ── round trip ───────────────────────────────────────────────────────────────


def test_response_is_a_generated_message(client_cls: Any) -> None:
    """Not a dict. The facade passes protobuf through in both directions."""
    from ironflow.rpc.v1 import WebhookSource

    with serve() as srv:
        client = client_cls(server_url=srv.url, api_key=API_KEY)
        try:
            got = call(
                client, CreateWebhookSourceRequest(name="Stripe", event_prefix="stripe.")
            )
        finally:
            close(client)

    assert isinstance(got, WebhookSource)
    assert got.id == "whs_stub"
    assert got.name == "Stripe"
