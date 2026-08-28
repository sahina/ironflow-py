"""An in-process Ironflow Connect server, for testing the RPC facade.

Why a real server rather than a mock (#1781, ADR 0062): a mocked transport would
assert that the interceptor calls a function the same author wrote. It cannot
catch an `Authorization` header that never reaches the wire, a codec mismatch,
or a Connect error that fails to translate — the three things the facade is
actually responsible for.

connect-py generates the SERVER side from the same protos as the client, so a
real Connect server costs one uvicorn thread. No Ironflow binary, no network.

WHY ASGI, AND NOT `wsgiref`
---------------------------
PR 2's version ran the generated WSGI app on `wsgiref.simple_server`. That
cannot serve Connect streaming in either direction, and it is not a missing
line — this was worked through before switching:

  * pyqwest sends the streaming REQUEST body chunked, so `CONTENT_LENGTH` is
    empty. `wsgiref` does not de-chunk it, and reading its unbounded
    `wsgi.input` blocks until the client disconnects.
  * With a hand-written de-chunking `WSGIRequestHandler` the request parses and
    the RESPONSE still never streams back: the client times out with zero
    frames.

`wsgiref` also fails to bound `wsgi.input` to `CONTENT_LENGTH` for ordinary
unary calls, which PEP 3333 says a server should. PR 2 carried a
`_bounded_input` shim for exactly that; uvicorn is compliant, so it is gone.

This is not the real Ironflow server — handlers return canned responses. The
suite in tests/integration/ runs against the actual binary. What this proves is
the wire contract between the facade and *a* Connect server.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Any

import uvicorn
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from typing_extensions import Self  # 3.10 has no typing.Self

from ironflow._gen.pubsub_connect import PubSubService, PubSubServiceASGIApplication
from ironflow._gen.webhook_connect import WebhookService, WebhookServiceASGIApplication

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from connectrpc.request import RequestContext

# Two services, each chosen for what it can prove.
#
# WebhookService — all 11 of its RPCs are classified `rpc`, so every method on
# rpc.webhooks goes over Connect and no failure can be masked by a REST sibling.
#
# PubSubService — carries Subscribe, the server stream the facade exposes.


class Recorder:
    """What the server saw, and what it should do next.

    One object shared by both stubs, so a test can arrange a failure without
    caring which service handles it.
    """

    def __init__(self) -> None:
        self.seen_headers: list[dict[str, str]] = []
        self.seen_timeouts: list[int | None] = []
        #: Fail the NEXT call with this Connect code. Consumed on use, so one
        #: test's arrangement cannot leak into the next call.
        self.raise_code: Code | None = None
        #: How many consecutive calls `raise_code` applies to. The retry tests
        #: need a failure that outlives one attempt; every other test wants the
        #: one-shot default, which is what 1 gives.
        self.raise_times = 1
        self.raise_message = "stub failure"
        #: Stream: fail after yielding this many events. None means never.
        self.fail_after: int | None = None
        #: How many STREAMS `fail_after` applies to. None means every one,
        #: which is what a test asserting a single failure wants. Set it to a
        #: number so a reconnect can succeed — otherwise a resuming client
        #: reconnects into the same failure forever.
        self.fail_after_times: int | None = None
        #: The `start_after_sequence` each subscribe call arrived with, in
        #: order. A reconnect that resumes from the wrong place still delivers
        #: events, so this is what distinguishes the two.
        self.seen_cursors: list[int] = []
        self.stream_code: Code = Code.RESOURCE_EXHAUSTED
        #: Events an untruncated stream yields. Finite on purpose — an
        #: unbounded stream turns a wrong assertion into a hung suite.
        self.stream_events = 3
        #: Incremented per event the SERVER yields, so a cancellation test can
        #: compare what was produced against what the client consumed.
        self.yielded = 0
        #: Handler work that actually completed.
        #:
        #: `raise_code` fires at the TOP of a handler, modelling a request lost
        #: before the server acted. `raise_after_work` fires at the BOTTOM,
        #: modelling a response lost after it did. The client cannot tell those
        #: two apart — that is the whole hazard #1809 reasons about — but the
        #: server can, and this counter is how a test reads it.
        self.work_done = 0
        self.raise_after_work: Code | None = None
        self.raise_after_work_times = 1

    def finish(self) -> None:
        """Call at the end of a unary handler, just before it returns."""
        self.work_done += 1
        if self.raise_after_work is not None:
            code = self.raise_after_work
            self.raise_after_work_times -= 1
            if self.raise_after_work_times <= 0:
                self.raise_after_work = None
            raise ConnectError(code, self.raise_message)

    def record(self, ctx: RequestContext[Any, Any]) -> None:
        self.seen_headers.append({k.lower(): v for k, v in ctx.request_headers.items()})
        self.seen_timeouts.append(ctx.timeout_ms)
        if self.raise_code is not None:
            code = self.raise_code
            self.raise_times -= 1
            if self.raise_times <= 0:
                self.raise_code = None
            raise ConnectError(code, self.raise_message)


class StubWebhookService(WebhookService):  # type: ignore[misc]
    """Unary handlers. `async def` because these run under ASGI."""

    def __init__(self, rec: Recorder) -> None:
        self.rec = rec

    async def create_webhook_source(
        self, request: Any, ctx: RequestContext[Any, Any]
    ) -> Any:
        from ironflow.rpc.v1 import WebhookSource

        self.rec.record(ctx)
        self.rec.finish()
        return WebhookSource(id="whs_stub", name=request.name)

    async def get_webhook_source(self, request: Any, ctx: RequestContext[Any, Any]) -> Any:
        from ironflow.rpc.v1 import WebhookSource

        self.rec.record(ctx)
        self.rec.finish()
        return WebhookSource(id=request.id, name="stub")

    async def list_webhook_sources(
        self, request: Any, ctx: RequestContext[Any, Any]
    ) -> Any:
        from ironflow.rpc.v1 import ListWebhookSourcesResponse

        self.rec.record(ctx)
        self.rec.finish()
        return ListWebhookSourcesResponse()


class StubPubSubService(PubSubService):  # type: ignore[misc]
    """The server stream.

    `subscribe` is an async GENERATOR — that is the shape the generated ASGI
    application expects for a server-streaming endpoint.
    """

    def __init__(self, rec: Recorder) -> None:
        self.rec = rec

    async def subscribe(
        self, request: Any, ctx: RequestContext[Any, Any]
    ) -> AsyncIterator[Any]:
        from ironflow.rpc.v1 import SubscriptionEvent

        self.rec.record(ctx)

        # Honor the resume cursor, so a test can tell a real reconnect from a
        # fresh subscription that happens to deliver the right number of
        # events. Event eN carries sequence N+1, and a cursor of N means "I
        # already have eN-1" — the same start-AFTER contract the server's
        # SubscribeOptions.start_after_sequence uses.
        start = 0
        options = getattr(request, "options", None)
        if options is not None and options.has_field("start_after_sequence"):
            start = options.start_after_sequence
        self.rec.seen_cursors.append(start)

        for i in range(self.rec.stream_events):
            if self.rec.fail_after is not None and i >= self.rec.fail_after:
                if self.rec.fail_after_times is not None:
                    self.rec.fail_after_times -= 1
                    if self.rec.fail_after_times <= 0:
                        self.rec.fail_after = None
                raise ConnectError(self.rec.stream_code, self.rec.raise_message)
            sequence = start + i + 1
            self.rec.yielded += 1
            yield SubscriptionEvent(event_id=f"e{sequence - 1}", sequence=sequence)

    async def list_topics(self, request: Any, ctx: RequestContext[Any, Any]) -> Any:
        from ironflow.rpc.v1 import ListTopicsResponse

        self.rec.record(ctx)
        self.rec.finish()
        return ListTopicsResponse()


class _Router:
    """Dispatch by service segment so one server hosts both stubs.

    connect-py generates one ASGI application per service, each owning its
    `/ironflow.v1.<Service>/<Method>` paths. A server per service would multiply
    fixture cost for no extra coverage.
    """

    def __init__(self, apps: dict[str, Any]) -> None:
        self._apps = apps

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            return
        service = scope["path"].lstrip("/").split("/")[0]
        app = self._apps.get(service)
        if app is None:
            # Answer, rather than hang. An unrouted path is a test bug, and a
            # silent stall would read as a client defect — which is exactly the
            # hour this file's header describes.
            await send({"type": "http.response.start", "status": 404, "headers": []})
            await send({"type": "http.response.body", "body": b"no such service"})
            return
        await app(scope, receive, send)


class RunningServer:
    """A started server and the URL to reach it. Use as a context manager."""

    def __init__(self, rec: Recorder, url: str, shutdown: Any) -> None:
        self.rec = rec
        self.url = url
        self._shutdown = shutdown

    @property
    def service(self) -> Recorder:
        """Alias kept so the PR-2 tests read unchanged across the ASGI move."""
        return self.rec

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self._shutdown()


def serve() -> RunningServer:
    """Start the stubs on an ephemeral port in a daemon thread.

    Port 0, not a fixed port: a fixed one turns a parallel or repeated run into
    an intermittent bind failure that reads as flakiness.
    """
    rec = Recorder()
    app = _Router(
        {
            "ironflow.v1.WebhookService": WebhookServiceASGIApplication(
                StubWebhookService(rec)
            ),
            "ironflow.v1.PubSubService": PubSubServiceASGIApplication(
                StubPubSubService(rec)
            ),
        }
    )
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("the test Connect server did not start within 10s")
        time.sleep(0.02)

    port = server.servers[0].sockets[0].getsockname()[1]

    def shutdown() -> None:
        server.should_exit = True
        thread.join(timeout=5)

    return RunningServer(rec, f"http://127.0.0.1:{port}", shutdown)
