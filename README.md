# Ironflow — Python SDK

Tier-1 Python SDK for [Ironflow](https://ironflow.run), with a polling worker and generated REST and ConnectRPC clients.

[![PyPI](https://img.shields.io/pypi/v/ironflow-py)](https://pypi.org/project/ironflow-py/)

## Installation

```bash
pip install ironflow-py
```

```python
import ironflow
```

> **Install `ironflow-py`, not `ironflow`.** The distribution name is `ironflow-py`; the
> import name is `ironflow`. The bare name `ironflow` on PyPI belongs to an unrelated
> third-party project (a materials-science tool from the pyiron group), which also
> installs a top-level `ironflow` module — a virtualenv holding both has two claimants on
> that name and the install order silently decides which wins. Keep them in separate
> environments.

Requires Python 3.10+.

The first PyPI release was v0.33.1; v0.33.0 was tagged but not published.
The polling worker documented below landed on `main` after the v0.39.0 tag and
ships with the next release, not in any published wheel up to 0.39.0. SDK and engine versions track
each other.

## Status

The SDK includes hand-written polling and ConnectRPC streaming pull workers,
both with durable `step.run`, `step.sleep`, `step.sleep_until`,
`step.wait_for_event`, `step.parallel`, `step.map`, `step.invoke`,
`step.invoke_async`, `step.publish`, and `step.compensate`. Both worker classes also run managed and external
projections and apply upcasters ([see below](#projections-and-upcasters)).
It also emits events and queries runs, projections, KV, and config through
generated clients.

**Two clients, two protocols, neither a superset of the other.** `IronflowClient`
speaks REST and retries idempotent methods. `IronflowRPC` / `AsyncIronflowRPC`
speak ConnectRPC and reach 97 capabilities REST does not serve — webhook
management, agent tools, time travel, pub/sub consumer groups, function
versioning, executable deployments, raw SQL, and environment lookup/key
rotation — including four server streams. `IronflowRPC` retries only the unary methods the protos annotate
side-effect-free, and reconnects a subscription only when you position it (see
[Retries](#retries-and-what-can-still-go-wrong)).

Routes the server's manifest annotates with schemas get a typed `models.*` `TypedDict`
return and keyword-only query parameters; routes it does not annotate still return `Any`
and take no query kwargs. `TypedDict` is a type-checker construct only — there is **no
runtime validation**. Headers declared by a route, including `If-Match`,
`If-None-Match`, and `Idempotency-Key`, are generated as typed keyword arguments.
`BaseClient.request()` remains the escape hatch for undeclared headers. WebSocket and
watch endpoints (`/ws`, config watch, KV watch) have no generated methods at all and
`request()` cannot reach them either; poll the corresponding read method, use a
ConnectRPC subscription, or use the Go or JavaScript SDK for a live watch.

### Pull worker

```python
from ironflow.worker import Worker, function

@function(id="process-order", triggers=[{"event": "order.placed"}])
async def process_order(ctx):
    return await ctx.step.run("charge", lambda: charge(ctx.event.data))

Worker(functions=[process_order]).run()
```

Replace `Worker` with `StreamingWorker` to keep one ConnectRPC connection open
for assignments and worker messages. It requires an HTTP/2-capable server endpoint.

#### step.map

`step.map` runs one durable branch per item, in parallel, and returns the
results in order.

```python
async def double(item, step, index):
    return await step.run("leaf", lambda: item * 2)

results = await ctx.step.map("double-all", [1, 2, 3], double)
```

Pass `concurrency=N` to cap parallel branches. Pass `on_error="collect"` to
get every branch's outcome instead of stopping at the first failure.

#### step.invoke and step.invoke_async

`step.invoke` calls another function and waits for its result. The target
function must have no triggers; only `step.invoke` can reach it.

```python
result = await ctx.step.invoke("charge-card", {"amount": 500})
```

If the child fails, or cannot start, `step.invoke` raises `InvokeError`. It
carries `function_id`, `child_run_id`, and `cause`. `InvokeError` is never
retried. The default timeout is 30 seconds; pass `timeout=` to change it.

```python
result = await ctx.step.invoke("charge-card", {"amount": 500}, timeout="2m")
```

`step.invoke_async` starts the child and returns immediately. It does not
wait for the result.

```python
handle = await ctx.step.invoke_async("send-receipt", {"order_id": order_id})
child_run_id = handle.run_id
```

#### step.compensate

`step.compensate` registers an undo for a step. If the run later fails with a
non-retryable error, Ironflow runs the registered undos in reverse order. A
failed undo does not stop the rest.

```python
await ctx.step.run("reserve-seat", reserve)
ctx.step.compensate("reserve-seat", lambda: release_seat(seat_id))

await ctx.step.run("charge-card", charge)
ctx.step.compensate("charge-card", lambda: refund_card(charge_id))

raise NonRetryableError("no seats left downstream")
# runs refund_card, then release_seat
```

See the [Python worker reference](../../docs/reference/api/python-sdk.md#pull-worker)
for handler fields, options, durations, signals, and retry behavior. The
[Go](../../docs/reference/api/go-sdk.md#pull-mode-worker) and
[Node](../../docs/reference/js-sdk/node.md#pull-mode-worker) references show
the corresponding Tier-1 worker forms.

### Push mode

The engine POSTs each run to your HTTP endpoint. `serve()` returns an ASGI app
(FastAPI, Starlette, uvicorn, Mangum on Lambda); `handle()` is the
framework-free core behind it.

```python
import asyncio

from ironflow.serve import register, serve
from ironflow.worker import function

@function(id="send-receipt", triggers=[{"event": "order.paid"}])
async def send_receipt(ctx):
    await ctx.step.run("email", lambda: send(ctx.event.data))

ironflow_app = serve([send_receipt])   # uvicorn module:ironflow_app
# FastAPI: api.mount("/ironflow", ironflow_app)

# once per deploy, e.g. from a release script
asyncio.run(register([send_receipt], endpoint_url="https://api.example.com/ironflow/"))  # trailing slash: a mount redirects "/ironflow"
```

Set `IRONFLOW_SIGNING_KEY` to the engine's key. With it set, the handler
rejects requests without a valid `X-Ironflow-Signature`; without it, any
well-formed request runs. Pass
`webhooks=[Webhook(...)]` to serve `POST /webhooks/{id}`.

- **Short functions only.** The engine gives each push request at most the push
  timeout (10s by default), or the function's `timeout_ms` if lower. After that
  it retries while your handler may still be running. Use the pull worker for
  long work.
- **Push or pull, not both.** Do not serve one function id from a push app and a
  pull `Worker`. The worker re-registers the function as pull and silently undoes
  the push registration.

### Projections and upcasters

```python
from ironflow import UpcasterRegistry
from ironflow.projection import create_projection
from ironflow.worker import Worker

totals = create_projection(
    name="order-totals", events=["order.created"],
    initial_state=lambda: {"total": 0},
    handler=lambda state, event, ctx: {"total": state["total"] + event.data["amount"]},
)

async def notify(event, ctx):
    await send_email(event.data["email"])

emails = create_projection(name="order-emails", events=["order.completed"], handler=notify)

upcasters = UpcasterRegistry()
upcasters.register("order.created", 1, 2, lambda d: {**d, "currency": "USD"})

Worker(functions=[...], projections=[totals, emails], upcasters=upcasters).run()
```

`mode` is auto-detected from `initial_state`: passing it makes a projection
managed, omitting it makes one external. A managed handler takes
`(state, event, ctx)` and returns the next state; an external handler takes
`(event, ctx)` and returns nothing. Both may be sync or async. `Worker` and
`StreamingWorker` take the same `projections` and `upcasters` keywords, and a
projection-only worker (`functions=[]`) is allowed.

External handlers are **at-least-once** — the same event can run twice.
Managed state commits only after a successful save to the server. The
runner saves each partition on its own, so a failure mid-batch can leave
some partitions saved and others not. Projection runners do
not apply upcasters; register them for the worker's function handlers only.

**Known limitation.** Events in a batch whose state save failed, or held in
stream frames lost when the connection drops, are not redelivered — tracked
for every SDK in [#2404](https://github.com/sahina/ironflow/issues/2404).

### Agents

`ironflow.agent` builds durable AI agents on the pull worker or `serve()`. Each
LLM turn and tool call runs as its own durable step.

**(a) A tool-calling loop.** Call the model, run each tool it asks for, and
feed the tool outputs back until it stops asking for tools:

```python
from ironflow.agent import agent, define_tool

search = define_tool(name="search", handler=lambda i: web_search(i["query"]),
                     input_schema={"type": "object", "properties": {"query": {"type": "string"}}})

@agent(id="research-agent", tools=[search])
async def research(ctx):
    messages = [{"role": "user", "content": ctx.event.data["question"]}]
    while True:
        r = await ctx.llm(call=lambda: call_model(messages), messages=messages)
        calls = r.get("tool_calls", [])
        if not calls:
            return r["content"]
        for c in calls:
            output = await ctx.tool_by_name(c["name"], c["input"])
            messages.append({"role": "tool", "name": c["name"], "content": output})
```

`define_tool()` does no schema validation: a `ToolValidationError` means the args
are not JSON-serialisable, not that they failed `input_schema`.

**(b) Expose tools to an external MCP client.** Register tools with the engine,
then serve the dispatch callback next to your other functions:

```python
from ironflow.agent import DISPATCH_PATH, define_tool, expose_mcp
from ironflow.serve import serve

echo = define_tool(name="echo", handler=lambda i: {"got": i})

ironflow_app = serve([...])   # mounted at public_url
handle = await expose_mcp(name="my-agent", callback_url=f"{public_url}{DISPATCH_PATH}", tools=[echo])
# await handle.unregister() when the agent goes away
```

The engine rejects a loopback or private-network `callback_url` unless it
runs with `--dev` or `IRONFLOW_AGENT_TOOLS_ALLOW_PRIVATE=true`.

**(c) Agent memory.** Back an agent with an entity stream and a projection;
`ctx.memory` reads and appends to it:

```python
from ironflow.agent import MemoryConfig, agent

memory = MemoryConfig(stream_id="research-agent-notes", projection="research-agent-memory")

@agent(id="research-agent", memory=memory)
async def research(ctx):
    history = await ctx.memory.get()
    await ctx.memory.append("note-added", {"text": "found a source"})
```

`stream_id` and `projection` are required. `data` passed to `append()` must be
a dict; the projection must exist before the agent reads it.

## Quick Start

```python
from protobuf.wkt import Struct
from ironflow.rpc import v1
from ironflow import IronflowRPC
from ironflow import IronflowClient

client = IronflowClient(
    server_url="http://localhost:9123",
    api_key="ifkey_...",
)

# Public server inspection — REST
health = client.health()
readiness = client.ready()
capabilities = client.capabilities()

# Stored events — REST
events = client.events_list()

# Listing runs and projections is ConnectRPC-only; there is no REST sibling.
with IronflowRPC(server_url=client.server_url, api_key=client.api_key) as rpc:
    # Emit an event
    rpc.events.emit(v1.TriggerRequest(event='user.created', data=Struct.from_python({'user_id': '123', 'email': 'user@example.com'})))

    # List runs
    runs = rpc.runs.list(v1.ListRunsRequest())

    # Get a specific run
    run = rpc.runs.get(v1.GetRunRequest(id="run_abc123"))

    # List projections
    projections = rpc.projections.list(v1.ListProjectionsRequest())
```

## ConnectRPC client

For the capabilities REST does not serve. Request and response types come from
`ironflow.rpc.v1`; `ironflow._gen` is private and its layout may change.

```python
from ironflow import IronflowRPC
from ironflow.rpc.v1 import CreateWebhookSourceRequest, SubscribeRequest

with IronflowRPC(server_url="http://localhost:9123", api_key="ifkey_...") as rpc:
    source = rpc.webhooks.create_source(
        CreateWebhookSourceRequest(name="Stripe", event_prefix="stripe.")
    )

    # Server streams are ordinary iterators; breaking out cancels.
    for event in rpc.pubsub.subscribe(SubscribeRequest(pattern="topic:orders.*")):
        print(event.event_id)
        break
```

`AsyncIronflowRPC` mirrors it method for method — `await` the unary calls,
`async for` the streams, and `await rpc.aclose()` instead of `close()`.

Failures raise `IronflowRPCError`, which subclasses `IronflowError`, so
`except IronflowError` still catches every failure from either client. Full
reference, including timeouts, `NO_TIMEOUT`, and stream lifetime:
[docs.ironflow.run/reference/api/python-sdk](https://docs.ironflow.run/reference/api/python-sdk).

### Retries, and what can still go wrong

A unary call that fails with `unavailable` — a refused connection, a socket
dropped mid-response, a server shedding load — is sent again, up to 3 attempts
with exponential backoff. Nothing else is retried: every other Connect code is a
decision the server will reach again identically.

**Only side-effect-free methods are retried.** The protobuf definitions annotate
them `idempotency_level = NO_SIDE_EFFECTS`, and the client reads that annotation
rather than any list of its own. A method without it — every create, update,
delete, rotate and emit — is sent exactly once and its failure is raised to you
immediately, because a transport error cannot tell you whether the server
committed the write before the connection dropped.

Two things this does **not** promise:

- **A retried read may execute more than once on the server.** A response lost
  on the way back is indistinguishable from a request that never arrived, so the
  retry re-runs the method. That is harmless for a read by definition, and it is
  the reason the annotation gates the behaviour.
- **A write is never retried for you.** If you need one repeated safely, repeat
  it yourself with an idempotency key — `EmitRequest` and `PublishRequest` both
  carry `idempotency_key` — and treat a failed write as *unknown*, not *failed*.

`timeout=` still bounds the whole call including every retry and backoff, not
each attempt. Pass `max_attempts=1` to the constructor to switch retries off.

**`subscribe` reconnects only if you positioned it.** Set
`options.start_after_sequence` to the last `event.sequence` you processed, and a
dropped connection is retried from there. That field is both the cursor and the
opt-in: without it there is no honest place to resume from, so the stream raises
and re-subscribing is yours.

```python
from ironflow.rpc.v1 import SubscribeRequest, SubscribeOptions

cursor = 0
for event in rpc.pubsub.subscribe(SubscribeRequest(
    pattern="topic:orders.*",
    options=SubscribeOptions(start_after_sequence=cursor),
)):
    handle(event)
    cursor = event.sequence   # persist this if you need to resume across restarts
```

A resumed stream is **at-least-once**: the frame in flight when the connection
dropped may arrive twice, because the server sending it is not you having
processed it. Make `handle` idempotent, or dedupe on `event.sequence`.

**The other three streams are not reconnected, and do not need to be.**
`stream_events`, `wait_catchup_stream` and `join_consumer_group` are positioned
by the server on a durable consumer, so simply calling them again resumes where
they left off. A client-side cursor there would move a position other readers
share.

## API Coverage

This SDK is auto-generated from the Ironflow server's route manifest. Run `make sdk-health`
for the current method count — it changes on every regeneration, so it is not reproduced
here. Route coverage is not the same as usable coverage; see the Status section above.

See the [SDK Comparison Matrix](https://docs.ironflow.run/reference/sdk-comparison) for
full coverage details.

## Dependencies

The direct runtime requirements are `connectrpc~=0.12.1`, meaning at least
0.12.1 and below 0.13, and `pyqwest>=0.10,<0.11`. ConnectRPC requires
`protobuf-py>=0.3.0`. These bring in native dependencies:

```text
connectrpc ─┬─ protobuf-py ── protobuf-py-ext   (native, CPython only)
            ├─ pyqwest                          (Rust)
            │    └─ opentelemetry-api
            └─ typing_extensions
```

They are required, not an extra. The ConnectRPC client is part of the default
contract, so putting it behind a flag would turn a heavier install into a runtime
`ImportError` — see ADR 0062 (engine repo, internal).

When upgrading from the ConnectRPC 0.11 bindings, update any application
lockfile that still pins `connectrpc` to 0.11.x or `protobuf-py` to 0.1.1.
Calls through `IronflowRPC`, `AsyncIronflowRPC`, and `ironflow.rpc.v1` keep
the same signatures. This dependency upgrade does not change the worker
runtime.

`IronflowClient` itself still uses only the standard library (`urllib`, `json`),
and `import ironflow` does not load the ConnectRPC stack — that cost is paid on
first use of `IronflowRPC`.

## What lives here

- `ironflow/` — SDK source: `client.py` (REST), `rpc/` (ConnectRPC), `models.py`, `_http.py`
- `ironflow/_gen/` — generated protobuf + ConnectRPC code, vendored from the engine repo
- `tests/` — the same suite the engine repo gates on
- `pyproject.toml`, `rpc-capabilities.yaml`, and the install-smoke script under `scripts/`
- `LICENSE` and security policy

## Where the engine source lives

The Ironflow engine is **closed source** and lives at `sahina/ironflow` (private). This
mirror exists so that:

- PyPI's "Repository" link resolves to public source
- README source links (`/blob/main/...`) resolve to public source
- PyPI Trusted Publishing attests each artifact to a publicly verifiable Git SHA

## Building locally

```bash
pip install -e '.[dev]'
pytest
python -m build
```

Requires Python 3.10+.

Import-check a built wheel from a **neutral working directory**. At the package root the
source `ironflow/` directory shadows the installed one, so a wheel shipping no modules
still imports cleanly:

```bash
python -m venv .venv-smoke
.venv-smoke/bin/pip install --only-binary=:all: dist/*.whl
SMOKE_PY="$PWD/.venv-smoke/bin/python"
SMOKE="$PWD/scripts/python-install-smoke.py"
cd / && "$SMOKE_PY" "$SMOKE"
```

`--only-binary=:all:` is the point: without it a dependency missing a wheel on your
target starts a Rust build (`pyqwest`) or a C build (`protobuf-py-ext`), and the check
passes having proved your toolchain works rather than that the wheel does.

## Read-only mirror

This repo is **read-only**. Pull requests will be closed without review. Source changes
land in the engine repo and are synced here at each release.

## Bug reports

Issues are disabled on this repo. All Ironflow bug reports — SDK, engine, CLI, dashboard,
desktop — go to one tracker:

- Bugs and feature requests → [sahina/ironflow-issues](https://github.com/sahina/ironflow-issues/issues/new/choose). Pick **Python SDK** as the component, and include your Python version, platform, and a minimal repro.
- Security issues → [private advisory](https://github.com/sahina/ironflow-issues/security/advisories/new) or see [SECURITY.md](https://github.com/sahina/ironflow-py/blob/main/SECURITY.md) — do **not** open a public issue
- Commercial-licensing enquiries → the support address in [LICENSE](https://github.com/sahina/ironflow-py/blob/main/LICENSE)

## Verifying release provenance

Two independent trails.

**The published artifact.** PyPI uploads run from this mirror over
[Trusted Publishing](https://docs.pypi.org/trusted-publishers/). No PyPI API token exists
for this project, so every file carries an attestation binding it to a public commit.
Take a file URL from the project's PyPI download list and verify it with
[`pypi-attestations`](https://docs.pypi.org/attestations/consuming-attestations/):

```bash
pipx run pypi-attestations verify pypi \
  --repository https://github.com/sahina/ironflow-py \
  https://files.pythonhosted.org/packages/.../ironflow_py-<version>-py3-none-any.whl
```

**The mirror commit.** Each release tag carries an annotated message containing the
engine-side commit SHA the snapshot was built from:

```bash
git fetch --tags
git for-each-ref --format='%(contents)' refs/tags/v<version>
```

This forensic trail correlates a mirror release to the private engine commit. The
mirror's Git history is squash-snapshot per release (no engine commit messages leak
through).

## License

See [LICENSE](https://github.com/sahina/ironflow-py/blob/main/LICENSE) — SPDX
`LicenseRef-Ironflow-EULA`. Not an OSI-approved open source licence; read it before
deploying commercially.
