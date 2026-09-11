# Ironflow — Python SDK

Python client for [Ironflow](https://ironflow.run) — the Continuous History platform for backend systems.

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

Available from v0.33.0. Versions track the Ironflow engine release, so `ironflow-py`
0.33.0 is the client for engine 0.33.0.

## Status

Experimental and client-only. It can emit events and query runs, projections,
KV, and config, but it ships **no worker runtime** — there is no `step.run`, no
`step.sleep`, and no push or pull mode.

**Two clients, two protocols, neither a superset of the other.** `IronflowClient`
speaks REST and retries idempotent methods. `IronflowRPC` / `AsyncIronflowRPC`
speak ConnectRPC and reach 86 capabilities REST does not serve — webhook
management, agent tools, time travel, pub/sub consumer groups, function
versioning, and environment lookup/key rotation — including four server
streams. `IronflowRPC` retries only the unary methods the protos annotate
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

For durable step execution, use the Go or JavaScript SDK.

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

# Runs and projections are ConnectRPC-only; there is no REST sibling.
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

Two direct runtime dependencies, `connectrpc` and `pyqwest`, which resolve to six
packages — two of them compiled:

```text
connectrpc ─┬─ protobuf-py ── protobuf-py-ext   (native, CPython only)
            ├─ pyqwest                          (Rust)
            │    └─ opentelemetry-api
            └─ typing_extensions
```

They are required, not an extra. The ConnectRPC client is part of the default
contract, so putting it behind a flag would turn a heavier install into a runtime
`ImportError` — see ADR 0062 (engine repo, internal).

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
