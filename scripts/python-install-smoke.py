#!/usr/bin/env python3
"""Prove an INSTALLED ironflow-py actually loads (#1781, #1829).

Run against an interpreter that has just done:

    python -m pip install --only-binary=:all: ./sdk/python

`--only-binary=:all:` is the point. Without it a missing wheel silently starts a
Rust build for pyqwest or a C build for protobuf-py-ext, and the smoke passes
having proved the toolchain works rather than that the wheel does.

WHY THIS EXISTS SEPARATELY FROM check-python-wheels.py
-----------------------------------------------------
That script resolves all 4x7 declared combinations from one machine, cheaply.
It cannot prove an installed wheel loads: a wheel built against the wrong libc
resolves perfectly and dies at import. musl is exactly where that happens, and
musl is why this runs in a container rather than on a runner.

WHY IT IMPORTS MORE THAN `ironflow`
-----------------------------------
The ConnectRPC facade resolves lazily through a PEP 562 module __getattr__, so
a bare `import ironflow` no longer loads connectrpc or either compiled package.
On a machine where those cannot load at all, that import succeeds and tells you
nothing. Touching the facade is what pulls them in.

The two compiled packages are also imported by their IMPORT names, which are
not their distribution names: protobuf-py imports as `protobuf` and
protobuf-py-ext as `protobuf_ext`.
"""

from __future__ import annotations

import platform
import sys


def main() -> int:
    import ironflow
    from ironflow import IronflowClient, IronflowError, IronflowRPC, IronflowRPCError

    # Every name the package promises. __all__ is the contract, and under a lazy
    # __getattr__ a typo in the name list fails at access rather than at import,
    # so a smoke that only did `import ironflow` would not catch it.
    missing = [n for n in ironflow.__all__ if not hasattr(ironflow, n)]
    if missing:
        print(f"FAIL: ironflow.__all__ names unbound symbols: {missing}")
        return 1

    # Forces the lazy import: builds the coordinator, its shared transport, and
    # all eight capability namespaces.
    rpc = IronflowRPC(server_url="http://127.0.0.1:1")
    rpc.webhooks.create_source
    rpc.pubsub.subscribe
    rpc.close()

    # The generated protobuf types, which is what needs the compiled extension.
    from ironflow.rpc.v1 import CreateWebhookSourceRequest, SubscribeRequest

    req = CreateWebhookSourceRequest(name="smoke", event_prefix="smoke")
    if req.name != "smoke":
        print(f"FAIL: protobuf round trip returned {req.name!r}")
        return 1
    SubscribeRequest(pattern="topic:smoke")

    # Import names, not distribution names.
    import protobuf
    import pyqwest

    # The REST client must keep working: it predates all of this and has its own
    # users. IronflowError is its error type and IronflowRPCError subclasses it,
    # so one `except IronflowError` still catches both.
    IronflowClient(server_url="http://127.0.0.1:1")
    if not issubclass(IronflowRPCError, IronflowError):
        print("FAIL: IronflowRPCError no longer subclasses IronflowError")
        return 1

    # Only meaningful on Linux, and only as a hint: libc_ver() returns an empty
    # string for musl AND for macOS, so reporting it unconditionally would print
    # "musl" on a Mac. Distinguishing glibc from musl is the reason it is here
    # at all — those are two of the seven declared targets.
    if platform.system() == "Linux":
        libc = platform.libc_ver()[0] or "musl"
    else:
        libc = "n/a"
    print(
        f"ok  python {sys.version.split()[0]}  {platform.system()}/"
        f"{platform.machine()}  libc={libc}  "
        f"pyqwest={pyqwest.__name__} protobuf={protobuf.__name__}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
