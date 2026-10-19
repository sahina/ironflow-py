"""Version skew (ADR 0125): how this SDK names itself to the server, and the
first server version of each call a newer SDK may make to an older server."""

from __future__ import annotations

from functools import cache
from importlib.metadata import PackageNotFoundError, version

#: Sent on every request as ``python/<version>``. The server reads no value from it.
SDK_HEADER = "X-Ironflow-SDK"

#: The first server that implements ``PubSubService.SubscribeBidirectional``
#: (ADR 0123). The Go and Node SDKs carry the same value;
#: scripts/skew/check-min-versions.sh keeps them equal.
MIN_SERVER_VERSION_SUBSCRIBE_BIDIRECTIONAL = "0.48.0"

#: Connect procedure -> first server version that serves it.
MIN_SERVER_VERSIONS = {
    "/ironflow.v1.PubSubService/SubscribeBidirectional": MIN_SERVER_VERSION_SUBSCRIBE_BIDIRECTIONAL,
}


@cache
def sdk_header_value() -> str:
    try:
        return f"python/{version('ironflow-py')}"
    except PackageNotFoundError:  # run from a source tree that was never installed
        return "python/0+unknown"
