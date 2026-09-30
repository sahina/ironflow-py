from __future__ import annotations

import asyncio
from collections.abc import Iterator

import pytest


@pytest.fixture()
def loop() -> Iterator[asyncio.AbstractEventLoop]:
    # The package has no async test plugin; drive the loop by hand, mirroring
    # tests/worker/conftest.py.
    lp = asyncio.new_event_loop()
    asyncio.set_event_loop(lp)
    yield lp
    lp.close()
