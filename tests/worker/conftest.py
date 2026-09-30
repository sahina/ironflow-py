from __future__ import annotations

import asyncio
from collections.abc import Coroutine, Iterator
from typing import Any, TypeVar

import pytest

from tests.worker.fake_engine import FakeEngine

T = TypeVar("T")


@pytest.fixture()
def engine() -> Iterator[FakeEngine]:
    e = FakeEngine()
    yield e
    e.shutdown()


@pytest.fixture()
def loop() -> Iterator[asyncio.AbstractEventLoop]:
    # The package has no async test plugin; drive the loop by hand, as
    # tests/test_rpc_client.py does.
    lp = asyncio.new_event_loop()
    asyncio.set_event_loop(lp)
    yield lp
    lp.close()


def run(loop: asyncio.AbstractEventLoop, coro: Coroutine[Any, Any, T]) -> T:
    # Bounded: a lost wakeup in the worker would otherwise block the whole suite
    # (and the release gate) forever. On timeout, say where every task is parked.
    async def bounded() -> T:
        task = asyncio.ensure_future(coro)
        done, _ = await asyncio.wait({task}, timeout=30)
        if not done:
            import io
            buf = io.StringIO()
            for t in asyncio.all_tasks():
                t.print_stack(file=buf)
            task.cancel()
            raise TimeoutError("scenario hung; task stacks:\n" + buf.getvalue())
        return task.result()

    return loop.run_until_complete(bounded())
