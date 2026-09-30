from __future__ import annotations

import asyncio
from typing import Any
from unittest import mock

import pytest

from ironflow.projection import create_projection
from tests.worker.conftest import run
from tests.worker.fake_engine import FakeEngine
from tests.worker.test_worker_loop import fast_worker


def proj(name: str) -> Any:
    return create_projection(name=name, events=["e"], handler=lambda e, c: None)


def test_duplicate_projection_names_rejected(engine: FakeEngine) -> None:
    with pytest.raises(ValueError, match="duplicate projection names"):
        fast_worker(engine, projections=[proj("a"), proj("a")])


def test_projection_only_worker_is_allowed(engine: FakeEngine) -> None:
    fast_worker(engine, functions=[], projections=[proj("a")])


def test_runners_start_and_stop_with_worker(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    started, stopped = [], []

    class FakeRunner:
        def __init__(self, p: Any, *_: Any) -> None:
            self.p = p

        async def run(self) -> None:
            started.append(self.p.name)
            await asyncio.sleep(3600)

        async def stop(self) -> None:
            stopped.append(self.p.name)

    with mock.patch("ironflow.projection._runner.ProjectionRunner", FakeRunner):
        w = fast_worker(engine, projections=[proj("a"), proj("b")])

        async def go() -> None:
            task = asyncio.ensure_future(w.start())
            while len(started) < 2:
                await asyncio.sleep(0.01)
            await w.stop()
            await asyncio.wait_for(task, 2)

        run(loop, go())
    assert sorted(started) == ["a", "b"] and sorted(stopped) == ["a", "b"]


def test_projection_only_worker_skips_worker_registration_and_polling(
    loop: asyncio.AbstractEventLoop, engine: FakeEngine
) -> None:
    """Against a real server, an empty function_ids list is rejected with 400.
    A projection-only worker must never attempt worker registration, polling,
    or heartbeats — only its projection runners (#2395 follow-up)."""
    started, stopped = [], []

    class FakeRunner:
        def __init__(self, p: Any, *_: Any) -> None:
            self.p = p

        async def run(self) -> None:
            started.append(self.p.name)
            await asyncio.sleep(3600)

        async def stop(self) -> None:
            stopped.append(self.p.name)

    with mock.patch("ironflow.projection._runner.ProjectionRunner", FakeRunner):
        w = fast_worker(engine, functions=[], projections=[proj("a")])

        async def go() -> None:
            task = asyncio.ensure_future(w.start())
            while not started:
                await asyncio.sleep(0.01)
            await w.stop()
            await asyncio.wait_for(task, 2)

        run(loop, go())
    assert started == ["a"] and stopped == ["a"]
    assert engine.calls("register") == []
    assert engine.calls("poll") == []
    assert engine.calls("heartbeat") == []


def test_runners_stop_concurrently(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    started, in_stop, overlap = [], [0], [0]

    class FakeRunner:
        def __init__(self, p: Any, *_: Any) -> None:
            self.p = p

        async def run(self) -> None:
            started.append(self.p.name)
            await asyncio.sleep(3600)

        async def stop(self) -> None:
            in_stop[0] += 1
            overlap[0] = max(overlap[0], in_stop[0])
            await asyncio.sleep(0.05)
            in_stop[0] -= 1
            if self.p.name == "a":
                raise RuntimeError("stop failed")  # must not keep "b" from stopping

    with mock.patch("ironflow.projection._runner.ProjectionRunner", FakeRunner):
        w = fast_worker(engine, projections=[proj("a"), proj("b")])

        async def go() -> None:
            task = asyncio.ensure_future(w.start())
            while len(started) < 2:
                await asyncio.sleep(0.01)
            await w.stop()
            await asyncio.wait_for(task, 2)

        run(loop, go())
    assert overlap[0] == 2 and w.state == "stopped"
