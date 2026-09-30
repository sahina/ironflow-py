from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import textwrap
import time
from typing import Any

from tests.worker.conftest import run
from tests.worker.fake_engine import FakeEngine, make_job
from tests.worker.test_worker_jobs import worker_for
from tests.worker.test_worker_loop import until


def test_drain_waits_for_short_job(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> str:
        await asyncio.sleep(0.1)
        return "done"

    engine.enqueue(make_job())
    w = worker_for(engine, h)

    async def scenario() -> None:
        task = asyncio.ensure_future(w.start())
        await until(lambda: engine.calls("ack"))
        await w.drain(timeout=5)
        await task

    run(loop, scenario())
    assert engine.calls("terminal")[0]["body"]["status"] == "completed"
    assert w.state == "stopped"


def test_drain_deadline_abandons_and_heartbeats_until_then(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> None:
        await ctx.step.run("a", lambda: 1)
        await asyncio.sleep(30)

    engine.enqueue(make_job())
    w = worker_for(engine, h, checkpoint_interval=5)

    async def scenario() -> None:
        task = asyncio.ensure_future(w.start())
        await until(lambda: engine.calls("ack"))
        await asyncio.sleep(0.05)
        started = time.monotonic()
        await w.drain(timeout=0.3)
        assert time.monotonic() - started < 2
        await task

    run(loop, scenario())
    assert engine.calls("terminal") == []
    assert [s["id"] for c in engine.calls("progress") for s in c["body"]["steps"]] == ["run_1:a:0"]
    listed = [c["body"]["jobs"] for c in engine.calls("heartbeat")]
    assert any(jobs for jobs in listed)


def test_drain_is_bounded_when_server_hangs(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    from ironflow.worker._transport import Transport

    async def h(ctx: Any) -> None:
        await ctx.step.run("a", lambda: 1)
        await asyncio.sleep(30)

    engine.enqueue(make_job())
    w = worker_for(engine, h, checkpoint_interval=5)
    w._transport = Transport(engine.url, None, "default", request_timeout=0.3)
    w._cleanup_timeout = 0.5

    async def scenario() -> None:
        task = asyncio.ensure_future(w.start())
        await until(lambda: engine.calls("ack"))
        await asyncio.sleep(0.05)
        engine.stall("progress", 30)
        engine.stall("heartbeat", 30)
        started = time.monotonic()
        await w.drain(timeout=0.2)
        await asyncio.wait_for(task, 5)
        assert time.monotonic() - started < 2

    run(loop, scenario())
    assert w.state == "stopped"


def test_stop_during_drain_forces(loop: asyncio.AbstractEventLoop, engine: FakeEngine) -> None:
    async def h(ctx: Any) -> None:
        await asyncio.sleep(30)

    engine.enqueue(make_job())
    w = worker_for(engine, h)

    async def scenario() -> None:
        task = asyncio.ensure_future(w.start())
        await until(lambda: engine.calls("ack"))
        drain = asyncio.ensure_future(w.drain(timeout=30))
        await asyncio.sleep(0.05)
        await w.stop()
        await drain
        await task

    started = time.monotonic()
    run(loop, scenario())
    assert time.monotonic() - started < 3


def test_run_exits_on_sigterm(engine: FakeEngine) -> None:
    script = textwrap.dedent(f"""
        from ironflow.worker._function import function
        from ironflow.worker._worker import Worker

        @function(id="fn", triggers=[{{"event": "e"}}])
        async def fn(ctx):
            return 1

        Worker(functions=[fn], server_url={engine.url!r}, worker_id="w-sig", drain_timeout=1).run()
    """)
    proc = subprocess.Popen([sys.executable, "-c", script], cwd=os.getcwd())
    try:
        deadline = time.monotonic() + 10
        while not engine.calls("poll") and time.monotonic() < deadline:
            time.sleep(0.05)
        assert engine.calls("poll")
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=10) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def test_second_signal_forces_running_job_to_stop(engine: FakeEngine) -> None:
    engine.enqueue(make_job())
    script = textwrap.dedent(f"""
        import asyncio
        from ironflow.worker._function import function
        from ironflow.worker._worker import Worker

        @function(id="fn", triggers=[{{"event": "e"}}])
        async def fn(ctx):
            await asyncio.sleep(30)

        Worker(functions=[fn], server_url={engine.url!r}, worker_id="w-sig-force", drain_timeout=30).run()
    """)
    proc = subprocess.Popen([sys.executable, "-c", script], cwd=os.getcwd())
    try:
        deadline = time.monotonic() + 10
        while not engine.calls("ack") and time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            time.sleep(0.05)
        assert engine.calls("ack")
        proc.send_signal(signal.SIGTERM)
        time.sleep(0.1)
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=5) == 0
        assert engine.calls("terminal") == []
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
