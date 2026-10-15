from __future__ import annotations

import asyncio
from typing import Any

from ironflow.worker import StreamingWorker, function
from tests.worker.conftest import run


def test_streaming_worker_heartbeats_while_draining(
    loop: asyncio.AbstractEventLoop,
) -> None:
    @function(id="hb-fn", triggers=[{"event": "hb.event"}])
    async def handler(_ctx: Any) -> None:
        return None

    async def scenario() -> tuple[int, int]:
        worker = StreamingWorker(functions=[handler], heartbeat_interval=0.02)
        sent: list[Any] = []
        worker._send = lambda m: sent.append((worker.state, m)) or True  # type: ignore[assignment]
        worker._registered = True
        worker.state = "connected"
        task = asyncio.ensure_future(worker._heartbeat_loop())
        await asyncio.sleep(0.25)
        connected = len(sent)
        worker.state = "draining"  # what Worker.drain() sets before waiting for jobs
        await asyncio.sleep(0.25)
        draining = len(sent) - connected
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        print(f"python streaming heartbeats connected={connected} draining={draining}")
        return connected, draining

    connected, draining = run(loop, scenario())
    assert connected > 0
    assert draining > 0, "no heartbeats sent while state == draining"
