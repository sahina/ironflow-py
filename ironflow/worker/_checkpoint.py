"""Mid-run step checkpoints for pull workers."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from ._protocol import (
    MAX_CHECKPOINT_STEPS,
    STALE_EXECUTION,
    JobAssignment,
    StepResult,
    fence,
)
from ._step import ExecutionContext
from ._transport import Transport


class Checkpointer:
    MAX_BACKOFF = 30.0

    def __init__(
        self, *, transport: Transport, worker_id: str, job: JobAssignment, ctx: ExecutionContext,
        interval: float, on_stale: Callable[[], None], logger: logging.Logger,
    ) -> None:
        self._transport = transport
        self._path = f"/api/v1/workers/{worker_id}/jobs/{job['job_id']}"
        self._job = job
        self._ctx = ctx
        self._interval = interval
        self._on_stale = on_stale
        self._log = logger
        self._base = job.get("step_sequence_base", 0)
        self._cursor = 0
        self._failures = 0
        self._disabled = interval <= 0
        self._stopped = False
        self._lock = asyncio.Lock()
        self._timer: asyncio.TimerHandle | None = None
        self._tasks: set[asyncio.Task[None]] = set()

    def schedule(self, delay: float | None = None) -> None:
        if self._disabled or self._stopped or self._timer is not None:
            return
        self._timer = asyncio.get_running_loop().call_later(
            self._interval if delay is None else delay, self._fire,
        )

    def _fire(self) -> None:
        self._timer = None
        task = asyncio.create_task(self.flush())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def flush(self) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._tasks.add(task)
        try:
            async with self._lock:
                if self._disabled or self._stopped:
                    return
                pending = self._ctx.executed[self._cursor:self._cursor + MAX_CHECKPOINT_STEPS]
                if not pending:
                    return
                body = {
                    "status": "progress", "steps": pending, "step_offset": self._base + self._cursor,
                    **fence(self._job),
                }
                try:
                    reply = await self._transport.request("PUT", self._path, body)
                except Exception as exc:  # noqa: BLE001 - transport errors leave the tail for report
                    self._log.debug("checkpoint failed for job %s: %s", self._job["job_id"], exc)
                    self._retry_later()
                    return
                if reply.ok:
                    self._cursor += len(pending)
                    self._failures = 0
                    if self._cursor < len(self._ctx.executed):
                        self.schedule()
                    return
                if reply.status == 409 and reply.error_code == STALE_EXECUTION:
                    self._stopped = True
                    self._on_stale()
                    return
                if 400 <= reply.status < 500:
                    self._disabled = True
                    self._log.warning(
                        "checkpoints disabled for job %s: server answered %s",
                        self._job["job_id"], reply.status,
                    )
                    return
                self._retry_later()
        finally:
            if task is not None:
                self._tasks.discard(task)

    def _retry_later(self) -> None:
        self._failures = min(self._failures + 1, 32)
        self.schedule(min(max(self._interval, 0.001) * 2 ** self._failures, self.MAX_BACKOFF))

    async def finish(self) -> tuple[list[StepResult], int]:
        """Stop checkpointing and return the unflushed tail and its offset."""
        self._stopped = True
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        async with self._lock:
            return list(self._ctx.executed[self._cursor:]), self._base + self._cursor

    async def close(self) -> None:
        """Cancel scheduled and in-flight checkpoint work. Idempotent."""
        self._stopped = True
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        current = asyncio.current_task()
        tasks = [task for task in self._tasks if task is not current and not task.done()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
