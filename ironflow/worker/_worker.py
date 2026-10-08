"""The pull-mode worker: register, heartbeat, poll, run jobs, report."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import os
import platform
import signal
import socket
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib import metadata
from typing import TYPE_CHECKING, Any

from .._discovery import hydrate_env_from_discovery
from .._http import DEFAULT_SERVER_URL, IronflowError, _error_retryable
from ._checkpoint import Checkpointer
from ._duration import Duration, iso_utc, to_seconds
from ._function import Function, registration_body
from ._protocol import STALE_EXECUTION, JobAssignment, fence, parse_job, parse_jobs
from ._publish import bind_publish
from ._run import run_function, upcast_event
from ._step import Event, ExecutionContext, RunInfo, encode_json
from ._transport import Reply, Transport

if TYPE_CHECKING:
    from ..projection import Projection
    from ..upcaster import UpcasterRegistry


class WorkerAuthError(IronflowError):
    """The server rejected the worker's credentials (401 or 403)."""


def _sdk_version() -> str:
    try:
        return metadata.version("ironflow-py")
    except metadata.PackageNotFoundError:
        return "0.0.0"


@dataclass
class _ActiveJob:
    job: JobAssignment
    started_at: str
    task: asyncio.Task[None] | None = None
    checkpointer: Checkpointer | None = None
    abandoned: bool = field(default=False)
    flush_on_cancel: bool = field(default=False)  # drain deadline: one bounded final flush


class Worker:
    def __init__(
        self, *, functions: Sequence[Function], server_url: str | None = None, api_key: str | None = None,
        environment: str | None = None, worker_id: str | None = None, max_concurrent_jobs: int = 10,
        heartbeat_interval: Duration = 30, reconnect_delay: Duration = 5, checkpoint_interval: Duration = 1,
        drain_timeout: Duration = 60, labels: Mapping[str, str] | None = None, logger: logging.Logger | None = None,
        upcasters: UpcasterRegistry | None = None, projections: Sequence[Projection] = (),
    ) -> None:
        if not functions and not projections:
            raise ValueError("a worker needs at least one function or projection")
        ids = [f.id for f in functions]
        if len(set(ids)) != len(ids):
            raise ValueError(f"duplicate function ids: {ids}")
        names = [p.name for p in projections]
        if len(set(names)) != len(names):
            raise ValueError(f"duplicate projection names: {names}")
        if max_concurrent_jobs < 1:
            raise ValueError("max_concurrent_jobs must be at least 1")
        self._functions = {f.id: f for f in functions}
        self._projections = list(projections)
        self._runners: list[Any] = []
        self._runner_tasks: list[asyncio.Task[None]] = []
        self.worker_id = worker_id or str(uuid.uuid4())
        self._max = max_concurrent_jobs
        self._heartbeat_interval = to_seconds(heartbeat_interval)
        self._reconnect_delay = to_seconds(reconnect_delay)
        self._checkpoint_interval = to_seconds(checkpoint_interval)
        self._drain_timeout = to_seconds(drain_timeout)
        self._labels = dict(labels or {})
        self._log = logger or logging.getLogger("ironflow.worker")
        self._upcasters = upcasters
        hydrate_env_from_discovery()
        self._transport = Transport(
            server_url or os.environ.get("IRONFLOW_SERVER_URL") or DEFAULT_SERVER_URL,
            api_key or os.environ.get("IRONFLOW_API_KEY") or None,
            environment or os.environ.get("IRONFLOW_ENV") or "default",
        )
        self.state = "idle"
        self._jobs: dict[str, _ActiveJob] = {}
        self._idle_poll = 1.0
        self._max_backoff = 60.0
        self._report_delays: tuple[float, ...] = (1.0, 2.0, 4.0, 8.0)
        self._cleanup_timeout = 5.0
        self._fatal: BaseException | None = None
        self._background: set[asyncio.Task[None]] = set()
        self._draining: asyncio.Event | None = None
        self._stopped: asyncio.Event | None = None
        self._force: asyncio.Event | None = None
        self._slot_freed: asyncio.Event | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._reregister = False

    # ---- lifecycle -------------------------------------------------------

    def run(self) -> None:
        """Run synchronously. A signal drains; a second signal stops immediately."""
        async def main() -> None:
            loop = asyncio.get_running_loop()
            count = 0

            def on_signal() -> None:
                nonlocal count
                count += 1
                asyncio.ensure_future(self.drain() if count == 1 else self.stop())

            for sig in (signal.SIGINT, signal.SIGTERM):
                with contextlib.suppress(NotImplementedError):
                    loop.add_signal_handler(sig, on_signal)
            await self.start()

        asyncio.run(main())

    async def start(self) -> None:
        """Run the worker. Does not return until drain(), stop(), or a fatal error."""
        if self.state != "idle":
            raise RuntimeError(f"worker already started (state {self.state})")
        self._draining, self._stopped = asyncio.Event(), asyncio.Event()
        self._force, self._slot_freed = asyncio.Event(), asyncio.Event()
        try:
            if not self._functions:
                # No functions to register: the server rejects an empty
                # function_ids list. Skip registration, heartbeat and polling
                # entirely and just run the projection runners (#2395 follow-up).
                self.state = "connected"
                self._start_projection_runners()
                await self._draining.wait()
            else:
                while not self._draining.is_set():
                    self.state = "connecting"
                    self._reregister = False
                    try:
                        await self._register()
                    except WorkerAuthError:
                        raise
                    except IronflowError as exc:
                        if not exc.retryable:
                            self._log.error("registration failed: %s", exc)
                            raise
                        self._log.warning("registration failed: %s", exc)
                        await self._pause(self._reconnect_delay)
                        continue
                    if self._draining.is_set():
                        break
                    if self._reregister:
                        continue
                    if self._heartbeat_task is None:
                        self._heartbeat_task = asyncio.ensure_future(self._heartbeat_loop())
                    self._start_projection_runners()
                    self.state = "connected"
                    await self._poll_loop()
            await self._stopped.wait()
        except BaseException:
            await self._shutdown_now()
            raise
        if self._fatal is not None:
            raise self._fatal

    def _fail(self, error: BaseException) -> None:
        """A fatal error seen off the poll path (e.g. heartbeat 401): stop everything, then start() raises it."""
        if self._fatal is not None:
            return
        self._fatal = error
        self._log.error("worker stopping: %s", error)
        assert self._draining is not None
        self._draining.set()
        for active in list(self._jobs.values()):
            self._abandon(active, "worker credentials rejected")
        task = asyncio.ensure_future(self._shutdown_now())
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def stop(self) -> None:
        """Stop now: drain with a zero deadline."""
        await self.drain(timeout=0)

    async def drain(self, timeout: Duration | None = None) -> None:
        """Stop polling and wait for active jobs, up to the deadline (Task 13)."""
        if self.state in ("idle", "stopped") or self._draining is None:
            return
        assert self._force is not None and self._stopped is not None
        if self._draining.is_set():
            if timeout == 0:
                self._force.set()
            await self._stopped.wait()
            return
        self.state = "draining"
        self._draining.set()
        limit = self._drain_timeout if timeout is None else to_seconds(timeout)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + limit
        pending = {j.task for j in self._jobs.values() if j.task is not None}
        while pending and loop.time() < deadline and not self._force.is_set():
            _, pending = await asyncio.wait(pending, timeout=min(0.1, max(deadline - loop.time(), 0)))
        # Deadline passed: cancel FIRST, then let each job's cancellation path do
        # one bounded checkpoint flush (see _execute). The whole cleanup is
        # bounded by cleanup_timeout, whatever the server does.
        leftover = []
        for active in list(self._jobs.values()):
            if active.task is not None and not active.task.done():
                active.flush_on_cancel = True
                self._abandon(active, "drain deadline passed")
                leftover.append(active.task)
        if leftover:
            await asyncio.wait(leftover, timeout=self._cleanup_timeout)
        await self._shutdown_now()

    def _start_projection_runners(self) -> None:
        if not self._projections or self._runner_tasks:
            return
        from .._gen.projection_connect import ProjectionServiceClient
        from ..projection import (
            _runner,  # lazy: keeps connectrpc out of `import ironflow.worker`
        )

        client = ProjectionServiceClient(self._transport._base)
        for p in self._projections:
            runner = _runner.ProjectionRunner(p, client, self._transport.headers, self._log)
            self._runners.append(runner)
            task = asyncio.ensure_future(runner.run())
            task.add_done_callback(functools.partial(self._runner_done, p.name))
            self._runner_tasks.append(task)
        self._log.info("started %d projection runner(s)", len(self._projections))

    def _runner_done(self, name: str, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            # Auth or a crash stops this runner only; the worker keeps running (#1673).
            self._log.error("projection runner %s stopped: %s", name, exc)

    async def _shutdown_now(self) -> None:
        tasks = [j.task for j in self._jobs.values() if j.task is not None and not j.task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            # Bounded: a job's cancellation path does at most one flush, itself
            # bounded by cleanup_timeout and the transport's request_timeout.
            await asyncio.wait(tasks, timeout=self._cleanup_timeout)
        if self._heartbeat_task is not None and self._heartbeat_task is not asyncio.current_task():
            self._heartbeat_task.cancel()
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
        await asyncio.gather(*(r.stop() for r in self._runners), return_exceptions=True)
        for task in self._runner_tasks:
            task.cancel()
        if self._runner_tasks:
            await asyncio.gather(*self._runner_tasks, return_exceptions=True)
        self.state = "stopped"
        if self._draining is not None:
            self._draining.set()
        if self._stopped is not None:
            self._stopped.set()

    async def _pause(self, seconds: float) -> None:
        """Sleep, but wake at once when draining starts."""
        assert self._draining is not None
        try:
            await asyncio.wait_for(self._draining.wait(), seconds)
        except asyncio.TimeoutError:
            pass

    # ---- registration and heartbeat ---------------------------------------

    def _raise_for(self, reply: Reply, what: str) -> None:
        if reply.ok:
            return
        if reply.status in (401, 403):
            raise WorkerAuthError(f"{what}: unauthorized ({reply.status})", status_code=reply.status)
        raise IronflowError(f"{what} failed: {reply.status} {reply.body}", status_code=reply.status,
                            code=reply.error_code, retryable=_error_retryable(reply.status, reply.body))

    async def _register(self) -> None:
        await self._register_functions()
        reply = await self._transport.request("POST", f"/api/v1/workers/{self.worker_id}/register", {
            "worker_id": self.worker_id,
            "hostname": socket.gethostname() or "unknown",
            "function_ids": list(self._functions),
            "max_concurrent_jobs": self._max,
            "labels": self._labels,
            "version": {"sdk": _sdk_version(), "runtime": f"python-{platform.python_version()}"},
        })
        self._raise_for(reply, "register worker")

    async def _register_functions(self) -> None:
        for fn in self._functions.values():
            reply = await self._transport.request(
                "POST", "/ironflow.v1.IronflowService/RegisterFunction", registration_body(fn))
            self._raise_for(reply, f"register function {fn.id}")

    async def _heartbeat_loop(self) -> None:
        # Runs in every state until stopped, including draining and report
        # retries: the server renews a lease ONLY for a job in this list.
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            body = {
                "worker_id": self.worker_id,
                "active_jobs": len(self._jobs),
                "jobs": [{"job_id": jid, "started_at": j.started_at}
                         for jid, j in self._jobs.items() if not j.abandoned],
            }
            try:
                reply = await self._transport.request("POST", f"/api/v1/workers/{self.worker_id}/heartbeat", body)
            except IronflowError as exc:
                self._log.debug("heartbeat failed: %s", exc)
                continue
            if reply.status in (401, 403):
                # With every slot busy the poll loop is idle, so this may be the
                # only call that sees a revoked key. Leases would expire while
                # handlers keep running: stop the worker instead.
                self._fail(WorkerAuthError(f"heartbeat: unauthorized ({reply.status})", status_code=reply.status))
                return
            if reply.status == 404:
                await self._lost_registration()
                return

    # ---- polling ------------------------------------------------------------

    async def _poll_loop(self) -> None:
        assert self._draining is not None and self._slot_freed is not None
        backoff = self._reconnect_delay
        while not self._draining.is_set():
            if self._reregister:
                return
            free = self._max - len(self._jobs)
            if free <= 0:
                self._slot_freed.clear()
                try:
                    await asyncio.wait_for(self._slot_freed.wait(), self._idle_poll)
                except asyncio.TimeoutError:
                    pass
                continue
            try:
                reply = await self._transport.request(
                    "GET", f"/api/v1/workers/{self.worker_id}/jobs?available={free}")
            except IronflowError as exc:
                self._log.warning("poll failed: %s", exc)
                await self._pause(backoff)
                backoff = min(backoff * 2, self._max_backoff)
                continue
            if reply.status in (401, 403):
                raise WorkerAuthError(f"poll: unauthorized ({reply.status})", status_code=reply.status)
            if reply.status == 404:
                await self._lost_registration()
                return
            if self._reregister:
                return
            if not reply.ok:
                self._log.warning("poll failed: %s %s", reply.status, reply.body)
                await self._pause(backoff)
                backoff = min(backoff * 2, self._max_backoff)
                continue
            backoff = self._reconnect_delay  # reset only on a 2xx (#1673)
            if reply.status == 204:
                await self._pause(self._idle_poll)
                continue
            try:
                raws = parse_jobs(reply.body)
            except (TypeError, ValueError) as exc:
                self._log.error("invalid poll reply: %s", exc)
                await self._pause(self._idle_poll)
                continue
            if self._draining.is_set():
                self._log.warning("draining; not starting %d polled job(s); their leases will expire", len(raws))
                return
            for raw in raws[:free]:
                try:
                    self._start_job(parse_job(raw))
                except ValueError as exc:
                    self._log.error("invalid job assignment: %s", exc)

    # ---- jobs ---------------------------------------------------------------

    async def _lost_registration(self) -> None:
        self._log.warning("worker not registered on the server; registering again")
        self._reregister = True
        for active in list(self._jobs.values()):
            active.flush_on_cancel = False
            self._abandon(active, "worker registration lost")
        heartbeat = self._heartbeat_task
        self._heartbeat_task = None
        if heartbeat is not None and heartbeat is not asyncio.current_task():
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
        if self._slot_freed is not None:
            self._slot_freed.set()

    def _start_job(self, job: JobAssignment) -> None:
        if job["job_id"] in self._jobs:
            self._log.warning("duplicate assignment for active job %s; ignoring", job["job_id"])
            return
        active = _ActiveJob(job=job, started_at=iso_utc(datetime.now(timezone.utc)))
        self._jobs[job["job_id"]] = active
        active.task = asyncio.ensure_future(self._execute(active))
        active.task.add_done_callback(lambda _t: self._job_done(job["job_id"]))

    def _job_done(self, job_id: str) -> None:
        self._jobs.pop(job_id, None)
        if self._slot_freed is not None:
            self._slot_freed.set()

    def _abandon(self, active: _ActiveJob, reason: str) -> None:
        # Leave the heartbeat list at once so the lease can expire into recovery.
        active.abandoned = True
        self._log.warning("abandoning job %s: %s", active.job["job_id"], reason)
        if active.task is not None and not active.task.done():
            active.task.cancel()

    def _path(self, job: JobAssignment, suffix: str = "") -> str:
        return f"/api/v1/workers/{self.worker_id}/jobs/{job['job_id']}{suffix}"

    def _event_for(self, job: JobAssignment) -> Event:
        return upcast_event(job["event"], self._upcasters)

    async def _execute(self, active: _ActiveJob) -> None:
        job = active.job
        fn = self._functions.get(job["function_id"])
        if fn is None:
            await self._report(active, {"status": "failed", "error": {
                "message": f"Function not found: {job['function_id']}", "code": "FUNCTION_NOT_FOUND",
                "retryable": False}}, [], job.get("step_sequence_base", 0))
            return
        try:
            ack = await self._transport.request("PUT", self._path(job, "/ack"), {
                "run_id": job["run_id"], **fence(job)})
        except IronflowError as exc:
            self._log.warning("ack failed for job %s: %s; dropping it", job["job_id"], exc)
            return
        if not ack.ok:
            if ack.status in (401, 403):
                self._fail(WorkerAuthError(f"ack: unauthorized ({ack.status})", status_code=ack.status))
            self._log.warning("ack rejected for job %s (%s); dropping it", job["job_id"], ack.status)
            return

        ctx = ExecutionContext(job["run_id"], job["completed_steps"], fn.step_timeout)
        ctx.publish = bind_publish(self._transport, job["run_id"])
        cp = Checkpointer(transport=self._transport, worker_id=self.worker_id, job=job, ctx=ctx,
                          interval=self._checkpoint_interval, logger=self._log,
                          on_stale=lambda: self._abandon(active, "fenced out on checkpoint"))
        active.checkpointer = cp
        ctx.on_step_recorded = cp.schedule
        try:
            outcome = await run_function(
                fn, raw_event=job["event"], upcasters=self._upcasters, ctx=ctx,
                run=RunInfo(id=job["run_id"], function_id=job["function_id"], attempt=job["attempt"],
                            environment=self._transport._environment),
                secrets=(job.get("context") or {}).get("secrets") or {},
                logger=logging.LoggerAdapter(self._log, {"run_id": job["run_id"], "function_id": job["function_id"]}),
            )

            steps, offset = await cp.finish()
            await self._report(active, outcome, steps, offset)
        except asyncio.CancelledError:
            # Abandoned (fenced out, credentials rejected, or drain deadline).
            # Only the drain path gets one bounded final flush; never a report.
            if active.flush_on_cancel:
                with contextlib.suppress(Exception, asyncio.CancelledError):
                    await asyncio.wait_for(cp.flush(), self._cleanup_timeout)
            raise
        finally:
            # Ownership ends here on EVERY path: no checkpoint timer, retry or
            # in-flight flush may outlive the job.
            await cp.close()

    async def _report(self, active: _ActiveJob, outcome: dict[str, Any], steps: list[Any], offset: int) -> None:
        job = active.job
        body = {**outcome, "steps": steps, "step_offset": offset, **fence(job)}
        try:
            encode_json(body)
        except (TypeError, ValueError) as exc:
            body = {"status": "failed", "error": {
                "message": f"result is not JSON-encodable: {exc}",
                "code": "SERIALIZATION_ERROR", "retryable": False,
            }, "steps": steps, "step_offset": offset, **fence(job)}
        for delay in (*self._report_delays, None):
            try:
                reply = await self._transport.request("PUT", self._path(job), body)
            except IronflowError as exc:
                self._log.warning("report failed for job %s: %s", job["job_id"], exc)
            else:
                if reply.ok:
                    return
                if reply.status in (401, 403):
                    self._fail(WorkerAuthError(f"report: unauthorized ({reply.status})", status_code=reply.status))
                    return
                if reply.status == 409 and reply.error_code == STALE_EXECUTION:
                    self._log.warning("job %s was superseded; its result is discarded", job["job_id"])
                    return
                if 400 <= reply.status < 500:
                    self._log.error("report rejected for job %s: %s %s", job["job_id"], reply.status, reply.body)
                    return
                self._log.warning("report failed for job %s: %s", job["job_id"], reply.status)
            if delay is None:
                break
            await asyncio.sleep(delay)
        # All attempts failed: abandon. The engine reclaims the run after the
        # lease expires and replays past the persisted steps.
        self._log.error("giving up on the report for job %s; the engine will reclaim the run", job["job_id"])
