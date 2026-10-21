"""The functions group (#3003) through the Python SDK.

One push function this module registers, serves and deletes. The server has no
signing key, so the handler accepts unsigned requests. The tests share it and
run in file order.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Iterator
from contextlib import ExitStack
from typing import Any

import pytest
from protobuf.wkt import Struct

from ironflow import IronflowRPC, IronflowRPCError
from ironflow.rpc.v1 import (
    DeleteFunctionRequest,
    ExecutionMode,
    FunctionStatus,
    GetFunctionRequest,
    GetRunRequest,
    InvokeFunctionRequest,
    InvokeFunctionSyncRequest,
    ListFunctionHistoryRequest,
    ListFunctionsRequest,
    RegisterFunctionRequest,
    RollbackFunctionRequest,
    RunStatus,
    TriggerRequest,
    UpdateFunctionStatusRequest,
)
from ironflow.serve import register, serve
from ironflow.worker import function

from .test_push_serve import running

FN_ID = f"readiness-fn-python-{uuid.uuid4().hex[:8]}"
NAME = "Readiness functions (Python)"


def _fn(description: str) -> Any:
    @function(
        id=FN_ID, name=NAME, description=description, triggers=[{"event": f"readiness.functions.{FN_ID}"}],
        retry={"max_attempts": 4, "initial_delay": "2s", "backoff_factor": 3, "max_delay": "60s"},
        timeout="45s", concurrency={"limit": 2, "key": "who"}, metadata={"team": "readiness"},
    )
    async def handler(ctx: Any) -> Any:
        return {"echoed": ctx.event.data}

    return handler


@pytest.fixture(scope="module")
def _servers() -> Iterator[ExitStack]:
    with ExitStack() as stack:
        yield stack


@pytest.fixture
def endpoint(readiness_seed: dict[str, Any], _servers: ExitStack) -> str:
    """The push server, started on first use so a skipped run starts nothing."""
    if not hasattr(_servers, "url"):
        _servers.url = _servers.enter_context(running(serve([_fn("first")]))) + "/"  # type: ignore[attr-defined]
    return _servers.url  # type: ignore[attr-defined,no-any-return]


def _register(description: str, endpoint: str, server_url: str, api_key: str) -> None:
    asyncio.run(register([_fn(description)], endpoint_url=endpoint, server_url=server_url, api_key=api_key))


def test_function_register_and_get(readiness_seed: dict[str, Any], rpc: IronflowRPC, endpoint: str, server_url: str, api_key: str) -> None:
    _register("first", endpoint, server_url, api_key)
    fn = rpc.functions.get(GetFunctionRequest(id=FN_ID))
    assert (fn.id, fn.name, fn.description) == (FN_ID, NAME, "first")
    assert [t.event for t in fn.triggers] == [f"readiness.functions.{FN_ID}"]
    assert (fn.retry.max_attempts, fn.retry.initial_delay_ms, fn.retry.backoff_factor, fn.retry.max_delay_ms) == (
        4, 2000, 3, 60000,
    )
    assert (fn.timeout_ms, fn.concurrency.limit, fn.concurrency.key) == (45000, 2, "who")
    assert (fn.preferred_mode, fn.endpoint_url) == (ExecutionMode.PUSH, endpoint)
    assert (fn.status, fn.version) == (FunctionStatus.ACTIVE, 1)
    assert fn.metadata.to_python()["team"] == "readiness"
    assert fn.created_at is not None and fn.updated_at is not None

    # A second registration updates in place: version 2, new description.
    _register("second", endpoint, server_url, api_key)
    fn = rpc.functions.get(GetFunctionRequest(id=FN_ID))
    assert (fn.version, fn.description) == (2, "second")


def test_function_list(readiness_seed: dict[str, Any], rpc: IronflowRPC) -> None:
    listed = {f.id: f for f in rpc.functions.list(ListFunctionsRequest(limit=1000)).functions}
    fn = listed[FN_ID]
    assert (fn.name, fn.status, fn.preferred_mode) == (NAME, FunctionStatus.ACTIVE, ExecutionMode.PUSH)
    only = rpc.functions.list(ListFunctionsRequest(name=NAME))
    assert [f.id for f in only.functions] == [FN_ID] and only.total_count == 1


def test_function_invoke(readiness_seed: dict[str, Any], rpc: IronflowRPC) -> None:
    data = {"who": "José"}
    res = rpc.runs.invoke_function_sync(
        InvokeFunctionSyncRequest(function_id=FN_ID, data=Struct.from_python(data), timeout_ms=30000)
    ).result
    assert res.run_id and res.function_id == FN_ID and res.status == RunStatus.COMPLETED
    assert res.output.to_python() == {"echoed": data}
    assert rpc.runs.get(GetRunRequest(id=res.run_id)).input.to_python() == data

    with pytest.raises(IronflowRPCError) as exc:
        rpc.runs.invoke_function_sync(InvokeFunctionSyncRequest(function_id=f"{FN_ID}-missing", timeout_ms=5000))
    assert exc.value.code == "not_found", exc.value.code


def _wait_completed(rpc: IronflowRPC, run_id: str) -> Any:
    deadline = time.monotonic() + 30
    while (run := rpc.runs.get(GetRunRequest(id=run_id))).status != RunStatus.COMPLETED:
        assert run.status != RunStatus.FAILED and time.monotonic() < deadline, run
        time.sleep(0.2)
    return run


def test_function_invoke_async(readiness_seed: dict[str, Any], rpc: IronflowRPC) -> None:
    data = {"who": "José"}
    started = rpc.functions.invoke(InvokeFunctionRequest(function_id=FN_ID, data=Struct.from_python(data)))
    assert started.run_id and started.event_id
    run = _wait_completed(rpc, started.run_id)
    assert run.function_id == FN_ID and run.event_id == started.event_id
    assert run.input.to_python() == data and run.output.to_python() == {"echoed": data}

    # A retry with the same key returns the original run and event instead of a second run.
    key = f"readiness-{uuid.uuid4().hex}"
    first = rpc.functions.invoke(InvokeFunctionRequest(function_id=FN_ID, data=Struct.from_python(data), idempotency_key=key))
    again = rpc.functions.invoke(InvokeFunctionRequest(function_id=FN_ID, data=Struct.from_python(data), idempotency_key=key))
    assert (again.run_id, again.event_id) == (first.run_id, first.event_id)
    assert first.run_id != started.run_id

    # A key belongs to one function in one environment: another function gets already_exists.
    other = f"{FN_ID}-other"
    rpc.functions.register(RegisterFunctionRequest(id=other, name=other))
    try:
        with pytest.raises(IronflowRPCError) as exc:
            rpc.functions.invoke(InvokeFunctionRequest(function_id=other, idempotency_key=key))
        assert exc.value.code == "already_exists", exc.value.code
    finally:
        rpc.functions.delete(DeleteFunctionRequest(id=other))

    with pytest.raises(IronflowRPCError) as exc:
        rpc.functions.invoke(InvokeFunctionRequest(function_id=f"{FN_ID}-missing"))
    assert exc.value.code == "not_found", exc.value.code


def test_function_status(readiness_seed: dict[str, Any], rpc: IronflowRPC) -> None:
    before = rpc.functions.get(GetFunctionRequest(id=FN_ID))
    paused = rpc.functions.update_status(UpdateFunctionStatusRequest(id=FN_ID, status=FunctionStatus.PAUSED))
    assert (paused.status, paused.version) == (FunctionStatus.PAUSED, before.version + 1)
    assert rpc.functions.get(GetFunctionRequest(id=FN_ID)).status == FunctionStatus.PAUSED
    # A paused function starts no run for its trigger event.
    assert not rpc.events.emit(TriggerRequest(event=f"readiness.functions.{FN_ID}")).run_ids
    active = rpc.functions.update_status(UpdateFunctionStatusRequest(id=FN_ID, status=FunctionStatus.ACTIVE))
    assert (active.status, active.version) == (FunctionStatus.ACTIVE, before.version + 2)
    assert len(rpc.events.emit(TriggerRequest(event=f"readiness.functions.{FN_ID}")).run_ids) == 1


def test_function_history(readiness_seed: dict[str, Any], rpc: IronflowRPC) -> None:
    hist = rpc.functions.list_history(ListFunctionHistoryRequest(function_id=FN_ID, limit=100))
    # Newest first: activate, pause, re-register, register.
    assert [e.change_type for e in hist.entries] == ["status_change", "status_change", "update", "created"]
    assert [e.function_snapshot.status for e in hist.entries] == [
        FunctionStatus.ACTIVE, FunctionStatus.PAUSED, FunctionStatus.ACTIVE, FunctionStatus.ACTIVE,
    ]
    assert not hist.has_more
    for i, e in enumerate(hist.entries):
        assert e.function_id == FN_ID and e.event_id and e.recorded_at is not None
        if i:
            assert e.entity_version == hist.entries[i - 1].entity_version - 1
    assert hist.entries[0].function_snapshot.description == "second"
    assert hist.entries[3].function_snapshot.description == "first"

    page1 = rpc.functions.list_history(ListFunctionHistoryRequest(function_id=FN_ID, limit=2))
    assert len(page1.entries) == 2 and page1.has_more
    page2 = rpc.functions.list_history(
        ListFunctionHistoryRequest(function_id=FN_ID, limit=2, from_version=page1.entries[1].entity_version)
    )
    assert [e.event_id for e in page2.entries] == [e.event_id for e in hist.entries[2:]] and not page2.has_more


def test_function_rollback(readiness_seed: dict[str, Any], rpc: IronflowRPC) -> None:
    rpc.functions.update_status(UpdateFunctionStatusRequest(id=FN_ID, status=FunctionStatus.PAUSED))
    before = rpc.functions.get(GetFunctionRequest(id=FN_ID))
    entries = rpc.functions.list_history(ListFunctionHistoryRequest(function_id=FN_ID, limit=100)).entries
    created = entries[-1]
    assert created.change_type == "created"

    back = rpc.functions.rollback(
        RollbackFunctionRequest(function_id=FN_ID, version=created.entity_version, change_reason="readiness rollback")
    ).function
    assert (back.description, back.status, back.version) == ("first", FunctionStatus.ACTIVE, before.version + 1)
    now = rpc.functions.get(GetFunctionRequest(id=FN_ID))
    assert (now.description, now.status) == ("first", FunctionStatus.ACTIVE)
    newest = rpc.functions.list_history(ListFunctionHistoryRequest(function_id=FN_ID, limit=1)).entries[0]
    assert (newest.change_type, newest.change_reason) == ("rollback", "readiness rollback")
    assert newest.function_snapshot.description == "first"
    with pytest.raises(IronflowRPCError) as exc:
        rpc.functions.rollback(RollbackFunctionRequest(function_id=FN_ID, version=9999))
    assert exc.value.code == "not_found", exc.value.code


def test_function_delete(readiness_seed: dict[str, Any], rpc: IronflowRPC) -> None:
    rpc.functions.delete(DeleteFunctionRequest(id=FN_ID))
    with pytest.raises(IronflowRPCError) as exc:
        rpc.functions.get(GetFunctionRequest(id=FN_ID))
    assert exc.value.code == "not_found", exc.value.code
    assert FN_ID not in {f.id for f in rpc.functions.list(ListFunctionsRequest(limit=1000)).functions}
    # The history outlives the function and ends with the delete.
    newest = rpc.functions.list_history(ListFunctionHistoryRequest(function_id=FN_ID, limit=1)).entries
    assert [e.change_type for e in newest] == ["delete"]
    with pytest.raises(IronflowRPCError) as exc:
        rpc.functions.delete(DeleteFunctionRequest(id=FN_ID))
    assert exc.value.code == "not_found", exc.value.code
