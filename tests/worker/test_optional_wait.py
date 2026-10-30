import asyncio

import pytest

from ironflow.worker._step import ExecutionContext, Step, _Yield
from tests.worker.conftest import run


def test_optional_wait_replays_null(loop: asyncio.AbstractEventLoop) -> None:
    ctx = ExecutionContext("r", [{"step_id":"r:reply:0", "name":"reply", "status":"completed", "output":None}])
    assert run(loop, Step(ctx).wait_for_event("reply", event="reply", on_timeout="return-null")) is None


def test_optional_wait_yield_policy(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(_Yield) as caught:
        run(loop, Step(ExecutionContext("r", [])).wait_for_event("reply", event="reply", on_timeout="return-null"))
    assert caught.value.info["event_filter"]["on_timeout"] == "return-null"
