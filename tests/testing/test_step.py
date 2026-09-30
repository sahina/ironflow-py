from __future__ import annotations

import asyncio
from typing import Any

import pytest

from ironflow.testing import TestStep, TestStepRecord
from ironflow.worker import NonRetryableError, Step


def _step(**mocks: Any) -> TestStep:
    return TestStep(mocks.get("steps", {}), mocks.get("invokes", {}), mocks.get("events", {}), [], [])


def test_parity_with_step() -> None:
    public = {n for n in dir(Step) if not n.startswith("_")}
    missing = public - {n for n in dir(TestStep) if not n.startswith("_")}
    assert not missing, f"TestStep is missing Step methods: {sorted(missing)}"


def test_run_uses_mock_else_real_fn() -> None:
    s = _step(steps={"charge": lambda: {"ok": True}})
    assert asyncio.run(s.run("charge", lambda: "real")) == {"ok": True}
    assert asyncio.run(s.run("other", lambda: "real")) == "real"
    assert s._records == [TestStepRecord("charge", "run", {"ok": True}), TestStepRecord("other", "run", "real")]


def test_run_accepts_sync_and_async() -> None:
    async def body() -> int:
        return 2

    s = _step(steps={"m": lambda: body()})
    assert asyncio.run(s.run("a", lambda: 1)) == 1
    assert asyncio.run(s.run("b", body)) == 2
    assert asyncio.run(s.run("m", lambda: None)) == 2


def test_run_error_is_recorded_and_raised() -> None:
    def boom() -> None:
        raise RuntimeError("nope")

    s = _step()
    with pytest.raises(RuntimeError, match="nope"):
        asyncio.run(s.run("x", boom))
    assert s._records[0].name == "x" and isinstance(s._records[0].error, RuntimeError)


def test_repeated_name_records_each_call() -> None:
    s = _step(steps={"n": lambda: 7})
    asyncio.run(s.run("n", lambda: 0))
    asyncio.run(s.run("n", lambda: 0))
    assert [r.name for r in s._records] == ["n", "n"]


def test_invoke_needs_mock() -> None:
    s = _step(invokes={"send": lambda data: {"sent": data}})
    assert asyncio.run(s.invoke("send", 3)) == {"sent": 3}
    with pytest.raises(LookupError, match="mock_invoke\\('missing'"):
        asyncio.run(s.invoke("missing"))


def test_invoke_async_returns_fake_run_id() -> None:
    calls: list[Any] = []
    s = _step(invokes={"job": calls.append})
    result = asyncio.run(s.invoke_async("job", {"a": 1}))
    assert result.run_id.startswith("test-run-") and calls == [{"a": 1}]


def test_wait_for_event_pops_queue() -> None:
    s = _step(events={"paid": [{"n": 1}, {"n": 2}]})
    first = asyncio.run(s.wait_for_event("w", event="paid"))
    second = asyncio.run(s.wait_for_event("w", event="paid"))
    assert (first.name, first.data, second.data) == ("paid", {"n": 1}, {"n": 2})
    with pytest.raises(LookupError, match="send_event\\('paid'"):
        asyncio.run(s.wait_for_event("w", event="paid"))


def test_sleep_records_and_returns() -> None:
    s = _step()
    asyncio.run(s.sleep("nap", "1h"))
    asyncio.run(s.sleep_until("later", "2999-01-01T00:00:00Z"))
    assert [(r.name, r.type) for r in s._records] == [("nap", "sleep"), ("later", "sleep")]


def test_parallel_and_map() -> None:
    s = _step(steps={"a": lambda: "A"})

    async def branch(st: Any) -> Any:
        return await st.run("a", lambda: "real")

    assert asyncio.run(s.parallel("p", [branch, branch])) == ["A", "A"]

    async def double(x: int, st: Any, i: int) -> int:
        return await st.run(f"d{i}", lambda: x * 2)

    assert asyncio.run(s.map("m", [1, 2], double)) == [2, 4]


def test_parallel_collect_returns_errors() -> None:
    async def bad(st: Any) -> None:
        raise ValueError("b")

    async def good(st: Any) -> int:
        return 1

    out = asyncio.run(_step().parallel("p", [bad, good], on_error="collect"))
    assert isinstance(out[0], ValueError) and out[1] == 1
    with pytest.raises(ValueError):
        asyncio.run(_step().parallel("p", [bad, good]))


def test_compensate_registers() -> None:
    s = _step()
    s.compensate("charge", lambda: None)
    assert [name for name, _ in s._compensations] == ["charge"]


def test_invoke_async_needs_mock() -> None:
    with pytest.raises(LookupError, match="mock_invoke\\('missing'"):
        asyncio.run(_step().invoke_async("missing"))


def test_compensate_rejects_empty_name_like_step() -> None:
    step = TestStep({}, {}, {}, [], [])
    with pytest.raises(ValueError, match="non-empty"):
        step.compensate("", lambda: None)


def test_publish_records_the_topic_and_data() -> None:
    s = _step()
    result = asyncio.run(s.publish("orders", {"a": 1}))
    assert result.event_id.startswith("test-evt-") and result.sequence >= 1
    assert s._records == [TestStepRecord("publish:orders", "publish", {"a": 1})]


def test_publish_rejects_what_the_real_step_rejects() -> None:
    s = _step()
    with pytest.raises(ValueError, match="topic"):
        asyncio.run(s.publish(""))
    with pytest.raises(NonRetryableError) as e:
        asyncio.run(s.publish("orders", {"x": float("nan")}))
    assert e.value.code == "SERIALIZATION_ERROR" and s._records == []
