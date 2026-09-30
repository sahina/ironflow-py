from __future__ import annotations

import json
from typing import Any

import pytest

from ironflow._gen.ironflow_pb import RegisterFunctionRequest
from ironflow.worker._function import Function, function, registration_body


def test_decorator_returns_function_and_keeps_handler() -> None:
    @function(id="agent", triggers=[{"event": "agent.requested"}])
    async def agent(ctx: Any) -> str:
        return "ok"

    assert isinstance(agent, Function)
    assert agent.id == "agent"
    assert agent.handler.__name__ == "agent"


def test_rejects_sync_handler() -> None:
    with pytest.raises(TypeError, match="async def"):
        function(id="x", triggers=[{"event": "e"}])(lambda ctx: 1)  # type: ignore[arg-type,return-value]


def test_function_without_triggers_is_invoke_only() -> None:
    @function(id="child")
    async def child(ctx: Any) -> None: ...

    assert child.triggers == []


@pytest.mark.parametrize("triggers", [[{}], [{"event": "e", "cron": "* * * * *"}], [{"evnt": "e"}]])
def test_rejects_bad_triggers(triggers: list[dict[str, str]]) -> None:
    async def h(ctx: Any) -> None: ...
    with pytest.raises(ValueError, match="trigger"):
        function(id="x", triggers=triggers)(h)  # type: ignore[arg-type]


@pytest.mark.parametrize("trigger", [{"event": ""}, {"cron": ""}])
def test_rejects_empty_trigger_value(trigger: dict[str, str]) -> None:
    async def h(ctx: Any) -> None: ...
    with pytest.raises(ValueError, match="trigger"):
        function(id="x", triggers=[trigger])(h)  # type: ignore[list-item]


def test_rejects_empty_id() -> None:
    async def h(ctx: Any) -> None: ...
    with pytest.raises(ValueError, match="id"):
        function(id="", triggers=[{"event": "e"}])(h)


def test_registration_body_maps_to_connect_json() -> None:
    @function(
        id="agent", name="Agent", triggers=[{"event": "a"}, {"cron": "0 * * * *"}],
        retry={"max_attempts": 3, "initial_delay": "1s", "backoff_factor": 2.0, "max_delay": 60},
        timeout="10m", concurrency={"limit": 2, "key": "event.data.user"},
    )
    async def agent(ctx: Any) -> None: ...

    body = registration_body(agent)
    assert body["id"] == "agent"
    assert body["name"] == "Agent"
    assert body["triggers"] == [{"event": "a"}, {"cron": "0 * * * *"}]
    assert body["preferredMode"] == "EXECUTION_MODE_PULL"
    assert body["retry"] == {"maxAttempts": 3, "initialDelayMs": 1000, "backoffFactor": 2.0, "maxDelayMs": 60000}
    assert body["timeoutMs"] == 600000
    assert body["concurrency"] == {"limit": 2, "key": "event.data.user"}
    assert len(body["metadata"]["__ironflow_code_hash"]) == 16


def test_registration_body_minimal() -> None:
    @function(id="m", triggers=[{"event": "e"}])
    async def m(ctx: Any) -> None: ...

    body = registration_body(m)
    assert body["name"] == "m"
    assert "retry" not in body and "timeoutMs" not in body and "concurrency" not in body


def test_public_exports() -> None:
    import ironflow.worker as w

    assert sorted(w.__all__) == sorted([
        "CancelOnSpec", "Context", "DebounceConfig", "Duration", "Event", "Function", "InvokeAsyncResult",
        "InvokeError", "NonRetryableError", "PublishResult", "RecordingProfile", "RunInfo",
        "SchemaValidationError", "Step", "StepError", "StepTimeoutError", "StreamingWorker", "Worker", "WorkerAuthError", "function",
    ])
    assert all(hasattr(w, name) for name in w.__all__)


def full_config_function() -> Function:
    @function(
        id="full", triggers=[{"event": "order.placed"}], description="Handles orders", step_timeout="30s",
        debounce={"period": "5s", "key": "data.customerId", "max_wait": "1m"},
        cancel_on=[{"event": "order.cancelled", "match": "data.orderId"}],
        actor_key="data.customerId", secrets=["STRIPE_KEY"], recording=True, recording_profile="steps",
        recording_retention="30d", metadata={"team": "payments", "__ironflow_code_hash": "user"},
    )
    async def full(ctx: Any) -> None: ...

    return full


def test_registration_body_carries_every_config_field() -> None:
    fn = full_config_function()
    body = registration_body(fn)
    assert body["description"] == "Handles orders"
    assert body["debounce"] == {"periodMs": 5000, "key": "data.customerId", "maxWaitMs": 60000}
    assert body["cancelOn"] == [{"event": "order.cancelled", "match": "data.orderId"}]
    assert body["actorKey"] == "data.customerId"
    assert body["secrets"] == ["STRIPE_KEY"]
    assert body["recording"] is True
    assert body["recordingProfile"] == "steps"
    assert body["recordingRetention"] == "30d"
    # The reserved code-hash key wins over user metadata, as in Node.
    assert body["metadata"] == {"team": "payments", "__ironflow_code_hash": fn.code_hash}
    # step_timeout and schema are SDK-side only.
    assert "stepTimeout" not in body and "stepTimeoutMs" not in body and "schema" not in body


def test_registration_body_parses_as_the_proto_request() -> None:
    """The body is valid proto JSON for RegisterFunctionRequest, field for field."""
    req = RegisterFunctionRequest.from_json(json.dumps(registration_body(full_config_function())))
    assert req.description == "Handles orders"
    assert req.debounce is not None
    assert (req.debounce.period_ms, req.debounce.key, req.debounce.max_wait_ms) == (5000, "data.customerId", 60000)
    assert [(c.event, c.match) for c in req.cancel_on] == [("order.cancelled", "data.orderId")]
    assert req.actor_key == "data.customerId"
    assert list(req.secrets) == ["STRIPE_KEY"]
    assert req.recording is True
    assert req.recording_profile == "steps"
    assert req.recording_retention == "30d"


@pytest.mark.parametrize("debounce", [{"period": 2}, {"period": 2, "max_wait": 0}])
def test_debounce_without_max_wait_omits_it(debounce: Any) -> None:
    @function(id="d", triggers=[{"event": "e"}], debounce=debounce)
    async def d(ctx: Any) -> None: ...

    assert registration_body(d)["debounce"] == {"periodMs": 2000, "key": ""}


def test_zero_step_timeout_means_no_default() -> None:
    @function(id="z", triggers=[{"event": "e"}], step_timeout=0)
    async def z(ctx: Any) -> None: ...

    assert z.step_timeout is None


@pytest.mark.parametrize(("kwargs", "match"), [
    ({"debounce": {"period": 0.5}}, "debounce period"),
    ({"debounce": {"period": "5s", "max_wait": "1s"}}, "max_wait"),
    ({"cancel_on": [{"event": "", "match": "data.id"}]}, "cancel_on"),
    ({"cancel_on": [{"event": "e", "match": ""}]}, "cancel_on"),
    ({"cancel_on": [{"event": "e", "match": "m"}, {"event": "e", "match": "m"}]}, "duplicate"),
    ({"recording_profile": "everything"}, "recording_profile"),
    ({"step_timeout": -1}, "duration"),
])
def test_rejects_invalid_config(kwargs: dict[str, Any], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        function(id="x", triggers=[{"event": "e"}], **kwargs)


def test_push_registration_body_parses_as_push() -> None:
    from ironflow._gen.types_pb import ExecutionMode

    @function(id="p", triggers=[{"event": "e"}])
    async def p(ctx: Any) -> None: ...
    body = registration_body(p, mode="push", endpoint_url="https://fn.example/ironflow")
    req = RegisterFunctionRequest.from_json(json.dumps(body))
    assert req.preferred_mode == ExecutionMode.PUSH
    assert req.endpoint_url == "https://fn.example/ironflow"


def test_push_registration_requires_endpoint_url() -> None:
    @function(id="p", triggers=[{"event": "e"}])
    async def p(ctx: Any) -> None: ...
    with pytest.raises(ValueError, match="endpoint_url"):
        registration_body(p, mode="push")


def test_pull_registration_body_unchanged() -> None:
    @function(id="p", triggers=[{"event": "e"}])
    async def p(ctx: Any) -> None: ...
    body = registration_body(p)
    assert body["preferredMode"] == "EXECUTION_MODE_PULL" and "endpointUrl" not in body
