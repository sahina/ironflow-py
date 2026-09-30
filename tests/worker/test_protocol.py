from __future__ import annotations

from typing import Any

import pytest

from ironflow.worker._protocol import fence, parse_job, parse_jobs


def job(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "job_id": "run_1", "run_id": "run_1", "function_id": "fn", "attempt": 1,
        "event": {"id": "ev_1", "name": "e", "version": 1, "data": {"a": 1}, "timestamp": "2026-09-24T10:00:00Z"},
        "completed_steps": [{"step_id": "run_1:a:0", "name": "run_1:a:0", "output": 1}],
        "step_sequence_base": 2, "execution_seq": 7, "lease_token": "tok",
    }
    base.update(overrides)
    return base


def test_parse_job_accepts_a_capacity_assignment() -> None:
    parsed = parse_job(job())
    assert parsed["run_id"] == "run_1"
    assert parsed["completed_steps"][0]["step_id"] == "run_1:a:0"


@pytest.mark.parametrize("missing", ["job_id", "run_id", "function_id", "event", "completed_steps", "execution_seq", "lease_token"])
def test_parse_job_rejects_missing_required_field(missing: str) -> None:
    raw = job()
    del raw[missing]
    with pytest.raises(ValueError, match=missing):
        parse_job(raw)


def test_parse_job_rejects_bool_as_int() -> None:
    with pytest.raises(ValueError, match="execution_seq"):
        parse_job(job(execution_seq=True))


def test_parse_job_rejects_completed_step_without_id() -> None:
    with pytest.raises(ValueError, match="completed_steps"):
        parse_job(job(completed_steps=[{"name": "x"}]))


def test_parse_jobs_requires_batched_shape() -> None:
    assert parse_jobs({"jobs": [1, 2]}) == [1, 2]
    with pytest.raises(TypeError):
        parse_jobs(job())


def test_fence() -> None:
    assert fence(parse_job(job())) == {"execution_seq": 7, "lease_token": "tok"}


@pytest.mark.parametrize("raw", [None, [], "job"])
def test_parse_job_rejects_non_object_with_value_error(raw: object) -> None:
    with pytest.raises(ValueError):
        parse_job(raw)


@pytest.mark.parametrize(
    ("changes", "field"),
    [
        ({"event": {"id": "e", "name": "n", "timestamp": "t"}}, "data"),
        ({"event": {"id": "e", "name": "n", "data": {}, "timestamp": "t", "version": True}}, "version"),
        ({"completed_steps": [{"step_id": "s", "output": 1}]}, "name"),
        ({"completed_steps": [{"step_id": "s", "name": "n"}]}, "output"),
        ({"step_sequence_base": True}, "step_sequence_base"),
        ({"context": {"secrets": {"secret": 1}}}, "secrets"),
    ],
)
def test_parse_job_rejects_malformed_nested_fields(changes: dict[str, Any], field: str) -> None:
    with pytest.raises(ValueError, match=field):
        parse_job(job(**changes))


def test_parse_job_rejects_non_object_completed_step_with_value_error() -> None:
    with pytest.raises(ValueError, match="completed_steps"):
        parse_job(job(completed_steps=[None]))


def test_parse_job_accepts_a_failed_step_row() -> None:
    parsed = parse_job(job(completed_steps=[
        {"step_id": "b", "name": "b", "output": None, "status": "failed", "error": {"cause": "x"}},
    ]))
    assert parsed["completed_steps"][0]["status"] == "failed"


def test_parse_job_rejects_non_string_status() -> None:
    with pytest.raises(ValueError, match="status"):
        parse_job(job(completed_steps=[{"step_id": "b", "name": "b", "output": None, "status": 1}]))
