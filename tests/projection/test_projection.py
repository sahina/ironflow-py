from __future__ import annotations

import subprocess
import sys

import pytest

from ironflow.projection import create_projection


def test_mode_detected_from_initial_state() -> None:
    managed = create_projection(name="p", events=["a"], handler=lambda s, e, c: s, initial_state=dict)
    external = create_projection(name="q", events=["a"], handler=lambda e, c: None)
    assert managed.mode == "managed" and external.mode == "external"


def test_defaults() -> None:
    p = create_projection(name="p", events=["a"], handler=lambda e, c: None)
    assert (p.max_retries, p.batch_size, p.partition_key, p.events) == (3, 100, "", ("a",))


def test_explicit_mode_wins() -> None:
    p = create_projection(name="p", events=["a"], handler=lambda s, e, c: s, mode="managed")
    assert p.mode == "managed"


@pytest.mark.parametrize("kw", [{"name": ""}, {"events": []}, {"mode": "weird"}, {"batch_size": 0}])
def test_validation(kw: dict) -> None:
    args = {"name": "p", "events": ["a"], "handler": lambda e, c: None, **kw}
    with pytest.raises(ValueError):
        create_projection(**args)


def test_import_does_not_load_connectrpc_runner() -> None:
    # A fresh interpreter, not this test process: pytest's collection phase imports
    # every test module up front (including test_runner_batch.py, which imports
    # ironflow.projection._runner), so sys.modules here is already tainted by the
    # time any test body runs.
    code = "import sys, ironflow.projection; assert 'ironflow.projection._runner' not in sys.modules"
    subprocess.run([sys.executable, "-c", code], check=True)
