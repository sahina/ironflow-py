"""Pytest fixtures.

The scripted server lives in harness.py so test modules can import Response
directly without going through conftest.
"""

from __future__ import annotations

import pytest

from tests.harness import ScriptedServer


@pytest.fixture()
def server():
    s = ScriptedServer()
    yield s
    s.shutdown()
