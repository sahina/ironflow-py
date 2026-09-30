# sdk/python/ironflow/serve/_response.py
from __future__ import annotations

import json
import os
from typing import Any

Response = tuple[int, dict[str, str], bytes]


def json_response(status: int, body: Any) -> Response:
    return status, {"content-type": "application/json"}, json.dumps(body, allow_nan=False).encode()


def error_response(status: int, code: str, message: str) -> Response:
    return json_response(status, {"error": {"code": code, "message": message}})


def env(value: str | None, name: str) -> str | None:
    """An explicit argument wins; otherwise the environment variable; empty means unset."""
    return value if value is not None else (os.environ.get(name) or None)
