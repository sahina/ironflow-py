"""Tool execution: step naming and args-based deduplication."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from ..worker import Step
from ._errors import ToolValidationError
from ._types import ToolDefinition


def hash_args(defn: ToolDefinition, args: Any) -> str:
    # Names a Python step only; it need not match Node's stableStringify byte for byte.
    try:
        raw = json.dumps(args, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        raise ToolValidationError(defn.name, f"args are not JSON-serialisable: {exc}") from None
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def validate_input(defn: ToolDefinition, args: Any) -> None:
    try:
        import jsonschema
    except ImportError:
        return  # `ironflow-py[validate]` not installed: the schema is passed to the model only.
    try:
        jsonschema.validate(args, defn.input_schema)
    except jsonschema.ValidationError as exc:
        path = "/".join(str(p) for p in exc.absolute_path)
        raise ToolValidationError(defn.name, f"{path or '<root>'}: {exc.message}") from None
    except jsonschema.SchemaError as exc:
        raise ToolValidationError(defn.name, f"input_schema is invalid: {exc.message}") from None


async def run_tool(step: Step, defn: ToolDefinition, args: Any, cache: dict[str, Any]) -> Any:
    validate_input(defn, args)
    if defn.idempotent == "by_args":
        h = hash_args(defn, args)
        key = f"{defn.name}:{h}"
        if key not in cache:
            cache[key] = await step.run(f"tool.{defn.name}.{h}", lambda: defn.handler(args), timeout=defn.timeout)
        return cache[key]
    return await step.run(f"tool.{defn.name}", lambda: defn.handler(args), timeout=defn.timeout)
