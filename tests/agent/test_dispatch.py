import asyncio
import hashlib
import hmac
import json
import time
from pathlib import Path

import pytest

from ironflow.agent import DISPATCH_PATH, _registry, define_tool
from ironflow.agent._dispatch import handle_dispatch, verify_hmac
from ironflow.agent._registry import RegisteredTool
from ironflow.serve import handle

VECTORS = json.loads((Path(__file__).resolve().parents[4] / "testdata" / "hmac_vectors.json").read_text())
SECRET = "ab" * 32


def test_dispatch_path_matches_serve_literal() -> None:
    assert DISPATCH_PATH == "/ironflow/agent-tools/dispatch"


@pytest.mark.parametrize("v", VECTORS["vectors"], ids=lambda v: v["name"])
def test_shared_vectors(v) -> None:
    sig = v["expected_signature"].removeprefix("sha256=")
    assert verify_hmac(v["body"].encode(), v["timestamp"], sig, v["secret_hex"])
    assert not verify_hmac(v["body"].encode() + b"x", v["timestamp"], sig, v["secret_hex"])


def test_verify_rejects_bad_hex() -> None:
    assert not verify_hmac(b"", 1, "zz", SECRET)
    assert not verify_hmac(b"", 1, "", SECRET)
    assert not verify_hmac(b"", 1, "00", "not-hex")


def signed(body: dict, ts: int | None = None, secret: str = SECRET) -> tuple[dict, bytes]:
    raw = json.dumps(body).encode()
    ts = int(time.time()) if ts is None else ts
    mac = hmac.new(bytes.fromhex(secret), f"{ts}.".encode() + raw, hashlib.sha256).hexdigest()
    return {"x-ironflow-signature": f"sha256={mac}", "x-ironflow-timestamp": str(ts)}, raw


@pytest.fixture(autouse=True)
def tools():
    _registry.clear_local()

    def boom(_):
        raise RuntimeError("kaput")

    async def aecho(i):
        return i

    def not_json(_):
        return object()

    def nan(_):
        return float("nan")
    _registry.register_local(RegisteredTool("demo", "demo.echo", SECRET, define_tool(name="echo", handler=aecho)))
    _registry.register_local(RegisteredTool("demo", "demo.boom", SECRET, define_tool(name="boom", handler=boom)))
    _registry.register_local(
        RegisteredTool("demo", "demo.not_json", SECRET, define_tool(name="not_json", handler=not_json)))
    _registry.register_local(RegisteredTool("demo", "demo.nan", SECRET, define_tool(name="nan", handler=nan)))
    yield
    _registry.clear_local()


def call(headers, raw):
    return asyncio.run(handle_dispatch(headers, raw))


def test_success() -> None:
    assert call(*signed({"qualified_name": "demo.echo", "input": {"x": 21}})) == (200, {"output": {"x": 21}})


def test_handler_error_is_envelope() -> None:
    status, body = call(*signed({"qualified_name": "demo.boom", "input": {}}))
    assert status == 200 and body == {"error": {"code": "HANDLER_ERROR", "message": "kaput"}}


def test_non_json_output_is_handler_error() -> None:
    status, body = call(*signed({"qualified_name": "demo.not_json", "input": {}}))
    assert status == 200 and body["error"]["code"] == "HANDLER_ERROR"


def test_nan_output_is_handler_error() -> None:
    status, body = call(*signed({"qualified_name": "demo.nan", "input": {}}))
    assert status == 200 and body["error"]["code"] == "HANDLER_ERROR"


def test_unknown_tool_looks_like_bad_signature() -> None:
    status, body = call(*signed({"qualified_name": "demo.nope", "input": {}}))
    assert status == 401 and body == {"error": {"code": "SIGNATURE_MISMATCH", "message": "HMAC mismatch"}}


@pytest.mark.parametrize("skew,msg", [(-301, "too old"), (61, "too far in future")])
def test_replay_window(skew, msg) -> None:
    status, body = call(*signed({"qualified_name": "demo.echo"}, ts=int(time.time()) + skew))
    assert status == 401 and body["error"]["code"] == "TIMESTAMP_SKEW" and msg in body["error"]["message"]


def test_header_errors() -> None:
    h, raw = signed({"qualified_name": "demo.echo"}, secret="cd" * 32)
    assert call(h, raw) == (401, {"error": {"code": "SIGNATURE_MISMATCH", "message": "HMAC mismatch"}})
    assert call({}, raw)[1]["error"]["message"] == "missing HMAC headers"
    assert call({**h, "x-ironflow-signature": "md5=00"}, raw)[1]["error"]["message"] == "invalid signature format"
    assert call({**h, "x-ironflow-timestamp": "soon"}, raw)[1]["error"]["message"] == "invalid timestamp"


def test_body_errors() -> None:
    h, _ = signed({})
    assert call(h, b"{not json")[1]["error"] == {"code": "INVALID_REQUEST",
                                                  "message": "callback body is not valid JSON"}
    assert call(h, b"[1]")[1]["error"]["message"] == "callback body is not valid JSON"
    assert call(*signed({"input": {}}))[1]["error"]["message"] == "qualified_name missing"
    assert call(h, b"x" * ((1 << 20) + 1))[1]["error"]["message"] == "failed to read body"


def test_serve_routes_dispatch() -> None:
    h, raw = signed({"qualified_name": "demo.echo", "input": 1})
    status, headers, body = asyncio.run(handle([], method="POST", path=DISPATCH_PATH, headers=h, body=raw))
    assert status == 200 and headers["content-type"] == "application/json"
    assert json.loads(body) == {"output": 1}


def test_asgi_serve_routes_dispatch_under_a_mount_prefix() -> None:
    from ironflow.serve import serve

    h, raw = signed({"qualified_name": "demo.echo", "input": 1})
    scope = {"type": "http", "method": "POST", "path": f"/api{DISPATCH_PATH}", "root_path": "/api",
             "headers": [(k.encode(), v.encode()) for k, v in h.items()]}
    inbox = [{"type": "http.request", "body": raw, "more_body": False}]
    sent: list[dict] = []

    async def receive():
        return inbox.pop(0)

    async def send(msg):
        sent.append(msg)

    asyncio.run(serve([])(scope, receive, send))
    start, body = sent
    assert start["status"] == 200
    assert json.loads(body["body"]) == {"output": 1}
