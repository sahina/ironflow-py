"""Engine discovery file (ADR 0098): fill unset IRONFLOW_* variables from ``.ironflow/engine.json``."""
from __future__ import annotations

import ipaddress
import json
import os
from collections.abc import Callable, Mapping
from pathlib import Path
from urllib.parse import urlparse

_MAX_BYTES = 64 * 1024
_URL_VARS = ("IRONFLOW_URL", "IRONFLOW_SERVER_URL", "IRONFLOW_API_KEY")


def _read(path: Path) -> bytes | None:
    try:
        with open(path, "rb") as f:
            return f.read(_MAX_BYTES + 1)
    except OSError:
        return None


def _loopback_http(url: object) -> bool:
    if not isinstance(url, str):
        return False
    try:
        u = urlparse(url)
        _ = u.port  # raises ValueError on a malformed port
        host = u.hostname
    except ValueError:
        return False
    if u.scheme != "http" or not host:
        return False
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def discover(
    env: Mapping[str, str], cwd: Path, read_file: Callable[[Path], bytes | None] = _read,
) -> dict[str, str]:
    """The variables to set, or ``{}``. Pure: reads files only through ``read_file``."""
    if env.get("IRONFLOW_NO_DISCOVERY") == "1" or any(env.get(k) for k in _URL_VARS):
        return {}
    for d in (cwd, *cwd.parents):
        raw = read_file(d / ".ironflow" / "engine.json")
        if raw is None:
            continue
        # The nearest file wins even when it is unusable.
        if len(raw) > _MAX_BYTES:
            return {}
        try:
            doc = json.loads(raw)
        except (ValueError, RecursionError):
            return {}
        if not isinstance(doc, dict) or doc.get("version") != 1 or not _loopback_http(doc.get("url")):
            return {}
        out = {"IRONFLOW_URL": doc["url"], "IRONFLOW_SERVER_URL": doc["url"]}
        key, environment = doc.get("api_key"), doc.get("environment")
        if isinstance(key, str) and key:
            out["IRONFLOW_API_KEY"] = key
        if isinstance(environment, str) and environment and not env.get("IRONFLOW_ENV"):
            out["IRONFLOW_ENV"] = environment
        return out
    return {}


_done = False


def hydrate_env_from_discovery() -> None:
    """Run once per process, before the first read of IRONFLOW_URL / _SERVER_URL / _API_KEY. Never at import."""
    global _done
    if _done:
        return
    _done = True
    try:
        found = discover(os.environ, Path.cwd())
    except OSError:  # cwd deleted
        return
    for k, v in found.items():
        if not os.environ.get(k):
            os.environ[k] = v
