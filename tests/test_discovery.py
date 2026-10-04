from __future__ import annotations

import json
from pathlib import Path

import pytest

from ironflow import _discovery
from ironflow._discovery import discover

DOC = {"version": 1, "url": "http://127.0.0.1:52431", "api_key": "ifkey_x", "environment": "dev"}


def files(**by_dir: object):
    """read_file over an in-memory map of {dir: doc-or-raw-bytes}."""
    def read(p: Path) -> bytes | None:
        v = by_dir.get(str(p.parent.parent))
        if v is None:
            return None
        return v if isinstance(v, bytes) else json.dumps(v).encode()
    return read


CWD = Path("/a/b/c")


def run(doc: object, env: dict[str, str] | None = None, at: str = "/a/b/c") -> dict[str, str]:
    return discover(env or {}, CWD, files(**{at: doc}))


def test_fills_all_from_nearest_parent():
    assert run(DOC, at="/a") == {
        "IRONFLOW_URL": DOC["url"], "IRONFLOW_SERVER_URL": DOC["url"],
        "IRONFLOW_API_KEY": "ifkey_x", "IRONFLOW_ENV": "dev",
    }


def test_nearest_parent_wins_even_when_invalid():
    other = {**DOC, "url": "http://localhost:1"}
    read = files(**{"/a": other, "/a/b": DOC})
    assert discover({}, CWD, read)["IRONFLOW_URL"] == DOC["url"]
    read = files(**{"/a": other, "/a/b": {**DOC, "version": 2}})
    assert discover({}, CWD, read) == {}


@pytest.mark.parametrize("name", ["IRONFLOW_URL", "IRONFLOW_SERVER_URL", "IRONFLOW_API_KEY"])
def test_any_set_var_is_a_noop(name):
    assert run(DOC, {name: "x"}) == {}


def test_empty_var_counts_as_unset():
    assert run(DOC, {"IRONFLOW_URL": ""})["IRONFLOW_URL"] == DOC["url"]


def test_opt_out():
    assert run(DOC, {"IRONFLOW_NO_DISCOVERY": "1"}) == {}


@pytest.mark.parametrize("url", [
    "http://example.com:9123", "https://127.0.0.1:9123", "http://10.0.0.1", "http://127.0.0.1.evil.com",
    "http://user@evil.com", "ftp://localhost", "localhost:9123", "", None, 5, "http://localhost:99999",
])
def test_bad_url_ignored(url):
    assert run({**DOC, "url": url}) == {}


@pytest.mark.parametrize("url", ["http://localhost:1", "http://127.9.9.9:1", "http://[::1]:1", "http://LOCALHOST"])
def test_loopback_urls_accepted(url):
    assert run({**DOC, "url": url})["IRONFLOW_URL"] == url


@pytest.mark.parametrize("doc", [
    {**DOC, "version": 2}, {"url": DOC["url"]}, [DOC], b"{not json", b"", b"null",
    json.dumps({**DOC, "pad": "x" * 70_000}).encode(),
])
def test_invalid_file_ignored(doc):
    assert run(doc) == {}


def test_api_key_and_environment_optional():
    assert run({"version": 1, "url": DOC["url"]}) == {
        "IRONFLOW_URL": DOC["url"], "IRONFLOW_SERVER_URL": DOC["url"]}


def test_environment_only_fills_when_unset():
    assert "IRONFLOW_ENV" not in run(DOC, {"IRONFLOW_ENV": "prod"})
    assert run(DOC, {"IRONFLOW_ENV": ""})["IRONFLOW_ENV"] == "dev"


@pytest.fixture()
def fresh(monkeypatch, tmp_path):
    for n in ("IRONFLOW_URL", "IRONFLOW_SERVER_URL", "IRONFLOW_API_KEY", "IRONFLOW_ENV", "IRONFLOW_NO_DISCOVERY"):
        monkeypatch.setenv(n, "")  # records the original so teardown undoes what hydrate sets
        monkeypatch.delenv(n)
    monkeypatch.setattr(_discovery, "_done", False)
    (tmp_path / ".ironflow").mkdir()
    (tmp_path / ".ironflow" / "engine.json").write_text(json.dumps(DOC))
    sub = tmp_path / "x" / "y"
    sub.mkdir(parents=True)
    monkeypatch.chdir(sub)


def test_hydrate_sets_only_unset_and_runs_once(fresh, monkeypatch):
    import os
    monkeypatch.setenv("IRONFLOW_ENV", "prod")
    _discovery.hydrate_env_from_discovery()
    assert os.environ["IRONFLOW_URL"] == DOC["url"]
    assert os.environ["IRONFLOW_ENV"] == "prod"
    monkeypatch.setenv("IRONFLOW_URL", "http://elsewhere")
    _discovery.hydrate_env_from_discovery()
    assert os.environ["IRONFLOW_URL"] == "http://elsewhere"


def test_worker_picks_up_discovery_file(fresh):
    from ironflow.worker import Worker, function

    @function(id="f", triggers=[{"event": "e"}])
    async def f(ctx):
        return None

    w = Worker(functions=[f])
    assert w._transport._base == DOC["url"]
    assert w._transport._api_key == "ifkey_x"
    assert w._transport._environment == "dev"
