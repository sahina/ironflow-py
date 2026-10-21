"""File storage round trip against a real server.

Skipped without IRONFLOW_TEST_SERVER like the rest of this directory. The
event-triggered read is covered by the Go and Node live tests; here the
`ironflow.file.created` event row is asserted through the events list API.
"""

from __future__ import annotations

import hashlib
import time
import urllib.error
import urllib.request
import uuid
from typing import Any

import pytest

from ironflow import IronflowClient, IronflowError


def _wait_for(poll: Any, what: str, timeout: float = 10.0) -> Any:
    end = time.monotonic() + timeout
    while True:
        got = poll()
        if got:
            return got
        assert time.monotonic() < end, f"timed out waiting for {what}"
        time.sleep(0.2)


def test_files_round_trip(server_url: str, api_key: str) -> None:
    client = IronflowClient(server_url, api_key=api_key)
    bucket = f"live-{uuid.uuid4().hex[:10]}"
    created = client.files_buckets(body={"name": bucket, "emitEvents": True, "allowSignedUrls": True})
    assert created["emitEvents"] is True and created["allowSignedUrls"] is True
    assert bucket in [b["name"] for b in client.files_list_buckets()["buckets"]]

    content = b"hello from the live round trip"
    want = hashlib.sha256(content).hexdigest()
    info = client.files_update_buckets_objects(bucket, "in/a.txt", content, content_type="text/plain")
    assert info["sha256"] == want

    def created_event() -> Any:
        events = client.events_list(name="ironflow.file.created")["events"]
        return next((e for e in events if e["data"].get("bucket") == bucket), None)

    event = _wait_for(created_event, "ironflow.file.created event row")
    assert event["data"]["path"] == "in/a.txt"
    assert event["data"]["etag"] == info["etag"]
    assert event["data"]["sha256"] == want

    with client.files_get_buckets_objects(bucket, "in/a.txt", if_match=info["etag"]) as got:
        assert hashlib.sha256(got.read()).hexdigest() == want

    signed = client.files_buckets_signed_urls_upload(
        bucket, body={"path": "in/b.txt", "maxBytes": 1024, "contentType": "text/plain"}
    )
    print(f"signed URL: {signed['url'].split('?')[0]}")
    assert signed["url"].startswith(f"{server_url}/api/v1/files/signed?token=")
    # No Authorization header: the token alone grants the write.
    req = urllib.request.Request(
        signed["url"], data=b"signed body", method="PUT", headers={"Content-Type": "text/plain"}
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        assert resp.status == 201
    assert client.files_get_buckets_info(bucket, "in/b.txt")["size"] == len(b"signed body")

    moved = client.files_buckets_move(bucket, body={"from": "in/b.txt", "to": "in/c.txt"})
    assert moved["path"] == "in/c.txt"
    assert client.files_buckets_move_prefix(bucket, body={"from": "in/", "to": "archive/"})["count"] == 2

    client.files_delete_buckets_objects(bucket, "archive/a.txt")
    client.files_delete_buckets_objects(bucket, "archive/c.txt")
    client.files_delete_buckets(bucket)


def _status(call: Any) -> int:
    with pytest.raises(IronflowError) as exc:
        call()
    return int(exc.value.status_code)


def test_files_routes(server_url: str, api_key: str) -> None:
    """files-storage (#3003): the bucket and object routes the round trip does not touch."""
    client = IronflowClient(server_url, api_key=api_key)
    bucket = f"live-routes-py-{uuid.uuid4().hex[:10]}"

    created = client.files_buckets(body={"name": bucket, "emitEvents": True, "allowSignedUrls": True})
    assert created["name"] == bucket and created["emitEvents"] and created["allowSignedUrls"]
    assert created["maxObjectBytes"] > 0 and created["createdAt"]
    assert bucket in [b["name"] for b in client.files_list_buckets()["buckets"]]
    assert client.files_get_buckets(bucket)["allowSignedUrls"] is True
    updated = client.files_patch_buckets(bucket, body={"allowedContentTypes": ["text/*"]})
    assert updated["allowedContentTypes"] == ["text/*"] and updated["emitEvents"] is True
    assert _status(lambda: client.files_update_buckets_objects(bucket, "blocked.pdf", b"x", content_type="application/pdf")) == 415

    content = b"hello from python"
    info = client.files_update_buckets_objects(
        bucket, "docs/a.txt", content, content_type="text/plain", extra_headers={"X-Ironflow-Meta-source": "python"}
    )
    assert (info["bucket"], info["path"], info["size"], info["contentType"]) == (bucket, "docs/a.txt", len(content), "text/plain")
    assert info["etag"] and len(info["sha256"]) == 64 and info["metadata"]["source"] == "python"
    assert _status(lambda: client.files_update_buckets_objects(bucket, "docs/a.txt", b"x", content_type="text/plain", if_none_match="*")) == 412
    assert _status(lambda: client.files_update_buckets_objects(bucket, "docs/a.txt", b"x", content_type="text/plain", if_match="stale")) == 412
    rewritten = client.files_update_buckets_objects(bucket, "docs/a.txt", content, content_type="text/plain", if_match=info["etag"])
    assert rewritten["etag"] != info["etag"]

    with client.files_get_buckets_objects(bucket, "docs/a.txt", if_match=rewritten["etag"]) as got:
        assert got.read() == content
    assert _status(lambda: client.files_get_buckets_objects(bucket, "docs/a.txt", if_match=info["etag"])) == 412
    with client.files_get_buckets_objects(bucket, "docs/a.txt", range="bytes=0-4") as ranged:
        assert ranged.read() == b"hello"
    stat = client.files_get_buckets_info(bucket, "docs/a.txt")
    assert (stat["etag"], stat["sha256"]) == (rewritten["etag"], info["sha256"])
    assert _status(lambda: client.files_get_buckets_info(bucket, "docs/missing.txt")) == 404

    client.files_update_buckets_objects(bucket, "top.txt", b"t", content_type="text/plain")
    client.files_update_buckets_objects(bucket, "docs/b.txt", b"b", content_type="text/plain")
    by_prefix = client.files_list_buckets_objects(bucket, prefix="docs/")
    assert sorted(f["path"] for f in by_prefix["files"]) == ["docs/a.txt", "docs/b.txt"]
    level = client.files_list_buckets_objects(bucket, delimiter="/")
    assert [f["path"] for f in level["files"]] == ["top.txt"] and level["prefixes"] == ["docs/"]
    page1 = client.files_list_buckets_objects(bucket, prefix="docs/", limit=1)
    assert len(page1["files"]) == 1 and page1["nextCursor"]
    page2 = client.files_list_buckets_objects(bucket, prefix="docs/", limit=1, cursor=page1["nextCursor"])
    assert sorted([page1["files"][0]["path"], page2["files"][0]["path"]]) == ["docs/a.txt", "docs/b.txt"]

    copied = client.files_buckets_copy(bucket, body={"from": "docs/a.txt", "to": "docs/copy.txt"})
    assert (copied["path"], copied["sha256"], copied["size"]) == ("docs/copy.txt", info["sha256"], len(content))
    assert client.files_get_buckets_info(bucket, "docs/a.txt")["path"] == "docs/a.txt"
    assert _status(lambda: client.files_buckets_move(bucket, body={"from": "docs/copy.txt", "to": "docs/moved.txt"}, if_match="stale")) == 412
    moved = client.files_buckets_move(bucket, body={"from": "docs/copy.txt", "to": "docs/moved.txt"}, if_match=copied["etag"])
    assert (moved["path"], moved["sha256"]) == ("docs/moved.txt", info["sha256"])
    assert _status(lambda: client.files_get_buckets_info(bucket, "docs/copy.txt")) == 404

    # A signed download carries its own credential.
    down = client.files_buckets_signed_urls_download(bucket, body={"path": "docs/a.txt", "ttlSeconds": 60})
    assert down["expiresAt"]
    with urllib.request.urlopen(down["url"], timeout=10) as resp:
        assert resp.status == 200 and resp.read() == content

    # A create-only upload URL writes a free path once and refuses to overwrite it.
    once = client.files_buckets_signed_urls_upload(
        bucket, body={"path": "once.txt", "contentType": "text/plain", "createOnly": True}
    )

    def put_once() -> int:
        req = urllib.request.Request(once["url"], data=b"first", method="PUT", headers={"Content-Type": "text/plain"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return int(resp.status)
        except urllib.error.HTTPError as e:
            return int(e.code)

    assert (put_once(), put_once()) == (201, 412)

    # A bucket with files will not delete.
    assert _status(lambda: client.files_delete_buckets(bucket)) == 409
    for path in ("docs/a.txt", "docs/b.txt", "docs/moved.txt", "top.txt", "once.txt"):
        client.files_delete_buckets_objects(bucket, path)
    client.files_delete_buckets(bucket)
    assert _status(lambda: client.files_get_buckets(bucket)) == 404
