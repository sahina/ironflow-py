"""File storage round trip against a real server.

Skipped without IRONFLOW_TEST_SERVER like the rest of this directory. The
event-triggered read is covered by the Go and Node live tests; here the
`ironflow.file.created` event row is asserted through the events list API.
"""

from __future__ import annotations

import hashlib
import time
import urllib.request
import uuid
from typing import Any

from ironflow import IronflowClient


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
