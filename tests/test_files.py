import io

import pytest

from ironflow import IronflowClient
from ironflow._http import (
    BinaryResponse,
    IronflowError,
    PayloadTooLargeError,
    PreconditionFailedError,
    UnsupportedMediaTypeError,
)
from tests.harness import Response


def client(server, **kw):
    return IronflowClient(server_url=server.url, initial_backoff=0.01, max_backoff=0.05, **kw)


def test_put_sends_raw_bytes_and_keeps_slashes(server):
    server.script(Response(201, {"bucket": "docs", "path": "a/b.txt", "etag": "e1", "size": 2}))
    c = client(server)
    c.files_update_buckets_objects("docs", "a/b.txt", b"hi", content_type="text/plain", extra_headers={"X-Ironflow-Meta-Src": "t"})
    req = server.requests[-1]
    assert req["path"] == "/api/v1/files/buckets/docs/objects/a/b.txt"
    assert req["raw"] == b"hi"
    # urllib capitalizes only the first letter ("Content-type"); compare lowercased.
    headers = {k.lower(): v for k, v in req["headers"].items()}
    assert headers["content-type"] == "text/plain"
    assert headers["x-ironflow-meta-src"] == "t"


def test_put_retries_replayable_body(server):
    server.script(Response(503, {"code": "INTERNAL", "message": "x"}), Response(201, {"etag": "e"}))
    client(server).files_update_buckets_objects("docs", "a", io.BytesIO(b"abc"), content_type="text/plain")
    assert [r["raw"] for r in server.requests] == [b"abc", b"abc"]


def test_put_rejects_unseekable_stream(server):
    class Stream(io.RawIOBase):
        def readable(self):
            return True

        def seekable(self):
            return False

        def readinto(self, b):
            return 0

    with pytest.raises(ValueError, match="seekable"):
        client(server).files_update_buckets_objects("docs", "a", Stream(), content_type="text/plain")
    assert len(server.requests) == 0


@pytest.mark.parametrize("status,exc", [(412, PreconditionFailedError), (413, PayloadTooLargeError), (415, UnsupportedMediaTypeError)])
def test_typed_errors(server, status, exc):
    server.script(Response(status, {"code": "X", "message": "m", "error": "m"}))
    with pytest.raises(exc) as ei:
        client(server).files_update_buckets_objects("docs", "a", b"x", content_type="text/plain")
    assert ei.value.status_code == status


def test_get_streams(server):
    server.script(Response(200, raw=b"0123456789", headers={"Content-Type": "application/octet-stream", "ETag": '"e1"'}))
    with client(server).files_get_buckets_objects("docs", "a/b.bin") as resp:
        assert isinstance(resp, BinaryResponse)
        assert b"".join(resp.iter_bytes(4)) == b"0123456789"
        assert resp.headers["ETag"] == '"e1"'


def test_put_file_sends_from_current_offset_and_omits_unset_headers(server):
    server.script(Response(201, {"etag": "e"}))
    f = io.BytesIO(b"xxhello")
    f.seek(2)
    client(server).files_update_buckets_objects("docs", "a", f, content_type="text/plain")
    req = server.requests[-1]
    assert req["raw"] == b"hello"
    headers = {k.lower() for k in req["headers"]}
    assert "if-match" not in headers and "if-none-match" not in headers


def test_get_raises_on_truncated_body():
    # The mock server cannot send a short body under a longer Content-Length,
    # so drive BinaryResponse with the stdlib response shape directly.
    class Short:
        status = 200

        def __init__(self):
            self.headers = {"Content-Length": "10"}
            self.chunks = [b"0123", b""]

        def read(self, amt=None):
            return self.chunks.pop(0)

        def close(self):
            pass

    with pytest.raises(IronflowError, match="truncated"):
        BinaryResponse(Short()).read()


# The URL library resolves dot segments, so "../../b/objects/x" would reach another bucket.
@pytest.mark.parametrize("bad", ["../../b/objects/x", "a/./b", "a/..", ".."])
def test_dot_segment_paths_are_rejected_before_sending(server, bad):
    c = client(server)
    with pytest.raises(ValueError, match="dot segment"):
        c.files_delete_buckets_objects("a", bad)
    with pytest.raises(ValueError, match="dot segment"):
        c.files_get_buckets_info("a", bad)
    with pytest.raises(ValueError, match="dot segment"):
        c.files_update_buckets_objects("a", bad, b"x", content_type="text/plain")
    assert server.requests == []


# A bucket name is one segment: "" leaves an empty segment and the URL library
# resolves "." and "..", so none of them may reach the server.
@pytest.mark.parametrize("bad", ["", ".", ".."])
def test_bad_bucket_names_are_rejected_before_sending(server, bad):
    c = client(server)
    with pytest.raises(ValueError, match="segment"):
        c.files_get_buckets(bad)
    with pytest.raises(ValueError, match="segment"):
        c.files_get_buckets_info(bad, "x")
    with pytest.raises(ValueError, match="segment"):
        c.files_buckets_signed_urls_upload(bad)
    assert server.requests == []


def test_sign_calls_are_retried_but_move_is_not(server):
    signed = {"url": "u", "expiresAt": "t"}
    server.script(
        Response(503, {"code": "INTERNAL", "message": "x"}),
        Response(200, signed),
        Response(503, {"code": "INTERNAL", "message": "x"}),
        Response(200, signed),
        Response(503, {"code": "INTERNAL", "message": "x"}),
    )
    c = client(server)
    assert c.files_buckets_signed_urls_upload("b", {"path": "x"}) == signed
    assert c.files_buckets_signed_urls_download("b", {"path": "x"}) == signed
    with pytest.raises(IronflowError):
        c.files_buckets_move("b", {"from": "a", "to": "c"})
    assert len(server.requests) == 5
