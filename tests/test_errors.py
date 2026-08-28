"""Error wrapping.

The defect these cover: before the _http.py split, only HTTPError was caught,
so `except IronflowError` did not catch a server being down, DNS failing, a TLS
error, a timeout, or a proxy returning HTML with a 200. Callers who wrote
correct error handling per the docs still got a raw urllib traceback.
"""

from __future__ import annotations

import socket

import pytest

from ironflow import IronflowClient, IronflowError
from tests.harness import Response


def _closed_port() -> int:
    """Bind and immediately release a port so connecting to it is refused."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class TestTransportErrorsAreWrapped:
    def test_connection_refused(self) -> None:
        c = IronflowClient(server_url=f"http://127.0.0.1:{_closed_port()}")
        c.initial_backoff = 0.01
        with pytest.raises(IronflowError) as exc:
            c.runs_list()
        assert exc.value.retryable is True
        assert "cannot reach" in str(exc.value)

    def test_dns_failure(self) -> None:
        c = IronflowClient(server_url="http://ironflow-nonexistent.invalid")
        c.initial_backoff = 0.01
        with pytest.raises(IronflowError) as exc:
            c.runs_list()
        assert exc.value.retryable is True

    def test_connection_refused_is_not_a_urllib_error(self) -> None:
        """The hostile-QA case: one line that used to produce a raw traceback."""
        import urllib.error

        c = IronflowClient(server_url=f"http://127.0.0.1:{_closed_port()}")
        c.initial_backoff = 0.01
        try:
            c.runs_list()
        except IronflowError:
            pass
        except urllib.error.URLError:  # pragma: no cover
            pytest.fail("URLError leaked; it must be wrapped in IronflowError")


class TestMalformedResponses:
    def test_non_json_200_is_wrapped(self, server) -> None:
        """A proxy or captive portal answering instead of the server."""
        server.script(Response(status=200, raw=b"<html>gateway</html>"))
        c = IronflowClient(server_url=server.url)
        with pytest.raises(IronflowError) as exc:
            c.runs_list()
        assert "expected JSON" in str(exc.value)
        assert exc.value.retryable is False

    def test_empty_body_returns_none(self, server) -> None:
        server.script(Response(status=200, raw=b""))
        assert IronflowClient(server_url=server.url).runs_list() is None

    def test_non_json_error_body_uses_text(self, server) -> None:
        server.script(Response(status=500, raw=b"upstream exploded"))
        c = IronflowClient(server_url=server.url, max_attempts=1)
        with pytest.raises(IronflowError) as exc:
            c.runs_list()
        assert "upstream exploded" in str(exc.value)


class TestErrorFields:
    def test_structured_error_body(self, server) -> None:
        server.script(
            Response(status=409, body={"code": "CONFLICT", "message": "version mismatch"})
        )
        with pytest.raises(IronflowError) as exc:
            IronflowClient(server_url=server.url).runs_list()
        assert exc.value.status_code == 409
        assert exc.value.code == "CONFLICT"
        assert str(exc.value) == "version mismatch"
        assert exc.value.retryable is False

    def test_retry_after_exposed_on_error(self, server) -> None:
        server.script(*[Response(status=429, headers={"Retry-After": "7"}) for _ in range(3)])
        c = IronflowClient(server_url=server.url, max_attempts=1)
        with pytest.raises(IronflowError) as exc:
            c.runs_list()
        assert exc.value.retry_after == 7.0

    def test_importable_from_both_paths(self) -> None:
        """`from ironflow.client import IronflowError` predates the split."""
        from ironflow import IronflowError as top
        from ironflow._http import IronflowError as viahttp
        from ironflow.client import IronflowError as viaclient

        assert top is viaclient is viahttp
