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
            c.events_list()
        assert exc.value.retryable is True
        assert "cannot reach" in str(exc.value)

    def test_dns_failure(self) -> None:
        c = IronflowClient(server_url="http://ironflow-nonexistent.invalid")
        c.initial_backoff = 0.01
        with pytest.raises(IronflowError) as exc:
            c.events_list()
        assert exc.value.retryable is True

    def test_connection_refused_is_not_a_urllib_error(self) -> None:
        """The hostile-QA case: one line that used to produce a raw traceback."""
        import urllib.error

        c = IronflowClient(server_url=f"http://127.0.0.1:{_closed_port()}")
        c.initial_backoff = 0.01
        try:
            c.events_list()
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
            c.events_list()
        assert "expected JSON" in str(exc.value)
        assert exc.value.retryable is False

    def test_empty_body_returns_none(self, server) -> None:
        server.script(Response(status=200, raw=b""))
        assert IronflowClient(server_url=server.url).events_list() is None

    def test_non_json_error_body_uses_text(self, server) -> None:
        server.script(Response(status=500, raw=b"upstream exploded"))
        c = IronflowClient(server_url=server.url, max_attempts=1)
        with pytest.raises(IronflowError) as exc:
            c.events_list()
        assert "upstream exploded" in str(exc.value)


class TestErrorFields:
    def test_structured_error_body(self, server) -> None:
        server.script(
            Response(status=409, body={"code": "already_exists", "message": "version mismatch"})
        )
        with pytest.raises(IronflowError) as exc:
            IronflowClient(server_url=server.url).events_list()
        assert exc.value.status_code == 409
        assert exc.value.code == "already_exists"
        assert str(exc.value) == "version mismatch"
        assert exc.value.retryable is False

    def test_error_field_body_keeps_server_message(self, server) -> None:
        """Most REST handlers answer {"error": "..."} with no message field.

        Reading only "message" turned "run not found" into "HTTP 404".
        """
        server.script(Response(status=404, body={"error": "run not found"}))
        with pytest.raises(IronflowError) as exc:
            IronflowClient(server_url=server.url).events_list()
        assert str(exc.value) == "run not found"
        assert exc.value.code == "not_found"  # a body with no code gets the Connect code of its status
        assert exc.value.status_code == 404

    def test_message_wins_over_error_field(self, server) -> None:
        server.script(Response(status=400, body={"message": "bad limit", "error": "invalid"}))
        with pytest.raises(IronflowError) as exc:
            IronflowClient(server_url=server.url).events_list()
        assert str(exc.value) == "bad limit"

    def test_retry_after_exposed_on_error(self, server) -> None:
        server.script(*[Response(status=429, headers={"Retry-After": "7"}) for _ in range(3)])
        c = IronflowClient(server_url=server.url, max_attempts=1)
        with pytest.raises(IronflowError) as exc:
            c.events_list()
        assert exc.value.retry_after == 7.0

    def test_importable_from_both_paths(self) -> None:
        """`from ironflow.client import IronflowError` predates the split."""
        from ironflow import IronflowError as top
        from ironflow._http import IronflowError as viahttp
        from ironflow.client import IronflowError as viaclient

        assert top is viaclient is viahttp


class TestConnectCodeContract:
    """One condition, one code on every transport (#3054): the lowercase Connect code."""

    def test_rest_not_found_answers_not_found(self, server) -> None:
        server.script(Response(status=404, body={"code": "not_found", "message": "gone"}))
        with pytest.raises(IronflowError) as exc:
            IronflowClient(server_url=server.url).events_list()
        assert exc.value.code == "not_found"

    @pytest.mark.parametrize(
        ("status", "code"),
        [
            (400, "invalid_argument"), (401, "unauthenticated"), (403, "permission_denied"),
            (404, "not_found"), (409, "already_exists"), (412, "failed_precondition"),
            (402, "failed_precondition"), (408, "deadline_exceeded"), (429, "resource_exhausted"), (500, "internal"),
            (501, "unimplemented"), (503, "unavailable"), (504, "deadline_exceeded"), (507, "failed_precondition"),
        ],
    )
    def test_code_less_body_gets_the_connect_code_of_its_status(self, server, status, code) -> None:
        server.script(Response(status=status, body={"error": "nope"}))
        with pytest.raises(IronflowError) as exc:
            IronflowClient(server_url=server.url, max_attempts=1).events_list()
        assert exc.value.code == code

    def test_capitalised_generic_code_from_an_older_server_is_normalised(self, server) -> None:
        server.script(Response(status=404, body={"code": "NOT_FOUND", "message": "gone"}))
        with pytest.raises(IronflowError) as exc:
            IronflowClient(server_url=server.url).events_list()
        assert exc.value.code == "not_found"
        server.script(Response(status=403, body={"code": "SIGNED_URLS_DISABLED", "message": "off"}))
        with pytest.raises(IronflowError) as exc:
            IronflowClient(server_url=server.url).events_list()
        assert exc.value.code == "SIGNED_URLS_DISABLED"  # a domain code keeps its name


class TestWorkerReplyErrorCode:
    """Reply.error_code feeds IronflowError.code on the pull-worker and serve paths."""

    @pytest.mark.parametrize(
        ("status", "body", "want"),
        [
            (404, {"error": "worker not registered", "message": "x", "code": "not_found"}, "not_found"),
            (404, {"error": "worker not registered"}, "not_found"),
            (409, {"error": "STALE_EXECUTION", "message": "superseded"}, "STALE_EXECUTION"),
            (403, {"error": "forbidden"}, "permission_denied"),  # a message, not a fence code
            (503, {"code": "unavailable", "message": "x"}, "unavailable"),
        ],
    )
    def test_error_code(self, status, body, want) -> None:
        from ironflow.worker._transport import Reply

        assert Reply(status=status, body=body).error_code == want


def test_worker_auth_error_carries_the_connect_code() -> None:
    from ironflow.worker import WorkerAuthError

    assert WorkerAuthError("x", status_code=401).code == "unauthenticated"
    assert WorkerAuthError("x", status_code=403).code == "permission_denied"


def test_a_refused_redirect_has_no_connect_code(server) -> None:
    server.script(Response(status=302, body={}, headers={"Location": "http://elsewhere.invalid/"}))
    with pytest.raises(IronflowError) as exc:
        IronflowClient(server_url=server.url, max_attempts=1).events_list()
    assert exc.value.code == ""


def test_a_fence_code_wins_over_a_code_a_shared_writer_adds() -> None:
    from ironflow.worker._transport import Reply

    reply = Reply(status=409, body={"error": "STALE_EXECUTION", "message": "x", "code": "already_exists"})
    assert reply.error_code == "STALE_EXECUTION"
