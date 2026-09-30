"""Command dedup helper on the REST client (#2412)."""

from __future__ import annotations

import base64
import json
import threading
import time
from typing import Any

import pytest

import ironflow
from ironflow import CommandDedup, IronflowClient, IronflowError
from tests.harness import Response

BUCKET = "order-commands"
KEY_PATH = f"/api/v1/kv/buckets/{BUCKET}/keys/"


def client(server: Any) -> IronflowClient:
    return IronflowClient(server_url=server.url, initial_backoff=0.01, max_backoff=0.05)


def entry(value: Any) -> Response:
    raw = value if isinstance(value, bytes) else json.dumps(value).encode()
    return Response(body={"key": "k", "operation": "put", "revision": 1, "created_at": "", "value": base64.b64encode(raw).decode()})


def paths(server: Any) -> list[tuple[str, str]]:
    return [(r["method"], r["path"]) for r in server.requests]


def test_default_ttl_is_seven_days() -> None:
    assert ironflow.DEFAULT_COMMAND_DEDUP_TTL_SECONDS == 604800


def test_client_builds_a_helper(server: Any) -> None:
    assert isinstance(client(server).command_dedup(BUCKET), CommandDedup)


def test_winner_gets_none_and_creates_the_bucket_once(server: Any) -> None:
    dedup = client(server).command_dedup(BUCKET)
    assert dedup.try_claim("c1", {"orderId": "o1"}) is None
    assert dedup.try_claim("c2", {"orderId": "o2"}) is None
    assert paths(server) == [
        ("POST", "/api/v1/kv/buckets"),
        ("PUT", KEY_PATH + "c1"),
        ("PUT", KEY_PATH + "c2"),
    ]
    assert server.requests[0]["body"] == {"name": BUCKET, "ttl_seconds": 604800}
    put = server.requests[1]
    assert put["headers"]["If-None-Match"] == "*" and put["body"] == {"orderId": "o1"}


def test_zero_ttl_means_no_expiry(server: Any) -> None:
    client(server).command_dedup(BUCKET, ttl_seconds=0).try_claim("c1", {})
    assert server.requests[0]["body"] == {"name": BUCKET}


def test_loser_gets_the_prior_entry(server: Any) -> None:
    server.script(Response(status=200), Response(status=412), entry({"orderId": "o1", "status": "done"}))
    assert client(server).command_dedup(BUCKET).try_claim("c1", {"orderId": "o1"}) == {"orderId": "o1", "status": "done"}
    assert paths(server)[-1] == ("GET", KEY_PATH + "c1")


def test_loser_racing_a_delete_claims_again(server: Any) -> None:
    # 412 then the winner releases (GET 404): the second create-only write wins.
    server.script(Response(status=200), Response(status=412), Response(status=404), Response(status=200))
    assert client(server).command_dedup(BUCKET).try_claim("c1", {}) is None
    assert [m for m, _ in paths(server)] == ["POST", "PUT", "GET", "PUT"]


def test_a_claim_that_keeps_losing_the_race_raises(server: Any) -> None:
    lost = [Response(status=412), Response(status=404)] * 3
    server.script(Response(status=200), *lost)
    with pytest.raises(IronflowError) as e:
        client(server).command_dedup(BUCKET).try_claim("c1", {})
    assert e.value.code == "COMMAND_DEDUP_RACE"
    assert "c1" in str(e.value) and "3" in str(e.value)
    assert [m for m, _ in paths(server)].count("PUT") == 3


def test_a_read_back_error_other_than_404_does_not_loop(server: Any) -> None:
    server.script(Response(status=200), Response(status=412), Response(status=403))
    with pytest.raises(IronflowError) as e:
        client(server).command_dedup(BUCKET).try_claim("c1", {})
    assert e.value.status_code == 403
    assert [m for m, _ in paths(server)].count("PUT") == 1


def owns(prior: Any) -> bool:
    return prior.get("token") == "t1" and prior.get("status") == "claimed"


def test_is_owner_reclaims_our_orphaned_claim(server: Any) -> None:
    server.script(Response(status=200), Response(status=412), entry({"token": "t1", "status": "claimed"}))
    claim = {"token": "t1", "status": "claimed"}
    assert client(server).command_dedup(BUCKET).try_claim("c1", claim, is_owner=owns) is None


def test_is_owner_false_returns_the_prior(server: Any) -> None:
    prior = {"token": "t2", "status": "claimed"}
    server.script(Response(status=200), Response(status=412), entry(prior))
    assert client(server).command_dedup(BUCKET).try_claim("c1", {"token": "t1"}, is_owner=owns) == prior


def test_is_owner_false_for_a_finalized_result_returns_it(server: Any) -> None:
    done = {"token": "t1", "status": "done"}  # same token, but finalized: must not replay
    server.script(Response(status=200), Response(status=412), entry(done))
    assert client(server).command_dedup(BUCKET).try_claim("c1", {"token": "t1"}, is_owner=owns) == done


def test_is_owner_is_not_called_when_the_write_wins(server: Any) -> None:
    def boom(_: Any) -> bool:
        raise AssertionError("is_owner must not run on the winner path")

    assert client(server).command_dedup(BUCKET).try_claim("c1", {}, is_owner=boom) is None


def test_a_deleted_bucket_is_recreated_for_the_claim_write(server: Any) -> None:
    server.script(Response(status=200), Response(status=404), Response(status=200), Response(status=200))
    assert client(server).command_dedup(BUCKET).try_claim("c1", {}) is None
    assert [m for m, _ in paths(server)] == ["POST", "PUT", "POST", "PUT"]


def test_a_deleted_bucket_is_recreated_for_finalize(server: Any) -> None:
    server.script(Response(status=200), Response(status=404), Response(status=200), Response(status=200))
    client(server).command_dedup(BUCKET).finalize("c1", {"status": "done"})
    assert [m for m, _ in paths(server)] == ["POST", "PUT", "POST", "PUT"]


def test_a_bucket_that_stays_missing_raises(server: Any) -> None:
    server.script(Response(status=200), Response(status=404), Response(status=200), Response(status=404))
    with pytest.raises(IronflowError) as e:
        client(server).command_dedup(BUCKET).try_claim("c1", {})
    assert e.value.status_code == 404
    assert [m for m, _ in paths(server)] == ["POST", "PUT", "POST", "PUT"]


def test_ensure_bucket_creates_once_under_concurrent_first_use() -> None:
    calls: list[Any] = []

    class SlowKV:
        def kv_buckets(self, body: Any = None) -> None:
            calls.append(body)
            time.sleep(0.05)  # widen the window so an unlocked check-then-act would double-create

    dedup = ironflow.CommandDedup(SlowKV(), BUCKET)  # type: ignore[arg-type]
    threads = [threading.Thread(target=dedup._ensure_bucket) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(calls) == 1


def test_a_corrupt_prior_entry_raises(server: Any) -> None:
    server.script(Response(status=200), Response(status=412), entry(b"not json"))
    with pytest.raises(ValueError):
        client(server).command_dedup(BUCKET).try_claim("c1", {})


def test_other_claim_errors_propagate(server: Any) -> None:
    server.script(Response(status=200), Response(status=403))
    with pytest.raises(IronflowError) as e:
        client(server).command_dedup(BUCKET).try_claim("c1", {})
    assert e.value.status_code == 403


def test_the_claim_write_is_never_retried(server: Any) -> None:
    # A retried claim could hit 412 on its own committed write and read back its own claim as "prior".
    claim = {"orderId": "o1"}
    server.script(Response(status=200), Response(status=503), Response(status=412), entry(claim))
    with pytest.raises(IronflowError) as e:
        client(server).command_dedup(BUCKET).try_claim("c1", claim)
    assert e.value.status_code == 503
    assert [m for m, _ in paths(server)].count("PUT") == 1


def test_command_ids_are_percent_encoded_once(server: Any) -> None:
    client(server).command_dedup(BUCKET).try_claim("a/b c", {})
    assert server.requests[1]["path"] == KEY_PATH + "a%2Fb%20c"


def test_an_existing_bucket_is_fine(server: Any) -> None:
    server.script(Response(status=409))
    assert client(server).command_dedup(BUCKET).try_claim("c1", {}) is None


def test_a_bucket_failure_is_retried_on_the_next_call(server: Any) -> None:
    server.script(Response(status=400), Response(status=200), Response(status=200))
    dedup = client(server).command_dedup(BUCKET)
    with pytest.raises(IronflowError):
        dedup.try_claim("c1", {})
    assert dedup.try_claim("c1", {}) is None
    assert [m for m, _ in paths(server)] == ["POST", "POST", "PUT"]


def test_finalize_overwrites_without_a_precondition(server: Any) -> None:
    client(server).command_dedup(BUCKET).finalize("c1", {"status": "done"})
    put = server.requests[-1]
    assert (put["method"], put["path"], put["body"]) == ("PUT", KEY_PATH + "c1", {"status": "done"})
    assert "If-None-Match" not in put["headers"]


def test_release_deletes_and_swallows_404(server: Any) -> None:
    server.script(Response(status=200), Response(status=404))
    client(server).command_dedup(BUCKET).release("c1")
    assert paths(server)[-1] == ("DELETE", KEY_PATH + "c1")


def test_release_propagates_other_errors(server: Any) -> None:
    server.script(Response(status=200), Response(status=403))
    with pytest.raises(IronflowError):
        client(server).command_dedup(BUCKET).release("c1")
