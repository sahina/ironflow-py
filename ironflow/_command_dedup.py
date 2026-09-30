"""Atomic command-level idempotency backed by KV (#2412).

Mirrors ``CommandDedup`` in the Go SDK and ``client.commandDedup()`` in Node.
"""

from __future__ import annotations

import base64
import json
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, cast
from urllib.parse import quote

if TYPE_CHECKING:  # pragma: no cover
    from . import models

T = TypeVar("T")

#: Default TTL for command dedup entries: 7 days. Pass 0 for no expiry.
DEFAULT_COMMAND_DEDUP_TTL_SECONDS = 604800

# Bound on create-write / read-back rounds when the winner keeps releasing between them.
_CLAIM_ATTEMPTS = 3


class _KV(Protocol):
    def kv_buckets(self, body: models.CreateBucketRequest | None = None) -> Any: ...
    def kv_get_buckets_keys(self, bucket: str, key: str) -> models.KVEntry: ...
    def kv_delete_buckets_keys(self, bucket: str, key: str, *, purge: bool | None = None) -> Any: ...
    def request(
        self, method: str, path: str, params: Any = None, headers: Any = None, body: Any = None, retry: bool | None = None,
    ) -> Any: ...


class CommandDedup(Generic[T]):
    """Claim-first command idempotency.

    ``try_claim`` reserves the command id before any handler work. The winner
    gets ``None`` and proceeds; a loser gets the prior entry without re-running
    the handler::

        dedup = client.command_dedup("order-commands")
        prior = dedup.try_claim(command_id, {"orderId": order_id})
        if prior is not None:
            return prior  # duplicate: return the cached result
        try:
            result = run_handler()
            dedup.finalize(command_id, result)
            return result
        except Exception:
            dedup.release(command_id)
            raise

    The returned entry is the winner's initial claim until it calls
    ``finalize``, so give the value type optional fields for data that only
    exists after the handler runs.

    Call ``release`` only on failure, before ``finalize`` succeeds. Releasing
    after ``finalize`` deletes the result and lets the command replay.

    If ``try_claim`` raises, the claim state is unknown: the write may have committed.
    The helper does not retry it on a transport error. To let a retry recognize its own
    orphaned claim, put a token that stays the same across retries in the claim and pass
    ``is_owner``::

        prior = dedup.try_claim(command_id, {"token": token, "status": "claimed"},
                                is_owner=lambda p: p.get("token") == token and p.get("status") == "claimed")

    ``is_owner`` must be false for a finalized result (keep the token out of the result, or
    check a status field), or a finished command replays. Only one retry lineage may run at
    a time: two concurrent callers sharing a token both win.

    Create one instance and reuse it; do not build one per request.
    """

    def __init__(self, kv: _KV, bucket_name: str, ttl_seconds: int = DEFAULT_COMMAND_DEDUP_TTL_SECONDS) -> None:
        self._kv = kv
        self._bucket = bucket_name
        self._ttl_seconds = ttl_seconds
        self._ready = False
        self._lock = threading.Lock()

    def _ensure_bucket(self) -> None:
        from ._http import IronflowError

        with self._lock:
            if self._ready:
                return
            body: Any = {"name": self._bucket}
            if self._ttl_seconds > 0:
                body["ttl_seconds"] = self._ttl_seconds
            try:
                self._kv.kv_buckets(body)
            except IronflowError as err:
                if err.status_code != 409:  # 409: bucket already exists
                    raise  # _ready stays False, so the next call retries
            self._ready = True

    def _write(self, command_id: str, body: Any, *, create_only: bool) -> None:
        """PUT ``body``. A 404 means the bucket is gone, so recreate it and retry once."""
        from ._http import IronflowError

        path = f"/api/v1/kv/buckets/{quote(self._bucket, safe='')}/keys/{quote(command_id, safe='')}"
        headers = {"If-None-Match": "*"} if create_only else None
        for recreated in (False, True):
            self._ensure_bucket()
            try:
                # No transport retry on the claim: if a first attempt commits and its reply is lost, a retry
                # gets 412 on our own claim and would read it back as a prior winner, so the handler never runs.
                # finalize overwrites, so it keeps the default retry.
                self._kv.request("PUT", path, headers=headers, body=body, retry=False if create_only else None)
                return
            except IronflowError as err:
                # A 404 on a write never committed, so the retry cannot double-claim.
                if err.status_code != 404 or recreated:
                    raise
                with self._lock:
                    self._ready = False

    def try_claim(self, command_id: str, claim: T, *, is_owner: Callable[[T], bool] | None = None) -> T | None:
        """Claim ``command_id``. ``None`` means this caller won; else the prior entry.

        ``is_owner(prior)`` is checked only on a prior entry. When true, the entry is this
        caller's own orphaned claim and the caller wins.
        """
        from ._http import IronflowError

        for _ in range(_CLAIM_ATTEMPTS):
            try:
                self._write(command_id, claim, create_only=True)
                return None
            except IronflowError as err:
                if err.status_code != 412:
                    raise
            try:
                entry = self._kv.kv_get_buckets_keys(self._bucket, command_id)
            except IronflowError as err:
                if err.status_code == 404:  # the winner released between our write and read: claim again
                    continue
                raise
            # A corrupt entry raises ValueError so the operator can investigate.
            prior = cast("T", json.loads(base64.b64decode(entry["value"])))
            return None if is_owner is not None and is_owner(prior) else prior
        raise IronflowError(
            f"command dedup: claim for {command_id!r} lost the race {_CLAIM_ATTEMPTS} times",
            code="COMMAND_DEDUP_RACE",
        )

    def finalize(self, command_id: str, result: T) -> None:
        """Store the handler's result for later ``try_claim`` callers."""
        self._write(command_id, result, create_only=False)

    def release(self, command_id: str) -> None:
        """Drop the claim so a retry can proceed. Idempotent."""
        from ._http import IronflowError

        self._ensure_bucket()
        try:
            self._kv.kv_delete_buckets_keys(self._bucket, command_id)
        except IronflowError as err:
            if err.status_code != 404:
                raise


class _CommandDedupMixin:
    def command_dedup(
        self, bucket_name: str, *, ttl_seconds: int = DEFAULT_COMMAND_DEDUP_TTL_SECONDS,
    ) -> CommandDedup[Any]:
        """Build a :class:`CommandDedup` over KV bucket ``bucket_name`` (created on first use).

        ``ttl_seconds=0`` means entries never expire.
        """
        return CommandDedup(cast("_KV", self), bucket_name, ttl_seconds)
