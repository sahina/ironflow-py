"""The webhooks group (#3003) through the Python client.

The secret operations and deliveries are in test_readiness_live.py.
Skips without IRONFLOW_READINESS_SEED; `make test-readiness-live` sets it.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import urllib.error
import urllib.request
from typing import Any

import pytest
from protobuf.wkt import Struct

from ironflow import IronflowRPCError
from ironflow.rpc.v1 import (
    CreateWebhookSourceRequest,
    DeleteWebhookSourceRequest,
    GetWebhookSourceRequest,
    ListWebhookSourcesRequest,
    UpdateWebhookSourceRequest,
    WebhookVerifyConfig,
)
from ironflow.rpc.v1 import (
    TestWebhookVerifyConfigRequest as VerifyRequest,  # aliased: pytest collects Test* names
)

pytestmark = pytest.mark.usefixtures("readiness_seed")


def test_webhook_source_crud(rpc: Any) -> None:
    name = f"pysdk-crud-{time.time_ns()}"
    created = rpc.webhooks.create_source(
        CreateWebhookSourceRequest(
            name=name,
            event_prefix="pysdk.crud",
            verify_header="x-sig",
            verify_algorithm="hmac-sha256",
            verify_secret="s1",
            metadata=Struct.from_python({"team": "payments", "tier": "gold"}),
        )
    )
    try:
        assert created.id.startswith("wh_")
        assert (created.name, created.event_prefix, created.verify_header, created.verify_algorithm) == (
            name, "pysdk.crud", "x-sig", "hmac-sha256")
        assert created.verify_secret_set
        assert created.metadata.to_python() == {"team": "payments", "tier": "gold"}
        assert created.ingest_token

        # The token comes back once, on create; every read after that has its prefix only.
        got = rpc.webhooks.get_source(GetWebhookSourceRequest(id=created.id))
        assert (got.id, got.name, got.event_prefix, got.verify_header) == (created.id, name, "pysdk.crud", "x-sig")
        assert got.verify_secret_set
        assert not got.ingest_token
        assert got.ingest_token_prefix and created.ingest_token.startswith(got.ingest_token_prefix)

        listed = rpc.webhooks.list_sources(ListWebhookSourcesRequest()).sources
        assert [s.name for s in listed if s.id == created.id] == [name]

        # Name and metadata are full-replace (the "tier" key must go); the header,
        # algorithm and secret are preserve-on-omit, and the event prefix cannot change.
        updated = rpc.webhooks.update_source(
            UpdateWebhookSourceRequest(
                id=created.id, name=f"{name}-renamed", metadata=Struct.from_python({"team": "billing"})
            )
        )
        assert (updated.name, updated.event_prefix, updated.verify_header, updated.verify_algorithm) == (
            f"{name}-renamed", "pysdk.crud", "x-sig", "hmac-sha256")
        assert updated.verify_secret_set
        assert updated.metadata.to_python() == {"team": "billing"}
        assert updated.updated_at != got.updated_at
        assert rpc.webhooks.get_source(GetWebhookSourceRequest(id=created.id)).name == f"{name}-renamed"
    finally:
        rpc.webhooks.delete_source(DeleteWebhookSourceRequest(id=created.id))

    with pytest.raises(IronflowRPCError) as exc:
        rpc.webhooks.get_source(GetWebhookSourceRequest(id=created.id))
    assert exc.value.code == "not_found"
    assert created.id not in [s.id for s in rpc.webhooks.list_sources(ListWebhookSourcesRequest()).sources]


def test_webhook_verify_test(rpc: Any, server_url: str) -> None:
    """The tester's verdict is the one real ingest reaches (tests/integration/webhook_verify_test.go)."""
    secret = "whsec_readiness"
    config = WebhookVerifyConfig(
        signature_header="Stripe-Signature",
        entry_separator=",",
        kv_delimiter="=",
        signature_key="v1",
        timestamp_key="t",
        signing_template="{ts}.{body}",
        encoding="hex",
        algorithm="hmac-sha256",
        tolerance_seconds=300,
        event_name_path="body:type",
        dedup_id_path="body:id",
    )
    src = rpc.webhooks.create_source(
        CreateWebhookSourceRequest(
            name=f"pysdk-verify-{time.time_ns()}", event_prefix="pysdk.verify", verify_secret=secret, verify_config=config
        )
    )

    def sign(key: str, stamp: str, body: str) -> str:
        sig = hmac.new(key.encode(), f"{stamp}.{body}".encode(), hashlib.sha256).hexdigest()
        return f"t={stamp},v1={sig}"

    def ingest(body: str, header: str) -> int:
        url = f"{server_url}/api/v1/webhooks/{src.id}?token={urllib.request.quote(src.ingest_token)}"
        request = urllib.request.Request(
            url, data=body.encode(), headers={"Content-Type": "application/json", "Stripe-Signature": header}, method="POST"
        )
        try:
            with urllib.request.urlopen(request) as resp:
                return int(resp.status)
        except urllib.error.HTTPError as err:
            return err.code

    try:
        # Ingest enforces the tolerance window, so the timestamp must be current.
        stamp = str(int(time.time()))
        body = json.dumps({"id": "evt_verify_1", "type": "payment.succeeded", "note": "café"})
        header = sign(secret, stamp, body)
        good = header.split("v1=")[1]

        def test(**extra: Any) -> Any:
            return rpc.webhooks.test_verify_config(
                VerifyRequest(verify_config=config, body=body, headers={"Stripe-Signature": header}, **extra)
            )

        verdict = test(verify_secret=secret)
        assert verdict.verified and not verdict.error
        assert verdict.signing_string == f"{stamp}.{body}"
        assert verdict.computed_signature == good
        assert list(verdict.presented_signatures) == [good]
        assert (verdict.resolved_timestamp, verdict.resolved_event_name, verdict.resolved_dedup_id) == (
            stamp, "payment.succeeded", "evt_verify_1")
        assert ingest(body, header) == 200

        borrowed = test(source_id=src.id)
        assert borrowed.verified and borrowed.computed_signature == good

        wrong = test(verify_secret="whsec_wrong")
        assert not wrong.verified
        assert wrong.signing_string == f"{stamp}.{body}"  # a wrong secret still reports what it signed
        other = json.dumps({"id": "evt_verify_2", "type": "payment.succeeded"})
        assert ingest(other, sign("whsec_wrong", stamp, other)) == 401
    finally:
        rpc.webhooks.delete_source(DeleteWebhookSourceRequest(id=src.id))
