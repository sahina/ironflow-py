from __future__ import annotations

import pytest

from ironflow.serve._signature import SignatureError, sign, verify_signature

BODY = b'{"run_id":"run_1"}'
KEY = "whsec_test"
TS = 1790000000
# Computed by a standalone Go program with signPayload's exact code
# (internal/engine/executor_transport.go:307: fmt.Sprintf("%d.%s") + crypto/hmac
# sha256 + hex), with this body, key and a fixed timestamp. Do not regenerate it
# with Python: a Python-derived vector would only prove Python agrees with itself.
GO_VECTOR = "t=1790000000,v1=0733159dcc5bdb1a3575e578becff60f88b0f25946589116376293d8a03453ee"


def test_sign_matches_go_byte_for_byte() -> None:
    assert sign(BODY, KEY, TS) == GO_VECTOR


def test_go_vector_verifies() -> None:
    verify_signature(BODY, GO_VECTOR, KEY, now=TS)


@pytest.mark.parametrize("skew", [-299, 299])
def test_inside_tolerance_passes(skew: int) -> None:
    verify_signature(BODY, GO_VECTOR, KEY, now=TS + skew)


@pytest.mark.parametrize("skew", [-301, 301])
def test_outside_tolerance_fails(skew: int) -> None:
    with pytest.raises(SignatureError) as e:
        verify_signature(BODY, GO_VECTOR, KEY, now=TS + skew)
    assert e.value.code == "SIGNATURE_INVALID"


def test_wrong_key_fails() -> None:
    with pytest.raises(SignatureError):
        verify_signature(BODY, GO_VECTOR, "other", now=TS)


def test_tampered_body_fails() -> None:
    with pytest.raises(SignatureError):
        verify_signature(BODY + b" ", GO_VECTOR, KEY, now=TS)


@pytest.mark.parametrize("header", [None, ""])
def test_missing_header(header: str | None) -> None:
    with pytest.raises(SignatureError) as e:
        verify_signature(BODY, header, KEY, now=TS)
    assert e.value.code == "SIGNATURE_MISSING"


@pytest.mark.parametrize("header", [
    "v1=abc", "t=1790000000", "t=abc,v1=00", "sha256=0733159d", "garbage",
])
def test_malformed_header(header: str) -> None:
    with pytest.raises(SignatureError) as e:
        verify_signature(BODY, header, KEY, now=TS)
    assert e.value.code == "SIGNATURE_INVALID"


def test_huge_timestamp_is_signature_invalid_not_overflow() -> None:
    with pytest.raises(SignatureError) as e:
        verify_signature(BODY, f"t={'9' * 400},v1=00", KEY)
    assert e.value.code == "SIGNATURE_INVALID"


def test_non_ascii_v1_is_signature_invalid_not_type_error() -> None:
    with pytest.raises(SignatureError) as e:
        verify_signature(BODY, "t=1790000000,v1=" + "é" * 4, KEY, now=TS)
    assert e.value.code == "SIGNATURE_INVALID"


def test_public_sign_round_trips() -> None:
    from ironflow.serve import sign, verify_signature

    header = sign(b'{"a":1}', "secret", 1_700_000_000)
    verify_signature(b'{"a":1}', header, "secret", now=1_700_000_000)
