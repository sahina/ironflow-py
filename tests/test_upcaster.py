from __future__ import annotations

import pytest

from ironflow import REDACTED_MARKER_KEY, UpcasterChainError, UpcasterRegistry


def reg() -> UpcasterRegistry:
    r = UpcasterRegistry()
    r.register("order", 1, 2, lambda d: {**d, "v2": True})
    r.register("order", 2, 3, lambda d: {**d, "v3": True})
    return r


def test_chain_applies_in_order() -> None:
    assert reg().upcast("order", {"a": 1}, 1, 3) == {"a": 1, "v2": True, "v3": True}


def test_same_or_lower_version_is_identity() -> None:
    data = {"a": 1}
    assert reg().upcast("order", data, 3, 3) is data
    assert reg().upcast("order", data, 3, 2) is data


def test_chain_can_skip_versions() -> None:
    r = UpcasterRegistry()
    r.register("e", 1, 3, lambda d: {"jumped": d["x"]})
    assert r.upcast("e", {"x": 5}, 1, 3) == {"jumped": 5}
    assert r.latest_version("e") == 3


def test_broken_chain_names_the_gap() -> None:
    r = UpcasterRegistry()
    r.register("e", 1, 2, lambda d: d)
    r.register("e", 3, 4, lambda d: d)
    with pytest.raises(UpcasterChainError, match=r'"e".*v2.*target v4'):
        r.upcast("e", {}, 1, 4)


def test_upcaster_exception_is_wrapped() -> None:
    r = UpcasterRegistry()

    def boom(d: object) -> object:
        raise KeyError("amount")

    r.register("e", 1, 2, boom)
    with pytest.raises(UpcasterChainError, match="v1.*v2") as info:
        r.upcast("e", {}, 1, 2)
    assert isinstance(info.value.__cause__, KeyError)


def test_redacted_passes_through() -> None:
    data = {REDACTED_MARKER_KEY: True}
    assert reg().upcast("order", data, 1, 3) is data


def test_latest_version_and_upcast_to_latest() -> None:
    r = reg()
    assert r.latest_version("order") == 3
    assert r.latest_version("nope") == 0
    assert r.upcast_to_latest("nope", {"a": 1}, 1) == {"a": 1}
    assert r.upcast_to_latest("order", {}, 2) == {"v3": True}


def test_non_dict_data_is_passed_to_upcaster() -> None:
    r = UpcasterRegistry()
    r.register("e", 1, 2, lambda d: [*d, 3])
    assert r.upcast("e", [1, 2], 1, 2) == [1, 2, 3]


def test_input_not_mutated_when_chain_fails() -> None:
    r = UpcasterRegistry()

    def mutate(d: dict) -> dict:
        d["touched"] = True
        return d

    r.register("e", 1, 2, mutate)
    data = {"a": 1}
    with pytest.raises(UpcasterChainError):
        r.upcast("e", data, 1, 3)
    assert data == {"a": 1}
