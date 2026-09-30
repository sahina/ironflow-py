"""Event schema upcasters: transform event data from an older version to a newer one.

Semantics match the Node SDK (sdk/js/core/src/upcaster.ts): each upcaster jumps
to its registered `to_version`, and the latest version is the max of every
registered from/to version.
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Any

from ._http import IronflowError, is_redacted

UpcasterFn = Callable[[Any], Any]


class UpcasterChainError(IronflowError):
    """The chain is incomplete, or one upcaster raised."""


class UpcasterRegistry:
    def __init__(self) -> None:
        self._chains: dict[str, dict[int, tuple[int, UpcasterFn]]] = {}

    def register(self, event_name: str, from_version: int, to_version: int, fn: UpcasterFn) -> None:
        if to_version <= from_version:
            raise ValueError(f"upcaster {event_name!r}: to_version {to_version} must exceed from_version {from_version}")
        self._chains.setdefault(event_name, {})[from_version] = (to_version, fn)

    def upcast(self, event_name: str, data: Any, from_version: int, to_version: int) -> Any:
        if from_version >= to_version:
            return data
        # A redacted payload has no fields left to migrate; an upcaster that
        # rebuilds the dict would drop the marker and blind every guard.
        if is_redacted(data):
            return data
        chain = self._chains.get(event_name, {})
        current, version = copy.deepcopy(data), from_version
        while version < to_version:
            link = chain.get(version)
            if link is None:
                raise UpcasterChainError(
                    f'incomplete upcaster chain for "{event_name}": no upcaster from v{version} '
                    f"(chain broken at v{version}, target v{to_version})"
                )
            nxt, fn = link
            try:
                current = fn(current)
            except Exception as exc:
                raise UpcasterChainError(f'upcaster "{event_name}" v{version}→v{nxt} failed: {exc}') from exc
            version = nxt
        return current

    def latest_version(self, event_name: str) -> int:
        chain = self._chains.get(event_name)
        if not chain:
            return 0
        return max(max(f, t) for f, (t, _) in chain.items())

    def upcast_to_latest(self, event_name: str, data: Any, from_version: int) -> Any:
        latest = self.latest_version(event_name)
        if latest == 0 or from_version >= latest:
            return data
        return self.upcast(event_name, data, from_version, latest)
