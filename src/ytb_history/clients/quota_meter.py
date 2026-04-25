"""Quota meter helper to track estimated API units."""

from __future__ import annotations

from collections import Counter


class QuotaMeter:
    def __init__(self) -> None:
        self._counter: Counter[str] = Counter()

    def add(self, endpoint: str, units: int = 1) -> None:
        self._counter[endpoint] += units

    def as_dict(self) -> dict[str, int]:
        return dict(self._counter)

    @property
    def total(self) -> int:
        return sum(self._counter.values())
