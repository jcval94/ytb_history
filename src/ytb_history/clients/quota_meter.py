"""Quota meter helper to track estimated API units."""

from __future__ import annotations

from collections import Counter


class QuotaMeter:
    """Tracks estimated quota units per endpoint."""

    COST_CHANNELS_LIST = 1
    COST_PLAYLIST_ITEMS_LIST = 1
    COST_VIDEOS_LIST = 1
    COST_SEARCH_LIST = 100

    PROHIBITED_ENDPOINTS = frozenset({"search.list"})

    ENDPOINT_COSTS: dict[str, int] = {
        "channels.list": COST_CHANNELS_LIST,
        "playlistItems.list": COST_PLAYLIST_ITEMS_LIST,
        "videos.list": COST_VIDEOS_LIST,
        "search.list": COST_SEARCH_LIST,
    }

    def __init__(self) -> None:
        self._counter: Counter[str] = Counter()

    def add(self, endpoint: str, units: int = 1) -> None:
        """Backward-compatible increment by explicit units."""
        self._counter[endpoint] += units

    def add_endpoint(self, endpoint: str, requests: int = 1) -> None:
        """Increment usage for an endpoint by number of requests."""
        if requests < 0:
            raise ValueError("requests must be >= 0")
        cost = self.ENDPOINT_COSTS.get(endpoint, 0)
        self._counter[endpoint] += cost * requests

    def as_dict(self) -> dict[str, int]:
        return dict(self._counter)

    @property
    def total(self) -> int:
        return sum(self._counter.values())

    def reset(self) -> None:
        self._counter.clear()
