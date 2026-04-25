"""YouTube API client scaffold (no real calls yet)."""

from __future__ import annotations


class YouTubeClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def channels_list(self, *_args, **_kwargs) -> dict:
        raise NotImplementedError("Scaffold only: no API calls implemented yet")

    def playlist_items_list(self, *_args, **_kwargs) -> dict:
        raise NotImplementedError("Scaffold only: no API calls implemented yet")

    def videos_list(self, *_args, **_kwargs) -> dict:
        raise NotImplementedError("Scaffold only: no API calls implemented yet")
