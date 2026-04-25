"""YouTube Data API v3 client."""

from __future__ import annotations

import os
import random
import time
from typing import Any

import requests

from ytb_history.clients.quota_meter import QuotaMeter
from ytb_history.utils.errors import (
    MissingYouTubeApiKeyError,
    YouTubeApiError,
    YouTubeRetryableApiError,
)


class YouTubeClient:
    """Minimal YouTube Data API v3 client with retry and quota tracking."""

    BASE_URL = "https://www.googleapis.com/youtube/v3"
    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
    NON_RETRYABLE_STATUS_CODES = {400, 401, 403, 404}

    _FIELDS_CHANNEL = "items(id,snippet/title,contentDetails/relatedPlaylists/uploads)"
    _FIELDS_PLAYLIST_ITEMS = (
        "nextPageToken,items(contentDetails/videoId,contentDetails/videoPublishedAt,"
        "snippet/title,snippet/channelTitle,snippet/publishedAt,snippet/thumbnails)"
    )
    _FIELDS_VIDEOS = (
        "items(id,snippet(channelTitle,title,description,publishedAt,tags,thumbnails),"
        "contentDetails(duration),statistics(viewCount,likeCount,commentCount))"
    )

    def __init__(
        self,
        api_key: str | None = None,
        *,
        timeout: float = 10.0,
        max_retries: int = 3,
        backoff_base: float = 0.5,
        quota_meter: QuotaMeter | None = None,
    ) -> None:
        resolved_key = api_key or os.getenv("YOUTUBE_API_KEY")
        if not resolved_key:
            raise MissingYouTubeApiKeyError(
                "YouTube API key is missing. Set YOUTUBE_API_KEY or pass api_key."
            )

        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if timeout <= 0:
            raise ValueError("timeout must be > 0")

        self._api_key = resolved_key
        self._timeout = timeout
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._quota_meter = quota_meter or QuotaMeter()

    def get_channel_by_handle(self, handle: str) -> dict[str, Any]:
        normalized_handle = handle.strip()
        if normalized_handle.startswith("@"):
            normalized_handle = normalized_handle[1:]

        return self._request(
            endpoint="/channels",
            endpoint_name="channels.list",
            params={
                "part": "snippet,contentDetails",
                "forHandle": normalized_handle,
                "fields": self._FIELDS_CHANNEL,
            },
        )

    def get_channel_by_id(self, channel_id: str) -> dict[str, Any]:
        return self._request(
            endpoint="/channels",
            endpoint_name="channels.list",
            params={
                "part": "snippet,contentDetails",
                "id": channel_id,
                "fields": self._FIELDS_CHANNEL,
            },
        )

    def list_playlist_items(
        self,
        playlist_id: str,
        page_token: str | None = None,
        max_results: int = 50,
    ) -> dict[str, Any]:
        if not 1 <= max_results <= 50:
            raise ValueError("max_results must be between 1 and 50")

        params: dict[str, Any] = {
            "part": "snippet,contentDetails",
            "playlistId": playlist_id,
            "maxResults": max_results,
            "fields": self._FIELDS_PLAYLIST_ITEMS,
        }
        if page_token:
            params["pageToken"] = page_token

        return self._request(
            endpoint="/playlistItems",
            endpoint_name="playlistItems.list",
            params=params,
        )

    def get_videos_by_ids(self, video_ids: list[str]) -> dict[str, Any]:
        if len(video_ids) > 50:
            raise ValueError("video_ids supports at most 50 IDs per request")

        return self._request(
            endpoint="/videos",
            endpoint_name="videos.list",
            params={
                "part": "snippet,contentDetails,statistics",
                "id": ",".join(video_ids),
                "fields": self._FIELDS_VIDEOS,
            },
        )

    def _request(self, *, endpoint: str, endpoint_name: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.BASE_URL}{endpoint}"
        safe_params = dict(params)
        safe_params["key"] = self._api_key

        attempts = self._max_retries + 1
        for attempt in range(1, attempts + 1):
            self._quota_meter.add_endpoint(endpoint_name, requests=1)
            try:
                response = requests.get(url, params=safe_params, timeout=self._timeout)
            except requests.RequestException as exc:
                if attempt < attempts:
                    self._sleep_before_retry(attempt)
                    continue
                raise YouTubeRetryableApiError(
                    f"YouTube API request failed after retries for {endpoint_name}"
                ) from exc

            if response.status_code < 400:
                return response.json()

            if response.status_code in self.RETRYABLE_STATUS_CODES:
                if attempt < attempts:
                    self._sleep_before_retry(attempt)
                    continue
                raise YouTubeRetryableApiError(
                    f"Retryable YouTube API error {response.status_code} on {endpoint_name}"
                )

            if response.status_code in self.NON_RETRYABLE_STATUS_CODES:
                raise YouTubeApiError(
                    f"Non-retryable YouTube API error {response.status_code} on {endpoint_name}"
                )

            raise YouTubeApiError(
                f"Unexpected YouTube API error {response.status_code} on {endpoint_name}"
            )

        raise YouTubeRetryableApiError(f"YouTube API retries exhausted for {endpoint_name}")

    def _sleep_before_retry(self, attempt: int) -> None:
        delay = self._backoff_base * (2 ** (attempt - 1))
        jitter = random.uniform(0.0, self._backoff_base)
        time.sleep(delay + jitter)
