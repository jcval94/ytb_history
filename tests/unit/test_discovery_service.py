from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from ytb_history.clients.quota_meter import QuotaMeter
from ytb_history.domain.models import ChannelRecord
from ytb_history.services import discovery_service


class FakeYouTubeClient:
    def __init__(
        self,
        responses: list[dict] | None = None,
        *,
        error_on_call: int | None = None,
        quota_meter: QuotaMeter | None = None,
    ) -> None:
        self._responses = responses or []
        self._error_on_call = error_on_call
        self._quota_meter = quota_meter
        self.calls: list[dict] = []

    def list_playlist_items(
        self,
        playlist_id: str,
        page_token: str | None = None,
        max_results: int = 50,
    ) -> dict:
        self.calls.append(
            {
                "playlist_id": playlist_id,
                "page_token": page_token,
                "max_results": max_results,
            }
        )
        if self._quota_meter is not None:
            self._quota_meter.add_endpoint("playlistItems.list")
        if self._error_on_call is not None and len(self.calls) == self._error_on_call:
            raise RuntimeError("boom")
        if len(self.calls) <= len(self._responses):
            return self._responses[len(self.calls) - 1]
        return {"items": []}


def _channel(
    channel_id: str = "UC1",
    uploads_playlist_id: str = "UU1",
    resolver_status: str = "ok",
) -> ChannelRecord:
    return ChannelRecord(
        channel_url=f"https://youtube.com/channel/{channel_id}",
        channel_id=channel_id,
        channel_name=f"Channel {channel_id}",
        uploads_playlist_id=uploads_playlist_id,
        resolved_at=datetime(2026, 4, 27, tzinfo=timezone.utc),
        resolver_status=resolver_status,
        error_message=None,
    )


def _item(video_id: str | None, *, video_published: str | None = None, snippet_published: str | None = None) -> dict:
    payload: dict = {"contentDetails": {}, "snippet": {}}
    if video_id is not None:
        payload["contentDetails"]["videoId"] = video_id
    if video_published is not None:
        payload["contentDetails"]["videoPublishedAt"] = video_published
    if snippet_published is not None:
        payload["snippet"]["publishedAt"] = snippet_published
    return payload


def test_channel_with_recent_videos_returns_video_ids() -> None:
    client = FakeYouTubeClient(
        responses=[{"items": [_item("v1", video_published="2026-04-26T10:00:00Z")] }]
    )
    quota = QuotaMeter()

    result = discovery_service.discover_recent_videos(
        [_channel()],
        since_datetime=datetime(2026, 4, 20, tzinfo=timezone.utc),
        youtube_client=client,
        quota_meter=quota,
    )

    assert result.recent_video_ids == ["v1"]


def test_channel_without_recent_videos_stops_older_than_window() -> None:
    client = FakeYouTubeClient(
        responses=[{"items": [_item("old", video_published="2026-04-01T00:00:00Z")], "nextPageToken": "n2"}]
    )

    result = discovery_service.discover_recent_videos(
        [_channel()],
        since_datetime=datetime(2026, 4, 20, tzinfo=timezone.utc),
        youtube_client=client,
        quota_meter=QuotaMeter(),
    )

    assert result.recent_video_ids == []
    assert result.channel_reports[0].stopped_reason == "older_than_window"


def test_uses_content_details_video_published_at_as_primary_source() -> None:
    client = FakeYouTubeClient(
        responses=[
            {
                "items": [
                    _item(
                        "v1",
                        video_published="2026-04-25T00:00:00Z",
                        snippet_published="2026-01-01T00:00:00Z",
                    )
                ]
            }
        ]
    )

    result = discovery_service.discover_recent_videos(
        [_channel()],
        since_datetime=datetime(2026, 4, 20, tzinfo=timezone.utc),
        youtube_client=client,
        quota_meter=QuotaMeter(),
    )

    assert result.recent_video_ids == ["v1"]


def test_uses_snippet_published_at_as_fallback() -> None:
    client = FakeYouTubeClient(
        responses=[{"items": [_item("v1", snippet_published="2026-04-24T00:00:00Z")]}]
    )

    result = discovery_service.discover_recent_videos(
        [_channel()],
        since_datetime=datetime(2026, 4, 20, tzinfo=timezone.utc),
        youtube_client=client,
        quota_meter=QuotaMeter(),
    )

    assert result.recent_video_ids == ["v1"]


def test_ignores_items_without_video_id() -> None:
    client = FakeYouTubeClient(responses=[{"items": [_item(None, video_published="2026-04-24T00:00:00Z")]}])

    result = discovery_service.discover_recent_videos(
        [_channel()],
        since_datetime=datetime(2026, 4, 20, tzinfo=timezone.utc),
        youtube_client=client,
        quota_meter=QuotaMeter(),
    )

    assert result.recent_video_ids == []
    assert result.channel_reports[0].videos_seen == 0


def test_ignores_items_with_invalid_date() -> None:
    client = FakeYouTubeClient(responses=[{"items": [_item("v1", video_published="not-a-date")]}])

    result = discovery_service.discover_recent_videos(
        [_channel()],
        since_datetime=datetime(2026, 4, 20, tzinfo=timezone.utc),
        youtube_client=client,
        quota_meter=QuotaMeter(),
    )

    assert result.recent_video_ids == []


def test_reads_second_page_with_next_page_token() -> None:
    client = FakeYouTubeClient(
        responses=[
            {"items": [_item("v1", video_published="2026-04-26T00:00:00Z")], "nextPageToken": "P2"},
            {"items": [_item("v2", video_published="2026-04-25T00:00:00Z")]},
        ],
    )

    result = discovery_service.discover_recent_videos(
        [_channel()],
        since_datetime=datetime(2026, 4, 20, tzinfo=timezone.utc),
        youtube_client=client,
        quota_meter=QuotaMeter(),
    )

    assert result.recent_video_ids == ["v1", "v2"]
    assert client.calls[1]["page_token"] == "P2"


def test_stops_when_no_next_page_token() -> None:
    client = FakeYouTubeClient(responses=[{"items": []}])

    result = discovery_service.discover_recent_videos(
        [_channel()],
        since_datetime=datetime(2026, 4, 20, tzinfo=timezone.utc),
        youtube_client=client,
        quota_meter=QuotaMeter(),
    )

    assert result.channel_reports[0].stopped_reason == "no_next_page"


def test_stops_when_reaches_max_pages_per_channel() -> None:
    client = FakeYouTubeClient(
        responses=[
            {"items": [_item("v1", video_published="2026-04-26T00:00:00Z")], "nextPageToken": "P2"},
            {"items": [_item("v2", video_published="2026-04-25T00:00:00Z")], "nextPageToken": "P3"},
            {"items": [_item("v3", video_published="2026-04-24T00:00:00Z")], "nextPageToken": "P4"},
        ]
    )

    result = discovery_service.discover_recent_videos(
        [_channel()],
        since_datetime=datetime(2026, 4, 20, tzinfo=timezone.utc),
        youtube_client=client,
        quota_meter=QuotaMeter(),
        max_pages_per_channel=2,
    )

    assert len(client.calls) == 2
    assert result.channel_reports[0].stopped_reason == "max_pages_reached"


def test_stops_when_page_is_older_than_window() -> None:
    client = FakeYouTubeClient(
        responses=[
            {
                "items": [
                    _item("old1", video_published="2026-04-10T00:00:00Z"),
                    _item("old2", video_published="2026-04-09T00:00:00Z"),
                ],
                "nextPageToken": "P2",
            }
        ]
    )

    result = discovery_service.discover_recent_videos(
        [_channel()],
        since_datetime=datetime(2026, 4, 20, tzinfo=timezone.utc),
        youtube_client=client,
        quota_meter=QuotaMeter(),
    )

    assert len(client.calls) == 1
    assert result.channel_reports[0].stopped_reason == "older_than_window"


def test_deduplicates_video_ids_preserving_order() -> None:
    client = FakeYouTubeClient(
        responses=[
            {
                "items": [
                    _item("v1", video_published="2026-04-26T00:00:00Z"),
                    _item("v1", video_published="2026-04-26T00:00:00Z"),
                    _item("v2", video_published="2026-04-25T00:00:00Z"),
                ]
            }
        ]
    )

    result = discovery_service.discover_recent_videos(
        [_channel()],
        since_datetime=datetime(2026, 4, 20, tzinfo=timezone.utc),
        youtube_client=client,
        quota_meter=QuotaMeter(),
    )

    assert result.recent_video_ids == ["v1", "v2"]


def test_deduplicates_repeated_channels_to_avoid_duplicate_calls() -> None:
    client = FakeYouTubeClient(responses=[{"items": []}])
    channels = [_channel(channel_id="UC1", uploads_playlist_id="UU1"), _channel(channel_id="UC1", uploads_playlist_id="UU1")]

    result = discovery_service.discover_recent_videos(
        channels,
        since_datetime=datetime(2026, 4, 20, tzinfo=timezone.utc),
        youtube_client=client,
        quota_meter=QuotaMeter(),
    )

    assert len(client.calls) == 1
    assert len(result.channel_reports) == 1


def test_skips_non_ok_channel_without_api_call() -> None:
    client = FakeYouTubeClient(responses=[{"items": []}])

    result = discovery_service.discover_recent_videos(
        [_channel(resolver_status="error")],
        since_datetime=datetime(2026, 4, 20, tzinfo=timezone.utc),
        youtube_client=client,
        quota_meter=QuotaMeter(),
    )

    assert len(client.calls) == 0
    assert result.channel_reports[0].stopped_reason == "skipped_non_ok_channel"


def test_missing_uploads_playlist_id_without_api_call() -> None:
    client = FakeYouTubeClient(responses=[{"items": []}])

    result = discovery_service.discover_recent_videos(
        [_channel(uploads_playlist_id="")],
        since_datetime=datetime(2026, 4, 20, tzinfo=timezone.utc),
        youtube_client=client,
        quota_meter=QuotaMeter(),
    )

    assert len(client.calls) == 0
    assert result.channel_reports[0].stopped_reason == "missing_uploads_playlist_id"


def test_channel_error_registers_report_and_continues() -> None:
    client = FakeYouTubeClient(
        responses=[{"items": [_item("v2", video_published="2026-04-26T00:00:00Z")]}],
        error_on_call=1,
    )

    result = discovery_service.discover_recent_videos(
        [_channel(channel_id="UC1", uploads_playlist_id="UU1"), _channel(channel_id="UC2", uploads_playlist_id="UU2")],
        since_datetime=datetime(2026, 4, 20, tzinfo=timezone.utc),
        youtube_client=client,
        quota_meter=QuotaMeter(),
    )

    assert result.channel_reports[0].stopped_reason == "channel_error"
    assert "boom" in (result.channel_reports[0].error_message or "")
    assert result.channel_reports[1].channel_id == "UC2"


def test_quota_meter_adds_one_per_page_consulted() -> None:
    quota = QuotaMeter()
    client = FakeYouTubeClient(
        responses=[
            {"items": [_item("v1", video_published="2026-04-26T00:00:00Z")], "nextPageToken": "P2"},
            {"items": [_item("v2", video_published="2026-04-25T00:00:00Z")]},
        ],
        quota_meter=quota,
    )

    discovery_service.discover_recent_videos(
        [_channel()],
        since_datetime=datetime(2026, 4, 20, tzinfo=timezone.utc),
        youtube_client=client,
        quota_meter=quota,
    )

    assert quota.as_dict().get("playlistItems.list") == 2


def test_no_search_list_usage() -> None:
    source = Path(discovery_service.__file__).read_text(encoding="utf-8")
    assert "search.list" not in source
