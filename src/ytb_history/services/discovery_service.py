"""Discovery of recent videos from channel uploads playlists."""

from __future__ import annotations

from datetime import datetime

from ytb_history.clients.quota_meter import QuotaMeter
from ytb_history.clients.youtube_client import YouTubeClient
from ytb_history.domain.models import ChannelDiscoveryReport, ChannelRecord, DiscoveryResult
from ytb_history.utils.dates import parse_iso8601_utc


def discover_recent_videos(
    channels: list[ChannelRecord],
    *,
    since_datetime: datetime,
    youtube_client: YouTubeClient,
    quota_meter: QuotaMeter,
    max_pages_per_channel: int = 5,
) -> DiscoveryResult:
    """Discover recently published videos from channel uploads playlists."""
    recent_video_ids: list[str] = []
    seen_video_ids: set[str] = set()
    channel_reports: list[ChannelDiscoveryReport] = []

    seen_channel_ids: set[str] = set()
    seen_uploads_playlists: set[str] = set()

    for channel in channels:
        if channel.channel_id in seen_channel_ids:
            continue
        if channel.uploads_playlist_id and channel.uploads_playlist_id in seen_uploads_playlists:
            continue

        seen_channel_ids.add(channel.channel_id)
        if channel.uploads_playlist_id:
            seen_uploads_playlists.add(channel.uploads_playlist_id)

        if channel.resolver_status != "ok":
            channel_reports.append(
                ChannelDiscoveryReport(
                    channel_id=channel.channel_id,
                    channel_name=channel.channel_name,
                    uploads_playlist_id=channel.uploads_playlist_id,
                    pages_read=0,
                    videos_seen=0,
                    videos_recent=0,
                    stopped_reason="skipped_non_ok_channel",
                    error_message=channel.error_message,
                )
            )
            continue

        if not channel.uploads_playlist_id:
            channel_reports.append(
                ChannelDiscoveryReport(
                    channel_id=channel.channel_id,
                    channel_name=channel.channel_name,
                    uploads_playlist_id=channel.uploads_playlist_id,
                    pages_read=0,
                    videos_seen=0,
                    videos_recent=0,
                    stopped_reason="missing_uploads_playlist_id",
                    error_message=None,
                )
            )
            continue

        pages_read = 0
        videos_seen = 0
        videos_recent = 0
        next_page_token: str | None = None
        stopped_reason = "max_pages_reached"
        error_message: str | None = None

        while pages_read < max_pages_per_channel:
            pages_read += 1
            try:
                payload = youtube_client.list_playlist_items(
                    channel.uploads_playlist_id,
                    page_token=next_page_token,
                    max_results=50,
                )
            except Exception as exc:  # pragma: no cover - behavior validated in tests
                stopped_reason = "channel_error"
                error_message = str(exc)
                break

            items = payload.get("items", [])
            valid_dates_in_page = 0
            recent_dates_in_page = 0

            for item in items:
                video_id = (item.get("contentDetails") or {}).get("videoId")
                if not video_id:
                    continue

                videos_seen += 1

                date_value = (item.get("contentDetails") or {}).get("videoPublishedAt")
                if not date_value:
                    date_value = (item.get("snippet") or {}).get("publishedAt")

                published_at = parse_iso8601_utc(date_value)
                if not published_at:
                    continue

                valid_dates_in_page += 1
                if published_at >= since_datetime:
                    recent_dates_in_page += 1
                    videos_recent += 1
                    if video_id not in seen_video_ids:
                        seen_video_ids.add(video_id)
                        recent_video_ids.append(video_id)

            next_page_token = payload.get("nextPageToken")
            if valid_dates_in_page > 0 and recent_dates_in_page == 0:
                stopped_reason = "older_than_window"
                break
            if not next_page_token:
                stopped_reason = "no_next_page"
                break
            if pages_read >= max_pages_per_channel:
                stopped_reason = "max_pages_reached"
                break

        channel_reports.append(
            ChannelDiscoveryReport(
                channel_id=channel.channel_id,
                channel_name=channel.channel_name,
                uploads_playlist_id=channel.uploads_playlist_id,
                pages_read=pages_read,
                videos_seen=videos_seen,
                videos_recent=videos_recent,
                stopped_reason=stopped_reason,
                error_message=error_message,
            )
        )

    return DiscoveryResult(recent_video_ids=recent_video_ids, channel_reports=channel_reports)
