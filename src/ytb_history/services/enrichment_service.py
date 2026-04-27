"""Video enrichment service using videos.list."""

from __future__ import annotations

from datetime import datetime

from ytb_history.clients.youtube_client import YouTubeClient
from ytb_history.domain.models import EnrichmentResult, VideoSnapshot
from ytb_history.utils.batching import chunked
from ytb_history.utils.dates import parse_iso8601_utc
from ytb_history.utils.durations import parse_youtube_duration_to_seconds


def fetch_video_snapshots(
    video_ids: list[str],
    *,
    youtube_client: YouTubeClient,
    execution_date: datetime,
    batch_size: int = 50,
) -> EnrichmentResult:
    """Fetch and normalize snapshots for a list of video ids."""
    if batch_size <= 0 or batch_size > 50:
        raise ValueError("batch_size must be between 1 and 50")

    unique_video_ids = _dedupe_preserving_order(video_ids)
    snapshots: list[VideoSnapshot] = []
    unavailable_video_ids: list[str] = []
    errors: list[str] = []

    for ids_batch in chunked(unique_video_ids, size=batch_size):
        try:
            payload = youtube_client.get_videos_by_ids(ids_batch)
        except Exception as exc:  # pragma: no cover - behavior tested at API level
            errors.append(
                f"Failed to fetch batch of {len(ids_batch)} videos; sample_ids={ids_batch[:3]}: {exc}"
            )
            unavailable_video_ids.extend(ids_batch)
            continue

        items = payload.get("items", []) or []
        found_by_id = {item.get("id", ""): item for item in items if item.get("id")}

        for requested_video_id in ids_batch:
            item = found_by_id.get(requested_video_id)
            if item is None:
                unavailable_video_ids.append(requested_video_id)
                continue
            snapshots.append(
                _normalize_snapshot(
                    item=item,
                    execution_date=execution_date,
                    errors=errors,
                )
            )

    return EnrichmentResult(
        snapshots=snapshots,
        unavailable_video_ids=unavailable_video_ids,
        errors=errors,
    )


def _normalize_snapshot(*, item: dict, execution_date: datetime, errors: list[str]) -> VideoSnapshot:
    snippet = item.get("snippet", {}) or {}
    statistics = item.get("statistics", {}) or {}
    content_details = item.get("contentDetails", {}) or {}

    video_id = item.get("id", "")
    published_at = parse_iso8601_utc(snippet.get("publishedAt"))
    if published_at is None:
        published_at = execution_date
        errors.append(f"Invalid or missing upload_date for video {video_id}; fallback to execution_date")

    thumbnails = snippet.get("thumbnails", {}) or {}

    return VideoSnapshot(
        execution_date=execution_date,
        channel_id=snippet.get("channelId", "") or "",
        channel_name=snippet.get("channelTitle", "") or "",
        video_id=video_id,
        title=snippet.get("title", "") or "",
        description=snippet.get("description", "") or "",
        upload_date=published_at,
        tags=snippet.get("tags", []) or [],
        thumbnail_url=_pick_thumbnail_url(thumbnails),
        duration_seconds=parse_youtube_duration_to_seconds(content_details.get("duration")),
        views=_safe_int(statistics.get("viewCount")),
        likes=_safe_int(statistics.get("likeCount")),
        comments=_safe_int(statistics.get("commentCount")),
    )


def _pick_thumbnail_url(thumbnails: dict) -> str:
    for quality in ("maxres", "high", "medium", "default"):
        url = ((thumbnails.get(quality, {}) or {}).get("url"))
        if isinstance(url, str) and url:
            return url
    return ""


def _safe_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _dedupe_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result
