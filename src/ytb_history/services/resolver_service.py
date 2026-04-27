"""Resolve configured channel URLs into persisted channel registry records."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ytb_history.clients.youtube_client import YouTubeClient
from ytb_history.domain.models import ChannelRecord
from ytb_history.repositories.channel_registry_repo import ChannelRegistryRepo


def normalize_channel_url(channel_url: str) -> str:
    raw = channel_url.strip()
    parsed = urlparse(raw)
    netloc = parsed.netloc.lower().replace("www.", "")
    if netloc not in {"youtube.com", "m.youtube.com"}:
        raise ValueError(f"Unsupported YouTube URL: {channel_url}")

    path = parsed.path.rstrip("/")
    if not path:
        raise ValueError(f"Invalid YouTube channel URL: {channel_url}")

    return f"https://www.youtube.com{path}"


def _extract_target(normalized_url: str) -> tuple[str, str]:
    path = urlparse(normalized_url).path
    if path.startswith("/@"):
        return "handle", path[2:]
    if path.startswith("/channel/"):
        return "channel_id", path.split("/", maxsplit=2)[2]
    raise ValueError(f"Unsupported YouTube channel URL format: {normalized_url}")


def _build_ok_record(source_url: str, item: dict) -> ChannelRecord:
    channel_id = item.get("id", "")
    channel_name = item.get("snippet", {}).get("title", "")
    uploads_playlist_id = item.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads", "")

    if not channel_id or not uploads_playlist_id:
        raise ValueError("YouTube API channel response missing required fields")

    return ChannelRecord(
        channel_url=source_url,
        channel_id=channel_id,
        channel_name=channel_name,
        uploads_playlist_id=uploads_playlist_id,
        resolved_at=datetime.now(timezone.utc),
        resolver_status="ok",
        error_message=None,
    )


def _build_error_record(source_url: str, error_message: str) -> ChannelRecord:
    return ChannelRecord(
        channel_url=source_url,
        channel_id="",
        channel_name="",
        uploads_playlist_id="",
        resolved_at=datetime.now(timezone.utc),
        resolver_status="error",
        error_message=error_message,
    )


def resolve_channels(
    channel_urls: list[str],
    *,
    youtube_client: "YouTubeClient",
    channel_registry_repo: ChannelRegistryRepo,
) -> list[ChannelRecord]:
    existing = channel_registry_repo.load()

    cache_by_url: dict[str, ChannelRecord] = {}
    cache_by_channel_id: dict[str, ChannelRecord] = {}
    for record in existing:
        if record.resolver_status == "ok" and record.channel_id and record.uploads_playlist_id:
            cache_by_channel_id[record.channel_id] = record
            cache_by_url[normalize_channel_url(record.channel_url)] = record

    seen_urls: set[str] = set()
    resolved_records: list[ChannelRecord] = []

    for channel_url in channel_urls:
        normalized_url = normalize_channel_url(channel_url)
        if normalized_url in seen_urls:
            continue
        seen_urls.add(normalized_url)

        if normalized_url in cache_by_url:
            resolved_records.append(cache_by_url[normalized_url])
            continue

        try:
            target_kind, target_value = _extract_target(normalized_url)

            if target_kind == "channel_id" and target_value in cache_by_channel_id:
                resolved_records.append(cache_by_channel_id[target_value])
                continue

            if target_kind == "handle":
                response = youtube_client.get_channel_by_handle(target_value)
            else:
                response = youtube_client.get_channel_by_id(target_value)

            items = response.get("items", [])
            if not items:
                raise ValueError("Channel not found")

            record = _build_ok_record(normalized_url, items[0])
            resolved_records.append(record)
            cache_by_url[normalized_url] = record
            cache_by_channel_id[record.channel_id] = record
        except Exception as exc:  # noqa: BLE001 - preserve partial resilience
            resolved_records.append(_build_error_record(normalized_url, str(exc)))

    if resolved_records:
        channel_registry_repo.upsert(resolved_records)

    return resolved_records


def resolve_channels_to_default_registry(
    channel_urls: list[str],
    *,
    youtube_client: "YouTubeClient",
    registry_path: str | Path = Path("data/state/channel_registry.jsonl"),
) -> list[ChannelRecord]:
    repo = ChannelRegistryRepo(path=registry_path)
    return resolve_channels(channel_urls, youtube_client=youtube_client, channel_registry_repo=repo)
