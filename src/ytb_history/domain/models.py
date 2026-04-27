"""Core data models for ytb_history."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime


@dataclass(slots=True)
class ChannelRecord:
    channel_url: str
    channel_id: str
    channel_name: str
    uploads_playlist_id: str
    resolved_at: datetime
    resolver_status: str = "ok"
    error_message: str | None = None

    def to_dict(self) -> dict:
        data = asdict(self)
        data["resolved_at"] = self.resolved_at.isoformat()
        return data


@dataclass(slots=True)
class ChannelDiscoveryReport:
    channel_id: str
    channel_name: str
    uploads_playlist_id: str
    pages_read: int
    videos_seen: int
    videos_recent: int
    stopped_reason: str
    error_message: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(slots=True)
class DiscoveryResult:
    recent_video_ids: list[str]
    channel_reports: list[ChannelDiscoveryReport]

    def to_dict(self) -> dict:
        return {
            "recent_video_ids": list(self.recent_video_ids),
            "channel_reports": [report.to_dict() for report in self.channel_reports],
        }


@dataclass(slots=True)
class TrackedVideoRecord:
    video_id: str
    channel_id: str
    channel_name: str
    first_seen_date: datetime
    last_seen_execution_date: datetime
    tracking_until_date: datetime
    active: bool = True

    def to_dict(self) -> dict:
        data = asdict(self)
        data["first_seen_date"] = self.first_seen_date.isoformat()
        data["last_seen_execution_date"] = self.last_seen_execution_date.isoformat()
        data["tracking_until_date"] = self.tracking_until_date.isoformat()
        return data


@dataclass(slots=True)
class VideoSnapshot:
    execution_date: datetime
    channel_id: str
    channel_name: str
    video_id: str
    title: str
    description: str
    upload_date: datetime
    tags: list[str]
    thumbnail_url: str
    duration_seconds: int
    views: int | None
    likes: int | None
    comments: int | None

    def to_dict(self) -> dict:
        data = asdict(self)
        data["execution_date"] = self.execution_date.isoformat()
        data["upload_date"] = self.upload_date.isoformat()
        return data


@dataclass(slots=True)
class EnrichmentResult:
    snapshots: list[VideoSnapshot]
    unavailable_video_ids: list[str]
    errors: list[str]

    def to_dict(self) -> dict:
        return {
            "snapshots": [snapshot.to_dict() for snapshot in self.snapshots],
            "unavailable_video_ids": list(self.unavailable_video_ids),
            "errors": list(self.errors),
        }


@dataclass(slots=True)
class VideoDelta:
    execution_date: datetime
    video_id: str
    views_delta: int | None
    likes_delta: int | None
    comments_delta: int | None
    previous_views: int | None
    current_views: int | None
    previous_likes: int | None
    current_likes: int | None
    previous_comments: int | None
    current_comments: int | None
    is_new_video: bool
    title_changed: bool
    description_changed: bool
    tags_changed: bool

    def to_dict(self) -> dict:
        data = asdict(self)
        data["execution_date"] = self.execution_date.isoformat()
        return data


@dataclass(slots=True)
class QuotaReport:
    execution_date: datetime
    estimated_units: dict[str, int]
    total_estimated_units: int
    operational_limit: int
    warning_limit: int
    soft_warning_limit: int
    limit_status: str

    def to_dict(self) -> dict:
        data = asdict(self)
        data["execution_date"] = self.execution_date.isoformat()
        return data


@dataclass(slots=True)
class RunSummary:
    execution_date: datetime
    status: str
    channels_total: int
    channels_ok: int
    channels_failed: int
    videos_discovered: int
    videos_tracked: int
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        data = asdict(self)
        data["execution_date"] = self.execution_date.isoformat()
        return data
