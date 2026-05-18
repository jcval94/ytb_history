"""Content-format classification for YouTube videos."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

SHORTS_CUTOFF_DATE = datetime(2024, 10, 15, tzinfo=timezone.utc)
CONTENT_FORMAT_SHORTS = "shorts"
CONTENT_FORMAT_VIDEOS = "videos"
CONTENT_FORMAT_UNKNOWN = "unknown"
CONTENT_FORMATS = (CONTENT_FORMAT_SHORTS, CONTENT_FORMAT_VIDEOS)


@dataclass(frozen=True, slots=True)
class ContentFormatClassification:
    content_format: str
    reason: str


def classify_content_format(
    *,
    duration_seconds: int | str | None,
    upload_date: datetime | str | None = None,
    title: str | None = None,
    description: str | None = None,
    tags: Iterable[Any] | str | None = None,
) -> ContentFormatClassification:
    """Classify content as Shorts, videos, or unknown using a conservative rule."""
    duration = _coerce_int(duration_seconds)
    if duration is None or duration <= 0:
        return ContentFormatClassification(CONTENT_FORMAT_UNKNOWN, "missing_or_invalid_duration")

    if duration <= 60:
        return ContentFormatClassification(CONTENT_FORMAT_SHORTS, "duration_lte_60")

    if duration <= 180:
        uploaded_at = _coerce_datetime(upload_date)
        if uploaded_at is not None and uploaded_at >= SHORTS_CUTOFF_DATE:
            if has_shorts_marker(title=title, description=description, tags=tags):
                return ContentFormatClassification(
                    CONTENT_FORMAT_SHORTS,
                    "duration_lte_180_after_cutoff_with_shorts_marker",
                )
            return ContentFormatClassification(CONTENT_FORMAT_VIDEOS, "three_minute_candidate_without_shorts_marker")
        return ContentFormatClassification(CONTENT_FORMAT_VIDEOS, "three_minute_candidate_before_cutoff")

    return ContentFormatClassification(CONTENT_FORMAT_VIDEOS, "duration_gt_180")


def has_shorts_marker(*, title: str | None = None, description: str | None = None, tags: Iterable[Any] | str | None = None) -> bool:
    text = " ".join(part for part in [title or "", description or ""] if part)
    if re.search(r"(?<!\w)#\s*shorts?\b", text, flags=re.IGNORECASE):
        return True

    for tag in _iter_tags(tags):
        normalized = str(tag).strip().lower().lstrip("#")
        if normalized in {"short", "shorts", "ytshorts", "youtube shorts"}:
            return True
    return False


def resolve_content_format(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in CONTENT_FORMATS:
        return normalized
    return CONTENT_FORMAT_UNKNOWN


def _coerce_int(value: int | str | None) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _coerce_datetime(value: datetime | str | None) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _iter_tags(tags: Iterable[Any] | str | None) -> Iterable[Any]:
    if tags is None:
        return []
    if isinstance(tags, str):
        stripped = tags.strip()
        if not stripped:
            return []
        if stripped.startswith("["):
            try:
                import json

                loaded = json.loads(stripped)
            except json.JSONDecodeError:
                return [stripped]
            return loaded if isinstance(loaded, list) else [stripped]
        return [part.strip() for part in re.split(r"[|,]", stripped) if part.strip()]
    return tags
