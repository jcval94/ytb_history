"""Video catalog repository."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ytb_history.domain.models import TrackedVideoRecord
from ytb_history.storage.jsonl import read_jsonl, write_jsonl

DEFAULT_VIDEO_CATALOG_PATH = Path("data/state/tracked_videos_catalog.jsonl")


class VideoCatalogRepo:
    def __init__(self, path: str | Path = DEFAULT_VIDEO_CATALOG_PATH) -> None:
        self._path = Path(path)

    def load(self) -> list[TrackedVideoRecord]:
        rows = read_jsonl(self._path)
        records: list[TrackedVideoRecord] = []
        for row in rows:
            records.append(
                TrackedVideoRecord(
                    video_id=row.get("video_id", ""),
                    channel_id=row.get("channel_id", ""),
                    channel_name=row.get("channel_name", ""),
                    first_seen_date=datetime.fromisoformat(row["first_seen_date"]),
                    last_seen_execution_date=datetime.fromisoformat(row["last_seen_execution_date"]),
                    tracking_until_date=datetime.fromisoformat(row["tracking_until_date"]),
                    active=bool(row.get("active", True)),
                )
            )
        return self._deduplicate(records)

    def save(self, records: list[TrackedVideoRecord]) -> None:
        deduped = self._deduplicate(records)
        write_jsonl(self._path, [record.to_dict() for record in deduped])

    def upsert(self, records: list[TrackedVideoRecord]) -> list[TrackedVideoRecord]:
        merged = self.load() + records
        deduped = self._deduplicate(merged)
        self.save(deduped)
        return deduped

    @staticmethod
    def _deduplicate(records: list[TrackedVideoRecord]) -> list[TrackedVideoRecord]:
        by_video_id: dict[str, TrackedVideoRecord] = {}
        for record in records:
            if not record.video_id:
                continue
            current = by_video_id.get(record.video_id)
            if current is None or record.last_seen_execution_date >= current.last_seen_execution_date:
                by_video_id[record.video_id] = record
        return sorted(by_video_id.values(), key=lambda item: item.first_seen_date)


def load_video_catalog(path: str | Path = DEFAULT_VIDEO_CATALOG_PATH) -> list[TrackedVideoRecord]:
    return VideoCatalogRepo(path).load()
