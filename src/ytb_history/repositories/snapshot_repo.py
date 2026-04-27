"""Snapshot repository for immutable historical snapshots."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from ytb_history.domain.models import VideoSnapshot
from ytb_history.storage.jsonl import read_jsonl_gz, write_jsonl_gz
from ytb_history.storage.partitioning import (
    extract_execution_date_from_snapshot_path,
    list_snapshot_files,
    snapshot_path_for_run,
)
from ytb_history.utils.dates import parse_iso8601_utc


class SnapshotRepo:
    def __init__(self, base_dir: str | Path = "data/snapshots") -> None:
        self._base_dir = Path(base_dir)

    def save_for_run(self, execution_date: datetime, snapshots: list[VideoSnapshot]) -> Path:
        path = snapshot_path_for_run(execution_date, base_dir=self._base_dir)
        if path.exists():
            raise FileExistsError(f"Snapshot already exists for run: {path}")
        write_jsonl_gz(path, [snapshot.to_dict() for snapshot in snapshots])
        return path

    def load_from_path(self, path: str | Path) -> list[VideoSnapshot]:
        rows = read_jsonl_gz(path)
        snapshots: list[VideoSnapshot] = []
        for row in rows:
            execution_date = self._safe_datetime(row.get("execution_date"))
            upload_date = self._safe_datetime(row.get("upload_date"))
            if execution_date is None or upload_date is None:
                continue

            raw_tags = row.get("tags", [])
            tags = raw_tags if isinstance(raw_tags, list) else []
            snapshots.append(
                VideoSnapshot(
                    execution_date=execution_date,
                    channel_id=str(row.get("channel_id", "")),
                    channel_name=str(row.get("channel_name", "")),
                    video_id=str(row.get("video_id", "")),
                    title=str(row.get("title", "")),
                    description=str(row.get("description", "")),
                    upload_date=upload_date,
                    tags=[str(tag) for tag in tags],
                    thumbnail_url=str(row.get("thumbnail_url", "")),
                    duration_seconds=self._safe_int(row.get("duration_seconds"), default=0),
                    views=self._safe_int_or_none(row.get("views")),
                    likes=self._safe_int_or_none(row.get("likes")),
                    comments=self._safe_int_or_none(row.get("comments")),
                )
            )
        return snapshots

    def list_snapshot_files(self) -> list[Path]:
        return list_snapshot_files(base_dir=self._base_dir)

    def latest_path_before(self, execution_date: datetime) -> Path | None:
        target = self._as_utc(execution_date)
        candidates: list[tuple[datetime, Path]] = []
        for path in self.list_snapshot_files():
            path_date = extract_execution_date_from_snapshot_path(path)
            if path_date is None:
                continue
            if path_date < target:
                candidates.append((path_date, path))

        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1]))
        return candidates[-1][1]

    def load_latest_before(self, execution_date: datetime) -> list[VideoSnapshot]:
        latest_path = self.latest_path_before(execution_date)
        if latest_path is None:
            return []
        return self.load_from_path(latest_path)

    @staticmethod
    def _safe_datetime(value: object) -> datetime | None:
        parsed = parse_iso8601_utc(value if isinstance(value, str) else None)
        return parsed

    @staticmethod
    def _safe_int(value: object, *, default: int) -> int:
        if value is None:
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_int_or_none(value: object) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_utc(execution_date: datetime) -> datetime:
        if execution_date.tzinfo is None:
            return execution_date.replace(tzinfo=timezone.utc)
        return execution_date.astimezone(timezone.utc)
