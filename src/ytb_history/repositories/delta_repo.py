"""Delta repository for immutable historical deltas."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ytb_history.domain.models import VideoDelta
from ytb_history.storage.jsonl import read_jsonl_gz, write_jsonl_gz
from ytb_history.storage.partitioning import delta_path_for_run, list_delta_files
from ytb_history.utils.dates import parse_iso8601_utc


class DeltaRepo:
    def __init__(self, base_dir: str | Path = "data/deltas") -> None:
        self._base_dir = Path(base_dir)

    def save_for_run(self, execution_date: datetime, deltas: list[VideoDelta]) -> Path:
        path = delta_path_for_run(execution_date, base_dir=self._base_dir)
        if path.exists():
            raise FileExistsError(f"Delta file already exists for run: {path}")
        write_jsonl_gz(path, [delta.to_dict() for delta in deltas])
        return path

    def load_from_path(self, path: str | Path) -> list[VideoDelta]:
        rows = read_jsonl_gz(path)
        deltas: list[VideoDelta] = []
        for row in rows:
            execution_date = parse_iso8601_utc(row.get("execution_date") if isinstance(row.get("execution_date"), str) else None)
            if execution_date is None:
                continue

            deltas.append(
                VideoDelta(
                    execution_date=execution_date,
                    video_id=str(row.get("video_id", "")),
                    views_delta=self._safe_int_or_none(row.get("views_delta")),
                    likes_delta=self._safe_int_or_none(row.get("likes_delta")),
                    comments_delta=self._safe_int_or_none(row.get("comments_delta")),
                    previous_views=self._safe_int_or_none(row.get("previous_views")),
                    current_views=self._safe_int_or_none(row.get("current_views")),
                    previous_likes=self._safe_int_or_none(row.get("previous_likes")),
                    current_likes=self._safe_int_or_none(row.get("current_likes")),
                    previous_comments=self._safe_int_or_none(row.get("previous_comments")),
                    current_comments=self._safe_int_or_none(row.get("current_comments")),
                    is_new_video=bool(row.get("is_new_video", False)),
                    title_changed=bool(row.get("title_changed", False)),
                    description_changed=bool(row.get("description_changed", False)),
                    tags_changed=bool(row.get("tags_changed", False)),
                )
            )
        return deltas

    def list_delta_files(self) -> list[Path]:
        return list_delta_files(base_dir=self._base_dir)

    @staticmethod
    def _safe_int_or_none(value: object) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
