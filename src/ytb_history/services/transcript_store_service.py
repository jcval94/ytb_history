"""Permanent transcript storage and auditable transcript registry."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

REGISTRY_FILENAME = "transcript_registry.jsonl"
TRANSCRIPTS_DIRNAME = "transcripts"
VIDEOS_DIRNAME = "videos"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _transcript_root(data_dir: str | Path) -> Path:
    return Path(data_dir) / TRANSCRIPTS_DIRNAME


def _video_dir(video_id: str, *, data_dir: str | Path = "data") -> Path:
    safe_video_id = video_id.strip()
    if not safe_video_id or "/" in safe_video_id or ".." in safe_video_id:
        raise ValueError("video_id inválido para ruta de storage")
    return _transcript_root(data_dir) / VIDEOS_DIRNAME / safe_video_id


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            rows.append(json.loads(text))
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_transcript_registry(data_dir: str | Path = "data") -> list[dict[str, Any]]:
    return _read_jsonl(_transcript_root(data_dir) / REGISTRY_FILENAME)


def list_transcribed_video_ids(*, data_dir: str | Path = "data") -> set[str]:
    success_ids = {
        str(row.get("video_id", "")).strip()
        for row in load_transcript_registry(data_dir=data_dir)
        if str(row.get("video_id", "")).strip() and str(row.get("status", "")).strip() == "success"
    }

    videos_root = _transcript_root(data_dir) / VIDEOS_DIRNAME
    if not videos_root.exists():
        return success_ids

    for video_dir in videos_root.iterdir():
        if not video_dir.is_dir():
            continue
        transcript_path = video_dir / "transcript.txt"
        metadata_path = video_dir / "transcript_metadata.json"
        if transcript_path.is_file() and metadata_path.is_file():
            success_ids.add(video_dir.name)
    return success_ids


def transcript_exists(video_id: str, *, data_dir: str | Path = "data") -> bool:
    return video_id.strip() in list_transcribed_video_ids(data_dir=data_dir)


def write_transcript_artifacts(
    *,
    video_id: str,
    transcript_text: str,
    metadata: dict[str, Any],
    insights: dict[str, Any] | None = None,
    data_dir: str | Path = "data",
) -> dict[str, str]:
    safe_video_id = video_id.strip()
    if not safe_video_id or "/" in safe_video_id or ".." in safe_video_id:
        raise ValueError("video_id inválido para ruta de storage")

    root = _transcript_root(data_dir)
    video_dir = root / VIDEOS_DIRNAME / safe_video_id
    video_dir.mkdir(parents=True, exist_ok=True)

    transcript_path = video_dir / "transcript.txt"
    metadata_path = video_dir / "transcript_metadata.json"
    insights_path = video_dir / "transcript_insights.json"

    transcript_path.write_text(transcript_text, encoding="utf-8")
    char_count = len(transcript_text)

    merged_metadata = {
        "video_id": safe_video_id,
        "channel_id": metadata.get("channel_id", ""),
        "channel_name": metadata.get("channel_name", ""),
        "title": metadata.get("title", ""),
        "source_type": metadata.get("source_type", "unknown"),
        "source_uri_or_path": metadata.get("source_uri_or_path", ""),
        "transcribed_at": metadata.get("transcribed_at") or _now_iso(),
        "transcription_model": metadata.get("transcription_model"),
        "language": metadata.get("language"),
        "text_char_count": char_count,
        "text_sha256": sha256(transcript_text.encode("utf-8")).hexdigest(),
        "repo_schema_version": "transcript_metadata_v1",
    }
    metadata_path.write_text(json.dumps(merged_metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    resolved_insights = insights or {
        "schema_version": "transcript_insights_v1",
        "status": "not_generated",
        "video_id": safe_video_id,
        "summary": None,
        "main_topics": [],
        "warnings": [],
    }
    insights_path.write_text(json.dumps(resolved_insights, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "transcript_path": str(transcript_path),
        "metadata_path": str(metadata_path),
        "insights_path": str(insights_path),
    }


def _is_transcript_failure_status(status: str) -> bool:
    return status == "failed" or status.startswith("failed_")


def update_transcript_registry(*, data_dir: str | Path = "data", entry: dict[str, Any]) -> dict[str, Any]:
    required = ["video_id", "status"]
    missing = [field for field in required if not entry.get(field)]
    if missing:
        raise ValueError(f"missing_required_fields:{','.join(missing)}")

    registry = load_transcript_registry(data_dir=data_dir)
    video_id = str(entry["video_id"]).strip()
    registry = [row for row in registry if str(row.get("video_id", "")).strip() != video_id]

    status = str(entry.get("status", "")).strip()
    failed_at = entry.get("failed_at")
    if _is_transcript_failure_status(status) and not failed_at:
        failed_at = _now_iso()

    normalized = {
        "video_id": video_id,
        "channel_id": entry.get("channel_id", ""),
        "channel_name": entry.get("channel_name", ""),
        "title": entry.get("title", ""),
        "selected_at": entry.get("selected_at"),
        "transcribed_at": entry.get("transcribed_at"),
        "failed_at": failed_at,
        "status": status,
        "transcript_path": entry.get("transcript_path"),
        "metadata_path": entry.get("metadata_path"),
        "insights_path": entry.get("insights_path"),
        "source_type": entry.get("source_type", "unknown"),
        "transcription_model": entry.get("transcription_model"),
        "language": entry.get("language"),
        "text_char_count": int(entry.get("text_char_count", 0) or 0),
        "error_category": entry.get("error_category"),
        "error_message": entry.get("error_message"),
    }

    registry.append(normalized)
    registry_path = _transcript_root(data_dir) / REGISTRY_FILENAME
    _write_jsonl(registry_path, registry)
    return normalized


def build_transcript_registry_report(*, data_dir: str | Path = "data") -> dict[str, Any]:
    rows = load_transcript_registry(data_dir=data_dir)
    by_status: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status", "unknown"))
        by_status[status] = by_status.get(status, 0) + 1

    report = {
        "generated_at": _now_iso(),
        "total_records": len(rows),
        "status_counts": by_status,
        "success_count": by_status.get("success", 0),
        "failed_count": by_status.get("failed", 0),
        "queued_count": by_status.get("queued", 0),
        "skipped_no_audio_source_count": by_status.get("skipped_no_audio_source", 0),
        "skipped_missing_ytdlp_count": by_status.get("skipped_missing_ytdlp", 0),
        "skipped_invalid_video_id_count": by_status.get("skipped_invalid_video_id", 0),
        "failed_audio_download_count": by_status.get("failed_audio_download", 0),
        "failed_audio_download_auth_required_count": by_status.get("failed_audio_download_auth_required", 0),
        "failed_audio_download_video_unavailable_count": by_status.get("failed_audio_download_video_unavailable", 0),
        "failed_audio_download_network_or_rate_limit_count": by_status.get("failed_audio_download_network_or_rate_limit", 0),
    }
    by_error_category: dict[str, int] = {}
    for row in rows:
        category = str(row.get("error_category", "")).strip()
        if category:
            by_error_category[category] = by_error_category.get(category, 0) + 1
    report["error_category_counts"] = by_error_category
    return report
