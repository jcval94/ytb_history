"""Backfill timestamp segments for existing local transcript artifacts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ytb_history.clients.openai_audio_client import OpenAIAudioClient
from ytb_history.services.transcript_store_service import (
    TRANSCRIPT_SEGMENTS_FILENAME,
    update_transcript_metadata_with_timestamps,
    update_transcript_registry_timestamp_metadata,
    write_transcript_segments,
)
from ytb_history.utils.environment import resolve_environment_variable

AUDIO_EXTENSIONS = [".mp3", ".m4a", ".wav", ".webm", ".mp4"]
ProgressCallback = Callable[[int, str], None]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _emit_progress(progress_callback: ProgressCallback | None, percent: int, message: str) -> None:
    if progress_callback is not None:
        progress_callback(max(0, min(100, percent)), message)


def _transcript_root(data_dir: str | Path) -> Path:
    return Path(data_dir) / "transcripts"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}


def _write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _resolve_metadata_audio_path(metadata: dict[str, Any], data_dir: Path) -> Path | None:
    raw = str(metadata.get("source_uri_or_path", "") or "").strip()
    if not raw:
        return None
    candidate = Path(raw)
    if candidate.is_file():
        return candidate
    if not candidate.is_absolute():
        relative_candidate = data_dir / candidate
        if relative_candidate.is_file():
            return relative_candidate
    return None


def _resolve_audio_path(*, video_id: str, metadata: dict[str, Any], data_dir: Path, audio_source_dir: Path) -> Path | None:
    metadata_audio_path = _resolve_metadata_audio_path(metadata, data_dir)
    if metadata_audio_path is not None:
        return metadata_audio_path
    for extension in AUDIO_EXTENSIONS:
        candidate = audio_source_dir / f"{video_id}{extension}"
        if candidate.is_file():
            return candidate
    return None


def _normalize_segments(response: dict[str, Any], *, video_id: str, model: str) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    raw_segments = response.get("segments", [])
    if not isinstance(raw_segments, list):
        return normalized
    for segment in raw_segments:
        if not isinstance(segment, dict):
            continue
        text = str(segment.get("text", "") or "").strip()
        start = float(segment.get("start", 0.0) or 0.0)
        end = float(segment.get("end", start) or start)
        normalized.append(
            {
                "video_id": video_id,
                "start_seconds": start,
                "end_seconds": end,
                "text": text,
                "transcription_model": model,
            }
        )
    return normalized


def _duration_from_response(response: dict[str, Any], segments: list[dict[str, Any]]) -> float | None:
    raw_duration = response.get("duration")
    if raw_duration is not None:
        try:
            return float(raw_duration)
        except (TypeError, ValueError):
            pass
    if not segments:
        return None
    return max(float(segment.get("end_seconds", 0.0) or 0.0) for segment in segments)


def _candidate_video_dirs(data_dir: Path) -> list[Path]:
    videos_root = _transcript_root(data_dir) / "videos"
    if not videos_root.exists():
        return []
    return sorted(
        [
            path
            for path in videos_root.iterdir()
            if path.is_dir()
            and (path / "transcript.txt").is_file()
            and (path / "transcript_metadata.json").is_file()
        ],
        key=lambda path: path.name,
    )


def backfill_transcript_timestamps(
    *,
    data_dir: str | Path = "data",
    limit: int = 10,
    audio_source_dir: str | Path = "data/audio_sources",
    model: str = "whisper-1",
    force: bool = False,
    openai_client: OpenAIAudioClient | None = None,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    root = Path(data_dir)
    transcript_root = _transcript_root(root)
    report_path = transcript_root / "transcript_timestamp_backfill_report.json"
    generated_at = _now_iso()
    report: dict[str, Any] = {
        "generated_at": generated_at,
        "status": "success",
        "data_dir": str(root),
        "audio_source_dir": str(audio_source_dir),
        "model": model,
        "force": force,
        "limit": limit,
        "processed": 0,
        "generated": 0,
        "skipped_existing_segments": 0,
        "skipped_missing_audio": 0,
        "failed": 0,
        "success_video_ids": [],
        "failed_details": [],
        "warnings": [],
    }

    api_key = resolve_environment_variable("OPENAI_API_KEY")
    if not api_key:
        report["status"] = "skipped"
        report["warnings"].append("skipped_missing_api_key")
        _write_report(report_path, report)
        _emit_progress(progress_callback, 100, "No se encontro OPENAI_API_KEY; se omite el backfill de timestamps.")
        return report

    client = openai_client or OpenAIAudioClient(api_key=api_key)
    effective_audio_source_dir = Path(audio_source_dir)
    candidates = _candidate_video_dirs(root)
    max_to_process = max(0, int(limit))
    _emit_progress(progress_callback, 5, f"Backfill de timestamps: {len(candidates)} transcripciones locales detectadas.")

    for video_dir in candidates:
        segments_path = video_dir / TRANSCRIPT_SEGMENTS_FILENAME
        if segments_path.exists() and not force:
            report["skipped_existing_segments"] += 1
            continue
        if report["processed"] >= max_to_process:
            break

        video_id = video_dir.name
        report["processed"] += 1
        percent = 10 + int(((report["processed"] - 1) / max(max_to_process, 1)) * 80)
        _emit_progress(progress_callback, percent, f"Video {report['processed']}: generando segmentos para {video_id}.")
        metadata_path = video_dir / "transcript_metadata.json"
        metadata = _read_json(metadata_path)
        audio_path = _resolve_audio_path(
            video_id=video_id,
            metadata=metadata,
            data_dir=root,
            audio_source_dir=effective_audio_source_dir,
        )
        if audio_path is None:
            report["skipped_missing_audio"] += 1
            report["failed_details"].append(
                {
                    "video_id": video_id,
                    "error_category": "missing_audio",
                    "error_message": "local_audio_not_found",
                }
            )
            continue

        try:
            response = client.transcribe_file_with_segments(file_path=audio_path, model=model)
            segments = _normalize_segments(response, video_id=video_id, model=model)
            written_segments_path = write_transcript_segments(
                video_id=video_id,
                segments=segments,
                data_dir=root,
            )
            timestamps_generated_at = _now_iso()
            update_transcript_metadata_with_timestamps(
                video_id=video_id,
                data_dir=root,
                segments_path=written_segments_path,
                segment_count=len(segments),
                timestamp_granularity="segment",
                timestamp_model=model,
                timestamps_generated_at=timestamps_generated_at,
                duration_seconds=_duration_from_response(response, segments),
            )
            update_transcript_registry_timestamp_metadata(
                data_dir=root,
                video_id=video_id,
                segments_path=written_segments_path,
                segment_count=len(segments),
                timestamp_granularity="segment",
                timestamp_model=model,
                timestamps_generated_at=timestamps_generated_at,
            )
            report["generated"] += 1
            report["success_video_ids"].append(video_id)
        except Exception as exc:  # noqa: BLE001
            report["failed"] += 1
            report["failed_details"].append(
                {
                    "video_id": video_id,
                    "audio_path": str(audio_path),
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                }
            )

    if report["failed"] or report["skipped_missing_audio"]:
        report["status"] = "partial_success" if report["generated"] else "failed"
    _emit_progress(progress_callback, 100, f"Backfill terminado: {report['generated']} videos con segmentos.")
    _write_report(report_path, report)
    return report
