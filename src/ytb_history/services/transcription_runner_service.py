"""Run transcription for queued videos using local audio sources and OpenAI STT."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ytb_history.clients.openai_audio_client import OpenAIAudioClient
from ytb_history.services.transcript_store_service import (
    load_transcript_registry,
    update_transcript_registry,
    write_transcript_artifacts,
)

AUDIO_EXTENSIONS = [".mp3", ".m4a", ".wav", ".webm", ".mp4"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if text:
                rows.append(json.loads(text))
    return rows


def _find_audio_source(audio_source_dir: Path, video_id: str) -> Path | None:
    for ext in AUDIO_EXTENSIONS:
        candidate = audio_source_dir / f"{video_id}{ext}"
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def transcribe_selected_videos(
    *,
    data_dir: str | Path = "data",
    limit: int = 10,
    audio_source_dir: str | Path = "data/audio_sources",
    model: str = "gpt-4o-mini-transcribe",
    openai_client: OpenAIAudioClient | None = None,
) -> dict[str, Any]:
    root = Path(data_dir)
    transcript_dir = root / "transcripts"
    queue_path = transcript_dir / "transcript_queue.jsonl"

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        report = {
            "generated_at": _now_iso(),
            "limit": limit,
            "processed": 0,
            "transcribed_success": 0,
            "skipped_no_audio_source": 0,
            "skipped_already_transcribed": 0,
            "failed": 0,
            "warnings": ["skipped_missing_api_key"],
        }
        (transcript_dir / "transcription_run_report.json").parent.mkdir(parents=True, exist_ok=True)
        (transcript_dir / "transcription_run_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return report

    client = openai_client or OpenAIAudioClient(api_key=api_key)
    queued = _read_jsonl(queue_path)
    registry = load_transcript_registry(data_dir=data_dir)
    success_ids = {
        str(row.get("video_id", "")).strip()
        for row in registry
        if str(row.get("status", "")).strip() == "success"
    }

    processed = 0
    transcribed_success = 0
    skipped_no_audio_source = 0
    skipped_already_transcribed = 0
    failed = 0
    warnings: list[str] = []

    source_root = Path(audio_source_dir)
    for row in queued:
        if processed >= max(0, limit):
            break
        video_id = str(row.get("video_id", "")).strip()
        if not video_id:
            continue

        if video_id in success_ids:
            skipped_already_transcribed += 1
            continue

        processed += 1
        audio_path = _find_audio_source(source_root, video_id)
        if audio_path is None:
            skipped_no_audio_source += 1
            update_transcript_registry(
                data_dir=data_dir,
                entry={
                    "video_id": video_id,
                    "channel_id": row.get("channel_id", ""),
                    "channel_name": row.get("channel_name", ""),
                    "title": row.get("title", ""),
                    "selected_at": row.get("selected_at"),
                    "transcribed_at": None,
                    "status": "skipped_no_audio_source",
                    "transcript_path": None,
                    "metadata_path": None,
                    "insights_path": None,
                    "source_type": "unknown",
                    "text_char_count": 0,
                    "error_message": "audio_source_not_found",
                },
            )
            continue

        try:
            transcript_text = client.transcribe_file(file_path=audio_path, model=model)
            artifacts = write_transcript_artifacts(
                video_id=video_id,
                transcript_text=transcript_text,
                metadata={
                    "channel_id": row.get("channel_id", ""),
                    "channel_name": row.get("channel_name", ""),
                    "title": row.get("title", ""),
                    "source_type": "audio_file",
                    "source_uri_or_path": str(audio_path),
                    "transcribed_at": _now_iso(),
                    "transcription_model": model,
                    "language": None,
                },
                data_dir=data_dir,
            )
            update_transcript_registry(
                data_dir=data_dir,
                entry={
                    "video_id": video_id,
                    "channel_id": row.get("channel_id", ""),
                    "channel_name": row.get("channel_name", ""),
                    "title": row.get("title", ""),
                    "selected_at": row.get("selected_at"),
                    "transcribed_at": _now_iso(),
                    "status": "success",
                    "transcript_path": artifacts["transcript_path"],
                    "metadata_path": artifacts["metadata_path"],
                    "insights_path": artifacts["insights_path"],
                    "source_type": "audio_file",
                    "transcription_model": model,
                    "language": None,
                    "text_char_count": len(transcript_text),
                    "error_message": None,
                },
            )
            transcribed_success += 1
            success_ids.add(video_id)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            warnings.append(f"transcription_failed:{video_id}")
            update_transcript_registry(
                data_dir=data_dir,
                entry={
                    "video_id": video_id,
                    "channel_id": row.get("channel_id", ""),
                    "channel_name": row.get("channel_name", ""),
                    "title": row.get("title", ""),
                    "selected_at": row.get("selected_at"),
                    "transcribed_at": _now_iso(),
                    "status": "failed",
                    "transcript_path": None,
                    "metadata_path": None,
                    "insights_path": None,
                    "source_type": "audio_file",
                    "transcription_model": model,
                    "language": None,
                    "text_char_count": 0,
                    "error_message": str(exc),
                },
            )

    report = {
        "generated_at": _now_iso(),
        "limit": limit,
        "processed": processed,
        "transcribed_success": transcribed_success,
        "skipped_no_audio_source": skipped_no_audio_source,
        "skipped_already_transcribed": skipped_already_transcribed,
        "failed": failed,
        "warnings": warnings,
    }
    (transcript_dir / "transcription_run_report.json").parent.mkdir(parents=True, exist_ok=True)
    (transcript_dir / "transcription_run_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report
