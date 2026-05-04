"""Run transcription for queued videos using local audio sources and OpenAI STT."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
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


def _candidate_audio_paths(audio_source_dir: Path, video_id: str) -> list[str]:
    return [str(audio_source_dir / f"{video_id}{ext}") for ext in AUDIO_EXTENSIONS]


def _youtube_watch_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def _download_audio_with_ytdlp(*, video_id: str, audio_source_dir: Path) -> tuple[Path | None, str | None]:
    ytdlp_bin = shutil.which("yt-dlp")
    if not ytdlp_bin:
        return None, "yt_dlp_not_installed"
    audio_source_dir.mkdir(parents=True, exist_ok=True)
    output_template = str(audio_source_dir / f"{video_id}.%(ext)s")
    cmd = [
        ytdlp_bin,
        "-x",
        "--audio-format",
        "mp3",
        "-o",
        output_template,
        _youtube_watch_url(video_id),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        stderr_tail = (result.stderr or "").strip()[-300:]
        return None, f"yt_dlp_failed:code={result.returncode};stderr={stderr_tail}"
    audio_path = _find_audio_source(audio_source_dir, video_id)
    if audio_path is None:
        return None, "yt_dlp_completed_but_audio_not_found"
    return audio_path, None


def transcribe_selected_videos(
    *,
    data_dir: str | Path = "data",
    limit: int = 10,
    audio_source_dir: str | Path = "data/audio_sources",
    model: str = "gpt-4o-mini-transcribe",
    openai_client: OpenAIAudioClient | None = None,
    allow_ytdlp_fallback: bool = True,
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
    missing_audio_video_ids: list[str] = []
    missing_audio_details: list[dict[str, Any]] = []
    already_transcribed_video_ids: list[str] = []
    success_video_ids: list[str] = []
    failed_video_ids: list[str] = []
    failed_details: list[dict[str, Any]] = []
    ytdlp_download_attempts = 0
    ytdlp_download_success = 0
    ytdlp_download_failures: list[dict[str, Any]] = []
    registry_success_before_run = len(success_ids)

    source_root = Path(audio_source_dir)
    source_root_exists = source_root.exists()
    source_root_is_dir = source_root.is_dir()
    available_audio_files_sample = sorted([p.name for p in source_root.glob("*") if p.is_file()])[:50] if source_root_is_dir else []
    for row in queued:
        if processed >= max(0, limit):
            break
        video_id = str(row.get("video_id", "")).strip()
        if not video_id:
            continue

        if video_id in success_ids:
            skipped_already_transcribed += 1
            already_transcribed_video_ids.append(video_id)
            continue

        processed += 1
        audio_path = _find_audio_source(source_root, video_id)
        if audio_path is None:
            ytdlp_error: str | None = None
            if allow_ytdlp_fallback:
                ytdlp_download_attempts += 1
                audio_path, ytdlp_error = _download_audio_with_ytdlp(video_id=video_id, audio_source_dir=source_root)
                if audio_path is not None:
                    ytdlp_download_success += 1
            if audio_path is None:
                if ytdlp_error:
                    ytdlp_download_failures.append({"video_id": video_id, "error": ytdlp_error, "video_url": _youtube_watch_url(video_id)})
                skipped_no_audio_source += 1
                missing_audio_video_ids.append(video_id)
                attempted_paths = _candidate_audio_paths(source_root, video_id)
                missing_audio_details.append(
                    {
                        "video_id": video_id,
                        "audio_source_dir": str(source_root),
                        "audio_source_dir_exists": source_root_exists,
                        "audio_source_dir_is_dir": source_root_is_dir,
                        "video_url": _youtube_watch_url(video_id),
                        "attempted_paths": attempted_paths,
                        "ytdlp_error": ytdlp_error,
                    }
                )
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
                        "error_message": f"audio_source_not_found; video_url={_youtube_watch_url(video_id)}; attempted={attempted_paths}; ytdlp={ytdlp_error}",
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
            success_video_ids.append(video_id)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            failed_video_ids.append(video_id)
            warnings.append(f"transcription_failed:{video_id}:{type(exc).__name__}")
            failed_details.append(
                {
                    "video_id": video_id,
                    "audio_path": str(audio_path),
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                }
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
        "queue_total": len(queued),
        "registry_success_before_run": registry_success_before_run,
        "audio_source_dir": str(source_root),
        "audio_source_dir_exists": source_root_exists,
        "audio_source_dir_is_dir": source_root_is_dir,
        "audio_source_files_sample": available_audio_files_sample,
        "allow_ytdlp_fallback": allow_ytdlp_fallback,
        "ytdlp_download_attempts": ytdlp_download_attempts,
        "ytdlp_download_success": ytdlp_download_success,
        "ytdlp_download_failures": ytdlp_download_failures,
        "processed_video_ids": success_video_ids + missing_audio_video_ids + failed_video_ids,
        "success_video_ids": success_video_ids,
        "already_transcribed_video_ids": already_transcribed_video_ids,
        "missing_audio_video_ids": missing_audio_video_ids,
        "missing_audio_details": missing_audio_details,
        "failed_video_ids": failed_video_ids,
        "failed_details": failed_details,
        "warnings": warnings,
    }
    (transcript_dir / "transcription_run_report.json").parent.mkdir(parents=True, exist_ok=True)
    (transcript_dir / "transcription_run_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report
