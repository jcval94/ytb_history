"""Run transcription for queued videos using local media and safe fallbacks."""

from __future__ import annotations

import base64
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from ytb_history.clients.openai_audio_client import OpenAIAudioClient
from ytb_history.services.transcript_store_service import (
    list_transcribed_video_ids,
    load_transcript_registry,
    update_transcript_registry,
    write_transcript_artifacts,
)
from ytb_history.utils.environment import resolve_environment_variable
from ytb_history.utils.video_ids import is_transcribable_video_id_candidate

AUDIO_EXTENSIONS = [".mp3", ".m4a", ".wav", ".webm", ".ogg", ".opus", ".flac", ".mp4"]
VIDEO_EXTENSIONS = [".mp4", ".webm", ".mkv", ".mov"]

CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _make_local_temp_dir(parent: Path, prefix: str) -> Path:
    parent.mkdir(parents=True, exist_ok=True)
    for _ in range(100):
        candidate = parent / f"{prefix}{uuid.uuid4().hex}"
        try:
            candidate.mkdir()
            return candidate
        except FileExistsError:
            continue
    raise RuntimeError(f"could_not_create_temp_dir:{parent}")


def _safe_rmtree(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


def _find_media_source(source_dir: Path, video_id: str, extensions: Sequence[str]) -> Path | None:
    for ext in extensions:
        candidate = source_dir / f"{video_id}{ext}"
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _find_audio_source(audio_source_dir: Path, video_id: str) -> Path | None:
    return _find_media_source(audio_source_dir, video_id, AUDIO_EXTENSIONS)


def _candidate_media_paths(source_dir: Path, video_id: str, extensions: Sequence[str]) -> list[str]:
    return [str(source_dir / f"{video_id}{ext}") for ext in extensions]


def _select_queue_rows(
    queued: list[dict[str, Any]],
    *,
    limit: int,
    include_forced: bool,
    ranked_limit: int | None,
) -> list[dict[str, Any]]:
    if not include_forced:
        return queued[: max(0, limit)]

    effective_ranked_limit = max(0, ranked_limit if ranked_limit is not None else limit)
    forced_rows = [
        row
        for row in queued
        if bool(row.get("forced_channel")) or row.get("selection_source") == "forced_channel_new_video"
    ]
    ranked_rows = [
        row
        for row in queued
        if not (bool(row.get("forced_channel")) or row.get("selection_source") == "forced_channel_new_video")
    ]
    return forced_rows + ranked_rows[:effective_ranked_limit]


def _sample_files(source_root: Path) -> tuple[bool, list[str]]:
    source_root_is_dir = source_root.is_dir()
    files = sorted([p.name for p in source_root.glob("*") if p.is_file()])[:50] if source_root_is_dir else []
    return source_root_is_dir, files


def _base_report(
    *,
    generated_at: str,
    limit: int,
    ranked_limit: int | None,
    include_forced: bool,
    dry_run: bool,
    queue_total: int,
    audio_source_root: Path,
    video_source_root: Path,
    allow_ytdlp_fallback: bool,
    segment_large_audio: bool,
    warnings: list[str],
) -> dict[str, Any]:
    audio_is_dir, audio_files_sample = _sample_files(audio_source_root)
    video_is_dir, video_files_sample = _sample_files(video_source_root)
    return {
        "generated_at": generated_at,
        "status": "success",
        "limit": limit,
        "ranked_limit": ranked_limit,
        "include_forced": include_forced,
        "dry_run": dry_run,
        "queue_total": queue_total,
        "processed": 0,
        "transcribed_success": 0,
        "skipped_no_audio_source": 0,
        "skipped_already_transcribed": 0,
        "skipped_missing_api_key": 0,
        "skipped_invalid_video_id": 0,
        "failed": 0,
        "audio_source_dir": str(audio_source_root),
        "audio_source_dir_exists": audio_source_root.exists(),
        "audio_source_dir_is_dir": audio_is_dir,
        "audio_source_files_sample": audio_files_sample,
        "video_source_dir": str(video_source_root),
        "video_source_dir_exists": video_source_root.exists(),
        "video_source_dir_is_dir": video_is_dir,
        "video_source_files_sample": video_files_sample,
        "allow_ytdlp_fallback": allow_ytdlp_fallback,
        "segment_large_audio": segment_large_audio,
        "extracted_audio_from_video": 0,
        "downloaded_audio_with_ytdlp": 0,
        "segmented_audio_transcriptions": 0,
        "processed_video_ids": [],
        "success_video_ids": [],
        "already_transcribed_video_ids": [],
        "missing_audio_video_ids": [],
        "missing_audio_details": [],
        "invalid_video_id_details": [],
        "failed_video_ids": [],
        "failed_details": [],
        "media_resolution_details": [],
        "warnings": warnings,
    }


def _command_to_report(command: Sequence[str], result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    return {
        "command": list(command),
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "ok": result.returncode == 0,
    }


def _resolve_ytdlp_command() -> list[str]:
    executable = shutil.which("yt-dlp")
    if executable:
        return [executable]
    if importlib.util.find_spec("yt_dlp") is not None:
        return [sys.executable, "-m", "yt_dlp"]
    return []


def _resolve_ffmpeg_executable() -> str | None:
    executable = shutil.which("ffmpeg")
    if executable:
        return executable
    try:
        import imageio_ffmpeg  # type: ignore[import-not-found]

        return str(imageio_ffmpeg.get_ffmpeg_exe())
    except Exception:  # noqa: BLE001
        return None


def _classify_ytdlp_error(stdout: str = "", stderr: str = "") -> str:
    text = f"{stdout}\n{stderr}".lower()
    if any(marker in text for marker in ["sign in", "not a bot", "cookies", "authentication", "login required"]):
        return "auth_required"
    if any(marker in text for marker in ["private video", "members-only", "this video is unavailable", "video unavailable", "removed"]):
        return "unavailable"
    if any(marker in text for marker in ["timed out", "timeout", "connection", "network", "temporary failure", "http error 5"]):
        return "network"
    if any(marker in text for marker in ["not recognized", "no module named yt_dlp", "no such file", "not found"]):
        return "tooling"
    return "download_failed"


def _inspect_ytdlp_cookies_file(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {"provided": False}
    return {
        "provided": True,
        "exists": path.exists(),
        "is_file": path.is_file(),
        "size_bytes": path.stat().st_size if path.exists() and path.is_file() else 0,
    }


def _materialize_cookies_file_from_b64(cookies_b64: str | None, work_dir: Path) -> tuple[Path | None, str | None]:
    if not cookies_b64:
        return None, None
    try:
        decoded = base64.b64decode(cookies_b64, validate=True)
    except Exception as exc:  # noqa: BLE001
        return None, f"invalid_ytdlp_cookies_b64:{type(exc).__name__}"
    path = work_dir / "yt_dlp_cookies.txt"
    path.write_bytes(decoded)
    return path, None


def _build_ytdlp_strategy(
    command: Sequence[str],
    *,
    url: str,
    output_template: str,
    cookies_file: Path | None,
    browser: str | None,
    extra_args: Sequence[str],
) -> list[list[str]]:
    strategies: list[list[str]] = []
    common = [
        "--no-playlist",
        "--extract-audio",
        "--audio-format",
        "mp3",
        "--audio-quality",
        "0",
        "-o",
        output_template,
    ]
    if cookies_file is not None:
        strategies.append(list(command) + ["--cookies", str(cookies_file), *extra_args, *common, url])
    if browser:
        strategies.append(list(command) + ["--cookies-from-browser", browser, *extra_args, *common, url])
    strategies.append(list(command) + [*extra_args, *common, url])
    return strategies


def _download_audio_with_ytdlp(
    *,
    video_id: str,
    audio_source_dir: Path,
    ytdlp_cookies_file: str | Path | None,
    ytdlp_browser: str | None,
    ytdlp_extra_args: Sequence[str] | None,
    ytdlp_cookies_b64: str | None,
    command_runner: CommandRunner,
) -> dict[str, Any]:
    command = _resolve_ytdlp_command()
    if not command:
        return {"ok": False, "error_category": "tooling", "error_message": "yt-dlp_not_available", "attempts": []}

    audio_source_dir.mkdir(parents=True, exist_ok=True)
    output_template = str(audio_source_dir / f"{video_id}.%(ext)s")
    url = f"https://www.youtube.com/watch?v={video_id}"
    attempts: list[dict[str, Any]] = []
    extra_args = list(ytdlp_extra_args or [])

    temp_root = _make_local_temp_dir(audio_source_dir / ".tmp", "yt_dlp_")
    try:
        materialized_cookies, cookies_warning = _materialize_cookies_file_from_b64(
            ytdlp_cookies_b64 or os.environ.get("YTDLP_COOKIES_B64", ""),
            temp_root,
        )
        cookies_path = Path(ytdlp_cookies_file) if ytdlp_cookies_file else materialized_cookies
        strategies = _build_ytdlp_strategy(
            command,
            url=url,
            output_template=output_template,
            cookies_file=cookies_path,
            browser=ytdlp_browser,
            extra_args=extra_args,
        )
        last_category = "download_failed"
        last_message = cookies_warning or ""
        for strategy in strategies:
            result = command_runner(strategy, capture_output=True, text=True, check=False)
            attempt = _command_to_report(strategy, result)
            attempt["error_category"] = _classify_ytdlp_error(result.stdout, result.stderr)
            attempts.append(attempt)
            if result.returncode == 0:
                audio_path = _find_audio_source(audio_source_dir, video_id)
                if audio_path is not None:
                    return {
                        "ok": True,
                        "source_type": "yt_dlp_audio_download",
                        "audio_path": str(audio_path),
                        "attempts": attempts,
                        "cookies_file": _inspect_ytdlp_cookies_file(cookies_path),
                        "cookies_warning": cookies_warning,
                    }
                last_category = "download_missing_output"
                last_message = "yt-dlp_completed_without_expected_audio_file"
            else:
                last_category = str(attempt["error_category"])
                last_message = result.stderr or result.stdout
    finally:
        _safe_rmtree(temp_root)

    return {
        "ok": False,
        "error_category": last_category,
        "error_message": last_message,
        "attempts": attempts,
        "cookies_warning": cookies_warning,
    }


def _extract_audio_from_video(
    *,
    video_path: Path,
    audio_path: Path,
    command_runner: CommandRunner,
) -> dict[str, Any]:
    ffmpeg = _resolve_ffmpeg_executable()
    if not ffmpeg:
        return {"ok": False, "error_category": "tooling", "error_message": "ffmpeg_not_available"}
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        ffmpeg,
        "-y",
        "-i",
        str(video_path),
        "-vn",
        "-acodec",
        "libmp3lame",
        "-ar",
        "16000",
        "-ac",
        "1",
        str(audio_path),
    ]
    result = command_runner(command, capture_output=True, text=True, check=False)
    report = _command_to_report(command, result)
    report["output_path"] = str(audio_path)
    if result.returncode == 0 and audio_path.exists():
        report["source_type"] = "video_file_extracted_audio"
        return report
    report["ok"] = False
    report["error_category"] = "tooling" if result.returncode != 0 else "extract_missing_output"
    report["error_message"] = result.stderr or result.stdout or "ffmpeg_completed_without_expected_audio_file"
    return report


def _is_input_too_large_error(exc: Exception) -> bool:
    text = f"{type(exc).__name__}: {exc}".lower()
    return any(
        marker in text
        for marker in [
            "413",
            "too large",
            "maximum content size",
            "maximum file size",
            "request entity too large",
            "content_length_exceeded",
        ]
    )


def _split_audio_for_transcription(
    *,
    audio_path: Path,
    output_dir: Path,
    segment_seconds: int,
    command_runner: CommandRunner,
) -> dict[str, Any]:
    ffmpeg = _resolve_ffmpeg_executable()
    if not ffmpeg:
        return {"ok": False, "error_category": "tooling", "error_message": "ffmpeg_not_available", "segments": []}
    output_dir.mkdir(parents=True, exist_ok=True)
    output_pattern = output_dir / f"{audio_path.stem}_part_%03d.mp3"
    command = [
        ffmpeg,
        "-y",
        "-i",
        str(audio_path),
        "-f",
        "segment",
        "-segment_time",
        str(max(1, segment_seconds)),
        "-c",
        "copy",
        str(output_pattern),
    ]
    result = command_runner(command, capture_output=True, text=True, check=False)
    report = _command_to_report(command, result)
    segments = sorted(output_dir.glob(f"{audio_path.stem}_part_*.mp3"))
    report["segments"] = [str(path) for path in segments]
    if result.returncode == 0 and segments:
        return report
    report["ok"] = False
    report["error_category"] = "tooling" if result.returncode != 0 else "segment_missing_output"
    report["error_message"] = result.stderr or result.stdout or "ffmpeg_completed_without_segment_files"
    return report


def _transcribe_with_optional_segments(
    *,
    client: OpenAIAudioClient,
    audio_path: Path,
    model: str,
    segment_large_audio: bool,
    segment_seconds: int,
    command_runner: CommandRunner,
) -> tuple[str, dict[str, Any]]:
    try:
        return client.transcribe_file(file_path=audio_path, model=model), {"segmented": False}
    except Exception as exc:
        if not segment_large_audio or not _is_input_too_large_error(exc):
            raise
        temp_root = _make_local_temp_dir(audio_path.parent / ".tmp_segments", "segments_")
        try:
            split_report = _split_audio_for_transcription(
                audio_path=audio_path,
                output_dir=temp_root,
                segment_seconds=segment_seconds,
                command_runner=command_runner,
            )
            if not split_report.get("ok"):
                raise RuntimeError(f"audio_segmentation_failed:{split_report.get('error_message')}") from exc
            parts: list[str] = []
            for segment_path in split_report.get("segments", []):
                parts.append(client.transcribe_file(file_path=Path(segment_path), model=model))
            return "\n\n".join(parts), {"segmented": True, "split_report": split_report}
        finally:
            _safe_rmtree(temp_root)


def _record_registry_skip(
    *,
    data_dir: str | Path,
    row: dict[str, Any],
    video_id: str,
    status: str,
    source_type: str,
    error_category: str,
    error_message: str,
) -> None:
    update_transcript_registry(
        data_dir=data_dir,
        entry={
            "video_id": video_id,
            "channel_id": row.get("channel_id", ""),
            "channel_name": row.get("channel_name", ""),
            "title": row.get("title", ""),
            "selected_at": row.get("selected_at"),
            "transcribed_at": None,
            "status": status,
            "transcript_path": None,
            "metadata_path": None,
            "insights_path": None,
            "source_type": source_type,
            "text_char_count": 0,
            "error_category": error_category,
            "error_message": error_message,
        },
    )


def transcribe_selected_videos(
    *,
    data_dir: str | Path = "data",
    limit: int = 10,
    audio_source_dir: str | Path = "data/audio_sources",
    video_source_dir: str | Path = "data/video_sources",
    model: str = "gpt-4o-mini-transcribe",
    openai_client: OpenAIAudioClient | None = None,
    include_forced: bool = False,
    ranked_limit: int | None = None,
    dry_run: bool = False,
    allow_ytdlp_fallback: bool = True,
    ytdlp_cookies_file: str | Path | None = None,
    ytdlp_browser: str | None = None,
    ytdlp_extra_args: Sequence[str] | None = None,
    ytdlp_cookies_b64: str | None = None,
    segment_large_audio: bool = True,
    segment_seconds: int = 1200,
    command_runner: CommandRunner = subprocess.run,
) -> dict[str, Any]:
    root = Path(data_dir)
    transcript_dir = root / "transcripts"
    queue_path = transcript_dir / "transcript_queue.jsonl"
    audio_root = Path(audio_source_dir)
    video_root = Path(video_source_dir)
    queued = _read_jsonl(queue_path)
    selected_rows = _select_queue_rows(
        queued,
        limit=limit,
        include_forced=include_forced,
        ranked_limit=ranked_limit,
    )

    generated_at = _now_iso()
    warnings: list[str] = []
    report = _base_report(
        generated_at=generated_at,
        limit=limit,
        ranked_limit=ranked_limit,
        include_forced=include_forced,
        dry_run=dry_run,
        queue_total=len(queued),
        audio_source_root=audio_root,
        video_source_root=video_root,
        allow_ytdlp_fallback=allow_ytdlp_fallback,
        segment_large_audio=segment_large_audio,
        warnings=warnings,
    )
    report["selected_count"] = len(selected_rows)
    report["selected_forced_count"] = sum(1 for row in selected_rows if bool(row.get("forced_channel")))
    report["selected_ranked_count"] = len(selected_rows) - int(report["selected_forced_count"])

    api_key = resolve_environment_variable("OPENAI_API_KEY")
    if not dry_run and not api_key:
        report["status"] = "skipped_missing_api_key"
        report["skipped_missing_api_key"] = len(selected_rows)
        warnings.append("skipped_missing_api_key")
        _write_json(transcript_dir / "transcription_run_report.json", report)
        return report

    client = openai_client or (OpenAIAudioClient(api_key=api_key) if not dry_run else None)
    registry = load_transcript_registry(data_dir=data_dir)
    registry_success_before_run = {
        str(row.get("video_id", "")).strip()
        for row in registry
        if str(row.get("status", "")).strip() == "success"
    }
    success_ids = list_transcribed_video_ids(data_dir=data_dir)
    report["registry_success_before_run"] = len(registry_success_before_run)
    report["persisted_success_before_run"] = len(success_ids)

    for row in selected_rows:
        video_id = str(row.get("video_id", "")).strip()
        if not video_id:
            continue

        if video_id in success_ids:
            report["skipped_already_transcribed"] += 1
            report["already_transcribed_video_ids"].append(video_id)
            continue

        if not is_transcribable_video_id_candidate(video_id):
            report["processed"] += 1
            report["skipped_invalid_video_id"] += 1
            report["processed_video_ids"].append(video_id)
            detail = {"video_id": video_id, "reason": "probable_channel_id_in_video_id_field"}
            report["invalid_video_id_details"].append(detail)
            if not dry_run:
                _record_registry_skip(
                    data_dir=data_dir,
                    row=row,
                    video_id=video_id,
                    status="skipped_invalid_video_id",
                    source_type="unknown",
                    error_category="invalid_video_id",
                    error_message="probable channel_id found in video_id field",
                )
            continue

        audio_path = _find_audio_source(audio_root, video_id)
        source_type = "audio_file" if audio_path is not None else "unknown"
        media_resolution: dict[str, Any] = {"video_id": video_id, "steps": []}

        if audio_path is None and not dry_run:
            video_path = _find_media_source(video_root, video_id, VIDEO_EXTENSIONS)
            if video_path is not None:
                extracted_path = audio_root / f"{video_id}.mp3"
                extract_report = _extract_audio_from_video(
                    video_path=video_path,
                    audio_path=extracted_path,
                    command_runner=command_runner,
                )
                extract_report["source_video_path"] = str(video_path)
                media_resolution["steps"].append({"type": "extract_audio_from_video", **extract_report})
                if extract_report.get("ok"):
                    audio_path = extracted_path
                    source_type = "video_file_extracted_audio"
                    report["extracted_audio_from_video"] += 1
                else:
                    warnings.append(f"video_audio_extract_failed:{video_id}:{extract_report.get('error_category')}")

        if audio_path is None and not dry_run and allow_ytdlp_fallback:
            download_report = _download_audio_with_ytdlp(
                video_id=video_id,
                audio_source_dir=audio_root,
                ytdlp_cookies_file=ytdlp_cookies_file,
                ytdlp_browser=ytdlp_browser,
                ytdlp_extra_args=ytdlp_extra_args,
                ytdlp_cookies_b64=ytdlp_cookies_b64,
                command_runner=command_runner,
            )
            media_resolution["steps"].append({"type": "download_audio_with_ytdlp", **download_report})
            if download_report.get("ok"):
                audio_path = Path(str(download_report["audio_path"]))
                source_type = "yt_dlp_audio_download"
                report["downloaded_audio_with_ytdlp"] += 1
            else:
                warnings.append(f"ytdlp_download_failed:{video_id}:{download_report.get('error_category')}")

        if media_resolution["steps"]:
            report["media_resolution_details"].append(media_resolution)

        if audio_path is None:
            report["processed"] += 1
            report["skipped_no_audio_source"] += 1
            report["processed_video_ids"].append(video_id)
            report["missing_audio_video_ids"].append(video_id)
            attempted_audio_paths = _candidate_media_paths(audio_root, video_id, AUDIO_EXTENSIONS)
            attempted_video_paths = _candidate_media_paths(video_root, video_id, VIDEO_EXTENSIONS)
            resolution_error_category = "media_missing"
            resolution_error_message = "audio_or_video_source_not_found"
            for step in reversed(media_resolution["steps"]):
                if step.get("ok"):
                    continue
                resolution_error_category = str(step.get("error_category") or resolution_error_category)
                resolution_error_message = str(step.get("error_message") or resolution_error_message)
                break
            report["missing_audio_details"].append(
                {
                    "video_id": video_id,
                    "audio_source_dir": str(audio_root),
                    "video_source_dir": str(video_root),
                    "audio_source_dir_exists": audio_root.exists(),
                    "audio_source_dir_is_dir": audio_root.is_dir(),
                    "video_source_dir_exists": video_root.exists(),
                    "video_source_dir_is_dir": video_root.is_dir(),
                    "attempted_audio_paths": attempted_audio_paths,
                    "attempted_video_paths": attempted_video_paths,
                    "allow_ytdlp_fallback": allow_ytdlp_fallback,
                    "resolution_error_category": resolution_error_category,
                    "resolution_error_message": resolution_error_message,
                }
            )
            if not dry_run:
                _record_registry_skip(
                    data_dir=data_dir,
                    row=row,
                    video_id=video_id,
                    status="skipped_no_audio_source",
                    source_type=source_type,
                    error_category=resolution_error_category,
                    error_message=f"audio_source_not_found; attempted_audio={attempted_audio_paths}; attempted_video={attempted_video_paths}; last_error={resolution_error_message}",
                )
            continue

        report["processed"] += 1
        report["processed_video_ids"].append(video_id)
        if dry_run:
            continue

        try:
            if client is None:
                raise RuntimeError("openai_client_unavailable")
            transcript_text, transcription_meta = _transcribe_with_optional_segments(
                client=client,
                audio_path=audio_path,
                model=model,
                segment_large_audio=segment_large_audio,
                segment_seconds=segment_seconds,
                command_runner=command_runner,
            )
            if transcription_meta.get("segmented"):
                report["segmented_audio_transcriptions"] += 1
                media_resolution = {"video_id": video_id, "steps": [{"type": "segment_large_audio", **transcription_meta}]}
                report["media_resolution_details"].append(media_resolution)
            transcribed_at = _now_iso()
            artifacts = write_transcript_artifacts(
                video_id=video_id,
                transcript_text=transcript_text,
                metadata={
                    "channel_id": row.get("channel_id", ""),
                    "channel_name": row.get("channel_name", ""),
                    "title": row.get("title", ""),
                    "source_type": source_type,
                    "source_uri_or_path": str(audio_path),
                    "transcribed_at": transcribed_at,
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
                    "transcribed_at": transcribed_at,
                    "status": "success",
                    "transcript_path": artifacts["transcript_path"],
                    "metadata_path": artifacts["metadata_path"],
                    "insights_path": artifacts["insights_path"],
                    "source_type": source_type,
                    "transcription_model": model,
                    "language": None,
                    "text_char_count": len(transcript_text),
                    "error_message": None,
                },
            )
            report["transcribed_success"] += 1
            report["success_video_ids"].append(video_id)
            success_ids.add(video_id)
        except Exception as exc:  # noqa: BLE001
            report["failed"] += 1
            report["failed_video_ids"].append(video_id)
            warnings.append(f"transcription_failed:{video_id}:{type(exc).__name__}")
            report["failed_details"].append(
                {
                    "video_id": video_id,
                    "audio_path": str(audio_path),
                    "source_type": source_type,
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
                    "source_type": source_type,
                    "transcription_model": model,
                    "language": None,
                    "text_char_count": 0,
                    "error_category": "openai_transcription",
                    "error_message": str(exc),
                },
            )

    _write_json(transcript_dir / "transcription_run_report.json", report)
    return report
