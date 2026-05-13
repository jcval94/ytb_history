"""Run transcription for queued videos using local audio sources and OpenAI STT."""

from __future__ import annotations

import base64
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ytb_history.clients.openai_audio_client import OpenAIAudioClient
from ytb_history.services.transcript_store_service import (
    list_transcribed_video_ids,
    load_transcript_registry,
    update_transcript_registry,
    write_transcript_artifacts,
)
from ytb_history.utils.environment import resolve_environment_variable
from ytb_history.utils.video_ids import is_transcribable_video_id_candidate

AUDIO_EXTENSIONS = [".mp3", ".m4a", ".wav", ".webm", ".mp4"]
VIDEO_EXTENSIONS = [".mp4", ".webm", ".mkv", ".mov", ".m4v"]
YTDLP_STRATEGY_COOLDOWN_SECONDS = 1.5
YTDLP_AUTH_REQUIRED_WITH_COOKIES_ABORT_THRESHOLD = 3
OPENAI_TRANSCRIPTION_RETRY_ATTEMPTS = 2
OPENAI_TRANSCRIPTION_RETRY_SECONDS = 2.0
SEGMENT_SECONDS = 600


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _materialize_cookies_file_from_b64(*, ytdlp_cookies_b64: str | None) -> str | None:
    raw = (ytdlp_cookies_b64 or "").strip()
    if not raw:
        return None
    try:
        decoded = base64.b64decode(raw, validate=True)
    except Exception:
        return None
    handle = tempfile.NamedTemporaryFile(prefix="yt_cookies_", suffix=".txt", delete=False)
    try:
        handle.write(decoded)
        handle.flush()
    finally:
        handle.close()
    os.chmod(handle.name, 0o600)
    return handle.name


def _inspect_ytdlp_cookies_file(cookies_file: str | None) -> dict[str, Any]:
    """Return non-secret diagnostics for the cookies file passed to yt-dlp."""
    diagnostics: dict[str, Any] = {
        "path_provided": bool(cookies_file),
        "exists": False,
        "is_file": False,
        "size_bytes": 0,
        "non_comment_cookie_rows": 0,
        "youtube_google_cookie_rows": 0,
        "expired_youtube_google_cookie_rows": 0,
    }
    if not cookies_file:
        return diagnostics

    path = Path(cookies_file)
    diagnostics["exists"] = path.exists()
    diagnostics["is_file"] = path.is_file()
    if not path.is_file():
        return diagnostics

    try:
        diagnostics["size_bytes"] = path.stat().st_size
        now_epoch = int(time.time())
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                text = line.strip()
                if not text or text.startswith("#"):
                    continue
                diagnostics["non_comment_cookie_rows"] += 1
                fields = text.split("\t")
                domain = fields[0].lower() if fields else ""
                if domain in {".youtube.com", "youtube.com", ".google.com", "google.com"}:
                    diagnostics["youtube_google_cookie_rows"] += 1
                    if len(fields) >= 5:
                        try:
                            expires_at = int(fields[4])
                        except ValueError:
                            expires_at = 0
                        if expires_at > 0 and expires_at <= now_epoch:
                            diagnostics["expired_youtube_google_cookie_rows"] += 1
    except OSError as exc:
        diagnostics["read_error_type"] = type(exc).__name__
    return diagnostics


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


def _find_video_source(video_source_dir: Path, video_id: str) -> Path | None:
    for ext in VIDEO_EXTENSIONS:
        candidate = video_source_dir / f"{video_id}{ext}"
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _candidate_audio_paths(audio_source_dir: Path, video_id: str) -> list[str]:
    return [str(audio_source_dir / f"{video_id}{ext}") for ext in AUDIO_EXTENSIONS]


def _candidate_video_paths(video_source_dir: Path, video_id: str) -> list[str]:
    return [str(video_source_dir / f"{video_id}{ext}") for ext in VIDEO_EXTENSIONS]


def _youtube_watch_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def _classify_ytdlp_error(stderr: str) -> str:
    text = (stderr or "").lower()
    if "requested format is not available" in text:
        return "format_unavailable"
    if "could not copy" in text and "cookie database" in text:
        return "browser_cookie_access_error"
    if "ffmpeg" in text and any(token in text for token in ["not found", "is required", "not installed"]):
        return "tooling_missing"
    if any(token in text for token in ["sign in to confirm", "use --cookies", "cookies", "login required", "authentication"]):
        return "auth_required"
    if any(token in text for token in ["private video", "video unavailable", "this video is unavailable", "unavailable"]):
        return "video_unavailable"
    if any(token in text for token in ["429", "too many requests", "timed out", "timeout", "temporarily unavailable", "connection reset", "network"]):
        return "network_or_rate_limit"
    return "unknown"


def _resolve_ytdlp_command() -> list[str] | None:
    ytdlp_bin = shutil.which("yt-dlp")
    if ytdlp_bin:
        return [ytdlp_bin]
    if importlib.util.find_spec("yt_dlp") is not None:
        return [sys.executable, "-m", "yt_dlp"]
    return None


def _resolve_ffmpeg_location() -> str | None:
    configured_path = resolve_environment_variable("YTDLP_FFMPEG_LOCATION")
    if configured_path:
        return configured_path

    ffmpeg_bin = shutil.which("ffmpeg")
    if ffmpeg_bin:
        return ffmpeg_bin

    if importlib.util.find_spec("imageio_ffmpeg") is None:
        return None

    try:
        from imageio_ffmpeg import get_ffmpeg_exe
    except ImportError:
        return None

    try:
        return str(Path(get_ffmpeg_exe()))
    except Exception:  # noqa: BLE001
        return None


def _extract_audio_from_video(*, video_path: Path, audio_source_dir: Path, video_id: str) -> tuple[Path | None, str | None, str | None]:
    ffmpeg_location = _resolve_ffmpeg_location()
    if not ffmpeg_location:
        return None, "ffmpeg_not_available_for_video_audio_extraction", "tooling_missing"
    audio_source_dir.mkdir(parents=True, exist_ok=True)
    output_path = audio_source_dir / f"{video_id}.mp3"
    cmd = [
        ffmpeg_location,
        "-y",
        "-i",
        str(video_path),
        "-vn",
        "-ac",
        "1",
        "-ar",
        "16000",
        "-b:a",
        "64k",
        str(output_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode == 0 and output_path.exists():
        return output_path, None, None
    stderr_tail = (result.stderr or "").strip()[-300:]
    return None, f"ffmpeg_audio_extraction_failed:code={result.returncode};stderr={stderr_tail}", "tooling_missing"


def _segment_audio_file(*, audio_path: Path, segment_dir: Path, segment_seconds: int = SEGMENT_SECONDS) -> tuple[list[Path], str | None, str | None]:
    ffmpeg_location = _resolve_ffmpeg_location()
    if not ffmpeg_location:
        return [], "ffmpeg_not_available_for_audio_segmentation", "tooling_missing"
    segment_dir.mkdir(parents=True, exist_ok=True)
    output_template = segment_dir / "segment_%03d.mp3"
    cmd = [
        ffmpeg_location,
        "-y",
        "-i",
        str(audio_path),
        "-f",
        "segment",
        "-segment_time",
        str(segment_seconds),
        "-vn",
        "-ac",
        "1",
        "-ar",
        "16000",
        "-b:a",
        "64k",
        str(output_template),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    segments = sorted(segment_dir.glob("segment_*.mp3"))
    if result.returncode == 0 and segments:
        return segments, None, None
    stderr_tail = (result.stderr or "").strip()[-300:]
    return [], f"ffmpeg_audio_segmentation_failed:code={result.returncode};stderr={stderr_tail}", "tooling_missing"


def _is_transient_openai_error(exc: Exception) -> bool:
    error_type = type(exc).__name__.lower()
    message = str(exc).lower()
    transient_types = ["apiconnectionerror", "apitimeouterror", "ratelimiterror", "internalservererror", "serviceunavailableerror"]
    transient_tokens = ["connection error", "timed out", "timeout", "temporarily unavailable", "rate limit", "try again"]
    return any(token in error_type for token in transient_types) or any(token in message for token in transient_tokens)


def _is_input_too_large_error(exc: Exception) -> bool:
    text = f"{type(exc).__name__} {exc}".lower()
    return "input_too_large" in text or ("too large" in text and ("audio" in text or "token" in text))


def _transcribe_with_retries(client: OpenAIAudioClient, *, file_path: Path, model: str) -> str:
    attempts = OPENAI_TRANSCRIPTION_RETRY_ATTEMPTS + 1
    for attempt in range(attempts):
        try:
            return client.transcribe_file(file_path=file_path, model=model)
        except Exception as exc:  # noqa: BLE001
            if attempt >= attempts - 1 or not _is_transient_openai_error(exc):
                raise
            time.sleep(OPENAI_TRANSCRIPTION_RETRY_SECONDS * (attempt + 1))
    raise RuntimeError("unreachable_transcription_retry_state")


def _transcribe_with_segmentation_fallback(
    client: OpenAIAudioClient,
    *,
    audio_path: Path,
    model: str,
    segment_root: Path,
) -> tuple[str, dict[str, Any]]:
    try:
        return _transcribe_with_retries(client, file_path=audio_path, model=model), {
            "segmented": False,
            "segment_count": 0,
            "segment_paths": [],
        }
    except Exception as exc:  # noqa: BLE001
        if not _is_input_too_large_error(exc):
            raise
        segments, segment_error, segment_error_category = _segment_audio_file(
            audio_path=audio_path,
            segment_dir=segment_root,
        )
        if not segments:
            raise RuntimeError(f"audio_segmentation_failed:{segment_error_category}:{segment_error}") from exc
        segment_texts = [
            _transcribe_with_retries(client, file_path=segment_path, model=model)
            for segment_path in segments
        ]
        return "\n\n".join(segment_texts), {
            "segmented": True,
            "segment_count": len(segments),
            "segment_paths": [str(segment_path) for segment_path in segments],
            "original_error_type": type(exc).__name__,
            "original_error_message": str(exc),
        }


def _ytdlp_download_strategies() -> list[tuple[str, list[str]]]:
    """Ordered yt-dlp strategies tuned for robust audio extraction.

    Start with yt-dlp's own YouTube defaults because that matches the simplest
    successful local/Colab command (`yt-dlp -x --audio-format mp3 ...`) and lets
    yt-dlp select the currently recommended player clients. Explicit clients are
    retained as fallbacks for environments where one client is temporarily more
    reliable than the defaults.
    """
    return [
        ("default", []),
        ("android", ["--extractor-args", "youtube:player_client=android"]),
        ("ios", ["--extractor-args", "youtube:player_client=ios"]),
        ("mweb", ["--extractor-args", "youtube:player_client=mweb"]),
        ("tv_simply", ["--extractor-args", "youtube:player_client=tv_simply"]),
        ("web", ["--extractor-args", "youtube:player_client=web"]),
    ]


def _should_stop_ytdlp_strategy_retries(*, error_category: str, has_auth_context: bool) -> bool:
    if error_category == "video_unavailable":
        return True
    if error_category == "auth_required":
        return not has_auth_context
    return False


def _download_audio_with_ytdlp(
    *,
    video_id: str,
    audio_source_dir: Path,
    ytdlp_cookies_file: str | None = None,
    ytdlp_browser: str | None = None,
    ytdlp_extra_args: list[str] | None = None,
) -> tuple[Path | None, str | None, str | None]:
    ytdlp_command = _resolve_ytdlp_command()
    if not ytdlp_command:
        return None, "yt_dlp_not_installed", "tooling_missing"
    audio_source_dir.mkdir(parents=True, exist_ok=True)
    output_template = str(audio_source_dir / f"{video_id}.%(ext)s")
    ffmpeg_location = _resolve_ffmpeg_location()

    last_error = "yt_dlp_failed:unknown"
    last_error_category = "unknown"
    strategies = _ytdlp_download_strategies()
    has_auth_context = bool(ytdlp_cookies_file or ytdlp_browser)
    for strategy_idx, (strategy_name, strategy_args) in enumerate(strategies):
        cmd = [
            *ytdlp_command,
            "--no-playlist",
            "--format",
            "bestaudio[ext=m4a]/bestaudio/best",
            "--retries",
            "5",
            "--fragment-retries",
            "5",
            "--socket-timeout",
            "30",
            "--force-ipv4",
            "-x",
            "--audio-format",
            "mp3",
            "--audio-quality",
            "5",
            "-o",
            output_template,
        ]
        if ffmpeg_location:
            cmd.extend(["--ffmpeg-location", ffmpeg_location])
        cmd.extend(strategy_args)
        if ytdlp_cookies_file:
            cmd.extend(["--cookies", ytdlp_cookies_file])
        if ytdlp_browser:
            cmd.extend(["--cookies-from-browser", ytdlp_browser])
        if ytdlp_extra_args:
            cmd.extend(ytdlp_extra_args)
        cmd.append(_youtube_watch_url(video_id))

        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode == 0:
            audio_path = _find_audio_source(audio_source_dir, video_id)
            if audio_path is not None:
                return audio_path, None, None
            last_error = f"yt_dlp_completed_but_audio_not_found:strategy={strategy_name}"
            last_error_category = "unknown"
            continue

        stderr_full = (result.stderr or "").strip()
        stderr_tail = stderr_full[-300:]
        last_error = f"yt_dlp_failed:strategy={strategy_name};code={result.returncode};stderr={stderr_tail}"
        last_error_category = _classify_ytdlp_error(stderr_full)
        if _should_stop_ytdlp_strategy_retries(error_category=last_error_category, has_auth_context=has_auth_context):
            break
        has_more_strategies = strategy_idx < len(strategies) - 1
        if has_more_strategies:
            time.sleep(YTDLP_STRATEGY_COOLDOWN_SECONDS)

    return None, last_error, last_error_category


def transcribe_selected_videos(
    *,
    data_dir: str | Path = "data",
    limit: int = 10,
    audio_source_dir: str | Path = "data/audio_sources",
    video_source_dir: str | Path = "data/video_sources",
    model: str = "gpt-4o-mini-transcribe",
    openai_client: OpenAIAudioClient | None = None,
    allow_ytdlp_fallback: bool = True,
    ytdlp_cookies_file: str | None = None,
    ytdlp_browser: str | None = None,
    ytdlp_extra_args: list[str] | None = None,
    ytdlp_cookies_b64: str | None = None,
) -> dict[str, Any]:
    root = Path(data_dir)
    transcript_dir = root / "transcripts"
    queue_path = transcript_dir / "transcript_queue.jsonl"

    api_key = resolve_environment_variable("OPENAI_API_KEY")
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
    registry_success_ids = {
        str(row.get("video_id", "")).strip()
        for row in registry
        if str(row.get("status", "")).strip() == "success"
    }
    success_ids = list_transcribed_video_ids(data_dir=data_dir)

    processed = 0
    transcribed_success = 0
    skipped_no_audio_source = 0
    skipped_invalid_video_id = 0
    skipped_missing_ytdlp = 0
    failed_audio_download = 0
    skipped_already_transcribed = 0
    failed = 0
    warnings: list[str] = []
    missing_audio_video_ids: list[str] = []
    missing_audio_details: list[dict[str, Any]] = []
    invalid_video_id_details: list[dict[str, Any]] = []
    already_transcribed_video_ids: list[str] = []
    success_video_ids: list[str] = []
    failed_video_ids: list[str] = []
    failed_details: list[dict[str, Any]] = []
    media_resolution_details: list[dict[str, Any]] = []
    media_resolution_counts = {
        "existing_audio": 0,
        "extracted_from_video": 0,
        "downloaded_ytdlp": 0,
        "failed_media_resolution": 0,
    }
    segmented_transcriptions = 0
    ytdlp_download_attempts = 0
    ytdlp_download_success = 0
    ytdlp_download_failures: list[dict[str, Any]] = []
    registry_success_before_run = len(registry_success_ids)
    persisted_success_before_run = len(success_ids)
    consecutive_auth_required_with_cookies = 0

    generated_cookies_file: str | None = None
    effective_cookies_file = ytdlp_cookies_file
    if not effective_cookies_file:
        generated_cookies_file = _materialize_cookies_file_from_b64(
            ytdlp_cookies_b64=ytdlp_cookies_b64 or resolve_environment_variable("YTDLP_COOKIES_B64")
        )
        effective_cookies_file = generated_cookies_file

    source_root = Path(audio_source_dir)
    video_root = Path(video_source_dir)
    source_root_exists = source_root.exists()
    source_root_is_dir = source_root.is_dir()
    video_root_exists = video_root.exists()
    video_root_is_dir = video_root.is_dir()
    available_audio_files_sample = sorted([p.name for p in source_root.glob("*") if p.is_file()])[:50] if source_root_is_dir else []
    available_video_files_sample = sorted([p.name for p in video_root.glob("*") if p.is_file()])[:50] if video_root_is_dir else []
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

        if (
            effective_cookies_file
            and consecutive_auth_required_with_cookies >= YTDLP_AUTH_REQUIRED_WITH_COOKIES_ABORT_THRESHOLD
        ):
            warnings.append("ytdlp_auth_required_circuit_open")
            break

        processed += 1
        if not is_transcribable_video_id_candidate(video_id):
            skipped_invalid_video_id += 1
            detail = {
                "video_id": video_id,
                "video_url": _youtube_watch_url(video_id),
                "reason": "probable_channel_id_in_video_id_field",
            }
            invalid_video_id_details.append(detail)
            missing_audio_video_ids.append(video_id)
            missing_audio_details.append(
                {
                    "video_id": video_id,
                    "audio_source_dir": str(source_root),
                    "audio_source_dir_exists": source_root_exists,
                    "audio_source_dir_is_dir": source_root_is_dir,
                    "video_url": _youtube_watch_url(video_id),
                    "attempted_paths": [],
                    "ytdlp_error": "invalid_video_id:probable_channel_id",
                    "ytdlp_error_category": "invalid_video_id",
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
                    "status": "skipped_invalid_video_id",
                    "transcript_path": None,
                    "metadata_path": None,
                    "insights_path": None,
                    "source_type": "unknown",
                    "text_char_count": 0,
                    "error_category": "invalid_video_id",
                    "error_message": "probable channel_id found in video_id field; yt-dlp was not called",
                },
            )
            continue
        media_resolution_source = "failed_media_resolution"
        media_resolution_error: str | None = None
        media_resolution_error_category: str | None = None
        audio_path = _find_audio_source(source_root, video_id)
        if audio_path is not None:
            media_resolution_source = "existing_audio"
        if audio_path is None:
            video_path = _find_video_source(video_root, video_id)
            if video_path is not None:
                audio_path, media_resolution_error, media_resolution_error_category = _extract_audio_from_video(
                    video_path=video_path,
                    audio_source_dir=source_root,
                    video_id=video_id,
                )
                if audio_path is not None:
                    media_resolution_source = "extracted_from_video"
            else:
                media_resolution_error = "video_source_not_found"
                media_resolution_error_category = "local_video_missing"

        if audio_path is None:
            ytdlp_error: str | None = None
            ytdlp_error_category: str | None = None
            if allow_ytdlp_fallback:
                ytdlp_download_attempts += 1
                audio_path, ytdlp_error, ytdlp_error_category = _download_audio_with_ytdlp(
                    video_id=video_id,
                    audio_source_dir=source_root,
                    ytdlp_cookies_file=effective_cookies_file,
                    ytdlp_browser=ytdlp_browser,
                    ytdlp_extra_args=ytdlp_extra_args,
                )
                if audio_path is not None:
                    ytdlp_download_success += 1
                    consecutive_auth_required_with_cookies = 0
                    media_resolution_source = "downloaded_ytdlp"
                    media_resolution_error = None
                    media_resolution_error_category = None
            if audio_path is None:
                if ytdlp_error:
                    ytdlp_download_failures.append({"video_id": video_id, "error": ytdlp_error, "error_category": ytdlp_error_category, "video_url": _youtube_watch_url(video_id)})
                    media_resolution_error = ytdlp_error
                    media_resolution_error_category = ytdlp_error_category
                if ytdlp_error == "yt_dlp_not_installed":
                    status = "skipped_missing_ytdlp"
                    skipped_missing_ytdlp += 1
                elif allow_ytdlp_fallback and ytdlp_error:
                    if ytdlp_error_category == "auth_required":
                        status = "failed_audio_download_auth_required"
                    elif ytdlp_error_category == "browser_cookie_access_error":
                        status = "failed_audio_download_browser_cookie_access"
                    elif ytdlp_error_category == "video_unavailable":
                        status = "failed_audio_download_video_unavailable"
                    elif ytdlp_error_category == "network_or_rate_limit":
                        status = "failed_audio_download_network_or_rate_limit"
                    else:
                        status = "failed_audio_download"
                    failed_audio_download += 1
                    if effective_cookies_file and ytdlp_error_category == "auth_required":
                        consecutive_auth_required_with_cookies += 1
                    else:
                        consecutive_auth_required_with_cookies = 0
                else:
                    status = "skipped_no_audio_source"
                    skipped_no_audio_source += 1
                missing_audio_video_ids.append(video_id)
                attempted_paths = _candidate_audio_paths(source_root, video_id)
                attempted_video_paths = _candidate_video_paths(video_root, video_id)
                missing_audio_details.append(
                    {
                        "video_id": video_id,
                        "audio_source_dir": str(source_root),
                        "audio_source_dir_exists": source_root_exists,
                        "audio_source_dir_is_dir": source_root_is_dir,
                        "video_source_dir": str(video_root),
                        "video_source_dir_exists": video_root_exists,
                        "video_source_dir_is_dir": video_root_is_dir,
                        "video_url": _youtube_watch_url(video_id),
                        "attempted_paths": attempted_paths,
                        "attempted_video_paths": attempted_video_paths,
                        "ytdlp_error": ytdlp_error,
                        "ytdlp_error_category": ytdlp_error_category,
                        "media_resolution_source": media_resolution_source,
                        "media_resolution_error": media_resolution_error,
                        "media_resolution_error_category": media_resolution_error_category,
                    }
                )
                media_resolution_counts["failed_media_resolution"] += 1
                media_resolution_details.append(
                    {
                        "video_id": video_id,
                        "source": "failed_media_resolution",
                        "error": media_resolution_error,
                        "error_category": media_resolution_error_category,
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
                        "status": status,
                        "transcript_path": None,
                        "metadata_path": None,
                        "insights_path": None,
                        "source_type": "unknown",
                        "media_resolution_source": "failed_media_resolution",
                        "text_char_count": 0,
                        "error_category": ytdlp_error_category,
                        "error_message": f"audio_source_not_found; video_url={_youtube_watch_url(video_id)}; attempted={attempted_paths}; ytdlp={ytdlp_error}",
                    },
                )
                continue

        try:
            media_resolution_counts[media_resolution_source] += 1
            media_resolution_details.append(
                {
                    "video_id": video_id,
                    "source": media_resolution_source,
                    "audio_path": str(audio_path),
                }
            )
            segment_root = source_root / "segments" / video_id
            transcript_text, transcription_metadata = _transcribe_with_segmentation_fallback(
                client,
                audio_path=audio_path,
                model=model,
                segment_root=segment_root,
            )
            if transcription_metadata.get("segmented"):
                segmented_transcriptions += 1
            artifacts = write_transcript_artifacts(
                video_id=video_id,
                transcript_text=transcript_text,
                metadata={
                    "channel_id": row.get("channel_id", ""),
                    "channel_name": row.get("channel_name", ""),
                    "title": row.get("title", ""),
                    "source_type": "audio_file",
                    "source_uri_or_path": str(audio_path),
                    "media_resolution_source": media_resolution_source,
                    "segmented_transcription": transcription_metadata.get("segmented", False),
                    "segment_count": transcription_metadata.get("segment_count", 0),
                    "segment_paths": transcription_metadata.get("segment_paths", []),
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
                    "media_resolution_source": media_resolution_source,
                    "transcription_model": model,
                    "language": None,
                    "text_char_count": len(transcript_text),
                    "segmented_transcription": transcription_metadata.get("segmented", False),
                    "segment_count": transcription_metadata.get("segment_count", 0),
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
                    "media_resolution_source": media_resolution_source,
                    "transcription_model": model,
                    "language": None,
                    "text_char_count": 0,
                    "error_message": str(exc),
                },
            )

    auth_required_with_cookies_count = (
        sum(1 for failure in ytdlp_download_failures if failure.get("error_category") == "auth_required")
        if effective_cookies_file
        else 0
    )
    cookie_file_diagnostics = _inspect_ytdlp_cookies_file(effective_cookies_file)
    if auth_required_with_cookies_count:
        warnings.append("ytdlp_auth_required_despite_cookies")
        warnings.append("rotate_ytdlp_cookies_or_validate_cookie_export")
        if cookie_file_diagnostics.get("youtube_google_cookie_rows") == 0:
            warnings.append("ytdlp_cookies_file_missing_youtube_google_domains")
        elif cookie_file_diagnostics.get("expired_youtube_google_cookie_rows") == cookie_file_diagnostics.get("youtube_google_cookie_rows"):
            warnings.append("ytdlp_cookies_file_youtube_google_cookies_expired")

    report = {
        "generated_at": _now_iso(),
        "limit": limit,
        "processed": processed,
        "transcribed_success": transcribed_success,
        "skipped_no_audio_source": skipped_no_audio_source,
        "skipped_invalid_video_id": skipped_invalid_video_id,
        "skipped_missing_ytdlp": skipped_missing_ytdlp,
        "failed_audio_download": failed_audio_download,
        "skipped_already_transcribed": skipped_already_transcribed,
        "failed": failed,
        "queue_total": len(queued),
        "registry_success_before_run": registry_success_before_run,
        "persisted_success_before_run": persisted_success_before_run,
        "audio_source_dir": str(source_root),
        "audio_source_dir_exists": source_root_exists,
        "audio_source_dir_is_dir": source_root_is_dir,
        "audio_source_files_sample": available_audio_files_sample,
        "video_source_dir": str(video_root),
        "video_source_dir_exists": video_root_exists,
        "video_source_dir_is_dir": video_root_is_dir,
        "video_source_files_sample": available_video_files_sample,
        "media_resolution_counts": media_resolution_counts,
        "media_resolution_details": media_resolution_details,
        "segmented_transcriptions": segmented_transcriptions,
        "allow_ytdlp_fallback": allow_ytdlp_fallback,
        "ytdlp_runtime_options": {
            "used_cookies_file": bool(effective_cookies_file),
            "used_browser_mode": bool(ytdlp_browser),
            "extra_args_count": len(ytdlp_extra_args or []),
        },
        "ytdlp_download_attempts": ytdlp_download_attempts,
        "ytdlp_download_success": ytdlp_download_success,
        "ytdlp_auth_required_with_cookies_count": auth_required_with_cookies_count,
        "ytdlp_auth_required_with_cookies_abort_threshold": YTDLP_AUTH_REQUIRED_WITH_COOKIES_ABORT_THRESHOLD,
        "ytdlp_cookies_file_diagnostics": cookie_file_diagnostics,
        "ytdlp_download_failures": ytdlp_download_failures,
        "processed_video_ids": success_video_ids + missing_audio_video_ids + failed_video_ids,
        "success_video_ids": success_video_ids,
        "already_transcribed_video_ids": already_transcribed_video_ids,
        "missing_audio_video_ids": missing_audio_video_ids,
        "missing_audio_details": missing_audio_details,
        "invalid_video_id_details": invalid_video_id_details,
        "failed_video_ids": failed_video_ids,
        "failed_details": failed_details,
        "warnings": warnings,
    }
    (transcript_dir / "transcription_run_report.json").parent.mkdir(parents=True, exist_ok=True)
    (transcript_dir / "transcription_run_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if generated_cookies_file:
        Path(generated_cookies_file).unlink(missing_ok=True)
    return report
