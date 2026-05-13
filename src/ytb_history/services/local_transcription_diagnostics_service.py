"""Local diagnostics for repository sync and transcription automation."""

from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from ytb_history.services.local_repo_sync_service import SYNC_SUCCESS_STATUSES
from ytb_history.utils.environment import resolve_environment_variable

JsonReport = dict[str, Any]
CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_report(report_path: Path, report: JsonReport) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_json(path: Path) -> JsonReport | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return None


def _run_command(
    command: Sequence[str],
    *,
    cwd: Path,
    command_runner: CommandRunner,
) -> JsonReport:
    result = command_runner(
        list(command),
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "command": list(command),
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "ok": result.returncode == 0,
    }


def _read_jsonl_count(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    count += 1
    except OSError:
        return 0
    return count


def _read_forced_channels(config_path: Path) -> list[str]:
    if not config_path.exists():
        return []
    namespace: dict[str, Any] = {}
    try:
        exec(config_path.read_text(encoding="utf-8"), namespace)  # noqa: S102
    except Exception:
        return []
    urls = namespace.get("TRANSCRIPTION_CHANNEL_URLS", [])
    if not isinstance(urls, list):
        return []
    return [str(url) for url in urls]


def _media_summary(path: Path) -> JsonReport:
    try:
        files = sorted([p for p in path.glob("*") if p.is_file()]) if path.exists() else []
        total_bytes = sum(p.stat().st_size for p in files[:200])
    except OSError as exc:
        return {
            "path": str(path),
            "exists": path.exists(),
            "is_dir": path.is_dir(),
            "file_count": 0,
            "sample": [],
            "total_bytes": 0,
            "read_error_type": type(exc).__name__,
        }
    return {
        "path": str(path),
        "exists": path.exists(),
        "is_dir": path.is_dir(),
        "file_count": len(files),
        "sample": [p.name for p in files[:20]],
        "total_bytes": total_bytes,
    }


def diagnose_local_transcription(
    *,
    repo_dir: str | Path,
    data_dir: str | Path = "data",
    audio_source_dir: str | Path = "data/audio_sources",
    video_source_dir: str | Path = "data/video_sources",
    sync_task_name: str = "YtbHistoryLocalRepoSync",
    transcription_task_name: str = "YtbHistoryLocalTranscription",
    command_runner: CommandRunner = subprocess.run,
) -> JsonReport:
    """Write a non-secret local automation diagnosis report."""
    repo_root = Path(repo_dir).expanduser().resolve()
    root = repo_root / data_dir if not Path(data_dir).is_absolute() else Path(data_dir)
    audio_root = repo_root / audio_source_dir if not Path(audio_source_dir).is_absolute() else Path(audio_source_dir)
    video_root = repo_root / video_source_dir if not Path(video_source_dir).is_absolute() else Path(video_source_dir)
    automation_dir = repo_root / "build" / "local_automation"
    latest_sync_report = _read_json(automation_dir / "latest_sync_report.json")
    latest_run_report = _read_json(automation_dir / "latest_run_report.json")
    schedule_state = _read_json(automation_dir / "schedule_state.json")

    sync_task = _run_command(["schtasks", "/Query", "/TN", sync_task_name, "/V", "/FO", "LIST"], cwd=repo_root, command_runner=command_runner)
    sync_logon_task = _run_command(["schtasks", "/Query", "/TN", f"{sync_task_name}LogonCatchup", "/V", "/FO", "LIST"], cwd=repo_root, command_runner=command_runner)
    transcription_task = _run_command(["schtasks", "/Query", "/TN", transcription_task_name, "/V", "/FO", "LIST"], cwd=repo_root, command_runner=command_runner)
    transcription_logon_task = _run_command(["schtasks", "/Query", "/TN", f"{transcription_task_name}LogonCatchup", "/V", "/FO", "LIST"], cwd=repo_root, command_runner=command_runner)

    git_status = _run_command(["git", "status", "--short", "--branch"], cwd=repo_root, command_runner=command_runner)
    git_head = _run_command(["git", "rev-parse", "HEAD"], cwd=repo_root, command_runner=command_runner)
    git_remote = _run_command(["git", "ls-remote", "origin", "refs/heads/main"], cwd=repo_root, command_runner=command_runner)

    queue_path = root / "transcripts" / "transcript_queue.jsonl"
    registry_path = root / "transcripts" / "transcript_registry.jsonl"
    forced_channels = _read_forced_channels(repo_root / "config" / "transcription_channels.py")
    openai_api_key_present = bool(resolve_environment_variable("OPENAI_API_KEY"))
    ytdlp_path = shutil.which("yt-dlp")
    ffmpeg_path = shutil.which("ffmpeg")
    imageio_ffmpeg_available = importlib.util.find_spec("imageio_ffmpeg") is not None

    report: JsonReport = {
        "generated_at": _now_iso(),
        "status": "success",
        "repo_dir": str(repo_root),
        "tasks": {
            "sync": sync_task,
            "sync_logon_catchup": sync_logon_task,
            "transcription": transcription_task,
            "transcription_logon_catchup": transcription_logon_task,
        },
        "git": {
            "status": git_status,
            "head": git_head,
            "remote_main": git_remote,
            "latest_sync_status": (latest_sync_report or {}).get("status"),
            "sync_ok": (latest_sync_report or {}).get("status") in SYNC_SUCCESS_STATUSES,
        },
        "automation_state": {
            "latest_sync_report": latest_sync_report,
            "latest_run_report_status": (latest_run_report or {}).get("status"),
            "schedule_state": schedule_state,
        },
        "transcription_queue": {
            "path": str(queue_path),
            "queued_count": _read_jsonl_count(queue_path),
            "registry_count": _read_jsonl_count(registry_path),
            "forced_channels": forced_channels,
        },
        "media": {
            "audio_sources": _media_summary(audio_root),
            "video_sources": _media_summary(video_root),
        },
        "tools": {
            "openai_api_key_present": openai_api_key_present,
            "yt_dlp_path": ytdlp_path,
            "ffmpeg_path": ffmpeg_path,
            "imageio_ffmpeg_available": imageio_ffmpeg_available,
        },
        "warnings": [],
    }
    if not openai_api_key_present:
        report["warnings"].append("missing_openai_api_key")
    if not ytdlp_path:
        report["warnings"].append("missing_ytdlp_binary")
    if not ffmpeg_path and not imageio_ffmpeg_available:
        report["warnings"].append("missing_ffmpeg")

    _write_report(automation_dir / "latest_diagnosis.json", report)
    return report
