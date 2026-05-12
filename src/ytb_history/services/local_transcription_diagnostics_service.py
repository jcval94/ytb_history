"""Local diagnostics for scheduled transcription automation."""

from __future__ import annotations

import json
import importlib.util
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ytb_history.services.local_repo_sync_service import sync_local_repo
from ytb_history.utils.environment import resolve_environment_variable

CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_jsonl_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def _sample_files(path: Path, *, limit: int = 20) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False, "is_dir": False, "file_count": 0, "sample": []}
    if not path.is_dir():
        return {"exists": True, "is_dir": False, "file_count": 0, "sample": []}
    files = sorted([item.name for item in path.iterdir() if item.is_file()])
    return {"exists": True, "is_dir": True, "file_count": len(files), "sample": files[:limit]}


def _resolve_ytdlp() -> dict[str, Any]:
    executable = shutil.which("yt-dlp")
    module_available = importlib.util.find_spec("yt_dlp") is not None
    return {
        "available": bool(executable or module_available),
        "executable": executable,
        "python_module_available": module_available,
    }


def _resolve_ffmpeg() -> dict[str, Any]:
    executable = shutil.which("ffmpeg")
    imageio_executable = None
    if not executable:
        try:
            import imageio_ffmpeg  # type: ignore[import-not-found]

            imageio_executable = str(imageio_ffmpeg.get_ffmpeg_exe())
        except Exception:  # noqa: BLE001
            imageio_executable = None
    return {
        "available": bool(executable or imageio_executable),
        "executable": executable or imageio_executable,
        "source": "path" if executable else ("imageio_ffmpeg" if imageio_executable else None),
    }


def _run_scheduler_query(*, task_name: str, command_runner: CommandRunner) -> dict[str, Any]:
    if sys.platform != "win32":
        return {"skipped": True, "reason": "not_windows"}
    result = command_runner(
        ["schtasks", "/Query", "/TN", task_name, "/V", "/FO", "LIST"],
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "command": ["schtasks", "/Query", "/TN", task_name, "/V", "/FO", "LIST"],
        "returncode": result.returncode,
        "ok": result.returncode == 0,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def diagnose_local_transcription(
    *,
    repo_dir: str | Path = ".",
    data_dir: str | Path = "data",
    audio_source_dir: str | Path = "data/audio_sources",
    video_source_dir: str | Path = "data/video_sources",
    task_name: str = "YtbHistoryLocalTranscription",
    command_runner: CommandRunner = subprocess.run,
) -> dict[str, Any]:
    repo_root = Path(repo_dir).expanduser().resolve()
    data_root = Path(data_dir)
    if not data_root.is_absolute():
        data_root = repo_root / data_root
    audio_root = Path(audio_source_dir)
    if not audio_root.is_absolute():
        audio_root = repo_root / audio_root
    video_root = Path(video_source_dir)
    if not video_root.is_absolute():
        video_root = repo_root / video_root

    sync_report = sync_local_repo(
        repo_dir=repo_root,
        check_only=True,
        command_runner=command_runner,
    )

    report = {
        "status": "success",
        "generated_at": _now_iso(),
        "repo_dir": str(repo_root),
        "data_dir": str(data_root),
        "task_name": task_name,
        "scheduler": _run_scheduler_query(task_name=task_name, command_runner=command_runner),
        "git": {
            "status": sync_report.get("status"),
            "head": sync_report.get("head"),
            "remote_head": sync_report.get("remote_head"),
            "remote_changed": sync_report.get("remote_changed"),
            "has_dirty_worktree": sync_report.get("has_dirty_worktree"),
            "dirty_file_count": sync_report.get("dirty_file_count"),
            "dirty_files": sync_report.get("dirty_files", [])[:50],
            "warnings": sync_report.get("warnings", []),
        },
        "transcription_queue": {
            "path": str(data_root / "transcripts" / "transcript_queue.jsonl"),
            "row_count": _read_jsonl_count(data_root / "transcripts" / "transcript_queue.jsonl"),
        },
        "audio_sources": _sample_files(audio_root),
        "video_sources": _sample_files(video_root),
        "environment": {
            "openai_api_key_present": bool(resolve_environment_variable("OPENAI_API_KEY")),
            "ytdlp_cookies_b64_present": bool(resolve_environment_variable("YTDLP_COOKIES_B64")),
        },
        "tools": {
            "yt_dlp": _resolve_ytdlp(),
            "ffmpeg": _resolve_ffmpeg(),
        },
        "warnings": [],
    }
    if sync_report.get("status") == "blocked_dirty_worktree":
        report["warnings"].append("dirty_worktree_blocks_scheduled_pull")
    return report
