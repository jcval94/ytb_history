"""Local end-to-end automation for refreshing and transcribing selected videos."""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from ytb_history.orchestrator import run_pipeline
from ytb_history.services.transcript_insights_service import generate_transcript_insights
from ytb_history.services.transcript_selection_service import select_transcription_candidates
from ytb_history.services.transcript_store_service import build_transcript_registry_report
from ytb_history.services.transcription_runner_service import transcribe_selected_videos

JsonReport = dict[str, Any]
CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_under_repo(repo_root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return repo_root / candidate


def _as_repo_relative(repo_root: Path, path: Path) -> str:
    try:
        return path.relative_to(repo_root).as_posix()
    except ValueError:
        return path.as_posix()


def _command_to_report(command: Sequence[str], result: subprocess.CompletedProcess[str]) -> JsonReport:
    return {
        "command": list(command),
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "ok": result.returncode == 0,
    }


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
    return _command_to_report(command, result)


def _write_report(report_path: Path, report: JsonReport) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _git_has_staged_or_worktree_changes(status_report: JsonReport) -> bool:
    return bool(str(status_report.get("stdout", "")).strip())


def _count_step_items(step_report: JsonReport, key: str) -> int:
    value = step_report.get(key, 0)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _has_publishable_transcription_outputs(report: JsonReport) -> bool:
    steps = report.get("steps", {})
    transcription_step = steps.get("transcription", {})
    insights_step = steps.get("transcript_insights", {})
    return (
        _count_step_items(transcription_step, "transcribed_success") > 0
        or _count_step_items(insights_step, "generated") > 0
    )


def run_local_transcription_automation(
    *,
    repo_dir: str | Path,
    data_dir: str | Path = "data",
    settings_path: str | Path = "config/settings.yaml",
    limit: int = 10,
    skip_youtube_refresh: bool = False,
    no_sync_git: bool = False,
    audio_source_dir: str | Path = "data/audio_sources",
    ytdlp_cookies_file: str | None = None,
    ytdlp_browser: str | None = None,
    ytdlp_extra_args: list[str] | None = None,
    command_runner: CommandRunner = subprocess.run,
    pipeline_runner: Callable[..., JsonReport] = run_pipeline,
    candidate_selector: Callable[..., JsonReport] = select_transcription_candidates,
    transcription_runner: Callable[..., JsonReport] = transcribe_selected_videos,
    insights_generator: Callable[..., JsonReport] = generate_transcript_insights,
    registry_report_builder: Callable[..., JsonReport] = build_transcript_registry_report,
) -> JsonReport:
    """Run the local transcription automation workflow.

    All external side effects are injectable so unit tests can use fakes instead
    of hitting Git, YouTube, OpenAI, or yt-dlp.
    """
    repo_root = Path(repo_dir).expanduser().resolve()
    effective_data_dir = _resolve_under_repo(repo_root, data_dir)
    effective_settings_path = _resolve_under_repo(repo_root, settings_path)
    effective_audio_source_dir = _resolve_under_repo(repo_root, audio_source_dir)
    report_path = repo_root / "build" / "local_automation" / "latest_run_report.json"
    transcripts_relative_path = _as_repo_relative(repo_root, effective_data_dir / "transcripts")

    report: JsonReport = {
        "generated_at": _now_iso(),
        "status": "success",
        "repo_dir": str(repo_root),
        "data_dir": str(effective_data_dir),
        "settings_path": str(effective_settings_path),
        "limit": limit,
        "skip_youtube_refresh": skip_youtube_refresh,
        "no_sync_git": no_sync_git,
        "audio_source_dir": str(effective_audio_source_dir),
        "steps": {},
        "git": {"enabled": not no_sync_git, "commands": []},
        "warnings": [],
    }

    if not no_sync_git:
        pull_report = _run_command(["git", "pull", "--rebase", "--autostash"], cwd=repo_root, command_runner=command_runner)
        report["git"]["commands"].append(pull_report)
        report["steps"]["git_pull"] = pull_report
        if not pull_report["ok"]:
            report["status"] = "failed"
            report["warnings"].append("git_pull_failed")
            _write_report(report_path, report)
            return report
    else:
        report["steps"]["git_pull"] = {"skipped": True, "reason": "no_sync_git"}

    if skip_youtube_refresh:
        report["steps"]["youtube_refresh"] = {"skipped": True, "reason": "skip_youtube_refresh"}
    else:
        report["steps"]["youtube_refresh"] = pipeline_runner(
            settings_path=str(effective_settings_path),
            data_dir=str(effective_data_dir),
        )

    report["steps"]["transcription_candidates"] = candidate_selector(data_dir=str(effective_data_dir), limit=limit)
    report["steps"]["transcription"] = transcription_runner(
        data_dir=str(effective_data_dir),
        limit=limit,
        audio_source_dir=str(effective_audio_source_dir),
        ytdlp_cookies_file=ytdlp_cookies_file,
        ytdlp_browser=ytdlp_browser,
        ytdlp_extra_args=ytdlp_extra_args,
    )
    report["steps"]["transcript_insights"] = insights_generator(data_dir=str(effective_data_dir), limit=limit)
    report["steps"]["transcript_registry_report"] = registry_report_builder(data_dir=str(effective_data_dir))

    if not no_sync_git:
        report["git"]["publishable_outputs"] = _has_publishable_transcription_outputs(report)
        _write_report(report_path, report)
        if not report["git"]["publishable_outputs"]:
            report["git"]["has_changes"] = False
            report["git"]["commit_attempted"] = False
            report["warnings"].append("git_sync_skipped_no_publishable_outputs")
            report["steps"]["git_add"] = {"skipped": True, "reason": "no_publishable_outputs"}
            report["steps"]["git_status"] = {"skipped": True, "reason": "no_publishable_outputs"}
            report["steps"]["git_commit"] = {"skipped": True, "reason": "no_publishable_outputs"}
            report["steps"]["git_push"] = {"skipped": True, "reason": "no_publishable_outputs"}
            _write_report(report_path, report)
            return report

        add_report = _run_command(
            ["git", "add", transcripts_relative_path],
            cwd=repo_root,
            command_runner=command_runner,
        )
        report["git"]["commands"].append(add_report)
        report["steps"]["git_add"] = add_report
        if not add_report["ok"]:
            report["status"] = "failed"
            report["warnings"].append("git_add_failed")
            _write_report(report_path, report)
            return report

        status_report = _run_command(
            ["git", "status", "--porcelain", "--", transcripts_relative_path],
            cwd=repo_root,
            command_runner=command_runner,
        )
        report["git"]["commands"].append(status_report)
        report["steps"]["git_status"] = status_report
        if not status_report["ok"]:
            report["status"] = "failed"
            report["warnings"].append("git_status_failed")
            _write_report(report_path, report)
            return report

        has_changes = _git_has_staged_or_worktree_changes(status_report)
        report["git"]["has_changes"] = has_changes
        report["git"]["commit_attempted"] = has_changes
        _write_report(report_path, report)

        if has_changes:
            commit_report = _run_command(
                ["git", "commit", "-m", "Run local transcription automation"],
                cwd=repo_root,
                command_runner=command_runner,
            )
            report["git"]["commands"].append(commit_report)
            report["steps"]["git_commit"] = commit_report
            if not commit_report["ok"]:
                report["status"] = "failed"
                report["warnings"].append("git_commit_failed")
                return report

            push_report = _run_command(["git", "push"], cwd=repo_root, command_runner=command_runner)
            report["git"]["commands"].append(push_report)
            report["steps"]["git_push"] = push_report
            if not push_report["ok"]:
                report["status"] = "failed"
                report["warnings"].append("git_push_failed")
        else:
            report["steps"]["git_commit"] = {"skipped": True, "reason": "no_changes"}
            report["steps"]["git_push"] = {"skipped": True, "reason": "no_changes"}
    else:
        report["steps"]["git_add"] = {"skipped": True, "reason": "no_sync_git"}
        report["steps"]["git_status"] = {"skipped": True, "reason": "no_sync_git"}
        report["steps"]["git_commit"] = {"skipped": True, "reason": "no_sync_git"}
        report["steps"]["git_push"] = {"skipped": True, "reason": "no_sync_git"}
        _write_report(report_path, report)

    return report
