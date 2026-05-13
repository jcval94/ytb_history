"""Safe local repository synchronization for scheduled local jobs."""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

JsonReport = dict[str, Any]
CommandRunner = Callable[..., subprocess.CompletedProcess[str]]

SYNC_SUCCESS_STATUSES = {"success", "up_to_date", "skipped_recent_success"}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now_utc().isoformat()


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


def _first_stdout_token(report: JsonReport, index: int = 0) -> str:
    first_line = str(report.get("stdout", "")).strip().splitlines()[0]
    parts = first_line.split()
    return parts[index] if len(parts) > index else ""


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _recent_success(previous_report: JsonReport | None, *, min_success_interval_hours: float | None) -> bool:
    if min_success_interval_hours is None or not previous_report:
        return False
    if previous_report.get("status") not in SYNC_SUCCESS_STATUSES:
        return False
    last_success = _parse_iso_datetime(previous_report.get("last_success_at"))
    if last_success is None:
        return False
    return _now_utc() - last_success < timedelta(hours=min_success_interval_hours)


def _parse_ahead_counts(report: JsonReport) -> tuple[int, int] | None:
    text = str(report.get("stdout", "")).strip()
    if not text:
        return None
    parts = text.replace("\t", " ").split()
    if len(parts) < 2:
        return None
    try:
        upstream_only = int(parts[0])
        local_only = int(parts[1])
    except ValueError:
        return None
    return upstream_only, local_only


def sync_local_repo(
    *,
    repo_dir: str | Path,
    remote: str = "origin",
    branch: str = "main",
    report_path: str | Path | None = None,
    min_success_interval_hours: float | None = None,
    command_runner: CommandRunner = subprocess.run,
) -> JsonReport:
    """Synchronize the local repo with a remote branch without destructive Git commands."""
    repo_root = Path(repo_dir).expanduser().resolve()
    if report_path:
        candidate_report_path = Path(report_path)
        effective_report_path = candidate_report_path if candidate_report_path.is_absolute() else repo_root / candidate_report_path
    else:
        effective_report_path = repo_root / "build" / "local_automation" / "latest_sync_report.json"
    previous_report = _read_json(effective_report_path)

    report: JsonReport = {
        "generated_at": _now_iso(),
        "status": "unknown",
        "repo_dir": str(repo_root),
        "remote": remote,
        "branch": branch,
        "min_success_interval_hours": min_success_interval_hours,
        "steps": {},
        "git": {},
        "warnings": [],
    }

    if _recent_success(previous_report, min_success_interval_hours=min_success_interval_hours):
        report["status"] = "skipped_recent_success"
        report["last_success_at"] = previous_report.get("last_success_at")
        report["git"] = {
            "remote_sha": previous_report.get("git", {}).get("remote_sha"),
            "head_sha": previous_report.get("git", {}).get("head_sha"),
            "action": "skipped_recent_success",
        }
        _write_report(effective_report_path, report)
        return report

    remote_ref = f"refs/heads/{branch}"
    ls_remote = _run_command(["git", "ls-remote", remote, remote_ref], cwd=repo_root, command_runner=command_runner)
    report["steps"]["git_ls_remote"] = ls_remote
    if not ls_remote["ok"] or not str(ls_remote.get("stdout", "")).strip():
        report["status"] = "failed"
        report["warnings"].append("git_ls_remote_failed")
        _write_report(effective_report_path, report)
        return report
    remote_sha = _first_stdout_token(ls_remote)
    report["git"]["remote_sha"] = remote_sha

    head = _run_command(["git", "rev-parse", "HEAD"], cwd=repo_root, command_runner=command_runner)
    report["steps"]["git_rev_parse_head"] = head
    if not head["ok"]:
        report["status"] = "failed"
        report["warnings"].append("git_head_rev_parse_failed")
        _write_report(effective_report_path, report)
        return report
    head_sha = str(head.get("stdout", "")).strip()
    report["git"]["head_sha"] = head_sha

    if remote_sha == head_sha:
        report["status"] = "up_to_date"
        report["last_success_at"] = report["generated_at"]
        report["git"]["action"] = "no_op"
        _write_report(effective_report_path, report)
        return report

    dirty = _run_command(["git", "status", "--porcelain", "--untracked-files=no"], cwd=repo_root, command_runner=command_runner)
    report["steps"]["git_status_tracked"] = dirty
    if not dirty["ok"]:
        report["status"] = "failed"
        report["warnings"].append("git_status_failed")
        _write_report(effective_report_path, report)
        return report
    dirty_lines = [line for line in str(dirty.get("stdout", "")).splitlines() if line.strip()]
    report["git"]["dirty_tracked_files"] = dirty_lines
    if dirty_lines:
        report["status"] = "blocked_dirty_worktree"
        report["warnings"].append("dirty_worktree_blocks_pull")
        _write_report(effective_report_path, report)
        return report

    upstream = _run_command(["git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"], cwd=repo_root, command_runner=command_runner)
    report["steps"]["git_upstream"] = upstream
    if upstream["ok"]:
        upstream_ref = str(upstream.get("stdout", "")).strip()
        report["git"]["upstream_ref"] = upstream_ref
        ahead = _run_command(["git", "rev-list", "--left-right", "--count", f"{upstream_ref}...HEAD"], cwd=repo_root, command_runner=command_runner)
        report["steps"]["git_ahead_count"] = ahead
        counts = _parse_ahead_counts(ahead) if ahead["ok"] else None
        if counts is not None:
            upstream_only, local_only = counts
            report["git"]["upstream_only_commits"] = upstream_only
            report["git"]["local_only_commits"] = local_only
            if local_only > 0:
                report["status"] = "blocked_non_fast_forward"
                report["warnings"].append("local_commits_block_automatic_pull")
                _write_report(effective_report_path, report)
                return report
    else:
        report["warnings"].append("git_upstream_not_configured")

    pull = _run_command(["git", "pull", "--ff-only"], cwd=repo_root, command_runner=command_runner)
    report["steps"]["git_pull_ff_only"] = pull
    if not pull["ok"]:
        report["status"] = "blocked_non_fast_forward"
        report["warnings"].append("git_pull_ff_only_failed")
        _write_report(effective_report_path, report)
        return report

    new_head = _run_command(["git", "rev-parse", "HEAD"], cwd=repo_root, command_runner=command_runner)
    report["steps"]["git_rev_parse_after_pull"] = new_head
    if new_head["ok"]:
        report["git"]["head_sha_after_pull"] = str(new_head.get("stdout", "")).strip()
    report["status"] = "success"
    report["last_success_at"] = report["generated_at"]
    report["git"]["action"] = "pull_ff_only"
    _write_report(effective_report_path, report)
    return report
