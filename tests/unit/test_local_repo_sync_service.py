from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from ytb_history.services.local_repo_sync_service import sync_local_repo


def _completed(command: list[str], *, stdout: str = "", stderr: str = "", returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(command, returncode, stdout=stdout, stderr=stderr)


def test_sync_local_repo_reports_up_to_date(tmp_path: Path) -> None:
    commands: list[list[str]] = []

    def _runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        if command == ["git", "branch", "--show-current"]:
            return _completed(command, stdout="main\n")
        if command[:2] == ["git", "ls-remote"]:
            return _completed(command, stdout="abc123\trefs/heads/main\n")
        if command == ["git", "rev-parse", "HEAD"]:
            return _completed(command, stdout="abc123\n")
        raise AssertionError(command)

    report = sync_local_repo(repo_dir=tmp_path, command_runner=_runner)

    assert report["status"] == "up_to_date"
    assert report["git"]["action"] == "no_op"
    assert report["git"]["branch"] == "main"
    assert commands == [
        ["git", "branch", "--show-current"],
        ["git", "ls-remote", "origin", "refs/heads/main"],
        ["git", "rev-parse", "HEAD"],
    ]


def test_sync_local_repo_pulls_fast_forward_when_clean(tmp_path: Path) -> None:
    commands: list[list[str]] = []

    def _runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        if command == ["git", "branch", "--show-current"]:
            return _completed(command, stdout="main\n")
        if command[:2] == ["git", "ls-remote"]:
            return _completed(command, stdout="remote123\trefs/heads/main\n")
        if command == ["git", "rev-parse", "HEAD"]:
            return _completed(command, stdout="local123\n")
        if command == ["git", "status", "--porcelain", "--untracked-files=no"]:
            return _completed(command)
        if command == ["git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"]:
            return _completed(command, stdout="origin/main\n")
        if command == ["git", "rev-list", "--left-right", "--count", "origin/main...HEAD"]:
            return _completed(command, stdout="1\t0\n")
        if command == ["git", "pull", "--ff-only"]:
            return _completed(command, stdout="Fast-forward\n")
        raise AssertionError(command)

    report = sync_local_repo(repo_dir=tmp_path, command_runner=_runner)

    assert report["status"] == "success"
    assert report["git"]["action"] == "pull_ff_only"
    assert report["git"]["branch"] == "main"
    assert ["git", "pull", "--ff-only"] in commands


def test_sync_local_repo_blocks_dirty_tracked_worktree(tmp_path: Path) -> None:
    def _runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        if command == ["git", "branch", "--show-current"]:
            return _completed(command, stdout="main\n")
        if command[:2] == ["git", "ls-remote"]:
            return _completed(command, stdout="remote123\trefs/heads/main\n")
        if command == ["git", "rev-parse", "HEAD"]:
            return _completed(command, stdout="local123\n")
        if command == ["git", "status", "--porcelain", "--untracked-files=no"]:
            return _completed(command, stdout=" M src/example.py\n")
        raise AssertionError(command)

    report = sync_local_repo(repo_dir=tmp_path, command_runner=_runner)

    assert report["status"] == "blocked_dirty_worktree"
    assert report["git"]["dirty_tracked_files"] == [" M src/example.py"]
    assert "dirty_worktree_blocks_pull" in report["warnings"]


def test_sync_local_repo_blocks_local_commits(tmp_path: Path) -> None:
    commands: list[list[str]] = []

    def _runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        if command == ["git", "branch", "--show-current"]:
            return _completed(command, stdout="main\n")
        if command[:2] == ["git", "ls-remote"]:
            return _completed(command, stdout="remote123\trefs/heads/main\n")
        if command == ["git", "rev-parse", "HEAD"]:
            return _completed(command, stdout="local123\n")
        if command == ["git", "status", "--porcelain", "--untracked-files=no"]:
            return _completed(command)
        if command == ["git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"]:
            return _completed(command, stdout="origin/main\n")
        if command == ["git", "rev-list", "--left-right", "--count", "origin/main...HEAD"]:
            return _completed(command, stdout="0\t2\n")
        raise AssertionError(command)

    report = sync_local_repo(repo_dir=tmp_path, command_runner=_runner)

    assert report["status"] == "blocked_non_fast_forward"
    assert "local_commits_block_automatic_pull" in report["warnings"]
    assert ["git", "pull", "--ff-only"] not in commands


def test_sync_local_repo_reports_ls_remote_failure(tmp_path: Path) -> None:
    report = sync_local_repo(
        repo_dir=tmp_path,
        command_runner=lambda command, **_kwargs: _completed(command, stdout="main\n") if command == ["git", "branch", "--show-current"] else _completed(command, stderr="network down", returncode=1),
    )

    assert report["status"] == "failed"
    assert report["warnings"] == ["git_ls_remote_failed"]


def test_sync_local_repo_skips_recent_success(tmp_path: Path) -> None:
    report_path = tmp_path / "build" / "local_automation" / "latest_sync_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(
            {
                "status": "success",
                "last_success_at": "2999-01-01T00:00:00+00:00",
                "git": {"remote_sha": "abc", "head_sha": "abc"},
            }
        ),
        encoding="utf-8",
    )

    report = sync_local_repo(
        repo_dir=tmp_path,
        min_success_interval_hours=6,
        command_runner=lambda command, **_kwargs: (_ for _ in ()).throw(AssertionError(command)),
    )

    assert report["status"] == "skipped_recent_success"
    assert report["last_success_at"] == "2999-01-01T00:00:00+00:00"


def test_sync_local_repo_switches_to_main_before_sync_when_clean(tmp_path: Path) -> None:
    commands: list[list[str]] = []
    branch_calls = 0

    def _runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        nonlocal branch_calls
        commands.append(command)
        if command == ["git", "branch", "--show-current"]:
            branch_calls += 1
            return _completed(command, stdout="feature/transcription\n" if branch_calls == 1 else "main\n")
        if command == ["git", "status", "--porcelain", "--untracked-files=no"]:
            return _completed(command)
        if command == ["git", "switch", "main"]:
            return _completed(command)
        if command[:2] == ["git", "ls-remote"]:
            return _completed(command, stdout="abc123\trefs/heads/main\n")
        if command == ["git", "rev-parse", "HEAD"]:
            return _completed(command, stdout="abc123\n")
        raise AssertionError(command)

    report = sync_local_repo(repo_dir=tmp_path, command_runner=_runner)

    assert report["status"] == "up_to_date"
    assert report["git"]["branch_before"] == "feature/transcription"
    assert report["git"]["branch_after_switch"] == "main"
    assert "switched_branch:feature/transcription->main" in report["warnings"]
    assert ["git", "switch", "main"] in commands


def test_sync_local_repo_blocks_branch_switch_when_dirty(tmp_path: Path) -> None:
    commands: list[list[str]] = []

    def _runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        if command == ["git", "branch", "--show-current"]:
            return _completed(command, stdout="feature/transcription\n")
        if command == ["git", "status", "--porcelain", "--untracked-files=no"]:
            return _completed(command, stdout=" M data/transcripts/transcript_registry.jsonl\n")
        raise AssertionError(command)

    report = sync_local_repo(repo_dir=tmp_path, command_runner=_runner)

    assert report["status"] == "blocked_wrong_branch_dirty_worktree"
    assert report["git"]["dirty_tracked_files"] == [" M data/transcripts/transcript_registry.jsonl"]
    assert "wrong_branch_with_dirty_worktree_blocks_switch" in report["warnings"]
    assert ["git", "switch", "main"] not in commands
