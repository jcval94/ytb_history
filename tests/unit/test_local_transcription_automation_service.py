from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from ytb_history.services.local_transcription_automation_service import run_local_transcription_automation


def _completed(command: list[str], *, stdout: str = "", returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(command, returncode, stdout=stdout, stderr="")


def _write_sync_report(tmp_path: Path, status: str = "up_to_date") -> None:
    report_path = tmp_path / "build" / "local_automation" / "latest_sync_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps({"status": status, "last_success_at": "2026-05-11T15:00:00+00:00"}, ensure_ascii=False), encoding="utf-8")


def test_run_local_transcription_automation_uses_stubs_and_skips_git_and_youtube(tmp_path: Path) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def _pipeline(**kwargs: Any) -> dict[str, Any]:
        raise AssertionError("pipeline should be skipped")

    def _record(name: str, payload: dict[str, Any]) -> dict[str, Any]:
        calls.append((name, payload))
        return {"status": "success", "name": name}

    report = run_local_transcription_automation(
        repo_dir=tmp_path,
        data_dir="custom_data",
        settings_path="custom_settings.yaml",
        limit=3,
        skip_youtube_refresh=True,
        no_sync_git=True,
        audio_source_dir="custom_audio",
        video_source_dir="custom_video",
        ytdlp_cookies_file="cookies.txt",
        ytdlp_browser="firefox",
        ytdlp_extra_args=["--force-ipv4"],
        ytdlp_cookies_b64="cookie64",
        pipeline_runner=_pipeline,
        candidate_selector=lambda **kwargs: _record("candidates", kwargs),
        transcription_runner=lambda **kwargs: _record("transcription", kwargs),
        insights_generator=lambda **kwargs: _record("insights", kwargs),
        registry_report_builder=lambda **kwargs: _record("registry", kwargs),
    )

    assert report["status"] == "success"
    assert report["steps"]["youtube_refresh"] == {"skipped": True, "reason": "skip_youtube_refresh"}
    assert report["steps"]["sync_preflight"] == {"skipped": True, "reason": "no_sync_git"}
    assert [name for name, _ in calls] == ["candidates", "transcription", "insights", "registry"]
    assert calls[0][1] == {"data_dir": str(tmp_path / "custom_data"), "limit": 3}
    assert calls[1][1] == {
        "data_dir": str(tmp_path / "custom_data"),
        "limit": 3,
        "audio_source_dir": str(tmp_path / "custom_audio"),
        "video_source_dir": str(tmp_path / "custom_video"),
        "ytdlp_cookies_file": "cookies.txt",
        "ytdlp_browser": "firefox",
        "ytdlp_extra_args": ["--force-ipv4"],
        "ytdlp_cookies_b64": "cookie64",
    }

    report_path = tmp_path / "build" / "local_automation" / "latest_run_report.json"
    assert report_path.exists()
    written = json.loads(report_path.read_text(encoding="utf-8"))
    assert written["steps"]["git_commit"] == {"skipped": True, "reason": "no_sync_git"}


def test_run_local_transcription_automation_commits_only_with_changes_after_successful_sync(tmp_path: Path) -> None:
    _write_sync_report(tmp_path)
    commands: list[list[str]] = []

    def _command_runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        if command[:3] == ["git", "status", "--porcelain"]:
            return _completed(command, stdout="M  data/example.json\n")
        return _completed(command)

    report = run_local_transcription_automation(
        repo_dir=tmp_path,
        limit=2,
        command_runner=_command_runner,
        pipeline_runner=lambda **kwargs: {"pipeline_kwargs": kwargs},
        candidate_selector=lambda **kwargs: {"candidate_kwargs": kwargs},
        transcription_runner=lambda **kwargs: {"transcribed_success": 1, "transcription_kwargs": kwargs},
        insights_generator=lambda **kwargs: {"insights_kwargs": kwargs},
        registry_report_builder=lambda **kwargs: {"registry_kwargs": kwargs},
    )

    assert report["status"] == "success"
    assert commands == [
        ["git", "add", "data/transcripts"],
        ["git", "status", "--porcelain", "--", "data/transcripts"],
        ["git", "commit", "-m", "Run local transcription automation"],
        ["git", "push"],
    ]
    assert report["git"]["has_changes"] is True
    assert report["git"]["publishable_outputs"] is True
    assert report["steps"]["youtube_refresh"]["pipeline_kwargs"] == {
        "settings_path": str(tmp_path / "config" / "settings.yaml"),
        "data_dir": str(tmp_path / "data"),
    }


def test_run_local_transcription_automation_does_not_commit_without_changes(tmp_path: Path) -> None:
    _write_sync_report(tmp_path)
    commands: list[list[str]] = []

    def _command_runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        return _completed(command, stdout="" if command[:3] == ["git", "status", "--porcelain"] else "")

    report = run_local_transcription_automation(
        repo_dir=tmp_path,
        skip_youtube_refresh=True,
        command_runner=_command_runner,
        candidate_selector=lambda **_kwargs: {},
        transcription_runner=lambda **_kwargs: {"transcribed_success": 1},
        insights_generator=lambda **_kwargs: {},
        registry_report_builder=lambda **_kwargs: {},
    )

    assert report["git"]["has_changes"] is False
    assert ["git", "commit", "-m", "Run local transcription automation"] not in commands
    assert ["git", "push"] not in commands
    assert report["steps"]["git_commit"] == {"skipped": True, "reason": "no_changes"}
    assert commands == [
        ["git", "add", "data/transcripts"],
        ["git", "status", "--porcelain", "--", "data/transcripts"],
    ]


def test_run_local_transcription_automation_skips_git_sync_without_publishable_outputs(tmp_path: Path) -> None:
    _write_sync_report(tmp_path)
    commands: list[list[str]] = []

    def _command_runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        return _completed(command)

    report = run_local_transcription_automation(
        repo_dir=tmp_path,
        skip_youtube_refresh=True,
        command_runner=_command_runner,
        candidate_selector=lambda **_kwargs: {},
        transcription_runner=lambda **_kwargs: {"transcribed_success": 0, "failed_audio_download": 2},
        insights_generator=lambda **_kwargs: {"generated": 0},
        registry_report_builder=lambda **_kwargs: {},
    )

    assert report["git"]["publishable_outputs"] is False
    assert report["git"]["has_changes"] is False
    assert report["warnings"] == ["git_sync_skipped_no_publishable_outputs"]
    assert report["steps"]["git_add"] == {"skipped": True, "reason": "no_publishable_outputs"}
    assert report["steps"]["git_status"] == {"skipped": True, "reason": "no_publishable_outputs"}
    assert report["steps"]["git_commit"] == {"skipped": True, "reason": "no_publishable_outputs"}
    assert report["steps"]["git_push"] == {"skipped": True, "reason": "no_publishable_outputs"}
    assert commands == []


def test_run_local_transcription_automation_stops_when_sync_report_is_blocked(tmp_path: Path) -> None:
    _write_sync_report(tmp_path, status="blocked_dirty_worktree")

    def _command_runner(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise AssertionError(f"git command should not run: {command}")

    report = run_local_transcription_automation(
        repo_dir=tmp_path,
        command_runner=_command_runner,
        pipeline_runner=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("pipeline should not run")),
    )

    assert report["status"] == "blocked_sync_dirty_worktree"
    assert report["warnings"] == ["sync_preflight_blocked:blocked_dirty_worktree"]
    assert "youtube_refresh" not in report["steps"]


def test_run_local_transcription_automation_allows_stale_repo_when_explicit(tmp_path: Path) -> None:
    _write_sync_report(tmp_path, status="blocked_dirty_worktree")

    report = run_local_transcription_automation(
        repo_dir=tmp_path,
        skip_youtube_refresh=True,
        allow_stale_repo=True,
        command_runner=lambda command, **_kwargs: _completed(command),
        candidate_selector=lambda **_kwargs: {},
        transcription_runner=lambda **_kwargs: {"transcribed_success": 0},
        insights_generator=lambda **_kwargs: {"generated": 0},
        registry_report_builder=lambda **_kwargs: {},
    )

    assert report["status"] == "success"
    assert "stale_repo_allowed_after_sync_status:blocked_dirty_worktree" in report["warnings"]


def test_runner_script_writes_schedule_state_without_utf8_bom() -> None:
    content = Path("scripts/run_local_transcription_automation.ps1").read_text(encoding="utf-8")

    assert "New-Object System.Text.UTF8Encoding $false" in content
    assert "Write-JsonNoBom -Path $statePath" in content
    assert "ConvertTo-Json -Depth 5 | Set-Content -Path $statePath -Encoding UTF8" not in content
