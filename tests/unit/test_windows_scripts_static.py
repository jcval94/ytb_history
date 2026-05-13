from __future__ import annotations

from pathlib import Path


def _script(name: str) -> str:
    return Path("scripts", name).read_text(encoding="utf-8")


def test_manual_play_script_runs_sync_before_transcription_and_writes_report() -> None:
    content = _script("run_local_play.ps1")

    sync_index = content.index("run_local_repo_sync.ps1")
    transcription_index = content.index("run_local_transcription_automation.ps1")

    assert sync_index < transcription_index
    assert "latest_play_report.json" in content
    assert "manual_play_$timestamp.log" in content
    assert '"success", "up_to_date", "skipped_recent_success"' in content
    assert '"sync_failed_or_blocked"' in content
    assert "$PauseOnExit" in content
    assert "-Force" in content
    assert "Inicio manual: sincronizar repo" in content
    assert "Cola seleccionada" in content
    assert "Resultado transcripcion" in content
    assert "Presiona Enter para cerrar esta ventana" in content


def test_play_shortcut_installer_targets_desktop_shortcut() -> None:
    content = _script("install_local_play_shortcut.ps1")

    assert 'ShortcutName = "YTB History - Play Local Automation"' in content
    assert '[Environment]::GetFolderPath("Desktop")' in content
    assert "WScript.Shell" in content
    assert "run_local_play.ps1" in content
    assert "-PauseOnExit" in content
    assert "latest_play_shortcut.json" in content


def test_windows_transcription_runner_forwards_ytdlp_options() -> None:
    content = _script("run_local_transcription_automation.ps1")

    assert "[string]$YtdlpCookiesFile" in content
    assert "[string]$YtdlpBrowser" in content
    assert "[string]$YtdlpExtraArgs" in content
    assert "[string]$YtdlpCookiesB64" in content
    assert "--ytdlp-cookies-file" in content
    assert "--ytdlp-browser" in content
    assert "--ytdlp-extra-args" in content
    assert "--ytdlp-cookies-b64" in content
    assert "--progress-log" in content
    assert "--no-json-output" in content
    assert "Seleccion:" in content
    assert "Transcripcion:" in content


def test_task_registration_preserves_six_hour_windows_and_ytdlp_options() -> None:
    content = _script("register_local_transcription_task.ps1")

    assert '[string]$SyncStartAt = "09:00"' in content
    assert '[string]$TranscriptionStartAt = "09:20"' in content
    assert '[string]$WindowEndAt = "23:00"' in content
    assert "/SC HOURLY /MO 6" in content
    assert "-YtdlpCookiesFile" in content
    assert "-YtdlpBrowser" in content
    assert "-YtdlpExtraArgs" in content
    assert "-YtdlpCookiesB64" in content


def test_windows_sync_runner_uses_human_progress_output() -> None:
    content = _script("run_local_repo_sync.ps1")

    assert "--progress-log" in content
    assert "--no-json-output" in content
    assert "Repo ya estaba al dia con origin/main" in content
    assert "Sync bloqueado" in content
