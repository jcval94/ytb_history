"""Configuration loading helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

DEFAULT_SETTINGS: dict[str, Any] = {
    "discovery_window_days": 7,
    "tracking_window_days": 183,
    "youtube_batch_size": 50,
    "operational_quota_limit": 7000,
    "warning_quota_limit": 5000,
    "soft_warning_quota_limit": 1000,
    "max_pages_per_channel": 5,
    "execution_timezone": "local",
    "transcription": {
        "daily_ranked_limit": 10,
        "forced_channels_enabled": True,
        "forced_channels_max_per_run": 50,
        "forced_channels_new_video_window_days": 14,
        "retry_cooldown_days": 7,
        "default_transcription_model": "gpt-4o-mini-transcribe",
        "default_insights_model": "gpt-5.5-mini",
        "max_transcriptions_per_run": 60,
    },
}



def load_settings(path: str | Path = "config/settings.yaml") -> dict[str, Any]:
    """Load settings YAML and fill missing keys with safe defaults."""
    settings_path = Path(path)
    try:
        if not settings_path.exists():
            loaded: dict[str, Any] = {}
        else:
            raw = yaml.safe_load(settings_path.read_text(encoding="utf-8"))
            loaded = raw if isinstance(raw, dict) else {}
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in settings file {settings_path}: {exc}") from exc

    resolved: dict[str, Any] = dict(DEFAULT_SETTINGS)
    for key in (
        "discovery_window_days",
        "tracking_window_days",
        "youtube_batch_size",
        "operational_quota_limit",
        "warning_quota_limit",
        "soft_warning_quota_limit",
        "max_pages_per_channel",
    ):
        if key in loaded and loaded[key] is not None:
            resolved[key] = int(loaded[key])

    execution_timezone = loaded.get("execution_timezone")
    if execution_timezone is not None:
        resolved["execution_timezone"] = str(execution_timezone).strip() or "local"

    transcription_loaded = loaded.get("transcription")
    transcription_defaults = dict(DEFAULT_SETTINGS["transcription"])
    if isinstance(transcription_loaded, dict):
        for key in (
            "daily_ranked_limit",
            "forced_channels_max_per_run",
            "forced_channels_new_video_window_days",
            "retry_cooldown_days",
            "max_transcriptions_per_run",
        ):
            if key in transcription_loaded and transcription_loaded[key] is not None:
                transcription_defaults[key] = int(transcription_loaded[key])
        if "forced_channels_enabled" in transcription_loaded:
            transcription_defaults["forced_channels_enabled"] = bool(transcription_loaded["forced_channels_enabled"])
        for key in ("default_transcription_model", "default_insights_model"):
            if key in transcription_loaded and transcription_loaded[key] is not None:
                transcription_defaults[key] = str(transcription_loaded[key]).strip() or transcription_defaults[key]
    resolved["transcription"] = transcription_defaults

    return resolved
