"""Configuration loading helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

DEFAULT_SETTINGS: dict[str, int] = {
    "discovery_window_days": 7,
    "tracking_window_days": 183,
    "youtube_batch_size": 50,
    "operational_quota_limit": 7000,
    "warning_quota_limit": 5000,
    "soft_warning_quota_limit": 1000,
    "max_pages_per_channel": 5,
}



def load_settings(path: str | Path = "config/settings.yaml") -> dict[str, int]:
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

    resolved = dict(DEFAULT_SETTINGS)
    for key in DEFAULT_SETTINGS:
        if key in loaded and loaded[key] is not None:
            resolved[key] = int(loaded[key])

    return resolved
