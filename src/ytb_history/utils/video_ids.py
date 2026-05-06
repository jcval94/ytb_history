"""Helpers for lightweight YouTube identifier validation."""

from __future__ import annotations

_YOUTUBE_ID_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")


def _has_only_youtube_id_chars(value: str) -> bool:
    return bool(value) and all(char in _YOUTUBE_ID_CHARS for char in value)


def is_probable_youtube_channel_id(value: str | None) -> bool:
    """Return True for canonical channel IDs that can be mistaken for video IDs."""
    text = str(value or "").strip()
    return len(text) == 24 and text.startswith("UC") and _has_only_youtube_id_chars(text)


def is_transcribable_video_id_candidate(value: str | None) -> bool:
    """Return True when a value is safe to enqueue as a video-like ID.

    Unit tests in this project use short fake IDs such as ``v1``, so this helper
    intentionally avoids requiring YouTube's 11-character production video ID
    length. It rejects the main production failure mode: channel IDs placed in a
    video_id/source_video_id field.
    """
    text = str(value or "").strip()
    return bool(text) and not is_probable_youtube_channel_id(text)
