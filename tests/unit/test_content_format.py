from __future__ import annotations

from ytb_history.domain.content_format import classify_content_format


def test_classifies_duration_lte_60_as_shorts() -> None:
    result = classify_content_format(duration_seconds=60, upload_date="2024-01-01T00:00:00+00:00")
    assert result.content_format == "shorts"
    assert result.reason == "duration_lte_60"


def test_classifies_three_minute_post_cutoff_with_shorts_marker_as_shorts() -> None:
    result = classify_content_format(
        duration_seconds=180,
        upload_date="2024-10-15T00:00:00+00:00",
        title="Nueva idea #Shorts",
    )
    assert result.content_format == "shorts"


def test_classifies_three_minute_post_cutoff_without_marker_as_videos() -> None:
    result = classify_content_format(
        duration_seconds=120,
        upload_date="2024-10-16T00:00:00+00:00",
        title="Tutorial compacto",
        tags=["tutorial"],
    )
    assert result.content_format == "videos"


def test_classifies_three_minute_pre_cutoff_as_videos_even_with_marker() -> None:
    result = classify_content_format(
        duration_seconds=120,
        upload_date="2024-10-14T23:59:59+00:00",
        title="#shorts antes del cambio",
    )
    assert result.content_format == "videos"


def test_classifies_missing_duration_as_unknown() -> None:
    result = classify_content_format(duration_seconds="", upload_date="2026-01-01T00:00:00+00:00")
    assert result.content_format == "unknown"
    assert result.reason == "missing_or_invalid_duration"
