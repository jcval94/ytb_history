from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ytb_history.services.quota_service import (
    build_quota_report,
    classify_quota_status,
    estimate_discovery_cost,
    estimate_resolution_cost,
    estimate_total_quota_cost,
    estimate_tracking_cost,
    evaluate_quota_status,
    should_abort_run,
)


def test_quota_status_ok() -> None:
    assert evaluate_quota_status(
        total_estimated_units=100,
        operational_limit=7000,
        warning_limit=5000,
        soft_warning_limit=1000,
    ) == "ok"


def test_quota_status_soft_warning() -> None:
    assert evaluate_quota_status(
        total_estimated_units=1200,
        operational_limit=7000,
        warning_limit=5000,
        soft_warning_limit=1000,
    ) == "soft_warning"


def test_quota_status_warning() -> None:
    assert evaluate_quota_status(
        total_estimated_units=5500,
        operational_limit=7000,
        warning_limit=5000,
        soft_warning_limit=1000,
    ) == "warning"


def test_quota_status_over_limit() -> None:
    assert evaluate_quota_status(
        total_estimated_units=8000,
        operational_limit=7000,
        warning_limit=5000,
        soft_warning_limit=1000,
    ) == "over_operational_limit"


def test_estimate_tracking_cost_1200_batch_50() -> None:
    assert estimate_tracking_cost(1200, batch_size=50) == 24


def test_estimate_tracking_cost_1201_batch_50() -> None:
    assert estimate_tracking_cost(1201, batch_size=50) == 25


def test_estimate_tracking_cost_zero() -> None:
    assert estimate_tracking_cost(0) == 0


def test_estimate_tracking_cost_rejects_batch_size_gt_50() -> None:
    with pytest.raises(ValueError):
        estimate_tracking_cost(10, batch_size=51)


def test_estimate_discovery_cost_100_channels() -> None:
    assert estimate_discovery_cost(100, pages_per_channel=1) == 100


def test_estimate_resolution_cost_4_uncached() -> None:
    assert estimate_resolution_cost(4) == 4


def test_estimate_total_quota_cost_100_channels_1200_videos_zero_uncached() -> None:
    estimated = estimate_total_quota_cost(
        uncached_channels=0,
        channels_to_check=100,
        pages_per_channel=1,
        videos_to_track=1200,
        batch_size=50,
    )
    assert estimated == {
        "channels.list": 0,
        "playlistItems.list": 100,
        "videos.list": 24,
    }
    assert sum(estimated.values()) == 124


def test_estimate_total_quota_cost_100_channels_1200_videos_four_uncached() -> None:
    estimated = estimate_total_quota_cost(
        uncached_channels=4,
        channels_to_check=100,
        pages_per_channel=1,
        videos_to_track=1200,
        batch_size=50,
    )
    assert estimated == {
        "channels.list": 4,
        "playlistItems.list": 100,
        "videos.list": 24,
    }
    assert sum(estimated.values()) == 128


def test_classify_quota_status_ranges() -> None:
    assert classify_quota_status(999) == "ok"
    assert classify_quota_status(1000) == "soft_warning"
    assert classify_quota_status(5000) == "warning"
    assert classify_quota_status(7000) == "over_operational_limit"


def test_classify_quota_status_rejects_incoherent_limits() -> None:
    with pytest.raises(ValueError):
        classify_quota_status(
            100,
            operational_limit=4000,
            warning_limit=5000,
            soft_warning_limit=1000,
        )


def test_should_abort_run_true_when_total_meets_operational_limit() -> None:
    assert should_abort_run(7000, operational_limit=7000) is True


def test_build_quota_report_calculates_estimated_and_observed_totals() -> None:
    report = build_quota_report(
        execution_date=datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc),
        estimated_units={"channels.list": 4, "playlistItems.list": 100, "videos.list": 24},
        observed_units={"channels.list": 2, "playlistItems.list": 90, "videos.list": 20},
    )
    assert report.total_estimated_units == 128
    assert report.total_observed_units == 112
    assert report.limit_status == "ok"


def test_build_quota_report_marks_abort_when_over_operational_limit() -> None:
    report = build_quota_report(
        execution_date=datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc),
        estimated_units={"videos.list": 7000},
        observed_units=None,
        operational_limit=7000,
    )
    assert report.should_abort is True
    assert report.limit_status == "over_operational_limit"
    assert report.observed_units == {}


def test_estimate_total_quota_cost_does_not_include_search_list() -> None:
    estimated = estimate_total_quota_cost(
        uncached_channels=0,
        channels_to_check=1,
        pages_per_channel=1,
        videos_to_track=1,
    )
    assert "search.list" not in estimated
