from __future__ import annotations

from ytb_history.clients.quota_meter import QuotaMeter


def test_quota_meter_sums_supported_endpoints() -> None:
    meter = QuotaMeter()

    meter.add_endpoint("channels.list")
    meter.add_endpoint("playlistItems.list", requests=2)
    meter.add_endpoint("videos.list", requests=3)

    data = meter.as_dict()
    assert data["channels.list"] == 1
    assert data["playlistItems.list"] == 2
    assert data["videos.list"] == 3
    assert meter.total == 6


def test_quota_meter_reset() -> None:
    meter = QuotaMeter()
    meter.add_endpoint("channels.list")

    meter.reset()

    assert meter.as_dict() == {}
    assert meter.total == 0
