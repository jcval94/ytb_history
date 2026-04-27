from __future__ import annotations

from datetime import datetime, timezone

from ytb_history.services.enrichment_service import fetch_video_snapshots


class DummyQuotaMeter:
    def __init__(self) -> None:
        self.calls = 0

    def add_endpoint(self, *_args, **_kwargs) -> None:
        self.calls += 1


class FakeYouTubeClient:
    def __init__(self, responses: list[dict] | None = None, *, fail_on_calls: set[int] | None = None) -> None:
        self.responses = responses or []
        self.fail_on_calls = fail_on_calls or set()
        self.calls: list[list[str]] = []
        self.quota_meter = DummyQuotaMeter()

    def get_videos_by_ids(self, video_ids: list[str]) -> dict:
        self.calls.append(video_ids)
        call_number = len(self.calls)
        if call_number in self.fail_on_calls:
            raise RuntimeError(f"boom-{call_number}")
        if call_number <= len(self.responses):
            return self.responses[call_number - 1]
        return {"items": []}

    def search_list(self, *_args, **_kwargs) -> dict:  # pragma: no cover
        raise AssertionError("search.list must never be called")


def _item(
    *,
    video_id: str,
    channel_id: str = "UC1",
    thumbnails: dict | None = None,
    statistics: dict | None = None,
    duration: str | None = "PT1H2M3S",
    published_at: str | None = "2026-04-26T10:00:00Z",
    tags: list[str] | None = None,
    title: str | None = "title",
    description: str | None = "desc",
) -> dict:
    snippet: dict = {
        "channelId": channel_id,
        "channelTitle": "Channel 1",
        "publishedAt": published_at,
    }
    if thumbnails is not None:
        snippet["thumbnails"] = thumbnails
    if tags is not None:
        snippet["tags"] = tags
    if title is not None:
        snippet["title"] = title
    if description is not None:
        snippet["description"] = description

    return {
        "id": video_id,
        "snippet": snippet,
        "statistics": ({"viewCount": "10", "likeCount": "2", "commentCount": "1"} if statistics is None else statistics),
        "contentDetails": {"duration": duration} if duration is not None else {},
    }


def test_fetch_video_snapshots_deduplicates_preserving_order() -> None:
    client = FakeYouTubeClient(responses=[{"items": [_item(video_id="v1"), _item(video_id="v2")]}])

    result = fetch_video_snapshots(
        ["v1", "v1", "v2"],
        youtube_client=client,
        execution_date=datetime(2026, 4, 27, tzinfo=timezone.utc),
    )

    assert client.calls == [["v1", "v2"]]
    assert [snapshot.video_id for snapshot in result.snapshots] == ["v1", "v2"]


def test_fetch_video_snapshots_batches_max_50_ids() -> None:
    ids = [f"v{i}" for i in range(51)]
    client = FakeYouTubeClient(
        responses=[
            {"items": [_item(video_id=video_id) for video_id in ids[:50]]},
            {"items": [_item(video_id=ids[50])]},
        ]
    )

    fetch_video_snapshots(ids, youtube_client=client, execution_date=datetime(2026, 4, 27, tzinfo=timezone.utc))

    assert len(client.calls) == 2
    assert len(client.calls[0]) == 50
    assert len(client.calls[1]) == 1


def test_fetch_video_snapshots_normalizes_full_snapshot() -> None:
    client = FakeYouTubeClient(
        responses=[
            {
                "items": [
                    _item(
                        video_id="v1",
                        channel_id="UC999",
                        tags=["a", "b"],
                        thumbnails={
                            "high": {"url": "https://img/high.jpg"},
                            "default": {"url": "https://img/default.jpg"},
                        },
                    )
                ]
            }
        ]
    )

    result = fetch_video_snapshots(["v1"], youtube_client=client, execution_date=datetime(2026, 4, 27, tzinfo=timezone.utc))

    snap = result.snapshots[0]
    assert snap.channel_id == "UC999"
    assert snap.views == 10
    assert snap.likes == 2
    assert snap.comments == 1
    assert snap.tags == ["a", "b"]
    assert snap.description == "desc"
    assert snap.thumbnail_url == "https://img/high.jpg"
    assert snap.duration_seconds == 3723


def test_fetch_video_snapshots_handles_missing_optionals() -> None:
    client = FakeYouTubeClient(
        responses=[
            {
                "items": [
                    _item(
                        video_id="v1",
                        statistics={},
                        duration=None,
                        tags=None,
                        title=None,
                        description=None,
                        thumbnails={"default": {"url": "https://img/default.jpg"}},
                    )
                ]
            }
        ]
    )

    result = fetch_video_snapshots(["v1"], youtube_client=client, execution_date=datetime(2026, 4, 27, tzinfo=timezone.utc))

    snap = result.snapshots[0]
    assert snap.views is None
    assert snap.likes is None
    assert snap.comments is None
    assert snap.tags == []
    assert snap.title == ""
    assert snap.description == ""
    assert snap.duration_seconds == 0


def test_fetch_video_snapshots_thumbnail_fallback_order() -> None:
    client = FakeYouTubeClient(
        responses=[
            {
                "items": [
                    _item(
                        video_id="v1",
                        thumbnails={
                            "default": {"url": "https://img/default.jpg"},
                            "medium": {"url": "https://img/medium.jpg"},
                            "high": {"url": "https://img/high.jpg"},
                            "maxres": {"url": "https://img/maxres.jpg"},
                        },
                    ),
                    _item(
                        video_id="v2",
                        thumbnails={
                            "default": {"url": "https://img/default.jpg"},
                            "medium": {"url": "https://img/medium.jpg"},
                        },
                    ),
                ]
            }
        ]
    )

    result = fetch_video_snapshots(["v1", "v2"], youtube_client=client, execution_date=datetime(2026, 4, 27, tzinfo=timezone.utc))

    assert result.snapshots[0].thumbnail_url == "https://img/maxres.jpg"
    assert result.snapshots[1].thumbnail_url == "https://img/medium.jpg"


def test_fetch_video_snapshots_marks_unavailable_ids() -> None:
    client = FakeYouTubeClient(responses=[{"items": [_item(video_id="v1")]}])

    result = fetch_video_snapshots(
        ["v1", "missing"],
        youtube_client=client,
        execution_date=datetime(2026, 4, 27, tzinfo=timezone.utc),
    )

    assert result.unavailable_video_ids == ["missing"]


def test_fetch_video_snapshots_continues_after_batch_error() -> None:
    ids = [f"v{i}" for i in range(55)]
    client = FakeYouTubeClient(
        responses=[
            {"items": [_item(video_id=video_id) for video_id in ids[:50]]},
            {"items": [_item(video_id=video_id) for video_id in ids[50:]]},
        ],
        fail_on_calls={1},
    )

    result = fetch_video_snapshots(ids, youtube_client=client, execution_date=datetime(2026, 4, 27, tzinfo=timezone.utc))

    assert len(result.errors) == 1
    assert len(result.snapshots) == 5
    assert len(result.unavailable_video_ids) == 50


def test_fetch_video_snapshots_invalid_upload_date_uses_execution_date() -> None:
    client = FakeYouTubeClient(
        responses=[{"items": [_item(video_id="v1", published_at="invalid")]}],
    )
    execution_date = datetime(2026, 4, 27, tzinfo=timezone.utc)

    result = fetch_video_snapshots(["v1"], youtube_client=client, execution_date=execution_date)

    assert result.snapshots[0].upload_date == execution_date
    assert result.errors


def test_fetch_video_snapshots_does_not_call_search_and_quota_meter_directly() -> None:
    client = FakeYouTubeClient(responses=[{"items": [_item(video_id="v1")]}])

    fetch_video_snapshots(["v1"], youtube_client=client, execution_date=datetime(2026, 4, 27, tzinfo=timezone.utc))

    assert client.quota_meter.calls == 0
