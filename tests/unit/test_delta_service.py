from datetime import datetime, timezone

from ytb_history.domain.models import VideoSnapshot
from ytb_history.services.delta_service import build_deltas


def _snap(
    *,
    video_id: str,
    title: str,
    views: int | None,
    likes: int | None,
    comments: int | None,
    tags: list[str],
    description: str,
) -> VideoSnapshot:
    return VideoSnapshot(
        execution_date=datetime(2026, 4, 25, tzinfo=timezone.utc),
        channel_name="channel",
        video_id=video_id,
        title=title,
        description=description,
        upload_date=datetime(2026, 4, 20, tzinfo=timezone.utc),
        tags=tags,
        thumbnail_url="http://example.com/thumb.jpg",
        duration_seconds=100,
        views=views,
        likes=likes,
        comments=comments,
    )


def test_build_deltas_computes_numeric_and_flags() -> None:
    previous = [_snap(video_id="v1", title="A", views=100, likes=10, comments=5, tags=["x"], description="d1")]
    current = [_snap(video_id="v1", title="B", views=130, likes=12, comments=8, tags=["x", "y"], description="d2")]

    deltas = build_deltas(current=current, previous=previous)
    assert len(deltas) == 1
    delta = deltas[0]
    assert delta.views_delta == 30
    assert delta.likes_delta == 2
    assert delta.comments_delta == 3
    assert delta.previous_views == 100
    assert delta.current_views == 130
    assert delta.previous_likes == 10
    assert delta.current_likes == 12
    assert delta.previous_comments == 5
    assert delta.current_comments == 8
    assert delta.is_new_video is False
    assert delta.title_changed is True
    assert delta.description_changed is True
    assert delta.tags_changed is True


def test_build_deltas_emits_new_video_when_missing_previous() -> None:
    current = [_snap(video_id="v2", title="A", views=10, likes=1, comments=1, tags=[], description="d")]
    deltas = build_deltas(current=current, previous=[])
    assert len(deltas) == 1
    delta = deltas[0]
    assert delta.video_id == "v2"
    assert delta.is_new_video is True
    assert delta.views_delta is None
    assert delta.likes_delta is None
    assert delta.comments_delta is None
    assert delta.previous_views is None
    assert delta.current_views == 10


def test_build_deltas_compares_tags_without_order() -> None:
    previous = [_snap(video_id="v3", title="A", views=100, likes=10, comments=5, tags=["x", "y"], description="d")]
    current = [_snap(video_id="v3", title="A", views=100, likes=10, comments=5, tags=["y", "x"], description="d")]
    deltas = build_deltas(current=current, previous=previous)
    assert len(deltas) == 1
    assert deltas[0].tags_changed is False
