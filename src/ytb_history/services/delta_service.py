"""Delta generation service."""

from __future__ import annotations

from collections.abc import Iterable

from ytb_history.domain.models import VideoDelta, VideoSnapshot


def _delta_int(current: int | None, previous: int | None) -> int | None:
    if current is None or previous is None:
        return None
    return current - previous


def build_deltas(current: Iterable[VideoSnapshot], previous: Iterable[VideoSnapshot]) -> list[VideoDelta]:
    prev_by_id = {item.video_id: item for item in previous}
    deltas: list[VideoDelta] = []

    for snap in current:
        prev = prev_by_id.get(snap.video_id)
        previous_views = prev.views if prev is not None else None
        previous_likes = prev.likes if prev is not None else None
        previous_comments = prev.comments if prev is not None else None
        previous_title = prev.title if prev is not None else None
        previous_description = prev.description if prev is not None else None
        previous_tags = prev.tags if prev is not None else None
        deltas.append(
            VideoDelta(
                execution_date=snap.execution_date,
                video_id=snap.video_id,
                views_delta=_delta_int(snap.views, previous_views),
                likes_delta=_delta_int(snap.likes, previous_likes),
                comments_delta=_delta_int(snap.comments, previous_comments),
                previous_views=previous_views,
                current_views=snap.views,
                previous_likes=previous_likes,
                current_likes=snap.likes,
                previous_comments=previous_comments,
                current_comments=snap.comments,
                is_new_video=prev is None,
                title_changed=snap.title != previous_title if prev is not None else False,
                description_changed=snap.description != previous_description if prev is not None else False,
                tags_changed=sorted(snap.tags) != sorted(previous_tags) if previous_tags is not None else False,
            )
        )

    return deltas
