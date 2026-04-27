from __future__ import annotations

from datetime import datetime, timezone

from ytb_history.domain.models import ChannelRecord
from ytb_history.repositories.channel_registry_repo import ChannelRegistryRepo


def _record(
    *,
    channel_url: str,
    channel_id: str,
    uploads_playlist_id: str,
    status: str = "ok",
    error_message: str | None = None,
    second: int = 0,
) -> ChannelRecord:
    return ChannelRecord(
        channel_url=channel_url,
        channel_id=channel_id,
        channel_name="name",
        uploads_playlist_id=uploads_playlist_id,
        resolved_at=datetime(2026, 4, 25, 0, 0, second, tzinfo=timezone.utc),
        resolver_status=status,
        error_message=error_message,
    )


def test_channel_registry_repo_saves_and_loads_jsonl(tmp_path) -> None:
    path = tmp_path / "state" / "channel_registry.jsonl"
    repo = ChannelRegistryRepo(path=path)

    repo.save(
        [
            _record(
                channel_url="https://www.youtube.com/channel/UC1",
                channel_id="UC1",
                uploads_playlist_id="UU1",
            )
        ]
    )

    rows = repo.load()
    assert len(rows) == 1
    assert rows[0].channel_id == "UC1"
    assert path.exists()


def test_channel_registry_repo_deduplicates_by_channel_id_and_keeps_errors(tmp_path) -> None:
    path = tmp_path / "state" / "channel_registry.jsonl"
    repo = ChannelRegistryRepo(path=path)

    repo.save(
        [
            _record(
                channel_url="https://www.youtube.com/channel/UC1",
                channel_id="UC1",
                uploads_playlist_id="UU_OLD",
                second=1,
            ),
            _record(
                channel_url="https://www.youtube.com/channel/UC1",
                channel_id="UC1",
                uploads_playlist_id="UU_NEW",
                second=2,
            ),
            _record(
                channel_url="https://www.youtube.com/@broken",
                channel_id="",
                uploads_playlist_id="",
                status="error",
                error_message="boom",
                second=3,
            ),
        ]
    )

    rows = repo.load()
    ok_rows = [row for row in rows if row.resolver_status == "ok"]
    error_rows = [row for row in rows if row.resolver_status == "error"]

    assert len(ok_rows) == 1
    assert ok_rows[0].uploads_playlist_id == "UU_NEW"
    assert len(error_rows) == 1
    assert error_rows[0].error_message == "boom"
