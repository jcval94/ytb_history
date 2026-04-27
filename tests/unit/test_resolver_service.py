from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

from ytb_history.repositories.channel_registry_repo import ChannelRegistryRepo
from ytb_history.services import resolver_service



def _ok_response(*, channel_id: str, title: str, uploads: str) -> dict:
    return {
        "items": [
            {
                "id": channel_id,
                "snippet": {"title": title},
                "contentDetails": {"relatedPlaylists": {"uploads": uploads}},
            }
        ]
    }


def test_resolve_handle_valid(tmp_path) -> None:
    client = Mock()
    client.get_channel_by_handle.return_value = _ok_response(
        channel_id="UC_HANDLE",
        title="Handle Name",
        uploads="UU_HANDLE",
    )
    repo = ChannelRegistryRepo(tmp_path / "state" / "registry.jsonl")

    records = resolver_service.resolve_channels(
        ["https://www.youtube.com/@myhandle"],
        youtube_client=client,
        channel_registry_repo=repo,
    )

    assert len(records) == 1
    assert records[0].channel_id == "UC_HANDLE"
    assert records[0].uploads_playlist_id == "UU_HANDLE"
    assert records[0].resolver_status == "ok"
    assert records[0].channel_url == "https://www.youtube.com/@myhandle"
    client.get_channel_by_handle.assert_called_once_with("myhandle")



def test_handle_cache_prevents_second_api_call(tmp_path) -> None:
    client = Mock()
    client.get_channel_by_handle.return_value = _ok_response(
        channel_id="UC_HANDLE_CACHE",
        title="Handle Cache",
        uploads="UU_HANDLE_CACHE",
    )
    repo = ChannelRegistryRepo(tmp_path / "state" / "registry.jsonl")

    first_records = resolver_service.resolve_channels(
        ["https://www.youtube.com/@myhandle"],
        youtube_client=client,
        channel_registry_repo=repo,
    )

    assert len(first_records) == 1
    assert first_records[0].channel_url == "https://www.youtube.com/@myhandle"
    assert client.get_channel_by_handle.call_count == 1

    second_records = resolver_service.resolve_channels(
        ["https://www.youtube.com/@myhandle"],
        youtube_client=client,
        channel_registry_repo=repo,
    )

    assert len(second_records) == 1
    assert second_records[0].channel_url == "https://www.youtube.com/@myhandle"
    assert second_records[0].channel_id == "UC_HANDLE_CACHE"
    assert client.get_channel_by_handle.call_count == 1


def test_resolve_channel_id_valid(tmp_path) -> None:
    client = Mock()
    client.get_channel_by_id.return_value = _ok_response(
        channel_id="UC123",
        title="By Id",
        uploads="UU123",
    )
    repo = ChannelRegistryRepo(tmp_path / "state" / "registry.jsonl")

    records = resolver_service.resolve_channels(
        ["https://www.youtube.com/channel/UC123"],
        youtube_client=client,
        channel_registry_repo=repo,
    )

    assert len(records) == 1
    assert records[0].channel_url == "https://www.youtube.com/channel/UC123"
    client.get_channel_by_id.assert_called_once_with("UC123")


def test_normalize_trailing_slash(tmp_path) -> None:
    client = Mock()
    client.get_channel_by_handle.return_value = _ok_response(
        channel_id="UC1",
        title="Name",
        uploads="UU1",
    )
    repo = ChannelRegistryRepo(tmp_path / "state" / "registry.jsonl")

    records = resolver_service.resolve_channels(
        ["https://youtube.com/@MyHandle/"],
        youtube_client=client,
        channel_registry_repo=repo,
    )

    assert len(records) == 1
    client.get_channel_by_handle.assert_called_once_with("MyHandle")


def test_deduplicate_repeated_urls(tmp_path) -> None:
    client = Mock()
    client.get_channel_by_handle.return_value = _ok_response(
        channel_id="UC_DUP",
        title="Dup",
        uploads="UU_DUP",
    )
    repo = ChannelRegistryRepo(tmp_path / "state" / "registry.jsonl")

    records = resolver_service.resolve_channels(
        [
            "https://www.youtube.com/@dup",
            "https://www.youtube.com/@dup/",
            "https://youtube.com/@dup",
        ],
        youtube_client=client,
        channel_registry_repo=repo,
    )

    assert len(records) == 1
    client.get_channel_by_handle.assert_called_once_with("dup")


def test_uses_cache_if_channel_exists(tmp_path) -> None:
    repo = ChannelRegistryRepo(tmp_path / "state" / "registry.jsonl")
    cached = resolver_service.ChannelRecord(
        channel_url="https://www.youtube.com/channel/UC_CACHE",
        channel_id="UC_CACHE",
        channel_name="cached",
        uploads_playlist_id="UU_CACHE",
        resolved_at=resolver_service.datetime.now(resolver_service.timezone.utc),
        resolver_status="ok",
        error_message=None,
    )
    repo.save([cached])

    client = Mock()
    records = resolver_service.resolve_channels(
        ["https://www.youtube.com/channel/UC_CACHE"],
        youtube_client=client,
        channel_registry_repo=repo,
    )

    assert len(records) == 1
    assert records[0].uploads_playlist_id == "UU_CACHE"
    client.get_channel_by_id.assert_not_called()
    client.get_channel_by_handle.assert_not_called()


def test_client_error_records_error_and_continues(tmp_path) -> None:
    client = Mock()
    client.get_channel_by_handle.side_effect = RuntimeError("network down")
    client.get_channel_by_id.return_value = _ok_response(
        channel_id="UC_OK",
        title="ok",
        uploads="UU_OK",
    )
    repo = ChannelRegistryRepo(tmp_path / "state" / "registry.jsonl")

    records = resolver_service.resolve_channels(
        [
            "https://www.youtube.com/@broken",
            "https://www.youtube.com/channel/UC_OK",
        ],
        youtube_client=client,
        channel_registry_repo=repo,
    )

    assert len(records) == 2
    assert records[0].resolver_status == "error"
    assert "network down" in (records[0].error_message or "")
    assert records[1].resolver_status == "ok"


def test_no_search_list_usage() -> None:
    source = Path(resolver_service.__file__).read_text(encoding="utf-8")
    assert "search.list" not in source
