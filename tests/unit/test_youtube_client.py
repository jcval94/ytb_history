from __future__ import annotations

import importlib
import sys
import types
from typing import Any
from unittest.mock import Mock

import pytest

from ytb_history.utils.errors import MissingYouTubeApiKeyError, YouTubeApiError


@pytest.fixture
def youtube_module(monkeypatch: pytest.MonkeyPatch):
    fake_requests = types.ModuleType("requests")

    class RequestException(Exception):
        pass

    fake_requests.RequestException = RequestException
    fake_requests.get = Mock()
    monkeypatch.setitem(sys.modules, "requests", fake_requests)

    module = importlib.import_module("ytb_history.clients.youtube_client")
    module = importlib.reload(module)
    return module


@pytest.fixture
def no_sleep(youtube_module, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(youtube_module.time, "sleep", lambda *_args: None)


@pytest.fixture
def no_jitter(youtube_module, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(youtube_module.random, "uniform", lambda *_args: 0.0)


def _response(status_code: int, payload: dict[str, Any] | None = None) -> Mock:
    response = Mock()
    response.status_code = status_code
    response.json.return_value = payload or {"items": []}
    return response


def test_missing_api_key_raises(youtube_module, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("YOUTUBE_API_KEY", raising=False)

    with pytest.raises(MissingYouTubeApiKeyError):
        youtube_module.YouTubeClient()


def test_accepts_explicit_api_key(youtube_module, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("YOUTUBE_API_KEY", raising=False)
    client = youtube_module.YouTubeClient(api_key="test-key")

    assert client is not None


def test_get_videos_by_ids_validates_upper_bound(youtube_module) -> None:
    client = youtube_module.YouTubeClient(api_key="test-key")

    with pytest.raises(ValueError):
        client.get_videos_by_ids([str(i) for i in range(51)])


def test_list_playlist_items_validates_max_results(youtube_module) -> None:
    client = youtube_module.YouTubeClient(api_key="test-key")

    with pytest.raises(ValueError):
        client.list_playlist_items("playlist-id", max_results=51)


def test_get_channel_by_handle_uses_channels_with_for_handle(youtube_module) -> None:
    get_mock = Mock(return_value=_response(200, {"items": [{"id": "x"}]}))
    youtube_module.requests.get = get_mock
    client = youtube_module.YouTubeClient(api_key="test-key")

    client.get_channel_by_handle("@mychannel")

    called_url = get_mock.call_args.args[0]
    params = get_mock.call_args.kwargs["params"]
    assert called_url.endswith("/channels")
    assert params["forHandle"] == "mychannel"


def test_get_channel_by_id_uses_channels_with_id(youtube_module) -> None:
    get_mock = Mock(return_value=_response(200, {"items": [{"id": "x"}]}))
    youtube_module.requests.get = get_mock
    client = youtube_module.YouTubeClient(api_key="test-key")

    client.get_channel_by_id("UC123")

    called_url = get_mock.call_args.args[0]
    params = get_mock.call_args.kwargs["params"]
    assert called_url.endswith("/channels")
    assert params["id"] == "UC123"


def test_list_playlist_items_includes_page_token_only_when_passed(youtube_module) -> None:
    get_mock = Mock(return_value=_response(200))
    youtube_module.requests.get = get_mock
    client = youtube_module.YouTubeClient(api_key="test-key")

    client.list_playlist_items("PLxxx")
    params_without_token = get_mock.call_args.kwargs["params"]
    assert "pageToken" not in params_without_token

    client.list_playlist_items("PLxxx", page_token="TOKEN")
    params_with_token = get_mock.call_args.kwargs["params"]
    assert params_with_token["pageToken"] == "TOKEN"


def test_get_videos_by_ids_joins_ids(youtube_module) -> None:
    get_mock = Mock(return_value=_response(200))
    youtube_module.requests.get = get_mock
    client = youtube_module.YouTubeClient(api_key="test-key")

    client.get_videos_by_ids(["a", "b", "c"])

    params = get_mock.call_args.kwargs["params"]
    assert params["id"] == "a,b,c"


def test_params_include_fields(youtube_module) -> None:
    get_mock = Mock(return_value=_response(200))
    youtube_module.requests.get = get_mock
    client = youtube_module.YouTubeClient(api_key="test-key")

    client.get_channel_by_id("UC123")

    params = get_mock.call_args.kwargs["params"]
    assert "fields" in params


def test_no_legacy_public_list_methods(youtube_module) -> None:
    public_methods = {
        name
        for name in dir(youtube_module.YouTubeClient)
        if callable(getattr(youtube_module.YouTubeClient, name)) and not name.startswith("_")
    }

    assert "channels_list" not in public_methods
    assert "playlist_items_list" not in public_methods
    assert "videos_list" not in public_methods


def test_exact_public_methods(youtube_module) -> None:
    public_methods = {
        name
        for name in dir(youtube_module.YouTubeClient)
        if callable(getattr(youtube_module.YouTubeClient, name))
        and not name.startswith("_")
        and not (name.startswith("__") and name.endswith("__"))
    }

    assert public_methods == {
        "get_channel_by_handle",
        "get_channel_by_id",
        "list_playlist_items",
        "get_videos_by_ids",
    }


def test_no_public_search_methods(youtube_module) -> None:
    public_methods = {
        name
        for name in dir(youtube_module.YouTubeClient)
        if callable(getattr(youtube_module.YouTubeClient, name)) and not name.startswith("_")
    }

    assert "search" not in public_methods
    assert "search_list" not in public_methods


@pytest.mark.usefixtures("no_sleep", "no_jitter")
def test_retry_occurs_for_500_then_success(youtube_module) -> None:
    get_mock = Mock(side_effect=[_response(500), _response(200, {"items": [{"id": "ok"}]})])
    youtube_module.requests.get = get_mock
    client = youtube_module.YouTubeClient(api_key="test-key", max_retries=2)

    result = client.get_channel_by_id("UC123")

    assert result["items"][0]["id"] == "ok"
    assert get_mock.call_count == 2


@pytest.mark.usefixtures("no_sleep", "no_jitter")
def test_retry_occurs_for_429_then_success(youtube_module) -> None:
    get_mock = Mock(side_effect=[_response(429), _response(200, {"items": [{"id": "ok"}]})])
    youtube_module.requests.get = get_mock
    client = youtube_module.YouTubeClient(api_key="test-key", max_retries=2)

    result = client.get_channel_by_id("UC123")

    assert result["items"][0]["id"] == "ok"
    assert get_mock.call_count == 2


def test_no_retry_for_400(youtube_module) -> None:
    get_mock = Mock(return_value=_response(400))
    youtube_module.requests.get = get_mock
    client = youtube_module.YouTubeClient(api_key="test-key", max_retries=3)

    with pytest.raises(YouTubeApiError):
        client.get_channel_by_id("UC123")

    assert get_mock.call_count == 1


def test_errors_do_not_include_api_key(youtube_module) -> None:
    secret = "super-secret-key"
    get_mock = Mock(return_value=_response(400))
    youtube_module.requests.get = get_mock
    client = youtube_module.YouTubeClient(api_key=secret)

    with pytest.raises(YouTubeApiError) as exc:
        client.get_channel_by_id("UC123")

    assert secret not in str(exc.value)
