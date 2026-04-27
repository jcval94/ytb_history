from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from ytb_history.clients.quota_meter import QuotaMeter
from ytb_history.orchestrator import run_dry_run, run_pipeline
from ytb_history.repositories.run_report_repo import RunReportRepo


class FakeYouTubeClient:
    def __init__(
        self,
        *,
        resolve_error_handles: set[str] | None = None,
        unavailable_video_ids: set[str] | None = None,
    ) -> None:
        self.quota_meter = QuotaMeter()
        self.resolve_error_handles = resolve_error_handles or set()
        self.unavailable_video_ids = unavailable_video_ids or set()

        self.playlist_items_calls = 0
        self.videos_calls = 0

    def get_channel_by_handle(self, handle: str) -> dict:
        self.quota_meter.add_endpoint("channels.list")
        if handle in self.resolve_error_handles:
            raise RuntimeError(f"cannot resolve {handle}")
        return {
            "items": [
                {
                    "id": f"UC_{handle}",
                    "snippet": {"title": f"Channel {handle}"},
                    "contentDetails": {"relatedPlaylists": {"uploads": f"UU_{handle}"}},
                }
            ]
        }

    def get_channel_by_id(self, channel_id: str) -> dict:
        self.quota_meter.add_endpoint("channels.list")
        return {
            "items": [
                {
                    "id": channel_id,
                    "snippet": {"title": f"Channel {channel_id}"},
                    "contentDetails": {"relatedPlaylists": {"uploads": f"UU_{channel_id}"}},
                }
            ]
        }

    def list_playlist_items(self, playlist_id: str, page_token: str | None = None, max_results: int = 50) -> dict:
        del page_token, max_results
        self.quota_meter.add_endpoint("playlistItems.list")
        self.playlist_items_calls += 1
        video_id = f"v_{playlist_id}"
        return {
            "items": [
                {
                    "contentDetails": {
                        "videoId": video_id,
                        "videoPublishedAt": "2026-04-27T00:00:00Z",
                    },
                    "snippet": {"publishedAt": "2026-04-27T00:00:00Z"},
                }
            ]
        }

    def get_videos_by_ids(self, video_ids: list[str]) -> dict:
        self.quota_meter.add_endpoint("videos.list")
        self.videos_calls += 1
        items: list[dict] = []
        for vid in video_ids:
            if vid in self.unavailable_video_ids:
                continue
            items.append(
                {
                    "id": vid,
                    "snippet": {
                        "channelId": "UC_any",
                        "channelTitle": "Channel any",
                        "title": f"Title {vid}",
                        "description": f"Desc {vid}",
                        "publishedAt": "2026-04-27T00:00:00Z",
                        "tags": ["tag"],
                        "thumbnails": {"high": {"url": "https://img/high.jpg"}},
                    },
                    "contentDetails": {"duration": "PT1M"},
                    "statistics": {"viewCount": "10", "likeCount": "1", "commentCount": "0"},
                }
            )
        return {"items": items}

    def search_list(self, *_args, **_kwargs) -> dict:
        raise AssertionError("search.list must never be called")


EXECUTION_DATE = datetime(2026, 4, 27, 9, 5, 1, tzinfo=timezone.utc)


def _write_settings(
    path: Path,
    *,
    operational_limit: int = 7000,
    warning_limit: int = 5000,
    soft_warning_limit: int = 1000,
) -> None:
    path.write_text(
        "\n".join(
            [
                "discovery_window_days: 7",
                "tracking_window_days: 183",
                "youtube_batch_size: 50",
                f"operational_quota_limit: {operational_limit}",
                f"warning_quota_limit: {warning_limit}",
                f"soft_warning_quota_limit: {soft_warning_limit}",
                "max_pages_per_channel: 5",
            ]
        ),
        encoding="utf-8",
    )


def test_run_pipeline_happy_path_and_persists_outputs(tmp_path: Path) -> None:
    client = FakeYouTubeClient()
    settings_path = tmp_path / "settings.yaml"
    _write_settings(settings_path)

    result = run_pipeline(
        execution_date=EXECUTION_DATE,
        channel_urls=["https://www.youtube.com/@alpha"],
        settings_path=settings_path,
        data_dir=tmp_path,
        youtube_client=client,
    )

    assert result["status"] == "success"
    assert result["channels_ok"] == 1
    assert result["videos_discovered"] == 1
    assert result["videos_tracked"] == 1
    assert result["videos_snapshotted"] == 1
    assert result["videos_unavailable"] == 0
    assert result["observed_quota_units"] == 3

    assert (tmp_path / "state" / "channel_registry.jsonl").exists()
    assert (tmp_path / "state" / "tracked_videos_catalog.jsonl").exists()
    assert Path(result["snapshot_path"]).exists()
    assert Path(result["delta_path"]).exists()
    assert Path(result["quota_report_path"]).exists()
    assert Path(result["run_summary_path"]).exists()
    assert Path(result["discovery_report_path"]).exists()
    assert Path(result["channel_errors_path"]).exists()


def test_run_pipeline_aborts_before_enrichment_when_quota_guardrail_hits(tmp_path: Path) -> None:
    client = FakeYouTubeClient()
    settings_path = tmp_path / "settings.yaml"
    _write_settings(settings_path, operational_limit=1, warning_limit=1, soft_warning_limit=1)

    result = run_pipeline(
        execution_date=EXECUTION_DATE,
        channel_urls=["https://www.youtube.com/@alpha"],
        settings_path=settings_path,
        data_dir=tmp_path,
        youtube_client=client,
    )

    assert result["status"] == "aborted_quota_guardrail"
    assert client.videos_calls == 0
    assert not (tmp_path / "snapshots").exists()
    assert not (tmp_path / "deltas").exists()
    assert Path(result["quota_report_path"]).exists()
    assert Path(result["run_summary_path"]).exists()


def test_partial_channel_error_and_unavailable_video_returns_success_with_warnings(tmp_path: Path) -> None:
    client = FakeYouTubeClient(resolve_error_handles={"bad"}, unavailable_video_ids={"v_UU_ok"})
    settings_path = tmp_path / "settings.yaml"
    _write_settings(settings_path)

    result = run_pipeline(
        execution_date=EXECUTION_DATE,
        channel_urls=[
            "https://www.youtube.com/@ok",
            "https://www.youtube.com/@bad",
        ],
        settings_path=settings_path,
        data_dir=tmp_path,
        youtube_client=client,
    )

    assert result["status"] == "success_with_warnings"
    assert result["channels_total"] == 2
    assert result["channels_failed"] == 1
    assert result["videos_unavailable"] == 1

    repo = RunReportRepo(base_dir=tmp_path / "reports")
    channel_errors = repo.load_channel_errors(result["channel_errors_path"])
    assert any(item.get("stage") == "resolver" for item in channel_errors)


def test_run_pipeline_uses_tmp_data_dir_and_never_uses_search_list(tmp_path: Path) -> None:
    client = FakeYouTubeClient()
    settings_path = tmp_path / "settings.yaml"
    _write_settings(settings_path)

    run_pipeline(
        execution_date=EXECUTION_DATE,
        channel_urls=["https://www.youtube.com/@alpha"],
        settings_path=settings_path,
        data_dir=tmp_path,
        youtube_client=client,
    )

    assert (tmp_path / "reports").exists()


def test_catalog_updates_with_new_video(tmp_path: Path) -> None:
    client = FakeYouTubeClient()
    settings_path = tmp_path / "settings.yaml"
    _write_settings(settings_path)

    run_pipeline(
        execution_date=EXECUTION_DATE,
        channel_urls=["https://www.youtube.com/@alpha"],
        settings_path=settings_path,
        data_dir=tmp_path,
        youtube_client=client,
    )

    catalog_path = tmp_path / "state" / "tracked_videos_catalog.jsonl"
    rows = [json.loads(line) for line in catalog_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any(row["video_id"] == "v_UU_alpha" for row in rows)


def test_dry_run_does_not_call_api_and_does_not_write_snapshots_or_deltas(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.yaml"
    _write_settings(settings_path)

    result = run_dry_run(
        execution_date=EXECUTION_DATE,
        channel_urls=["https://www.youtube.com/@alpha"],
        settings_path=settings_path,
        data_dir=tmp_path,
    )

    assert "estimated_units" in result
    assert "should_abort" in result
    assert not (tmp_path / "snapshots").exists()
    assert not (tmp_path / "deltas").exists()
