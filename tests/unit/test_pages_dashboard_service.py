from __future__ import annotations

import json
from pathlib import Path

from ytb_history.services.pages_dashboard_service import build_pages_dashboard


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _prepare_minimal_analytics(data_dir: Path) -> None:
    latest_dir = data_dir / "analytics" / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    _write_text(latest_dir / "dashboard_index.json", json.dumps({"dashboard": "ok"}, ensure_ascii=False))
    _write_text(latest_dir / "analytics_manifest.json", json.dumps({"version": 1}, ensure_ascii=False))
    _write_text(
        latest_dir / "latest_video_metrics.csv",
        "video_id,views,is_new_video,title,ratio,empty_col,channel_name,duration_bucket\n"
        "v1,123,True,Canción,1.5,,Canal Uno,short\n"
        "v2,001,False,Título UTF-8,,,Canal Dos,long\n",
    )


def test_build_pages_dashboard_warns_when_dashboard_index_missing(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _write_text(data_dir / "analytics" / "latest" / "analytics_manifest.json", "{}")

    summary = build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    assert summary["status"] == "success_with_warnings"
    assert any("dashboard_index" in warning for warning in summary["warnings"])


def test_build_pages_dashboard_generates_dashboard_index_json(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    output_path = site_dir / "data" / "dashboard_index.json"
    assert output_path.exists()
    assert json.loads(output_path.read_text(encoding="utf-8")) == {"dashboard": "ok"}


def test_build_pages_dashboard_generates_site_manifest(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    manifest = json.loads((site_dir / "data" / "site_manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "pages_dashboard_v1"
    assert "latest_video_metrics" in manifest["tables"]


def test_build_pages_dashboard_converts_csv_to_json_with_columns_rows_and_types(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    payload = json.loads((site_dir / "data" / "latest_video_metrics.json").read_text(encoding="utf-8"))
    assert payload["columns"][:6] == ["video_id", "views", "is_new_video", "title", "ratio", "empty_col"]
    assert len(payload["rows"]) == 2

    row1 = payload["rows"][0]
    row2 = payload["rows"][1]
    assert row1["views"] == 123
    assert isinstance(row1["views"], int)
    assert row1["is_new_video"] is True
    assert row1["ratio"] == 1.5
    assert row1["empty_col"] is None
    assert row2["is_new_video"] is False
    assert row2["empty_col"] is None


def test_build_pages_dashboard_missing_csv_generates_empty_table_and_warning(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    summary = build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    missing_payload = json.loads((site_dir / "data" / "latest_channel_metrics.json").read_text(encoding="utf-8"))
    assert missing_payload["row_count"] == 0
    assert missing_payload["rows"] == []
    assert any("latest_channel_metrics" in warning for warning in summary["warnings"])


def test_build_pages_dashboard_copies_real_index_and_assets(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    assert "YouTube Intelligence Dashboard" in (site_dir / "index.html").read_text(encoding="utf-8")
    for asset in ["styles.css", "app.js", "formatters.js", "tables.js", "charts.js"]:
        assert (site_dir / "assets" / asset).exists()


def test_index_html_references_relative_assets(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    index_html = (site_dir / "index.html").read_text(encoding="utf-8")
    assert './assets/styles.css' in index_html
    assert './assets/app.js' in index_html


def test_app_js_uses_relative_data_paths_and_hardening_rules(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    app_js = (site_dir / "assets" / "app.js").read_text(encoding="utf-8")
    assert "./data/site_manifest.json" in app_js
    assert "./data/latest_video_metrics.json" in app_js
    assert "search.list" not in app_js
    assert "http://" not in app_js
    assert "https://" not in app_js
    assert "low_confidence_flag" not in app_js
    assert "metric_confidence_score" in app_js
    assert "< 50" in app_js
    assert "success_horizon_label" in app_js


def test_dashboard_has_expected_sections(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    index_html = (site_dir / "index.html").read_text(encoding="utf-8")
    for label in ["Overview", "Videos", "Channels", "Scores", "Advanced", "Titles", "Periods", "Data Quality"]:
        assert label in index_html


def test_build_pages_dashboard_does_not_write_outside_site_dir(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    summary = build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    site_root = site_dir.resolve()
    for output_file in summary["files_written"]:
        assert Path(output_file).resolve().is_relative_to(site_root)


def test_build_pages_dashboard_no_api_calls(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    def _boom(*_args, **_kwargs):
        raise AssertionError("YouTube API should not be called")

    monkeypatch.setattr("ytb_history.clients.youtube_client.YouTubeClient.__init__", _boom)

    summary = build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    assert summary["status"] in {"success", "success_with_warnings"}


def test_build_pages_dashboard_creates_site_index_and_site_manifest(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site"
    _prepare_minimal_analytics(data_dir)

    build_pages_dashboard(data_dir=data_dir, site_dir=site_dir)

    assert (site_dir / "index.html").exists()
    assert (site_dir / "data" / "site_manifest.json").exists()
