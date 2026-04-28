from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _read(relpath: str) -> str:
    return (REPO_ROOT / relpath).read_text(encoding="utf-8")


def test_frontend_js_references_alerts_paths_and_hardening() -> None:
    app_js = _read("apps/pages_dashboard/src/assets/app.js")
    assert "latestAlerts" in app_js
    assert "latestSignalCandidates" in app_js
    assert "latestWeeklyBriefJson" in app_js
    assert "./data/latest_alerts.json" in app_js
    assert "./data/latest_model_leaderboard.json" in app_js
    assert "./data/latest_feature_importance.json" in app_js
    assert "./data/latest_weekly_brief.json" in app_js
    assert "search.list" not in app_js
    assert "http://" not in app_js
    assert "https://" not in app_js


def test_index_html_contains_brief_tab() -> None:
    index_html = _read("apps/pages_dashboard/src/index.html")
    assert 'data-tab="brief"' in index_html
    assert "Brief" in index_html


def test_index_html_contains_topics_nlp_and_content_drivers_tabs() -> None:
    index_html = _read("apps/pages_dashboard/src/index.html")
    assert 'data-tab="topics"' in index_html
    assert 'data-tab="nlp"' in index_html
    assert 'data-tab="content-drivers"' in index_html


def test_index_html_contains_models_tab() -> None:
    index_html = _read("apps/pages_dashboard/src/index.html")
    assert 'data-tab="models"' in index_html
    assert "Models" in index_html


def test_app_js_contains_render_brief() -> None:
    app_js = _read("apps/pages_dashboard/src/assets/app.js")
    assert "function renderBrief()" in app_js


def test_app_js_contains_render_models() -> None:
    app_js = _read("apps/pages_dashboard/src/assets/app.js")
    assert "function renderModels()" in app_js
    assert "RF importance does not imply direction; direction is estimated with prediction-based directional analysis." in app_js


def test_app_js_contains_render_topics_nlp_and_content_drivers() -> None:
    app_js = _read("apps/pages_dashboard/src/assets/app.js")
    assert "function renderTopics()" in app_js
    assert "function renderNlp()" in app_js
    assert "function renderContentDrivers()" in app_js


def test_app_js_data_files_brief_and_signal_candidates_are_unique_and_well_formed() -> None:
    app_js = _read("apps/pages_dashboard/src/assets/app.js")
    assert app_js.count('latestSignalCandidates: "./data/latest_signal_candidates.json"') == 1
    assert app_js.count('latestWeeklyBriefJson: "./data/latest_weekly_brief.json"') == 1
    assert app_js.count('latestWeeklyBriefHtml: "./data/latest_weekly_brief.html"') == 1

    bad_sequence = (
        'latestSignalCandidates: "./data/latest_signal_candidates.json"\n'
        "  latestSignalCandidates:"
    )
    assert bad_sequence not in app_js


def test_app_js_data_files_no_duplicate_period_monthly_channel_without_comma() -> None:
    app_js = _read("apps/pages_dashboard/src/assets/app.js")
    bad_sequence = (
        'periodMonthlyChannel: "./data/period_monthly_channel_metrics.json"\n'
        '  periodMonthlyChannel:'
    )
    assert bad_sequence not in app_js
    assert app_js.count('periodMonthlyChannel: "./data/period_monthly_channel_metrics.json"') == 1


def test_readme_has_no_streamlit_phrase_duplication() -> None:
    readme = _read("README.md")
    assert readme.count("El dashboard no usa Streamlit") == 1
    assert "aún no se integra al workflow monitor/pages automático" not in readme
    assert readme.count("El workflow de Pages construye") == 1
    assert readme.count("- Ejecuta en orden:") == 1


@pytest.mark.parametrize(
    "relpath",
    [
        "apps/pages_dashboard/src/assets/app.js",
        "apps/pages_dashboard/src/assets/charts.js",
        "apps/pages_dashboard/src/assets/formatters.js",
        "apps/pages_dashboard/src/assets/tables.js",
    ],
)
def test_frontend_js_syntax_is_valid_with_node_check(relpath: str) -> None:
    node_bin = shutil.which("node")
    if not node_bin:
        pytest.skip("node not available")

    file_path = REPO_ROOT / relpath
    result = subprocess.run([node_bin, "--check", str(file_path)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
