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
    assert "./data/latest_alerts.json" in app_js
    assert "search.list" not in app_js
    assert "http://" not in app_js
    assert "https://" not in app_js


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
