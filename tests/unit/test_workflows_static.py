from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def _read(relpath: str) -> str:
    return (REPO_ROOT / relpath).read_text(encoding="utf-8")


def test_ci_workflow_exists() -> None:
    assert (REPO_ROOT / ".github/workflows/ci.yml").exists()


def test_monitor_workflow_exists() -> None:
    assert (REPO_ROOT / ".github/workflows/monitor.yml").exists()


def test_monitor_has_required_settings() -> None:
    content = _read(".github/workflows/monitor.yml")
    assert "contents: write" in content
    assert "concurrency:" in content
    assert "group: youtube-monitor" in content
    assert "cancel-in-progress: false" in content
    assert "${{ secrets.YOUTUBE_API_KEY }}" in content
    assert "git add data/" in content
    assert "git add ." not in content
    assert "python -m ytb_history.cli validate-latest" in content
    assert "python -m ytb_history.cli export-latest" in content
    assert "python -m ytb_history.cli build-analytics" in content
    assert "python -m ytb_history.cli generate-alerts" in content
    assert "node --check apps/pages_dashboard/src/assets/app.js" in content
    assert "node --check apps/pages_dashboard/src/assets/charts.js" in content
    assert "node --check apps/pages_dashboard/src/assets/formatters.js" in content
    assert "node --check apps/pages_dashboard/src/assets/tables.js" in content

    run_pos = content.find("python -m ytb_history.cli run")
    test_pos = content.find("pytest -q")
    validate_pos = content.find("python -m ytb_history.cli validate-latest")
    export_pos = content.find("python -m ytb_history.cli export-latest")
    analytics_pos = content.find("python -m ytb_history.cli build-analytics")
    alerts_pos = content.find("python -m ytb_history.cli generate-alerts")
    git_add_pos = content.find("git add data/")
    assert run_pos != -1
    assert test_pos != -1
    assert validate_pos != -1
    assert export_pos != -1
    assert analytics_pos != -1
    assert alerts_pos != -1
    assert git_add_pos != -1
    assert content.count("python -m ytb_history.cli run") == 1
    assert test_pos < run_pos
    assert run_pos < validate_pos < export_pos < analytics_pos < alerts_pos < git_add_pos
    assert "build-analytics" in content[analytics_pos - 120 : analytics_pos + 120]
    assert "${{ secrets.YOUTUBE_API_KEY }}" not in content[analytics_pos - 200 : analytics_pos + 200]
    assert "${{ secrets.YOUTUBE_API_KEY }}" not in content[alerts_pos - 200 : alerts_pos + 200]


def test_workflows_do_not_use_search_list() -> None:
    ci_content = _read(".github/workflows/ci.yml")
    monitor_content = _read(".github/workflows/monitor.yml")
    pages_content = _read(".github/workflows/pages.yml")
    assert "search.list" not in ci_content
    assert "search.list" not in monitor_content
    assert "search.list" not in pages_content


def test_git_add_dot_is_not_used_outside_monitor() -> None:
    ci_content = _read(".github/workflows/ci.yml")
    pages_content = _read(".github/workflows/pages.yml")
    assert "git add ." not in ci_content
    assert "git add ." not in pages_content


def test_ci_does_not_require_youtube_api_key_secret() -> None:
    ci_content = _read(".github/workflows/ci.yml")
    assert "YOUTUBE_API_KEY" not in ci_content


def test_ci_workflow_validates_frontend_js_syntax() -> None:
    ci_content = _read(".github/workflows/ci.yml")
    assert "node --check apps/pages_dashboard/src/assets/app.js" in ci_content
    assert "node --check apps/pages_dashboard/src/assets/charts.js" in ci_content
    assert "node --check apps/pages_dashboard/src/assets/formatters.js" in ci_content
    assert "node --check apps/pages_dashboard/src/assets/tables.js" in ci_content
