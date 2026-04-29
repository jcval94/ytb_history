from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def _read(relpath: str) -> str:
    return (REPO_ROOT / relpath).read_text(encoding="utf-8")


def test_pages_workflow_exists() -> None:
    assert (REPO_ROOT / ".github/workflows/pages.yml").exists()


def test_pages_workflow_permissions() -> None:
    content = _read(".github/workflows/pages.yml")
    assert "pages: write" in content
    assert "id-token: write" in content


def test_pages_workflow_uses_pages_actions() -> None:
    content = _read(".github/workflows/pages.yml")
    assert "actions/upload-pages-artifact" in content
    assert "actions/deploy-pages" in content


def test_pages_workflow_validates_frontend_js_syntax() -> None:
    content = _read(".github/workflows/pages.yml")
    assert "node --check apps/pages_dashboard/src/assets/app.js" in content
    assert "node --check apps/pages_dashboard/src/assets/charts.js" in content
    assert "node --check apps/pages_dashboard/src/assets/formatters.js" in content
    assert "node --check apps/pages_dashboard/src/assets/tables.js" in content


def test_pages_workflow_build_order() -> None:
    content = _read(".github/workflows/pages.yml")
    analytics_pos = content.find("python -m ytb_history.cli build-analytics")
    nlp_pos = content.find("python -m ytb_history.cli build-nlp-features")
    alerts_pos = content.find("python -m ytb_history.cli generate-alerts")
    decision_pos = content.find("python -m ytb_history.cli build-decision-layer")
    model_int_pos = content.find("python -m ytb_history.cli build-model-intelligence")
    topic_pos = content.find("python -m ytb_history.cli build-topic-intelligence")
    creative_pos = content.find("python -m ytb_history.cli generate-creative-packages")
    brief_pos = content.find("python -m ytb_history.cli generate-weekly-brief")
    pages_pos = content.find("python -m ytb_history.cli build-pages-dashboard")
    assert analytics_pos != -1
    assert nlp_pos != -1
    assert alerts_pos != -1
    assert decision_pos != -1
    assert model_int_pos != -1
    assert topic_pos != -1
    assert creative_pos != -1
    assert brief_pos != -1
    assert pages_pos != -1
    assert analytics_pos < nlp_pos < alerts_pos < decision_pos < model_int_pos < topic_pos < creative_pos < brief_pos < pages_pos


def test_pages_workflow_does_not_use_forbidden_commands_or_secrets() -> None:
    content = _read(".github/workflows/pages.yml")
    assert "YOUTUBE_API_KEY" not in content
    assert "python -m ytb_history.cli run" not in content
    assert "python -m ytb_history.cli train-content-driver-models" not in content
    assert "python -m ytb_history.cli train-model-suite" not in content
    assert "python -m ytb_history.cli smoke-test-model-training" not in content
    assert "git add" not in content
    assert "git push" not in content
    assert "search.list" not in content


def test_pages_workflow_trigger_paths_include_brief_inputs() -> None:
    content = _read(".github/workflows/pages.yml")
    assert "data/briefs/**" in content
    assert "src/ytb_history/services/brief_service.py" in content
    assert "data/creative_packages/**" in content
    assert "data/topic_intelligence/**" in content
    assert "src/ytb_history/services/creative_packages_service.py" in content
    assert "src/ytb_history/services/topic_intelligence_service.py" in content


def test_pages_workflow_publishes_site_path() -> None:
    content = _read(".github/workflows/pages.yml")
    assert "path: site" in content
