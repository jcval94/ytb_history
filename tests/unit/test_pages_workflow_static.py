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


def test_pages_workflow_build_order() -> None:
    content = _read(".github/workflows/pages.yml")
    analytics_pos = content.find("python -m ytb_history.cli build-analytics")
    pages_pos = content.find("python -m ytb_history.cli build-pages-dashboard")
    assert analytics_pos != -1
    assert pages_pos != -1
    assert analytics_pos < pages_pos


def test_pages_workflow_does_not_use_forbidden_commands_or_secrets() -> None:
    content = _read(".github/workflows/pages.yml")
    assert "YOUTUBE_API_KEY" not in content
    assert "python -m ytb_history.cli run" not in content
    assert "git add" not in content
    assert "git push" not in content
    assert "search.list" not in content


def test_pages_workflow_publishes_site_path() -> None:
    content = _read(".github/workflows/pages.yml")
    assert "path: site" in content
