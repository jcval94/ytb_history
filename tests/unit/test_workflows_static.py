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
    assert "${{ secrets.YOUTUBE_API_KEY }}" in content
    assert "git add data/" in content
    assert "git add ." not in content
    assert "python -m ytb_history.cli validate-latest" in content
    assert "python -m ytb_history.cli export-latest" in content

    run_pos = content.find("python -m ytb_history.cli run")
    validate_pos = content.find("python -m ytb_history.cli validate-latest")
    export_pos = content.find("python -m ytb_history.cli export-latest")
    assert run_pos != -1
    assert validate_pos != -1
    assert export_pos != -1
    assert run_pos < validate_pos < export_pos


def test_workflows_do_not_use_search_list() -> None:
    ci_content = _read(".github/workflows/ci.yml")
    monitor_content = _read(".github/workflows/monitor.yml")
    assert "search.list" not in ci_content
    assert "search.list" not in monitor_content
