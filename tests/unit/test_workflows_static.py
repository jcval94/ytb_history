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
    assert "python -m ytb_history.cli build-nlp-features" in content
    assert "python -m ytb_history.cli generate-alerts" in content
    assert "python -m ytb_history.cli build-decision-layer" in content
    assert "python -m ytb_history.cli build-model-intelligence" in content
    assert "python -m ytb_history.cli build-topic-intelligence" in content
    assert "python -m ytb_history.cli generate-creative-packages" in content
    assert "python -m ytb_history.cli generate-weekly-brief" in content
    assert "python -m ytb_history.cli train-content-driver-models" not in content
    assert "python -m ytb_history.cli build-model-dataset" not in content
    assert "node --check apps/pages_dashboard/src/assets/app.js" in content
    assert "node --check apps/pages_dashboard/src/assets/charts.js" in content
    assert "node --check apps/pages_dashboard/src/assets/formatters.js" in content
    assert "node --check apps/pages_dashboard/src/assets/tables.js" in content

    run_pos = content.find("python -m ytb_history.cli run")
    test_pos = content.find("pytest -q")
    validate_pos = content.find("python -m ytb_history.cli validate-latest")
    export_pos = content.find("python -m ytb_history.cli export-latest")
    analytics_pos = content.find("python -m ytb_history.cli build-analytics")
    nlp_pos = content.find("python -m ytb_history.cli build-nlp-features")
    alerts_pos = content.find("python -m ytb_history.cli generate-alerts")
    decision_pos = content.find("python -m ytb_history.cli build-decision-layer")
    model_int_pos = content.find("python -m ytb_history.cli build-model-intelligence")
    topic_pos = content.find("python -m ytb_history.cli build-topic-intelligence")
    creative_pos = content.find("python -m ytb_history.cli generate-creative-packages")
    brief_pos = content.find("python -m ytb_history.cli generate-weekly-brief")
    git_add_pos = content.find("git add data/")
    assert run_pos != -1
    assert test_pos != -1
    assert validate_pos != -1
    assert export_pos != -1
    assert analytics_pos != -1
    assert nlp_pos != -1
    assert alerts_pos != -1
    assert decision_pos != -1
    assert model_int_pos != -1
    assert topic_pos != -1
    assert creative_pos != -1
    assert brief_pos != -1
    assert git_add_pos != -1
    assert content.count("python -m ytb_history.cli run") == 1
    assert test_pos < run_pos
    assert run_pos < validate_pos < export_pos < analytics_pos < nlp_pos < alerts_pos < decision_pos < model_int_pos < topic_pos < creative_pos < brief_pos < git_add_pos
    assert "build-analytics" in content[analytics_pos - 120 : analytics_pos + 120]
    assert "${{ secrets.YOUTUBE_API_KEY }}" not in content[analytics_pos - 200 : analytics_pos + 200]
    assert "${{ secrets.YOUTUBE_API_KEY }}" not in content[alerts_pos - 200 : alerts_pos + 200]
    assert "${{ secrets.YOUTUBE_API_KEY }}" not in content[decision_pos - 200 : decision_pos + 200]
    assert "${{ secrets.YOUTUBE_API_KEY }}" not in content[creative_pos - 200 : creative_pos + 200]
    assert "${{ secrets.YOUTUBE_API_KEY }}" not in content[brief_pos - 200 : brief_pos + 200]


def test_workflows_do_not_use_search_list() -> None:
    ci_content = _read(".github/workflows/ci.yml")
    monitor_content = _read(".github/workflows/monitor.yml")
    pages_content = _read(".github/workflows/pages.yml")
    train_model_content = _read(".github/workflows/train_model.yml")
    predict_model_content = _read(".github/workflows/predict_model.yml")
    assert "search.list" not in ci_content
    assert "search.list" not in monitor_content
    assert "search.list" not in pages_content
    assert "search.list" not in train_model_content
    assert "search.list" not in predict_model_content


def test_monitor_and_pages_do_not_run_build_model_dataset_yet() -> None:
    monitor_content = _read(".github/workflows/monitor.yml")
    pages_content = _read(".github/workflows/pages.yml")
    assert "python -m ytb_history.cli build-model-dataset" not in monitor_content
    assert "python -m ytb_history.cli build-model-dataset" not in pages_content


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



def test_train_model_workflow_contract() -> None:
    content = _read(".github/workflows/train_model.yml")
    assert "workflow_dispatch:" in content
    assert "cron: '30 10 * * 1,4'" in content
    assert "actions/upload-artifact" in content
    assert "retention-days: 30" in content
    assert "git add data/model_registry/ data/model_reports/" in content
    assert "data/modeling/latest_model_readiness_diagnostics.json" in content
    assert "data/modeling/latest_model_readiness_timeline.csv" in content
    assert "data/modeling/latest_target_coverage_report.csv" in content
    assert "data/modeling/latest_training_gap_report.json" in content
    assert "data/modeling/latest_model_readiness_report.md" in content
    assert "data/modeling/latest_model_readiness_report.html" in content
    assert "git add build/model_artifact" not in content
    assert "YOUTUBE_API_KEY" not in content
    assert "search.list" not in content
    assert "python -m ytb_history.cli train-model-suite" in content
    assert "python -m ytb_history.cli smoke-test-model-training" in content
    assert "python -m ytb_history.cli analyze-model-readiness" in content
    assert "skipped_missing_ml_dependencies" in content
    assert "python -m ytb_history.cli build-nlp-features" in content
    assert "python -m ytb_history.cli build-topic-intelligence" in content
    assert "python -m ytb_history.cli train-content-driver-models" in content
    assert "python -m ytb_history.cli register-trained-artifact" in content
    assert "git add build/content_driver_artifact" not in content
    assert "git add build/model_smoke_test" not in content
    assert "git add *.joblib" not in content
    assert content.index("python -m ytb_history.cli build-model-dataset") < content.index("python -m ytb_history.cli analyze-model-readiness")
    assert content.index("python -m ytb_history.cli smoke-test-model-training") < content.index("python -m ytb_history.cli train-model-suite")



def test_predict_model_workflow_contract() -> None:
    content = _read(".github/workflows/predict_model.yml")
    assert "actions/download-artifact" in content
    assert "run-id:" in content
    assert "github-token: ${{ github.token }}" in content
    assert "python -m ytb_history.cli predict-with-model-artifact" in content
    assert "No valid model artifact registered; skipping predictions" in content
    assert "YOUTUBE_API_KEY" not in content
    assert "search.list" not in content
    assert "git add downloaded_model" not in content
    assert "git add build/model_artifact" not in content
    assert "git add data/predictions/latest_predictions.csv" not in content


def test_pages_workflow_does_not_run_smoke_test_training() -> None:
    content = _read(".github/workflows/pages.yml")
    assert "smoke-test-model-training" not in content


def test_train_model_schedule_is_twice_weekly() -> None:
    train_model_content = _read(".github/workflows/train_model.yml")
    assert "cron: '30 10 * * 1,4'" in train_model_content
