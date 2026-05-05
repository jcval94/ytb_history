from __future__ import annotations

import json

from ytb_history import cli


def test_cli_run_calls_run_pipeline_once(monkeypatch, capsys) -> None:
    calls: list[dict] = []

    def _fake_run_pipeline(**kwargs):
        calls.append(kwargs)
        return {"status": "success", "channels_total": 1}

    monkeypatch.setattr(cli, "run_pipeline", _fake_run_pipeline)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ytb_history",
            "run",
            "--settings",
            "custom/settings.yaml",
            "--data-dir",
            "custom/data",
        ],
    )

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert len(calls) == 1
    assert calls[0] == {
        "settings_path": "custom/settings.yaml",
        "data_dir": "custom/data",
    }
    assert json.loads(out)["status"] == "success"


def test_cli_dry_run_calls_run_dry_run_once(monkeypatch, capsys) -> None:
    calls: list[dict] = []

    def _fake_run_dry_run(**kwargs):
        calls.append(kwargs)
        return {"total_estimated_units": 100, "should_abort": False}

    monkeypatch.setattr(cli, "run_dry_run", _fake_run_dry_run)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ytb_history",
            "dry-run",
            "--settings",
            "custom/settings.yaml",
            "--data-dir",
            "custom/data",
        ],
    )

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert len(calls) == 1
    assert calls[0] == {
        "settings_path": "custom/settings.yaml",
        "data_dir": "custom/data",
    }
    assert json.loads(out)["total_estimated_units"] == 100


def test_cli_validate_latest_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "validate_latest_run",
        lambda **kwargs: {"status": "success", "latest_report_dir": kwargs["data_dir"]},
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "validate-latest", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out) == {"status": "success", "latest_report_dir": "custom/data"}


def test_cli_validate_latest_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "validate_latest_run", lambda **_kwargs: {"status": "failed"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "validate-latest"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "failed"


def test_cli_export_latest_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "export_latest_run",
        lambda **kwargs: {"status": "success", "export_dir": kwargs["data_dir"]},
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "export-latest", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out) == {"status": "success", "export_dir": "custom/data"}


def test_cli_export_latest_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "export_latest_run", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "export-latest"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"




def test_cli_build_analytics_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "build_analytics",
        lambda **kwargs: {"status": "success", "analytics_dir": kwargs["data_dir"]},
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-analytics", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out) == {"status": "success", "analytics_dir": "custom/data"}


def test_cli_build_analytics_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "build_analytics", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-analytics"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"

def test_build_parser_has_exact_subcommands() -> None:
    parser = cli.build_parser()
    subcommands: set[str] = set()

    for action in parser._actions:
        if hasattr(action, "choices") and action.choices:
            subcommands = set(action.choices.keys())
            break

    assert subcommands == {"run", "dry-run", "validate-latest", "export-latest", "build-analytics", "build-pages-dashboard", "generate-alerts", "build-decision-layer", "generate-weekly-brief", "build-model-dataset", "model-artifact-registry-report", "train-model-suite", "train-baseline-model", "register-trained-artifact", "build-nlp-features", "build-topic-intelligence", "build-model-intelligence", "generate-creative-packages", "train-content-driver-models", "select-transcription-candidates", "transcript-registry-report", "transcribe-selected-videos", "generate-transcript-insights", "predict-with-model-artifact", "analyze-model-readiness", "smoke-test-model-training"}


def test_cli_build_pages_dashboard_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "build_pages_dashboard",
        lambda **kwargs: {
            "status": "success",
            "site_dir": kwargs["site_dir"],
            "row_counts": {},
            "warnings": [],
            "files_written": [],
        },
    )
    monkeypatch.setattr(
        "sys.argv",
        ["ytb_history", "build-pages-dashboard", "--data-dir", "custom/data", "--site-dir", "custom/site"],
    )

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["site_dir"] == "custom/site"


def test_cli_build_pages_dashboard_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "build_pages_dashboard", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-pages-dashboard"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"


def test_cli_generate_alerts_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "generate_alerts",
        lambda **kwargs: {"status": "success", "signals_dir": kwargs["data_dir"], "total_alerts": 3},
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "generate-alerts", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["signals_dir"] == "custom/data"


def test_cli_generate_alerts_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "generate_alerts", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "generate-alerts"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"


def test_cli_transcribe_selected_videos_forwards_ytdlp_options(monkeypatch, capsys) -> None:
    calls: list[dict] = []

    def _fake_transcribe_selected_videos(**kwargs):
        calls.append(kwargs)
        return {"status": "success"}

    monkeypatch.setattr(cli, "transcribe_selected_videos", _fake_transcribe_selected_videos)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ytb_history",
            "transcribe-selected-videos",
            "--data-dir",
            "custom/data",
            "--limit",
            "5",
            "--audio-source-dir",
            "custom/audio",
            "--model",
            "gpt-4o-mini-transcribe",
            "--ytdlp-cookies-file",
            "/tmp/cookies.txt",
            "--ytdlp-browser",
            "firefox",
            "--ytdlp-extra-args",
            "--proxy http://localhost:8080",
        ],
    )

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"
    assert calls == [
        {
            "data_dir": "custom/data",
            "limit": 5,
            "audio_source_dir": "custom/audio",
            "model": "gpt-4o-mini-transcribe",
            "ytdlp_cookies_file": "/tmp/cookies.txt",
            "ytdlp_browser": "firefox",
            "ytdlp_extra_args": ["--proxy", "http://localhost:8080"],
        }
    ]


def test_cli_build_decision_layer_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "build_decision_layer",
        lambda **kwargs: {"status": "success", "decision_dir": kwargs["data_dir"], "total_action_candidates": 2},
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-decision-layer", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    parsed = json.loads(out)
    assert parsed["status"] == "success"
    assert parsed["decision_dir"] == "custom/data"


def test_cli_build_decision_layer_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "build_decision_layer", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-decision-layer"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"


def test_cli_generate_weekly_brief_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "generate_weekly_brief",
        lambda **kwargs: {"status": "success", "latest_json_path": f"{kwargs['data_dir']}/briefs/latest_weekly_brief.json"},
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "generate-weekly-brief", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["latest_json_path"] == "custom/data/briefs/latest_weekly_brief.json"


def test_cli_generate_weekly_brief_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "generate_weekly_brief", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "generate-weekly-brief"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"


def test_cli_build_model_dataset_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "build_model_dataset",
        lambda **kwargs: {"status": "success", "modeling_dir": kwargs["data_dir"]},
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-model-dataset", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["modeling_dir"] == "custom/data"


def test_cli_build_model_dataset_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "build_model_dataset", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-model-dataset"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"


def test_cli_model_artifact_registry_report_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "build_model_artifact_registry_report",
        lambda **kwargs: {"status": "success", "data_dir": kwargs["data_dir"], "config": kwargs["modeling_config_path"]},
    )
    monkeypatch.setattr(
        "sys.argv",
        ["ytb_history", "model-artifact-registry-report", "--data-dir", "custom/data", "--modeling-config", "custom/modeling.yaml"],
    )

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    parsed = json.loads(out)
    assert parsed["status"] == "success"
    assert parsed["data_dir"] == "custom/data"
    assert parsed["config"] == "custom/modeling.yaml"


def test_cli_model_artifact_registry_report_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "build_model_artifact_registry_report", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "model-artifact-registry-report"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"



def test_cli_train_model_suite_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "train_model_suite",
        lambda **kwargs: {"status": "success", "artifact_dir": kwargs["artifact_dir"], "config": kwargs["modeling_config_path"]},
    )
    monkeypatch.setattr(
        "sys.argv",
        ["ytb_history", "train-model-suite", "--data-dir", "custom/data", "--modeling-config", "custom/modeling.yaml", "--artifact-dir", "custom/build"],
    )

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    parsed = json.loads(out)
    assert parsed["status"] == "success"
    assert parsed["artifact_dir"] == "custom/build"


def test_cli_train_baseline_model_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "train_baseline_model",
        lambda **kwargs: {"status": "success", "artifact_dir": kwargs["artifact_dir"], "config": kwargs["modeling_config_path"]},
    )
    monkeypatch.setattr(
        "sys.argv",
        ["ytb_history", "train-baseline-model", "--data-dir", "custom/data", "--modeling-config", "custom/modeling.yaml", "--artifact-dir", "custom/build"],
    )

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    parsed = json.loads(out)
    assert parsed["status"] == "success"
    assert parsed["artifact_dir"] == "custom/build"
    assert parsed["config"] == "custom/modeling.yaml"


def test_cli_register_trained_artifact_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "register_trained_artifact",
        lambda **kwargs: {"status": "success", "artifact_name": kwargs["artifact_name"], "workflow_run_id": kwargs["workflow_run_id"]},
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "ytb_history",
            "register-trained-artifact",
            "--artifact-name",
            "artifact-1",
            "--workflow-run-id",
            "12345",
            "--artifact-dir",
            "build/model_artifact",
            "--data-dir",
            "custom/data",
        ],
    )

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    parsed = json.loads(out)
    assert parsed["status"] == "success"
    assert parsed["artifact_name"] == "artifact-1"
    assert parsed["workflow_run_id"] == "12345"


def test_cli_predict_with_model_artifact_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "predict_with_model_artifact",
        lambda **kwargs: {"status": "success", "model_dir": kwargs["model_dir"], "output_dir": kwargs["output_dir"]},
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "ytb_history",
            "predict-with-model-artifact",
            "--model-dir",
            "downloaded_model",
            "--data-dir",
            "custom/data",
            "--output-dir",
            "custom/predictions",
        ],
    )

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    parsed = json.loads(out)
    assert parsed["status"] == "success"
    assert parsed["model_dir"] == "downloaded_model"
    assert parsed["output_dir"] == "custom/predictions"


def test_cli_build_nlp_features_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "build_nlp_features",
        lambda **kwargs: {"status": "success", "output_dir": f"{kwargs['data_dir']}/nlp_features"},
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-nlp-features", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out) == {"status": "success", "output_dir": "custom/data/nlp_features"}


def test_cli_build_nlp_features_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "build_nlp_features", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-nlp-features"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"


def test_cli_build_topic_intelligence_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "build_topic_intelligence",
        lambda **kwargs: {"status": "success", "output_dir": f"{kwargs['data_dir']}/topic_intelligence"},
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-topic-intelligence", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out) == {"status": "success", "output_dir": "custom/data/topic_intelligence"}


def test_cli_build_topic_intelligence_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "build_topic_intelligence", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-topic-intelligence"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"


def test_cli_train_content_driver_models_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "train_content_driver_models",
        lambda **kwargs: {"status": "success", "artifact_dir": kwargs["artifact_dir"]},
    )
    monkeypatch.setattr(
        "sys.argv",
        ["ytb_history", "train-content-driver-models", "--data-dir", "custom/data", "--artifact-dir", "custom/build"],
    )

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out) == {"status": "success", "artifact_dir": "custom/build"}


def test_cli_train_content_driver_models_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "train_content_driver_models", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "train-content-driver-models"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"


def test_cli_build_model_intelligence_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "build_model_intelligence",
        lambda **kwargs: {"status": "success", "output_dir": f"{kwargs['data_dir']}/model_intelligence"},
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-model-intelligence", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out) == {"status": "success", "output_dir": "custom/data/model_intelligence"}


def test_cli_build_model_intelligence_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "build_model_intelligence", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-model-intelligence"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"


def test_cli_generate_creative_packages_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "generate_creative_packages",
        lambda **kwargs: {"status": "success", "output_dir": kwargs["data_dir"]},
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "generate-creative-packages", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["output_dir"] == "custom/data"


def test_cli_generate_creative_packages_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "generate_creative_packages", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "generate-creative-packages"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"


def test_cli_analyze_model_readiness_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "analyze_model_readiness",
        lambda **_kwargs: {
            "status": "not_ready",
            "can_train_now": False,
            "recommended_status": "not_ready",
            "diagnostics_path": "data/modeling/latest_model_readiness_diagnostics.json",
            "gap_report_path": "data/modeling/latest_training_gap_report.json",
            "warnings": [],
        },
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "analyze-model-readiness", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    payload = json.loads(out)
    assert payload["status"] == "not_ready"
    assert payload["can_train_now"] is False


def test_cli_smoke_test_model_training_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "smoke_test_model_training",
        lambda **_kwargs: {
            "status": "success",
            "models_trained": 3,
            "predictions_generated": 100,
            "leaderboard_rows": 9,
            "feature_importance_rows": 20,
            "artifact_path": "build/model_smoke_test/model_artifact",
            "warnings": [],
        },
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "smoke-test-model-training", "--output-dir", "build/model_smoke_test", "--n-rows", "300"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    payload = json.loads(out)
    assert payload["status"] == "success"
    assert payload["models_trained"] == 3

def test_cli_generate_creative_packages_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "generate_creative_packages",
        lambda **kwargs: {
            "status": "success",
            "creative_packages_dir": f"{kwargs['data_dir']}/creative_packages",
            "total_packages": 2,
            "outputs": {},
            "warnings": [],
        },
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "generate-creative-packages", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    payload = json.loads(out)
    assert payload["status"] == "success"
    assert payload["creative_packages_dir"] == "custom/data/creative_packages"
    assert payload["total_packages"] == 2
