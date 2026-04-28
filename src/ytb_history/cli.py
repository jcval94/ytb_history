"""CLI entrypoint for ytb_history."""

from __future__ import annotations

import argparse
import json

from ytb_history.orchestrator import run_dry_run, run_pipeline
from ytb_history.services.alerts_service import generate_alerts
from ytb_history.services.analytics_service import build_analytics
from ytb_history.services.decision_service import build_decision_layer
from ytb_history.services.brief_service import generate_weekly_brief
from ytb_history.services.creative_packages_service import build_creative_packages
from ytb_history.services.export_service import export_latest_run
from ytb_history.services.pages_dashboard_service import build_pages_dashboard
from ytb_history.services.model_dataset_service import build_model_dataset
from ytb_history.services.model_artifact_registry_service import build_model_artifact_registry_report
from ytb_history.services.model_training_service import train_baseline_model, train_model_suite, register_trained_artifact
from ytb_history.services.model_prediction_service import predict_with_model_artifact
from ytb_history.services.nlp_feature_service import build_nlp_features
from ytb_history.services.topic_intelligence_service import build_topic_intelligence
from ytb_history.services.model_intelligence_service import build_model_intelligence
from ytb_history.services.content_driver_model_service import train_content_driver_models
from ytb_history.services.validation_service import validate_latest_run


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ytb_history")
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="Run daily pipeline")
    run_parser.add_argument("--settings", default="config/settings.yaml")
    run_parser.add_argument("--data-dir", default="data")

    dry_run_parser = sub.add_parser("dry-run", help="Estimate quota without API calls")
    dry_run_parser.add_argument("--settings", default="config/settings.yaml")
    dry_run_parser.add_argument("--data-dir", default="data")

    validate_parser = sub.add_parser("validate-latest", help="Validate latest run artifacts")
    validate_parser.add_argument("--data-dir", default="data")

    export_parser = sub.add_parser("export-latest", help="Export latest run artifacts to CSV")
    export_parser.add_argument("--data-dir", default="data")

    analytics_parser = sub.add_parser("build-analytics", help="Build latest analytics data mart")
    analytics_parser.add_argument("--data-dir", default="data")

    pages_parser = sub.add_parser("build-pages-dashboard", help="Build static dashboard data for GitHub Pages")
    pages_parser.add_argument("--data-dir", default="data")
    pages_parser.add_argument("--site-dir", default="site")

    alerts_parser = sub.add_parser("generate-alerts", help="Generate actionable signals and alerts")
    alerts_parser.add_argument("--data-dir", default="data")

    decision_parser = sub.add_parser("build-decision-layer", help="Build decision intelligence layer outputs")
    decision_parser.add_argument("--data-dir", default="data")

    brief_parser = sub.add_parser("generate-weekly-brief", help="Generate weekly intelligence brief outputs")
    brief_parser.add_argument("--data-dir", default="data")

    model_parser = sub.add_parser("build-model-dataset", help="Build supervised model-ready dataset and readiness report")
    model_parser.add_argument("--data-dir", default="data")

    registry_parser = sub.add_parser("model-artifact-registry-report", help="Build artifact-based model registry readiness report")
    registry_parser.add_argument("--data-dir", default="data")
    registry_parser.add_argument("--modeling-config", default="config/modeling.yaml")

    suite_parser = sub.add_parser("train-model-suite", help="Train interpretable model suite and write artifact directory")
    suite_parser.add_argument("--data-dir", default="data")
    suite_parser.add_argument("--modeling-config", default="config/modeling.yaml")
    suite_parser.add_argument("--artifact-dir", default="build/model_artifact")

    train_parser = sub.add_parser("train-baseline-model", help="Train baseline model (temporary alias) and write artifact directory")
    train_parser.add_argument("--data-dir", default="data")
    train_parser.add_argument("--modeling-config", default="config/modeling.yaml")
    train_parser.add_argument("--artifact-dir", default="build/model_artifact")

    register_parser = sub.add_parser("register-trained-artifact", help="Register trained model artifact into model registry manifests")
    register_parser.add_argument("--artifact-name", required=True)
    register_parser.add_argument("--workflow-run-id", required=True)
    register_parser.add_argument("--artifact-dir", default="build/model_artifact")
    register_parser.add_argument("--data-dir", default="data")

    nlp_parser = sub.add_parser("build-nlp-features", help="Build lightweight NLP feature layer from analytics artifacts")
    nlp_parser.add_argument("--data-dir", default="data")

    topic_parser = sub.add_parser("build-topic-intelligence", help="Build topic and title intelligence from NLP feature artifacts")
    topic_parser.add_argument("--data-dir", default="data")

    model_int_parser = sub.add_parser("build-model-intelligence", help="Build hybrid model intelligence outputs from local prediction and decision artifacts")
    model_int_parser.add_argument("--data-dir", default="data")

    creative_parser = sub.add_parser("generate-creative-packages", help="Generate deterministic creative execution packages from intelligence layers")
    creative_parser.add_argument("--data-dir", default="data")

    content_driver_parser = sub.add_parser("train-content-driver-models", help="Train supervised content driver models with NLP/topic features")
    content_driver_parser.add_argument("--data-dir", default="data")
    content_driver_parser.add_argument("--artifact-dir", default="build/content_driver_artifact")

    predict_parser = sub.add_parser("predict-with-model-artifact", help="Generate predictions using a downloaded model artifact directory")
    predict_parser.add_argument("--model-dir", required=True)
    predict_parser.add_argument("--data-dir", default="data")
    predict_parser.add_argument("--output-dir", default="data/predictions")
    predict_parser.add_argument("--target", default="is_top_growth_7d")
    predict_parser.add_argument("--model-id")
    predict_parser.add_argument("--allow-historical-supervised-fallback", action="store_true")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "run":
        summary = run_pipeline(settings_path=args.settings, data_dir=args.data_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "dry-run":
        summary = run_dry_run(settings_path=args.settings, data_dir=args.data_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "validate-latest":
        summary = validate_latest_run(data_dir=args.data_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "export-latest":
        summary = export_latest_run(data_dir=args.data_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "build-analytics":
        summary = build_analytics(data_dir=args.data_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "build-pages-dashboard":
        summary = build_pages_dashboard(data_dir=args.data_dir, site_dir=args.site_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "generate-alerts":
        summary = generate_alerts(data_dir=args.data_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "build-decision-layer":
        summary = build_decision_layer(data_dir=args.data_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "generate-weekly-brief":
        summary = generate_weekly_brief(data_dir=args.data_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "build-model-dataset":
        summary = build_model_dataset(data_dir=args.data_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "model-artifact-registry-report":
        summary = build_model_artifact_registry_report(data_dir=args.data_dir, modeling_config_path=args.modeling_config)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "train-model-suite":
        summary = train_model_suite(data_dir=args.data_dir, modeling_config_path=args.modeling_config, artifact_dir=args.artifact_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "train-baseline-model":
        summary = train_baseline_model(data_dir=args.data_dir, modeling_config_path=args.modeling_config, artifact_dir=args.artifact_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "register-trained-artifact":
        summary = register_trained_artifact(
            artifact_name=args.artifact_name,
            workflow_run_id=args.workflow_run_id,
            artifact_dir=args.artifact_dir,
            data_dir=args.data_dir,
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "build-nlp-features":
        summary = build_nlp_features(data_dir=args.data_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "build-topic-intelligence":
        summary = build_topic_intelligence(data_dir=args.data_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "build-model-intelligence":
        summary = build_model_intelligence(data_dir=args.data_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "generate-creative-packages":
        summary = build_creative_packages(data_dir=args.data_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "train-content-driver-models":
        summary = train_content_driver_models(data_dir=args.data_dir, artifact_dir=args.artifact_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "predict-with-model-artifact":
        summary = predict_with_model_artifact(
            model_dir=args.model_dir,
            data_dir=args.data_dir,
            output_dir=args.output_dir,
            target=args.target,
            model_id=args.model_id,
            allow_historical_supervised_fallback=args.allow_historical_supervised_fallback,
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
