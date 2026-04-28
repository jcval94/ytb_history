"""CLI entrypoint for ytb_history."""

from __future__ import annotations

import argparse
import json

from ytb_history.orchestrator import run_dry_run, run_pipeline
from ytb_history.services.alerts_service import generate_alerts
from ytb_history.services.analytics_service import build_analytics
from ytb_history.services.decision_service import build_decision_layer
from ytb_history.services.brief_service import generate_weekly_brief
from ytb_history.services.export_service import export_latest_run
from ytb_history.services.pages_dashboard_service import build_pages_dashboard
from ytb_history.services.model_dataset_service import build_model_dataset
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

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
